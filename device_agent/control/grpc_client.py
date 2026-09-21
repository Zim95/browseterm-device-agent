'''
Device Agent's outbound control-stream client (Part 7). Establishes the bidirectional
DeviceControl.Connect() stream, sends Hello + periodic Heartbeat, dispatches ExecuteCommand to a
pluggable handler, resends unacknowledged journal results on connect, and replies to Cloud's Ping
with Pong. Reconnects forever with capped exponential backoff + jitter (control.backoff).

Deliberately split from the raw grpc plumbing: `_build_hello`/`_handle_inbound` are pure/async
methods with no direct I/O dependency, so tests can drive them without a real stream (mirrors how
browseterm-server's DeviceControlServicer tests work).
'''
import asyncio
import json
import time
from typing import Awaitable, Callable, Optional

import grpc

from device_control_spec import device_control_pb2_grpc
from device_control_spec.device_control_pb2 import DeviceToCloud, CloudToDevice
from device_control_spec.device_control_types_pb2 import (
    Hello, Heartbeat, Pong, CommandResult, CommandAccepted, CommandProgress, LocalEvent,
    TerminalTunnelRegistration, DevicePlatform, DeviceArchitecture,
    COMMAND_STATUS_SUCCEEDED, COMMAND_STATUS_FAILED,
)

from device_agent.config import (
    CLOUD_CONTROL_HOST, CLOUD_CONTROL_PORT, CLOUD_CONTROL_TLS, AGENT_VERSION, PROTOCOL_VERSION,
    HEARTBEAT_INTERVAL_SECONDS,
)
from device_agent.control.backoff import backoff_seconds
from device_agent.commands._util import operation_name
from device_agent.state.command_journal import CommandJournal
from device_agent.observability.logging_setup import get_logger

logger = get_logger("grpc_client")

_STOP = object()

_STATUS_TO_WIRE = {"succeeded": COMMAND_STATUS_SUCCEEDED, "failed": COMMAND_STATUS_FAILED}


class ConnectionManager:
    def __init__(
        self,
        device_id: str,
        device_token: str,
        journal: CommandJournal,
        on_execute_command: Callable[[str, "ExecuteCommand"], Awaitable[None]],
        platform: int = DevicePlatform.DEVICE_PLATFORM_UNSPECIFIED,
        architecture: int = DeviceArchitecture.DEVICE_ARCHITECTURE_UNSPECIFIED,
        runtime_version: str = "",
        startup_id: str = "",
        channel_factory: Optional[Callable[[], grpc.aio.Channel]] = None,
    ) -> None:
        self.device_id = device_id
        self.device_token = device_token
        self.journal = journal
        self.on_execute_command = on_execute_command
        self.platform = platform
        self.architecture = architecture
        self.runtime_version = runtime_version
        self.startup_id = startup_id
        self._channel_factory = channel_factory or self._default_channel_factory
        self._outbound: Optional[asyncio.Queue] = None
        self._stopped = False

    def _default_channel_factory(self) -> grpc.aio.Channel:
        target = f"{CLOUD_CONTROL_HOST}:{CLOUD_CONTROL_PORT}"
        if CLOUD_CONTROL_TLS:
            return grpc.aio.secure_channel(target, grpc.ssl_channel_credentials())
        return grpc.aio.insecure_channel(target)

    def build_hello(self) -> DeviceToCloud:
        return DeviceToCloud(hello=Hello(
            device_id=self.device_id, agent_version=AGENT_VERSION, protocol_version=PROTOCOL_VERSION,
            platform=self.platform, architecture=self.architecture, runtime_version=self.runtime_version,
            startup_id=self.startup_id,
        ))

    def build_unreported_results(self) -> list:
        '''"Resend unacknowledged results from the local journal" - one CommandResult per
        journal entry the Agent computed but never heard Cloud accept. `placement_generation`
        isn't tracked in the journal (it's not needed for dedup) - Cloud's own staleness check on
        the command row is authoritative regardless of what this resend carries, so a mismatched
        generation here is safely rejected there rather than silently misapplied.'''
        messages = []
        for entry in self.journal.unreported_terminal_entries():
            messages.append(DeviceToCloud(command_result=CommandResult(
                command_id=entry.command_id,
                status=_STATUS_TO_WIRE[entry.status],
                result_json=entry.result_json or "",
                error_code=entry.error_code or "",
                error_message=entry.error_message or "",
            )))
        return messages

    async def run_forever(self) -> None:
        attempt = 0
        while not self._stopped:
            try:
                logger.info("control.connecting", extra={"attempt": attempt})
                await self._connect_once()
                attempt = 0  # a session that ran and then ended cleanly resets backoff
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error("control.connection_failed", exc_info=True, extra={"attempt": attempt})
            if self._stopped:
                return
            delay = backoff_seconds(attempt)
            logger.info("control.reconnecting", extra={"delay_seconds": delay})
            await asyncio.sleep(delay)
            attempt += 1

    def stop(self) -> None:
        self._stopped = True

    async def _connect_once(self) -> None:
        channel = self._channel_factory()
        try:
            stub = device_control_pb2_grpc.DeviceControlStub(channel)
            self._outbound = asyncio.Queue()

            async def outbound_gen():
                yield self.build_hello()
                for resend in self.build_unreported_results():
                    yield resend
                while True:
                    item = await self._outbound.get()
                    if item is _STOP:
                        return
                    yield item

            metadata = (("authorization", f"Bearer {self.device_token}"),)
            call = stub.Connect(outbound_gen(), metadata=metadata)

            heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            try:
                async for message in call:
                    await self._handle_inbound(message)
            finally:
                heartbeat_task.cancel()
        finally:
            await channel.close()

    async def _heartbeat_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
                await self._send(DeviceToCloud(heartbeat=Heartbeat(sent_at_unix_ms=int(time.time() * 1000))))
        except asyncio.CancelledError:
            pass

    async def _send(self, message: DeviceToCloud) -> None:
        if self._outbound is not None:
            await self._outbound.put(message)

    async def send_command_result(
        self, command_id: str, status: str, result: Optional[dict], error_code: Optional[str], error_message: Optional[str],
    ) -> None:
        '''The CommandExecutor's report_result callback. status is "succeeded" or "failed".'''
        await self._send(DeviceToCloud(command_result=CommandResult(
            command_id=command_id,
            status=_STATUS_TO_WIRE[status],
            result_json=json.dumps(result) if result is not None else "",
            error_code=error_code or "",
            error_message=error_message or "",
        )))

    async def send_command_progress(self, command_id: str, stage: str, message: str) -> None:
        await self._send(DeviceToCloud(command_progress=CommandProgress(
            command_id=command_id, progress_stage=stage, progress_message=message,
        )))

    async def send_local_event(self, event_type: str, payload_json: str) -> None:
        '''Migration Part 12: the generic forwarding channel for local_api/service.py's
        ReportContainerStatus (event_type="container_status_report") and any future local report
        type that doesn't warrant its own dedicated wire message.'''
        await self._send(DeviceToCloud(local_event=LocalEvent(event_type=event_type, payload_json=payload_json)))

    async def send_terminal_tunnel_registration(self, provider: str, public_url: str, generation: int, status: str) -> None:
        await self._send(DeviceToCloud(terminal_tunnel_registration=TerminalTunnelRegistration(
            provider=provider, public_url=public_url, generation=generation, status=status,
        )))

    async def _handle_inbound(self, message: CloudToDevice) -> None:
        kind = message.WhichOneof("payload")
        if kind == "hello_accepted":
            logger.info("control.hello_accepted", extra={
                "connection_generation": message.hello_accepted.connection_generation,
                "reconciliation_required": message.hello_accepted.reconciliation_required,
            })
        elif kind == "execute_command":
            command_id = message.execute_command.command_id
            if self.journal.has_seen(command_id):
                logger.info("control.duplicate_command_ignored", extra={"command_id": command_id})
                # Still worth re-acking/resending a stored result if we have one - but not
                # re-executing. build_unreported_results (sent on every fresh connect) already
                # covers the "we finished but never acked" case; a duplicate arriving mid-session
                # for a command already terminal needs nothing further here.
                return
            self.journal.record_accepted(command_id, operation_name(message.execute_command.operation))
            await self._send(DeviceToCloud(command_accepted=CommandAccepted(
                command_id=command_id, accepted_at_unix_ms=int(time.time() * 1000),
            )))
            await self.on_execute_command(command_id, message.execute_command)
        elif kind == "ping":
            await self._send(DeviceToCloud(pong=Pong(echoed_sent_at_unix_ms=message.ping.sent_at_unix_ms)))
        elif kind == "server_draining":
            logger.info("control.server_draining", extra={"drain_by_unix_ms": message.server_draining.drain_by_unix_ms})
        # inventory_request / cancel_command / active_state_changed / credential_rotation_notice:
        # Parts 22/14/4 - not handled yet.
