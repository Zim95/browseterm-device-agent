'''
Part 7 required tests covered here: "Duplicate command delivery" (dedup via journal.has_terminal_result),
Ping/Pong liveness reply, Hello/HelloAccepted handling, "Resend unacknowledged results from the
local journal" (build_unreported_results).
'''
import asyncio
import os
import tempfile
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

from device_agent.control.grpc_client import ConnectionManager
from device_agent.state.command_journal import CommandJournal
from device_control_spec.device_control_pb2 import CloudToDevice
from device_control_spec.device_control_types_pb2 import (
    HelloAccepted, ExecuteCommand, Ping, ServerDraining, COMMAND_OPERATION_CREATE,
)


class TestConnectionManager(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.journal = CommandJournal(os.path.join(self._tmpdir.name, "journal.sqlite3"))
        self.on_execute_command = AsyncMock()
        self.manager = ConnectionManager(
            device_id="device-1", device_token="tok-1", journal=self.journal,
            on_execute_command=self.on_execute_command,
        )
        self.manager._outbound = asyncio.Queue()

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def test_build_hello_carries_device_identity(self) -> None:
        hello_envelope = self.manager.build_hello()
        self.assertEqual(hello_envelope.WhichOneof("payload"), "hello")
        self.assertEqual(hello_envelope.hello.device_id, "device-1")

    async def test_execute_command_is_dispatched_and_journaled(self) -> None:
        message = CloudToDevice(execute_command=ExecuteCommand(command_id="cmd-1", operation=COMMAND_OPERATION_CREATE))
        await self.manager._handle_inbound(message)
        self.on_execute_command.assert_awaited_once()
        self.assertTrue(self.journal.has_seen("cmd-1"))
        # CommandAccepted must have been queued for send.
        sent = self.manager._outbound.get_nowait()
        self.assertEqual(sent.WhichOneof("payload"), "command_accepted")
        self.assertEqual(sent.command_accepted.command_id, "cmd-1")

    async def test_duplicate_of_a_terminal_command_is_not_redispatched(self) -> None:
        '''Doc-required: "Duplicate command delivery" must not re-run the handler - for a command
        that actually finished. Journaling the terminal result directly (rather than relying on
        on_execute_command, which is a bare mock here and would never call record_result on its
        own) is what makes has_terminal_result() true for the redelivery below.'''
        message = CloudToDevice(execute_command=ExecuteCommand(command_id="cmd-1", operation=COMMAND_OPERATION_CREATE))
        await self.manager._handle_inbound(message)
        self.journal.record_result("cmd-1", "succeeded", result={"ok": True})
        self.on_execute_command.reset_mock()
        self.manager._outbound.get_nowait()  # drain the first CommandAccepted

        await self.manager._handle_inbound(message)  # redelivered
        self.on_execute_command.assert_not_awaited()
        self.assertTrue(self.manager._outbound.empty(), "a duplicate of a terminal command must not re-send CommandAccepted either")

    async def test_redelivery_of_an_interrupted_command_is_redispatched(self) -> None:
        '''Real bug, caught live: a command this process merely accepted (never reached a
        terminal result - e.g. the stream tore down mid-execution) must be retried on
        redelivery, not silently dropped forever. has_seen()-based dedup got this wrong (true for
        "accepted" too, not just terminal), which left a real RESUME command permanently stuck -
        Cloud kept redelivering it, every redelivery was ignored, and Container Maker was never
        actually called.'''
        message = CloudToDevice(execute_command=ExecuteCommand(command_id="cmd-1", operation=COMMAND_OPERATION_CREATE))
        await self.manager._handle_inbound(message)  # accepted, never finishes (mock, no record_result)
        self.on_execute_command.reset_mock()
        self.manager._outbound.get_nowait()  # drain the first CommandAccepted

        await self.manager._handle_inbound(message)  # redelivered while still only "accepted"
        self.on_execute_command.assert_awaited_once()

    async def test_ping_replies_with_pong_echoing_the_timestamp(self) -> None:
        message = CloudToDevice(ping=Ping(sent_at_unix_ms=123456))
        await self.manager._handle_inbound(message)
        sent = self.manager._outbound.get_nowait()
        self.assertEqual(sent.WhichOneof("payload"), "pong")
        self.assertEqual(sent.pong.echoed_sent_at_unix_ms, 123456)

    async def test_hello_accepted_does_not_raise(self) -> None:
        message = CloudToDevice(hello_accepted=HelloAccepted(connection_generation=1))
        await self.manager._handle_inbound(message)  # must not raise

    async def test_server_draining_does_not_raise(self) -> None:
        message = CloudToDevice(server_draining=ServerDraining(drain_by_unix_ms=0))
        await self.manager._handle_inbound(message)  # must not raise

    def test_build_unreported_results_includes_unacked_terminal_entries(self) -> None:
        self.journal.record_accepted("cmd-1", "Create")
        self.journal.record_result("cmd-1", "succeeded", result={"kubernetes_id": "pod-1"})
        messages = self.manager.build_unreported_results()
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0].WhichOneof("payload"), "command_result")
        self.assertEqual(messages[0].command_result.command_id, "cmd-1")

    def test_build_unreported_results_carries_the_real_placement_generation(self) -> None:
        '''
        Regression test for a real production bug: a resent CommandResult never set
        placement_generation, defaulting to 0 on the wire - Cloud's own staleness check rejects
        any result whose generation doesn't match the container's current one, so every resend
        (and, before the executor.py fix, every live send too) was silently and permanently
        rejected. Caught live: Cloud logged "stale command_result rejected ...
        result_generation: 0, current_generation: 1" for a command that had already finished
        successfully on this side.
        '''
        self.journal.record_accepted("cmd-1", "Delete", placement_generation=1)
        self.journal.record_result("cmd-1", "succeeded", result={"ok": True})
        messages = self.manager.build_unreported_results()
        self.assertEqual(messages[0].command_result.placement_generation, 1)

    def test_build_unreported_results_empty_when_nothing_pending(self) -> None:
        self.assertEqual(self.manager.build_unreported_results(), [])

    async def test_hello_accepted_marks_this_connections_resend_batch_reported(self) -> None:
        '''
        Regression test for a real production bug: _finish() used to mark reported_to_cloud=1
        as soon as a CommandResult was queued for send, which proved nothing about actual
        delivery - the outbound queue is discarded on every reconnect (_connect_once creates a
        fresh one), so a result queued right before a drop was silently lost forever while
        already marked "reported", meaning unreported_terminal_entries() never resent it again.
        Caught live: a CREATE's failure result stuck ACCEPTED in Cloud's own device_commands row
        indefinitely while the connection cycled every ~60s. Fixed by deferring mark_reported
        until hello_accepted proves this connection's resend batch actually round-tripped.
        '''
        self.journal.record_accepted("cmd-1", "Create")
        self.journal.record_result("cmd-1", "failed", error_code="X", error_message="boom")
        self.assertFalse(self.journal.get("cmd-1").reported_to_cloud)

        self.manager._pending_resend_ids = ["cmd-1"]
        message = CloudToDevice(hello_accepted=HelloAccepted(connection_generation=1))
        await self.manager._handle_inbound(message)

        self.assertTrue(self.journal.get("cmd-1").reported_to_cloud)
        self.assertEqual(self.manager._pending_resend_ids, [])

    async def test_hello_accepted_with_no_pending_resend_is_a_no_op(self) -> None:
        message = CloudToDevice(hello_accepted=HelloAccepted(connection_generation=1))
        await self.manager._handle_inbound(message)  # must not raise with an empty batch
        self.assertEqual(self.manager._pending_resend_ids, [])

    async def test_send_command_progress_queues_correct_message(self) -> None:
        await self.manager.send_command_progress("cmd-1", "pushing_image", "pushing to registry")
        sent = self.manager._outbound.get_nowait()
        self.assertEqual(sent.WhichOneof("payload"), "command_progress")
        self.assertEqual(sent.command_progress.progress_stage, "pushing_image")

    async def test_send_local_event_queues_correct_message(self) -> None:
        await self.manager.send_local_event("container_status_report", '{"a":1}')
        sent = self.manager._outbound.get_nowait()
        self.assertEqual(sent.WhichOneof("payload"), "local_event")
        self.assertEqual(sent.local_event.event_type, "container_status_report")
        self.assertEqual(sent.local_event.payload_json, '{"a":1}')

    async def test_send_terminal_tunnel_registration_queues_correct_message(self) -> None:
        await self.manager.send_terminal_tunnel_registration("ngrok", "https://x.example.com", 3, "online")
        sent = self.manager._outbound.get_nowait()
        self.assertEqual(sent.WhichOneof("payload"), "terminal_tunnel_registration")
        self.assertEqual(sent.terminal_tunnel_registration.generation, 3)
