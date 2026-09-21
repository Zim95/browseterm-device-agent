'''
Part 7 required tests covered here: "Duplicate command delivery" (dedup via journal.has_seen),
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

    async def test_duplicate_execute_command_is_not_redispatched(self) -> None:
        '''Doc-required: "Duplicate command delivery" must not re-run the handler.'''
        message = CloudToDevice(execute_command=ExecuteCommand(command_id="cmd-1", operation=COMMAND_OPERATION_CREATE))
        await self.manager._handle_inbound(message)
        self.on_execute_command.reset_mock()
        self.manager._outbound.get_nowait()  # drain the first CommandAccepted

        await self.manager._handle_inbound(message)  # redelivered
        self.on_execute_command.assert_not_awaited()
        self.assertTrue(self.manager._outbound.empty(), "a duplicate must not re-send CommandAccepted either")

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

    def test_build_unreported_results_empty_when_nothing_pending(self) -> None:
        self.assertEqual(self.manager.build_unreported_results(), [])
