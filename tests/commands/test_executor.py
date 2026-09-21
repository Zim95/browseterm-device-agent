import os
import tempfile
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

from device_agent.commands.executor import CommandExecutor
from device_agent.state.command_journal import CommandJournal
from device_control_spec.device_control_types_pb2 import ExecuteCommand, COMMAND_OPERATION_CREATE, COMMAND_OPERATION_DELETE


class TestCommandExecutor(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.journal = CommandJournal(os.path.join(self._tmpdir.name, "journal.sqlite3"))
        self.report_result = AsyncMock()
        self.executor = CommandExecutor(journal=self.journal, report_result=self.report_result)

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    async def test_successful_handler_reports_succeeded_and_journals(self) -> None:
        async def handler(execute_command):
            return {"kubernetes_id": "pod-1"}, None, None
        self.executor.register(COMMAND_OPERATION_CREATE, handler)

        await self.executor.execute("cmd-1", ExecuteCommand(command_id="cmd-1", operation=COMMAND_OPERATION_CREATE))

        self.report_result.assert_awaited_once_with("cmd-1", "succeeded", {"kubernetes_id": "pod-1"}, None, None)
        entry = self.journal.get("cmd-1")
        self.assertEqual(entry.status, "succeeded")
        self.assertTrue(entry.reported_to_cloud)

    async def test_handler_returning_error_code_reports_failed(self) -> None:
        async def handler(execute_command):
            return None, "CONTAINER_MAKER_ERROR", "node full"
        self.executor.register(COMMAND_OPERATION_CREATE, handler)

        await self.executor.execute("cmd-1", ExecuteCommand(command_id="cmd-1", operation=COMMAND_OPERATION_CREATE))

        self.report_result.assert_awaited_once_with("cmd-1", "failed", None, "CONTAINER_MAKER_ERROR", "node full")

    async def test_handler_raising_is_caught_and_reported_as_failed(self) -> None:
        '''A handler bug must never leave a command unreported forever.'''
        async def handler(execute_command):
            raise RuntimeError("unexpected")
        self.executor.register(COMMAND_OPERATION_CREATE, handler)

        await self.executor.execute("cmd-1", ExecuteCommand(command_id="cmd-1", operation=COMMAND_OPERATION_CREATE))

        self.report_result.assert_awaited_once()
        args, _ = self.report_result.call_args
        self.assertEqual(args[1], "failed")
        self.assertEqual(args[3], "HANDLER_EXCEPTION")

    async def test_unregistered_operation_reports_failed_without_raising(self) -> None:
        await self.executor.execute("cmd-1", ExecuteCommand(command_id="cmd-1", operation=COMMAND_OPERATION_DELETE))
        self.report_result.assert_awaited_once()
        args, _ = self.report_result.call_args
        self.assertEqual(args[3], "NO_HANDLER")

    async def test_execute_without_prior_record_accepted_still_journals_correctly(self) -> None:
        '''Defensive: execute() must not assume the caller already journaled - it must be
        durable/dedup-able even if invoked directly.'''
        async def handler(execute_command):
            return {"ok": True}, None, None
        self.executor.register(COMMAND_OPERATION_CREATE, handler)

        await self.executor.execute("cmd-1", ExecuteCommand(command_id="cmd-1", operation=COMMAND_OPERATION_CREATE))

        self.assertTrue(self.journal.has_seen("cmd-1"))
        entry = self.journal.get("cmd-1")
        self.assertEqual(entry.status, "succeeded")
        self.assertTrue(entry.reported_to_cloud)

    async def test_journal_transitions_through_running_before_terminal(self) -> None:
        async def handler(execute_command):
            entry = self.journal.get("cmd-1")
            self.assertEqual(entry.status, "running", "handler must observe 'running' status while it executes")
            return {}, None, None
        self.executor.register(COMMAND_OPERATION_CREATE, handler)
        self.journal.record_accepted("cmd-1", "Create")

        await self.executor.execute("cmd-1", ExecuteCommand(command_id="cmd-1", operation=COMMAND_OPERATION_CREATE))
