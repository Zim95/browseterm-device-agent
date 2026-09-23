import os
import tempfile
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

from device_agent.commands.executor import CommandExecutor
from device_agent.state.command_journal import CommandJournal
from device_agent.state.placement_cache import PlacementCache
from device_control_spec.device_control_types_pb2 import (
    ExecuteCommand, COMMAND_OPERATION_CREATE, COMMAND_OPERATION_DELETE,
    COMMAND_OPERATION_HIBERNATE, COMMAND_OPERATION_RESUME,
)


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

        self.report_result.assert_awaited_once_with("cmd-1", "succeeded", 0, {"kubernetes_id": "pod-1"}, None, None)
        entry = self.journal.get("cmd-1")
        self.assertEqual(entry.status, "succeeded")
        # _finish() no longer marks reported_to_cloud itself - queuing for send proves nothing
        # about actual delivery (the outbound queue is discarded on reconnect). grpc_client.py's
        # hello_accepted handler is what marks it, only once a live round-trip proves the resend
        # went out - see executor.py's _finish() docstring for the production bug this fixed.
        self.assertFalse(entry.reported_to_cloud)
        self.assertIn("cmd-1", [e.command_id for e in self.journal.unreported_terminal_entries()])

    async def test_placement_generation_from_execute_command_is_carried_to_report_result(self) -> None:
        '''
        Regression test for a real production bug: report_result (and the journal entry behind
        every resend) never carried placement_generation at all, so it always defaulted to 0 on
        the wire. Cloud's own staleness check rejects any CommandResult whose generation doesn't
        match the container's current one - and since a container's generation is 1 after its
        very first command, EVERY result (live sends and resends alike) was being silently and
        permanently rejected. Caught live: Cloud's logs showed "stale command_result rejected ...
        result_generation: 0, current_generation: 1" for a DELETE that had already finished
        successfully on this side. The fix threads the real value from ExecuteCommand through
        record_accepted -> the journal -> _finish -> report_result.
        '''
        async def handler(execute_command):
            return {"ok": True}, None, None
        self.executor.register(COMMAND_OPERATION_CREATE, handler)

        await self.executor.execute("cmd-1", ExecuteCommand(command_id="cmd-1", operation=COMMAND_OPERATION_CREATE, placement_generation=3))

        self.report_result.assert_awaited_once_with("cmd-1", "succeeded", 3, {"ok": True}, None, None)
        entry = self.journal.get("cmd-1")
        self.assertEqual(entry.placement_generation, 3)

    async def test_handler_returning_error_code_reports_failed(self) -> None:
        async def handler(execute_command):
            return None, "CONTAINER_MAKER_ERROR", "node full"
        self.executor.register(COMMAND_OPERATION_CREATE, handler)

        await self.executor.execute("cmd-1", ExecuteCommand(command_id="cmd-1", operation=COMMAND_OPERATION_CREATE))

        self.report_result.assert_awaited_once_with("cmd-1", "failed", 0, None, "CONTAINER_MAKER_ERROR", "node full")

    async def test_handler_raising_is_caught_and_reported_as_failed(self) -> None:
        '''A handler bug must never leave a command unreported forever.'''
        async def handler(execute_command):
            raise RuntimeError("unexpected")
        self.executor.register(COMMAND_OPERATION_CREATE, handler)

        await self.executor.execute("cmd-1", ExecuteCommand(command_id="cmd-1", operation=COMMAND_OPERATION_CREATE))

        self.report_result.assert_awaited_once()
        args, _ = self.report_result.call_args
        self.assertEqual(args[1], "failed")
        self.assertEqual(args[4], "HANDLER_EXCEPTION")

    async def test_unregistered_operation_reports_failed_without_raising(self) -> None:
        await self.executor.execute("cmd-1", ExecuteCommand(command_id="cmd-1", operation=COMMAND_OPERATION_DELETE))
        self.report_result.assert_awaited_once()
        args, _ = self.report_result.call_args
        self.assertEqual(args[4], "NO_HANDLER")

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
        # See test_successful_handler_reports_succeeded_and_journals - reported_to_cloud is now
        # grpc_client.py's responsibility, confirmed only via a live hello_accepted round-trip.
        self.assertFalse(entry.reported_to_cloud)

    async def test_journal_transitions_through_running_before_terminal(self) -> None:
        async def handler(execute_command):
            entry = self.journal.get("cmd-1")
            self.assertEqual(entry.status, "running", "handler must observe 'running' status while it executes")
            return {}, None, None
        self.executor.register(COMMAND_OPERATION_CREATE, handler)
        self.journal.record_accepted("cmd-1", "Create")

        await self.executor.execute("cmd-1", ExecuteCommand(command_id="cmd-1", operation=COMMAND_OPERATION_CREATE))


class TestCommandExecutorPlacementCache(IsolatedAsyncioTestCase):
    '''migration Part 12 gap-fill: successful Create/Resume must populate the placement cache
    (so ReportContainerStatus can later supply a real placement_generation); successful
    Delete/Hibernate must clear it (the container no longer lives on this device).'''

    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.journal = CommandJournal(os.path.join(self._tmpdir.name, "journal.sqlite3"))
        self.placement_cache = PlacementCache()
        self.executor = CommandExecutor(journal=self.journal, report_result=AsyncMock(), placement_cache=self.placement_cache)

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    async def test_successful_create_records_placement(self) -> None:
        async def handler(execute_command):
            return {"kubernetes_id": "pod-1"}, None, None
        self.executor.register(COMMAND_OPERATION_CREATE, handler)

        await self.executor.execute(
            "cmd-1",
            ExecuteCommand(command_id="cmd-1", operation=COMMAND_OPERATION_CREATE, container_id="c1", device_id="d1", placement_generation=1),
        )

        self.assertEqual(self.placement_cache.get("c1"), ("d1", 1))

    async def test_successful_resume_updates_placement_generation(self) -> None:
        self.placement_cache.record("c1", "d1", 1)

        async def handler(execute_command):
            return {"kubernetes_id": "pod-2"}, None, None
        self.executor.register(COMMAND_OPERATION_RESUME, handler)

        await self.executor.execute(
            "cmd-2",
            ExecuteCommand(command_id="cmd-2", operation=COMMAND_OPERATION_RESUME, container_id="c1", device_id="d1", placement_generation=2),
        )

        self.assertEqual(self.placement_cache.get("c1"), ("d1", 2))

    async def test_failed_create_does_not_record_placement(self) -> None:
        async def handler(execute_command):
            return None, "CONTAINER_MAKER_ERROR", "node full"
        self.executor.register(COMMAND_OPERATION_CREATE, handler)

        await self.executor.execute(
            "cmd-1",
            ExecuteCommand(command_id="cmd-1", operation=COMMAND_OPERATION_CREATE, container_id="c1", device_id="d1", placement_generation=1),
        )

        self.assertIsNone(self.placement_cache.get("c1"))

    async def test_successful_delete_forgets_placement(self) -> None:
        self.placement_cache.record("c1", "d1", 1)

        async def handler(execute_command):
            return {}, None, None
        self.executor.register(COMMAND_OPERATION_DELETE, handler)

        await self.executor.execute(
            "cmd-1", ExecuteCommand(command_id="cmd-1", operation=COMMAND_OPERATION_DELETE, container_id="c1", device_id="d1"),
        )

        self.assertIsNone(self.placement_cache.get("c1"))

    async def test_successful_hibernate_forgets_placement(self) -> None:
        self.placement_cache.record("c1", "d1", 1)

        async def handler(execute_command):
            return {"saved_image": "img:1"}, None, None
        self.executor.register(COMMAND_OPERATION_HIBERNATE, handler)

        await self.executor.execute(
            "cmd-1", ExecuteCommand(command_id="cmd-1", operation=COMMAND_OPERATION_HIBERNATE, container_id="c1", device_id="d1"),
        )

        self.assertIsNone(self.placement_cache.get("c1"))

    async def test_no_placement_cache_does_not_raise(self) -> None:
        '''placement_cache is optional (defaults to None) - existing callers that never pass one
        must keep working unchanged.'''
        executor = CommandExecutor(journal=self.journal, report_result=AsyncMock())

        async def handler(execute_command):
            return {}, None, None
        executor.register(COMMAND_OPERATION_CREATE, handler)

        await executor.execute(
            "cmd-1",
            ExecuteCommand(command_id="cmd-1", operation=COMMAND_OPERATION_CREATE, container_id="c1", device_id="d1", placement_generation=1),
        )  # must not raise
