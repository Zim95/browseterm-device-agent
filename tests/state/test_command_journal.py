'''
Part 7 required tests covered here: "Duplicate command delivery", "Local journal recovery",
"Crash after Container Maker succeeds but before Cloud acknowledgement".
'''
import tempfile
import os
from unittest import TestCase

from device_agent.state.command_journal import CommandJournal


class TestCommandJournal(TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.journal = CommandJournal(os.path.join(self._tmpdir.name, "journal.sqlite3"))

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def test_has_seen_false_for_unknown_command(self) -> None:
        self.assertFalse(self.journal.has_seen("cmd-1"))

    def test_record_accepted_then_has_seen_true(self) -> None:
        self.journal.record_accepted("cmd-1", "Create")
        self.assertTrue(self.journal.has_seen("cmd-1"))
        entry = self.journal.get("cmd-1")
        self.assertEqual(entry.status, "accepted")
        self.assertEqual(entry.operation, "Create")

    def test_duplicate_record_accepted_does_not_reset_progress(self) -> None:
        '''Doc-required: "Duplicate command delivery" must be safe.'''
        self.journal.record_accepted("cmd-1", "Create")
        self.journal.record_result("cmd-1", "succeeded", result={"kubernetes_id": "pod-1"})
        self.journal.record_accepted("cmd-1", "Create")  # a redelivered ExecuteCommand
        entry = self.journal.get("cmd-1")
        self.assertEqual(entry.status, "succeeded", "a duplicate accept must not roll back an already-completed command")
        self.assertEqual(entry.result_json, '{"kubernetes_id": "pod-1"}')

    def test_record_result_leaves_reported_to_cloud_false(self) -> None:
        '''Doc-required: "Crash after Container Maker succeeds but before Cloud acknowledgement" -
        the result is durable but not yet marked reported until the Agent hears back from Cloud.'''
        self.journal.record_accepted("cmd-1", "Create")
        self.journal.record_result("cmd-1", "succeeded", result={"kubernetes_id": "pod-1"})
        entry = self.journal.get("cmd-1")
        self.assertFalse(entry.reported_to_cloud)

    def test_unreported_terminal_entries_returns_unacked_results(self) -> None:
        self.journal.record_accepted("cmd-1", "Create")
        self.journal.record_result("cmd-1", "succeeded", result={"kubernetes_id": "pod-1"})
        self.journal.record_accepted("cmd-2", "Delete")  # still in-flight, not terminal yet

        unreported = self.journal.unreported_terminal_entries()
        self.assertEqual([e.command_id for e in unreported], ["cmd-1"])

    def test_mark_reported_removes_entry_from_unreported_list(self) -> None:
        self.journal.record_accepted("cmd-1", "Create")
        self.journal.record_result("cmd-1", "succeeded")
        self.journal.mark_reported("cmd-1")
        self.assertEqual(self.journal.unreported_terminal_entries(), [])

    def test_journal_recovery_new_instance_same_db_path_sees_prior_state(self) -> None:
        '''Doc-required: "Local journal recovery" - a fresh CommandJournal (simulating a Device
        Agent pod restart onto the same PersistentVolume) must see everything the prior process
        instance wrote.'''
        self.journal.record_accepted("cmd-1", "Create")
        self.journal.record_result("cmd-1", "succeeded", result={"kubernetes_id": "pod-1"})

        recovered_journal = CommandJournal(self.journal.db_path)
        self.assertTrue(recovered_journal.has_seen("cmd-1"))
        entry = recovered_journal.get("cmd-1")
        self.assertEqual(entry.status, "succeeded")
        self.assertFalse(entry.reported_to_cloud)

    def test_record_result_failed_stores_error_fields(self) -> None:
        self.journal.record_accepted("cmd-1", "Create")
        self.journal.record_result("cmd-1", "failed", error_code="POD_CREATE_FAILED", error_message="node out of capacity")
        entry = self.journal.get("cmd-1")
        self.assertEqual(entry.status, "failed")
        self.assertEqual(entry.error_code, "POD_CREATE_FAILED")
        self.assertEqual(entry.error_message, "node out of capacity")
