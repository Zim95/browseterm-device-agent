import json
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

from device_agent.commands.hibernate import make_handler
from device_agent.clients.container_maker_client import ContainerMakerClientError
from device_control_spec.device_control_types_pb2 import ExecuteCommand


def _execute_command(**overrides) -> ExecuteCommand:
    fields = {
        "command_id": "cmd-1", "container_id": "container-1", "trace_id": "req-1",
        "container_config_json": json.dumps({"network_name": "user1-namespace"}),
    }
    fields.update(overrides)
    return ExecuteCommand(**fields)


def _saved_pod_response() -> SimpleNamespace:
    '''The RPC's own response - pod_name/namespace are real, image_name here is a PREDICTION
    (not present at all, to make sure nothing accidentally reads it) - only Cloud's confirmed
    poll result may supply the real image reference.'''
    return SimpleNamespace(saved_pods=[SimpleNamespace(pod_name="pod-1", namespace_name="user1-namespace")])


_REAL_IMAGE = "registry.example.com/browseterm/user1/container1/img:1"


class TestHibernateHandler(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.container_maker_client = AsyncMock()
        self.cloud_client = AsyncMock()
        self.handler = make_handler(self.container_maker_client, self.cloud_client)
        # Fast tests - real timeout/interval would make the timeout test take 10 minutes.
        patcher1 = patch("device_agent.commands.save_execution.config.SNAPSHOT_POLL_TIMEOUT_SECONDS", 0.05)
        patcher2 = patch("device_agent.commands.save_execution.config.SNAPSHOT_POLL_INTERVAL_SECONDS", 0.01)
        patcher1.start()
        patcher2.start()
        self.addCleanup(patcher1.stop)
        self.addCleanup(patcher2.stop)

    async def test_successful_full_workflow_saves_then_deletes(self) -> None:
        self.container_maker_client.save_container.return_value = _saved_pod_response()
        self.cloud_client.get_save_status.return_value = {"status": "Succeeded", "image_reference": _REAL_IMAGE}
        self.container_maker_client.delete_container.return_value = SimpleNamespace(container_id="pod-1", status="Deleted")

        result, error_code, error_message = await self.handler(_execute_command())

        self.assertIsNone(error_code)
        self.assertEqual(result["saved_image"], _REAL_IMAGE)
        self.container_maker_client.save_container.assert_awaited_once()
        self.container_maker_client.delete_container.assert_awaited_once()
        # save must be CONFIRMED (via cloud_client) before delete - the whole point of the fix.
        self.cloud_client.get_save_status.assert_awaited()

    async def test_snapshot_trigger_failure_leaves_pod_running(self) -> None:
        '''Doc-required: "Snapshot failure leaves pod running and quota reserved" - delete must
        never be called if the RPC itself fails to trigger a save.'''
        self.container_maker_client.save_container.side_effect = ContainerMakerClientError("snapshot job failed")

        result, error_code, error_message = await self.handler(_execute_command())

        self.assertIsNone(result)
        self.assertEqual(error_code, "SNAPSHOT_FAILED")
        self.container_maker_client.delete_container.assert_not_awaited()

    async def test_save_returning_no_pod_is_treated_as_snapshot_failure(self) -> None:
        self.container_maker_client.save_container.return_value = SimpleNamespace(saved_pods=[])

        result, error_code, error_message = await self.handler(_execute_command())

        self.assertIsNone(result)
        self.assertEqual(error_code, "SNAPSHOT_FAILED")
        self.container_maker_client.delete_container.assert_not_awaited()

    async def test_confirmed_snapshot_failure_leaves_pod_running(self) -> None:
        '''The RPC trigger succeeds, but Cloud later confirms the snapshot itself failed
        (build/push error) - this is the core bug fix: the pod must NOT be deleted just because
        the RPC returned successfully.'''
        self.container_maker_client.save_container.return_value = _saved_pod_response()
        self.cloud_client.get_save_status.return_value = {"status": "Failed", "error_detail": "registry push failed: unauthorized"}

        result, error_code, error_message = await self.handler(_execute_command())

        self.assertIsNone(result)
        self.assertEqual(error_code, "SNAPSHOT_FAILED")
        self.container_maker_client.delete_container.assert_not_awaited()

    async def test_poll_timeout_leaves_pod_running(self) -> None:
        '''Cloud never confirms Succeeded or Failed within the timeout window (e.g. snapshot_job
        crashed without ever reporting) - must NOT be treated as success, pod stays running.'''
        self.container_maker_client.save_container.return_value = _saved_pod_response()
        self.cloud_client.get_save_status.return_value = {"status": "Running"}

        result, error_code, error_message = await self.handler(_execute_command())

        self.assertIsNone(result)
        self.assertEqual(error_code, "SNAPSHOT_TIMED_OUT")
        self.container_maker_client.delete_container.assert_not_awaited()

    async def test_pod_delete_failure_after_confirmed_save_is_reported_with_saved_image(self) -> None:
        '''Doc-required: "Pod delete failure does not release quota prematurely" - the command
        must be reported FAILED (not succeeded) so Cloud never marks the container HIBERNATED
        while the pod is still actually running, but the saved image is preserved in the error
        payload so a retry doesn't need to re-snapshot.'''
        self.container_maker_client.save_container.return_value = _saved_pod_response()
        self.cloud_client.get_save_status.return_value = {"status": "Succeeded", "image_reference": _REAL_IMAGE}
        self.container_maker_client.delete_container.side_effect = ContainerMakerClientError("pod delete timed out")

        result, error_code, error_message = await self.handler(_execute_command())

        self.assertIsNone(result)
        self.assertEqual(error_code, "POD_DELETE_FAILED_AFTER_SAVE")
        payload = json.loads(error_message)
        self.assertEqual(payload["saved_image"], _REAL_IMAGE)

    async def test_duplicate_hibernate_command_is_independently_idempotent(self) -> None:
        '''Doc-required: "Duplicate hibernate command." Executing the handler twice (simulating
        two deliveries both reaching execution, e.g. before ConnectionManager-level dedup would
        normally prevent the second) must not corrupt anything - each run independently follows
        the same safe ordering; whether the second run's delete succeeds or hits "already gone"
        depends on delete.py's own idempotency, exercised there.'''
        self.container_maker_client.save_container.return_value = _saved_pod_response()
        self.cloud_client.get_save_status.return_value = {"status": "Succeeded", "image_reference": _REAL_IMAGE}
        self.container_maker_client.delete_container.return_value = SimpleNamespace(container_id="pod-1", status="Deleted")

        first = await self.handler(_execute_command())
        second = await self.handler(_execute_command())

        self.assertIsNone(first[1])
        self.assertIsNone(second[1])
        self.assertEqual(self.container_maker_client.save_container.await_count, 2)
