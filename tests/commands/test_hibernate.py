import json
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

from device_agent.commands.hibernate import make_handler
from device_agent.clients.container_maker_client import ContainerMakerClientError
from device_control_spec.device_control_types_pb2 import ExecuteCommand


def _execute_command(**overrides) -> ExecuteCommand:
    fields = {"command_id": "cmd-1", "container_id": "container-1", "container_config_json": json.dumps({"network_name": "user1-namespace"})}
    fields.update(overrides)
    return ExecuteCommand(**fields)


def _saved_pod_response(image_name="registry.example.com/browseterm/user1/container1/img:1") -> SimpleNamespace:
    return SimpleNamespace(saved_pods=[SimpleNamespace(image_name=image_name, pod_name="pod-1", namespace_name="user1-namespace")])


class TestHibernateHandler(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.container_maker_client = AsyncMock()
        self.handler = make_handler(self.container_maker_client, cloud_client=None)

    async def test_successful_full_workflow_saves_then_deletes(self) -> None:
        self.container_maker_client.save_container.return_value = _saved_pod_response()
        self.container_maker_client.delete_container.return_value = SimpleNamespace(container_id="pod-1", status="Deleted")

        result, error_code, error_message = await self.handler(_execute_command())

        self.assertIsNone(error_code)
        self.assertEqual(result["saved_image"], "registry.example.com/browseterm/user1/container1/img:1")
        self.container_maker_client.save_container.assert_awaited_once()
        self.container_maker_client.delete_container.assert_awaited_once()
        # save must happen before delete - the whole point of the ordering invariant.
        save_call_order = self.container_maker_client.mock_calls.index(
            [c for c in self.container_maker_client.mock_calls if c[0] == "save_container"][0]
        )
        delete_call_order = self.container_maker_client.mock_calls.index(
            [c for c in self.container_maker_client.mock_calls if c[0] == "delete_container"][0]
        )
        self.assertLess(save_call_order, delete_call_order)

    async def test_snapshot_failure_leaves_pod_running(self) -> None:
        '''Doc-required: "Snapshot failure leaves pod running and quota reserved" - delete must
        never be called if save fails.'''
        self.container_maker_client.save_container.side_effect = ContainerMakerClientError("snapshot job failed")

        result, error_code, error_message = await self.handler(_execute_command())

        self.assertIsNone(result)
        self.assertEqual(error_code, "SNAPSHOT_FAILED")
        self.container_maker_client.delete_container.assert_not_awaited()

    async def test_save_returning_no_image_is_treated_as_snapshot_failure(self) -> None:
        self.container_maker_client.save_container.return_value = SimpleNamespace(saved_pods=[])

        result, error_code, error_message = await self.handler(_execute_command())

        self.assertIsNone(result)
        self.assertEqual(error_code, "SNAPSHOT_FAILED")
        self.container_maker_client.delete_container.assert_not_awaited()

    async def test_push_failure_leaves_pod_running(self) -> None:
        '''"Push failure leaves pod running" - container-maker's save_container call itself
        covers build+push; a failure anywhere in that pipeline surfaces as the same
        ContainerMakerClientError path as any other save failure.'''
        self.container_maker_client.save_container.side_effect = ContainerMakerClientError("registry push failed: unauthorized")

        result, error_code, error_message = await self.handler(_execute_command())

        self.assertEqual(error_code, "SNAPSHOT_FAILED")
        self.container_maker_client.delete_container.assert_not_awaited()

    async def test_pod_delete_failure_after_successful_save_is_reported_with_saved_image(self) -> None:
        '''Doc-required: "Pod delete failure does not release quota prematurely" - the command
        must be reported FAILED (not succeeded) so Cloud never marks the container HIBERNATED
        while the pod is still actually running, but the saved image is preserved in the error
        payload so a retry doesn't need to re-snapshot.'''
        self.container_maker_client.save_container.return_value = _saved_pod_response()
        self.container_maker_client.delete_container.side_effect = ContainerMakerClientError("pod delete timed out")

        result, error_code, error_message = await self.handler(_execute_command())

        self.assertIsNone(result)
        self.assertEqual(error_code, "POD_DELETE_FAILED_AFTER_SAVE")
        payload = json.loads(error_message)
        self.assertEqual(payload["saved_image"], "registry.example.com/browseterm/user1/container1/img:1")

    async def test_duplicate_hibernate_command_is_independently_idempotent(self) -> None:
        '''Doc-required: "Duplicate hibernate command." Executing the handler twice (simulating
        two deliveries both reaching execution, e.g. before ConnectionManager-level dedup would
        normally prevent the second) must not corrupt anything - each run independently follows
        the same safe ordering; whether the second run's delete succeeds or hits "already gone"
        depends on delete.py's own idempotency, exercised there.'''
        self.container_maker_client.save_container.return_value = _saved_pod_response()
        self.container_maker_client.delete_container.return_value = SimpleNamespace(container_id="pod-1", status="Deleted")

        first = await self.handler(_execute_command())
        second = await self.handler(_execute_command())

        self.assertIsNone(first[1])
        self.assertIsNone(second[1])
        self.assertEqual(self.container_maker_client.save_container.await_count, 2)
