import json
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

from device_agent.commands.save import make_handler
from device_control_spec.device_control_types_pb2 import ExecuteCommand

_REAL_IMAGE = "registry.example.com/browseterm/user1/container1/img:1"


def _execute_command(**overrides) -> ExecuteCommand:
    fields = {
        "command_id": "cmd-1", "container_id": "container-1", "trace_id": "req-1",
        "container_config_json": json.dumps({"network_name": "user1-namespace"}),
    }
    fields.update(overrides)
    return ExecuteCommand(**fields)


def _saved_pod_response() -> SimpleNamespace:
    return SimpleNamespace(saved_pods=[SimpleNamespace(pod_name="pod-1", namespace_name="user1-namespace")])


class TestSaveHandler(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.container_maker_client = AsyncMock()
        self.cloud_client = AsyncMock()
        self.handler = make_handler(self.container_maker_client, self.cloud_client)
        patcher1 = patch("device_agent.commands.save_execution.config.SNAPSHOT_POLL_TIMEOUT_SECONDS", 0.05)
        patcher2 = patch("device_agent.commands.save_execution.config.SNAPSHOT_POLL_INTERVAL_SECONDS", 0.01)
        patcher1.start()
        patcher2.start()
        self.addCleanup(patcher1.stop)
        self.addCleanup(patcher2.stop)

    async def test_happy_path_never_deletes_the_pod(self) -> None:
        '''The core distinction from Hibernate: SAVE must never call delete_container, regardless
        of outcome - the container keeps running.'''
        self.container_maker_client.save_container.return_value = _saved_pod_response()
        self.cloud_client.get_save_status.return_value = {"status": "Succeeded", "image_reference": _REAL_IMAGE}

        result, error_code, error_message = await self.handler(_execute_command())

        self.assertIsNone(error_code)
        self.assertEqual(result["saved_image"], _REAL_IMAGE)
        self.container_maker_client.delete_container.assert_not_awaited()

    async def test_confirmed_failure_reports_failed_and_never_deletes(self) -> None:
        self.container_maker_client.save_container.return_value = _saved_pod_response()
        self.cloud_client.get_save_status.return_value = {"status": "Failed", "error_detail": "build failed"}

        result, error_code, error_message = await self.handler(_execute_command())

        self.assertIsNone(result)
        self.assertEqual(error_code, "SNAPSHOT_FAILED")
        self.container_maker_client.delete_container.assert_not_awaited()

    async def test_poll_timeout_reports_timed_out_and_never_deletes(self) -> None:
        self.container_maker_client.save_container.return_value = _saved_pod_response()
        self.cloud_client.get_save_status.return_value = {"status": "Running"}

        result, error_code, error_message = await self.handler(_execute_command())

        self.assertIsNone(result)
        self.assertEqual(error_code, "SNAPSHOT_TIMED_OUT")
        self.container_maker_client.delete_container.assert_not_awaited()

    async def test_duplicate_save_command_is_independently_idempotent(self) -> None:
        '''Mirrors hibernate's own duplicate-command test - each execution runs the same safe
        sequence independently (the "only one active command per container" DB constraint is
        Cloud's job, not this handler's).'''
        self.container_maker_client.save_container.return_value = _saved_pod_response()
        self.cloud_client.get_save_status.return_value = {"status": "Succeeded", "image_reference": _REAL_IMAGE}

        first = await self.handler(_execute_command())
        second = await self.handler(_execute_command())

        self.assertIsNone(first[1])
        self.assertIsNone(second[1])
        self.assertEqual(self.container_maker_client.save_container.await_count, 2)
        self.container_maker_client.delete_container.assert_not_awaited()
