import json
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

from device_agent.commands.resume import make_handler
from device_agent.clients.container_maker_client import ContainerMakerClientError
from device_control_spec.device_control_types_pb2 import ExecuteCommand


def _valid_config(**overrides) -> str:
    cfg = {
        "saved_image": "registry.example.com/browseterm/user1/container1/img:1", "container_name": "my-terminal",
        "network_name": "user1-namespace", "cpu_request": "500m", "cpu_limit": "1",
        "memory_request": "512Mi", "memory_limit": "1Gi", "ephemeral_request": "1Gi", "ephemeral_limit": "2Gi",
    }
    cfg.update(overrides)
    return json.dumps(cfg)


class TestResumeHandler(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.container_maker_client = AsyncMock()
        self.handler = make_handler(self.container_maker_client, cloud_client=None)

    async def test_resume_on_same_device_uses_saved_image(self) -> None:
        self.container_maker_client.create_container.return_value = SimpleNamespace(
            container_id="pod-abc123", container_name="my-terminal-pod-1706565890", container_ip="10.0.0.9",
        )
        execute_command = ExecuteCommand(command_id="cmd-1", container_config_json=_valid_config())
        result, error_code, error_message = await self.handler(execute_command)

        self.assertIsNone(error_code)
        self.assertEqual(result["kubernetes_id"], "pod-abc123")
        _, kwargs = self.container_maker_client.create_container.call_args
        self.assertEqual(kwargs["image_name"], "registry.example.com/browseterm/user1/container1/img:1")

    async def test_resume_reports_the_real_pod_ip(self) -> None:
        '''Regression test - see create.py's matching test for the production bug this guards
        against (container-maker-spec's response field is container_ip, not ip_address).'''
        self.container_maker_client.create_container.return_value = SimpleNamespace(
            container_id="pod-abc123", container_name="my-terminal-pod-1706565890", container_ip="10.0.0.9",
        )
        execute_command = ExecuteCommand(command_id="cmd-1", container_config_json=_valid_config())
        result, _, _ = await self.handler(execute_command)
        self.assertEqual(result["ip_address"], "10.0.0.9")

    async def test_missing_saved_image_is_a_clean_failure(self) -> None:
        '''Doc-required: "Missing/corrupt image."'''
        execute_command = ExecuteCommand(command_id="cmd-1", container_config_json=_valid_config(saved_image=""))
        result, error_code, error_message = await self.handler(execute_command)
        self.assertIsNone(result)
        self.assertEqual(error_code, "MISSING_SAVED_IMAGE")
        self.container_maker_client.create_container.assert_not_awaited()

    async def test_missing_config_json_is_a_clean_failure(self) -> None:
        execute_command = ExecuteCommand(command_id="cmd-1", container_config_json="")
        result, error_code, error_message = await self.handler(execute_command)
        self.assertEqual(error_code, "INVALID_CONFIG")

    async def test_container_maker_failure_is_reported_not_raised(self) -> None:
        self.container_maker_client.create_container.side_effect = ContainerMakerClientError("image pull backoff")
        execute_command = ExecuteCommand(command_id="cmd-1", container_config_json=_valid_config())
        result, error_code, error_message = await self.handler(execute_command)
        self.assertIsNone(result)
        self.assertEqual(error_code, "CONTAINER_MAKER_ERROR")
