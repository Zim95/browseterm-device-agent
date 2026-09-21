import json
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

from device_agent.commands.create import make_handler
from device_agent.clients.container_maker_client import ContainerMakerClientError
from device_control_spec.device_control_types_pb2 import ExecuteCommand


def _valid_config(**overrides) -> str:
    cfg = {
        "image_name": "browseterm/base:latest", "container_name": "my-terminal",
        "network_name": "user1-namespace", "cpu_request": "500m", "cpu_limit": "1",
        "memory_request": "512Mi", "memory_limit": "1Gi", "ephemeral_request": "1Gi", "ephemeral_limit": "2Gi",
    }
    cfg.update(overrides)
    return json.dumps(cfg)


class TestCreateHandler(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.container_maker_client = AsyncMock()
        self.handler = make_handler(self.container_maker_client, cloud_client=None)

    async def test_happy_path_strips_pod_suffix_and_returns_kubernetes_id(self) -> None:
        self.container_maker_client.create_container.return_value = SimpleNamespace(
            container_id="pod-abc123", container_name="my-terminal-pod-1706565890", ip_address="10.0.0.5",
        )
        execute_command = ExecuteCommand(command_id="cmd-1", container_config_json=_valid_config())
        result, error_code, error_message = await self.handler(execute_command)

        self.assertIsNone(error_code)
        self.assertEqual(result["kubernetes_id"], "pod-abc123")
        self.assertEqual(result["container_name"], "my-terminal")

    async def test_service_suffix_without_timestamp_is_stripped(self) -> None:
        self.container_maker_client.create_container.return_value = SimpleNamespace(
            container_id="pod-abc123", container_name="my-terminal-service", ip_address=None,
        )
        execute_command = ExecuteCommand(command_id="cmd-1", container_config_json=_valid_config())
        result, _, _ = await self.handler(execute_command)
        self.assertEqual(result["container_name"], "my-terminal")

    async def test_missing_config_json_is_a_clean_failure(self) -> None:
        execute_command = ExecuteCommand(command_id="cmd-1", container_config_json="")
        result, error_code, error_message = await self.handler(execute_command)
        self.assertIsNone(result)
        self.assertEqual(error_code, "INVALID_CONFIG")

    async def test_missing_required_field_is_a_clean_failure(self) -> None:
        execute_command = ExecuteCommand(command_id="cmd-1", container_config_json=json.dumps({"image_name": "x"}))
        result, error_code, error_message = await self.handler(execute_command)
        self.assertIsNone(result)
        self.assertEqual(error_code, "INVALID_CONFIG")
        self.assertIn("container_name", error_message)

    async def test_container_maker_failure_is_reported_not_raised(self) -> None:
        self.container_maker_client.create_container.side_effect = ContainerMakerClientError("node out of capacity")
        execute_command = ExecuteCommand(command_id="cmd-1", container_config_json=_valid_config())
        result, error_code, error_message = await self.handler(execute_command)
        self.assertIsNone(result)
        self.assertEqual(error_code, "CONTAINER_MAKER_ERROR")
        self.assertIn("node out of capacity", error_message)
