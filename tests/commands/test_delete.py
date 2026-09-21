import json
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

from device_agent.commands.delete import make_handler
from device_agent.clients.container_maker_client import ContainerMakerClientError
from device_control_spec.device_control_types_pb2 import ExecuteCommand


class TestDeleteHandler(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.container_maker_client = AsyncMock()
        self.handler = make_handler(self.container_maker_client, cloud_client=None)

    async def test_happy_path_deletes_existing_workload(self) -> None:
        self.container_maker_client.delete_container.return_value = SimpleNamespace(container_id="pod-1", status="Deleted")
        execute_command = ExecuteCommand(
            command_id="cmd-1", container_id="container-1",
            container_config_json=json.dumps({"network_name": "user1-namespace"}),
        )
        result, error_code, error_message = await self.handler(execute_command)
        self.assertIsNone(error_code)
        self.assertEqual(result["status"], "Deleted")

    async def test_already_missing_workload_is_treated_as_success(self) -> None:
        '''Doc-required: "Missing pod/service is success."'''
        self.container_maker_client.delete_container.side_effect = ContainerMakerClientError("pods \"my-pod\" not found: 404")
        execute_command = ExecuteCommand(
            command_id="cmd-1", container_id="container-1",
            container_config_json=json.dumps({"network_name": "user1-namespace"}),
        )
        result, error_code, error_message = await self.handler(execute_command)
        self.assertIsNone(error_code)
        self.assertEqual(result["status"], "already_deleted")

    async def test_real_failure_is_reported_as_failed(self) -> None:
        self.container_maker_client.delete_container.side_effect = ContainerMakerClientError("connection refused")
        execute_command = ExecuteCommand(
            command_id="cmd-1", container_id="container-1",
            container_config_json=json.dumps({"network_name": "user1-namespace"}),
        )
        result, error_code, error_message = await self.handler(execute_command)
        self.assertIsNone(result)
        self.assertEqual(error_code, "CONTAINER_MAKER_ERROR")

    async def test_missing_config_json_does_not_raise(self) -> None:
        execute_command = ExecuteCommand(command_id="cmd-1", container_id="container-1", container_config_json="")
        self.container_maker_client.delete_container.return_value = SimpleNamespace(container_id="pod-1", status="Deleted")
        result, error_code, error_message = await self.handler(execute_command)
        self.assertIsNone(error_code)
