from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock

import grpc

from device_agent.clients.container_maker_client import ContainerMakerClient, ContainerMakerClientError


class TestContainerMakerClient(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.client = ContainerMakerClient(host="localhost", port=50052, client_key=b"k", client_cert=b"c", ca_cert=b"ca")
        self.client._stub = MagicMock()

    def tearDown(self) -> None:
        self.client.close()

    async def test_create_container_builds_the_expected_request_shape(self) -> None:
        self.client._stub.createContainer.return_value = MagicMock(container_id="pod-1", container_name="my-terminal-pod-1")
        response = await self.client.create_container(
            image_name="img:latest", container_name="my-terminal", network_name="ns1", exposure_level=0,
            publish_information=[{"publish_port": 2222, "target_port": 22, "protocol": "TCP"}],
            environment_variables={"FOO": "bar"}, cpu_request="500m", cpu_limit="1",
            memory_request="512Mi", memory_limit="1Gi", ephemeral_request="1Gi", ephemeral_limit="2Gi",
            snapshot_size_limit=None, request_id="req-1",
        )
        self.assertEqual(response.container_id, "pod-1")
        call_args = self.client._stub.createContainer.call_args
        request = call_args[0][0]
        self.assertEqual(request.image_name, "img:latest")
        self.assertEqual(request.resource_requirements.cpu_limit, "1")
        self.assertEqual(request.publish_information[0].publish_port, 2222)
        self.assertEqual(call_args[1]["metadata"], (("x-request-id", "req-1"),))

    async def test_create_container_wraps_rpc_errors(self) -> None:
        error = grpc.RpcError()
        self.client._stub.createContainer.side_effect = error
        with self.assertRaises(ContainerMakerClientError):
            await self.client.create_container(
                image_name="img", container_name="c1", network_name="ns1", exposure_level=0,
                publish_information=[], environment_variables={}, cpu_request="1", cpu_limit="1",
                memory_request="1Gi", memory_limit="1Gi", ephemeral_request="1Gi", ephemeral_limit="1Gi",
                snapshot_size_limit=None,
            )

    async def test_delete_container_wraps_rpc_errors(self) -> None:
        self.client._stub.deleteContainer.side_effect = grpc.RpcError()
        with self.assertRaises(ContainerMakerClientError):
            await self.client.delete_container(container_id="c1", network_name="ns1")

    async def test_delete_container_happy_path(self) -> None:
        self.client._stub.deleteContainer.return_value = MagicMock(container_id="c1", status="Deleted")
        response = await self.client.delete_container(container_id="c1", network_name="ns1")
        self.assertEqual(response.status, "Deleted")

    async def test_save_container_happy_path(self) -> None:
        self.client._stub.saveContainer.return_value = MagicMock(saved_pods=[MagicMock(image_name="img:1")])
        response = await self.client.save_container(container_id="c1", network_name="ns1")
        self.assertEqual(response.saved_pods[0].image_name, "img:1")

    async def test_save_container_wraps_rpc_errors(self) -> None:
        self.client._stub.saveContainer.side_effect = grpc.RpcError()
        with self.assertRaises(ContainerMakerClientError):
            await self.client.save_container(container_id="c1", network_name="ns1")
