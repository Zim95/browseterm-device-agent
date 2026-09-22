from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock

from device_agent.clients.cloud_client import CloudClient


class TestCloudClient(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.client = CloudClient(device_id="d1", device_token="tok")
        self.client._client = AsyncMock()

    async def test_request_hibernate_success(self) -> None:
        response = MagicMock(status_code=202)
        response.json.return_value = {"command": {"id": "cmd-1"}}
        self.client._client.post.return_value = response

        result = await self.client.request_hibernate("c1")

        self.assertTrue(result["created"])
        self.assertEqual(result["command_id"], "cmd-1")
        self.client._client.post.assert_awaited_once_with("/devices/d1/containers/c1/hibernate-request")

    async def test_request_hibernate_failure(self) -> None:
        response = MagicMock(status_code=409)
        response.json.return_value = {"error": "Only a running terminal can be hibernated"}
        self.client._client.post.return_value = response

        result = await self.client.request_hibernate("c1")

        self.assertFalse(result["created"])
        self.assertIn("running terminal", result["error"])

    async def test_consume_terminal_ticket_valid(self) -> None:
        response = MagicMock(status_code=200)
        response.json.return_value = {"container_id": "c1", "ssh_host": "10.0.0.5", "ssh_port": 22}
        self.client._client.post.return_value = response

        result = await self.client.consume_terminal_ticket("t1")

        self.assertEqual(result["ssh_host"], "10.0.0.5")
        self.client._client.post.assert_awaited_once_with("/internal/terminal-tickets/consume", json={"ticket": "t1"})

    async def test_consume_terminal_ticket_invalid_returns_none(self) -> None:
        response = MagicMock(status_code=401)
        self.client._client.post.return_value = response

        result = await self.client.consume_terminal_ticket("bad")

        self.assertIsNone(result)

    async def test_get_save_status_success(self) -> None:
        response = MagicMock(status_code=200)
        response.json.return_value = {"status": "Succeeded", "image_reference": "registry.example.com/img:1", "error_detail": None}
        self.client._client.get.return_value = response

        result = await self.client.get_save_status("c1", "req-1")

        self.assertEqual(result["status"], "Succeeded")
        self.assertEqual(result["image_reference"], "registry.example.com/img:1")
        self.client._client.get.assert_awaited_once_with(
            "/devices/d1/containers/c1/save-status", params={"request_id": "req-1"},
        )

    async def test_get_save_status_not_found_returns_none_status(self) -> None:
        response = MagicMock(status_code=404)
        self.client._client.get.return_value = response

        result = await self.client.get_save_status("c1", "req-1")

        self.assertIsNone(result["status"])

    async def test_get_save_status_network_error_returns_none_status(self) -> None:
        import httpx
        self.client._client.get.side_effect = httpx.ConnectError("connection refused")

        result = await self.client.get_save_status("c1", "req-1")

        self.assertIsNone(result["status"])
