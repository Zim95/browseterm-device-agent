import json
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

from device_agent.local_api.service import LocalDeviceAgentServicer
from device_control_spec.local_device_agent_pb2 import (
    StatusReport, SnapshotProgress, HibernateRequest, TerminalTicketRequest, TunnelReport,
)


class TestLocalDeviceAgentServicer(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.connection_manager = AsyncMock()
        self.cloud_client = AsyncMock()
        self.servicer = LocalDeviceAgentServicer(self.connection_manager, self.cloud_client)

    async def test_report_container_status_forwards_as_local_event(self) -> None:
        ack = await self.servicer.ReportContainerStatus(
            StatusReport(container_id="c1", device_id="d1", placement_generation=2, observed_status="Running", kubernetes_id="pod-1"),
            context=None,
        )
        self.assertTrue(ack.ok)
        self.connection_manager.send_local_event.assert_awaited_once()
        event_type, payload_json = self.connection_manager.send_local_event.call_args[0]
        self.assertEqual(event_type, "container_status_report")
        payload = json.loads(payload_json)
        self.assertEqual(payload["container_id"], "c1")
        self.assertEqual(payload["kubernetes_id"], "pod-1")

    async def test_report_snapshot_progress_forwards_when_command_id_present(self) -> None:
        await self.servicer.ReportSnapshotProgress(
            SnapshotProgress(container_id="c1", command_id="cmd-1", stage="pushing_image", message="pushing to registry"),
            context=None,
        )
        self.connection_manager.send_command_progress.assert_awaited_once_with("cmd-1", "pushing_image", "pushing to registry")

    async def test_report_snapshot_progress_without_command_id_is_a_no_op(self) -> None:
        await self.servicer.ReportSnapshotProgress(SnapshotProgress(container_id="c1", stage="snapshotting"), context=None)
        self.connection_manager.send_command_progress.assert_not_awaited()

    async def test_request_hibernate_success(self) -> None:
        self.cloud_client.request_hibernate.return_value = {"created": True, "command_id": "cmd-1", "error": None}
        ref = await self.servicer.RequestHibernate(HibernateRequest(container_id="c1", reason="idle_timeout"), context=None)
        self.assertTrue(ref.created)
        self.assertEqual(ref.command_id, "cmd-1")

    async def test_request_hibernate_failure(self) -> None:
        self.cloud_client.request_hibernate.return_value = {"created": False, "command_id": None, "error": "already hibernating"}
        ref = await self.servicer.RequestHibernate(HibernateRequest(container_id="c1", reason="idle_timeout"), context=None)
        self.assertFalse(ref.created)
        self.assertEqual(ref.error, "already hibernating")

    async def test_consume_terminal_ticket_valid(self) -> None:
        self.cloud_client.consume_terminal_ticket.return_value = {
            "container_id": "c1", "ssh_host": "10.0.0.5", "ssh_port": 22, "ssh_username": "user", "ssh_password": "pass",
        }
        target = await self.servicer.ConsumeTerminalTicket(TerminalTicketRequest(ticket="t1"), context=None)
        self.assertTrue(target.valid)
        self.assertEqual(target.ssh_host, "10.0.0.5")
        self.assertEqual(target.ssh_port, 22)

    async def test_consume_terminal_ticket_invalid(self) -> None:
        self.cloud_client.consume_terminal_ticket.return_value = None
        target = await self.servicer.ConsumeTerminalTicket(TerminalTicketRequest(ticket="bad"), context=None)
        self.assertFalse(target.valid)

    async def test_report_tunnel_forwards(self) -> None:
        ack = await self.servicer.ReportTunnel(
            TunnelReport(provider="ngrok", public_url="https://x.example.com", generation=3, status="online"), context=None,
        )
        self.assertTrue(ack.ok)
        self.connection_manager.send_terminal_tunnel_registration.assert_awaited_once_with(
            "ngrok", "https://x.example.com", 3, "online",
        )
