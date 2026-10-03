import json
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

from device_agent.clients.cloud_client import CloudClientError
from device_agent.local_api.service import LocalDeviceAgentServicer
from device_agent.state.placement_cache import PlacementCache
from device_control_spec.local_device_agent_pb2 import (
    AllocateSnapshotRequest, Empty, GetContainerRequest, GetIdleContainersRequest, GetTunnelGenerationRequest,
    ReconcileResourcesRequest, SnapshotResultReport, StatusReport, SnapshotProgress, HibernateRequest,
    TerminalTicketRequest, TunnelReport, UpdateKubernetesIdRequest,
)


class _AbortCalled(Exception):
    """Raised by the mock context.abort() below, mirroring grpc.aio's own real behavior that
    abort() never returns - it always raises to stop the RPC handler."""


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

    async def test_report_container_status_without_placement_cache_uses_caller_value(self) -> None:
        '''No placement_cache wired (defaults to None) - existing/simple callers must keep
        working exactly as before this change.'''
        await self.servicer.ReportContainerStatus(
            StatusReport(container_id="c1", device_id="d1", placement_generation=5, observed_status="Running"),
            context=None,
        )
        payload = json.loads(self.connection_manager.send_local_event.call_args[0][1])
        self.assertEqual(payload["placement_generation"], 5)

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
        self.cloud_client.request_hibernate.assert_awaited_once_with("c1", skip_save=False)

    async def test_request_hibernate_pod_lost_requests_skip_save(self) -> None:
        '''status_monitor's pod_watcher, for a crashed/externally-removed pod - nothing left to
        snapshot, so this must request the same skip_save hibernate the browser's manual
        hibernate route uses, not the default save-then-delete.'''
        self.cloud_client.request_hibernate.return_value = {"created": True, "command_id": "cmd-1", "error": None}
        ref = await self.servicer.RequestHibernate(HibernateRequest(container_id="c1", reason="pod_lost"), context=None)
        self.assertTrue(ref.created)
        self.cloud_client.request_hibernate.assert_awaited_once_with("c1", skip_save=True)

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

    async def test_get_tunnel_generation_returns_clouds_value(self) -> None:
        self.cloud_client.get_tunnel_generation.return_value = 35
        response = await self.servicer.GetTunnelGeneration(GetTunnelGenerationRequest(), context=None)
        self.assertEqual(response.generation, 35)

    async def test_get_tunnel_generation_falls_back_to_zero_on_cloud_failure(self) -> None:
        '''Never raises, never returns None to the caller (protobuf int32 can't carry that
        distinction anyway) - a caller resyncing as max(local, 0) is a safe no-op.'''
        self.cloud_client.get_tunnel_generation.return_value = None
        response = await self.servicer.GetTunnelGeneration(GetTunnelGenerationRequest(), context=None)
        self.assertEqual(response.generation, 0)

    # Part 12 completion - unlike GetTunnelGeneration above, these abort the gRPC call on
    # CloudClientError rather than returning a safe default (see service.py's own docstring).

    def _abort_context(self) -> AsyncMock:
        context = AsyncMock()
        context.abort.side_effect = _AbortCalled()
        return context

    async def test_get_active_container_ids_success(self) -> None:
        self.cloud_client.get_active_container_ids.return_value = ["c1", "c2"]
        response = await self.servicer.GetActiveContainerIds(Empty(), context=None)
        self.assertEqual(list(response.container_ids), ["c1", "c2"])

    async def test_get_active_container_ids_aborts_on_cloud_error(self) -> None:
        self.cloud_client.get_active_container_ids.side_effect = CloudClientError("down")
        context = self._abort_context()
        with self.assertRaises(_AbortCalled):
            await self.servicer.GetActiveContainerIds(Empty(), context=context)
        context.abort.assert_awaited_once()

    async def test_reconcile_device_resources_forwards_lists_and_map(self) -> None:
        ack = await self.servicer.ReconcileDeviceResources(
            ReconcileResourcesRequest(running_container_ids=["c1"], running_pod_ips={"c1": "10.0.0.1"}),
            context=None,
        )
        self.assertTrue(ack.ok)
        self.cloud_client.reconcile_device_resources.assert_awaited_once_with(["c1"], {"c1": "10.0.0.1"})

    async def test_reconcile_device_resources_aborts_on_cloud_error(self) -> None:
        self.cloud_client.reconcile_device_resources.side_effect = CloudClientError("down")
        context = self._abort_context()
        with self.assertRaises(_AbortCalled):
            await self.servicer.ReconcileDeviceResources(ReconcileResourcesRequest(), context=context)

    async def test_get_idle_containers_success(self) -> None:
        self.cloud_client.get_idle_containers.return_value = ["c1"]
        response = await self.servicer.GetIdleContainers(GetIdleContainersRequest(idle_threshold_seconds=1800), context=None)
        self.assertEqual(list(response.container_ids), ["c1"])
        self.cloud_client.get_idle_containers.assert_awaited_once_with(1800)

    async def test_get_idle_containers_aborts_on_cloud_error(self) -> None:
        self.cloud_client.get_idle_containers.side_effect = CloudClientError("down")
        context = self._abort_context()
        with self.assertRaises(_AbortCalled):
            await self.servicer.GetIdleContainers(GetIdleContainersRequest(idle_threshold_seconds=1800), context=context)

    async def test_allocate_snapshot_success(self) -> None:
        self.cloud_client.allocate_snapshot.return_value = {
            "id": "s1", "version_sequence": 1, "version": "v1", "image_repository": "r", "status": "Pending",
        }
        response = await self.servicer.AllocateSnapshot(
            AllocateSnapshotRequest(container_id="c1", request_id="req-1"), context=None,
        )
        self.assertEqual(response.id, "s1")
        self.assertEqual(response.status, "Pending")
        self.cloud_client.allocate_snapshot.assert_awaited_once_with("c1", "req-1")

    async def test_allocate_snapshot_aborts_on_cloud_error(self) -> None:
        self.cloud_client.allocate_snapshot.side_effect = CloudClientError("down")
        context = self._abort_context()
        with self.assertRaises(_AbortCalled):
            await self.servicer.AllocateSnapshot(AllocateSnapshotRequest(container_id="c1", request_id="req-1"), context=context)

    async def test_report_snapshot_result_forwards_optional_fields(self) -> None:
        ack = await self.servicer.ReportSnapshotResult(
            SnapshotResultReport(container_id="c1", snapshot_id="s1", status="Succeeded", image_reference="img:1"),
            context=None,
        )
        self.assertTrue(ack.ok)
        self.cloud_client.report_snapshot_result.assert_awaited_once_with(
            "c1", "s1", "Succeeded", image_reference="img:1", registry_digest=None, error_detail=None,
        )

    async def test_report_snapshot_result_aborts_on_cloud_error(self) -> None:
        self.cloud_client.report_snapshot_result.side_effect = CloudClientError("down")
        context = self._abort_context()
        with self.assertRaises(_AbortCalled):
            await self.servicer.ReportSnapshotResult(
                SnapshotResultReport(container_id="c1", snapshot_id="s1", status="Failed"), context=context,
            )

    async def test_get_container_found(self) -> None:
        self.cloud_client.get_container.return_value = {"id": "c1", "name": "my-ws"}
        response = await self.servicer.GetContainer(GetContainerRequest(container_id="c1"), context=None)
        self.assertTrue(response.found)
        self.assertEqual(json.loads(response.container_json)["id"], "c1")

    async def test_get_container_not_found(self) -> None:
        self.cloud_client.get_container.return_value = None
        response = await self.servicer.GetContainer(GetContainerRequest(container_id="c1"), context=None)
        self.assertFalse(response.found)

    async def test_get_container_aborts_on_cloud_error(self) -> None:
        self.cloud_client.get_container.side_effect = CloudClientError("down")
        context = self._abort_context()
        with self.assertRaises(_AbortCalled):
            await self.servicer.GetContainer(GetContainerRequest(container_id="c1"), context=context)

    async def test_update_container_kubernetes_id_success(self) -> None:
        ack = await self.servicer.UpdateContainerKubernetesId(
            UpdateKubernetesIdRequest(container_id="c1", kubernetes_id="new-pod-uid"), context=None,
        )
        self.assertTrue(ack.ok)
        self.cloud_client.update_container_kubernetes_id.assert_awaited_once_with("c1", "new-pod-uid")

    async def test_update_container_kubernetes_id_aborts_on_cloud_error(self) -> None:
        self.cloud_client.update_container_kubernetes_id.side_effect = CloudClientError("down")
        context = self._abort_context()
        with self.assertRaises(_AbortCalled):
            await self.servicer.UpdateContainerKubernetesId(
                UpdateKubernetesIdRequest(container_id="c1", kubernetes_id="x"), context=context,
            )


class TestLocalDeviceAgentServicerPlacementCache(IsolatedAsyncioTestCase):
    '''migration Part 12 gap-fill: status_monitor (a pod watcher) cannot know a container's real
    placement_generation - the servicer must prefer its own cached record over the caller's
    (unreliable) guess. See device_agent/state/placement_cache.py's own docstring.'''

    def setUp(self) -> None:
        self.connection_manager = AsyncMock()
        self.cloud_client = AsyncMock()
        self.placement_cache = PlacementCache()
        self.servicer = LocalDeviceAgentServicer(self.connection_manager, self.cloud_client, self.placement_cache)

    async def test_uses_cached_placement_generation_when_present(self) -> None:
        self.placement_cache.record("c1", "d1", 7)

        await self.servicer.ReportContainerStatus(
            # caller sends a stale/unknown 0 - the cache's 7 must win.
            StatusReport(container_id="c1", device_id="d1", placement_generation=0, observed_status="Running"),
            context=None,
        )

        payload = json.loads(self.connection_manager.send_local_event.call_args[0][1])
        self.assertEqual(payload["placement_generation"], 7)

    async def test_falls_back_to_caller_value_when_cache_has_no_entry(self) -> None:
        '''No CREATE/RESUME has run in this process for this container yet (e.g. fresh restart) -
        forward what the caller sent; Cloud's own conditional update safely no-ops if it's wrong,
        rather than this RPC failing outright.'''
        await self.servicer.ReportContainerStatus(
            StatusReport(container_id="unknown-container", device_id="d1", placement_generation=0, observed_status="Running"),
            context=None,
        )

        payload = json.loads(self.connection_manager.send_local_event.call_args[0][1])
        self.assertEqual(payload["placement_generation"], 0)
