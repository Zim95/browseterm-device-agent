'''
Device Agent's private, ClusterIP-only local API (migration Part 12) - the ONLY thing
status_monitor, reaper, snapshot_job, socket-ssh and (as of Part 12's completion) container-maker
are allowed to talk to locally now. None of them hold any Cloud credential any more; NetworkPolicy
(infra/deployment.yaml), not authentication, is what restricts who can reach this service.

ReportContainerStatus/ReportSnapshotProgress/ReportTunnel relay over the already-authenticated
Device Control stream (fire-and-forget from this RPC's perspective - Ack just means "queued for
send", not "Cloud received it", same accepted tradeoff as ConnectionManager.send_command_result).
RequestHibernate/ConsumeTerminalTicket/GetTunnelGeneration and the 7 Part-12-completion RPCs below
need a synchronous answer their callers can't get from the async stream, so they go through
CloudClient's device-scoped HTTP calls instead. Unlike those three, the 7 below raise
CloudClientError on failure rather than returning a safe default (see cloud_client.py's own
docstring for why) - this servicer catches it and aborts the gRPC call so the original caller sees
a real failure, matching what their old direct-to-Cloud CloudClientError already gave them.
'''
import json

import grpc
from device_control_spec import local_device_agent_pb2_grpc
from device_control_spec.local_device_agent_pb2 import (
    Ack, CommandReference, ContainerIdList, GetContainerResponse, GetTunnelGenerationResponse,
    SnapshotAllocation, TerminalTarget,
)

from device_agent.clients.cloud_client import CloudClient, CloudClientError
from device_agent.control.grpc_client import ConnectionManager
from device_agent.state.placement_cache import PlacementCache
from device_agent.observability.logging_setup import get_logger

logger = get_logger("local_api")


async def _abort_on_cloud_error(context, e: CloudClientError, rpc_name: str) -> None:
    logger.error(f"local_api.{rpc_name}.cloud_error", extra={"error": e.message})
    await context.abort(grpc.StatusCode.UNAVAILABLE, e.message)


class LocalDeviceAgentServicer(local_device_agent_pb2_grpc.LocalDeviceAgentServicer):
    def __init__(self, connection_manager: ConnectionManager, cloud_client: CloudClient, placement_cache: PlacementCache = None) -> None:
        self.connection_manager = connection_manager
        self.cloud_client = cloud_client
        self.placement_cache = placement_cache

    async def ReportContainerStatus(self, request, context) -> Ack:
        '''status_monitor (a cluster-wide pod watcher) cannot know a container's real
        placement_generation - nothing about a pod carries it (see placement_cache.py's own
        docstring). Prefer this Device Agent's own record of the generation it placed the
        container under; only fall back to the caller-supplied value (which is 0 from
        status_monitor today) when this process has no record yet - that report then safely
        no-ops server-side (conditional_container_update) rather than being trusted blindly.'''
        placement_generation = request.placement_generation
        if self.placement_cache is not None:
            cached = self.placement_cache.get(request.container_id)
            if cached is not None:
                _, placement_generation = cached
            else:
                logger.warning(
                    "local_api.report_container_status.no_placement_cache_entry",
                    extra={"container_id": request.container_id},
                )
        payload = {
            "container_id": request.container_id, "placement_generation": placement_generation,
            "observed_status": request.observed_status, "kubernetes_id": request.kubernetes_id or None,
        }
        await self.connection_manager.send_local_event("container_status_report", json.dumps(payload))
        return Ack(ok=True)

    async def ReportSnapshotProgress(self, request, context) -> Ack:
        if request.command_id:
            await self.connection_manager.send_command_progress(request.command_id, request.stage, request.message)
        return Ack(ok=True)

    async def RequestHibernate(self, request, context) -> CommandReference:
        '''reason="pod_lost" (status_monitor's pod_watcher, for a crashed or externally-removed
        pod) requests a skip_save hibernate - the pod is already gone/unusable, so there is
        nothing to snapshot; every other reason ("idle_timeout"/"manual", Reaper's own callers)
        keeps the default save-then-delete behavior.'''
        result = await self.cloud_client.request_hibernate(
            request.container_id, skip_save=(request.reason == "pod_lost"),
        )
        return CommandReference(
            created=result["created"], command_id=result.get("command_id") or "", error=result.get("error") or "",
        )

    async def ConsumeTerminalTicket(self, request, context) -> TerminalTarget:
        target = await self.cloud_client.consume_terminal_ticket(request.ticket)
        if not target:
            return TerminalTarget(valid=False)
        return TerminalTarget(
            valid=True, container_id=target.get("container_id", ""), ssh_host=target.get("ssh_host") or "",
            ssh_port=target.get("ssh_port") or 0, ssh_username=target.get("ssh_username") or "",
            ssh_password=target.get("ssh_password") or "",
        )

    async def ReportTunnel(self, request, context) -> Ack:
        await self.connection_manager.send_terminal_tunnel_registration(
            request.provider, request.public_url, request.generation, request.status,
        )
        return Ack(ok=True)

    async def GetTunnelGeneration(self, request, context) -> GetTunnelGenerationResponse:
        '''Synchronous, unlike ReportTunnel above (fire-and-forget over the control stream) -
        Tunnel Registrar needs an actual answer to resync against, not just an Ack, so this goes
        through cloud_client's device-scoped HTTP path instead (same reasoning as
        RequestHibernate/ConsumeTerminalTicket). On a failure to reach Cloud, returns 0 rather
        than raising - a caller resyncing as max(local, 0) is a safe no-op, never a regression of
        its own already-correct local value.'''
        generation = await self.cloud_client.get_tunnel_generation()
        return GetTunnelGenerationResponse(generation=generation if generation is not None else 0)

    # The 7 RPCs below finish Part 12 - see this module's own docstring for why they abort on
    # CloudClientError instead of returning a safe default like GetTunnelGeneration above.

    async def GetActiveContainerIds(self, request, context) -> ContainerIdList:
        try:
            container_ids = await self.cloud_client.get_active_container_ids()
        except CloudClientError as e:
            await _abort_on_cloud_error(context, e, "get_active_container_ids")
            return
        return ContainerIdList(container_ids=container_ids)

    async def ReconcileDeviceResources(self, request, context) -> Ack:
        try:
            await self.cloud_client.reconcile_device_resources(
                list(request.running_container_ids), dict(request.running_pod_ips),
            )
        except CloudClientError as e:
            await _abort_on_cloud_error(context, e, "reconcile_device_resources")
            return
        return Ack(ok=True)

    async def GetIdleContainers(self, request, context) -> ContainerIdList:
        try:
            container_ids = await self.cloud_client.get_idle_containers(request.idle_threshold_seconds)
        except CloudClientError as e:
            await _abort_on_cloud_error(context, e, "get_idle_containers")
            return
        return ContainerIdList(container_ids=container_ids)

    async def AllocateSnapshot(self, request, context) -> SnapshotAllocation:
        try:
            snapshot = await self.cloud_client.allocate_snapshot(request.container_id, request.request_id)
        except CloudClientError as e:
            await _abort_on_cloud_error(context, e, "allocate_snapshot")
            return
        return SnapshotAllocation(
            id=snapshot["id"], version_sequence=snapshot["version_sequence"], version=snapshot["version"],
            image_repository=snapshot["image_repository"], status=snapshot["status"],
        )

    async def ReportSnapshotResult(self, request, context) -> Ack:
        try:
            await self.cloud_client.report_snapshot_result(
                request.container_id, request.snapshot_id, request.status,
                image_reference=request.image_reference or None,
                registry_digest=request.registry_digest or None,
                error_detail=request.error_detail or None,
            )
        except CloudClientError as e:
            await _abort_on_cloud_error(context, e, "report_snapshot_result")
            return
        return Ack(ok=True)

    async def GetContainer(self, request, context) -> GetContainerResponse:
        try:
            container = await self.cloud_client.get_container(request.container_id)
        except CloudClientError as e:
            await _abort_on_cloud_error(context, e, "get_container")
            return
        if container is None:
            return GetContainerResponse(found=False)
        return GetContainerResponse(found=True, container_json=json.dumps(container))

    async def UpdateContainerKubernetesId(self, request, context) -> Ack:
        try:
            await self.cloud_client.update_container_kubernetes_id(request.container_id, request.kubernetes_id)
        except CloudClientError as e:
            await _abort_on_cloud_error(context, e, "update_container_kubernetes_id")
            return
        return Ack(ok=True)
