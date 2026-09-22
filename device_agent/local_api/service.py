'''
Device Agent's private, ClusterIP-only local API (migration Part 12) - the ONLY thing
status_monitor, reaper, snapshot_job and socket-ssh are allowed to talk to locally now. None of
them hold any Cloud credential any more; NetworkPolicy (infra/deployment.yaml), not
authentication, is what restricts who can reach this service.

ReportContainerStatus/ReportSnapshotProgress/ReportTunnel relay over the already-authenticated
Device Control stream (fire-and-forget from this RPC's perspective - Ack just means "queued for
send", not "Cloud received it", same accepted tradeoff as ConnectionManager.send_command_result).
RequestHibernate/ConsumeTerminalTicket need a synchronous answer their callers can't get from the
async stream, so they go through CloudClient's device-scoped HTTP calls instead.
'''
import json

from device_control_spec import local_device_agent_pb2_grpc
from device_control_spec.local_device_agent_pb2 import Ack, CommandReference, TerminalTarget

from device_agent.clients.cloud_client import CloudClient
from device_agent.control.grpc_client import ConnectionManager
from device_agent.state.placement_cache import PlacementCache
from device_agent.observability.logging_setup import get_logger

logger = get_logger("local_api")


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
        result = await self.cloud_client.request_hibernate(request.container_id)
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
