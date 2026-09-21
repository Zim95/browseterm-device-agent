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
from device_agent.observability.logging_setup import get_logger

logger = get_logger("local_api")


class LocalDeviceAgentServicer(local_device_agent_pb2_grpc.LocalDeviceAgentServicer):
    def __init__(self, connection_manager: ConnectionManager, cloud_client: CloudClient) -> None:
        self.connection_manager = connection_manager
        self.cloud_client = cloud_client

    async def ReportContainerStatus(self, request, context) -> Ack:
        payload = {
            "container_id": request.container_id, "placement_generation": request.placement_generation,
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
