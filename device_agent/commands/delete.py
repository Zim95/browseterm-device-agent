'''
DELETE command handler (Part 9). "Missing pod/service is success" - idempotent delete.

Known limitation: container-maker's own deleteContainer re-raises any Kubernetes ApiException
(including a plain 404-on-already-gone) as a generic exception, with no distinct gRPC status
code for "not found" vs. a real failure (see container-maker/src/grpc/servicer.py). Detecting
"already gone" here is therefore a message-text heuristic, not a status-code check - flagged as a
known gap; a clean fix would have container-maker itself surface grpc.StatusCode.NOT_FOUND for a
missing pod, which is a container-maker change, not made here (out of scope for this migration
part, which explicitly keeps Container Maker "private and focused on Kubernetes operations" -
not touched unless the owner asks for it).

Quota release happens server-side (Cloud's DeviceCommandOps.release_quota_for_command, wired from
CommandResult handling - Part 6) - this handler has nothing to do with quota accounting itself.
'''
import json

from device_agent.clients.container_maker_client import ContainerMakerClient, ContainerMakerClientError
from device_agent.observability.logging_setup import get_logger

logger = get_logger("command.delete")

_ALREADY_GONE_MARKERS = ("404", "not found", "notfound")


def make_handler(container_maker_client: ContainerMakerClient, cloud_client):
    async def handle(execute_command) -> tuple:
        try:
            cfg = json.loads(execute_command.container_config_json) if execute_command.container_config_json else {}
        except (json.JSONDecodeError, ValueError):
            cfg = {}
        network_name = cfg.get("network_name", "")

        try:
            response = await container_maker_client.delete_container(
                container_id=execute_command.container_id, network_name=network_name,
                request_id=execute_command.trace_id,
            )
            return {"container_id": response.container_id, "status": response.status}, None, None
        except ContainerMakerClientError as e:
            message = str(e).lower()
            if any(marker in message for marker in _ALREADY_GONE_MARKERS):
                logger.info("command.delete.already_gone_treated_as_success", extra={"command_id": execute_command.command_id})
                return {"container_id": execute_command.container_id, "status": "already_deleted"}, None, None
            logger.error("command.delete.container_maker_failed", extra={"command_id": execute_command.command_id, "error": str(e)})
            return None, "CONTAINER_MAKER_ERROR", str(e)[:1000]

    return handle
