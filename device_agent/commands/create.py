'''
CREATE command handler (Part 8). Ported from browseterm-server-local's
containers_service.py::create_container_in_k8s - same container-maker request shape, same
container-name-suffix-stripping cleanup - adapted to read from ExecuteCommand.container_config_json
instead of a FastAPI request body, and to return a (result, error_code, error_message) tuple
instead of raising HTTPException.

Expected container_config_json shape (built by Cloud):
{
  "image_name": str, "container_name": str, "network_name": str, "exposure_level": int,
  "publish_information": [{"publish_port": int, "target_port": int, "protocol": str, "node_port": int|null}, ...],
  "environment_variables": {str: str},
  "cpu_request": str, "cpu_limit": str, "memory_request": str, "memory_limit": str,
  "ephemeral_request": str, "ephemeral_limit": str, "snapshot_size_limit": str|null
}
'''
import json

from device_agent.clients.container_maker_client import ContainerMakerClient, ContainerMakerClientError
from device_agent.commands._util import strip_container_maker_suffix
from device_agent.observability.logging_setup import get_logger

logger = get_logger("command.create")


def make_handler(container_maker_client: ContainerMakerClient, cloud_client):
    async def handle(execute_command) -> tuple:
        try:
            cfg = json.loads(execute_command.container_config_json)
        except (json.JSONDecodeError, ValueError):
            return None, "INVALID_CONFIG", "container_config_json was missing or not valid JSON"

        required = ["image_name", "container_name", "network_name", "cpu_request", "cpu_limit",
                    "memory_request", "memory_limit", "ephemeral_request", "ephemeral_limit"]
        missing = [k for k in required if k not in cfg]
        if missing:
            return None, "INVALID_CONFIG", f"container_config_json missing required fields: {missing}"

        try:
            response = await container_maker_client.create_container(
                image_name=cfg["image_name"], container_name=cfg["container_name"],
                network_name=cfg["network_name"], exposure_level=cfg.get("exposure_level", 0),
                publish_information=cfg.get("publish_information", []),
                environment_variables=cfg.get("environment_variables", {}),
                cpu_request=cfg["cpu_request"], cpu_limit=cfg["cpu_limit"],
                memory_request=cfg["memory_request"], memory_limit=cfg["memory_limit"],
                ephemeral_request=cfg["ephemeral_request"], ephemeral_limit=cfg["ephemeral_limit"],
                snapshot_size_limit=cfg.get("snapshot_size_limit"),
                request_id=execute_command.trace_id,
            )
        except ContainerMakerClientError as e:
            logger.error("command.create.container_maker_failed", extra={"command_id": execute_command.command_id, "error": str(e)})
            return None, "CONTAINER_MAKER_ERROR", str(e)[:1000]

        container_name = strip_container_maker_suffix(response.container_name)
        return {
            "kubernetes_id": response.container_id, "container_name": container_name,
            "ip_address": getattr(response, "ip_address", None),
            "associated_resources": {"network_name": cfg["network_name"]},
        }, None, None

    return handle
