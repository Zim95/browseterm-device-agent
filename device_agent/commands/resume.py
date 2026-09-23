'''
RESUME command handler (Part 11 - as revised, see BROWSETERM_MIGRATION_PROGRESS.md's "RECHECK"
note): "Fetch the canonical container record and its saved_image name/reference... Pass that
saved image name into the existing Container Maker resume/create path exactly as the current
code does... Do not redesign the snapshot image reference format during the control-plane
migration." No digest-based immutability - saved_image is used exactly as stored, unchanged.

This is CREATE's same container-maker request shape, using cfg["saved_image"] as the image
instead of resolving a base image_id - mirrors browseterm-server-local's
create_container_in_k8s(request, image_name_override=saved_image), the resume-from-snapshot path
that function already supported.

Expected container_config_json shape (superset of create.py's, with saved_image required):
{
  "saved_image": str, "container_name": str, "network_name": str, "exposure_level": int,
  "publish_information": [...], "environment_variables": {...},
  "cpu_request": str, "cpu_limit": str, "memory_request": str, "memory_limit": str,
  "ephemeral_request": str, "ephemeral_limit": str, "snapshot_size_limit": str|null
}
'''
import json

from device_agent.clients.container_maker_client import ContainerMakerClient, ContainerMakerClientError
from device_agent.commands._util import strip_container_maker_suffix
from device_agent.observability.logging_setup import get_logger

logger = get_logger("command.resume")


def make_handler(container_maker_client: ContainerMakerClient, cloud_client):
    async def handle(execute_command) -> tuple:
        try:
            cfg = json.loads(execute_command.container_config_json)
        except (json.JSONDecodeError, ValueError):
            return None, "INVALID_CONFIG", "container_config_json was missing or not valid JSON"

        required = ["saved_image", "container_name", "network_name", "cpu_request", "cpu_limit",
                    "memory_request", "memory_limit", "ephemeral_request", "ephemeral_limit"]
        missing = [k for k in required if k not in cfg]
        if missing:
            return None, "INVALID_CONFIG", f"container_config_json missing required fields: {missing}"
        if not cfg["saved_image"]:
            # "Never lose the saved image metadata" / "Missing/corrupt image" (Part 11 test) -
            # fail cleanly rather than attempting to create from an empty image reference.
            return None, "MISSING_SAVED_IMAGE", "Container has no saved_image to resume from"

        try:
            response = await container_maker_client.create_container(
                image_name=cfg["saved_image"], container_name=cfg["container_name"],
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
            logger.error("command.resume.container_maker_failed", extra={"command_id": execute_command.command_id, "error": str(e)})
            return None, "CONTAINER_MAKER_ERROR", str(e)[:1000]

        container_name = strip_container_maker_suffix(response.container_name)
        return {
            "kubernetes_id": response.container_id, "container_name": container_name,
            # See create.py's matching comment - container-maker-spec's ContainerResponse names
            # this field container_ip, not ip_address.
            "ip_address": getattr(response, "container_ip", None),
            "associated_resources": {"network_name": cfg["network_name"]},
        }, None, None

    return handle
