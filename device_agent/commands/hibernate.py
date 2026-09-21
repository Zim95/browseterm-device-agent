'''
HIBERNATE command handler (Part 10). Ported ordering from browseterm-server-local's
api_handlers.py::hibernate_container (the manual-hibernate endpoint) and reaper.py's automatic
path, which both enforce: save -> confirm success -> delete pod -> only then hibernate is real.

Architectural adaptation from the old Local code: the old handler polled the container's
`save_status` DB column after calling save_container_in_k8s, because Local had direct Cloud/DB
visibility. Device Agent has none - Cloud is the only thing that reads/writes Postgres now. This
is not a gap: container-maker's SaveContainerResponse already carries the pushed image name
directly in its RPC response (`saved_pods[0].image_name`, container-maker-spec's types.proto) -
the RPC genuinely is the authoritative synchronous completion signal, not merely a trigger for
async work the caller then has to poll for. Treating the RPC return as final preserves the exact
correctness property the old polling achieved (never delete before a confirmed successful save)
without needing DB access Device Agent doesn't have.

Hard invariant preserved: the pod is deleted ONLY after save_container succeeds AND returns a
saved image name. Any failure before that point leaves the pod running untouched.
'''
import json

from device_agent.clients.container_maker_client import ContainerMakerClient, ContainerMakerClientError
from device_agent.observability.logging_setup import get_logger

logger = get_logger("command.hibernate")


def make_handler(container_maker_client: ContainerMakerClient, cloud_client):
    async def handle(execute_command) -> tuple:
        try:
            cfg = json.loads(execute_command.container_config_json) if execute_command.container_config_json else {}
        except (json.JSONDecodeError, ValueError):
            cfg = {}
        network_name = cfg.get("network_name", "")
        container_id = execute_command.container_id

        # 1. Save/snapshot. Any failure here leaves the pod running - "snapshot failure leaves
        #    pod running and quota reserved" (Part 10's required test - quota accounting itself
        #    is Part 12's used_cpu/status_monitor concern, not this handler's; a HIBERNATE command
        #    never holds a reserve_quota_and_create_command-style reservation to release in the
        #    first place, only CREATE/RESUME do).
        try:
            save_response = await container_maker_client.save_container(
                container_id=container_id, network_name=network_name, request_id=execute_command.trace_id,
            )
        except ContainerMakerClientError as e:
            logger.error("command.hibernate.save_failed", extra={"command_id": execute_command.command_id, "error": str(e)})
            return None, "SNAPSHOT_FAILED", str(e)[:1000]

        saved_pods = list(getattr(save_response, "saved_pods", []))
        if not saved_pods or not saved_pods[0].image_name:
            logger.error("command.hibernate.save_returned_no_image", extra={"command_id": execute_command.command_id})
            return None, "SNAPSHOT_FAILED", "Snapshot completed but container-maker returned no saved image name"

        saved_image = saved_pods[0].image_name

        # 2. Delete the pod - ONLY reached after a confirmed successful save with a real image.
        try:
            await container_maker_client.delete_container(
                container_id=container_id, network_name=network_name, request_id=execute_command.trace_id,
            )
        except ContainerMakerClientError as e:
            # "Pod delete failure does not release quota prematurely" - reported as a FAILED
            # result (the pod is still running, this is not a successful hibernate), but the
            # saved_image is included so Cloud can choose to persist it for a future retry
            # without pretending the container is actually hibernated (device_id/status must NOT
            # change - the pod is still there).
            logger.error("command.hibernate.delete_after_save_failed", extra={
                "command_id": execute_command.command_id, "error": str(e), "saved_image": saved_image,
            })
            return None, "POD_DELETE_FAILED_AFTER_SAVE", json.dumps({"saved_image": saved_image, "detail": str(e)[:800]})

        # 3. Success - pod is confirmed gone AND the image is confirmed saved.
        return {"saved_image": saved_image, "pod_name": saved_pods[0].pod_name, "namespace": saved_pods[0].namespace_name}, None, None

    return handle
