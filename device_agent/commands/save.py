'''
SAVE command handler. Snapshots a running container WITHOUT deleting it - the pod stays running
throughout and after, unlike HIBERNATE which saves then deletes. Core reliability feature, wired
identically to every other command: durable command row, delivered over the control stream,
executed here, one terminal CommandResult reported back.

All the "trigger + wait for confirmed completion" logic lives in save_execution.perform_save() -
shared with hibernate.py, not duplicated here.
'''
import json

from device_agent.clients.container_maker_client import ContainerMakerClient
from device_agent.commands.save_execution import perform_save
from device_agent.observability.logging_setup import get_logger

logger = get_logger("command.save")


def make_handler(container_maker_client: ContainerMakerClient, cloud_client):
    async def handle(execute_command) -> tuple:
        try:
            cfg = json.loads(execute_command.container_config_json) if execute_command.container_config_json else {}
        except (json.JSONDecodeError, ValueError):
            cfg = {}
        network_name = cfg.get("network_name", "")
        container_id = execute_command.container_id

        outcome = await perform_save(
            container_maker_client, cloud_client, container_id=container_id,
            network_name=network_name, request_id=execute_command.trace_id,
        )

        if not outcome.succeeded:
            error_code = "SNAPSHOT_TIMED_OUT" if outcome.timed_out else "SNAPSHOT_FAILED"
            logger.error("command.save.failed", extra={
                "command_id": execute_command.command_id, "container_id": container_id,
                "timed_out": outcome.timed_out, "error": outcome.error,
            })
            return None, error_code, (outcome.error or "Save failed")[:1000]

        # No pod deletion, no quota release - the container keeps running. Cloud's own
        # container_mutation.py updates saved_image/save_status; container status stays RUNNING.
        return {"saved_image": outcome.image_reference, "pod_name": outcome.pod_name, "namespace": outcome.namespace}, None, None

    return handle
