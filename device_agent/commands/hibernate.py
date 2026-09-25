'''
HIBERNATE command handler (Part 10). Ported ordering from browseterm-server-local's
api_handlers.py::hibernate_container (the manual-hibernate endpoint) and reaper.py's automatic
path, which both enforce: save -> CONFIRM success -> delete pod -> only then hibernate is real.

Correction (2026-09-22): an earlier version of this handler deleted the pod as soon as
container-maker's saveContainer RPC *returned*, treating that as confirmation. That was wrong -
the RPC only triggers the snapshot Job and returns a PREDICTED image name; it does not wait for
the tar->MinIO->snapshot_job->registry pipeline to actually finish (see
save_execution.py's module docstring for the full explanation). This handler now calls
save_execution.perform_save() - shared with save.py, not reimplemented here - which polls Cloud
for the confirmed outcome before this handler does anything else.

Hard invariant preserved: the pod is deleted ONLY after perform_save() reports a CONFIRMED
success with a real (not predicted) saved image name. Any failure or timeout before that point
leaves the pod running, untouched, quota still reserved.

Correction (2026-09-25, qa.md items 3/4): manual (UI) hibernate and Reaper's idle-timeout
hibernate are NOT the same operation - the owner's own QA spec requires manual hibernate to be
fast (free the resource and delete the pod immediately, no automatic save; the user is expected
to hit Save themselves first if they want a snapshot) while Reaper's hibernate must still save
before deleting. Cloud threads that distinction through as `container_config_json["skip_save"]`
(see container_config_snapshot.py::build_hibernate_config_json) - set true only for the
browser-initiated `/app/containers/{id}/hibernate` route, never for Reaper's device-command path.
'''
import json

from device_agent.clients.container_maker_client import ContainerMakerClient, ContainerMakerClientError
from device_agent.commands.save_execution import perform_save
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

        if cfg.get("skip_save"):
            # Manual/UI hibernate (qa.md item 3): free the resource fast - delete the pod
            # directly, no save step at all. Whatever saved_image the container already had
            # (from an earlier explicit Save) is left untouched by container_mutation.py, since
            # this handler reports no saved_image here.
            try:
                await container_maker_client.delete_container(
                    container_id=container_id, network_name=network_name, request_id=execute_command.trace_id,
                )
            except ContainerMakerClientError as e:
                logger.error("command.hibernate.delete_failed", extra={
                    "command_id": execute_command.command_id, "error": str(e),
                })
                return None, "POD_DELETE_FAILED", str(e)[:1000]
            return {}, None, None

        # 1. Save, and WAIT for Cloud's confirmation - never proceed on the RPC's own return.
        #    Any failure/timeout here leaves the pod running and quota reserved (Part 10's
        #    required test - quota accounting itself is Part 12's used_cpu/status_monitor
        #    concern, not this handler's; a HIBERNATE command never holds a
        #    reserve_quota_and_create_command-style reservation to release in the first place,
        #    only CREATE/RESUME do).
        outcome = await perform_save(
            container_maker_client, cloud_client, container_id=container_id,
            network_name=network_name, request_id=execute_command.trace_id,
        )
        if not outcome.succeeded:
            error_code = "SNAPSHOT_TIMED_OUT" if outcome.timed_out else "SNAPSHOT_FAILED"
            logger.error("command.hibernate.save_failed", extra={
                "command_id": execute_command.command_id, "timed_out": outcome.timed_out, "error": outcome.error,
            })
            return None, error_code, (outcome.error or "Snapshot failed")[:1000]

        saved_image = outcome.image_reference

        # 2. Delete the pod - ONLY reached after a CONFIRMED successful save with a real image.
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
        return {"saved_image": saved_image, "pod_name": outcome.pod_name, "namespace": outcome.namespace}, None, None

    return handle
