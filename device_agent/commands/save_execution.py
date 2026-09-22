'''
Shared "trigger save, then wait for CONFIRMED completion" sequence for SAVE and HIBERNATE - the
two operations that share this mechanism. Do not duplicate this logic per operation.

Container Maker's saveContainer RPC returns as soon as it creates the snapshot Kubernetes Job -
it does NOT block on the Job actually finishing (see container-maker's pod_manager.py::save_image
docstring: "Returns as soon as the Job is created - does NOT block on Job completion"), and the
image name in its response is a deterministic PREDICTION (`{repo}/{pod_name}-image:latest`), not
a confirmation anything was built or pushed. The only authoritative "this save actually succeeded,
here is the real pushed image" signal in the whole system is `snapshot_job` reporting directly to
Cloud (`POST /internal/containers/{id}/snapshots/{snapshot_id}/report`) once the tar->MinIO->
build->push pipeline finishes. Device Agent has no direct database access by design, so it polls
Cloud for that confirmed outcome instead of trusting the RPC response - the same correctness
property the old Local code's `save_status` DB polling achieved, just reached over HTTP instead of
a DB connection Device Agent doesn't have.

An earlier version of this handler treated the RPC's return as the authoritative signal and
deleted the pod immediately after - that was a real bug (a pod could be deleted before its image
was actually pushed, losing the workspace). This module exists specifically to fix that: nothing
that depends on a successful save (image updates, pod deletion, quota release) may happen until
`perform_save()` returns a confirmed `succeeded=True` outcome.
'''
import time
import asyncio
from dataclasses import dataclass
from typing import Optional

from device_agent import config
from device_agent.clients.container_maker_client import ContainerMakerClient, ContainerMakerClientError
from device_agent.observability.logging_setup import get_logger

logger = get_logger("command.save_execution")


@dataclass
class SaveOutcome:
    succeeded: bool
    timed_out: bool
    image_reference: Optional[str] = None
    pod_name: Optional[str] = None
    namespace: Optional[str] = None
    error: Optional[str] = None


async def perform_save(
    container_maker_client: ContainerMakerClient, cloud_client, container_id: str, network_name: str, request_id: str,
) -> SaveOutcome:
    '''
    1. Trigger the snapshot via container-maker's saveContainer RPC. Any failure here is a clean
       "the save never started" outcome - nothing was created, nothing to clean up.
    2. Poll Cloud's device-scoped save-status endpoint (backed by the exact same
       container_snapshots row snapshot_job's own report call writes) until it reports a terminal
       status for THIS request_id, or SNAPSHOT_POLL_TIMEOUT_SECONDS elapses.
    3. Only a Cloud-confirmed "Succeeded" with a real image_reference counts as success. A
       confirmed "Failed", or a timeout, both leave whatever triggered this (the pod, in
       hibernate's case) exactly as it was - callers must not guess a recovery action.

    `pod_name`/`namespace` come from the initial RPC response (a real pod that genuinely exists,
    not a prediction) and are always populated when the trigger succeeds, regardless of the final
    outcome - callers may want them for logging even on failure/timeout.
    '''
    try:
        save_response = await container_maker_client.save_container(
            container_id=container_id, network_name=network_name, request_id=request_id,
        )
    except ContainerMakerClientError as e:
        logger.error("save_execution.trigger_failed", extra={"container_id": container_id, "error": str(e)})
        return SaveOutcome(succeeded=False, timed_out=False, error=str(e)[:1000])

    saved_pods = list(getattr(save_response, "saved_pods", []))
    if not saved_pods:
        logger.error("save_execution.trigger_returned_no_pod", extra={"container_id": container_id})
        return SaveOutcome(succeeded=False, timed_out=False, error="Snapshot trigger returned no pod information")
    pod_name = saved_pods[0].pod_name
    namespace = saved_pods[0].namespace_name

    deadline = time.monotonic() + config.SNAPSHOT_POLL_TIMEOUT_SECONDS
    while True:
        try:
            status = await cloud_client.get_save_status(container_id=container_id, request_id=request_id)
        except Exception as e:
            # Cloud unreachable mid-poll - bounded by the same deadline, not an immediate failure
            # (a transient outage must not prematurely delete a pod that may yet save correctly).
            logger.error("save_execution.poll_request_failed", extra={"container_id": container_id, "error": str(e)})
            status = {}

        snapshot_status = status.get("status")
        if snapshot_status == "Succeeded" and status.get("image_reference"):
            return SaveOutcome(
                succeeded=True, timed_out=False, image_reference=status["image_reference"],
                pod_name=pod_name, namespace=namespace,
            )
        if snapshot_status == "Failed":
            return SaveOutcome(
                succeeded=False, timed_out=False, error=status.get("error_detail") or "Snapshot reported failed",
                pod_name=pod_name, namespace=namespace,
            )

        if time.monotonic() >= deadline:
            logger.error("save_execution.poll_timed_out", extra={"container_id": container_id, "request_id": request_id})
            return SaveOutcome(
                succeeded=False, timed_out=True, error="Timed out waiting for snapshot confirmation",
                pod_name=pod_name, namespace=namespace,
            )
        await asyncio.sleep(config.SNAPSHOT_POLL_INTERVAL_SECONDS)
