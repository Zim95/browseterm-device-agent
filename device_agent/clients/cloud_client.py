'''
Device-scoped HTTP client to Cloud - always the device's own Bearer token, never
CLOUD_INTERNAL_API_TOKEN or any other global credential. Canonical container config for
CREATE/DELETE/HIBERNATE/RESUME arrives inline in ExecuteCommand.container_config_json instead of
a fetch call (Parts 8-11 chose Cloud-sends-a-snapshot, both sanctioned by Part 5) - the two calls
here exist because their callers (Reaper, Socket-SSH via local_api) need a synchronous answer the
async control stream can't give: RequestHibernate needs a command reference back immediately, and
ConsumeTerminalTicket needs the actual SSH target right now.

get_save_status is a third such synchronous-answer call: save_execution.py's perform_save() polls
it in a loop, since snapshot_job reports the real completion signal directly to Cloud (not to
Device Agent), and Device Agent has no other way to learn a save's confirmed outcome.

get_active_container_ids/reconcile_device_resources/get_idle_containers/allocate_snapshot/
report_snapshot_result/get_container/update_container_kubernetes_id (finishing Part 12) are a
fourth class: unlike get_tunnel_generation/get_save_status above, a wrong safe-default here could
cause real harm (e.g. an empty active-container list could make status_monitor think every
genuinely-running container is lost) - so these raise CloudClientError on any failure instead of
returning a default, and local_api/service.py aborts the gRPC call so the original caller
(status_monitor/reaper/snapshot_job/container-maker) sees the same kind of failure their old
direct-to-Cloud CloudClientError already gave them.
'''
from typing import Optional

import httpx

from device_agent.clients.retry import call_with_retry


class CloudClientError(Exception):
    """Raised by the Part-12-completion methods below on any non-2xx response or transport-level
    failure - see this module's own docstring for why these can't use a safe-default convention."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(f"Cloud API error: {message}")

# request_hibernate is idempotent (a duplicate collapses to a no-op via Cloud's own partial
# unique index on device_commands - see reaper's own request_hibernate docstring for the
# equivalent property on its side of this same call), so any transport-level failure (never got a
# response at all) is safe to retry broadly.
def _is_retryable_transport_error(e: BaseException) -> bool:
    return isinstance(e, httpx.HTTPError) and not isinstance(e, httpx.HTTPStatusError)


# consume_terminal_ticket is NOT idempotent - a ticket is single-use, so retrying after a response
# (even an error response) risks re-submitting a ticket the server already consumed successfully,
# turning a real success into a false "invalid ticket" for the caller. Only retry the strictly
# "never reached the server at all" cases.
def _is_retryable_connect_error(e: BaseException) -> bool:
    return isinstance(e, (httpx.ConnectError, httpx.ConnectTimeout))


class CloudClient:
    def __init__(self, device_id: str, device_token: str, base_url: str = "https://app.browseterm.puhtaeto.com") -> None:
        self.device_id = device_id
        self._client = httpx.AsyncClient(
            base_url=base_url, headers={"Authorization": f"Bearer {device_token}"}, timeout=10.0,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def request_hibernate(self, container_id: str, skip_save: bool = False) -> dict:
        '''POST /devices/{device_id}/containers/{container_id}/hibernate-request (Part 12).
        Returns {"created": bool, "command_id": str|None, "error": str|None}. A transport-level
        failure (Cloud briefly unreachable) is retried a few times before falling back to the
        same "not created" shape every other rejection already uses - never raises, callers
        (local_api/service.py's RequestHibernate) always get a well-formed response.

        skip_save (added 2026-09-27, for status_monitor's pod_watcher reporting a crashed/lost
        pod): the pod is already gone or unusable by the time this fires, so there is nothing
        left to snapshot - forwarded to Cloud so it builds the same skip_save HIBERNATE config
        the browser's own manual hibernate route already uses, which deletes (or no-ops on an
        already-gone) pod directly instead of attempting a doomed save first.'''
        try:
            response = await call_with_retry(
                lambda: self._client.post(
                    f"/devices/{self.device_id}/containers/{container_id}/hibernate-request",
                    json={"skip_save": True} if skip_save else {},
                ),
                is_retryable=_is_retryable_transport_error,
            )
        except httpx.HTTPError as e:
            return {"created": False, "command_id": None, "error": str(e)}
        if response.status_code == 202:
            body = response.json()
            return {"created": True, "command_id": body.get("command", {}).get("id"), "error": None}
        try:
            error = response.json().get("error")
        except ValueError:
            error = response.text
        return {"created": False, "command_id": None, "error": str(error)}

    async def consume_terminal_ticket(self, ticket: str) -> Optional[dict]:
        '''POST /internal/terminal-tickets/consume (Part 13, existing endpoint - see
        browseterm-server's terminal_handlers.py::consume_terminal_session). Returns None if the
        ticket is invalid/expired/wrong-device/the container isn't available - the exact reason
        is intentionally not distinguished here, matching that endpoint's own "Invalid or expired
        ticket" catch-all (never leaking which case it was). A connect-level failure (request
        never reached Cloud at all) is retried; anything past that point is NOT retried - a ticket
        is single-use, so retrying after Cloud may already have consumed it risks turning a real
        success into a false "invalid ticket" for the caller.'''
        try:
            response = await call_with_retry(
                lambda: self._client.post("/internal/terminal-tickets/consume", json={"ticket": ticket}),
                is_retryable=_is_retryable_connect_error,
            )
        except httpx.HTTPError:
            return None
        if response.status_code != 200:
            return None
        return response.json()

    async def get_save_status(self, container_id: str, request_id: str) -> dict:
        '''GET /devices/{device_id}/containers/{container_id}/save-status?request_id=... . Backed
        by the same container_snapshots row snapshot_job's own report call writes - see
        browseterm-server's snapshot_handlers.py::get_save_status. Returns
        {"status": "Pending"|"Running"|"Succeeded"|"Failed"|None, "image_reference": str|None,
        "error_detail": str|None}. A non-200 response (device/container not found, wrong device)
        is treated the same as "no result yet" (status=None) - the caller's poll loop just keeps
        waiting until its own timeout, rather than needing a distinct error path here.'''
        try:
            response = await self._client.get(
                f"/devices/{self.device_id}/containers/{container_id}/save-status",
                params={"request_id": request_id},
            )
        except httpx.HTTPError:
            return {"status": None, "image_reference": None, "error_detail": None}
        if response.status_code != 200:
            return {"status": None, "image_reference": None, "error_detail": None}
        return response.json()

    async def get_tunnel_generation(self) -> Optional[int]:
        '''GET /devices/{device_id}/tunnel/generation - Cloud's own authoritative
        devices.tunnel_generation for this device, 0 if it has never registered a tunnel at all.
        Backs local_api's GetTunnelGeneration RPC, which Tunnel Registrar calls on startup to
        resync a locally-persisted counter that may have fallen behind Cloud's real value (its own
        PVC lost/recreated, or a manual reset that undershot) - the exact gap that let a 2026-09-30
        incident silently reject every one of its reports forever, with nothing surfacing the
        rejection back to the registrar. Returns None (not 0) on any failure to reach Cloud, so
        the caller can tell "genuinely zero" apart from "couldn't check" and fall back to trusting
        its own local value alone rather than wrongly resetting a real counter to 0.'''
        try:
            response = await self._client.get(f"/devices/{self.device_id}/tunnel/generation")
        except httpx.HTTPError:
            return None
        if response.status_code != 200:
            return None
        return response.json().get("generation")

    async def report_command_result(
        self, command_id: str, status: str, result: Optional[dict],
        error_code: Optional[str], error_message: Optional[str], placement_generation: int,
    ) -> bool:
        '''POST /devices/{device_id}/commands/{command_id}/result (added 2026-09-26).

        This is now the PRIMARY way a finished command's result reaches Cloud - not the Device
        Control stream. That stream drops every ~30-90s in practice (Traefik's reverse-proxy
        times out reading its long-lived response - see BROWSETERM_MIGRATION_PROGRESS.md's own
        repeated notes on this), so a result queued on it could sit unreported for that long even
        though the real work (pod deleted, pod created, save confirmed) had already finished -
        this made Hibernate/Delete/Resume look "stuck" for up to ~90s. This call is a short-lived,
        synchronous HTTP request/response, same as request_hibernate/get_save_status above, which
        never had that problem.

        Retries transient transport failures - this call reports a real, already-computed
        outcome, so it's worth trying harder than a single attempt before giving up. Returns True
        only on a confirmed 200 from Cloud; the caller (main.py's report_result) leaves the
        journal entry unreported on any other outcome, so the existing stream-based
        resend-on-reconnect mechanism (grpc_client.py's build_unreported_results) still catches it
        eventually as a backstop - this is a faster PRIMARY path, not a replacement for that
        safety net.'''
        body = {
            "status": status, "result": result, "placement_generation": placement_generation,
            "error_code": error_code, "error_message": error_message,
        }
        try:
            response = await call_with_retry(
                lambda: self._client.post(f"/devices/{self.device_id}/commands/{command_id}/result", json=body),
                is_retryable=_is_retryable_transport_error,
                max_attempts=5,
            )
        except httpx.HTTPError:
            return False
        return response.status_code == 200

    async def get_active_container_ids(self) -> list[str]:
        '''GET /devices/{device_id}/active-containers (finishing Part 12 - status_monitor's own
        durability check, formerly a direct internal-token call). Raises CloudClientError on any
        failure - see this module's own docstring for why no safe default exists here.'''
        try:
            response = await call_with_retry(
                lambda: self._client.get(f"/devices/{self.device_id}/active-containers"),
                is_retryable=_is_retryable_transport_error,
            )
        except httpx.HTTPError as e:
            raise CloudClientError(str(e)) from e
        if response.status_code != 200:
            raise CloudClientError(f"unexpected status {response.status_code}")
        return response.json().get("container_ids", [])

    async def reconcile_device_resources(self, running_container_ids: list[str], running_pod_ips: dict) -> None:
        '''POST /devices/{device_id}/resources/reconcile (finishing Part 12 - status_monitor's own
        resource-drift repair, formerly a direct internal-token call).'''
        try:
            response = await call_with_retry(
                lambda: self._client.post(
                    f"/devices/{self.device_id}/resources/reconcile",
                    json={"running_container_ids": running_container_ids, "running_pod_ips": running_pod_ips},
                ),
                is_retryable=_is_retryable_transport_error,
            )
        except httpx.HTTPError as e:
            raise CloudClientError(str(e)) from e
        if response.status_code != 200:
            raise CloudClientError(f"unexpected status {response.status_code}")

    async def get_idle_containers(self, idle_threshold_seconds: int) -> list[str]:
        '''GET /devices/{device_id}/containers/idle (finishing Part 12 - reaper's own idle sweep,
        formerly a direct internal-token call). Reaper only ever reads each row's own id, so this
        returns just the ids rather than the full container rows the old HTTP response carried.'''
        try:
            response = await call_with_retry(
                lambda: self._client.get(
                    f"/devices/{self.device_id}/containers/idle",
                    params={"idle_threshold_seconds": idle_threshold_seconds},
                ),
                is_retryable=_is_retryable_transport_error,
            )
        except httpx.HTTPError as e:
            raise CloudClientError(str(e)) from e
        if response.status_code != 200:
            raise CloudClientError(f"unexpected status {response.status_code}")
        return [c["id"] for c in response.json().get("containers", [])]

    async def allocate_snapshot(self, container_id: str, request_id: str) -> dict:
        '''POST /devices/{device_id}/containers/{container_id}/snapshots/allocate (finishing Part
        12 - snapshot_job's own save-attempt allocation, formerly a direct internal-token call).
        Returns the same {"id", "version_sequence", "version", "image_repository", "status"}
        shape the old HTTP response's "snapshot" key carried.'''
        try:
            response = await call_with_retry(
                lambda: self._client.post(
                    f"/devices/{self.device_id}/containers/{container_id}/snapshots/allocate",
                    json={"request_id": request_id},
                ),
                is_retryable=_is_retryable_transport_error,
            )
        except httpx.HTTPError as e:
            raise CloudClientError(str(e)) from e
        if response.status_code not in (200, 201):
            raise CloudClientError(f"unexpected status {response.status_code}")
        return response.json()["snapshot"]

    async def report_snapshot_result(
        self, container_id: str, snapshot_id: str, status: str,
        image_reference: Optional[str] = None, registry_digest: Optional[str] = None,
        error_detail: Optional[str] = None,
    ) -> None:
        '''POST /devices/{device_id}/containers/{container_id}/snapshots/{snapshot_id}/report
        (finishing Part 12 - snapshot_job's own save-attempt reporting, formerly a direct
        internal-token call). This is the only signal anywhere that a real build+push
        succeeded/failed, so it retries noticeably harder than the default before giving up,
        matching the old direct-to-Cloud call's own max_attempts=5/max_delay_seconds=30.0.'''
        body: dict = {"status": status}
        if image_reference is not None:
            body["image_reference"] = image_reference
        if registry_digest is not None:
            body["registry_digest"] = registry_digest
        if error_detail is not None:
            body["error_detail"] = error_detail
        try:
            response = await call_with_retry(
                lambda: self._client.post(
                    f"/devices/{self.device_id}/containers/{container_id}/snapshots/{snapshot_id}/report",
                    json=body,
                ),
                is_retryable=_is_retryable_transport_error,
                max_attempts=5,
                max_delay_seconds=30.0,
            )
        except httpx.HTTPError as e:
            raise CloudClientError(str(e)) from e
        if response.status_code != 200:
            raise CloudClientError(f"unexpected status {response.status_code}")

    async def get_container(self, container_id: str) -> Optional[dict]:
        '''GET /devices/{device_id}/containers/{container_id} (finishing Part 12 -
        container-maker's own save() lookup, formerly a direct internal-token call). Returns None
        on a 404, matching the old direct-DB-backed lookup's own "no row" contract.'''
        try:
            response = await call_with_retry(
                lambda: self._client.get(f"/devices/{self.device_id}/containers/{container_id}"),
                is_retryable=_is_retryable_transport_error,
            )
        except httpx.HTTPError as e:
            raise CloudClientError(str(e)) from e
        if response.status_code == 404:
            return None
        if response.status_code != 200:
            raise CloudClientError(f"unexpected status {response.status_code}")
        return response.json()["container"]

    async def update_container_kubernetes_id(self, container_id: str, kubernetes_id: str) -> None:
        '''POST /devices/{device_id}/containers/{container_id}/kubernetes-id (finishing Part 12 -
        container-maker's own save() self-heal, formerly a direct internal-token call).'''
        try:
            response = await call_with_retry(
                lambda: self._client.post(
                    f"/devices/{self.device_id}/containers/{container_id}/kubernetes-id",
                    json={"kubernetes_id": kubernetes_id},
                ),
                is_retryable=_is_retryable_transport_error,
            )
        except httpx.HTTPError as e:
            raise CloudClientError(str(e)) from e
        if response.status_code != 200:
            raise CloudClientError(f"unexpected status {response.status_code}")
