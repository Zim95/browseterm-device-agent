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
'''
from typing import Optional

import httpx


class CloudClient:
    def __init__(self, device_id: str, device_token: str, base_url: str = "https://app.browseterm.puhtaeto.com") -> None:
        self.device_id = device_id
        self._client = httpx.AsyncClient(
            base_url=base_url, headers={"Authorization": f"Bearer {device_token}"}, timeout=10.0,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def request_hibernate(self, container_id: str) -> dict:
        '''POST /devices/{device_id}/containers/{container_id}/hibernate-request (Part 12).
        Returns {"created": bool, "command_id": str|None, "error": str|None}.'''
        response = await self._client.post(f"/devices/{self.device_id}/containers/{container_id}/hibernate-request")
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
        ticket" catch-all (never leaking which case it was).'''
        response = await self._client.post("/internal/terminal-tickets/consume", json={"ticket": ticket})
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
