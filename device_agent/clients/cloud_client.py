'''
Device-scoped HTTP client to Cloud. Currently unused by the CREATE/DELETE/HIBERNATE/RESUME
handlers themselves - canonical container config arrives inline in ExecuteCommand.
container_config_json (Parts 8-11 chose Cloud-sends-a-snapshot over a separate fetch RPC, both
sanctioned by the migration doc's Part 5). Kept as the one place any FUTURE device-scoped Cloud
HTTP call would go (e.g. Part 13's terminal ticket consumption is Socket-SSH's concern, not this
one, but something analogous could land here later) - always using the device's own Bearer token,
never CLOUD_INTERNAL_API_TOKEN or any other global credential.
'''
import httpx


class CloudClient:
    def __init__(self, device_id: str, device_token: str, base_url: str = "https://app.browseterm.puhtaeto.com") -> None:
        self.device_id = device_id
        self._client = httpx.AsyncClient(
            base_url=base_url, headers={"Authorization": f"Bearer {device_token}"}, timeout=10.0,
        )

    async def close(self) -> None:
        await self._client.aclose()
