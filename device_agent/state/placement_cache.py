'''
Local cache of container_id -> (device_id, placement_generation) for containers THIS Device
Agent has placed on this device (migration Part 12 gap-fill).

Why this exists: status_monitor watches raw Kubernetes Pod objects. Nothing about a pod today
carries its DB placement_generation (no label, no annotation, no env var - container-maker only
ever stamps the container-id label). Cloud's `conditional_container_update` (the handler behind
`ReportContainerStatus`) requires an exact device_id+placement_generation match or it safely
no-ops the update (by design - see browseterm-db's DeviceCommandOps.conditional_container_update
docstring), so a status report sent with a wrong/zero generation is silently dropped, not applied.

Device Agent already learns a container's true placement_generation at the one moment it matters
- when it executes a CREATE or RESUME ExecuteCommand for it (the command carries device_id and
placement_generation itself). This module remembers that, so `ReportContainerStatus` can supply
the real value instead of trusting whatever status_monitor guesses (status_monitor cannot know
this from the pod alone - see LocalDeviceAgentServicer's own docstring for the rest of the
reasoning).

Known limitation (not silently hidden): this is in-memory-per-process state, not durable. A
container placed by a PRIOR Device Agent process lifetime (or a fresh install adopting
already-running pods) has no entry until its next CREATE/RESUME. Status reports for such a
container are forwarded with the caller's own (unreliable) value and safely no-op server-side
until the entry is populated. This is the same class of gap Part 7's own reconciliation module
(device_agent/reconciliation/, currently unbuilt) is meant to close generally - not papered over
here with a heavier, untested persistence mechanism.
'''
import threading
from typing import Optional, Tuple


class PlacementCache:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entries: dict[str, Tuple[str, int]] = {}  # container_id -> (device_id, placement_generation)

    def record(self, container_id: str, device_id: str, placement_generation: int) -> None:
        if not container_id:
            return
        with self._lock:
            self._entries[container_id] = (device_id, placement_generation)

    def forget(self, container_id: str) -> None:
        with self._lock:
            self._entries.pop(container_id, None)

    def get(self, container_id: str) -> Optional[Tuple[str, int]]:
        with self._lock:
            return self._entries.get(container_id)
