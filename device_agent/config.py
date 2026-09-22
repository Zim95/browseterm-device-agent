'''
Device Agent configuration. Everything here comes from env vars / mounted files - no hardcoded
production values, per the migration doc's "add documented placeholders" rule.
'''
import os

# Cloud Device Control gRPC endpoint. Production default is the real app host on 443 - Device
# Agent always dials OUT, over TLS, never the reverse (migration doc, fixed product decisions).
CLOUD_CONTROL_HOST: str = os.getenv("CLOUD_CONTROL_HOST", "app.browseterm.puhtaeto.com")
CLOUD_CONTROL_PORT: int = int(os.getenv("CLOUD_CONTROL_PORT", "443"))
CLOUD_CONTROL_TLS: bool = os.getenv("CLOUD_CONTROL_TLS", "true").lower() == "true"

# Device identity/credential - mounted as a mode-0400 file by the Desktop/CLI device-linking flow
# (Part 4), never passed as a CLI argument or env var (would leak into `ps`/logs).
DEVICE_ID_FILE: str = os.getenv("DEVICE_ID_FILE", "/etc/browseterm/device/device_id")
DEVICE_TOKEN_FILE: str = os.getenv("DEVICE_TOKEN_FILE", "/etc/browseterm/device/token")

AGENT_VERSION: str = os.getenv("AGENT_VERSION", "0.1.0")
PROTOCOL_VERSION: str = os.getenv("PROTOCOL_VERSION", "v1")

# A stable ID minted once per real Desktop/CLI process startup (Part 14) - passed in by whatever
# supervises this process (systemd/launchd/Multipass unit), not generated here, since Device
# Agent itself may restart independently of a "real" startup (a crash-restart must NOT count as a
# new startup for activation purposes).
STARTUP_ID_FILE: str = os.getenv("STARTUP_ID_FILE", "/etc/browseterm/device/startup_id")

# Reconnect backoff schedule (Part 7): "Retry 1s, 2s, 4s, 8s, 15s, then around 30s with jitter."
RECONNECT_BACKOFF_SCHEDULE_SECONDS: list = [1, 2, 4, 8, 15, 30]
RECONNECT_JITTER_FRACTION: float = float(os.getenv("RECONNECT_JITTER_FRACTION", "0.2"))

HEARTBEAT_INTERVAL_SECONDS: int = int(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "20"))

# Local SQLite command journal, on a PersistentVolume so it survives pod restarts.
COMMAND_JOURNAL_PATH: str = os.getenv("COMMAND_JOURNAL_PATH", "/var/lib/browseterm/device-agent/journal.sqlite3")

# Container Maker (existing private mTLS gRPC interface - unchanged by this migration).
CONTAINER_MAKER_HOST: str = os.getenv("CONTAINER_MAKER_HOST", "container-maker-service")
CONTAINER_MAKER_PORT: int = int(os.getenv("CONTAINER_MAKER_PORT", "50052"))
CONTAINER_MAKER_CERTS_SECRET_NAME: str = os.getenv("CONTAINER_MAKER_CERTS_SECRET_NAME", "container-maker-certs")
NAMESPACE: str = os.getenv("NAMESPACE", "browseterm")

# Private in-cluster API for Status Monitor/Reaper/Socket-SSH (Part 12).
LOCAL_API_PORT: int = int(os.getenv("LOCAL_API_PORT", "50061"))

RECONCILE_ON_STARTUP: bool = os.getenv("RECONCILE_ON_STARTUP", "true").lower() == "true"

# Save/Hibernate completion polling (Parts 10/SAVE): container-maker's saveContainer RPC returns
# as soon as it creates the snapshot Job, not once the tar->MinIO->snapshot_job->registry pipeline
# actually finishes - the real completion signal only exists on Cloud (snapshot_job reports there
# directly). Device Agent has no DB access, so it polls Cloud for the confirmed outcome instead of
# trusting the RPC response. Docker builds can be slow, hence the generous timeout.
SNAPSHOT_POLL_INTERVAL_SECONDS: float = float(os.getenv("SNAPSHOT_POLL_INTERVAL_SECONDS", "5"))
SNAPSHOT_POLL_TIMEOUT_SECONDS: float = float(os.getenv("SNAPSHOT_POLL_TIMEOUT_SECONDS", "600"))
