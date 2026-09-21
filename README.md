# browseterm-device-agent

The Device Agent (BROWSETERM_CLOUD_CONTROL_PLANE_MIGRATION.md Part 7): the ONLY component
allowed to communicate from a customer-controlled machine to Browseterm Cloud. Runs headless
inside the local k3s cluster (macOS/Windows Multipass VM, or native Linux k3s).

## What it does

- Establishes one outbound, bidirectional gRPC stream to Cloud (`DeviceControl.Connect`,
  `browseterm-device-control-spec`) over TLS, authenticated with this device's own Bearer token -
  never a global/shared credential.
- Reconnects forever with capped exponential backoff + jitter (1s, 2s, 4s, 8s, 15s, ~30s).
- Sends `Hello` + periodic `Heartbeat`; replies to Cloud's `Ping` with `Pong`.
- Receives `ExecuteCommand` (Create/Delete/Hibernate/Resume), deduplicates by `command_id` via a
  local SQLite journal (`device_agent/state/command_journal.py`) on a PersistentVolume, and
  dispatches to the matching handler in `device_agent/commands/`.
- Calls Container Maker through the existing private mTLS gRPC interface
  (`device_agent/clients/container_maker_client.py` - ported from
  `browseterm-server-local`'s `containers_service.py`, unchanged request/response shape).
- Reports `CommandAccepted`/`CommandProgress`/`CommandResult` back to Cloud, and resends any
  locally-journaled result Cloud never acknowledged on every fresh connection.

## What it does NOT do

No browser UI, no OAuth, no public ingress, no arbitrary Kubernetes API supplied by Cloud/browser,
no global Cloud credential, no direct Kubernetes manipulation where Container Maker already owns
that operation (see the migration doc's Part 7 "Non-responsibilities").

## Command config

`ExecuteCommand.container_config_json` carries the canonical, already-resolved container config
Cloud built at command-creation time (image name, resource limits, env vars, publish info,
`saved_image` for Resume) - see each handler module's docstring in `device_agent/commands/` for
the exact expected shape per operation. This is the migration doc's own sanctioned alternative to
a separate device-scoped fetch RPC ("Cloud sends a strictly typed, validated configuration
snapshot with a version").

## Local development

```bash
poetry install --no-root
poetry run pytest tests/ -v
```

Running the agent itself requires a mounted device credential
(`DEVICE_ID_FILE`/`DEVICE_TOKEN_FILE`, see `device_agent/config.py`) issued by the device-linking
flow (Part 4) and a reachable Container Maker mTLS Secret - not available outside a real cluster,
so `main.py` is not exercised by the test suite directly (every module it wires together is,
individually, with fakes/mocks at the I/O boundaries).

## Deployment

See `infra/deployment.yaml` - single replica, `Recreate` strategy (never two pods of the same
device identity connected at once), non-privileged Pod Security, PersistentVolume for the SQLite
journal, read-only credential mount. Several values are explicit `<TODO>` placeholders (image
registry/tag, storage class) pending real infrastructure decisions - never filled with invented
values, per the migration doc's own rule.
