#!/bin/bash
set -euo pipefail

exec /app/.venv/bin/python -m device_agent.main
