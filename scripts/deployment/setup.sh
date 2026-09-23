#!/bin/bash
# Deploy browseterm-device-agent (ServiceAccount, Service, PVC, Deployment, NetworkPolicies) to
# the cluster. Does NOT create the browseterm-device-credential Secret this Deployment mounts -
# that's the caller's job (it holds this specific device's Bearer token, sourced from wherever the
# caller keeps it - e.g. browseterm-desktop's Keychain), same convention every other Secret this
# stack needs follows (never templated into a manifest here).
set -euo pipefail
if [ $# -lt 2 ]; then
    echo "Usage: $0 <namespace> <docker-repository>"
    exit 1
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

export NAMESPACE=$1
export REPO_NAME=$2

envsubst < ./infra/deployment.yaml | kubectl apply -f -
echo "browseterm-device-agent applied (namespace ${NAMESPACE})"
