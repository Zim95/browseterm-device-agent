#!/bin/bash
# Build + push the browseterm-device-agent image. Usage: ./scripts/deployment/build.sh <docker-username> <docker-repository>
set -euo pipefail
if [ $# -lt 2 ]; then
    echo "Usage: $0 <docker-username> <docker-repository>"
    exit 1
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

USERNAME=$1
REPOSITORY=$2
IMAGE_NAME=browseterm-device-agent
IMAGE_TAG=latest
DOCKERFILE=./infra/deployment/Dockerfile

# A plain `docker login -u "$USERNAME"` with no password always blocks on an interactive prompt -
# fine for a developer running this by hand, but it hard-fails ("Cannot perform an interactive
# login from a non TTY device") when invoked non-interactively, as the Desktop app's own Setup
# button does (desktop/local_stack.py's _build_device_agent_image, via `make prod_build`). Docker
# Desktop/CLI record a completed login for a registry under `auths` in ~/.docker/config.json even
# when the actual secret is delegated to a credsStore (Keychain, etc.) - checking for that key is
# the same thing the Docker CLI itself consults, and needs no extra dependency (jq etc.) beyond
# what's already here.
DOCKER_CONFIG_FILE="${DOCKER_CONFIG:-$HOME/.docker}/config.json"
if [ -f "$DOCKER_CONFIG_FILE" ] && grep -q '"https://index.docker.io/v1/"' "$DOCKER_CONFIG_FILE"; then
    echo "Already authenticated with Docker Hub (cached credentials found) - skipping docker login"
else
    docker login -u "$USERNAME"
fi
docker image build -t "$IMAGE_NAME:$IMAGE_TAG" -f "$DOCKERFILE" .
docker image tag "$IMAGE_NAME:$IMAGE_TAG" "$REPOSITORY/$IMAGE_NAME:$IMAGE_TAG"
docker push "$REPOSITORY/$IMAGE_NAME:$IMAGE_TAG"
