#!/usr/bin/env bash
# Run the integration tests from a container on the Compose dev stack's
# network, talking to nginx, mockllm, the gateways' admin and metrics ports,
# Postgres and Redis by service name.
#
#   make up-dev
#   gateway/integration/run-in-network.sh [test binary flags, e.g. -test.run X]
#   gateway/integration/run-in-network.sh -test.run TestClientAbortsCancelUpstreamAtScale
#
# Why a container: the admin API (8081), the gateways' metrics (9090) and
# Redis are on the Compose network only, so the admin API tests (AC-M3-1/2),
# the abort test's admission counters (AC-M2-4) and the Redis tests in
# internal/redis and internal/arbitrator can only run there. On macOS the
# Docker VM's published ports also go through a userspace port forwarder
# (observed with OrbStack) that delays some client disconnects by 0.2-10 s
# before the container sees them, even with no gateway in the path; that
# would measure the forwarder, not the gateway.
#
# Environment:
#   COMPOSE_PROJECT  compose project name          llm-synapse-gw
#   RUNNER_IMAGE     image the test binary runs in  llm-synapse-gw/mockllm:dev (built by make up-dev)
#   OUT_DIR          host dir mounted at /out       a new directory under build/
#   INTEGRATION_*    passed through (INTEGRATION_ABORT_RESULTS is relative to /out)
#   INTEGRATION_ADMIN_TOKEN, INTEGRATION_POSTGRES_DSN
#                    default to GATEWAY_ADMIN_TOKEN_PROMPT_OWNERS and
#                    GATEWAY_TEMPLATES_POSTGRES_DSN in deploy/compose/gateway.env
#   REDIS_ADDR       Redis for the internal/redis and internal/arbitrator tests  redis:6379
#
# The repository is mounted read-only at its own path and each test binary
# runs in its package directory, as go test would, so the tests' relative
# paths (wire goldens, harness template specs) resolve. The test binaries
# and the default OUT_DIR live under build/ (gitignored) rather than /tmp, so
# the Docker daemon can mount them even when this script itself runs in a
# container (a CI job container, act, a dev container).
# TestTraceIDJoinsUpstreamAndGatewayLog reads gateway logs with the docker CLI
# and skips here; run it from the host (see doc.go).
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
GATEWAY_DIR=$(cd "$HERE/.." && pwd)
REPO_DIR=$(cd "$GATEWAY_DIR/.." && pwd)
PROJECT=${COMPOSE_PROJECT:-llm-synapse-gw}
NETWORK="${PROJECT}_default"
IMAGE=${RUNNER_IMAGE:-llm-synapse-gw/mockllm:dev}
mkdir -p "$REPO_DIR/build"
OUT_DIR=${OUT_DIR:-$(mktemp -d "$REPO_DIR/build/in-network-out.XXXXXX")}
ARCH=$(docker version -f '{{.Server.Arch}}')
GATEWAY_ENV="$REPO_DIR/deploy/compose/gateway.env"

if ! docker network inspect "$NETWORK" >/dev/null 2>&1; then
	echo "network $NETWORK not found: start the stack with 'make up-dev' (or set COMPOSE_PROJECT)" >&2
	exit 1
fi

# envfile KEY: the last KEY= value in gateway.env, or empty.
envfile() { [ -f "$GATEWAY_ENV" ] && sed -n "s/^$1=//p" "$GATEWAY_ENV" | tail -n 1 || true; }

mkdir -p "$OUT_DIR"
BIN_DIR=$(mktemp -d "$REPO_DIR/build/in-network-bin.XXXXXX")
trap 'rm -rf "$BIN_DIR"' EXIT
# package:binary pairs, each run in its own directory.
PACKAGES=(integration:integration internal/redis:redis internal/arbitrator:arbitrator)
for p in "${PACKAGES[@]}"; do
	(cd "$GATEWAY_DIR" && CGO_ENABLED=0 GOOS=linux GOARCH="$ARCH" \
		go test -c -tags integration -o "$BIN_DIR/${p#*:}.test" "./${p%%:*}/")
done

env_args=(
	-e "INTEGRATION_GATEWAY_URL=${INTEGRATION_GATEWAY_URL:-http://nginx:80}"
	-e "INTEGRATION_MOCKLLM_URLS=${INTEGRATION_MOCKLLM_URLS:-http://mockllm:8000,http://mockllm-small:8000}"
	-e "INTEGRATION_WIRE_GOLDENS=$GATEWAY_DIR/testdata/wire"
	-e "INTEGRATION_GATEWAY_METRICS_URLS=${INTEGRATION_GATEWAY_METRICS_URLS:-http://gateway-1:9090/metrics,http://gateway-2:9090/metrics}"
	-e "INTEGRATION_ADMIN_URL=${INTEGRATION_ADMIN_URL:-http://gateway-1:8081}"
	-e "INTEGRATION_ADMIN_TOKEN=${INTEGRATION_ADMIN_TOKEN:-$(envfile GATEWAY_ADMIN_TOKEN_PROMPT_OWNERS)}"
	-e "INTEGRATION_POSTGRES_DSN=${INTEGRATION_POSTGRES_DSN:-$(envfile GATEWAY_TEMPLATES_POSTGRES_DSN)}"
	-e "REDIS_ADDR=${REDIS_ADDR:-redis:6379}"
)
for v in INTEGRATION_ABORTS INTEGRATION_ABORT_WAVE INTEGRATION_WIRE_MODEL INTEGRATION_UPSTREAM_AUTH REDIS_PASSWORD; do
	if [ -n "${!v:-}" ]; then env_args+=(-e "$v=${!v}"); fi
done
if [ -n "${INTEGRATION_ABORT_RESULTS:-}" ]; then
	env_args+=(-e "INTEGRATION_ABORT_RESULTS=/out/${INTEGRATION_ABORT_RESULTS#/}")
fi

echo "running in network $NETWORK (image $IMAGE, linux/$ARCH); output in $OUT_DIR" >&2
rc=0
for p in "${PACKAGES[@]}"; do
	echo "== ${p%%:*}" >&2
	docker run --rm --network "$NETWORK" \
		--user "$(id -u):$(id -g)" \
		-v "$BIN_DIR:/bin-test:ro" -v "$OUT_DIR:/out" -v "$REPO_DIR:$REPO_DIR:ro" \
		-w "$GATEWAY_DIR/${p%%:*}" \
		"${env_args[@]}" \
		--entrypoint "/bin-test/${p#*:}.test" \
		"$IMAGE" -test.v -test.count=1 "$@" || rc=1
done
exit "$rc"
