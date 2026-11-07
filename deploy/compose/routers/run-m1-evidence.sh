#!/usr/bin/env bash
# M1-18 evidence on the mockllm routers stack (AC-M1-3, AC-M1-4). mockllm
# upstreams: this proves routing behaviour (placement, balance, stickiness,
# removal) only, never prefix-cache hit rate or GPU behaviour (ADR 0010).
#
#   deploy/compose/routers/run-m1-evidence.sh balance [out_dir]
#       For each rate in RATES, the chat profile through gateway-rr (baseline)
#       and then gateway-affinity, same seed, same mocks. During each run
#       balance.py samples gateway_inflight every INTERVAL; after it,
#       probe.py reports stickiness from the mocks' captures and balance.py
#       checks every sample against ceil((1+eps)*avg)+slack.
#   deploy/compose/routers/run-m1-evidence.sh removal [out_dir]
#       gateway-affinity recreated on a live copy of its config
#       (docker-compose.live-config.yml); the chat profile at REMOVAL_RATE for
#       REMOVAL_DURATION, and REMOVE_AFTER seconds in balance.py drops
#       REMOVE (mock-2) from the live config. Failed requests are matched to
#       replicas through the gateway's per-request log (trace IDs only).
#
# Knobs: RATES ("2 5 20 60"), DURATION (120s; LOW_DURATION 300s for rates
# below 5), REMOVAL_RATES ("20 60"), REMOVAL_DURATION (120s), REMOVE_AFTER
# (60), REMOVE (mock-2), INTERVAL (0.1), SEED (1), EPSILON (0.25), SLACK (1):
# EPSILON and SLACK must match gateway.affinity.yaml.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../../.." && pwd)
MODE=${1:?usage: run-m1-evidence.sh balance|removal [out_dir]}
OUT=${2:-$ROOT/harness/out/m1-18-$MODE-$(date -u +%Y%m%dT%H%M%SZ)}
RATES=${RATES:-2 5 20 60}
DURATION=${DURATION:-120s}
LOW_DURATION=${LOW_DURATION:-300s}
REMOVAL_RATES=${REMOVAL_RATES:-20 60}
REMOVAL_DURATION=${REMOVAL_DURATION:-120s}
REMOVE_AFTER=${REMOVE_AFTER:-60}
REMOVE=${REMOVE:-mock-2}
INTERVAL=${INTERVAL:-0.1}
SEED=${SEED:-1}
EPSILON=${EPSILON:-0.25}
SLACK=${SLACK:-1}
IMAGE=${BENCH_IMAGE:-alpine:3.22}
COMPOSE=(docker compose -f "$HERE/docker-compose.yml")
LIVE=(-f "$HERE/docker-compose.live-config.yml")
NET=llm-synapse-gw-routers_default
SERVICES=(mock-0 mock-1 mock-2 gateway-rr gateway-affinity)
# Gateway metrics port on the host and URL on the compose network, per path.
metrics_url() { if [ "$1" = rr ]; then echo http://127.0.0.1:18140/metrics; else echo http://127.0.0.1:18143/metrics; fi; }
target_url() { echo "http://gateway-$1:8080"; }

mkdir -p "$OUT/bin"
OUT=$(cd "$OUT" && pwd)
ARCH=$(docker version --format '{{.Server.Arch}}')
echo "==> building linux/$ARCH loadgen into $OUT/bin"
(cd "$ROOT/harness" && GOOS=linux GOARCH=$ARCH CGO_ENABLED=0 go build -trimpath -o "$OUT/bin/" ./cmd/loadgen)
cp "$ROOT/harness/profiles/chat.yaml" "$OUT/chat.yaml"
git -C "$ROOT" rev-parse HEAD >"$OUT/git-sha.txt"
git -C "$ROOT" status --porcelain >"$OUT/git-status.txt"

SAMPLER=
trap '[ -n "$SAMPLER" ] && kill "$SAMPLER" 2>/dev/null || true' EXIT

# run NAME PATH RATE DURATION [sampler args...]
run() {
  local name=$1 path=$2 rate=$3 dur=$4
  shift 4
  local dir=$OUT/$name
  echo "==> $name: chat at $rate/s for $dur via $(target_url "$path")"
  mkdir -p "$dir"
  python3 "$HERE/probe.py" snapshot -out "$dir"
  local since
  since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  python3 "$HERE/balance.py" sample -out "$dir/samples" -metrics "$(metrics_url "$path")" -interval "$INTERVAL" "$@" &
  SAMPLER=$!
  docker run --rm --network "$NET" -v "$OUT:/bench" "$IMAGE" \
    /bench/bin/loadgen -profile /bench/chat.yaml -rate "$rate" -duration "$dur" -seed "$SEED" \
      -url "$(target_url "$path")" -label "$name" -out "/bench/$name/run" -max-inflight 20000 \
      >"$dir/loadgen.log" 2>&1
  sleep 1
  kill -TERM "$SAMPLER"
  wait "$SAMPLER" || true
  SAMPLER=
  docker logs --since "$since" "llm-synapse-gw-routers-gateway-$path-1" >"$dir/gateway.log" 2>&1
  python3 "$HERE/probe.py" stickiness -out "$dir" >/dev/null
  local rundir
  rundir=$(find "$dir/run" -mindepth 1 -maxdepth 1 -type d | head -1)
  python3 "$HERE/balance.py" analyze -dir "$dir/samples" -epsilon "$EPSILON" -slack "$SLACK" \
    -run "$rundir" -gateway-log "$dir/gateway.log" >/dev/null
  echo "    stickiness $(python3 -c "import json;print(json.load(open('$dir/stickiness.json'))['stickiness'])")," \
    "excursions>1s $(python3 -c "import json;print(json.load(open('$dir/samples/balance.json'))['excursions_over_1s'])")"
}

case $MODE in
balance)
  "${COMPOSE[@]}" up -d --build --wait --force-recreate "${SERVICES[@]}" >"$OUT/compose.log" 2>&1
  for rate in $RATES; do
    dur=$DURATION
    if [ "$rate" -lt 5 ]; then dur=$LOW_DURATION; fi
    run "rr-$rate" rr "$rate" "$dur"
    run "affinity-$rate" affinity "$rate" "$dur"
  done
  ;;
removal)
  for rate in $REMOVAL_RATES; do
    live=$OUT/live-config-$rate
    mkdir -p "$live"
    cp "$HERE/config/gateway.affinity.yaml" "$live/gateway.affinity.yaml"
    ROUTERS_LIVE_CONFIG=$live "${COMPOSE[@]}" "${LIVE[@]}" up -d --build --wait --force-recreate \
      "${SERVICES[@]}" >>"$OUT/compose.log" 2>&1
    run "removal-$rate" affinity "$rate" "$REMOVAL_DURATION" \
      -live-config "$live/gateway.affinity.yaml" -remove "$REMOVE" -remove-after "$REMOVE_AFTER"
  done
  # Back to the committed config.
  "${COMPOSE[@]}" up -d --wait --force-recreate gateway-affinity >>"$OUT/compose.log" 2>&1
  ;;
*)
  echo "unknown mode $MODE" >&2
  exit 2
  ;;
esac
echo "==> done: $OUT"
