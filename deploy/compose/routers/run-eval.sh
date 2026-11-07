#!/usr/bin/env bash
# M0-18 external router evaluation (ADR 0003): vLLM Router and SGLang Model
# Gateway behind the gateway, against the gateway's own round-robin, on the
# `chat` profile. mockllm upstreams: this measures conformance and added
# latency only, never prefix-cache or GPU behaviour.
#
#   deploy/compose/routers/run-eval.sh [out_dir]
#
# Steps (each writes under out_dir):
#   1. probe.py all                 forwarding, SSE, faults (header), disconnect, metrics
#   2. faults with the mocks failing every request (env), for routers that
#      drop X-Mock-* headers: 5xx before any token, then mid-stream drop
#   3. the same faults with retries disabled on both routers
#   4. chat-profile load, one path at a time, same mocks, same seed:
#      gateway-rr, gateway -> vllm-router, gateway -> sgl-gateway, at RATES;
#      plus vllm-router consistent_hash (no session header) at the first rate,
#      and each router alone (loadgen -> router, no gateway) at the last rate.
#      After each run: per-replica load and conversation stickiness from the
#      mocks' request captures, and router CPU/memory from docker stats.
#
# Knobs: RATES ("20 80"), DURATION (120s), WARMUP (20s, for the report), SEED (1).
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../../.." && pwd)
OUT=${1:-$ROOT/harness/out/m0-18-routers-$(date -u +%Y%m%dT%H%M%SZ)}
RATES=${RATES:-20 80}
DURATION=${DURATION:-120s}
SEED=${SEED:-1}
IMAGE=${BENCH_IMAGE:-alpine:3.22}
COMPOSE=(docker compose -f "$HERE/docker-compose.yml")
NET=llm-synapse-gw-routers_default
# Load runs use the configuration we would deploy: retries off (a retried
# generation is a silent retry, design-principles.md).
export VLLM_ROUTER_EXTRA=--disable-retries SGL_GATEWAY_EXTRA=--disable-retries

mkdir -p "$OUT/bin"
OUT=$(cd "$OUT" && pwd)
ARCH=$(docker version --format '{{.Server.Arch}}')
echo "==> building linux/$ARCH loadgen into $OUT/bin"
(cd "$ROOT/harness" && GOOS=linux GOARCH=$ARCH CGO_ENABLED=0 go build -trimpath -o "$OUT/bin/" ./cmd/loadgen)
cp "$ROOT/harness/profiles/chat.yaml" "$OUT/chat.yaml"

up() { "${COMPOSE[@]}" up -d --build --wait "$@" >>"$OUT/compose.log" 2>&1; }
# Fresh routers for each fault phase: an open circuit breaker from the
# previous phase would answer 503 without trying any worker.
fresh_routers() { up --force-recreate vllm-router sgl-gateway; }
SAMPLER=
trap '[ -n "$SAMPLER" ] && kill "$SAMPLER" 2>/dev/null || true' EXIT

echo "==> router defaults: probes"
VLLM_ROUTER_EXTRA= SGL_GATEWAY_EXTRA= up
python3 "$HERE/probe.py" all -out "$OUT/probe" >/dev/null
for mode in 5xx drop; do
  echo "==> router defaults: faults, every mock request fails ($mode)"
  if [ $mode = 5xx ]; then export MOCK_FAULT_5XX_RATE=1; else export MOCK_FAULT_DROP_RATE=1; fi
  VLLM_ROUTER_EXTRA= SGL_GATEWAY_EXTRA= up
  VLLM_ROUTER_EXTRA= SGL_GATEWAY_EXTRA= fresh_routers
  python3 "$HERE/probe.py" faults -fault-mode env-$mode -out "$OUT/probe" >/dev/null
  unset MOCK_FAULT_5XX_RATE MOCK_FAULT_DROP_RATE
done

echo "==> retries disabled: faults"
up
fresh_routers
python3 "$HERE/probe.py" faults -fault-mode header -label no-retries -out "$OUT/probe" >/dev/null
MOCK_FAULT_5XX_RATE=1 up
MOCK_FAULT_5XX_RATE=1 fresh_routers
python3 "$HERE/probe.py" faults -fault-mode env-5xx -label no-retries -out "$OUT/probe" >/dev/null
up  # faults off again
fresh_routers

# Sample CPU and memory of the router and gateway containers during a run.
sample() {
  while :; do
    docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' \
      llm-synapse-gw-routers-vllm-router-1 llm-synapse-gw-routers-sgl-gateway-1 \
      llm-synapse-gw-routers-gateway-rr-1 llm-synapse-gw-routers-gateway-vllm-router-1 \
      llm-synapse-gw-routers-gateway-sgl-gateway-1 2>/dev/null | sed "s/^/$(date +%s) /"
    sleep 5
  done
}

run() { # name base-url rate
  local name=$1 url=$2 rate=$3
  echo "==> load $name: rate $rate/s for $DURATION via $url"
  mkdir -p "$OUT/$name"
  python3 "$HERE/probe.py" snapshot -out "$OUT/$name"
  sample >"$OUT/stats-$name.txt" &
  SAMPLER=$!
  docker run --rm --network "$NET" -v "$OUT:/bench" "$IMAGE" \
    /bench/bin/loadgen -profile /bench/chat.yaml -rate "$rate" -duration "$DURATION" -seed "$SEED" \
      -url "$url" -label "$name" -out "/bench/$name" -max-inflight 20000 \
      -scrape http://mock-0:8000 -scrape http://mock-1:8000 -scrape http://mock-2:8000 \
      >"$OUT/$name.log" 2>&1
  kill "$SAMPLER" 2>/dev/null || true
  wait "$SAMPLER" 2>/dev/null || true
  SAMPLER=
  python3 "$HERE/probe.py" stickiness -out "$OUT/$name" >/dev/null
}

for rate in $RATES; do
  run rr-$rate http://gateway-rr:8080 "$rate"
  run vllm-router-$rate http://gateway-vllm-router:8080 "$rate"
  run sgl-gateway-$rate http://gateway-sgl-gateway:8080 "$rate"
done
first=${RATES%% *}
last=${RATES##* }
VLLM_ROUTER_POLICY=consistent_hash up
run vllm-router-chash-$first http://gateway-vllm-router:8080 "$first"
up  # back to cache_aware
# Router alone (loadgen -> router, no gateway) at the highest rate: shows
# whether added latency belongs to the router or to the gateway->router hop.
run alone-vllm-router-$last http://vllm-router:8000 "$last"
run alone-sgl-gateway-$last http://sgl-gateway:8000 "$last"

run_dir() { find "$OUT/$1" -mindepth 1 -maxdepth 1 -type d -name '*-seed*' | head -1; }
echo "==> done: $OUT"
echo "Pairs for harness/cmd/report (baseline gateway round-robin):"
for rate in $RATES; do
  echo "  -pair vllm-router-$rate=$(run_dir rr-$rate),$(run_dir vllm-router-$rate)"
  echo "  -pair sgl-gateway-$rate=$(run_dir rr-$rate),$(run_dir sgl-gateway-$rate)"
done
echo "  -pair vllm-router-chash-$first=$(run_dir rr-$first),$(run_dir vllm-router-chash-$first)"
echo "  -pair alone-vllm-router-$last=$(run_dir rr-$last),$(run_dir alone-vllm-router-$last)"
echo "  -pair alone-sgl-gateway-$last=$(run_dir rr-$last),$(run_dir alone-sgl-gateway-$last)"
