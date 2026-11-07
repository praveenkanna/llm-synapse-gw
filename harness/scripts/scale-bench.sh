#!/usr/bin/env bash
# Scale run of ONE gateway instance against mockllm (AC-M0-6), with a
# same-session direct baseline. Proves overhead and connection scale only;
# mockllm says nothing about GPU or prefix-cache behaviour.
#
#   harness/scripts/scale-bench.sh [out_dir]
#
# Why a container: 5,000 streams need ~10,000 sockets in the gateway and
# ~20,000 across loadgen, gateway and mockllm, beyond macOS's per-process and
# system file limits. Everything runs in one Linux container (Docker's VM),
# talking over loopback, with nofile raised and a wide ephemeral port range.
# The container is capped (BENCH_CPUS, BENCH_MEMORY) so other containers on
# the same Docker host keep headroom; the three processes share those CPUs,
# so absolute latencies are pessimistic, and both sides of every comparison
# pay the same cost.
#
# Runs, in order (each writes a loadgen run directory under out_dir):
#   streams-direct   closed loop, STREAMS concurrent streams -> mockllm
#   streams-gateway  same through the gateway (CPU/RSS sampled from /proc)
#   nostream-direct  open loop, RPS req/s non-streaming -> mockllm
#   nostream-gateway same through the gateway
# Every run scrapes mockllm /metrics before and after, and records the
# container's cgroup CPU usage and throttling (<run>/cgroup-cpu.txt).
#
# Then write the results file with harness/cmd/report (command printed at the end).
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
OUT=${1:-$ROOT/harness/out/scale-$(date -u +%Y%m%dT%H%M%SZ)}
IMAGE=${BENCH_IMAGE:-alpine:3.22}
BENCH_CPUS=${BENCH_CPUS:-7}
BENCH_MEMORY=${BENCH_MEMORY:-2g}

# Load shape.
STREAMS=${STREAMS:-5000}          # concurrent open streams (closed loop)
STREAM_DURATION=${STREAM_DURATION:-75s}
RPS=${RPS:-1200}                  # non-streaming arrivals per second (open loop)
NOSTREAM_DURATION=${NOSTREAM_DURATION:-75s}
# mockllm timing: each stream lasts TTFT + tokens / TPS. Output lengths vary
# (MOCK_OUTPUT) so closed-loop workers do not restart in lockstep; OUT_TOKENS
# is the request's max_tokens and must cover the distribution.
TTFT=${TTFT:-50ms}
TPS=${TPS:-20}
MOCK_OUTPUT=${MOCK_OUTPUT:-uniform:100:300}
OUT_TOKENS=${OUT_TOKENS:-300}
NOSTREAM_TOKENS=${NOSTREAM_TOKENS:-16}
MAX_IDLE=${MAX_IDLE:-8192}        # gateway upstream.max_idle_conns_per_host
RUNS=${RUNS:-streams nostream}    # which direct/gateway pairs to run
GW_GOGC=${GW_GOGC:-}              # gateway GOGC override (empty: Go default)

mkdir -p "$OUT/bin"
OUT=$(cd "$OUT" && pwd) # docker -v needs an absolute path
ARCH=$(docker version --format '{{.Server.Arch}}')
echo "==> building linux/$ARCH binaries into $OUT/bin"
(cd "$ROOT/gateway" && GOOS=linux GOARCH=$ARCH CGO_ENABLED=0 go build -trimpath -o "$OUT/bin/" ./cmd/gateway ./cmd/mockllm)
(cd "$ROOT/harness" && GOOS=linux GOARCH=$ARCH CGO_ENABLED=0 go build -trimpath -o "$OUT/bin/" ./cmd/loadgen)

cat >"$OUT/gateway.yaml" <<EOF
# Scale bench: one gateway, one mockllm replica, loopback only.
server:
  listen_addr: "127.0.0.1:8080"
  metrics_listen_addr: "127.0.0.1:9091"
upstream:
  max_idle_conns_per_host: $MAX_IDLE
pools:
  - name: large
    replicas:
      - name: mockllm-0
        url: http://127.0.0.1:8001
models:
  large-model: large
auth:
  enabled: false
EOF

cat >"$OUT/bin/inner.sh" <<'EOF'
#!/bin/sh
set -eu
B=/bench/bin; O=/bench/out
ulimit -n 1048576
MOCKLLM_LISTEN_ADDR=127.0.0.1:8001 MOCKLLM_MODELS=large-model MOCKLLM_TTFT=$TTFT \
  MOCKLLM_TOKENS_PER_SECOND=$TPS MOCKLLM_OUTPUT_TOKENS=$MOCK_OUTPUT \
  "$B/mockllm" >"$O/mockllm.log" 2>&1 &
MOCK=$!
[ -n "$GW_GOGC" ] && export GOGC=$GW_GOGC
GATEWAY_CONFIG=$O/gateway.yaml "$B/gateway" >"$O/gateway.log" 2>&1 &
unset GOGC
GW=$!
trap 'kill $GW $MOCK 2>/dev/null || true' EXIT
for url in http://127.0.0.1:8001/health http://127.0.0.1:8080/livez; do
  i=0
  until wget -q -O /dev/null "$url"; do
    i=$((i + 1)); [ $i -lt 50 ] || { echo "not ready: $url"; exit 1; }; sleep 0.2
  done
done
# Whole-container CPU (cgroup v2) around each run: shows whether the shared
# CPUs, not the gateway, were the limit. Written to <run>/cgroup-cpu.txt.
cpu() { awk '/^(usage_usec|nr_throttled|throttled_usec) /{printf "%s=%s ", $1, $2}' /sys/fs/cgroup/cpu.stat; awk '{printf "uptime_s=%s\n", $1}' /proc/uptime; }
lg() {
  name=$1; shift
  echo "==> $name"
  before=$(cpu)
  "$B/loadgen" -model large-model -scrape http://127.0.0.1:8001 -max-inflight 20000 -out "$O/$name" "$@"
  after=$(cpu)
  printf 'before %s\nafter  %s\n' "$before" "$after" >"$O/$name/cgroup-cpu.txt"
}
S="-concurrency $STREAMS -duration $STREAM_DURATION -class interactive:1:stream:$OUT_TOKENS:64"
N="-rate $RPS -duration $NOSTREAM_DURATION -class structured:1:nostream:$NOSTREAM_TOKENS:64"
for run in $RUNS; do
  case $run in
    streams)  lg streams-direct  -label direct  -url http://127.0.0.1:8001 $S
              lg streams-gateway -label gateway -url http://127.0.0.1:8080 -watch-pid $GW $S ;;
    nostream) lg nostream-direct  -label direct  -url http://127.0.0.1:8001 $N
              lg nostream-gateway -label gateway -url http://127.0.0.1:8080 -watch-pid $GW $N ;;
    *) echo "unknown run $run"; exit 1 ;;
  esac
done
EOF
chmod +x "$OUT/bin/inner.sh"

echo "==> running in $IMAGE (cpus=$BENCH_CPUS memory=$BENCH_MEMORY)"
docker run --rm --name "llm-synapse-gw-scale-bench-$$" \
  --cpus "$BENCH_CPUS" --memory "$BENCH_MEMORY" \
  --ulimit nofile=1048576:1048576 \
  --sysctl net.ipv4.ip_local_port_range="1024 65000" \
  --sysctl net.ipv4.tcp_tw_reuse=1 \
  --sysctl net.core.somaxconn=65535 \
  -e STREAMS="$STREAMS" -e STREAM_DURATION="$STREAM_DURATION" -e RPS="$RPS" \
  -e NOSTREAM_DURATION="$NOSTREAM_DURATION" -e TTFT="$TTFT" -e TPS="$TPS" \
  -e OUT_TOKENS="$OUT_TOKENS" -e MOCK_OUTPUT="$MOCK_OUTPUT" -e NOSTREAM_TOKENS="$NOSTREAM_TOKENS" -e RUNS="$RUNS" -e GW_GOGC="$GW_GOGC" \
  -v "$OUT/bin:/bench/bin:ro" -v "$OUT:/bench/out" \
  "$IMAGE" /bench/bin/inner.sh

run_dir() { find "$OUT/$1" -mindepth 1 -maxdepth 1 -type d | head -1; }
cat <<EOF
==> done. Write the results file with (from harness/):
go run ./cmd/report -profile scale -load "$STREAMS streams, $RPS req/s" -warmup 15s \\
  -checks AC-M0-3,AC-M0-5,AC-M0-6 -upstream mockllm \\
  -config-diff "upstream.max_idle_conns_per_host: $MAX_IDLE; auth.enabled: false" \\
  -pools "large: 1 mockllm replica (TTFT $TTFT, $TPS tok/s, output $MOCK_OUTPUT tokens)" \\
  -pair streams=$(run_dir streams-direct),$(run_dir streams-gateway) \\
  -pair nostream=$(run_dir nostream-direct),$(run_dir nostream-gateway)
EOF
