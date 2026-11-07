#!/bin/sh
# AC-M4-5 soak: the dev stack at 200% load for an hour, with the response
# cache on, a 16 KiB replay bound and a 32 MiB Redis, so sealing, eviction
# and Redis's maxmemory are all reached. POSIX sh: it runs on a CI runner
# (.github/workflows/soak.yml) and on a workstation.
#
#   (cd harness && CGO_ENABLED=0 go build -o ../build/m4/bin/ ./cmd/loadgen ./cmd/burst)
#   make compose-env
#   sh harness/scripts/m4-soak.sh    # from the repo root
#
# The binaries must be built for the tools container (linux, its arch).
# Writes build/m4/soak/: samples.jsonl, docker-stats.jsonl, run/, slow/,
# summary.txt and report.md. Exits non-zero when a bound or check fails.
# Env: SOAK_SECONDS (3600), TOOLS_IMAGE, COMPOSE, UP_FLAGS ("--build";
# "--no-build" when the images are already built).
set -eu

SOAK_SECONDS=${SOAK_SECONDS:-3600}
TOOLS_IMAGE=${TOOLS_IMAGE:-golang:1.26.8-bookworm}
COMPOSE=${COMPOSE:-docker compose -f deploy/compose/docker-compose.yml}
UP_FLAGS=${UP_FLAGS:---build}
REPLAY_BYTES=16384
WS=$(pwd)
OUT=build/m4/soak
TOOLS=m4-tools

[ -x build/m4/bin/loadgen ] && [ -x build/m4/bin/burst ] || { echo "build build/m4/bin/{loadgen,burst} first" >&2; exit 2; }
[ -f deploy/compose/gateway.env ] || { echo "deploy/compose/gateway.env missing: run make compose-env" >&2; exit 2; }
rm -rf "$OUT"
mkdir -p "$OUT"

# Soak-only overrides, appended so they win; removed again on exit. The
# backup holds the admin tokens, so it stays out of the archived $OUT.
ENV_BACKUP=build/m4/gateway.env.orig
cp deploy/compose/gateway.env "$ENV_BACKUP"
cat >>deploy/compose/gateway.env <<EOF
GATEWAY_CACHE_ENABLED=true
GATEWAY_CACHE_REDIS_ADDR=redis:6379
GATEWAY_COALESCING_MAX_REPLAY_BYTES=$REPLAY_BYTES
GATEWAY_CACHE_MAX_ENTRY_BYTES=$REPLAY_BYTES
EOF
cleanup() {
  docker rm -f "$TOOLS" >/dev/null 2>&1 || true
  cp "$ENV_BACKUP" deploy/compose/gateway.env && rm -f "$ENV_BACKUP"
}
trap cleanup EXIT

$COMPOSE --profile dev down -v --remove-orphans >/dev/null 2>&1 || true
# shellcheck disable=SC2086
REDIS_MAXMEMORY=32mb $COMPOSE --profile dev up -d $UP_FLAGS --wait
{
  echo "start: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "git: ${GIT_COMMIT:-$(git rev-parse HEAD 2>/dev/null || echo unknown)}"
  echo "soak_seconds: $SOAK_SECONDS  replay_bytes: $REPLAY_BYTES  redis_maxmemory: 32mb"
  echo "host: $(uname -a)"
  docker info --format 'engine: {{.ServerVersion}}  cpus: {{.NCPU}}  mem: {{.MemTotal}}' 2>/dev/null || true
} | tee "$OUT/start.txt"

docker rm -f "$TOOLS" >/dev/null 2>&1 || true
docker run -d --name "$TOOLS" --network llm-synapse-gw_default -v "$WS:$WS" -w "$WS" "$TOOLS_IMAGE" sleep infinity >/dev/null

token=$(sed -n 's/^GATEWAY_ADMIN_TOKEN_PROMPT_OWNERS=//p' deploy/compose/gateway.env | tail -n 1)
for t in m4-faq-coalesce m4-faq-cache; do
  i=0
  until docker exec -e GATEWAY_ADMIN_TOKEN="$token" "$TOOLS" bash harness/scripts/register-template.sh "$t" "harness/profiles/templates/$t.json"; do
    i=$((i + 1)); [ "$i" -lt 30 ] || exit 1; sleep 2
  done
done
sleep 5 # the gateways' registry refresh picks up the active versions

# The sampler starts first so its first samples show the idle stack.
docker exec "$TOOLS" python3 harness/scripts/m4-soak-sampler.py sample \
  --out "$OUT/samples.jsonl" --duration $((SOAK_SECONDS + 150)) &
sampler=$!
(
  end=$(($(date +%s) + SOAK_SECONDS + 60))
  while [ "$(date +%s)" -lt "$end" ]; do
    t=$(date +%s)
    docker stats --no-stream --format '{{json .}}' llm-synapse-gw-gateway-1-1 llm-synapse-gw-gateway-2-1 \
      llm-synapse-gw-redis-1 llm-synapse-gw-mockllm-1 llm-synapse-gw-nginx-1 2>/dev/null \
      | sed "s/^{/{\"unix\":$t,/" >>"$OUT/docker-stats.jsonl" || true
    sleep 10
  done
) &
stats=$!
sleep 20
docker exec "$TOOLS" bash harness/scripts/m4-slow-readers.sh build/m4/bin/burst "$OUT/slow" "$SOAK_SECONDS" 60 &
slow=$!

rc=0
docker exec -w "$WS/harness" "$TOOLS" ../build/m4/bin/loadgen -label soak-nginx -url http://nginx:80 \
  -profile profiles/m4-soak.yaml -duration "${SOAK_SECONDS}s" -seed 1 -scrape http://mockllm:8000/metrics \
  -out "../$OUT/run" >"$OUT/loadgen.log" 2>&1 || rc=$?
echo "loadgen exit $rc" | tee -a "$OUT/start.txt"
wait "$slow" || true
wait "$sampler" || true
wait "$stats" || true
echo "end: $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$OUT/start.txt"
[ -s "$OUT/docker-stats.jsonl" ] || echo "docker stats: no samples (unsupported by this engine)" | tee -a "$OUT/start.txt"

summ=0
docker exec "$TOOLS" python3 harness/scripts/m4-soak-sampler.py summarize "$OUT/samples.jsonl" \
  --replay-bytes "$REPLAY_BYTES" --docker-stats "$OUT/docker-stats.jsonl" >"$OUT/summary.txt" 2>&1 || summ=$?
cat "$OUT/summary.txt"
rep=0
docker exec "$TOOLS" python3 harness/scripts/m4-soak-report.py "$OUT" --soak-seconds "$SOAK_SECONDS" \
  --replay-bound-exit "$summ" >"$OUT/report.md" 2>&1 || rep=$?
cat "$OUT/report.md"
# The tools container wrote these files (as its own user), so it compresses them.
docker exec "$TOOLS" sh -c "gzip -f $OUT/run/*/results.jsonl" 2>/dev/null || true
[ "$rc" -eq 0 ] && [ "$summ" -eq 0 ] && [ "$rep" -eq 0 ]
