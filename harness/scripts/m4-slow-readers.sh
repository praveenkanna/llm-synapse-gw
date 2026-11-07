#!/usr/bin/env bash
# Slow coalescing followers for the AC-M4-5 soak: every PERIOD seconds, for
# DURATION seconds, send 3 identical long streams of the m4-faq-coalesce
# template to one gateway (alternating): a fast leader, a fast follower and
# a follower that reads at 2 KiB/s, which falls behind the replay bound and
# must be evicted. Each round uses its own trail section, so it never merges
# with the profile's traffic. Run on the Compose network:
#
#   harness/scripts/m4-slow-readers.sh build/m4/bin/burst build/m4/soak/slow 3600 60
set -euo pipefail

burst=${1:?usage: m4-slow-readers.sh <burst> <outdir> [duration_s] [period_s]}
out=${2:?usage: m4-slow-readers.sh <burst> <outdir> [duration_s] [period_s]}
duration=${3:-3600}
period=${4:-60}
gateways=(gateway-1 gateway-2)
system='You answer questions about the trail network of the invented Larkspur Valley park. Give opening status, surface conditions and the nearest shelter in three short sentences. Never mention individual visitors.'

mkdir -p "$out"
start=$(date +%s)
i=0
while (( $(date +%s) - start < duration )); do
  gw=${gateways[i % 2]}
  section=$((90000 + i))
  body="$out/round-$i.body.json"
  printf '{"model":"large-model","temperature":0,"max_tokens":2048,"stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"system","content":"%s"},{"role":"user","content":"What are the conditions on trail section %d today?"}]}' \
    "$system" "$section" >"$body"
  "$burst" -url "http://$gw:8080" -n 3 -lead-delay 300ms -slow-readers 1 -slow-rate 2048 -body-file "$body" \
    -H 'X-Template-Id: m4-faq-coalesce' -H 'X-Template-Version: 1' -H 'X-Mock-Output-Tokens: 2000' \
    -out "$out/round-$i.json" >"$out/round-$i.log" 2>&1 &
  i=$((i + 1))
  sleep "$period"
done
wait
