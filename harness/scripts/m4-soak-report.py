#!/usr/bin/env python3
"""Checks an AC-M4-5 soak (harness/scripts/m4-soak.sh) and prints a Markdown
report: how long the load ran, status by class, the slow-reader rounds and
the Redis ceiling and TTL checks. The replay bound is checked by
m4-soak-sampler.py summarize; pass its exit code with --replay-bound-exit.

    m4-soak-report.py build/m4/soak --soak-seconds 3600 --replay-bound-exit 0

Exits 1 when a check fails. Redis used_memory above maxmemory is reported
with its largest overshoot, not failed on: Redis evicts only once an
allocation has taken it past the limit.
"""
import argparse
import collections
import glob
import json
import os
import sys

sys.dont_write_bytecode = True  # no __pycache__ in the source tree (root-owned from the tools container)
from outcomes import read_records, run  # noqa: E402


def results_rows(out):
    for p in glob.glob(os.path.join(out, "run", "*", "results.jsonl*")):
        yield from read_records(p)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("out")
    ap.add_argument("--soak-seconds", type=float, required=True)
    ap.add_argument("--replay-bound-exit", type=int, required=True)
    a = ap.parse_args()
    fails = []

    rows = list(results_rows(a.out))
    ran = max((r.get("start_ms", 0) / 1000 for r in rows), default=0.0)
    by_class = collections.defaultdict(collections.Counter)
    cut = 0
    for r in rows:
        status = r.get("status", 0)
        if r.get("error") == "canceled" and r.get("start_ms", 0) / 1000 >= ran - 120:
            cut += 1  # still running when the load generator stopped, before or after its headers
            continue
        if r["ok"]:
            by_class[r["class"]]["ok"] += 1
        elif 200 <= status < 300:
            by_class[r["class"]]["2xx_failed"] += 1  # failed after its headers: a stream error, a truncated response
        else:
            by_class[r["class"]][status] += 1
    print("# AC-M4-5 soak report\n")
    print(f"- Load ran {ran:.0f} s of {a.soak_seconds:.0f} s; {cut} requests still running at the end were cancelled by the load generator (not counted below).")
    if ran < 0.98 * a.soak_seconds:
        fails.append(f"load ran only {ran:.0f} s")
    print("\n| Class | Requests | OK | 503 | 2xx, failed after headers | Other |\n| --- | --- | --- | --- | --- | --- |")
    for c in sorted(by_class):
        n = sum(by_class[c].values())
        ok, shed, late = by_class[c]["ok"], by_class[c][503], by_class[c]["2xx_failed"]
        print(f"| {c} | {n} | {ok} | {shed} | {late} | {n - ok - shed - late} |")
        if n - ok - shed:
            fails.append(f"{c}: {n - ok - shed} responses neither successful (ADR 0021) nor a 503")

    good = shed = bad = 0
    for p in sorted(glob.glob(os.path.join(a.out, "slow", "round-*.json"))):
        if p.endswith(".body.json"):
            continue
        rs = json.load(open(p)).get("responses", [])
        fast = [x for x in rs if not x.get("slow")]
        slow = [x for x in rs if x.get("slow")]
        if rs and all(x.get("status") == 503 for x in rs):
            shed += 1
        elif (len(fast) == 2 and all(x.get("complete") for x in fast) and fast[0].get("sha256") == fast[1].get("sha256")
              and len(slow) == 1 and slow[0].get("error_event") and not slow[0].get("complete")):
            good += 1
        else:
            bad += 1
    print(f"\n- Slow-reader rounds: {good} as designed (two identical complete streams, the slow reader evicted with an error event), "
          f"{shed} shed whole with 503, {bad} otherwise.")
    if bad:
        fails.append(f"{bad} slow-reader rounds misbehaved")
    if good == 0:
        fails.append("no slow-reader round exercised eviction")

    red = []
    for line in open(os.path.join(a.out, "samples.jsonl")):
        r = json.loads(line).get("redis", {})
        if "used_memory" in r:
            red.append(r)
    if not red:
        fails.append("no Redis samples")
    else:
        over = [r["used_memory"] - r["maxmemory"] for r in red if r["used_memory"] > r["maxmemory"]]
        evicted = red[-1]["evicted_keys"] - red[0]["evicted_keys"]
        no_expiry = sum(r["keys"] != r["expires"] for r in red)
        no_ttl = sum(r.get("ttl_sample_without_ttl", 0) for r in red)
        print(f"- Redis: {len(red)} samples, max used_memory {max(r['used_memory'] for r in red)} B vs maxmemory "
              f"{red[0]['maxmemory']} B; {len(over)} samples above it (largest overshoot {max(over, default=0)} B); "
              f"{evicted} keys evicted; samples with keys != keys with expiry: {no_expiry}; sampled keys without a TTL: {no_ttl}.")
        if evicted == 0:
            fails.append("Redis never reached maxmemory (no evictions)")
        if no_expiry or no_ttl:
            fails.append("a Redis key without a TTL")

    print(f"- Replay bound (m4-soak-sampler.py summarize): {'held' if a.replay_bound_exit == 0 else 'EXCEEDED'}.")
    if a.replay_bound_exit:
        fails.append("replay bytes exceeded flights x bound")
    print("\n## Verdict\n")
    print("PASS" if not fails else "FAIL: " + "; ".join(fails))
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    run(main)
