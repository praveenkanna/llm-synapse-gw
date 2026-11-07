#!/usr/bin/env python3
"""Client-side latency of a loadgen run split by class and cache outcome.

Reads results.jsonl and groups successful requests by class and the
X-Gateway-Cache header (recorded as "cache": "hit"; absent = miss or not
cached). Prints count, p50/p95/p99/max of the whole response time
(duration_ms) and of TTFT (ttft_ms), plus error counts per class.

  harness/scripts/cache-latency-stats.py <run>/results.jsonl [--skip-first-s 0] [--json]
"""
import argparse
import json
import sys
from collections import defaultdict

sys.dont_write_bytecode = True  # no __pycache__ in the source tree (root-owned from the tools container)
from outcomes import pct, read_records, run  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("results")
    ap.add_argument("--skip-first-s", type=float, default=0, help="leave out requests started in the first N seconds")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()
    groups = defaultdict(lambda: {"duration": [], "ttft": []})
    errors = defaultdict(lambda: defaultdict(int))
    total = defaultdict(int)
    for r in read_records(a.results):
        if r["start_ms"] < a.skip_first_s * 1000:
            continue
        total[r["class"]] += 1
        if not r["ok"]:
            errors[r["class"]][r.get("error") or str(r["status"])] += 1
            continue
        g = groups[(r["class"], r.get("cache") or "miss")]
        g["duration"].append(r["duration_ms"])
        g["ttft"].append(r["ttft_ms"])
    out = []
    for (cls, cache), g in sorted(groups.items()):
        d, t = g["duration"], g["ttft"]
        out.append({"class": cls, "cache": cache, "n": len(d),
                    "duration_ms": {"p50": pct(d, 0.50), "p95": pct(d, 0.95), "p99": pct(d, 0.99), "max": max(d)},
                    "ttft_ms": {"p50": pct(t, 0.50), "p95": pct(t, 0.95), "p99": pct(t, 0.99), "max": max(t)}})
    if a.json:
        print(json.dumps({"groups": out, "requests": total, "errors": errors}, indent=1))
        return
    print(f"{'class':14s} {'cache':5s} {'n':>6s}  {'dur p50':>8s} {'p95':>8s} {'p99':>8s} {'max':>8s}   {'ttft p50':>8s} {'p95':>8s} {'p99':>8s}")
    for o in out:
        d, t = o["duration_ms"], o["ttft_ms"]
        print(f"{o['class']:14s} {o['cache']:5s} {o['n']:6d}  {d['p50']:8.2f} {d['p95']:8.2f} {d['p99']:8.2f} {d['max']:8.2f}   {t['p50']:8.2f} {t['p95']:8.2f} {t['p99']:8.2f}")
    for cls in sorted(total):
        errs = dict(errors[cls])
        print(f"{cls}: {total[cls]} requests, errors {errs if errs else 0}")


if __name__ == "__main__":
    run(main)
