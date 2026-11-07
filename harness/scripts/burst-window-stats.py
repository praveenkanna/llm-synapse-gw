#!/usr/bin/env python3
"""Per-class latency and status stats from a loadgen results.jsonl, split by
whether each request was scheduled inside a burst window.

Each record says whether it was (in_burst, ADR 0021). A run without bursts
(run.json beside the results has none) gives one window, "all". Requests
that start before WARMUP are left out.

    harness/scripts/burst-window-stats.py RESULTS.jsonl [--warmup 10]

Prints one line per class and window: requests, statuses, how many answered
2xx but failed after it (a stream error, a truncated response), TTFT
p50/p95/p99 and max over successful requests, and the slowest answer per
non-2xx status (time from send to the full error response, i.e. how fast a
refusal came back).
"""
import argparse
import collections
import json
import os
import sys

sys.dont_write_bytecode = True  # no __pycache__ in the source tree (root-owned from the tools container)
from outcomes import pct, read_records, run  # noqa: E402


def had_bursts(results, rows):
    """Whether the run had burst windows: run.json beside the results says;
    without one, whether any record of the run was in one."""
    try:
        with open(os.path.join(os.path.dirname(results), "run.json")) as f:
            return json.load(f)["run"]["burst"].get("every_ns", 0) > 0
    except (OSError, KeyError, TypeError, ValueError):
        return any(r["in_burst"] for r in rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("results")
    ap.add_argument("--warmup", type=float, default=10, help="leave out requests started before this, seconds")
    ap.add_argument("--json", action="store_true", help="print JSON instead of a table")
    a = ap.parse_args()

    every = list(read_records(a.results))
    bursts = had_bursts(a.results, every)
    rows = [r for r in every if r["start_ms"] / 1000 >= a.warmup]

    def window(r):
        if not bursts:
            return "all"
        return "burst" if r["in_burst"] else "steady"

    groups = collections.defaultdict(list)
    for r in rows:
        groups[(r["class"], window(r))].append(r)

    out = []
    for (cls, win), rs in sorted(groups.items()):
        ok = [r for r in rs if r["ok"]]
        ttft = [r["ttft_ms"] for r in ok]
        statuses = collections.Counter(r["status"] for r in rs)
        failed_after_2xx = sum(1 for r in rs if 200 <= r["status"] < 300 and not r["ok"])
        refusals = {}
        for r in rs:
            if not 200 <= r["status"] < 300:
                refusals[r["status"]] = max(refusals.get(r["status"], 0), r["duration_ms"])
        out.append({
            "class": cls, "window": win, "requests": len(rs), "ok": len(ok),
            "statuses": dict(sorted(statuses.items())), "failed_after_2xx": failed_after_2xx,
            "ttft_p50_ms": pct(ttft, 0.50), "ttft_p95_ms": pct(ttft, 0.95),
            "ttft_p99_ms": pct(ttft, 0.99), "ttft_max_ms": max(ttft) if ttft else float("nan"),
            "slowest_refusal_ms": dict(sorted(refusals.items())),
        })
    if a.json:
        print(json.dumps(out, indent=2))
        return
    print(f"{'class':>11} {'window':>7} {'reqs':>6} {'ok':>6} {'2xx-fail':>8}  {'TTFT p50':>9} {'p95':>9} {'p99':>9} {'max':>9}  statuses / slowest refusal ms")
    for o in out:
        print(f"{o['class']:>11} {o['window']:>7} {o['requests']:>6} {o['ok']:>6} {o['failed_after_2xx']:>8}  {o['ttft_p50_ms']:9.1f} {o['ttft_p95_ms']:9.1f} "
              f"{o['ttft_p99_ms']:9.1f} {o['ttft_max_ms']:9.1f}  {o['statuses']} {o['slowest_refusal_ms']}")


if __name__ == "__main__":
    run(main)
