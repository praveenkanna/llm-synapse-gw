#!/usr/bin/env python3
"""Load-balance and replica-removal evidence for M1 (AC-M1-3, AC-M1-4).

Runs against the stack in deploy/compose/routers/docker-compose.yml (host
ports on 127.0.0.1). Standard library only. Reads only metrics, counters and
the gateway's per-request log lines (IDs, replica, status); never a prompt or
a response.

  balance.py sample  -out DIR [-metrics URL] [-interval 0.1]
                     [-live-config FILE -remove REPLICA -remove-after SECONDS]
      Until SIGTERM/SIGINT: every interval, scrape gateway_inflight{pool,
      replica,class} from the gateway's /metrics and each mock's request
      counter (/mock/stats), one JSON line per sample in DIR/samples.jsonl.
      Saves the full /metrics text at start and stop. With -live-config, at
      -remove-after seconds the replica is dropped from that config file
      (atomic rename) and the time is recorded in DIR/events.json.

  balance.py analyze -dir DIR [-epsilon 0.25] [-slack 1] [-bucket 10]
                     [-run LOADGEN_RUN_DIR] [-gateway-log FILE]
      AC-M1-3: every sample inside the placement window (first to last new
      request on any mock) is checked against bound = ceil((1+eps)*avg)+slack,
      avg = total gateway in-flight / active replicas. Reports excursions
      (contiguous out-of-bound samples), their length and size, plus a
      bucketed time series. With a removal event (AC-M1-4) the removed
      replica leaves the active set at the reload, and the report adds the
      reload time, the last new request on the removed replica, when the
      remaining replicas are back in bound, and failed requests (matched to
      replicas through the gateway's log by trace ID when -gateway-log and
      -run are given). Writes DIR/balance.json and DIR/balance.md.

  balance.py failopen -out DIR [-n 30]
      AC-M1-6 live check: N requests with an unparseable body and N with no
      affinity key (no messages or prompt) to gateway-affinity and to mock-0
      directly. Reports statuses on both paths, how the gateway spread them
      over the mocks, and the fail-open and route-decision counter deltas.
"""

import argparse
import calendar
import json
import math
import os
import re
import signal
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

HOST = "127.0.0.1"
MOCKS = {"mock-0": 18100, "mock-1": 18101, "mock-2": 18102}
DEFAULT_METRICS = f"http://{HOST}:18143/metrics"  # gateway-affinity
INFLIGHT_RE = re.compile(r"^gateway_inflight\{([^}]*)\}\s+(\S+)")
LABEL_RE = re.compile(r'(\w+)="((?:[^"\\]|\\.)*)"')
FETCH_TIMEOUT = 2.0


# ---------------------------------------------------------------- sampling

def fetch(url):
    with urllib.request.urlopen(url, timeout=FETCH_TIMEOUT) as resp:
        return resp.read().decode()


def parse_inflight(text):
    """Per-replica gateway in-flight, summed over class (and pool)."""
    out = {}
    for line in text.splitlines():
        m = INFLIGHT_RE.match(line)
        if not m:
            continue
        labels = dict(LABEL_RE.findall(m.group(1)))
        rep = labels.get("replica", "")
        if rep:
            out[rep] = out.get(rep, 0) + int(float(m.group(2)))
    return out


def drop_replica(path, replica):
    """Rewrite the config without the replica's two lines (name, url).
    The file layout is the committed gateway.affinity.yaml's."""
    with open(path) as f:
        lines = f.readlines()
    out, skip = [], 0
    for line in lines:
        if skip:
            skip -= 1
            continue
        if line.strip() == f"- name: {replica}":
            skip = 1  # the url line that follows
            continue
        out.append(line)
    if len(out) != len(lines) - 2:
        raise SystemExit(f"replica {replica} not found as a two-line entry in {path}")
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        f.writelines(out)
    os.replace(tmp, path)


def sample(args):
    os.makedirs(args.out, exist_ok=True)
    stop = {"now": False}

    def on_signal(*_):
        stop["now"] = True

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    with open(os.path.join(args.out, "metrics-before.txt"), "w") as f:
        f.write(fetch(args.metrics))
    events = {"start_unix": time.time(), "interval_s": args.interval}
    urls = {"_gw": args.metrics, **{m: f"http://{HOST}:{p}/mock/stats" for m, p in MOCKS.items()}}
    pool = ThreadPoolExecutor(max_workers=len(urls))
    t0 = time.monotonic()
    removed = False
    errors = 0
    with open(os.path.join(args.out, "samples.jsonl"), "w") as out:
        nxt = t0
        while not stop["now"]:
            now = time.monotonic()
            if args.live_config and not removed and now - t0 >= args.remove_after:
                drop_replica(args.live_config, args.remove)
                events["removal"] = {"replica": args.remove, "t": round(time.monotonic() - t0, 3),
                                     "unix": time.time()}
                removed = True
            ts = time.monotonic() - t0
            futs = {k: pool.submit(fetch, u) for k, u in urls.items()}
            row = {"t": round(ts, 3)}
            try:
                row["inflight"] = parse_inflight(futs["_gw"].result())
                row["mock_requests"] = {m: json.loads(futs[m].result())["requests"] for m in MOCKS}
            except (OSError, ValueError, KeyError) as err:
                errors += 1
                row = {"t": round(ts, 3), "error": type(err).__name__}
            out.write(json.dumps(row) + "\n")
            nxt += args.interval
            time.sleep(max(0.0, nxt - time.monotonic()))
    events["stop_unix"] = time.time()
    events["sample_errors"] = errors
    with open(os.path.join(args.out, "metrics-after.txt"), "w") as f:
        f.write(fetch(args.metrics))
    with open(os.path.join(args.out, "events.json"), "w") as f:
        json.dump(events, f, indent=2)
    pool.shutdown()


# ---------------------------------------------------------------- analysis

def load_samples(d):
    rows = []
    with open(os.path.join(d, "samples.jsonl")) as f:
        for line in f:
            r = json.loads(line)
            if "error" not in r:
                rows.append(r)
    return rows


def counters(text, name):
    """{label-string: value} for one metric family's samples."""
    out = {}
    for line in text.splitlines():
        if line.startswith(name + "{") or line.startswith(name + " "):
            key, _, val = line.rpartition(" ")
            out[key[len(name):]] = float(val)
    return out


def delta(d, name):
    before = counters(open(os.path.join(d, "metrics-before.txt")).read(), name)
    after = counters(open(os.path.join(d, "metrics-after.txt")).read(), name)
    return {k: after[k] - before.get(k, 0.0) for k in after if after[k] - before.get(k, 0.0)}


def router_p99_us(d):
    """p99 of gateway_router_duration_seconds over the run, as the upper edge
    of the histogram bucket holding it (a bound, not an estimate)."""
    buckets = {}
    for k, v in delta(d, "gateway_router_duration_seconds_bucket").items():
        m = re.search(r'le="([^"]+)"', k)
        pol = re.search(r'policy="([^"]+)"', k)
        if m and pol:
            buckets.setdefault(pol.group(1), {})
            le = float("inf") if m.group(1) == "+Inf" else float(m.group(1))
            buckets[pol.group(1)][le] = buckets[pol.group(1)].get(le, 0) + v
    out = {}
    for pol, b in buckets.items():
        total = b.get(float("inf"), 0)
        if not total:
            continue
        for le in sorted(b):
            if b[le] >= 0.99 * total:
                out[pol] = {"decisions": int(total), "p99_le_us": None if le == float("inf") else round(le * 1e6, 1)}
                break
    return out


def bound_of(active_total, n, eps, slack):
    return math.ceil((1 + eps) * active_total / n) + slack if n else 0


def analyze(args):
    d = args.dir
    rows = load_samples(d)
    events = json.load(open(os.path.join(d, "events.json")))
    removal = events.get("removal")
    replicas = sorted(MOCKS)
    reload_t = None
    log = parse_gateway_log(args.gateway_log) if args.gateway_log else None
    if removal:
        reload_t = removal["t"]
        if log and log["reloads"]:
            # Reload logged by the gateway, on the host's sample clock.
            after = [u for u in log["reloads"] if u >= removal["unix"] - 0.5]
            if after:
                reload_t = round(after[0] - events["start_unix"], 3)

    # Placement window: first to last sample interval with a new mock request.
    new = []
    for prev, cur in zip(rows, rows[1:]):
        new.append({m: cur["mock_requests"][m] - prev["mock_requests"][m] for m in replicas})
    active_idx = [i + 1 for i, n in enumerate(new) if sum(n.values()) > 0]
    if not active_idx:
        raise SystemExit("no placements observed")
    lo, hi = active_idx[0], active_idx[-1]

    def active_set(t):
        if removal and reload_t is not None and t >= reload_t:
            return [r for r in replicas if r != removal["replica"]]
        return replicas

    checked = []  # (t, excess, worst replica, bound, values)
    for r in rows[lo:hi + 1]:
        act = active_set(r["t"])
        vals = {m: r["inflight"].get(m, 0) for m in replicas}
        tot = sum(vals[m] for m in act)
        b = bound_of(tot, len(act), args.epsilon, args.slack)
        worst = max(act, key=lambda m: vals[m])
        checked.append({"t": r["t"], "excess": vals[worst] - b, "worst": worst, "bound": b,
                        "total": tot, "vals": vals})

    # Excursions: runs of out-of-bound samples; length until the next in-bound sample.
    exc, cur = [], None
    for i, c in enumerate(checked):
        if c["excess"] > 0:
            if cur is None:
                cur = {"start": c["t"], "max_excess": c["excess"], "replica": c["worst"], "samples": 0}
            cur["samples"] += 1
            cur["max_excess"] = max(cur["max_excess"], c["excess"])
        elif cur is not None:
            cur["end"] = c["t"]
            exc.append(cur)
            cur = None
    if cur is not None:
        cur["end"] = checked[-1]["t"]
        exc.append(cur)
    for e in exc:
        e["seconds"] = round(e["end"] - e["start"], 3)
    window = checked[-1]["t"] - checked[0]["t"]
    out_time = sum(e["seconds"] for e in exc)
    longest = max(exc, key=lambda e: e["seconds"]) if exc else None

    # Bucketed time series.
    buckets = []
    start = checked[0]["t"]
    for c in checked:
        k = int((c["t"] - start) // args.bucket)
        while len(buckets) <= k:
            buckets.append([])
        buckets[k].append(c)
    series = []
    for k, cs in enumerate(buckets):
        if not cs:
            continue
        series.append({
            "from_s": round(k * args.bucket),
            "samples": len(cs),
            "mean_total": round(sum(c["total"] for c in cs) / len(cs), 1),
            "mean_bound": round(sum(c["bound"] for c in cs) / len(cs), 1),
            "max_per_replica": {m: max(c["vals"][m] for c in cs) for m in replicas},
            "max_excess": max(c["excess"] for c in cs),
            "out_of_bound_share": round(sum(c["excess"] > 0 for c in cs) / len(cs), 3),
        })

    res = {
        "epsilon": args.epsilon, "slack": args.slack,
        "samples_checked": len(checked),
        "interval_s": events["interval_s"],
        "sample_errors": events.get("sample_errors", 0),
        "placement_window_s": round(window, 1),
        "mean_total_inflight": round(sum(c["total"] for c in checked) / len(checked), 1),
        "excursions": len(exc),
        "excursions_over_1s": sum(e["seconds"] > 1.0 for e in exc),
        "longest_excursion": longest,
        "max_excess": max((e["max_excess"] for e in exc), default=0),
        "out_of_bound_time_share": round(out_time / window, 4) if window else 0,
        "series": series,
        "route_decisions": delta(d, "gateway_route_decisions_total"),
        "requests": delta(d, "gateway_requests_total"),
        "retries": delta(d, "gateway_retries_total"),
        "failopen": delta(d, "gateway_failopen_total"),
        "ejections": delta(d, "gateway_replica_ejections_total"),
        "router_p99": router_p99_us(d),
    }
    if log:
        res["from_gateway_log"] = log_balance(log, args.epsilon, args.slack, removal)
    if removal:
        res["removal"] = removal_report(rows, removal, reload_t, events, args, log)
    with open(os.path.join(d, "balance.json"), "w") as f:
        json.dump(res, f, indent=2)
    md = markdown(res)
    with open(os.path.join(d, "balance.md"), "w") as f:
        f.write(md)
    print(md)


def parse_gateway_log(path):
    """Reload times and per-request records (trace ID, replica, status,
    outcome, end time and duration) from the gateway's JSON log. Only IDs,
    names, statuses and timings are read."""
    reloads, reqs, spans = [], {}, []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                e = json.loads(line)
            except ValueError:
                continue
            if e.get("msg") == "replica set reloaded":
                reloads.append(parse_time(e["time"]))
            elif "trace_id" in e and "replica" in e:
                reqs[e["trace_id"]] = {"replica": e.get("replica"), "status": e.get("status"),
                                       "outcome": e.get("outcome", "")}
                if e.get("replica") and e.get("path", "").startswith("/v1/") and "duration_ms" in e:
                    end = parse_time(e["time"])
                    spans.append((end - e["duration_ms"] / 1000.0, end, e["replica"]))
    return {"reloads": sorted(reloads), "requests": reqs, "spans": spans}


def log_balance(log, eps, slack, removal=None):
    """AC-M1-3 from the gateway's own per-request spans (start = log time -
    duration): the exact per-replica in-flight at every placement and between
    them. "Placed over bound": the replica a request was placed on is above
    ceil((1+eps)*avg)+slack right after the placement (avg over the active
    replicas, including the new request). Excursions: any active replica above
    the bound, with their exact length, and whether a placement landed on the
    replica that was over the bound while it was over (zero means the
    excursion came from requests finishing elsewhere, not from routing)."""
    spans = log["spans"]
    if not spans:
        return None
    reload_at = None
    if removal and log["reloads"]:
        later = [u for u in log["reloads"] if u >= removal["unix"] - 0.5]
        reload_at = later[0] if later else None
    events = sorted([(s, 1, r) for s, _, r in spans] + [(e, -1, r) for _, e, r in spans],
                    key=lambda x: (x[0], x[1]))  # ends before starts at the same instant
    first, last = min(s for s, _, _ in spans), max(s for s, _, _ in spans)
    reps = sorted({r for _, _, r in spans} | set(MOCKS))
    cnt = {r: 0 for r in reps}
    placed = placed_over = any_over_at_placement = 0
    max_placed_excess = None
    exc, cur = [], None

    def active(t):
        if removal and reload_at is not None and t >= reload_at:
            return [r for r in reps if r != removal["replica"]]
        return reps

    for t, d, r in events:
        cnt[r] += d
        if t < first or t > last:
            continue
        act = active(t)
        b = bound_of(sum(cnt[m] for m in act), len(act), eps, slack)
        over = {m: cnt[m] - b for m in act if cnt[m] > b}
        if d == 1 and r in act:
            placed += 1
            ex = cnt[r] - b
            max_placed_excess = ex if max_placed_excess is None else max(max_placed_excess, ex)
            placed_over += ex > 0
            any_over_at_placement += bool(over)
        if over and cur is None:
            cur = {"start": t, "max_excess": 0, "replicas": set(), "placements_on_over_replica": 0}
        if cur is not None:
            if over:
                cur["max_excess"] = max(cur["max_excess"], max(over.values()))
                cur["replicas"] |= set(over)
                if d == 1 and r in over:
                    cur["placements_on_over_replica"] += 1
            else:
                cur["seconds"] = round(t - cur["start"], 3)
                exc.append(cur)
                cur = None
    if cur is not None:
        cur["seconds"] = round(last - cur["start"], 3)
        exc.append(cur)
    window = last - first
    for e in exc:
        e["start_s"] = round(e.pop("start") - first, 3)
        e["replicas"] = sorted(e["replicas"])
    longest = max(exc, key=lambda e: e["seconds"]) if exc else None
    over_1s = [e for e in exc if e["seconds"] > 1.0]
    return {
        "requests": len(spans),
        "placement_window_s": round(window, 1),
        "placements_checked": placed,
        "placed_over_bound": placed_over,
        "max_placed_excess": max_placed_excess,
        "placements_with_any_replica_over_bound": any_over_at_placement,
        "excursions": len(exc),
        "excursions_over_1s": len(over_1s),
        "excursions_over_1s_with_placements_on_over_replica": sum(
            e["placements_on_over_replica"] > 0 for e in over_1s),
        "longest_excursion": longest,
        "max_excess": max((e["max_excess"] for e in exc), default=0),
        "out_of_bound_time_share": round(sum(e["seconds"] for e in exc) / window, 4) if window else 0,
        "reload_unix": reload_at,
    }


def parse_time(s):
    """RFC 3339 with fractional seconds and offset -> unix seconds."""
    m = re.match(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(\.\d+)?(Z|[+-]\d\d:\d\d)", s)
    secs = float(calendar.timegm(time.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S")))
    secs += float(m.group(2) or 0)
    tz = m.group(3)
    if tz != "Z":
        sign = 1 if tz[0] == "+" else -1
        secs -= sign * (int(tz[1:3]) * 3600 + int(tz[4:6]) * 60)
    return secs


def removal_report(rows, removal, reload_t, events, args, log):
    rep = removal["replica"]
    rest = [m for m in sorted(MOCKS) if m != rep]
    # Last sample whose interval saw a new request arrive on the removed replica.
    last_new = None
    for prev, cur in zip(rows, rows[1:]):
        if cur["mock_requests"][rep] > prev["mock_requests"][rep]:
            last_new = cur["t"]
    new_after_reload = 0
    base = None
    for r in rows:
        if r["t"] >= reload_t:
            base = r["mock_requests"][rep] if base is None else base
    if base is not None:
        new_after_reload = rows[-1]["mock_requests"][rep] - base
    # First sample at/after the reload from which the remaining replicas stay
    # in bound (n = 2) for at least 1 s, while placements continue.
    in_bound_from = None
    post = [r for r in rows if r["t"] >= reload_t]
    for i, r in enumerate(post):
        ok = True
        j = i
        while j < len(post) and post[j]["t"] - r["t"] < 1.0:
            v = post[j]["inflight"]
            tot = sum(v.get(m, 0) for m in rest)
            b = bound_of(tot, len(rest), args.epsilon, args.slack)
            if max(v.get(m, 0) for m in rest) > b:
                ok = False
                break
            j += 1
        if ok:
            in_bound_from = r["t"]
            break
    drained = next((r["t"] for r in post if r["inflight"].get(rep, 0) == 0), None)
    at_reload = next((r["inflight"].get(rep, 0) for r in post), None)
    out = {
        "replica": rep,
        "config_written_t": removal["t"],
        "reload_t": reload_t,
        "reload_after_write_s": round(reload_t - removal["t"], 3),
        "inflight_on_removed_at_reload": at_reload,
        "last_new_request_on_removed_t": last_new,
        "new_requests_on_removed_after_reload": new_after_reload,
        "remaining_in_bound_from_t": in_bound_from,
        "rebalanced_after_reload_s": None if in_bound_from is None else round(
            max(in_bound_from, last_new or 0) - reload_t, 3),
        "rebalanced_after_write_s": None if in_bound_from is None else round(
            max(in_bound_from, last_new or 0) - removal["t"], 3),
        "removed_drained_t": drained,
    }
    if args.run:
        out.update(failures(args.run, log, rep))
    return out


def failures(run_dir, log, rep):
    total = failed = 0
    by_replica = {}
    with open(os.path.join(run_dir, "results.jsonl")) as f:
        for line in f:
            r = json.loads(line)
            total += 1
            if r.get("status") == 200 and not r.get("error"):
                continue
            failed += 1
            where = "unknown"
            if log and r.get("trace_id") in log["requests"]:
                where = log["requests"][r["trace_id"]]["replica"] or "none"
            by_replica[where] = by_replica.get(where, 0) + 1
    placed = {}
    if log:
        for e in log["requests"].values():
            placed[e["replica"]] = placed.get(e["replica"], 0) + 1
    return {"requests": total, "failed": failed, "failed_by_replica": by_replica,
            "failed_not_on_removed": failed - by_replica.get(rep, 0),
            "gateway_log_requests_by_replica": placed}


def markdown(res):
    L = [f"eps {res['epsilon']}, slack {res['slack']}, {res['samples_checked']} samples every "
         f"{res['interval_s']} s over a {res['placement_window_s']} s placement window "
         f"(sample errors {res['sample_errors']}); mean total in-flight {res['mean_total_inflight']}", ""]
    lg = res["longest_excursion"]
    L.append(f"- Excursions above ceil((1+eps)*avg)+slack: {res['excursions']} "
             f"({res['excursions_over_1s']} longer than 1 s); max excess {res['max_excess']}; "
             f"longest {lg['seconds'] if lg else 0} s"
             + (f" ({lg['replica']} at t={lg['start']} s, excess {lg['max_excess']})" if lg else "")
             + f"; out of bound {res['out_of_bound_time_share'] * 100:.2f}% of the window")
    L.append(f"- Route decisions: {res['route_decisions']}")
    L.append(f"- Requests: {res['requests']}; retries {res['retries'] or 0}; "
             f"fail-opens {res['failopen'] or 0}; ejections {res['ejections'] or 0}")
    L.append(f"- Live route-decision p99 (histogram bucket bound): {res['router_p99']}")
    lb = res.get("from_gateway_log")
    if lb:
        lg2 = lb["longest_excursion"]
        L.append(f"- From the gateway log ({lb['requests']} requests, exact spans): "
                 f"{lb['placements_checked']} placements, {lb['placed_over_bound']} placed above the bound "
                 f"(max placed excess {lb['max_placed_excess']}); {lb['placements_with_any_replica_over_bound']} "
                 f"placements while some replica was above it; excursions {lb['excursions']}, "
                 f"{lb['excursions_over_1s']} longer than 1 s "
                 f"({lb['excursions_over_1s_with_placements_on_over_replica']} of them with a placement on the "
                 f"over-bound replica); max excess {lb['max_excess']}; longest "
                 f"{lg2['seconds'] if lg2 else 0} s; out of bound {lb['out_of_bound_time_share'] * 100:.2f}%")
    L += ["", "| t from (s) | samples | mean total | mean bound | max mock-0 | max mock-1 | max mock-2 | max excess | out-of-bound share |",
          "| --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
    for s in res["series"]:
        mp = s["max_per_replica"]
        L.append(f"| {s['from_s']} | {s['samples']} | {s['mean_total']} | {s['mean_bound']} | "
                 f"{mp['mock-0']} | {mp['mock-1']} | {mp['mock-2']} | {s['max_excess']} | {s['out_of_bound_share']} |")
    if "removal" in res:
        L += ["", "Removal:", "", "```", json.dumps(res["removal"], indent=2), "```"]
    return "\n".join(L) + "\n"


# ---------------------------------------------------------------- fail open

FAILOPEN_BODIES = {
    "unparseable_body": b'{"model": "large-model", "messages": [',
    "no_affinity_key": b'{"model": "large-model", "max_tokens": 8}',
}


def post(port, body):
    req = urllib.request.Request(f"http://{HOST}:{port}/v1/chat/completions", data=body, method="POST",
                                 headers={"Content-Type": "application/json", "X-Mock-Output-Tokens": "4"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
            return resp.status
    except urllib.error.HTTPError as err:
        return err.code


def failopen(args):
    os.makedirs(args.out, exist_ok=True)
    gw_port, metrics = 18133, DEFAULT_METRICS
    res = {}
    for name, body in FAILOPEN_BODIES.items():
        direct = {}
        for _ in range(args.n):
            st = post(MOCKS["mock-0"], body)
            direct[st] = direct.get(st, 0) + 1
        before_m = fetch(metrics)
        before = {m: json.loads(fetch(f"http://{HOST}:{p}/mock/stats"))["requests"] for m, p in MOCKS.items()}
        via = {}
        for _ in range(args.n):
            st = post(gw_port, body)
            via[st] = via.get(st, 0) + 1
        after = {m: json.loads(fetch(f"http://{HOST}:{p}/mock/stats"))["requests"] for m, p in MOCKS.items()}
        after_m = fetch(metrics)
        diff = lambda fam: {k: v - counters(before_m, fam).get(k, 0.0)
                            for k, v in counters(after_m, fam).items() if v - counters(before_m, fam).get(k, 0.0)}
        res[name] = {
            "requests": args.n,
            "direct_statuses": direct,
            "gateway_statuses": via,
            "per_mock_via_gateway": {m: after[m] - before[m] for m in MOCKS},
            "failopen_delta": diff("gateway_failopen_total"),
            "route_decisions_delta": diff("gateway_route_decisions_total"),
        }
    with open(os.path.join(args.out, "failopen.json"), "w") as f:
        json.dump(res, f, indent=2)
    print(json.dumps(res, indent=2))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("sample")
    s.add_argument("-out", required=True)
    s.add_argument("-metrics", default=DEFAULT_METRICS)
    s.add_argument("-interval", type=float, default=0.1)
    s.add_argument("-live-config", dest="live_config")
    s.add_argument("-remove", default="mock-2")
    s.add_argument("-remove-after", dest="remove_after", type=float, default=60.0)
    a = sub.add_parser("analyze")
    a.add_argument("-dir", required=True)
    a.add_argument("-epsilon", type=float, default=0.25)
    a.add_argument("-slack", type=int, default=1)
    a.add_argument("-bucket", type=float, default=10.0)
    a.add_argument("-run", help="loadgen run directory (results.jsonl)")
    a.add_argument("-gateway-log", dest="gateway_log")
    fo = sub.add_parser("failopen")
    fo.add_argument("-out", required=True)
    fo.add_argument("-n", type=int, default=30)
    args = ap.parse_args()
    if args.cmd == "sample":
        sample(args)
    elif args.cmd == "failopen":
        failopen(args)
    else:
        analyze(args)


if __name__ == "__main__":
    sys.exit(main())
