#!/usr/bin/env python3
"""Conformance probes for the M0-18 external router evaluation (ADR 0003).

Runs against the stack in deploy/compose/routers/docker-compose.yml (host
ports are published on 127.0.0.1). Standard library only. Every request body
is synthetic (the wire goldens in gateway/testdata/wire); nothing here reads,
stores or prints real prompts. Captured request bodies are compared in memory
and only field NAMES are reported.

  probe.py all -out DIR          run every probe, write DIR/probe.json + DIR/probe.md
  probe.py snapshot -out DIR     before a chat run: clear the captures, save counters
  probe.py stickiness -out DIR   after it: per-replica load and conversation
                                 stickiness from the mocks' captures

Probes, per router (vllm-router, sgl-gateway), alone and behind the gateway:
  forwarding  each wire golden arrives upstream byte-identical (body, headers)
              and the response keeps vLLM's shape (status, headers, framing)
  sse         chunks arrive spread over the generation, not in one burst
  faults      a 5xx before any token and a mid-stream drop: how many upstream
              attempts one client request caused (more than one = silent retry)
  disconnect  a client that goes away mid-stream cancels the upstream request
  metrics     what the router's Prometheus endpoint exports
"""

import argparse
import base64
import http.client
import json
import os
import re
import socket
import sys
import time
import urllib.request

HOST = "127.0.0.1"
MOCKS = {"mock-0": 18100, "mock-1": 18101, "mock-2": 18102}
# name -> (port, what the path is)
TARGETS = {
    "direct": (18100, "mock-0 directly (reference)"),
    "vllm-router": (18110, "vLLM Router alone"),
    "sgl-gateway": (18111, "SGLang Model Gateway alone"),
    "gateway-rr": (18130, "gateway round-robin (reference behind the gateway)"),
    "gateway-affinity": (18133, "gateway affinity router (in-process M1)"),
    "gateway-vllm-router": (18131, "gateway -> vLLM Router"),
    "gateway-sgl-gateway": (18132, "gateway -> SGLang Model Gateway"),
}
ROUTER_METRICS = {"vllm-router": 18120, "sgl-gateway": 18121}
ROUTERS = ["vllm-router", "sgl-gateway"]
# Headers every HTTP hop legitimately sets or drops.
HOP_HEADERS = {"content-length", "connection", "keep-alive", "transfer-encoding", "host"}
# The gateway's documented differences (M0-07): traceparent generated when
# absent, Accept-Encoding: identity upstream for streams.
GATEWAY_ADDED = {"traceparent"}
REQUEST_TIMEOUT = 30.0
SETTLE_SECONDS = 0.3  # time for a cancellation to reach the mock's counters

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
WIRE_DIR = os.path.join(ROOT, "gateway", "testdata", "wire")


# ---------------------------------------------------------------- helpers

def http_json(method, port, path):
    req = urllib.request.Request(f"http://{HOST}:{port}{path}", method=method)
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = resp.read()
    return json.loads(data) if data else None


def clear_captures():
    for port in MOCKS.values():
        http_json("DELETE", port, "/mock/requests")


def captures():
    """Every captured request on every mock: list of (mock, entry)."""
    out = []
    for name, port in MOCKS.items():
        for entry in http_json("GET", port, "/mock/requests") or []:
            out.append((name, entry))
    return out


def mock_stats():
    return {name: http_json("GET", port, "/mock/stats") for name, port in MOCKS.items()}


def stats_delta(before, after, key):
    return sum(after[m][key] - before[m][key] for m in MOCKS)


def send(port, method, path, headers, body, stream_reader=None, timeout=REQUEST_TIMEOUT):
    """Send exactly these headers (plus Host and Content-Length) and return
    (status, header list, body bytes, chunk arrival offsets in seconds)."""
    conn = http.client.HTTPConnection(HOST, port, timeout=timeout)
    start = time.monotonic()
    conn.putrequest(method, path, skip_accept_encoding=True)
    for k, v in headers.items():
        conn.putheader(k, v)
    if body is not None:
        conn.putheader("Content-Length", str(len(body)))
    conn.endheaders(body)
    resp = conn.getresponse()
    arrivals, chunks = [], []
    if stream_reader is not None:
        return stream_reader(conn, resp, start)
    while True:
        line = resp.readline()
        if not line:
            break
        arrivals.append(time.monotonic() - start)
        chunks.append(line)
    conn.close()
    return resp.status, resp.getheaders(), b"".join(chunks), arrivals


def mask_json_values(raw):
    """Replace string and number VALUES (not keys) so two responses with
    different generated text/ids compare equal iff their shape and byte layout
    (key order, spacing, framing) match."""
    s = raw.decode("utf-8", "replace")
    s = re.sub(r'"(?:[^"\\]|\\.)*"(?=\s*[,}\]])', '"S"', s)
    s = re.sub(r'(?<=[:\[,])(\s*)-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?(?=\s*[,}\]])', r"\1N", s)
    return s


def json_diff(a, b, prefix=""):
    """Field-level differences from a to b, as names only (never values)."""
    diffs = []
    if isinstance(a, dict) and isinstance(b, dict):
        for k in a:
            if k not in b:
                diffs.append(f"removed {prefix}{k}")
            else:
                diffs.extend(json_diff(a[k], b[k], f"{prefix}{k}."))
        for k in b:
            if k not in a:
                diffs.append(f"added {prefix}{k}")
    elif isinstance(a, list) and isinstance(b, list) and len(a) == len(b):
        for i, (x, y) in enumerate(zip(a, b)):
            diffs.extend(json_diff(x, y, f"{prefix}{i}."))
    elif a != b:
        diffs.append(f"changed {prefix.rstrip('.')}")
    return diffs


def header_names(pairs):
    return {k.lower() for k, _ in pairs}


STREAM_CASES = set()


def load_goldens():
    with open(os.path.join(WIRE_DIR, "manifest.json")) as f:
        manifest = json.load(f)
    cases = []
    for c in manifest["cases"]:
        body = None
        if c.get("body"):
            with open(os.path.join(WIRE_DIR, c["body"]), "rb") as f:
                body = f.read()
        if c.get("stream"):
            STREAM_CASES.add(c["name"])
        cases.append({**c, "body_bytes": body})
    return cases


# ---------------------------------------------------------------- probes

def arrived(case):
    """The one request the mocks received for this case (None if none)."""
    got = [(m, e) for m, e in captures() if e["path"] == case["path"]]
    if len(got) != 1:
        return None, len(got)
    m, e = got[0]
    body = base64.b64decode(e["body"]) if e.get("body") else b""
    return {"mock": m, "header": e["header"], "body": body}, 1


def probe_forwarding():
    results = []
    for case in load_goldens():
        for target, (port, _) in TARGETS.items():
            clear_captures()
            # Deterministic lengths so framing can be compared across paths.
            headers = dict(case["header"])
            headers["X-Mock-Output-Tokens"] = "8"
            try:
                status, rh, rb, _ = send(port, case["method"], case["path"], headers, case["body_bytes"])
            except (OSError, http.client.HTTPException) as err:
                results.append({"case": case["name"], "target": target, "error": str(err)})
                continue
            time.sleep(0.05)
            up, n = arrived(case)
            row = {
                "case": case["name"],
                "target": target,
                "status": status,
                "upstream_requests": n,
                "resp_headers": sorted(header_names(rh)),
                "resp_masked": mask_json_values(rb),
            }
            if up is not None:
                sent = case["body_bytes"] or b""
                row["body_identical"] = up["body"] == sent
                row["body_diff"] = []
                if not row["body_identical"] and sent:
                    try:
                        row["body_diff"] = json_diff(json.loads(sent), json.loads(up["body"]))
                    except ValueError:
                        row["body_diff"] = ["not JSON"]
                    if not row["body_diff"]:
                        row["body_diff"] = ["same JSON, different bytes (re-serialized)"]
                row["up_headers"] = sorted(k.lower() for k in up["header"])
            results.append(row)
    return results


def shape(masked, case):
    """Streams compare by the set of distinct event shapes (the number of
    events depends on output length, which a router that drops the
    X-Mock-Output-Tokens header changes); other bodies compare whole."""
    if case in STREAM_CASES:
        return frozenset(line for line in masked.splitlines() if line)
    return masked


def summarize_forwarding(rows):
    """Per target: cases byte-identical, header and response differences
    against the matching reference (direct for routers alone, gateway-rr for
    routers behind the gateway)."""
    by = {}
    for r in rows:
        by.setdefault(r["target"], {})[r["case"]] = r
    sent_headers = {c["name"]: {k.lower() for k in c["header"]} | {"x-mock-output-tokens"}
                    for c in load_goldens()}
    summary = {}
    for target in TARGETS:
        refname = "gateway-rr" if target.startswith("gateway-") else "direct"
        s = {"cases": 0, "identical": 0, "not_forwarded": [], "body_changes": {},
             "headers_dropped": set(), "headers_added": set(), "resp_headers_added": set(),
             "resp_headers_dropped": set(), "resp_shape_differs": [], "status_differs": []}
        for case, r in by.get(target, {}).items():
            s["cases"] += 1
            ref = by.get(refname, {}).get(case)
            if "error" in r:
                s["not_forwarded"].append(f"{case} (error)")
                continue
            # mockllm captures generation requests only (not GET /v1/models):
            # judge forwarding against what the reference path delivered.
            ref_n = ref["upstream_requests"] if ref and "error" not in ref else 1
            if r["upstream_requests"] != ref_n:
                s["not_forwarded"].append(f"{case} ({r['upstream_requests']} upstream requests, status {r['status']})")
            elif r["upstream_requests"] == 0:
                s["cases"] -= 1  # not observable upstream on any path
            elif r["body_identical"]:
                s["identical"] += 1
            else:
                for d in r["body_diff"]:
                    s["body_changes"].setdefault(d, []).append(case)
            if "up_headers" in r:
                expected = sent_headers[case] - HOP_HEADERS
                got = set(r["up_headers"]) - HOP_HEADERS
                ref_got = set(ref["up_headers"]) - HOP_HEADERS if ref and "up_headers" in ref else expected
                s["headers_dropped"] |= (ref_got - got)
                s["headers_added"] |= (got - ref_got)
            if ref and "error" not in ref:
                s["resp_headers_added"] |= set(r["resp_headers"]) - set(ref["resp_headers"])
                s["resp_headers_dropped"] |= set(ref["resp_headers"]) - set(r["resp_headers"])
                if r["status"] != ref["status"]:
                    s["status_differs"].append(f"{case} ({ref['status']} -> {r['status']})")
                elif shape(r["resp_masked"], case) != shape(ref["resp_masked"], case):
                    s["resp_shape_differs"].append(case)
        for k in ("headers_dropped", "headers_added", "resp_headers_added", "resp_headers_dropped"):
            s[k] = sorted(s[k] - {"date"})
        summary[target] = s
    return summary


def probe_sse():
    """20 tokens at the mock's rate after a 300 ms TTFT: an unbuffered relay
    shows the chunks spread over the generation."""
    body = json.dumps({"model": "large-model", "stream": True, "max_tokens": 20,
                       "messages": [{"role": "user", "content": "synthetic sse probe"}]}).encode()
    headers = {"Content-Type": "application/json", "Accept": "text/event-stream",
               "X-Mock-TTFT-Ms": "300", "X-Mock-Output-Tokens": "20"}
    out = {}
    for target, (port, _) in TARGETS.items():
        runs = []
        for _ in range(5):
            status, _, raw, arrivals = send(port, "POST", "/v1/chat/completions", headers, body)
            data_times = arrivals  # one entry per line; framing lines included
            events = raw.count(b"data: ")
            runs.append({
                "status": status,
                "events": events,
                "first_byte_ms": round(data_times[0] * 1000, 1) if data_times else None,
                "last_byte_ms": round(data_times[-1] * 1000, 1) if data_times else None,
                "spread_ms": round((data_times[-1] - data_times[0]) * 1000, 1) if data_times else None,
                "distinct_arrivals": len({round(t, 3) for t in data_times}),
                "done": raw.rstrip().endswith(b"data: [DONE]"),
            })
        out[target] = runs
    return out


FAULT_CASES = {
    # mode -> {label: (extra headers, stream)}
    "header": {
        "5xx before any token (non-stream)": ({"X-Mock-Fault": "5xx"}, False),
        "5xx before any token (stream)": ({"X-Mock-Fault": "5xx"}, True),
        "connection drop mid-stream": ({"X-Mock-Fault": "drop"}, True),
    },
    # For routers that drop X-Mock-* headers: the mocks were started with
    # MOCK_FAULT_5XX_RATE=1 or MOCK_FAULT_DROP_RATE=1 (run-eval.sh does this).
    "env-5xx": {
        "5xx before any token (non-stream)": ({}, False),
        "5xx before any token (stream)": ({}, True),
    },
    "env-drop": {
        "connection drop mid-stream": ({}, True),
    },
}


def probe_faults(mode="header"):
    """How many upstream attempts one failing client request produces."""
    out = {}
    cases = FAULT_CASES[mode]
    for target in ["direct", "vllm-router", "sgl-gateway", "gateway-rr",
                   "gateway-vllm-router", "gateway-sgl-gateway"]:
        port = TARGETS[target][0]
        rows = {}
        for label, (extra, stream) in cases.items():
            body = json.dumps({"model": "large-model", "stream": stream, "max_tokens": 16,
                               "messages": [{"role": "user", "content": "synthetic fault probe"}]}).encode()
            headers = {"Content-Type": "application/json", **extra}
            before = mock_stats()
            t0 = time.monotonic()
            try:
                status, _, raw, _ = send(port, "POST", "/v1/chat/completions", headers, body, timeout=120)
                outcome = f"{status}"
                if stream and status == 200:
                    outcome += ", stream ended " + ("with [DONE]" if raw.rstrip().endswith(b"[DONE]") else "without [DONE]")
            except (OSError, http.client.HTTPException) as err:
                outcome = f"client error: {type(err).__name__}"
            elapsed = time.monotonic() - t0
            time.sleep(SETTLE_SECONDS)
            after = mock_stats()
            rows[label] = {"upstream_attempts": stats_delta(before, after, "requests"),
                           "client_saw": outcome, "elapsed_s": round(elapsed, 2)}
        out[target] = rows
    return out


def probe_disconnect():
    """Read one SSE chunk of a long generation, then close the socket."""
    body = json.dumps({"model": "large-model", "stream": True, "max_tokens": 2000,
                       "messages": [{"role": "user", "content": "synthetic disconnect probe"}]}).encode()
    headers = {"Content-Type": "application/json", "Accept": "text/event-stream",
               "X-Mock-Output-Tokens": "2000"}

    def read_one_then_close(conn, resp, start):
        resp.readline()
        conn.sock.shutdown(socket.SHUT_RDWR)
        conn.close()
        return resp.status, [], b"", [time.monotonic() - start]

    out = {}
    for target, (port, _) in TARGETS.items():
        before = mock_stats()
        send(port, "POST", "/v1/chat/completions", headers, body, stream_reader=read_one_then_close)
        cancelled, waited = 0, 0.0
        while waited < 5.0:
            time.sleep(0.1)
            waited += 0.1
            after = mock_stats()
            cancelled = stats_delta(before, after, "cancelled")
            if cancelled:
                break
        out[target] = {"upstream_cancelled": cancelled, "within_s": round(waited, 1)}
        time.sleep(0.2)
    return out


def probe_metrics():
    out = {}
    for router, port in ROUTER_METRICS.items():
        try:
            with urllib.request.urlopen(f"http://{HOST}:{port}/metrics", timeout=10) as resp:
                text = resp.read().decode()
        except OSError as err:
            out[router] = {"error": str(err)}
            continue
        families, labels = {}, set()
        for line in text.splitlines():
            if line.startswith("# TYPE "):
                _, _, name, kind = line.split(" ", 3)
                families[name] = kind
            elif line and not line.startswith("#"):
                m = re.match(r"[^{\s]+\{([^}]*)\}", line)
                if m:
                    labels |= {kv.split("=", 1)[0] for kv in m.group(1).split(",") if "=" in kv}
        out[router] = {"families": families, "label_names": sorted(labels)}
    return out


# ------------------------------------------------------------- stickiness

def conversation_key(msgs):
    """A conversation is its system prompt plus first user message (the
    harness resends both on every turn). Hashed; text never leaves memory."""
    first_user = next((m.get("content") for m in msgs if m.get("role") == "user"), "")
    system = msgs[0].get("content") if msgs and msgs[0].get("role") == "system" else ""
    return hash((json.dumps(system), json.dumps(first_user)))


def snapshot(out):
    """Clear the captures and record the counters a run starts from."""
    clear_captures()
    with open(os.path.join(out, "stats-before.json"), "w") as f:
        json.dump(mock_stats(), f)


def stickiness(out):
    per_replica, turns, dropped = {}, {}, {}
    stats = mock_stats()
    before_path = os.path.join(out, "stats-before.json")
    before = {m: {"requests": 0} for m in MOCKS}
    if os.path.exists(before_path):
        with open(before_path) as f:
            before = json.load(f)
    for mock, entry in captures():
        per_replica[mock] = per_replica.get(mock, 0) + 1
        if entry.get("truncated") or not entry.get("body"):
            continue
        try:
            req = json.loads(base64.b64decode(entry["body"]))
        except ValueError:
            continue
        msgs = req.get("messages") or []
        conv = conversation_key(msgs)
        turn = sum(1 for m in msgs if m.get("role") == "user")
        turns.setdefault(conv, []).append((turn, mock))
    for mock in MOCKS:
        dropped[mock] = stats[mock]["requests"] - before[mock]["requests"] - per_replica.get(mock, 0)
    followups = same = 0
    for seq in turns.values():
        seq.sort()
        for (_, prev), (_, cur) in zip(seq, seq[1:]):
            followups += 1
            same += prev == cur
    total = sum(per_replica.values())
    return {
        "requests_per_replica": per_replica,
        "share_per_replica": {m: round(n / total, 3) for m, n in per_replica.items()} if total else {},
        "max_over_mean": round(max(per_replica.values()) / (total / len(MOCKS)), 2) if total else None,
        "conversations": len(turns),
        "follow_up_turns": followups,
        "follow_ups_on_same_replica": same,
        "stickiness": round(same / followups, 3) if followups else None,
        "requests_not_captured": dropped,
    }


# ------------------------------------------------------------------ report

def fmt_list(xs):
    return ", ".join(f"`{x}`" for x in xs) if xs else "none"


def faults_md(faults, mode):
    first = next(iter(faults.values()))
    lines = ["", f"## Faults ({mode}): upstream attempts per client request", "",
             "| Path | " + " | ".join(first.keys()) + " |",
             "| --- |" + " --- |" * len(first)]
    for t, rows in faults.items():
        lines.append(f"| {t} | " + " | ".join(
            f"{r['upstream_attempts']} attempts; client got {r['client_saw']} after {r['elapsed_s']} s"
            for r in rows.values()) + " |")
    return lines


def write_markdown(res, path):
    lines = ["# Router probe results", ""]
    lines += ["## Forwarding (wire goldens)", "",
              "| Path | Byte-identical bodies | Not forwarded | Body changes (field names only) | Request headers dropped | added | Response headers added | Response shape differs |",
              "| --- | --- | --- | --- | --- | --- | --- | --- |"]
    for t, s in res["forwarding_summary"].items():
        changes = "; ".join(f"{d} ({len(c)})" for d, c in sorted(s["body_changes"].items())) or "none"
        lines.append(f"| {t} | {s['identical']}/{s['cases']} | {', '.join(s['not_forwarded']) or 'none'} | {changes} | "
                     f"{fmt_list(s['headers_dropped'])} | {fmt_list(s['headers_added'])} | {fmt_list(s['resp_headers_added'])} | "
                     f"{', '.join(s['resp_shape_differs'] + s['status_differs']) or 'none'} |")
    lines += ["", "## SSE relay (TTFT 300 ms, 20 tokens)", "",
              "| Path | first byte ms (median) | last byte ms (median) | spread ms (median) | distinct arrival times | [DONE] |",
              "| --- | --- | --- | --- | --- | --- |"]
    for t, runs in res["sse"].items():
        med = lambda k: sorted(r[k] for r in runs)[len(runs) // 2]
        lines.append(f"| {t} | {med('first_byte_ms')} | {med('last_byte_ms')} | {med('spread_ms')} | "
                     f"{med('distinct_arrivals')} | {all(r['done'] for r in runs)} |")
    lines += faults_md(res["faults"], "header")
    lines += ["", "## Client disconnect", "", "| Path | upstream cancelled | within |", "| --- | --- | --- |"]
    for t, r in res["disconnect"].items():
        lines.append(f"| {t} | {r['upstream_cancelled']} | {r['within_s']} s |")
    lines += ["", "## Router metrics", ""]
    for router, m in res["metrics"].items():
        if "error" in m:
            lines.append(f"- {router}: error {m['error']}")
            continue
        lines.append(f"- {router}: {len(m['families'])} families; labels {fmt_list(m['label_names'])}")
        lines.append(f"  - {fmt_list(sorted(m['families']))}")
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["all", "forwarding", "sse", "faults", "disconnect", "metrics",
                                    "snapshot", "stickiness"])
    ap.add_argument("-out", required=True, help="output directory")
    ap.add_argument("-fault-mode", default="header", choices=sorted(FAULT_CASES),
                    help="faults: inject by X-Mock-Fault header or by the mocks' env (run-eval.sh)")
    ap.add_argument("-label", default="", help="faults: suffix for the output file, e.g. no-retries")
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)
    if args.cmd == "faults":
        res = probe_faults(args.fault_mode)
        name = f"faults-{args.fault_mode}{'-' + args.label if args.label else ''}"
        with open(os.path.join(args.out, name + ".json"), "w") as f:
            json.dump(res, f, indent=2)
        md = "\n".join(faults_md(res, f"{args.fault_mode} {args.label}".strip())) + "\n"
        with open(os.path.join(args.out, name + ".md"), "w") as f:
            f.write(md)
        print(md)
        return
    if args.cmd == "snapshot":
        snapshot(args.out)
        return
    if args.cmd == "stickiness":
        res = stickiness(args.out)
        with open(os.path.join(args.out, "stickiness.json"), "w") as f:
            json.dump(res, f, indent=2)
        print(json.dumps(res, indent=2))
        return
    res = {}
    if args.cmd in ("all", "forwarding"):
        rows = probe_forwarding()
        res["forwarding"] = rows
        res["forwarding_summary"] = summarize_forwarding(rows)
    if args.cmd in ("all", "sse"):
        res["sse"] = probe_sse()
    if args.cmd == "all":
        res["faults"] = probe_faults("header")
    if args.cmd in ("all", "disconnect"):
        res["disconnect"] = probe_disconnect()
    if args.cmd in ("all", "metrics"):
        res["metrics"] = probe_metrics()
    with open(os.path.join(args.out, "probe.json"), "w") as f:
        json.dump(res, f, indent=2, default=sorted)
    if args.cmd == "all":
        write_markdown(res, os.path.join(args.out, "probe.md"))
        print(open(os.path.join(args.out, "probe.md")).read())
    else:
        print(json.dumps({k: v for k, v in res.items() if k != "forwarding"}, indent=2, default=sorted))


if __name__ == "__main__":
    sys.exit(main())
