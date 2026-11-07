#!/usr/bin/env python3
"""AC-M4-1 proof driver: N identical concurrent requests, upstream generations
counted on the replica.

Runs on the Compose network (e.g. in a container on llm-synapse-gw_default) with
the burst binary built for it. For each case it waits until no coalescing
flight is open on any gateway, scrapes the replica's
mockllm_requests_total{endpoint="chat"} and each gateway's
gateway_coalesced_total and gateway_template_requests_total, fires the burst,
scrapes again and records the deltas next to the burst's own report (status,
completeness and SHA-256 per response). The gateways' priority probes use
the completions endpoint, so the chat counter counts only these requests.

  python3 harness/scripts/m4-coalesce-proof.py --burst build/m4/bin/burst \
      --out build/m4/coalesce --repeat 3

Templates (register them first, harness/scripts/register-template.sh):
m4-faq-coalesce (coalesce: true) and m4-faq-plain (no opt-in, the baseline).
"""
import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.request

SYSTEM = ("You answer questions about the trail network of the invented Larkspur Valley park. "
          "Give opening status, surface conditions and the nearest shelter in three short sentences. "
          "Never mention individual visitors.")


def body(section, stream, max_tokens):
    b = {"model": "large-model", "temperature": 0, "max_tokens": max_tokens, "stream": stream,
         "messages": [{"role": "system", "content": SYSTEM},
                      {"role": "user", "content": f"What are the conditions on trail section {section} today?"}]}
    if stream:
        b["stream_options"] = {"include_usage": True}
    return json.dumps(b, separators=(",", ":"))


def scrape(url):
    with urllib.request.urlopen(url, timeout=5) as r:
        return r.read().decode()


def series_sum(text, name, labels=None):
    """Sum of every sample of metric name whose labels include labels."""
    total = 0.0
    for line in text.splitlines():
        if not line.startswith(name):
            continue
        m = re.match(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(\S+)$', line)
        if not m or m.group(1) != name:
            continue
        lab = dict(re.findall(r'(\w+)="([^"]*)"', m.group(2) or ""))
        if all(lab.get(k) == v for k, v in (labels or {}).items()):
            total += float(m.group(3))
    return total


def snapshot(args, template):
    mock = scrape(args.mock_metrics)
    snap = {"mock_chat_requests": series_sum(mock, "mockllm_requests_total", {"endpoint": "chat"}),
            "mock_cancelled": series_sum(mock, "mockllm_cancelled_requests_total")}
    for gw in args.gateways:
        text = scrape(f"http://{gw}:9090/metrics")
        snap[f"{gw}.coalesced"] = series_sum(text, "gateway_coalesced_total", {"template": template})
        snap[f"{gw}.template_requests"] = series_sum(text, "gateway_template_requests_total", {"template": template})
        snap[f"{gw}.flights"] = series_sum(text, "gateway_coalesce_flights")
    return snap


def wait_idle(args):
    deadline = time.time() + 60
    while time.time() < deadline:
        if all(series_sum(scrape(f"http://{gw}:9090/metrics"), "gateway_coalesce_flights") == 0 for gw in args.gateways):
            return
        time.sleep(0.2)
    sys.exit("flights did not drain")


def run_case(args, name, template, stream, target, section, lead_delay):
    os.makedirs(args.out, exist_ok=True)
    bf = os.path.join(args.out, f"{name}.body.json")
    with open(bf, "w") as f:
        f.write(body(section, stream, args.max_tokens))
    wait_idle(args)
    time.sleep(0.5)
    before = snapshot(args, template)
    rep_file = os.path.join(args.out, f"{name}.burst.json")
    cmd = [args.burst, "-url", target, "-n", str(args.n), "-body-file", bf, "-out", rep_file,
           "-lead-delay", lead_delay,
           "-H", f"X-Template-Id: {template}", "-H", "X-Template-Version: 1",
           "-H", f"X-Mock-Output-Tokens: {args.output_tokens}"]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)
    wait_idle(args)
    time.sleep(0.5)
    after = snapshot(args, template)
    with open(rep_file) as f:
        rep = json.load(f)
    delta = {k: after[k] - before[k] for k in before if not k.endswith(".flights")}
    row = {"case": name, "template": template, "stream": stream, "target": target, "lead_delay": lead_delay,
           "n": rep["n"], "ok": rep["ok"], "complete": rep["complete"], "distinct": rep["distinct_responses"],
           "bytes": sorted({r["bytes"] for r in rep["responses"]}),
           "generations": delta["mock_chat_requests"], "coalesced": sum(v for k, v in delta.items() if k.endswith(".coalesced")),
           "per_gateway_requests": {gw: delta[f"{gw}.template_requests"] for gw in args.gateways},
           "upstream_cancelled": delta["mock_cancelled"]}
    print(json.dumps(row), flush=True)
    return row


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--burst", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--n", type=int, default=50)
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--max-tokens", type=int, default=128)
    ap.add_argument("--output-tokens", type=int, default=100, help="X-Mock-Output-Tokens: generation length")
    ap.add_argument("--mock-metrics", default="http://mockllm:8000/metrics")
    ap.add_argument("--gateways", nargs="+", default=["gateway-1", "gateway-2"])
    ap.add_argument("--targets", nargs="+", default=["nginx=http://nginx:80", "gateway-1=http://gateway-1:8080"])
    ap.add_argument("--lead-delays", nargs="+", default=["0s"])
    a = ap.parse_args()
    rows = []
    section = 100
    for target in a.targets:
        tname, url = target.split("=", 1)
        for stream in (True, False):
            for template in ("m4-faq-coalesce", "m4-faq-plain"):
                for lead in a.lead_delays:
                    for i in range(a.repeat):
                        section += 1
                        kind = "coalesce" if template.endswith("coalesce") else "plain"
                        name = f"{tname}-{'stream' if stream else 'body'}-{kind}-lead{lead}-{i}"
                        rows.append(run_case(a, name, template, stream, url, section, lead))
    with open(os.path.join(a.out, "rows.json"), "w") as f:
        json.dump(rows, f, indent=1)


if __name__ == "__main__":
    main()
