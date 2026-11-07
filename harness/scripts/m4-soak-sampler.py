#!/usr/bin/env python3
"""Bounded-memory sampler for the AC-M4-5 soak, and its summary.

sample: runs on the Compose network (e.g. in a container on
llm-synapse-gw_default). Every --every seconds it reads each gateway's /metrics
(gateway_coalesce_retained_bytes, gateway_coalesce_flights, coalescing
events, cache counters, fail-open reasons) and, every --redis-every seconds,
Redis INFO memory/stats/keyspace and the TTL of --ttl-sample random keys.
One JSON object per line. No key names or values are written, only TTLs and
the key's prefix up to its second colon.

  python3 harness/scripts/m4-soak-sampler.py sample --out build/m4/soak/samples.jsonl --duration 3720

summarize: reads the samples (and optionally `docker stats` lines written by
the host, see --docker-stats) and prints the maxima against their bounds.

  python3 harness/scripts/m4-soak-sampler.py summarize build/m4/soak/samples.jsonl \
      --replay-bytes 16384 --docker-stats build/m4/soak/docker-stats.jsonl
"""
import argparse
import json
import re
import socket
import sys
import time
import urllib.request

SLACK = 4096  # coalesce: a flight holds at most max_replay_bytes plus this

GATEWAY_SERIES = ("gateway_coalesce_retained_bytes", "gateway_coalesce_flights", "gateway_coalesce_events_total",
                  "gateway_coalesced_total", "gateway_cache_hits_total", "gateway_cache_misses_total",
                  "gateway_cache_stores_total", "gateway_failopen_total")


def metrics(host):
    with urllib.request.urlopen(f"http://{host}/metrics", timeout=5) as r:
        text = r.read().decode()
    out = {}
    for line in text.splitlines():
        m = re.match(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(\S+)$', line)
        if not m or m.group(1) not in GATEWAY_SERIES:
            continue
        key = m.group(1) + (m.group(2) or "")
        out[key] = float(m.group(3))
    return out


class Redis:
    """The few RESP commands the sampler needs."""

    def __init__(self, addr):
        host, port = addr.rsplit(":", 1)
        self.s = socket.create_connection((host, int(port)), timeout=5)
        self.f = self.s.makefile("rb")

    def cmd(self, *args):
        parts = [f"*{len(args)}\r\n".encode()]
        for a in args:
            b = str(a).encode()
            parts.append(f"${len(b)}\r\n".encode() + b + b"\r\n")
        self.s.sendall(b"".join(parts))
        return self._read()

    def _read(self):
        line = self.f.readline().rstrip(b"\r\n")
        t, rest = line[:1], line[1:]
        if t == b"+":
            return rest.decode()
        if t == b"-":
            raise RuntimeError(rest.decode())
        if t == b":":
            return int(rest)
        if t == b"$":
            n = int(rest)
            if n < 0:
                return None
            data = self.f.read(n + 2)[:-2]
            return data.decode(errors="replace")
        if t == b"*":
            return [self._read() for _ in range(int(rest))]
        raise RuntimeError(f"bad reply {line!r}")


def info(r, section):
    out = {}
    for line in r.cmd("INFO", section).splitlines():
        if ":" in line and not line.startswith("#"):
            k, v = line.split(":", 1)
            out[k] = v
    return out


def redis_sample(r, n):
    mem, stats, ks = info(r, "memory"), info(r, "stats"), info(r, "keyspace")
    db0 = dict(kv.split("=") for kv in ks.get("db0", "keys=0,expires=0").split(","))
    ttls = {}
    no_ttl = 0
    for _ in range(n):
        k = r.cmd("RANDOMKEY")
        if k is None:
            break
        ttl = r.cmd("TTL", k)
        prefix = ":".join(k.split(":")[:2])
        ttls.setdefault(prefix, []).append(ttl)
        if ttl < 0:
            no_ttl += 1
    return {"used_memory": int(mem["used_memory"]), "used_memory_peak": int(mem["used_memory_peak"]),
            "maxmemory": int(mem["maxmemory"]), "maxmemory_policy": mem.get("maxmemory_policy"),
            "evicted_keys": int(stats["evicted_keys"]), "expired_keys": int(stats["expired_keys"]),
            "keys": int(db0.get("keys", 0)), "expires": int(db0.get("expires", 0)),
            "ttl_sample": ttls, "ttl_sample_without_ttl": no_ttl}


def sample(a):
    r = None
    t0 = time.time()
    next_redis = 0.0
    with open(a.out, "a") as out:
        while time.time() - t0 < a.duration:
            tick = time.time()
            row = {"t": round(tick - t0, 1), "unix": round(tick, 1)}
            for gw in a.gateways:
                try:
                    row[gw] = metrics(gw)
                except Exception as e:  # a missed scrape is recorded, not fatal
                    row[gw] = {"error": str(e)}
            if tick >= next_redis:
                next_redis = tick + a.redis_every
                try:
                    r = r or Redis(a.redis)
                    row["redis"] = redis_sample(r, a.ttl_sample)
                except Exception as e:
                    r = None
                    row["redis"] = {"error": str(e)}
            out.write(json.dumps(row) + "\n")
            out.flush()
            time.sleep(max(0, a.every - (time.time() - tick)))


def summarize(a):
    per_flight_bound = a.replay_bytes + SLACK
    rows = [json.loads(l) for l in open(a.samples)]
    gws = [k for k in rows[0] if k not in ("t", "unix", "redis")]
    print(f"samples: {len(rows)} over {rows[-1]['t'] - rows[0]['t']:.0f} s")
    worst = {"over_bound_samples": 0}
    for gw in gws:
        ret = flights = 0
        per = 0.0
        over = 0
        scrapes = errors = 0
        for row in rows:
            m = row.get(gw, {})
            if "error" in m:
                errors += 1
                continue
            scrapes += 1
            rb = m.get("gateway_coalesce_retained_bytes", 0)
            fl = m.get("gateway_coalesce_flights", 0)
            ret, flights = max(ret, rb), max(flights, fl)
            if fl > 0:
                per = max(per, rb / fl)
            if rb > fl * per_flight_bound:
                over += 1
        last = next(r[gw] for r in reversed(rows) if "error" not in r.get(gw, {"error": 1}))
        ev = {k: v for k, v in last.items() if k.startswith("gateway_coalesce_events_total")}
        print(f"{gw}: scrapes {scrapes} (failed {errors}); max retained {ret:.0f} B, max flights {fl_fmt(flights)}, "
              f"max mean per flight {per:.0f} B (bound {per_flight_bound} B per flight); "
              f"samples with retained > flights x bound: {over}")
        print(f"  final counters: {json.dumps(ev)}")
        for k in sorted(last):
            if k.startswith(("gateway_cache", "gateway_coalesced_total", "gateway_failopen_total")):
                print(f"  {k} {last[k]:.0f}")
        worst["over_bound_samples"] += over
    red = [r["redis"] for r in rows if "redis" in r and "error" not in r["redis"]]
    if red:
        mx = max(red, key=lambda x: x["used_memory"])
        first_evict = next((r["t"] for r in rows if "used_memory" in r.get("redis", {})
                            and r["redis"]["evicted_keys"] > red[0]["evicted_keys"]), "none")
        ttl_vals = [t for r in red for ts in r["ttl_sample"].values() for t in ts]
        cache_ttls = [t for r in red for p, ts in r["ttl_sample"].items() if p.startswith("gw:cache") for t in ts]
        print(f"redis: samples {len(red)}; max used_memory {mx['used_memory']} B vs maxmemory {mx['maxmemory']} B "
              f"({100 * mx['used_memory'] / mx['maxmemory']:.2f}%), used_memory_peak {red[-1]['used_memory_peak']} B; "
              f"evicted_keys {red[0]['evicted_keys']} -> {red[-1]['evicted_keys']}, expired_keys {red[0]['expired_keys']} -> {red[-1]['expired_keys']}")
        print(f"  samples with used_memory > maxmemory: {sum(r['used_memory'] > r['maxmemory'] for r in red)}")
        print(f"  first eviction seen at t={first_evict} s")
        at_ceiling = [r for r in red if r["evicted_keys"] > red[0]["evicted_keys"]]
        if at_ceiling:
            lo = min(r["used_memory"] for r in at_ceiling)
            print(f"  used_memory once evicting: min {lo} B, max {max(r['used_memory'] for r in at_ceiling)} B over {len(at_ceiling)} samples")
        print(f"  keys vs keys with expiry: max keys {max(r['keys'] for r in red)}; samples with keys != expires: "
              f"{sum(r['keys'] != r['expires'] for r in red)}")
        print(f"  TTL sample: {len(ttl_vals)} keys ({len(cache_ttls)} gw:cache), without TTL (-1/-2): "
              f"{sum(r['ttl_sample_without_ttl'] for r in red)}; cache TTL min {min(cache_ttls, default=None)} max {max(cache_ttls, default=None)}")
    if a.docker_stats:
        mem = {}
        for line in open(a.docker_stats):
            d = json.loads(line)
            mem.setdefault(d["Name"], []).append(parse_mib(d["MemUsage"].split("/")[0]))
        for name in sorted(mem):
            v = mem[name]
            print(f"docker {name}: samples {len(v)}, first {v[0]:.1f} MiB, max {max(v):.1f} MiB, last {v[-1]:.1f} MiB")
    sys.exit(1 if worst["over_bound_samples"] else 0)


def fl_fmt(x):
    return f"{x:.0f}"


def parse_mib(s):
    s = s.strip()
    m = re.match(r"([\d.]+)\s*([KMG]i?B|B)", s)
    v, u = float(m.group(1)), m.group(2)
    return v * {"B": 1 / 2**20, "KiB": 1 / 1024, "kB": 1 / 1024, "MiB": 1, "MB": 1, "GiB": 1024, "GB": 1024}.get(u, 1)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("sample")
    s.add_argument("--out", required=True)
    s.add_argument("--duration", type=float, default=3720)
    s.add_argument("--every", type=float, default=2)
    s.add_argument("--redis-every", type=float, default=10)
    s.add_argument("--ttl-sample", type=int, default=20)
    s.add_argument("--redis", default="redis:6379")
    s.add_argument("--gateways", nargs="+", default=["gateway-1:9090", "gateway-2:9090"])
    m = sub.add_parser("summarize")
    m.add_argument("samples")
    m.add_argument("--replay-bytes", type=int, required=True)
    m.add_argument("--docker-stats")
    a = ap.parse_args()
    sample(a) if a.cmd == "sample" else summarize(a)


if __name__ == "__main__":
    main()
