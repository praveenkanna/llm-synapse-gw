#!/usr/bin/env python3
"""Summarize route-decision p99 benchmarks and gate them (AC-M1-5).

Reads `go test -bench` output from benchmarks that report a `p99-µs` metric
(BenchmarkExtractLatency, BenchmarkAffinityRouteLatency) and, optionally,
`go test -v -run TestExtractP99` output. Prints a markdown summary with
every run's p99 per shape (no run is dropped) and exits 1 when any run of
any shape has p99 above the limit, when a shape has fewer runs than
--min-runs, when no benchmark result was found, or when TestExtractP99 did
not pass. With --expect-shapes, a benchmark must report exactly the shapes
listed for it, so a dropped or renamed shape fails the gate too.

With --ratio-limit the script also checks the cost ratios (ADR 0017): the
cost per work unit of every budget-bound shape and every cost-sweep point
against the calibrated shape (--ratio-limit, 1.15 in CI), and, with
--route-ratio-limit, whole-route p50 against the key scan's own p50 for each
shape both benchmarks report. A ratio is compared with its limit through the
minimum over the passes: noise on a shared host only adds time, so a point
fails only when every pass saw it over. Absolute p99 failures set bit 1 of
the exit code and ratio failures bit 2, so a caller can gate them apart.

Usage:
  python3 scripts/bench_p99.py --limit-us 100 --min-runs 5 \
      --expect-shapes BenchmarkAffinityRouteLatency=chat_small,system_1KB \
      --test p99test.txt extract.txt route.txt > summary.md
"""

import argparse
import re
import statistics
import sys

# BenchmarkName/shape-4   25592   46698 ns/op ... 45.67 p50-µs  54.71 p99-µs ...
BENCH = re.compile(r"^(Benchmark[^/\s]+)/(\S+?)(?:-\d+)?\s+\d+\s+(.*)$")
METRIC = re.compile(r"([\d.]+) (p50|p99|max)-µs")
RUN = re.compile(r"^=== RUN\s+TestExtractP99/(\S+)")
LOG = re.compile(r"^\s+\S+\.go:\d+: p99 ([\d.]+) µs \((\d+) bytes\)")
RESULT = re.compile(r"^\s*--- (PASS|FAIL|SKIP): TestExtractP99/(\S+)")
TOP = re.compile(r"^--- (PASS|FAIL|SKIP): TestExtractP99 ")


def parse_bench(paths: list[str]) -> dict[tuple[str, str], list[dict[str, float]]]:
    """Returns {(benchmark, shape): [{"p50":..,"p99":..,"max":..}, ...]} in input order."""
    runs: dict[tuple[str, str], list[dict[str, float]]] = {}
    for path in paths:
        with open(path, encoding="utf-8") as f:
            for line in f:
                m = BENCH.match(line.rstrip("\n"))
                if not m:
                    continue
                metrics = {k: float(v) for v, k in METRIC.findall(m.group(3))}
                if "p99" not in metrics:
                    continue
                runs.setdefault((m.group(1), m.group(2)), []).append(metrics)
    return runs


def parse_test(path: str) -> tuple[str | None, list[tuple[str, float | None, int | None, str]]]:
    """Returns (top-level result or None, [(shape, p99, bytes, result)])."""
    rows: list[tuple[str, float | None, int | None, str]] = []
    logs: dict[str, tuple[float, int]] = {}
    top: str | None = None
    current: str | None = None
    with open(path, encoding="utf-8") as f:
        for line in f:
            if m := RUN.match(line):
                current = m.group(1)
            elif (m := LOG.match(line)) and current:
                logs[current] = (float(m.group(1)), int(m.group(2)))
            elif m := RESULT.match(line):
                p99, size = logs.get(m.group(2), (None, None))
                rows.append((m.group(2), p99, size, m.group(1)))
            elif m := TOP.match(line):
                top = m.group(1)
    return top, rows


def parse_expect(specs: list[str]) -> dict[str, list[str]]:
    """Parses BENCHMARK=shape,shape,... arguments."""
    expect: dict[str, list[str]] = {}
    for spec in specs:
        bench, sep, shapes = spec.partition("=")
        names = [n for n in shapes.split(",") if n]
        if not sep or not bench or not names:
            raise SystemExit(f"--expect-shapes {spec!r}: want BENCHMARK=shape,shape,...")
        expect.setdefault(bench, []).extend(names)
    return expect


def check_shapes(runs: dict[tuple[str, str], list[dict[str, float]]], expect: dict[str, list[str]]) -> list[str]:
    """Returns a failure per expected shape missing and per unexpected shape."""
    failures = []
    for bench, names in expect.items():
        got = {shape for (b, shape) in runs if b == bench}
        failures += [f"{bench}/{n}: expected shape has no result" for n in names if n not in got]
        failures += [f"{bench}/{n}: shape not in --expect-shapes" for n in sorted(got - set(names))]
    return failures


CALIBRATED = "tools_1MB_before_messages_failopen"
COST = re.compile(r"^BenchmarkExtractCostPerUnit/(\S+?)(?:-\d+)?\s+\d+\s+.*?([\d.]+) ns/unit\s+(\d+) units")


def cost_table(path: str, min_units: int = 30_000) -> list[str]:
    """Markdown table of ns/unit per shape and its ratio to the calibrated
    shape, for shapes that spend most of the budget (advisory, not gated)."""
    runs: dict[str, list[tuple[float, int]]] = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            if m := COST.match(line.strip()):
                runs.setdefault(m.group(1), []).append((float(m.group(2)), int(m.group(3))))
    if CALIBRATED not in runs:
        return ["", "### Cost per unit: no result"]
    base = statistics.median(ns for ns, _ in runs[CALIBRATED])
    out = ["", f"### Cost per work unit (advisory; ratio to `{CALIBRATED}`, {base:.3f} ns/unit)", ""]
    out += ["| Shape | Units | ns/unit (median) | Ratio |", "| --- | --- | --- | --- |"]
    for shape, rs in runs.items():
        units = rs[0][1]
        if units < min_units:
            continue
        ns = statistics.median(n for n, _ in rs)
        out.append(f"| `{shape}` | {units} | {ns:.3f} | {ns / base:.2f} |")
    return out


SWEEP = re.compile(r"^BenchmarkExtractCostSweep/(\S+?)/(\S+?)(?:-\d+)?\s+\d+\s+.*?([\d.]+) ns/unit\s+(\d+) units")


def sweep_points(path: str, min_units: int = 30_000):
    """Parses BenchmarkExtractCostSweep output.

    bench.yml runs the sweep as separate passes (processes). Speed shifts
    from pass to pass (memory layout, interference), so each point is
    compared with the ref of its own pass (fastest sample of each within
    the pass). Returns (passes, {(position, point): [ratio per pass]},
    {(position, point): units})."""
    passes: list[dict[tuple[str, str], float]] = []
    units: dict[tuple[str, str], int] = {}
    last_ref = False
    with open(path, encoding="utf-8") as f:
        for line in f:
            m = SWEEP.match(line.strip())
            if not m:
                continue
            k = (m.group(1), m.group(2))
            is_ref = k[0] == "ref"
            if is_ref and not last_ref:
                passes.append({})
            last_ref = is_ref
            if not passes:
                passes.append({})
            cur = passes[-1]
            cur[k] = min(cur.get(k, float("inf")), float(m.group(3)))
            units[k] = int(m.group(4))
    per: dict[tuple[str, str], list[float]] = {}
    for p in passes:
        refs = [v for (pos, _), v in p.items() if pos == "ref"]
        if not refs:
            continue
        base = min(refs)
        for k, v in p.items():
            if k[0] != "ref" and units[k] >= min_units:
                per.setdefault(k, []).append(v / base)
    return passes, per, units


def sweep_table(path: str, min_units: int = 30_000, worst: int = 4, limit: float = 0.0) -> tuple[list[str], list[str]]:
    """Markdown summary of BenchmarkExtractCostSweep: per position, the
    highest ratio to the ref point (calibrated shape) and where it is, and
    the overall worst points. The ratio shown is the median over passes and
    the gated one the minimum (see the module doc). Returns (lines,
    failures); failures is empty when limit is 0."""
    passes, per, units = sweep_points(path, min_units)
    if not per:
        return ["", "### Cost sweep: no result"], ([f"cost sweep: no result for {path}"] if limit > 0 else [])
    ratio = {k: statistics.median(v) for k, v in per.items()}
    floor = {k: min(v) for k, v in per.items()}
    short = sorted(f"{p}/{q}" for (p, q) in units if p != "ref" and units[(p, q)] < min_units)
    out = ["", f"### Cost sweep (ratio to the calibrated shape in the same pass; median and minimum of {len(passes)} passes; {len(ratio)} points)", ""]
    out += ["| Position | Points | Max ratio (median) | At | Max ratio (minimum) | At | Min ratio |", "| --- | --- | --- | --- | --- | --- | --- |"]
    for pos in sorted({p for p, _ in ratio}):
        pts = {q: r for (p, q), r in ratio.items() if p == pos}
        fl = {q: r for (p, q), r in floor.items() if p == pos}
        at, fat = max(pts, key=pts.get), max(fl, key=fl.get)
        out.append(f"| `{pos}` | {len(pts)} | {pts[at]:.2f} | `{at}` | {fl[fat]:.2f} | `{fat}` | {min(pts.values()):.2f} |")
    top = sorted(floor.items(), key=lambda kv: -kv[1])[:worst]
    out += ["", "Worst points (minimum over passes): " + ", ".join(f"`{p}/{q}` {r:.2f}" for (p, q), r in top)]
    if short:
        out += ["", f"Points under {min_units} units (not budget-bound, left out): " + ", ".join(f"`{x}`" for x in short)]
    failures = []
    if limit > 0:
        bad = sorted(((r, k) for k, r in floor.items() if r > limit), reverse=True)
        failures = [f"cost sweep {p}/{q}: ratio {r:.2f} over {limit:g} in every pass" for r, (p, q) in bad]
    return out, failures


def cost_failures(path: str, limit: float, min_units: int = 30_000) -> list[str]:
    """Shapes whose cost per unit, fastest run over the calibrated shape's
    fastest run, is over limit (the cost table's gated form)."""
    runs: dict[str, list[tuple[float, int]]] = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            if m := COST.match(line.strip()):
                runs.setdefault(m.group(1), []).append((float(m.group(2)), int(m.group(3))))
    if CALIBRATED not in runs:
        return [f"cost per unit: no result for {CALIBRATED}"]
    base = min(ns for ns, _ in runs[CALIBRATED])
    out = []
    for shape, rs in runs.items():
        if rs[0][1] < min_units:
            continue
        r = min(ns for ns, _ in rs) / base
        if r > limit:
            out.append(f"cost per unit {shape}: ratio {r:.2f} over {limit:g} in every run")
    return out


def route_ratio(runs: dict[tuple[str, str], list[dict[str, float]]], limit: float, floor_us: float = 10.0) -> tuple[list[str], list[str]]:
    """Whole-route p50 over the key scan's own p50, per shape both benchmarks
    report: it is 1 plus the ring pick when the two binaries run the scan
    equally fast, and several times that when the layout of the router test
    binary slows the scan (M1-24). Shapes whose scan takes under floor_us
    are left out as too small to time."""
    ext = {s: statistics.median(r["p50"] for r in rs if "p50" in r) for (b, s), rs in runs.items() if b == "BenchmarkExtractLatency" and any("p50" in r for r in rs)}
    rte = {s: statistics.median(r["p50"] for r in rs if "p50" in r) for (b, s), rs in runs.items() if b == "BenchmarkAffinityRouteLatency" and any("p50" in r for r in rs)}
    rows = [(s, ext[s], rte[s], rte[s] / ext[s]) for s in sorted(ext.keys() & rte.keys()) if ext[s] >= floor_us]
    if not rows:
        return ["", "### Route over Extract: no shape in both benchmarks"], []
    out = ["", f"### Route p50 over Extract p50 (same shape, two test binaries; limit {limit:g})", ""]
    out += ["| Shape | Extract p50 (µs) | Route p50 (µs) | Ratio |", "| --- | --- | --- | --- |"]
    out += [f"| `{s}` | {e:.1f} | {r:.1f} | {x:.2f} |" for s, e, r, x in rows]
    failures = [f"route/extract {s}: p50 ratio {x:.2f} over {limit:g}" for s, _, _, x in rows if x > limit]
    return out, failures


def fmt(v: float | None) -> str:
    return "–" if v is None else f"{v:.1f}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--limit-us", type=float, default=100.0, help="p99 limit per run in µs (AC-M1-5)")
    ap.add_argument("--min-runs", type=int, default=5, help="runs each shape must have")
    ap.add_argument("--test", help="go test -v output of TestExtractP99")
    ap.add_argument("--cost", help="go test -bench output of BenchmarkExtractCostPerUnit (advisory table)")
    ap.add_argument("--sweep", help="go test -bench output of BenchmarkExtractCostSweep (advisory summary)")
    ap.add_argument("--ratio-limit", type=float, default=0.0, help="cost-ratio limit over the calibrated shape for --cost and --sweep points (0: report only; CI passes 1.15)")
    ap.add_argument("--route-ratio-limit", type=float, default=0.0, help="limit on whole-route p50 over key-scan p50 per shape (0: report only)")
    ap.add_argument(
        "--expect-shapes",
        action="append",
        default=[],
        metavar="BENCHMARK=shape,...",
        help="the exact shapes a benchmark must report (repeatable)",
    )
    ap.add_argument("bench", nargs="+", help="go test -bench output files")
    args = ap.parse_args()

    failures = []
    ratio_failures: list[str] = []
    runs = parse_bench(args.bench)
    if not runs:
        failures.append("no benchmark result with a p99-µs metric found")
    failures += check_shapes(runs, parse_expect(args.expect_shapes))

    out = [f"### Route-decision p99 per run (limit {args.limit_us:g} µs)", ""]
    out.append("| Benchmark | Shape | p99 per run (µs) | Worst p99 | Median p50 | Max (worst run) | Runs over limit |")
    out.append("| --- | --- | --- | --- | --- | --- | --- |")
    for (bench, shape), rs in runs.items():
        p99s = [r["p99"] for r in rs]
        over = sum(p > args.limit_us for p in p99s)
        p50 = statistics.median(r.get("p50", 0.0) for r in rs)
        worst_max = max(r.get("max", 0.0) for r in rs)
        out.append(
            f"| `{bench}` | `{shape}` | {', '.join(fmt(p) for p in p99s)} | {fmt(max(p99s))} "
            f"| {fmt(p50)} | {fmt(worst_max)} | {over} / {len(rs)} |"
        )
        if over:
            failures.append(f"{bench}/{shape}: {over} of {len(rs)} runs with p99 > {args.limit_us:g} µs")
        if len(rs) < args.min_runs:
            failures.append(f"{bench}/{shape}: {len(rs)} runs, want at least {args.min_runs}")

    if args.test:
        top, rows = parse_test(args.test)
        out += ["", f"### TestExtractP99: {top or 'no result'}", ""]
        out.append("| Shape | Best p99 (µs) | Bytes | Result |")
        out.append("| --- | --- | --- | --- |")
        for shape, p99, size, result in rows:
            out.append(f"| `{shape}` | {fmt(p99)} | {size if size is not None else '–'} | {result} |")
        if top != "PASS":
            failures.append(f"TestExtractP99: {top or 'no result'}")

    if args.cost:
        out += cost_table(args.cost)
        if args.ratio_limit > 0:
            ratio_failures += cost_failures(args.cost, args.ratio_limit)
    if args.sweep:
        lines, bad = sweep_table(args.sweep, limit=args.ratio_limit)
        out += lines
        ratio_failures += bad
    lines, bad = route_ratio(runs, args.route_ratio_limit or 1.0)
    out += lines
    if args.route_ratio_limit > 0:
        ratio_failures += bad

    out += ["", f"**Verdict: {'FAIL' if failures else 'PASS'}** (absolute p99, advisory in CI)"]
    out += [f"- {f}" for f in failures]
    if args.ratio_limit > 0 or args.route_ratio_limit > 0:
        out += ["", f"**Ratio verdict: {'FAIL' if ratio_failures else 'PASS'}** (cost ratio limit {args.ratio_limit:g}, route/extract limit {args.route_ratio_limit:g})"]
        out += [f"- {f}" for f in ratio_failures]
    print("\n".join(out))
    return (1 if failures else 0) | (2 if ratio_failures else 0)


if __name__ == "__main__":
    sys.exit(main())
