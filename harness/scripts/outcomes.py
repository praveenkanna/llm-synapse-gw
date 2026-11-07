"""Read harness results in the proof scripts (ADR 0021).

The rules that decide a request's outcome live in Go, in
harness/internal/outcome: the load generator writes each record with the
derived facts already set (ok, complete, ttft_ms, first_byte_ms, in_burst).
Scripts read those fields and never re-derive them. The one rule a script
still applies itself, the percentile of a set of values, is nearest rank,
the same as outcome.Rank; TestPythonRankMatchesGo checks the two agree.
"""
import gzip
import json
import math
import os
import sys

# Fields every record written under ADR 0021 carries.
FIELDS = ("ok", "complete", "ttft_ms", "first_byte_ms", "in_burst")


class OldRecordError(ValueError):
    """A record written before ADR 0021, which the scripts refuse."""


def run(main):
    """Run a script's main, ending with a one-line message (not a traceback)
    when it meets a record written before ADR 0021."""
    try:
        main()
    except OldRecordError as e:
        sys.exit(f"{os.path.basename(sys.argv[0])}: {e}")


def read_records(path):
    """Yield the records of a results.jsonl (or .jsonl.gz) file. Raises
    OldRecordError for a record written before ADR 0021 (no ok field): re-run
    the load, or use the scripts as of 41915b4 for old results."""
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt") as f:
        for n, line in enumerate(f, 1):
            if not line.strip():
                continue
            r = json.loads(line)
            missing = [k for k in FIELDS if k not in r]
            if missing:
                raise OldRecordError(f"{path}:{n}: record has no {', '.join(missing)} (written before ADR 0021)")
            yield r


def pct(values, q):
    """Nearest-rank q-quantile (0 < q <= 1) of values; NaN when empty."""
    if not values:
        return float("nan")
    s = sorted(values)
    return s[min(len(s) - 1, max(0, math.ceil(q * len(s)) - 1))]
