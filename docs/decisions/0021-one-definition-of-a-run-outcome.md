# 0021 — One definition of a run outcome

- Status: accepted (amended 2026-10-28: a stream with no token-bearing event has the whole response as its TTFT)
- Date: 2026-10-26
- Milestone: track AD (AD-01; architecture deepening, [plan](../plan/architecture-deepening.md))
- Brief sections: none (how load proofs are measured; no target changes)
- Builds on: [ADR 0005](0005-synthetic-workload-harness.md)

## Context

Every acceptance proof that runs load reduces a run to verdicts: how many requests succeeded, their TTFT percentiles, whether responses were complete. Those rules are written in five places, and they disagree:

| Rule | `runner` / `report` | `cmd/burst` | `burst-window-stats.py` | `cache-latency-stats.py` |
| --- | --- | --- | --- | --- |
| Success | 2xx and no error (transport, in-stream error event, truncated, bad JSON) | status 200 and no transport error | any 2xx | 2xx and no error |
| TTFT | first token-bearing event (streams); whole response (bodies) | first body byte | read from the record | read from the record |
| Complete | stream reached `data: [DONE]` | `[DONE]` after a `finish_reason`, no error event; bodies: every choice has a `finish_reason` | not checked | not checked |
| Percentiles | bucketed histogram (`summary.json`), nearest rank (`report`) | none | nearest rank | nearest rank |
| Burst window | `Arrivals` (k·every, k ≥ 1, for length) | n/a | own copy, on actual start time | n/a |

`report`'s tests also declare their own copy of the results record, so nothing checks that what `runner` writes is what `report` reads. The hardware gate re-runs these proofs on GPUs (ADR 0013), and copies that disagree can flip a verdict. One known case: the M2-08 arbitration proof counted any 2xx as a success, so a stream that answered 200 and then failed mid-way counted as served.

## Options

1. **Document the differences and keep the copies.** Cheapest, but every proof still has to say which rule it used, and the next copy will drift too.
2. **One outcome module in the harness, and the scripts read its fields (chosen).** The harness owns one definition of each rule, writes the results with it, and records the derived facts (`ok`, `complete`, both TTFTs, the burst window) so a script never re-derives one.
3. **Port every proof script to Go now.** One definition is guaranteed, but it is a large rewrite of scripts that only run in proofs.

## Decision

The harness has one outcome module (`harness/internal/outcome`). Every harness verdict uses its definitions:

- **Complete**: a stream is complete when it reaches `data: [DONE]` after at least one `finish_reason` and carries no error event. A response body is complete when it parses as JSON with at least one choice, and every choice has a non-empty `finish_reason`. An incomplete response is recorded with error kind `truncated`.
- **Success (`ok`)**: status 2xx, no transport error, no in-stream error event, and complete. In other words, no error kind recorded, and a 2xx status.
- **TTFT**: the time from send to the first token-bearing event of a stream (what a caller sees). For a response body, and for a stream that ends with no token-bearing event, it is the whole response time, since the caller sees nothing before it. The time to the first body byte is recorded as its own field (`first_byte_ms`), for comparison with the gateway's `gateway_ttft_seconds`. It is never the TTFT of a verdict.
- **Percentiles for verdicts**: nearest rank on the exact samples. The bucketed histogram stays only for the live summary (`summary.json`) and progress output.
- **Windows**: the warm-up window leaves out requests that started before it. A request is in a burst window when its *scheduled* arrival falls in one (windows start at k·every for k ≥ 1 and last for length), the same rule the arrival process uses.
- **The record**: `results.jsonl` lines carry these facts: `ok`, `complete`, `ttft_ms`, `first_byte_ms` and `in_burst`. Readers use them instead of re-deriving them. This is the one schema the writer and every reader share.

## Consequences

- `runner`, `report` and `cmd/burst` call the outcome module. The proof scripts read the record's fields, and a script is ported to Go only when a hardware-gate proof needs it again (AD-04).
- Results recorded before this ADR keep their numbers. Their files say which rule they used:
  - **M2-08 (any 2xx):** M2-10, its real-vLLM re-run, uses the success rule above and reports both counts for comparison.
  - **M4-07 coalescing (`cmd/burst`):** it already checked completeness the way this ADR defines it, so its verdict stands.
- The stricter completeness check (a `finish_reason` is required) changes nothing on mockllm or vLLM, which always send one. A replica that stops without one is now counted as a failure.
- Revisit if a workload needs streams without a `finish_reason` (for example a future API mode), or if vLLM changes how it ends a stream.
