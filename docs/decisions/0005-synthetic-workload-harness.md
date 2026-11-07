# 0005 — Synthetic workload profiles calibrated from benchmarks

- Status: accepted
- Date: 2026-09-24
- Milestone: M0
- Brief sections: 2, 10

## Context

The brief's harness "replays captured requests at multiples of current volume". The app isn't public: there is no traffic to capture, and capturing real prompts would store user data. The requester has replica benchmarks.

## Decision

The harness generates load from synthetic profiles matching the brief's four traffic shapes (`chat`, `structured`, `classify`, `background`) plus `mixed`. Each profile defines prompt/output length distributions, shared-prefix ratio, turns per conversation, arrival process and bursts. Load is expressed as % of benchmarked fleet capacity (50/100/200%). Every run includes a same-session baseline.

## Consequences

- Results are only as realistic as the profiles; after launch, re-fit profiles to real traffic shape (lengths, rates, prefix sharing — never content).
- Capacity numbers live in `docs/plan/roadmap.md` (Calibration) and must be filled before GPU proof runs.
- Mockllm runs prove gateway overhead and correctness, not cache or GPU behaviour.
