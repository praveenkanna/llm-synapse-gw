# 0013 — Build every milestone first; prove on hardware before Release 1

- Status: accepted
- Date: 2026-09-30 (decided 2026-09-28)
- Milestone: M0–M4
- Brief sections: 10 (milestones), 11 (proof)

## Context

The brief and our roadmap sequence the work so that "M0 gates everything": M1–M3 start only after M0's acceptance proof passes. Several M0 criteria (AC-M0-2's real-vLLM half, AC-M0-3/4 overhead and throughput vs. direct, AC-M0-8 soak, AC-M0-9 memory at 200% load, AC-M0-10 rolling restart at load) can only be measured on a GPU server running real vLLM, and AC-M0-6 needs a Linux host where the load generator doesn't share CPUs with the gateway. Neither is available yet. Later milestones have the same shape: their proof runs (M1-07, M2-08, M3-11, M4-07) measure prefix-cache hit rate, preemption and TTFT on real replicas, which mockllm cannot prove (ADR 0010).

Evidence so far: every functional criterion that mockllm can check passes (wire-compat goldens, 0 gateway 5xx, abort cancellation, spikes decided). Non-streaming overhead measured on the dev host is +0.8 ms p50 / +1.9 ms p95. The streaming TTFT tail on the dev host is attributed to CPU sharing, not the gateway (docs/results/2026-09-27-m0-39-scale.md).

The owner decided (2026-09-28) to build the whole gateway first and run hardware proofs when a proper machine is connected, updating code from the results.

## Options

1. **Keep "M0 gates everything".** No M1 work until hardware is available. Safest ordering; stalls all development on hardware access with no date.
2. **Stack milestones on unmerged phase branches.** M1 branches from `phase/m0`, and so on. Keeps the gate formally, but produces a growing chain of unmerged branches and PRs that become hard to review and rebase.
3. **Split each milestone's criteria into build criteria and hardware criteria (chosen).** A milestone ships to `main` when its build criteria pass verification; its hardware criteria move to one **hardware gate** that must pass before Release 1 (first production traffic). Targets never change; only *when* they are proven.

## Decision

Option 3.

- Each phase file's acceptance table gets a **Proven** column: `build` (provable with tests or mockllm on any machine), `hardware` (needs a GPU server with real vLLM, or a dedicated load host), or `both` (checked now on the runs that exist and re-checked on hardware).
- Tasks that only produce hardware evidence stay in their milestone file, marked `deferred: hardware gate (ADR 0013)`.
- `/verify-phase` verifies `build` and `both` rows as today and reports `hardware` rows as `DEFERRED`; the overall verdict is PASS when every verified row passes. The hardware gate later verifies the `hardware` and `both` rows on real hardware.
- `docs/plan/roadmap.md` lists the hardware gate: every deferred task and criterion across milestones, and the rule that Release 1 requires all of them to pass.
- Hardware results that miss a target open bug issues or tasks in the milestone that owns the code, as usual.

## Consequences

- M1–M4 can be built and merged now on the mock upstream; M1, M2 and M3 can run in parallel as the roadmap already allows.
- Risk: a design flaw that only shows on real hardware (e.g. gateway overhead under GPU-scale streaming) is found later and may touch several milestones' code. Mitigation: the hardware gate runs M0's overhead criteria first, and any failure there is fixed before later hardware runs.
- Calibration numbers (capacity table, admission thresholds) stay provisional config defaults until the hardware gate runs (ADR 0005 already makes them config).
- Nothing reaches production before the hardware gate passes; Release 1 is defined by it.
- Revisit when hardware becomes available: from then on, a milestone's hardware criteria can be proven before it ships.
