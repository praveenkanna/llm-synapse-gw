# 0011 — Model- and vLLM-version-agnostic gateway

- Status: accepted
- Date: 2026-09-27
- Milestone: M0 (applies to M1, M3, M4, M5)
- Brief sections: 4.1, 4.2, 4.3, 5.1, 5.3, 6

## Context

The target models (Q2) and vLLM version (Q4) are still unknown, and both will change over the gateway's life: models get swapped per tier, and vLLM ships breaking changes often. Several places in the plan assumed one fixed pair:

- **Metric names.** vLLM renamed its KV-cache and prefix-cache metrics between releases (for example `vllm:gpu_cache_usage_perc` → `vllm:kv_cache_usage_perc`, `vllm:gpu_prefix_cache_*` → `vllm:prefix_cache_*`). The M1 load view, the harness (M0-15) and the dashboards (M0-12) read these.
- **Error body shape.** Older vLLM returns `{"object":"error",...}`; newer returns `{"error":{...}}` (noted in M0-26).
- **Tokenizer and chat template.** ADR 0008 found that exact counts depend on the model's tokenizer, the tokenizer mode vLLM picks (HF vs `mistral_common`) and version-specific message preprocessing. It asked for a rerun once Q2/Q4 are answered (M0-27).
- **The GPU compose profile** needs an image tag and model names.

Hard-coding any of these makes M0 wait on Q2/Q4 and makes every model swap or vLLM upgrade a code change. Principle 4.3 (fail open) already says an unknown situation must degrade an optimization, never a request. The goal "Generic: pools, replicas, thresholds … are data, not code" (`docs/goals.md`) points the same way.

What is already agnostic: the request path forwards body bytes untouched (principle 4.1), so any model and any vLLM request field passes through; model → pool is config; M1 affinity hashes message bytes with no tokenizer; M2 admission counts requests; priority is a per-pool flag (ADR 0009); M3 registration counts through the replica's own `/tokenize` (ADR 0008).

## Options

1. **Pin one model set and one vLLM version.** Wait for Q2/Q4, code to them, redo the spikes and metric names on every change. Simplest code. Blocks M0 on the requester, and every upgrade becomes a gateway release with a risk of silent breakage (a renamed metric makes the load view blind without an error).
2. **Agnostic by configuration and self-check (proposed).** Everything that differs between models or versions is data, and each optimization verifies its assumptions against the running replica before relying on them; failed checks fail open with a recorded reason. A little more code (an alias table, a startup probe, a self-check); the gateway keeps working across model swaps and vLLM upgrades, with optimizations switching off visibly rather than misbehaving.
3. **Agnostic by always asking the replica.** Use `/tokenize` and replica-reported state for everything, no local tokenizer or alias table. Fully exact, no version knowledge in the gateway. Adds a network hop and replica load to anything that runs per request, which the M0 TTFT budget (≤ 5 ms p50) can't afford; metric names still have to be known to scrape.

## Decision

Option 2. Model- and version-specific facts are configuration or detected at run time, and every optimization that depends on them is enabled per pool only after a check against the live replica passes:

1. **Metrics:** a config table maps each logical signal (running, waiting, KV usage, prefix-cache queries/hits, preemptions) to an ordered list of candidate metric names, defaults covering current and previous vLLM names. First match wins. No match → that signal is unavailable for the pool, routing falls back to round-robin for it, and `gateway_failopen_total{stage="loadview",reason="metric_missing"}` counts it.
2. **Capabilities:** at startup and on a slow refresh the gateway reads each replica's `/version` and `/v1/models` (served names, `max_model_len`) and exports `gateway_replica_info{pool,replica,vllm_version}`. Priority stays an explicit per-pool flag (ADR 0009), because probing it would mean sending a request that fails when it's off; ADR 0009's startup check stays.
3. **Tokenizer:** the tokenizer file is configured per model. Before a pool uses local counting, the gateway compares its counts with the replica's `/tokenize` on a fixed synthetic sample set (out of band). Any mismatch disables local counting for that model with `reason="tokenizer_mismatch"`; `/tokenize` stays the ground truth for registration (ADR 0008). This replaces the one-time "rerun on final models" with a check that runs on every deploy and upgrade.
4. **Errors:** upstream errors are relayed unchanged. Errors the gateway originates use the OpenAI shape `{"error":{...}}`. mockllm can emit either shape and either metric-name set (env switch), and tests cover both.
5. **Compose GPU profile:** vLLM image tag, model names and flags come from environment variables with open-model defaults.

## Consequences

- Q2 and Q4 stop blocking M0: M0-26 becomes "mockllm supports both vLLM error shapes and metric-name sets"; M0-27 is replaced by the tokenizer self-check (M3-04/M3-05) and closed. AC-M0-11 is met by the stand-in evidence already in ADR 0008 (100/100 on three models) plus the self-check; recorded in the roadmap's Target changes table. Q3 becomes optional: the harness can calibrate capacity on the GPU server itself. GPU server access is still required for M0-19/M0-20.
- M1-04 (load view) gains the alias table and capability refresh; M3-04/M3-05 gain the self-check; M5's per-pool conformance check (ADR 0008) already follows this pattern.
- New failure modes are visible, not silent: a renamed metric or a tokenizer drift shows up as a fail-open reason and a dashboard panel, not as wrong routing.
- The default alias list must be maintained when vLLM renames metrics again; a CI test runs mockllm in both metric modes against the load view.
- "Agnostic" means "degrades safely", not "fully optimized on any version": a vLLM release that removes `/tokenize` or a metric entirely turns the dependent optimization off for that pool until config catches up.
- Revisit if the self-check or probing adds measurable load on replicas, or if vLLM exposes a stable capabilities endpoint that replaces the alias table.
