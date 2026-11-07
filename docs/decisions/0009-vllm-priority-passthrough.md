# 0009 — Pass priority to vLLM only on flagged pools

- Status: accepted; amended by [ADR 0018](0018-gateway-owns-vllm-priority.md) (a caller's own priority no longer stands)
- Date: 2026-09-24
- Milestone: M2
- Brief sections: 5.4, 4.3

## Context

Gateway queueing can't preempt background requests already admitted to a replica; they hold its KV cache. vLLM supports `--scheduling-policy priority` with a per-request `priority` field (lower = sooner) that orders and preempts requests ([vLLM docs](https://docs.vllm.ai/en/v0.8.3/serving/openai_compatible_server.html)). vLLM rejects a non-zero priority when priority scheduling is off.

## Decision

Map each priority class to a vLLM `priority` value in config. Inject it into the upstream request only for pools with `priority_scheduling: true`, and only when the caller didn't set one. Enable `--scheduling-policy priority` on the compose vLLM services in M2.

## Consequences

- This is the one case where the gateway modifies the request body (a sampling/scheduling field, not prompt text). It must be covered by wire tests and never touch `messages`.
- A misconfigured flag would cause vLLM errors; add a startup check that probes each flagged pool.
- Verify behaviour on the requester's vLLM version (Q4); there are reports of priority having little effect under some loads.
