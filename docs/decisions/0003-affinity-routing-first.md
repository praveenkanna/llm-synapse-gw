# 0003 — Affinity routing first; evaluate external routers; token trie optional

- Status: accepted (revisited 2026-09-28 after M0-18: build, do not adopt)
- Date: 2026-09-24; updated 2026-09-28
- Milestone: M1, M5
- Brief sections: 5.1, 9

## Context

- With round-robin, each replica misses a shared system prompt once and then hits it; the 1/N claim in 5.1 mostly doesn't hold for a few shared prompts. The main routing win is multi-turn chat, where each conversation's growing history should stay on one replica.
- A token-block trie needs the exact token sequence vLLM prefills, which requires rendering the model's Jinja chat template (including `tools`) in Go.
- The brief rejects external routers as Kubernetes-only. The [vLLM Router](https://vllm.ai/blog/2025-12-13-vllm-router-release) (Rust, fork of SGLang's gateway) runs on bare metal with session-keyed consistent hashing; the [SGLang Model Gateway](https://github.com/sgl-project/sglang/blob/main/docs/advanced_features/sgl_model_gateway.md) runs from a static worker list with a `cache_aware` policy.

## Options

1. **Token-block radix trie in M1** (brief). Highest potential hit rate; highest complexity and risk.
2. **Conversation affinity with bounded-load consistent hashing in M1**; trie as optional M5. Simple, no tokenizer on the hot path, captures the chat win.
3. **Adopt an external router** as the routing stage behind our gateway.

## Decision

Option 2, with option 3 evaluated at the end of M0 (task M0-18) behind the router interface. The trie (M5) is built only if harness results show a gap.

**M0-18 result (2026-09-28): build, do not adopt.** We build conversation affinity in-process (M1). Neither router is adopted as the routing stage.

## Evaluation (M0-18)

vLLM Router 0.1.15 (`vllm/vllm-router:v0.0.0@sha256:9321abfa…`) and SGLang Model Gateway 0.3.2 (`lmsysorg/sgl-model-gateway:v0.3.2@sha256:83711812…`) each ran behind the gateway in `deploy/compose/routers/`, in front of three mockllm replicas. We compared them with the gateway's own round-robin on the `chat` profile at 20 and 80 req/s. Evidence: `docs/results/2026-09-28-m0-18-routers-chat.md`.

| Criterion | vLLM Router | SGLang Model Gateway |
| --- | --- | --- |
| Static worker list | pass | pass |
| Unmodified request forwarding | **fail**: 0/13 wire goldens byte-identical; body re-serialized with SGLang fields added, `tools[].function.strict` and others dropped | **fail**: 0/13; also drops vLLM guided decoding (`guided_json`, `structured_outputs`, …), renames `max_tokens`, strips non-standard headers |
| Unbuffered SSE, disconnect propagation | pass | pass |
| No silent retry | fail by default (5 attempts on a 5xx); pass with `--disable-retries` | same |
| Metrics | Prometheus, no TTFT | Prometheus, no TTFT |
| Added TTFT vs round-robin | +4 ms p50 / +42 ms p95 at 20 req/s; +41 ms p50 at 80 req/s (the same when the router is called directly, so the step is the router's) | +5 ms / +42 ms; +42 ms p50 at 80 req/s |
| Chat stickiness (share of follow-up turns on the previous turn's replica; round-robin 0.32–0.34) | 0.34–0.35 with `cache_aware`: chat routes only on a session key, and our requests carry none; `consistent_hash` with no key sent every request to one replica | 0.98–0.997 with `cache_aware`, at 1.3× max/mean load skew |

Why build:

1. **Principle 1 (byte-compatible wire) fails for both, and no flag fixes it.** Both parse the request into their own OpenAI model and re-serialize it. SGLang silently disables vLLM guided decoding, which breaks callers. Fail-open cannot help, because the router is the path itself.
2. **~40 ms of TTFT under load**, eight times the gateway's whole p50 budget (AC-M0-3: +5 ms), is intrinsic to both routers. It looks like a Nagle / delayed-ACK stall; neither router exposes a setting for it.
3. **The routing win is small next to what we would build anyway.** SGLang's text-prefix tree does give chat stickiness, but so will the M1 conversation key, without a second proxy hop. vLLM Router's affinity needs that same key sent as `X-Session-ID`.
4. Retries-by-default and a separate circuit breaker and health checker would put retry and eject decisions outside the gateway's view (principle: never silently retry a generation).

## Consequences

- M1 has no dependency on the tokenizer or chat templates.
- The router must be an interface so an external router or the trie can replace the policy.
- M1 proof compares against measured round-robin, not a theoretical 1/N.
- M1's affinity policy should at least match SGLang `cache_aware` on the M0-18 stickiness measure (≈ 0.98 on `chat`) with less load skew. `deploy/compose/routers/probe.py stickiness` and the mocks' request capture give M1 that measurement.
- Revisit if a router release forwards request bytes unmodified (a raw-proxy mode) and removes the ~40 ms step: rerun `deploy/compose/routers/run-eval.sh`. Also revisit after moving to Kubernetes, or if the harness shows a hit-rate gap on the gpu profile that a token-level router closes (M5).
