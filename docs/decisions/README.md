# Decisions

Create new ones with `/new-adr <title>`. Accepted ADRs override the brief where they conflict.

| ADR | Title | Status | Milestone |
| --- | --- | --- | --- |
| [0001](0001-go-single-binary.md) | Go single binary with request path and admin API | accepted | all |
| [0002](0002-compose-first-kubernetes-ready.md) | Docker Compose on one server first, Kubernetes-ready code | accepted | all |
| [0003](0003-affinity-routing-first.md) | Affinity routing first; evaluate external routers; token trie optional | accepted (M0-18: build, do not adopt) | M1, M5 |
| [0004](0004-redis-and-postgres.md) | Redis for hot shared state, Postgres for registry and policy | accepted | M2–M4 |
| [0005](0005-synthetic-workload-harness.md) | Synthetic workload profiles calibrated from benchmarks | accepted | M0 |
| [0006](0006-level2-fallback-mode.md) | Level 2 fallback when the gateway is down | accepted (option 1: rendered messages + template headers) | M3 |
| [0007](0007-max-tokens-policy.md) | max_tokens policy without truncation | accepted | M3 |
| [0008](0008-tokenizer-and-chat-template.md) | Tokenizer binding and chat-template rendering | accepted (spikes M0-16/17; recheck via ADR 0011) | M3, M5 |
| [0009](0009-vllm-priority-passthrough.md) | Pass priority to vLLM only on flagged pools | accepted | M2 |
| [0010](0010-mock-upstream.md) | Mock upstream for GPU-less development and CI | accepted | M0 |
| [0011](0011-model-and-vllm-version-agnostic.md) | Model- and vLLM-version-agnostic gateway | accepted | M0, M1, M3 |
| [0012](0012-transport-retries-and-auth-before-routing.md) | Transport retries on Idempotency-Key count as caller opt-in; auth before routing (401 before 404) | accepted | M0 |
| [0013](0013-build-first-hardware-gate.md) | Build every milestone first; prove on hardware before Release 1 | accepted | M0–M4 |
| [0014](0014-connect-retry-on-single-replica-pools.md) | Retry a failed connect on the same replica when the pool has no other | accepted | M0 |
| [0015](0015-bounded-load-slack.md) | Bounded load with one request of slack | accepted | M1 |
| [0016](0016-ci-on-github-actions.md) | CI on GitHub Actions: lean triggers, local evidence when minutes run out (amends 0017) | accepted | all |
| [0017](0017-portable-key-scan-evidence.md) | Prove AC-M1-5 by a CPU-portable cost ratio in CI and the absolute p99 on reference hardware | accepted | M1 |
| [0018](0018-gateway-owns-vllm-priority.md) | The gateway owns vLLM priority on flagged pools (amends 0009) | accepted | M2 |
| [0019](0019-coalescing-cache-and-budgeted-context.md) | How coalescing, the response cache and budgeted context work | accepted; amended 2026-10-31 | M4 |
| [0020](0020-route-templated-requests-to-one-gateway.md) | Route each template's requests to one gateway | accepted | M4 |
| [0021](0021-one-definition-of-a-run-outcome.md) | One definition of a run outcome (success, complete, TTFT, percentiles, windows) | accepted; amended 2026-10-28 | AD |
| [0022](0022-packages-consume-their-config-section.md) | Packages consume their config section; config owns defaults and bounds | accepted; amended 2026-11-01 | AD |
