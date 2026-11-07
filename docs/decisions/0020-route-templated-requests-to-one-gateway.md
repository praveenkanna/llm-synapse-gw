# 0020 — Route each template's requests to one gateway

- Status: accepted
- Date: 2026-10-22
- Milestone: M4 (AC-M4-1)
- Brief sections: 5.5
- Builds on: [ADR 0002](0002-compose-first-kubernetes-ready.md), [ADR 0019](0019-coalescing-cache-and-budgeted-context.md)

## Context

AC-M4-1 asks that 50 identical concurrent requests cause exactly 1 upstream generation. A coalescing flight lives in one gateway process's memory (ADR 0019). The Compose stack runs two gateways behind nginx, which balanced them round robin, so a burst was split between them and each gateway led its own flight. The M4-07 proof ([coalesce results](../results/2026-10-22-m4-07-coalesce.md)) measured 2 generations through nginx and exactly 1 direct to a gateway. With N gateways, a burst costs up to N generations, and the response cache doesn't help: every gateway misses at the same moment.

Identical requests share their template. The template ID arrives in the `X-Template-Id` header (ADR 0006), so a front proxy can see it without reading the body.

## Options

1. **Hash the template ID at the front proxy.** nginx balances on a consistent hash of `X-Template-Id`. Untemplated requests hash on nginx's per-request ID, so they spread as before. Pros: a config change, no gateway code, and no request waits on anything new. Cons: all of one template's traffic lands on one gateway, and the Kubernetes ingress must do the same.
2. **Scope AC-M4-1 to one gateway instance.** Accept up to N generations per burst and change the target in the roadmap. Pros: nothing to build. Cons: the saving shrinks as gateways are added, and the deployment never meets the brief's intent.
3. **Share flights across gateways.** Take a Redis lease per key, and have followers on other gateways wait on the leader's output (through Redis or by forwarding to the leader). Pros: works under any balancer. Cons: puts Redis on the request path ahead of the generation, which goes against "never block a request on Redis". It also needs a cross-gateway stream relay, new failure modes and much more code.

## Decision

Option 1. nginx maps `X-Template-Id` to the balancing key, falling back to `$request_id` when the header is empty, and uses `hash $key consistent` on the gateway upstream. The gateway is unchanged. Any later front proxy for the gateways (Kubernetes ingress or Gateway API) must hash the same way. That routing is part of the coalescing design, not an optimization.

## Consequences

- AC-M4-1 is met through nginx: 1 generation in 12 of 12 coalescing runs ([results](../results/2026-10-22-m4-07-coalesce-adr20.md)).
- A hot template's whole load sits on one gateway. That is acceptable here because the gateway is cheap next to the replicas: in the M4-07 soak, each of the two gateways handled about 38 req/s at about 44 MiB RSS. Admission counters live in Redis, and the router reads replica load from the replicas' own metrics, so pinning a template to a gateway changes no limit. Revisit if one gateway's CPU or connections become the bottleneck. The hash could then take the version as well. That costs no merges, since versions never share a flight, but it only spreads load while several versions are live.
- When a gateway fails or returns, its templates move. While a burst is in flight during that move, it can run on two gateways and cost 2 generations, as before this ADR. A consistent hash keeps the moved share to 1/N of the templates.
- `proxy_next_upstream error timeout` still applies: a connect failure retries on another gateway, and a POST that reached a gateway is never re-sent. Measured failover is the same as under round robin, including an existing race: a stopped gateway's first connect can time out to a 504 without a retry.
- Kubernetes (ADR 0002): a Deployment of gateways behind a Service balanced at L4 would split bursts again. The ingress or Gateway API route must hash on `X-Template-Id` with a per-request fallback. An empty header must not hash to one fixed pod. `docs/architecture.md` records this next to the nginx row.
- Callers that bypass the gateway (ADR 0006 Level 2) are unaffected.
