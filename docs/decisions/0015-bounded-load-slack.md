# 0015 — Bounded load with one request of slack

- Status: accepted
- Date: 2026-10-04
- Milestone: M1
- Brief sections: 4.1, 5 (routing)

## Context

M1 routes a conversation to its consistent-hash owner unless that would overload the replica (bounded-load consistent hashing, Mirrokni et al.). The review of the merged M1 (M1-15) found the bound was off by one: the threshold ⌈(1+ε)·average⌉ was computed before placement and a replica was accepted while its load was ≤ that threshold, so the owner could end one request above (1+ε)·average. M1-15 made the bound strict: cap = ⌈(1+ε)·(total+1)/n⌉, where total is the pool's load before placement, and a replica is eligible only if load + 1 ≤ cap.

The independent review of the strict bound simulated 3 replicas at ε = 0.25 and found it costs conversation stickiness at low and mid load. Sticky is the share of a conversation's follow-up turns that land on the same replica as its previous turn (the `deploy/compose/routers/probe.py` definition; the share landing on the primary owner is slightly higher, e.g. 0.997 / 0.987 / 0.956 / 0.967 / 0.985 with slack 1); balance is the share of time any replica is out of bound.

| Mean in-flight | 1 | 3 | 10 | 30 | 100 |
| --- | --- | --- | --- | --- | --- |
| Old (off by one) | 0.96 | 0.92 | 0.88 | 0.92 | 0.97 |
| Strict cap | 0.78 | 0.77 | 0.78 | 0.89 | 0.96 |
| Strict cap + 1 slot | 0.995 | 0.975 | 0.92 | 0.94 | 0.97 |

At 100 in flight, balance barely changes (0.006 vs 0.000 of the time out of bound).

The cause is integer loads. On an idle 3-replica pool, if the owner already serves one request of a conversation, total = 1 and cap = ⌈1.25 × 2 / 3⌉ = 1, so the strict bound sends the conversation's next request elsewhere and loses its prefix cache. AC-M1-3 as written ("no replica's in-flight above (1 + ε) × average for more than 1 s") cannot be met at 1–10 in flight: with integer loads, (1 + ε) × average is often below 2. Two requests on one replica are then already "above" it, and they are unavoidable whenever two requests of one conversation overlap. Stickiness is the point of M1 (AC-M1-1, AC-M1-2), so losing 10–18 percentage points at low load undoes most of the gain for little balance benefit.

## Options

1. **Strict cap.** load + 1 ≤ ⌈(1+ε)·(total+1)/n⌉. Matches the paper's bound exactly. Costs 10–18 pp of stickiness at mean in-flight 1–10 (table above).
2. **Strict cap with a minimum cap of 3.** cap = max(3, …). Fixes the idle case but is a step function: at a mid load it allows up to 3 on a replica whatever the average is, and it does not scale with pool size.
3. **Strict cap + 1 slot of slack.** load + 1 ≤ cap + 1. Recovers stickiness at low load (0.995 / 0.975 / 0.92) and leaves balance at high load essentially unchanged. The excess is one request, a constant that shrinks relative to the cap as load grows.
4. **ε = 0.5.** More headroom at every load. Weaker balance at high load, where the bound matters most, and still loses the idle case at small n.

## Decision

Use the strict cap plus a configurable slack: a replica is eligible only if load + 1 ≤ ⌈(1+ε)·(total+1)/n⌉ + slack. `router.overflow_slack` sets the slack (env `GATEWAY_ROUTER_OVERFLOW_SLACK`, default 1, range 0..16), and 0 restores the strict bound. Load is the replica's gateway in-flight plus vLLM's waiting requests while scraped metrics are fresh.

## Consequences

- AC-M1-3's target becomes "no replica's in-flight above (1 + ε) × average + slack (slack = 1 request) for more than 1 s; ε = 0.25 default". The change is recorded in `docs/plan/roadmap.md` (Target changes).
- Tests assert the post-placement load is ≤ cap + slack: the ring property and skew tests (slack 0 and 1), a low-load test (owner with one in flight keeps the conversation's second request with slack 1 and diverts it with slack 0, at ring and router level), and the concurrent burst test.
- Concurrent routes acquire and then re-check against cap + slack, so concurrency does not add to the slack except as measured in `TestAffinityConcurrentBurstRespectsCap` (at most one more request, observed) and through recorded `over_cap` decisions.
- Revisit if the hardware gate shows hot replicas at high load with slack 1, or if stickiness at low load is still short of the M1 routing-quality target (≥ 0.98 on `chat`). Either is a config change, not a code change.
