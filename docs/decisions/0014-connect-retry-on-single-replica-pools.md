# 0014 — Retry a failed connect on the same replica when the pool has no other

- Status: accepted
- Date: 2026-10-02
- Milestone: M0
- Brief sections: 4.3, 8

## Context

`.claude/rules/streaming-and-failure.md` allows one retry when the upstream connect fails before any byte is sent, "on another replica". A pool with one replica therefore turns every connect failure into a 502. The dev stack's pools have one replica each, and early production pools may too.

The M0-75 re-run after M0-76 (`docs/results/2026-10-02-m0-75-ac-m0-5-rerun.md`) found the last gateway-originated 5xx at 5,000 concurrent streams: 3 connect timeouts (`dial tcp …: i/o timeout`) during the opening burst on a CPU-saturated host, plus one connect-budget expiry misclassified as a 504 timeout. The replica was healthy (no listen overflow or drops); the 2 s connect budget went to waiting for CPU. A direct client, with Go's default 30 s dial timeout, saw only latency. The gateway failed requests a direct call would have served, which AC-M0-5 and principle 4.1 forbid.

Retrying a connect is safe: nothing was sent, so no generation can run twice (design-principles: "Retrying is allowed only before any byte is sent upstream").

## Options

1. **Retry on the same replica when the pool has no other (chosen).** Keep exactly one retry before any byte is sent; prefer another replica, otherwise the same one after a short pause. Covers single-replica pools and transient connect stalls; failover to a healthy replica is unchanged when one exists.
2. **Longer connect timeout.** Fewer failures on a stalled host, but slower failover when a replica is really down, and the single-replica gap remains.
3. **Treat the burst errors as a dev-host artifact** and prove that part of AC-M0-5 only on hardware. Leaves the single-replica gap in the code.

## Decision

A connect or DNS failure (no byte sent upstream) may be retried once. The retry goes to another replica of the pool when there is one; when the pool has only one replica, it goes to the same replica after a short configurable pause (`upstream.connect_retry_backoff`, default 100 ms, bounded and validated). Never more than one retry per request; never after any byte is sent; the caller's context still bounds everything. Retries stay counted in `gateway_retries_total{pool,reason}`.

A connect budget that expires before or during the dial is classified as a connect failure (502 `upstream_connect`, retryable), never as a response timeout.

## Consequences

- `.claude/rules/streaming-and-failure.md` row 1 now reads "may retry once: on another replica, or on the same one if the pool has no other".
- A replica that is really down costs a single-replica pool one extra connect budget (≤ 2 s + backoff) per request before the 502; pools with more replicas are unchanged.
- Tests: single-replica pool recovers from a transient connect failure; a dead single replica still fails after exactly one retry; multi-replica pools still prefer another replica; the expired-budget path maps to connect, not timeout.
- Revisit when M1 adds health and ejection (M1-05): retries may then prefer healthy replicas across pools of the same model tier.
