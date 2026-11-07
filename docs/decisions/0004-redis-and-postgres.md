# 0004 — Redis for hot shared state, Postgres for registry and policy

- Status: accepted
- Date: 2026-09-24
- Milestone: M2–M4
- Brief sections: 6, 8

## Context

The brief calls for a shared store for routing state, counters and cache, and a relational store for templates and policy. Redis and Postgres are available in the requester's environment.

## Decision

- **Redis:** admission counters with leases (M2), response cache and coalescing coordination (M4). Tight timeouts; local fallback on failure. `maxmemory` + LRU eviction.
- **Postgres:** template registry and policy (M3). Read only by the admin API and the out-of-band snapshot refresher, never on the request path.
- **Routing state (M1):** local per gateway. The consistent-hash ring is deterministic from the replica list, so no shared store is needed.

## Consequences

- Redis down → local-only admission, no coalescing, no response cache (brief degradation table).
- Postgres down → serve last snapshot; unknown template IDs handled per ADR 0006.
