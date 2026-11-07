# 0001 — Go single binary with request path and admin API

- Status: accepted
- Date: 2026-09-24
- Milestone: all
- Brief sections: 6, 6.1

## Context

The hot path is thousands of concurrent long-lived SSE relays. GC pauses or event-loop stalls show up directly as TTFT. The admin surface is small CRUD over templates and policy.

## Options

1. **One Go binary, two listeners** (request path, admin API on a separate port). One deployable, no extra hop.
2. **Separate admin service.** Independent deploys, but a second component to run for little gain.
3. **Python/Node gateway.** Faster to prototype; worse tail latency under many concurrent streams.

## Decision

Option 1, as the brief specifies. The admin API listens on its own port and is never exposed through the public load balancer.

## Consequences

- Request path must never call the admin handlers or block on Postgres (in-memory snapshot).
- Tokenization requires cgo (ADR 0008), which affects the Docker build.
