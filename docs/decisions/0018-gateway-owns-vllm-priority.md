# 0018 — The gateway owns vLLM priority on flagged pools

- Status: accepted
- Date: 2026-10-20
- Milestone: M2 (task M2-11)
- Brief sections: 5.4, 4.3
- Amends: [ADR 0009](0009-vllm-priority-passthrough.md)

## Context

ADR 0009 injects the class's vLLM `priority` on pools with `priority_scheduling: true` "only when the caller didn't set one", so a caller's own value stood. The M2 verification (D1, `docs/results/2026-10-20-M2-verification.md`) and the PR review bot both found what that allows. Any caller, including an anonymous one clamped to `user` (M2-09) or a `background` caller, can send `"priority": -1000` and be scheduled ahead of interactive traffic in vLLM. On real vLLM, priority scheduling also preempts running requests. Gateway admission still applies, but at the replica the class clamp no longer holds.

vLLM parses the body with Python's JSON decoder, which keeps the last of duplicate keys. So a body with two top-level `priority` members carries the caller's choice past any check that reads only one of them.

## Decision

On a flagged pool, the request's class alone decides its vLLM priority.

- If the caller sent no `priority`, the class's value is spliced in when it is non-zero and the pool's probe has verified its replicas, as before.
- If the caller sent exactly one `priority`, it is replaced with the class's value. A value equal to what the class sends is left alone, so the body stays byte for byte. When no class value is sent (the class maps to 0, or the pool is not verified yet), a non-zero or non-numeric caller value is removed.
- A body with more than one top-level `priority` is refused with 400 `invalid_request_error`, code `duplicate_priority`, before admission. Picking one value would still forward the other to vLLM.

Unflagged pools are unchanged: a single numeric non-zero caller value is removed (M2-06). Other cases there are task M2-12.

## Consequences

- Callers can no longer choose their vLLM priority. The way to raise it is a higher class, which `X-Priority` and caller policy already bound. If trusted callers ever need finer ordering inside a class, that is a new decision with its own policy.
- `gateway_upstream_priority_total` gains the outcome `replaced`; `stripped` now also counts caller values removed on flagged pools.
- The 400 is a new gateway-originated error, for a malformed request only. No well-formed body is refused.
- The splice is still bytes only and never touches `messages`. `FuzzPrioritySplice` covers strip followed by inject.
