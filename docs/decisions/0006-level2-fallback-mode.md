# 0006 — Level 2 fallback when the gateway is down

- Status: accepted (option 1, decided by the owner 2026-10-20, answering Q5)
- Date: 2026-09-24 (accepted 2026-10-20)
- Milestone: M3
- Brief sections: 4.2, 4.4, 5.2, 7, 8

## Context

Level 2 callers send a template ID, version and variables instead of messages. Raw vLLM can't serve that, so the brief's "callers fall back to the direct model URL" mitigation fails for exactly those callers. This contradicts principle 4.2.

## Options

1. **Callers keep sending rendered messages plus template ID/version headers.** The gateway validates that the messages match the template (or recompiles them) and uses the ID for metrics, cache keys and coalescing. Fallback works unchanged. Prompt changes still ship with a service deploy.
2. **Client libraries render locally from a synced registry copy** (Python and TypeScript). Prompt changes ship without deploys; fallback works if the local copy is fresh. Two client libraries to build and maintain.
3. **Accept no fallback for Level 2 call sites.** Simplest; violates 4.2.

## Decision

Option 1, with validation and no recompiling. A Level 2 request is an ordinary chat-completions request whose `messages` the caller rendered from a registered template version, plus two headers:

- `X-Template-Id`: the template's ID.
- `X-Template-Version`: the version, a positive integer.

The gateway never rewrites `messages`. It checks the request against the snapshot of the registry (Postgres, refreshed out of band). The request matches when it opens with the version's static segments, byte for byte, with the same roles. A matched request is labelled with the template and version in metrics and logs. That match is what M4 coalescing and caching build on.

Anything else is forwarded unchanged, with a fail-open reason (stage `compile`): an unknown template or version, a missing or malformed version, content that isn't a string, a static-prefix mismatch, or a registry that never loaded.

The admin API renders a version from variables (`/render`), so call sites and their tests can build messages from the registry instead of keeping their own copy.

## Consequences

- AC-M3-5 holds by construction. Headers are ignored by raw vLLM, so a Level 2 call sent straight to a replica is a valid request. A test pins this.
- Prompt changes still ship with the calling service's deploy, as option 1 says. The registry makes them reviewable, versioned, validated (segment order, variables), diffable and measured, and the gateway sees which versions are still called. A call site that renders through `/render` at build or start time gets new versions without a code change. Deploy-free changes through client libraries (option 2) stay a later enhancement.
- "Staged rollout" means version lifecycle, since callers choose the version they send: `draft` (registered, not served) → `active` → `deprecated` (still served, counted separately so owners see who still calls it) → `retired` (no longer matched). More than one version can be active at a time while callers migrate.
- The gateway checks only static segments. `semi_static` and `volatile` content can't be verified without the caller's variables. That is enough for what the match is used for: a static prefix that every caller of the version shares byte for byte.
- Template stop sequences are not injected. Under option 1 the caller sends its own. ADR 0007's `max_tokens` cap applies when the caller sent none (M3-09).
- Revisit if deploy-free prompt changes become a requirement (option 2), or if callers need the gateway to re-render.
