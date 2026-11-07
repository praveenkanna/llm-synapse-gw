# 0019 — How coalescing, the response cache and budgeted context work

- Status: accepted; amended 2026-10-31 (a body past the body index's member ceiling skips these stages, budget included)
- Date: 2026-10-21
- Milestone: M4 (tasks M4-01 to M4-06)
- Brief sections: 5.5, 5.6
- Builds on: [ADR 0006](0006-level2-fallback-mode.md), [ADR 0007](0007-max-tokens-policy.md), [ADR 0011](0011-model-and-vllm-version-agnostic.md)

## Context

M4 needs the same answer to be generated once, and a long history to fit a token budget. The brief names the key ("template ID + canonical variables + model + sampling") but under ADR 0006 the gateway never renders a template, so it never holds the variables. It sees only the caller's rendered body. Several rules also pull against each other here: never buffer a stream, never block on Redis, never retry a generation, never modify prompt text, bound all state.

## Decision

### The key is the whole request, not a variable map

A coalescing or cache key is a SHA-256 over: the endpoint path, template ID and version, the pool, the pool's `weights_version`, the caller's `Accept-Encoding` (forwarded to the replica), and every top-level member of the body except `priority` and `user`. Those two are left out only when they appear once with a type vLLM accepts (a number, a string or null): a repeated or mistyped one is forwarded for the replica to refuse, so it is keyed like any other member. Members are sorted by name, a repeated name keeps its last value (as vLLM's parser does), and each value is hashed as its bytes with insignificant whitespace removed. Nothing is decoded or re-encoded, so two bodies collide only if they are the same token stream. The key never merges two requests vLLM would treat differently. A difference in key order inside a nested value, or in an escape, only costs a merge. The in-flight key also carries the priority class (below).

The `max_tokens` the gateway learns for a template (ADR 0007) is not in the key: it is set after the key is taken, so a follower gets the leader's output under the leader's cap. A response that ended on that cap is never cached (below).

Bodies that are not valid UTF-8, hold an escaped top-level key, or exceed `coalescing.max_key_body_bytes` have no key and run as ordinary requests, with a fail-open reason.

### Only deterministic requests of opted-in templates are eligible

A request is eligible when its template is `coalesce: true` (coalescing) or `cacheable: true` with a positive `cache_ttl` (cache), and the body fixes its sampling: `temperature` is exactly 0, or `seed` is an integer. A missing `temperature` means vLLM's default of 1.0, which is not deterministic. Anything else forwards as before and counts `gateway_failopen_total{stage="coalesce"|"cache",reason="not_deterministic"}`.

A template that opts in may not declare a variable that is not marked `"scope":"shared"`. Registration answers 422 naming `variables[i].scope`. This is how "templates with user-specific content can never opt in" is enforced: the default is no.

### One mechanism serves coalescing, replay and the cache

A request of an eligible template runs through a flight: the first request is the leader. Its own handler goroutine drives the upstream call exactly as today, but its response writer also appends every chunk to a bounded buffer. Later requests with the same key subscribe: they get the headers and every chunk so far, then live chunks as the leader writes them. The leader never waits on a subscriber, and a stream is relayed as it arrives, never held back. Subscribers have their own handler goroutines and their own telemetry records. The leader's record is never touched from another goroutine.

- **Cancellation.** The upstream context is detached from the leader's client. It is cancelled when the leader's client is gone and no subscriber is left. A subscriber that leaves while others remain changes nothing. The check and the closing of the flight to new joins happen under one lock, so a request is never attached to a flight that is already being cancelled.
- **Shared fate.** Everything the leader's exchange produces is shared: admission refusals (429/503 with `Retry-After`), a 502, a mid-stream error event. Subscribers join before admission, so the pool sees one request. The flight key includes the priority class, so an interactive request is never queued behind a background leader.
- **Replay bound.** A flight keeps at most `coalescing.max_replay_bytes` (default 1 MiB). While the whole response fits, any request may still join. When it grows past the bound, the flight is sealed: new requests run their own upstream call (`replay_overflow`), and chunks every subscriber has read are dropped. A subscriber more than the bound behind is evicted: a stream gets one SSE error event, a body is aborted. A slow client cannot hold more than the bound, and cannot slow the leader.
- **Limits.** `max_flights` and `max_subscribers` bound the table; a request over either runs its own call. A flight exists only while its leader's handler runs. Replay memory is at most `max_flights × (max_replay_bytes + 4 KiB)` (about 1 GiB with the defaults, so size the two together), plus at most 64 KiB per subscriber blocked in a write to its client.
- **No generation is ever retried.** A subscriber that is evicted after it was sent part of a response, or whose flight failed, gets the failure, never a second upstream call. One evicted before it was sent a byte never had a generation of its own: like a request that finds a sealed flight, it runs its own call.
- **Join before lookup.** A request of a template that is both coalescing and cacheable first joins a running flight if there is one, and looks in the cache only when it would lead. A burst of identical requests costs Redis one lookup.

### The cache stores finished, successful, small responses

After a flight ends normally with status 200 that fit the replay bound and the entry limit, the leader hands the response to a bounded background writer that does `SET key value EX ttl` on Redis. A full queue drops the write. A stream that ended on `finish_reason: length` at a cap the gateway set (ADR 0007) is not stored, since the cap, not the model, ended it. Neither is a response with a `Content-Encoding`: an entry keeps only status, `Content-Type` and body. The TTL is the template's `cache_ttl`, at most `cache.max_ttl`; every entry has one, and Redis's `maxmemory` with `allkeys-lru` is the memory ceiling. Per-entry size is also capped (`cache.max_entry_bytes`).

A lookup runs before admission, with a deadline (`cache.timeout`, 20 ms by default). Redis down, slow or returning garbage is a fail-open miss (`stage="cache"`), never an error. A hit is answered from memory at once: status, `Content-Type` and the stored body, as one write for a body and as the stored event bytes for a stream. The response `id` and `created` are the original's, as for a coalesced response.

The cache connection pool is separate from admission's (`cache.connections`, default 4).

### Budgeted context (Level 3) takes the history the caller sends

Q7 (where history lives) is open, so Level 3 reads only history in the request's own `messages`. The caller opts in with `X-Context-Budget: <tokens>`: the most prompt tokens the request may use.

- Always kept: every leading `system` and `developer` message, and the last turn. History between them is dropped oldest turn first. A request that matched a template also keeps its static prefix whole, whatever the roles (few-shot pairs), and the message its first variable is in: the kept prefix is the larger of the two. A turn is a `user` message and the messages after it up to the next `user` message, so a tool call is never separated from its result.
- Tokens are measured, not estimated. The assembler asks a replica's `/tokenize` for the whole candidate and binary-searches the number of turns kept, at most `context.max_probes` calls. The largest fitting suffix wins. Kept messages are forwarded as the caller's bytes; dropped ones are cut out as byte ranges. No message is edited or reordered.
- Every drop is reported: `X-Context-Dropped-Messages` (a count) and `X-Context-Dropped-Turns` on the response, and `gateway_context_dropped_messages_total`.
- If the kept minimum alone is over budget, the answer is 422 `context_budget_exceeded`. The caller asked for a hard limit, so forwarding an over-budget prompt would break it. If the gateway cannot measure (no replica answers, a probe fails, probes run out), the request is forwarded unchanged with `X-Context-Assembly: skipped` and the fail-open reason `context` counted. Not measuring is an optimization failing; refusing to stay within a budget is the contract.
- If measuring stops (probes run out, a probe fails) after a suffix was proven to fit, that suffix is sent, within budget and with its drops reported, marked `X-Context-Assembly: approximate` and counted as outcome `approximate`; only when nothing was proven to fit is the request skipped and forwarded unchanged.
- A count must be one `/tokenize` can make for the chat call. Members that shape the render are passed to it (`tools`, `chat_template`, `chat_template_kwargs`, `mm_processor_kwargs`, the generation-prompt flags); a body with a member that changes the render but that `/tokenize` does not take (`documents`, `reasoning_effort`) is skipped with reason `unmeasurable`, and one with a non-text content part with reason `multimodal`, since every probe would make the replica process the media. An invalid budget (not a positive integer) is skipped too (`invalid_budget`), not refused: raw vLLM ignores the header.
- Probes go to the pool's replicas that passive health has not ejected, the rest only if none answers, and the whole assembly runs under one deadline, `context.assemble_timeout` (default 4 s, twice `tokenize_timeout`); its expiry before any fit is proven is the reason `assemble_timeout`.
- A budgeted request coalesces with the budget in the flight key, so it is assembled once, by the flight's leader, and followers get the leader's `X-Context-*` headers. It bypasses the response cache: entries keep only status, `Content-Type` and body, not the `X-Context-*` headers that report what was dropped.

The cgo tokenizer (task M4-08) is not needed for this to be correct, only to make it faster. It stays deferred, with its follow-ups, to the hardware gate.

## Consequences

- Identical concurrent requests cost the GPU one generation; callers' responses share an `id`.
- Adding `X-Context-Budget` to a request costs a few `/tokenize` calls, bounded by `context.max_probes`.
- A coalesced stream's subscribers see the same bytes at the same moments the leader does, plus a replay for late joiners.
- Redis with `allkeys-lru` can evict routing state to make room for cache entries. The compose `REDIS_MAXMEMORY` default is sized for that, and a separate Redis for the cache is a config change (`cache.redis.addr`).
- Q6 (is the shared summary identical for every user) and Q7 still gate any production opt-in. Nothing is on by default.

## Amendment 2026-10-31 (AD-15)

The gateway's three top-level body readers now share one index of the request body, with a ceiling of 1024 top-level members (`bodyindex.MaxMembers`). A body past it is valid JSON that vLLM serves, so it is still routed by its model and its `priority` is still handled (ADR 0018), but this ADR's stages do not run on it. Coalescing and the response cache fail open with the reason `too_many_members`, after the size and UTF-8 rules, which come first.

Context assembly does too, and that narrows the rule above that only not being able to measure justifies a skip. A body past the ceiling with a valid `X-Context-Budget` is forwarded whole and marked `X-Context-Assembly: skipped`, even when its history could have been measured and cut, or it is over budget and would have been answered 422.

Within the ceiling nothing changes: the budget stays the contract, with the skips listed above. Past the ceiling it is given up because assembling such a body would need a second way to find and replace `messages`, for a request shape nothing on the OpenAI wire produces (bodies have a few dozen members at most). The budget is a limit the caller sets on itself, so a caller that sends such a body gives up only its own limit.
