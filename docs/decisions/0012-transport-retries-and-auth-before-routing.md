# 0012 — Transport retries on Idempotency-Key, and auth before routing

- Status: accepted
- Date: 2026-09-28
- Milestone: M0
- Brief sections: 4 (principles 1, 3), 8 (never silently retry a generation)

Two wire behaviours the M0 review (M0-06, M0-08) found in the code as built. Neither is a bug, but both surprise a reader of the design principles, so they are recorded here.

## Part 1: net/http may resend a POST that carries `Idempotency-Key`

### Context

`design-principles.md` says: *never silently retry a non-idempotent generation; retrying is allowed only before any byte is sent upstream (connect failure) or when the caller opted in.* The gateway's own retry (`upstream.Retryable`) honours this: it retries on another replica only after a dial failure.

Go's `http.Transport` has its own retry, below the gateway's code. Checked against the Go 1.27.1 source (`net/http/transport.go` `shouldRetryRequest`, `net/http/request.go` `isReplayable`), a failed attempt is sent again on a new connection to the **same replica** only when all of these hold:

1. The failed connection was a **reused** keep-alive connection (never a fresh one).
2. The body can be rewound. `upstream.newRequest` builds the body from `bytes.NewReader`, so `GetBody` is set for every request with a body.
3. One of:
   - **Nothing was written** (`nothingWrittenError`): the connection died before any request byte left. Retried for every method. This is the "before any byte is sent upstream" case the principles already allow.
   - The request was written, then the connection failed **before the first response byte** (`transportReadFromServerError`, or the server closed the idle connection as the request went out, `errServerClosedIdle`). Retried only if the request is *replayable*: method GET/HEAD/OPTIONS/TRACE, **or the request carries an `Idempotency-Key` or `X-Idempotency-Key` header**.

The gateway forwards caller headers unchanged apart from hop-by-hop and caller-only ones (`upstream/headers.go`), so a caller's `Idempotency-Key` reaches the Transport. A `POST /v1/chat/completions` with that header can therefore reach the replica twice, when a keep-alive connection breaks after the request was written but before any response came back.

Limits of the Transport retry:

- It never happens once response headers have come back. The caller never sees duplicated or spliced output; streams are unaffected after their first byte.
- It stays on the same replica. It is not the gateway's cross-replica retry.
- It needs a reused connection, so a replica that keeps hanging up can't cause an endless loop.
- vLLM does not deduplicate by `Idempotency-Key`. A GitHub code search of `vllm-project/vllm` on 2026-09-28 found no handling of the header in its entrypoints. A resend can start a second generation. vLLM normally aborts the first when its connection drops, but some GPU work may be done twice.

### Options

1. **Treat the header as caller opt-in (keep stdlib behaviour).** A caller that sends `Idempotency-Key` is saying the request may safely be repeated, which is the header's meaning in the IETF draft and in Go's documented behaviour (golang/go#19943). Pros: no code change; the header still reaches the upstream unchanged; the safe nothing-written retry keeps working. Cons: the gateway can't see or count these retries (they happen inside the Transport), and the replica may do the work twice.
2. **Clear `GetBody` on upstream requests.** This turns off both retry paths for requests with a body. Cons: it also turns off the nothing-written retry, so the keep-alive race (the replica closes an idle connection just as a request goes out) becomes a 502 under load. That is a fail-open regression, and it protects against a retry the caller asked for.
3. **Strip `Idempotency-Key` / `X-Idempotency-Key` before forwarding.** Cons: changes what the caller sent (principle 1, wire compatibility), and the header may matter to a proxy or a future upstream that does honour it.
4. **Store the header under a non-canonical map key** so `Header.has` misses it. Cons: relies on stdlib internals; a Go release could silently undo it.
5. **Wrap the Transport and send each attempt through a fresh `RoundTrip` with retries disabled.** No stdlib option exists; this means reimplementing connection-pool retry logic. Too much for M0 when option 1 meets the principle.

### Decision

Option 1. A caller-supplied `Idempotency-Key` or `X-Idempotency-Key` header **counts as caller opt-in** under `design-principles.md` ("or when the caller opted in"). The gateway forwards the header unchanged and lets `net/http` resend on a broken reused connection before any response byte. Without the header, the only Transport retry is the nothing-written one, which the principles already allow.

## Part 2: with auth on, unknown `/v1/*` paths return 401 before 404

### Context

`cmd/gateway/main.go` mounts every OpenAI-wire route under one `/v1/` prefix handler wrapped in the auth middleware. The inner mux (`ingress.Register`) is consulted only after authentication succeeds. So with `auth.enabled: true`:

| Request | No or invalid token | Valid token |
| --- | --- | --- |
| Known route, right method (`POST /v1/chat/completions`) | 401 | forwarded |
| Known route, wrong method (`GET /v1/chat/completions`) | 401 | 405 |
| Unknown path under `/v1/` (`/v1/embeddings`, `/v1/nope`) | 401 | 404 |
| Path outside `/v1/` (`/nope`) | 404 (no auth) | 404 |
| Health (`/livez`, `/readyz`) | 200 (never behind auth) | 200 |

### Decision

Keep it. Unauthenticated callers learn nothing about which routes exist or which methods they accept. vLLM behaves the same way when it runs with `--api-key`: its `AuthenticationMiddleware` (`vllm/entrypoints/serve/middleware/authenticate.py`, checked 2026-09-28) guards every path starting with `/v1` before routing, and it too returns 401 for unknown paths. Two differences remain:

- The gateway's 401 body is OpenAI-style (`{"error":{"message","type","param","code"}}` with `code` `missing_api_key` or `invalid_api_key`). vLLM returns `{"error":"Unauthorized"}`. Clients key on the status, not the body.
- vLLM lets `OPTIONS` through without a token. The gateway does not; there is no browser/CORS caller on this path.

With `auth.enabled: false` the table collapses to plain routing: 404 and 405 come back directly.

## Consequences

- Callers that send `Idempotency-Key` on generations accept that a replica may process the request twice after a connection failure. Documented in `docs/architecture.md` (wire-compatibility notes).
- These Transport retries are invisible to gateway metrics. Counting them would need an `httptrace` hook (`GotConn` firing more than once per request). Not needed for M0 acceptance; left as a follow-up.
- A stdlib test that pins the Transport behaviour (a POST with the header is resent once on a reused connection the server drops after reading the request) would catch a Go upgrade that changes it. Also a follow-up.
- Anyone debugging a 401 on a mistyped path must check the path as well as the token. The 401 body names the auth problem, not the route.
- Revisit Part 1 if: upstream moves to HTTP/2 (different retry rules), the M4 coalescer or response cache starts keying on `Idempotency-Key`, a harness run shows duplicate generations, or a Go release changes `isReplayable`. Revisit Part 2 if a browser caller needs CORS preflight.
