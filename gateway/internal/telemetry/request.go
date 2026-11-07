package telemetry

import (
	"context"
	"time"
)

// Request is the telemetry record of one request on the OpenAI-wire routes.
// The Middleware creates it and puts it in the request context; request-path
// stages annotate it (pool, replica, fail-open reasons, upstream errors, first
// byte), and the Middleware turns it into metrics and one log line when the
// handler returns.
//
// A Request belongs to its handler goroutine and is not safe for concurrent
// use. Every method is a no-op on a nil *Request, so stages can call them
// unconditionally when no Middleware is installed (for example in tests).
type Request struct {
	m     *Metrics
	start time.Time
	now   func() time.Time
	trace traceContext

	caller        string // authenticated caller name (config), never a token
	authRejection string // auth rejection reason, when auth answered 401
	pool, replica string
	class         string
	inflight      *gauge // held while a replica is serving the request
	retries       int    // connect retries (another replica, or the same one after a backoff)

	firstByte     time.Duration
	hasFirstByte  bool
	firstByteHook func(ttft time.Duration)

	queueWait time.Duration // time spent in the admission queue
	queued    bool

	failOpen       []string // "stage:reason"; a handful at most (a stage recorded per attempt, such as stuck_engine, repeats on a retry)
	upstreamError  string   // kind of the last upstream failure
	disconnected   bool
	responseFailed bool // upstream failed after the response status was sent

	tmpl *templateRecord // set when the request matched a template version

	// Inline request-scoped state carried on the single Middleware context to
	// avoid per-stage r.WithContext and header-slice allocations on the hot path.
	tpHeader     [1]string
	callerSet    bool
	callerAuth   bool
	prioritySet  bool
	reqBody      []byte
	hasReqBody   bool
	cacheOutcome string

	// Cached affinity extraction result across pre-byte connect retries (P3.2).
	affinityKey           string
	affinityTier          uint8
	affinityHeaderTooLong bool
	affinityReason        string
	hasAffinity           bool
}

type requestKey struct{}

// FromContext returns the request's telemetry record, or nil if none.
func FromContext(ctx context.Context) *Request {
	r, _ := ctx.Value(requestKey{}).(*Request)
	return r
}

func withRequest(ctx context.Context, r *Request) context.Context {
	return context.WithValue(ctx, requestKey{}, r)
}

// TraceID returns the request's W3C trace ID ("" on a nil record).
func (r *Request) TraceID() string {
	if r == nil {
		return ""
	}
	return r.trace.traceID
}

// Route records the pool the request was resolved to and the replica picked
// ("" when the pool had none). A picked replica counts as in flight until the
// request completes. Calling it again with another replica moves the in-flight
// count there; use Reroute for a retry, which also counts it.
func (r *Request) Route(pool, replica string) {
	if r == nil {
		return
	}
	if r.inflight != nil && (pool != r.pool || replica != r.replica) {
		r.inflight.add(-1)
		r.inflight = nil
	}
	r.pool, r.replica = pool, replica
	if replica != "" && r.inflight == nil {
		r.inflight = r.m.inflight.with(pool, replica, r.class)
		r.inflight.add(1)
	}
}

// Reroute records a connect retry after a failed attempt (nothing reached
// the replica): the request is now sent to replica, in the same pool. That is
// another replica when the pool has one, or the same replica after a backoff
// when it has no other (ADR 0014). The in-flight count moves to replica (it
// stays put for the same replica), and gateway_retries_total{pool,reason}
// counts the retry. reason is a fixed, low-cardinality value such as the
// upstream failure kind ("connect").
func (r *Request) Reroute(replica, reason string) {
	if r == nil {
		return
	}
	pool := r.pool
	if pool == "" {
		pool = PoolNone
	}
	r.m.retries.with(pool, reason).inc()
	r.retries++
	r.Route(r.pool, replica)
}

// ResponseFailed records that the upstream failed after the response status
// was sent to the caller (mid-stream, or mid-body on a non-streaming
// response): the caller got an incomplete response under a success status.
// The request is then counted and logged with a distinct outcome
// (OutcomeStreamError, or OutcomeAborted when the connection was aborted)
// instead of the upstream status.
func (r *Request) ResponseFailed() {
	if r == nil {
		return
	}
	r.responseFailed = true
}

// FailOpen records that stage was skipped for reason and the request went on
// unchanged.
func (r *Request) FailOpen(stage, reason string) {
	if r == nil {
		return
	}
	r.m.failOpen.with(stage, reason).inc()
	r.failOpen = append(r.failOpen, stage+":"+reason)
}

// UpstreamError records an upstream failure of the given kind on replica,
// before or during the response.
func (r *Request) UpstreamError(replica, kind string) {
	if r == nil {
		return
	}
	r.m.upstreamErrors.with(replica, kind).inc()
	r.upstreamError = kind
}

// ClientDisconnected records that the caller went away before the response
// was complete. Only the first call per request counts. The request is then
// counted and logged as 499 when no status had been sent, or with the
// distinct outcome OutcomeClientClosed when it went away mid-response.
func (r *Request) ClientDisconnected() {
	if r == nil || r.disconnected {
		return
	}
	r.disconnected = true
	r.m.clientDisconnects.with().inc()
}

// FirstByte records that the first response body byte was relayed to the
// caller (TTFT). Only the first call per request counts.
func (r *Request) FirstByte() {
	if r == nil || r.hasFirstByte {
		return
	}
	r.hasFirstByte = true
	r.firstByte = r.now().Sub(r.start)
	if r.firstByteHook != nil {
		r.firstByteHook(r.firstByte)
	}
}

// OnFirstByte sets a function called once with the request's TTFT when its
// first response body byte is relayed (admission's shedding signal). It runs
// on the relay goroutine, so it must be quick and must not block.
func (r *Request) OnFirstByte(hook func(ttft time.Duration)) {
	if r == nil {
		return
	}
	r.firstByteHook = hook
}

// Queued records the time the request waited in the admission queue; it
// goes on the request's log line.
func (r *Request) Queued(wait time.Duration) {
	if r == nil {
		return
	}
	r.queued, r.queueWait = true, wait
}

// SetCaller records the name of the caller the request was authenticated as
// (a configured caller name, or the anonymous caller when auth is disabled).
// It goes on the log line only, never on a metric label. Pass the name, never
// the token.
func (r *Request) SetCaller(name string) {
	if r == nil {
		return
	}
	r.caller = name
}

// SetCallerIdentity records both the caller's name and whether it authenticated
// with a valid token, so downstream stages can read auth.CallerFrom without a
// second context.WithValue + r.WithContext copy.
func (r *Request) SetCallerIdentity(name string, authenticated bool) {
	if r == nil {
		return
	}
	r.caller = name
	r.callerAuth = authenticated
	r.callerSet = true
}

// CallerIdentity returns the caller stored by SetCallerIdentity, if any.
func (r *Request) CallerIdentity() (name string, authenticated, ok bool) {
	if r == nil || !r.callerSet {
		return "", false, false
	}
	return r.caller, r.callerAuth, true
}

// Now returns the request's clock reading (or time.Now on a nil record).
func (r *Request) Now() time.Time {
	if r == nil || r.now == nil {
		return time.Now()
	}
	return r.now()
}

// AuthRejected records that caller auth answered 401 for reason (for example
// missing_api_key or invalid_api_key). Only the first call per request counts.
func (r *Request) AuthRejected(reason string) {
	if r == nil || r.authRejection != "" {
		return
	}
	r.authRejection = reason
	r.m.authRejections.with(reason).inc()
}

// SetClass sets the request's priority class.
func (r *Request) SetClass(class string) {
	if r == nil || class == "" {
		return
	}
	if r.inflight != nil && r.class != class {
		r.inflight.add(-1)
		r.class = class
		if r.replica != "" {
			r.inflight = r.m.inflight.with(r.pool, r.replica, r.class)
			r.inflight.add(1)
		}
		return
	}
	r.class = class
}

// SetPriorityClass sets the request's priority class and marks it as resolved
// so ingress.PriorityFrom can read it without a separate context.WithValue.
func (r *Request) SetPriorityClass(class string) {
	if r == nil {
		return
	}
	r.SetClass(class)
	r.prioritySet = true
}

// PriorityClass returns the priority class stored by SetPriorityClass, if any.
func (r *Request) PriorityClass() (string, bool) {
	if r == nil || !r.prioritySet {
		return "", false
	}
	return r.class, true
}

// SetRequestBody stores the request body slice for affinity routing without
// allocating a new context/http.Request wrapper, invalidating any cached
// affinity key if the body changed.
func (r *Request) SetRequestBody(body []byte) {
	if r == nil {
		return
	}
	r.reqBody = body
	r.hasReqBody = true
	r.hasAffinity = false
}

// RequestBody returns the request body stored by SetRequestBody, if any.
func (r *Request) RequestBody() ([]byte, bool) {
	if r == nil || !r.hasReqBody {
		return nil, false
	}
	return r.reqBody, true
}

// SetCacheOutcome records the low-cardinality dedup/cache outcome for the
// completion log line ("hit", "miss", "coalesced").
func (r *Request) SetCacheOutcome(outcome string) {
	if r == nil || outcome == "" {
		return
	}
	r.cacheOutcome = outcome
}

// SetAffinityResult caches the extracted affinity key for connect retries.
func (r *Request) SetAffinityResult(key string, tier uint8, headerTooLong bool, reason string) {
	if r == nil {
		return
	}
	r.affinityKey = key
	r.affinityTier = tier
	r.affinityHeaderTooLong = headerTooLong
	r.affinityReason = reason
	r.hasAffinity = true
}

// AffinityResult returns the cached affinity extraction result, if any.
func (r *Request) AffinityResult() (key string, tier uint8, headerTooLong bool, reason string, ok bool) {
	if r == nil || !r.hasAffinity {
		return "", 0, false, "", false
	}
	return r.affinityKey, r.affinityTier, r.affinityHeaderTooLong, r.affinityReason, true
}

// Class returns the request's priority class.
func (r *Request) Class() string {
	if r == nil {
		return ""
	}
	return r.class
}
