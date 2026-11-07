// Package dedup decides, for a request of a template that opted in, whether
// it shares the flight of an identical request already running, is served
// from the response cache, or leads, and whether its finished exchange is
// stored (M4, ADR 0019).
//
// It decides from the request's facts and from what the leader's relay
// reports it delivered, never from the request's telemetry. The flight
// mechanics are coalesce's and the store is respcache's (or any Cache);
// serving a follower or a cached entry over HTTP is the caller's.
package dedup

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/coalesce"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/respcache"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// Cache stores finished responses and serves them back (*respcache.Cache).
// A Get error is a fail-open miss, never an error to the caller; Put never
// blocks.
type Cache interface {
	Get(ctx context.Context, key string) (respcache.Entry, bool, error)
	Put(key string, ttl time.Duration, e respcache.Entry) string
}

// Recorder is told what dedup skipped and what a cache lookup found
// (*telemetry.Request). It is written to, never read.
type Recorder interface {
	FailOpen(stage, reason string)
	CacheLookup(hit bool)
}

// Config configures a Policy. Group nil shares nothing; Cache nil caches
// nothing.
type Config struct {
	Group        *coalesce.Group
	Cache        Cache
	KeyBodyBytes int           // coalescing.max_key_body_bytes
	MaxTTL       time.Duration // cache.max_ttl
	// OnStore, if set, counts a store's outcome when it is known at once
	// (skipped, dropped); a queued store reports its own later.
	OnStore func(outcome string)
}

// Policy is safe for concurrent use.
type Policy struct{ cfg Config }

// New returns a Policy for cfg.
func New(cfg Config) *Policy { return &Policy{cfg: cfg} }

// Request is what dedup knows of a request that matched a template. Its
// key parts are kept raw: they are joined only for a request that gets a
// key, so one that does not costs nothing.
type Request struct {
	// Index is the request body's index, the one ingress read for the
	// request (AD-15). Begin reads it during the call and never keeps it.
	Index       *bodyindex.Index
	Endpoint    string        // the request path
	Template    string        // template ID
	Version     int           // template version
	Pool        string        // pool the request resolved to
	Weights     string        // the pool's weights_version
	Encodings   []string      // the caller's Accept-Encoding lines, forwarded to the replica
	Priority    string        // the priority class: only the in-flight key holds it
	Budget      string        // X-Context-Budget when assembly is on: the leader assembles for its followers
	Budgeted    bool          // the budget is valid, so assembly runs: the cache is skipped
	Coalescible bool          // the template shares flights
	CacheTTL    time.Duration // the template's cache TTL; 0: not cached
}

// parts are what, besides the body, keys req's response.
func (req *Request) parts() coalesce.Parts {
	return coalesce.Parts{
		Endpoint: req.Endpoint,
		Template: req.Template,
		Version:  strconv.Itoa(req.Version),
		Pool:     req.Pool,
		Weights:  req.Weights,
		Encoding: strings.Join(req.Encodings, ","),
	}
}

// Kind is what a request gets.
type Kind int

// Kinds of Decision.
const (
	Pass   Kind = iota // goes on as an ordinary request
	Gone               // its caller left during the cache lookup
	Cached             // Decision.Entry answers it
	Follow             // Decision.Sub serves it from a running flight
	Lead               // it leads Decision.Leader's flight
)

// Decision is what Begin decided.
type Decision struct {
	Kind   Kind
	Entry  respcache.Entry
	Sub    *coalesce.Subscription
	Leader *Leader
}

// Begin decides how req is served. A running flight is joined before the
// cache is asked: it answers sooner than Redis and costs it nothing, so a
// burst of identical requests makes one lookup, not one each. ctx is the
// caller's and bounds the lookup. Every skip is recorded on rec with its
// reason.
func (p *Policy) Begin(ctx context.Context, rec Recorder, req Request) Decision {
	share := p.cfg.Group != nil && req.Coalescible
	caching := p.cfg.Cache != nil && req.CacheTTL > 0 && !req.Budgeted
	if !share && !caching {
		return Decision{}
	}
	stage := telemetry.StageCache
	if share {
		stage = telemetry.StageCoalesce
	}
	fp, reason := coalesce.FingerprintOf(req.Index, p.cfg.KeyBodyBytes)
	if reason == "" && !fp.Deterministic {
		reason = telemetry.ReasonNotDeterministic
	}
	if reason != "" {
		rec.FailOpen(stage, reason)
		return Decision{}
	}
	parts := req.parts()
	flightKey := fp.Key(parts, req.Priority, req.Budget)
	if share {
		if sub := p.cfg.Group.Join(flightKey); sub != nil {
			return Decision{Kind: Follow, Sub: sub}
		}
	}
	cacheKey := ""
	if caching {
		cacheKey = fp.Key(parts)
		if d, done := p.lookup(ctx, rec, cacheKey); done {
			return d
		}
	}
	if p.cfg.Group == nil {
		return Decision{} // nothing records the response, so nothing can be stored
	}
	f, sub, reason := p.cfg.Group.Acquire(flightKey, share)
	switch {
	case sub != nil:
		return Decision{Kind: Follow, Sub: sub}
	case f != nil:
		l := &Leader{p: p, flight: f}
		if caching {
			l.cacheKey, l.ttl = cacheKey, min(req.CacheTTL, p.cfg.MaxTTL)
		}
		return Decision{Kind: Lead, Leader: l}
	default:
		rec.FailOpen(stage, reason)
		return Decision{}
	}
}

// lookup asks the cache for key. It is done on a hit, or when the caller
// left; a lookup that fails is a miss with its reason recorded.
func (p *Policy) lookup(ctx context.Context, rec Recorder, key string) (Decision, bool) {
	e, hit, err := p.cfg.Cache.Get(ctx, key)
	if err != nil {
		if ctx.Err() != nil { // the caller left; nobody to answer
			return Decision{Kind: Gone}, true
		}
		reason := telemetry.ReasonRedisError
		var ce *respcache.Error
		if errors.As(err, &ce) {
			reason = ce.Reason
		}
		rec.FailOpen(telemetry.StageCache, reason)
		rec.CacheLookup(false)
		return Decision{}, false
	}
	rec.CacheLookup(hit)
	if !hit {
		return Decision{}, false
	}
	return Decision{Kind: Cached, Entry: e}, true
}

func (p *Policy) stored(outcome string) {
	if p.cfg.OnStore != nil {
		p.cfg.OnStore(outcome)
	}
}

// End is how a leader's exchange ended, as its relay saw it.
type End int

// Ends of a leader's exchange.
const (
	// Cut: it ended without saying how (a panic, or a body aborted mid-way),
	// or its caller left. Followers fail as the leader did.
	Cut End = iota
	// Answered: the gateway answered instead of the replica (a refusal, a
	// replica that could not be reached). Followers get the same answer.
	Answered
	// Failed: the replica's response failed mid-way and what was written
	// says so (a stream's error event). Followers get all of it and fail
	// too.
	Failed
	// Whole: the replica's response was relayed to its end.
	Whole
)

// Delivery is what the leader's relay delivered.
type Delivery struct {
	End End
	// Runaway: Whole, but ended on finish_reason length under a max_tokens
	// the gateway set (ADR 0007), a cut the caller did not choose.
	Runaway bool
}

// Leader leads a flight. Its own handler drives the exchange, through
// Start's writer and context, and ends it with Finish.
type Leader struct {
	p        *Policy
	flight   *coalesce.Flight
	tee      *coalesce.Writer
	cacheKey string        // set when the finished response may be stored
	ttl      time.Duration // ... for this long
}

// Start returns the writer and context the leader's exchange runs on: the
// writer records what is sent, and the context is detached from the
// leader's client, so the upstream call survives that client as long as a
// follower is still attached.
func (l *Leader) Start(ctx context.Context, w http.ResponseWriter) (context.Context, http.ResponseWriter) {
	l.tee = l.flight.Writer(w)
	return l.flight.Context(ctx), l.tee
}

// Swallowed reports whether the leader's own client failed while followers
// kept the call going, so its relay never saw that client leave.
func (l *Leader) Swallowed() bool { return l.tee != nil && l.tee.Swallowed() }

// Finish ends the flight as d says, and stores the response when it is
// storable. It must run, once, however the exchange ended: in a defer, a
// panic mid-exchange is the zero Delivery (Cut) and still tells the
// followers their call failed.
func (l *Leader) Finish(d Delivery) {
	keep := l.cacheKey != "" && d.End == Whole && !d.Runaway
	var rec coalesce.Recorded
	whole := false
	switch d.End {
	case Cut:
		l.flight.Finish(false, false)
	case Failed:
		l.flight.FinishFailed()
	default:
		rec, whole = l.flight.Finish(true, keep)
	}
	if l.cacheKey == "" {
		return
	}
	if !whole || !storable(rec) {
		l.p.stored(respcache.StoreSkipped)
		return
	}
	outcome := l.p.cfg.Cache.Put(l.cacheKey, l.ttl, respcache.Entry{
		Status: rec.Status, ContentType: rec.Header.Get("Content-Type"), Body: rec.Body,
	})
	if outcome != respcache.StoreQueued { // a queued store reports its result later
		l.p.stored(outcome)
	}
}

// storable reports whether a whole recorded response may be stored: only a
// success is, and only as what it is, since an entry keeps the Content-Type
// alone and an encoded body could not be served back.
func storable(r coalesce.Recorded) bool {
	ce := r.Header.Get("Content-Encoding")
	return r.Status == http.StatusOK && (ce == "" || ce == "identity")
}
