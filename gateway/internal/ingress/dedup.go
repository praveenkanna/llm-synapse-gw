package ingress

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/coalesce"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/dedup"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/respcache"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

// Request coalescing and the response cache (M4, ADR 0019), as the
// handler serves them: package dedup decides whether a request follows a
// running flight, is answered from the cache or leads, and whether the
// leader's response is stored; this file serves those answers over HTTP.
// Joining happens before admission: a follower holds no slot and sends
// nothing upstream.
//
// The leader's own handler drives the upstream call exactly as an ordinary
// request would, writing through a writer that also records what it sends.
// Telemetry records belong to their handler goroutines, so a follower
// serves from the recording on its own goroutine with its own record, and
// the leader never waits for a follower.

// HeaderCache marks a response served from the response cache.
const HeaderCache = "X-Gateway-Cache"

// Codes of the 502 a follower gets when the call it shared failed before it
// sent any response.
const (
	codeSharedCallFailed = upstreamCodePrefix + "shared_call_failed"
)

// leadership is a request that leads a flight.
type leadership struct {
	lead *dedup.Leader
	rec  *telemetry.Request
	// delivered is how the exchange ended, as the progression was told; it
	// stays Cut when the exchange panicked.
	delivered dedup.Delivery
}

// deduplicate applies the response cache and coalescing to a request that
// matched tmpl. The request is answered (or its caller gone) when the cache
// or another request's flight served it, and it gets a leadership when it
// leads a flight. Otherwise it goes on as an ordinary one.
//
// budget is the request's X-Context-Budget, if assembly is on: it is part
// of the flight's key, since the leader assembles for its followers.
// budgeted (the budget is valid, so assembly runs) skips the cache, whose
// entries do not keep the X-Context-* headers that report what was dropped.
// ix is the request body's index; dedup reads it for the call only.
func (h *Handler) deduplicate(w http.ResponseWriter, r *http.Request, rec *telemetry.Request, pool string, tmpl *templates.Version, ix *bodyindex.Index, budget string, budgeted bool) (*leadership, step) {
	if tmpl == nil {
		return nil, proceed
	}
	d := h.dedup.Begin(r.Context(), rec, dedup.Request{
		Index:       ix,
		Endpoint:    r.URL.Path,
		Template:    tmpl.ID,
		Version:     tmpl.Version,
		Pool:        pool,
		Weights:     h.weights[pool],
		Encodings:   r.Header.Values("Accept-Encoding"),
		Priority:    PriorityFrom(r.Context()),
		Budget:      budget,
		Budgeted:    budgeted,
		Coalescible: tmpl.Coalescible(),
		CacheTTL:    tmpl.CacheTTL(),
	})
	switch d.Kind {
	case dedup.Gone:
		return nil, gone
	case dedup.Cached:
		return nil, serveCached(w, rec, d.Entry)
	case dedup.Follow:
		return nil, h.follow(w, r, rec, d.Sub)
	case dedup.Lead:
		return &leadership{lead: d.Leader, rec: rec}, proceed
	}
	return nil, proceed
}

// serveCached answers with a cached entry.
func serveCached(w http.ResponseWriter, rec *telemetry.Request, e respcache.Entry) step {
	hdr := w.Header()
	hdr.Set("Content-Type", e.ContentType)
	hdr.Set(HeaderCache, "hit")
	if isEventStream(hdr) {
		markUnbuffered(hdr)
	} else {
		hdr.Set("Content-Length", strconv.Itoa(len(e.Body)))
	}
	w.WriteHeader(e.Status)
	if _, err := w.Write(e.Body); err != nil {
		return gone
	}
	flushWriter(w)
	rec.FirstByte()
	return answered
}

// begin returns the writer and request the leader's exchange runs on: the
// writer records what is sent, and the request's context is detached from
// its client, so the upstream call survives the leader's client as long as
// a follower is still attached.
func (l *leadership) begin(w http.ResponseWriter, r *http.Request) (http.ResponseWriter, *http.Request) {
	ctx, w := l.lead.Start(r.Context(), w)
	return w, r.WithContext(ctx)
}

// end finishes the flight with how the exchange ended. It runs on the
// leader's goroutine, in a defer, so a panic mid-exchange still tells the
// followers their call failed.
func (l *leadership) end() {
	l.lead.Finish(l.delivered)
	if l.lead.Swallowed() && !l.rec.Disconnected() {
		// The leader's client left while followers kept the call alive: the
		// relay never saw the failure (unless the followers left too and the
		// call was cancelled), so it is counted here.
		l.rec.ClientDisconnected()
	}
}

// follow serves r from sub's flight. It returns proceed only when r must
// run as an ordinary request: its flight evicted it before it was sent a
// byte.
func (h *Handler) follow(w http.ResponseWriter, r *http.Request, rec *telemetry.Request, sub *coalesce.Subscription) step {
	defer sub.Close()
	var tail sseTail
	first := true
	err := sub.Serve(r.Context(), w, func(p []byte) {
		tail.observe(p)
		if first {
			first = false
			rec.FirstByte()
		}
	})
	if sub.Started() {
		rec.Coalesced()
	}
	switch {
	case err == nil:
		return answered
	case errors.Is(err, coalesce.ErrEvicted) && !sub.Started():
		rec.FailOpen(telemetry.StageCoalesce, coalesce.ReasonEvicted)
		return proceed
	case errors.Is(err, coalesce.ErrEvicted), errors.Is(err, coalesce.ErrFlightFailed):
		if !sub.Started() { // the leader never produced a response
			rec.FailOpen(telemetry.StageCoalesce, coalesce.ReasonFlightError)
			writeError(w, http.StatusBadGateway, "the shared upstream call failed", upstreamErrorType, codeSharedCallFailed)
			return answered
		}
		rec.ResponseFailed()
		if !isEventStream(w.Header()) {
			abortResponse()
		}
		if werr := writeStreamError(w, &tail, upstream.KindOther); werr != nil {
			return gone
		}
		return answered
	case errors.Is(err, coalesce.ErrResponseFailed):
		rec.ResponseFailed() // what it was sent ends in the leader's error event
		return answered
	default: // the caller left, or a write to it failed
		return gone
	}
}
