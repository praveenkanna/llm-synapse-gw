package dispatch

import (
	"context"
	"io"
	"net/http"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

// Exchange is a replica's response to a dispatched request, headers
// received. The caller relays Response and closes the Exchange (not just
// the body) on every path it leaves by, panics included: Close ends the
// attempt and gives back the replica's lease. An Exchange belongs to the
// request's goroutine; Close is idempotent but not safe for concurrent use.
type Exchange struct {
	// Response is the replica's response. Reading its Body tells
	// stuck-engine detection about the replica's first body byte, before the
	// byte is handed out, since the replica delivered it whether or not the
	// caller is still there.
	Response *http.Response
	// Replica is the replica that answered.
	Replica discovery.Replica

	lease router.Lease
	a     attempt
	body  firstChunkBody // Response.Body on a watched stream, in the same allocation
}

// newExchange wraps resp, the response to attempt a on the replica lease
// holds. The lease moves into the Exchange: *lease is left holding nothing.
// An error response ends the attempt's first-chunk wait at once, so it never
// counts as a first chunk: it says nothing about whether the engine
// generates (End still counts a stall if it came only after the
// stuck-engine timeout). That End runs first, while Send still holds the
// lease, so a panic in it is cleaned up there.
func newExchange(resp *http.Response, lease *router.Lease, a attempt) *Exchange {
	success := resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices
	if !success {
		a.wait.End()
	}
	ex := &Exchange{Response: resp, Replica: lease.Replica, lease: *lease, a: a}
	*lease = router.Lease{}
	if success && a.wait != NoFirstChunkWait {
		ex.body = firstChunkBody{ReadCloser: resp.Body, wait: a.wait}
		resp.Body = &ex.body
	}
	return ex
}

// Close closes the response body (which cancels the upstream request if it
// is still running), ends the attempt and releases the replica's lease, the
// last two deferred so a panic in one step still runs the next.
func (e *Exchange) Close() {
	defer e.lease.Release()
	defer e.a.end()
	_ = e.Response.Body.Close()
}

// firstChunkBody tells a first-chunk wait about the first byte read. It
// adds no allocation per read.
type firstChunkBody struct {
	io.ReadCloser
	wait FirstChunkWait
	seen bool
}

func (b *firstChunkBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if n > 0 && !b.seen {
		b.seen = true
		b.wait.Received()
	}
	return n, err
}

// attempt is one upstream attempt: the context it runs on, its cancel and
// its first-chunk wait.
type attempt struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
	wait   FirstChunkWait
}

// noCancel is the cancel of an attempt that runs on the caller's context.
func noCancel(error) {}

// startAttempt starts an attempt on rep for the caller's context parent. A
// streaming attempt runs on a context of its own, ended with the caller's
// (a client disconnect still cancels the upstream request), and holds a
// first-chunk wait that cancels it with upstream.ErrReplicaStuck if rep is
// ejected as a stuck engine while it still hangs: the stream then ends with
// the mid-stream SSE error event, or a 502 before its headers. Any other
// attempt runs on parent and is not watched.
func (d *Dispatcher) startAttempt(parent context.Context, rep discovery.Replica, stream bool) attempt {
	if !stream {
		return attempt{ctx: parent, cancel: noCancel, wait: NoFirstChunkWait}
	}
	ctx, cancel := context.WithCancelCause(parent)
	a := attempt{ctx: ctx, cancel: cancel, wait: NoFirstChunkWait}
	if d.health != nil {
		a.wait = d.health.WatchFirstChunk(parent, rep)
		a.wait.OnStuckEject(func() { cancel(upstream.ErrReplicaStuck) })
	}
	return a
}

// end ends the attempt: its wait without a first chunk (a no-op after
// Received) and its context.
func (a attempt) end() {
	defer a.cancel(nil)
	a.wait.End()
}
