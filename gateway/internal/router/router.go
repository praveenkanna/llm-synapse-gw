// Package router directs incoming requests to upstream replicas.
package router

import (
	"context"
	"errors"
	"net/http"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

var (
	// ErrNoReplicas is returned when a pool has no available or healthy replicas.
	ErrNoReplicas = errors.New("no replicas available")
)

// Router picks an upstream replica for a request to a given pool. It only
// selects: sending, the connect retry and passive health reports belong to
// the caller (package dispatch).
//
// Every successful Route or RouteOther returns a Lease on the replica it
// picked. The caller releases each lease exactly once, when it stops using
// that replica (Release is idempotent, so releasing on every exit path is
// safe). A lease names the replica actually acquired, so in-flight
// accounting can never decrement a replica the request did not count.
type Router interface {
	// Route selects a replica of pool for the given request.
	// Returns ErrNoReplicas if the pool has no replicas.
	Route(ctx context.Context, r *http.Request, pool string) (Lease, error)

	// RouteOther selects a replica of pool other than exclude (e.g. for connect retries).
	// Returns ErrNoReplicas if no alternate replica is available.
	RouteOther(ctx context.Context, r *http.Request, pool string, exclude string) (Lease, error)
}

// Releaser gives back the in-flight slot a Router acquired on a replica.
type Releaser interface {
	Release(replica discovery.Replica)
}

// Lease is the in-flight slot a route acquired on one replica. The zero
// Lease holds nothing.
//
// A Lease belongs to the request that acquired it: Release is idempotent but
// not safe for concurrent use, and a held Lease must not be copied (a copy
// would release the same slot again). Move it by assignment and stop using
// the source, as dispatch.Dispatcher does. It is a value type so that routing
// allocates nothing for it.
type Lease struct {
	// Replica is the replica the slot was acquired on.
	Replica discovery.Replica
	rel     Releaser // nil: nothing to give back (the router tracks no load)
}

// NewLease returns a lease on replica whose Release calls rel.Release once.
// A nil rel gives a lease whose Release does nothing, for routers that do
// not track in-flight load.
func NewLease(replica discovery.Replica, rel Releaser) Lease {
	return Lease{Replica: replica, rel: rel}
}

// Release gives the slot back. Only the first call has any effect.
func (l *Lease) Release() {
	rel := l.rel
	if rel == nil {
		return
	}
	l.rel = nil
	rel.Release(l.Replica)
}

type requestBodyKey struct{}

// WithRequestBody stores the raw request body bytes in context for affinity extraction.
func WithRequestBody(ctx context.Context, body []byte) context.Context {
	return context.WithValue(ctx, requestBodyKey{}, body)
}

// RequestBody retrieves the raw request body bytes from context.
func RequestBody(ctx context.Context) []byte {
	if ctx == nil {
		return nil
	}
	if b, ok := telemetry.FromContext(ctx).RequestBody(); ok {
		return b
	}
	if b, ok := ctx.Value(requestBodyKey{}).([]byte); ok {
		return b
	}
	return nil
}
