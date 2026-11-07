package dispatch

import (
	"context"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/health"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// Health is the passive health tracking a Dispatcher reports to.
// TrackerHealth adapts a *health.Tracker. A nil Health tracks nothing:
// nothing is reported and no attempt is watched (router.policy
// round_robin, whose picks ignore health).
type Health interface {
	// RecordTransportFailure records a passive failure of a request on rep
	// (the replica its lease names). The Dispatcher reports only hard
	// connect failures (upstream.IsHardConnectFailure: refused,
	// unreachable, NXDOMAIN) while the caller is still connected, at most
	// once per request and replica: never dial timeouts, cancellations,
	// other timeouts or failures after the request was sent.
	RecordTransportFailure(rep discovery.Replica, err error)

	// WatchFirstChunk starts a first-chunk wait for a streaming attempt on
	// rep, for stuck-engine detection; ctx is the attempt's request, where a
	// wait that cannot be watched is recorded as a fail-open. The
	// Dispatcher ends the wait as the attempt ends. It never returns nil: a
	// replica that is not watched gets NoFirstChunkWait.
	WatchFirstChunk(ctx context.Context, rep discovery.Replica) FirstChunkWait
}

// FirstChunkWait is one upstream attempt waiting for its replica's first
// body byte. Received and End may each be called any number of times, in
// any order: only the first call has an effect. Neither allocates.
type FirstChunkWait interface {
	// Received ends the wait: the replica's first body byte arrived.
	Received()
	// End ends the wait without a first body byte.
	End()
	// OnStuckEject sets f to run once if the wait's replica is ejected as a
	// stuck engine while the wait is judged still hung (no first body byte
	// well past the stuck-engine timeout). f runs on the ejecting goroutine
	// and must not block: the Dispatcher passes a context cancel. It never
	// runs after Received or End. A wait that does not watch ignores it.
	OnStuckEject(f func())
}

// NoFirstChunkWait watches nothing: for replicas that are not watched.
var NoFirstChunkWait FirstChunkWait = noFirstChunkWait{}

type noFirstChunkWait struct{}

func (noFirstChunkWait) Received()           {}
func (noFirstChunkWait) End()                {}
func (noFirstChunkWait) OnStuckEject(func()) {}

// TrackerHealth adapts passive health tracking to a Dispatcher. A nil
// tracker gives a nil Health: nothing tracked.
func TrackerHealth(t *health.Tracker) Health {
	if t == nil {
		return nil
	}
	return trackerHealth{t: t}
}

type trackerHealth struct{ t *health.Tracker }

func (h trackerHealth) RecordTransportFailure(rep discovery.Replica, err error) {
	h.t.RecordTransportFailure(rep, err)
}

// WatchFirstChunk starts a tracker wait. For a replica a reload has
// re-pointed or removed it watches nothing; with the tracker's wait registry
// full (a fail-open on the request's record) the wait is not scanned but
// still counts its first chunk.
func (h trackerHealth) WatchFirstChunk(ctx context.Context, rep discovery.Replica) FirstChunkWait {
	if w := h.t.StartFirstChunkWait(rep, telemetry.FromContext(ctx)); w != nil {
		return w
	}
	return NoFirstChunkWait
}
