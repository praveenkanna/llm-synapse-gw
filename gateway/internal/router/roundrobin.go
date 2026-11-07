package router

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// RoundRobin routes requests across discovered replicas in strict round-robin order.
// It tracks no in-flight load, so its leases hold nothing to release.
// It is safe for concurrent use.
type RoundRobin struct {
	disc     discovery.Discovery
	rr       map[string]*atomic.Uint64
	rrOthers atomic.Uint64
	metrics  *telemetry.Metrics
}

// NewRoundRobin builds a round-robin router over disc.
// pools provides the initial known pool names so their counters can be allocated upfront.
func NewRoundRobin(disc discovery.Discovery, pools []string, tm ...*telemetry.Metrics) *RoundRobin {
	rr := make(map[string]*atomic.Uint64, len(pools))
	for _, p := range pools {
		rr[p] = new(atomic.Uint64)
	}
	var m *telemetry.Metrics
	if len(tm) > 0 {
		m = tm[0]
	}
	return &RoundRobin{
		disc:    disc,
		rr:      rr,
		metrics: m,
	}
}

func (r *RoundRobin) cursor(pool string) *atomic.Uint64 {
	if c := r.rr[pool]; c != nil {
		return c
	}
	return &r.rrOthers
}

var _ Router = (*RoundRobin)(nil)

// Route picks the next replica of pool in round-robin order.
func (r *RoundRobin) Route(_ context.Context, _ *http.Request, pool string) (Lease, error) {
	rep, err := r.pick(pool)
	if err != nil {
		return Lease{}, err
	}
	return Lease{Replica: rep}, nil
}

// pick is next with the round-robin policy's decision and duration recorded.
func (r *RoundRobin) pick(pool string) (discovery.Replica, error) {
	start := time.Now()
	rep, err := r.next(pool)
	if r.metrics != nil {
		outcome := "round_robin"
		if err != nil {
			outcome = "no_replicas"
		}
		r.metrics.RecordRouterDecision(pool, "round_robin", outcome)
		r.metrics.RecordRouterDuration(pool, "round_robin", time.Since(start).Seconds())
	}
	return rep, err
}

// next picks the next replica of pool in round-robin order and records
// nothing.
func (r *RoundRobin) next(pool string) (discovery.Replica, error) {
	return r.nextOf(pool, r.disc.Replicas(pool))
}

// nextOf picks the next of reps (replicas of pool) in pool's round-robin
// order and records nothing. Routers that wrap RoundRobin as their
// fail-open fallback use it with the replicas they prefer, so a fallback
// decision is counted once, under the wrapping router's policy.
func (r *RoundRobin) nextOf(pool string, reps []discovery.Replica) (discovery.Replica, error) {
	if len(reps) == 0 {
		return discovery.Replica{}, ErrNoReplicas
	}
	n := r.cursor(pool).Add(1) - 1
	return reps[n%uint64(len(reps))], nil
}

// RouteOther picks a replica of pool other than exclude, in round-robin order,
// for connect failure retries (ADR 0014).
func (r *RoundRobin) RouteOther(_ context.Context, _ *http.Request, pool, exclude string) (Lease, error) {
	rep, err := r.otherOf(pool, r.disc.Replicas(pool), exclude)
	if err != nil {
		return Lease{}, err
	}
	return Lease{Replica: rep}, nil
}

// otherOf picks a replica of reps (replicas of pool) other than exclude, in
// pool's round-robin order. Like nextOf, it records nothing.
func (r *RoundRobin) otherOf(pool string, reps []discovery.Replica, exclude string) (discovery.Replica, error) {
	if len(reps) == 0 {
		return discovery.Replica{}, ErrNoReplicas
	}
	n := r.cursor(pool).Add(1) - 1
	for i := range uint64(len(reps)) {
		if rep := reps[(n+i)%uint64(len(reps))]; rep.Name != exclude {
			return rep, nil
		}
	}
	return discovery.Replica{}, ErrNoReplicas
}
