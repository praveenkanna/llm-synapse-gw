package loadview

import (
	"math"
	"sync/atomic"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
)

// prune forgets replicas not in reps, the lister's current set: their
// scraped state, their in-flight counters once idle, and their per-replica
// telemetry series (gateway_replica_info, idle gateway_inflight,
// gateway_upstream_errors_total). A counter still above zero belongs to
// requests in flight on a removed replica; it is kept until they release
// it and dropped by a later prune.
//
// An idle counter is retired by swapping 0 for retiredCounter before it is
// deleted. A counter above zero can never be retired, so it stays in the
// map until its requests release it; an IncInFlight that raced and loaded
// the counter just retired sees a negative value and retries on a fresh
// one. Hence no increment is lost to a deleted counter, and every release
// finds the counter its acquire incremented: accounting stays exact even
// for a replica removed and re-added while requests were in flight. A replica re-pointed to a new URL loses its
// scraped state (it described the old address) but keeps its in-flight
// counter.
//
// Pruning runs at the start of every scrape and probe pass, so state left by
// a pass that raced with a reload is dropped by the next one: bounded by the
// replica set.
func (v *View) prune(reps []discovery.Replica) {
	current := make(map[string]string, len(reps))
	pools := make(map[string]string, len(reps))
	for _, rep := range reps {
		current[rep.Name] = baseURL(rep)
		pools[rep.Name] = rep.Pool
	}

	v.stateMu.Lock()
	defer v.stateMu.Unlock()
	prev := v.current
	v.current = current
	v.states.Range(func(key, _ any) bool {
		name := key.(string)
		u, ok := current[name]
		if !ok {
			v.states.Delete(key)
		} else if old, known := prev[name]; known && old != u {
			v.states.Delete(key) // re-pointed: scraped state was about the old address
		}
		return true
	})
	v.inFlight.Range(func(key, val any) bool {
		if _, ok := current[key.(string)]; ok {
			return true
		}
		if val.(*atomic.Int64).CompareAndSwap(0, retiredCounter) {
			v.inFlight.CompareAndDelete(key, val)
		}
		return true
	})
	if v.metrics != nil {
		v.metrics.RetainReplicas(pools)
	}
}

// retiredCounter marks an in-flight counter prune has deleted. It is far
// enough below zero that racing increments and decrements cannot bring it
// back to zero or overflow it.
const retiredCounter = math.MinInt64 / 2

// isCurrent reports whether rep is still listed at the same address. Before
// the first prune every replica counts. Callers hold stateMu.
func (v *View) isCurrent(rep discovery.Replica) bool {
	if v.current == nil {
		return true
	}
	u, ok := v.current[rep.Name]
	return ok && u == baseURL(rep)
}

func baseURL(rep discovery.Replica) string {
	if rep.BaseURL == nil {
		return ""
	}
	return rep.BaseURL.String()
}
