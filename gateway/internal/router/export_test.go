package router

import (
	"context"
	"net/http"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/affinity"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/ring"
)

// RouteAllRefused is Route with every in-limit acquisition refused, the case
// concurrent routes cause by filling each replica between a decision's
// snapshot and its acquire, and which one goroutine cannot produce: tryAcquire
// runs in full against the real load view and finds the replica over a limit
// no load can meet. Pick decides on all its snapshots, then takes the
// least-loaded replica past the limit. r must carry a key.
func (a *AffinityRouter) RouteAllRefused(ctx context.Context, r *http.Request, pool string) (Lease, error) {
	start := time.Now()
	res := affinity.ExtractBudget(r, RequestBody(ctx), a.keyScanBudget)
	rRing, err := a.health.Ring(pool)
	if err != nil {
		return Lease{}, err
	}
	refused := func(rep discovery.Replica, limit int64) bool {
		if limit == ring.NoLimit {
			return a.tryAcquire(rep, limit)
		}
		return a.tryAcquire(rep, -1)
	}
	picked, err := rRing.Pick(res.Key, a.loadView.Load, refused, "")
	if err != nil {
		return Lease{}, err
	}
	outcome := outcomeOverflow
	if picked.OverCap {
		outcome = outcomeOverCap
	}
	a.record(pool, outcome, start)
	return a.lease(picked.Replica), nil
}
