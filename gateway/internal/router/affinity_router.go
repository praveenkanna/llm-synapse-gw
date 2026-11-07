package router

import (
	"context"
	"net/http"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/affinity"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/health"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/loadview"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/ring"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// AffinityRouter routes requests based on conversation affinity keys using a consistent-hash
// ring with bounded load, falling back gracefully to round-robin on any failure.
type AffinityRouter struct {
	rr       *RoundRobin // fail-open fallback; never nil
	health   *health.Tracker
	loadView *loadview.View
	metrics  *telemetry.Metrics
	inflight *inflightReleaser // nil when there is no load view

	// keyScanBudget is the key scan budget in work units (router.key_scan_budget);
	// 0 means affinity.DefaultKeyScanBudget.
	keyScanBudget int
}

var _ Router = (*AffinityRouter)(nil)

// Router decision outcomes recorded by AffinityRouter, beyond the fail-open
// ones ("failopen": no affinity key or no load view; "no_healthy_ring";
// "no_healthy_replicas" when the fallback finds no replica either).
const (
	outcomeAffinity = "affinity" // the key's primary owner
	outcomeOverflow = "overflow" // a later ring candidate: the primary was at cap + slack
	outcomeOverCap  = "over_cap" // concurrent routes filled every replica; least-loaded taken past cap + slack
	outcomeRetry    = "retry"    // RouteOther
)

// NewAffinityRouter constructs an AffinityRouter over discovered replicas and sub-components.
// rr is the fail-open fallback; nil gives the router its own round-robin over
// disc. The fallback's picks are recorded once, as this router's decision,
// whether or not rr was built with metrics.
func NewAffinityRouter(disc discovery.Discovery, rr *RoundRobin, tracker *health.Tracker, lv *loadview.View, tm *telemetry.Metrics) *AffinityRouter {
	if rr == nil {
		rr = NewRoundRobin(disc, nil)
	}
	a := &AffinityRouter{
		rr:       rr,
		health:   tracker,
		loadView: lv,
		metrics:  tm,
	}
	if lv != nil {
		a.inflight = &inflightReleaser{loadView: lv}
	}
	return a
}

// SetKeyScanBudget sets how many work units the key scan may spend on one
// request body (router.key_scan_budget); 0 or less restores the default.
// Call it before the router serves requests.
func (a *AffinityRouter) SetKeyScanBudget(units int) { a.keyScanBudget = units }

// inflightReleaser gives back the load-view in-flight slot an AffinityRouter
// lease holds.
type inflightReleaser struct {
	loadView *loadview.View
}

// Release decrements replica's in-flight count.
func (r *inflightReleaser) Release(replica discovery.Replica) {
	r.loadView.DecInFlight(replica.Name)
}

// acquire counts one more request in flight on rep and returns the lease
// that gives it back. Fallback picks go through here; ring picks acquire in
// tryAcquire and wrap the slot with lease. Either way acquisition and
// release are symmetric on every path.
func (a *AffinityRouter) acquire(rep discovery.Replica) Lease {
	if a.inflight == nil {
		return Lease{Replica: rep}
	}
	a.loadView.IncInFlight(rep.Name)
	return a.lease(rep)
}

// lease wraps an in-flight slot already counted on rep.
func (a *AffinityRouter) lease(rep discovery.Replica) Lease {
	if a.inflight == nil {
		return Lease{Replica: rep}
	}
	return NewLease(rep, a.inflight)
}

// tryAcquire is the ring's AcquireFunc. It counts the slot first and checks
// the limit (cap + slack) after, so of several concurrent routes racing for
// the last slot under it, the ones that pushed the replica over see that and
// back off to the next candidate: a held slot never leaves the replica above
// the limit its own decision computed. That limit comes from per-replica
// counters read one at a time, so concurrent routes can inflate a decision's
// total slightly; TestAffinityConcurrentBurstRespectsCap measures the effect.
// It runs only when loadView is set.
func (a *AffinityRouter) tryAcquire(rep discovery.Replica, limit int64) bool {
	a.loadView.IncInFlight(rep.Name)
	if limit == ring.NoLimit || a.loadView.Load(rep) <= limit {
		return true
	}
	a.loadView.DecInFlight(rep.Name)
	return false
}

// Route picks a replica for pool using tiered affinity key extraction and consistent hashing with bounded load.
// If any stage fails (missing key, unparseable body, no ring, overflow), it fails open to round-robin.
// Each call records exactly one decision and one duration.
func (a *AffinityRouter) Route(ctx context.Context, r *http.Request, pool string) (Lease, error) {
	start := time.Now()
	lease, outcome, err := a.route(ctx, r, pool)
	a.record(pool, outcome, start)
	return lease, err
}

func (a *AffinityRouter) route(ctx context.Context, r *http.Request, pool string) (Lease, string, error) {
	if a.health == nil || a.loadView == nil {
		return a.fallback(pool, "failopen")
	}

	res := a.extract(ctx, r)
	if res.HeaderTooLong {
		a.failOpen(ctx, telemetry.StageAffinityHeader, affinity.ReasonSessionHeaderTooLong)
	}
	// Tier 3: Fail-open to round-robin if no valid affinity key was extracted
	if res.Tier == affinity.TierNone {
		a.failOpen(ctx, telemetry.StageAffinity, res.Reason)
		return a.fallback(pool, "failopen")
	}

	// Pick never refuses a non-empty ring (past the limit it takes the
	// least-loaded replica), so its only error is an empty ring: the same
	// fail-open as no ring at all.
	var picked ring.Result
	rRing, err := a.health.Ring(pool)
	if err == nil {
		picked, err = rRing.Pick(res.Key, a.loadView.Load, a.tryAcquire, "")
	}
	if err != nil {
		a.failOpen(ctx, telemetry.StageAffinity, reasonNoHealthyRing)
		return a.fallback(pool, reasonNoHealthyRing)
	}
	outcome := outcomeOverflow
	switch {
	case picked.OverCap:
		outcome = outcomeOverCap
	case picked.Primary:
		outcome = outcomeAffinity
	}
	return a.lease(picked.Replica), outcome, nil
}

// Tier codes for the affinity result cached on the request's telemetry
// record (telemetry cannot import affinity).
var tierCodes = [...]affinity.Tier{affinity.TierNone, affinity.TierHeader, affinity.TierPrefixHash}

func tierCode(t affinity.Tier) uint8 {
	for i, c := range tierCodes {
		if c == t {
			return uint8(i)
		}
	}
	return 0
}

// extract returns the request's affinity key. The result is cached on the
// request's telemetry record, so a connect retry (RouteOther) reuses it
// instead of scanning the body again; without a record it extracts.
func (a *AffinityRouter) extract(ctx context.Context, r *http.Request) affinity.Result {
	rec := telemetry.FromContext(ctx)
	if key, tier, tooLong, reason, ok := rec.AffinityResult(); ok && int(tier) < len(tierCodes) {
		return affinity.Result{Key: key, Tier: tierCodes[tier], HeaderTooLong: tooLong, Reason: reason}
	}
	res := affinity.ExtractBudget(r, RequestBody(ctx), a.keyScanBudget)
	rec.SetAffinityResult(res.Key, tierCode(res.Tier), res.HeaderTooLong, res.Reason)
	return res
}

// RouteOther picks an alternate replica (excluding exclude) for connect retry.
// It records one duration, and a "retry" decision when the ring picked. It
// records no fail-open: Route already did for this request.
func (a *AffinityRouter) RouteOther(ctx context.Context, r *http.Request, pool, exclude string) (Lease, error) {
	start := time.Now()
	lease, outcome, err := a.routeOther(ctx, r, pool, exclude)
	a.record(pool, outcome, start)
	return lease, err
}

func (a *AffinityRouter) routeOther(ctx context.Context, r *http.Request, pool, exclude string) (Lease, string, error) {
	if a.health != nil && a.loadView != nil {
		res := a.extract(ctx, r)
		if res.Tier != affinity.TierNone {
			if rRing, err := a.health.Ring(pool); err == nil {
				if picked, err := rRing.Pick(res.Key, a.loadView.Load, a.tryAcquire, exclude); err == nil {
					return a.lease(picked.Replica), outcomeRetry, nil
				}
			}
		}
	}
	rep, err := a.otherReplica(pool, exclude)
	if err != nil {
		return Lease{}, "", err
	}
	return a.acquire(rep), "", nil
}

// otherReplica is the retry fallback's pick (ADR 0014): a healthy replica
// of pool other than exclude, else an ejected one other than exclude (the
// pool has another replica, so the retry goes there, as it did before
// health tracking), else ErrNoReplicas and ingress retries exclude itself
// after the connect-retry backoff. Neither list is copied.
func (a *AffinityRouter) otherReplica(pool, exclude string) (discovery.Replica, error) {
	if a.health != nil {
		if rep, err := a.rr.otherOf(pool, a.health.Healthy(pool), exclude); err == nil {
			return rep, nil
		}
	}
	return a.rr.otherOf(pool, a.rr.disc.Replicas(pool), exclude)
}

// fallback is the fail-open round-robin pick. It returns outcome for the
// decision, or "no_healthy_replicas" when the pool has no replica at all.
func (a *AffinityRouter) fallback(pool, outcome string) (Lease, string, error) {
	rep, err := a.rr.nextOf(pool, a.fallbackReplicas(pool))
	if err != nil {
		return Lease{}, "no_healthy_replicas", err
	}
	return a.acquire(rep), outcome, nil
}

// fallbackReplicas is what the fail-open round-robin picks from: the
// pool's healthy replicas when it has any, every replica only when none is
// healthy, so an all-ejected pool still serves (fail open). Neither list is
// copied: the request path allocates nothing here.
//
// The healthy list is the tracker's applied replica set, so between a
// discovery reload and the tracker's SyncDiscovery (run at once on
// Inventory.Changed, normally milliseconds) it can still name a replica the
// reload just removed, exactly like the ring; a request sent there fails
// to connect and is retried once (ADR 0014).
func (a *AffinityRouter) fallbackReplicas(pool string) []discovery.Replica {
	if a.health != nil {
		if healthy := a.health.Healthy(pool); len(healthy) > 0 {
			return healthy
		}
	}
	return a.rr.disc.Replicas(pool)
}

// record counts one routing decision (none when outcome is "") and its duration.
func (a *AffinityRouter) record(pool, outcome string, start time.Time) {
	if a.metrics == nil {
		return
	}
	if outcome != "" {
		a.metrics.RecordRouterDecision(pool, "affinity", outcome)
	}
	a.metrics.RecordRouterDuration(pool, "affinity", time.Since(start).Seconds())
}

// reasonNoHealthyRing is both the affinity fail-open reason and the route
// decision outcome when the pool's ring has no healthy replica.
const reasonNoHealthyRing = "no_healthy_ring"

// failOpen records that part of affinity routing was skipped for the request:
// on its telemetry record, which counts the metric and puts the reason on the
// request's log line, or only as a metric when the request has none (a
// router used outside the request path, as in benchmarks).
func (a *AffinityRouter) failOpen(ctx context.Context, stage, reason string) {
	if rec := telemetry.FromContext(ctx); rec != nil {
		rec.FailOpen(stage, reason)
		return
	}
	if a.metrics != nil {
		a.metrics.FailOpen(stage, reason)
	}
}
