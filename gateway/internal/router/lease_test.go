package router_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/health"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/loadview"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// countingReleaser records every Release it receives.
type countingReleaser struct{ released []string }

func (c *countingReleaser) Release(rep discovery.Replica) { c.released = append(c.released, rep.Name) }

func TestLeaseReleaseIsIdempotent(t *testing.T) {
	rep := discovery.Replica{Pool: "p", Name: "r0"}
	rel := &countingReleaser{}
	l := router.NewLease(rep, rel)
	l.Release()
	l.Release()
	if len(rel.released) != 1 || rel.released[0] != "r0" {
		t.Fatalf("released %v, want exactly [r0]", rel.released)
	}

	var zero router.Lease
	zero.Release() // holds nothing: must not panic
	noop := router.NewLease(rep, nil)
	noop.Release()
	if noop.Replica.Name != "r0" {
		t.Fatalf("lease without a releaser lost its replica: %+v", noop.Replica)
	}
}

// TestAffinityLeasesAreSymmetric: whichever path picks the replica (ring,
// fail-open round-robin, retry via the ring or the fallback, no inner
// round-robin), the lease counts exactly one in-flight request on the replica
// it names, and releasing it (twice) brings every replica back to 0.
func TestAffinityLeasesAreSymmetric(t *testing.T) {
	reps := []discovery.Replica{
		{Pool: "p", Name: "a", BaseURL: mustURL("http://127.0.0.1:8001")},
		{Pool: "p", Name: "b", BaseURL: mustURL("http://127.0.0.1:8002")},
	}
	disc := staticDiscovery{"p": reps}
	keyed := func() *http.Request {
		r, _ := http.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		r.Header.Set("X-Session-Id", "sess-lease")
		return r
	}
	unkeyed := func() *http.Request {
		r, _ := http.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		return r
	}
	tests := []struct {
		name  string
		noRR  bool
		other bool // RouteOther excluding "a"
		req   func() *http.Request
	}{
		{name: "ring route", req: keyed},
		{name: "fail-open route", req: unkeyed},
		{name: "fail-open route without round-robin", noRR: true, req: unkeyed},
		{name: "ring retry", other: true, req: keyed},
		{name: "fallback retry", other: true, req: unkeyed},
		{name: "fallback retry without round-robin", noRR: true, other: true, req: unkeyed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tm := telemetry.NewMetrics()
			lv := loadview.New(loadview.Config{LoadView: config.Defaults().Router.LoadView}, disc, tm, nil)
			var rr *router.RoundRobin
			if !tc.noRR {
				rr = router.NewRoundRobin(disc, []string{"p"})
			}
			ar := router.NewAffinityRouter(disc, rr, health.New(health.ConfigFrom(config.Defaults().Router), disc, tm, nil), lv, tm)
			ctx := context.Background()
			var (
				l   router.Lease
				err error
			)
			if tc.other {
				l, err = ar.RouteOther(ctx, tc.req(), "p", "a")
			} else {
				l, err = ar.Route(ctx, tc.req(), "p")
			}
			if err != nil {
				t.Fatalf("route: %v", err)
			}
			if tc.other && l.Replica.Name != "b" {
				t.Fatalf("RouteOther excluding a picked %q", l.Replica.Name)
			}
			for _, rep := range reps {
				want := int64(0)
				if rep.Name == l.Replica.Name {
					want = 1
				}
				if got := lv.InFlight(rep.Name); got != want {
					t.Fatalf("in-flight on %s = %d after routing to %s, want %d", rep.Name, got, l.Replica.Name, want)
				}
			}
			l.Release()
			l.Release()
			for _, rep := range reps {
				if got := lv.InFlight(rep.Name); got != 0 {
					t.Fatalf("in-flight on %s = %d after release, want 0", rep.Name, got)
				}
			}
		})
	}
}

// A double release must not take a slot that belongs to another request.
func TestAffinityDoubleReleaseKeepsOtherRequestsSlot(t *testing.T) {
	// One replica, so both requests share it whatever the bounded load says.
	disc := staticDiscovery{"llama-pool": {{Pool: "llama-pool", Name: "solo", BaseURL: mustURL("http://127.0.0.1:8001")}}}
	tm := telemetry.NewMetrics()
	lv := loadview.New(loadview.Config{LoadView: config.Defaults().Router.LoadView}, disc, tm, nil)
	ar := router.NewAffinityRouter(disc, nil, health.New(health.ConfigFrom(config.Defaults().Router), disc, tm, nil), lv, tm)
	ctx := context.Background()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "/v1/chat/completions", nil)
	req.Header.Set("X-Session-Id", "sess-shared")

	first, err := ar.Route(ctx, req, "llama-pool")
	if err != nil {
		t.Fatal(err)
	}
	second, err := ar.Route(ctx, req, "llama-pool")
	if err != nil {
		t.Fatal(err)
	}
	if first.Replica.Name != second.Replica.Name {
		t.Fatalf("same key routed to %s then %s; the test needs one replica", first.Replica.Name, second.Replica.Name)
	}
	first.Release()
	first.Release()
	if got := lv.InFlight(second.Replica.Name); got != 1 {
		t.Fatalf("in-flight = %d after releasing one of two requests twice, want 1", got)
	}
	second.Release()
	if got := lv.InFlight(second.Replica.Name); got != 0 {
		t.Fatalf("in-flight = %d after both releases, want 0", got)
	}
}

func TestRoundRobinLeaseHoldsNothing(t *testing.T) {
	disc := staticDiscovery{"p": {{Pool: "p", Name: "a", BaseURL: mustURL("http://127.0.0.1:8001")}}}
	rr := router.NewRoundRobin(disc, []string{"p"})
	l, err := rr.Route(context.Background(), nil, "p")
	if err != nil {
		t.Fatal(err)
	}
	if l.Replica.Name != "a" {
		t.Fatalf("picked %q, want a", l.Replica.Name)
	}
	l.Release() // round-robin tracks no load: a no-op, never a panic
	l.Release()
}
