package router_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/health"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/loadview"
)

// Healthy-first fallback (M1-12): the fail-open round-robin and the retry
// fallback pick from the pool's healthy replicas, and from all of them only
// when none is healthy.

// fallbackRouter is an affinity router over pool "p" with replicas r0..r2
// (discovery.Static, as in production), its tracker and load view.
func fallbackRouter(t *testing.T) (*router.AffinityRouter, *health.Tracker, *loadview.View) {
	t.Helper()
	p := config.Pool{Name: "p", Replicas: []config.Replica{
		{Name: "r0", URL: "http://127.0.0.1:8001"},
		{Name: "r1", URL: "http://127.0.0.1:8002"},
		{Name: "r2", URL: "http://127.0.0.1:8003"},
	}}
	disc, err := discovery.NewStatic([]config.Pool{p})
	if err != nil {
		t.Fatal(err)
	}
	tr := health.New(health.ConfigFrom(config.Defaults().Router), disc, nil, nil)
	lv := loadview.New(loadview.Config{LoadView: config.Defaults().Router.LoadView}, disc, nil, nil)
	return router.NewAffinityRouter(disc, nil, tr, lv, nil), tr, lv
}

// unkeyed is a request with no affinity key: the router fails open to
// round-robin.
func unkeyed(t *testing.T) (context.Context, *http.Request) {
	t.Helper()
	ctx := router.WithRequestBody(context.Background(), []byte(`{"model":"m"}`))
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, "/v1/chat/completions", nil)
	if err != nil {
		t.Fatal(err)
	}
	return ctx, r
}

func assertNoInFlight(t *testing.T, lv *loadview.View) {
	t.Helper()
	for _, name := range []string{"r0", "r1", "r2"} {
		if n := lv.InFlight(name); n != 0 {
			t.Errorf("%s in-flight = %d after every lease was released, want 0", name, n)
		}
	}
}

func TestFallbackAvoidsEjectedReplicas(t *testing.T) {
	ar, tr, lv := fallbackRouter(t)
	tr.Eject("r1", health.ReasonStuckEngine)
	ctx, r := unkeyed(t)
	seen := map[string]int{}
	for range 30 {
		l, err := ar.Route(ctx, r, "p")
		if err != nil {
			t.Fatalf("Route: %v", err)
		}
		seen[l.Replica.Name]++
		l.Release()
	}
	if seen["r1"] != 0 || seen["r0"] == 0 || seen["r2"] == 0 {
		t.Fatalf("fallback picks = %v, want r0 and r2 only", seen)
	}
	assertNoInFlight(t, lv)
}

func TestFallbackAllEjectedStillServes(t *testing.T) {
	ar, tr, lv := fallbackRouter(t)
	for _, name := range []string{"r0", "r1", "r2"} {
		tr.Eject(name, health.ReasonTransportFailure)
	}
	keyed, err := http.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	if err != nil {
		t.Fatal(err)
	}
	keyed.Header.Set("X-Session-Id", "sess-all-ejected")
	ctx, r := unkeyed(t)
	seen := map[string]int{}
	for i := range 30 {
		req := r
		if i%2 == 0 {
			req = keyed // empty ring: fails open too
		}
		l, err := ar.Route(ctx, req, "p")
		if err != nil {
			t.Fatalf("Route with every replica ejected: %v (must fail open)", err)
		}
		seen[l.Replica.Name]++
		l.Release()
	}
	if len(seen) != 3 {
		t.Fatalf("picks with every replica ejected = %v, want all three", seen)
	}
	assertNoInFlight(t, lv)
}

// TestRouteOtherPrefersHealthyThenOthers: the retry after a connect
// failure goes to a healthy replica other than the failed one, else to an
// ejected one (the pool has another replica: ADR 0014), and only a pool with
// no other replica at all gives ErrNoReplicas (same-replica retry). Keyed
// requests take the ring path first, unkeyed ones the fallback directly.
func TestRouteOtherPrefersHealthyThenOthers(t *testing.T) {
	tests := []struct {
		name    string
		ejected []string
		exclude string
		want    []string // acceptable picks
	}{
		{name: "skips an ejected replica", ejected: []string{"r2"}, exclude: "r0", want: []string{"r1"}},
		{name: "only the failed replica is healthy: an ejected other", ejected: []string{"r1", "r2"}, exclude: "r0", want: []string{"r1", "r2"}},
		{name: "all ejected: any other replica", ejected: []string{"r0", "r1", "r2"}, exclude: "r0", want: []string{"r1", "r2"}},
		{name: "failed replica already ejected", ejected: []string{"r0", "r2"}, exclude: "r0", want: []string{"r1"}},
		{name: "none ejected", exclude: "r0", want: []string{"r1", "r2"}},
	}
	for _, keyed := range []bool{false, true} {
		for _, tc := range tests {
			t.Run(fmt.Sprintf("keyed=%v/%s", keyed, tc.name), func(t *testing.T) {
				ar, tr, lv := fallbackRouter(t)
				for _, name := range tc.ejected {
					tr.Eject(name, health.ReasonTransportFailure)
				}
				ctx, r := unkeyed(t)
				for i := range 10 {
					if keyed {
						r.Header.Set("X-Session-Id", fmt.Sprintf("sess-retry-%d", i))
					}
					l, err := ar.RouteOther(ctx, r, "p", tc.exclude)
					if err != nil {
						t.Fatalf("RouteOther: %v", err)
					}
					if !slices.Contains(tc.want, l.Replica.Name) {
						t.Fatalf("RouteOther picked %s, want one of %v", l.Replica.Name, tc.want)
					}
					l.Release()
				}
				assertNoInFlight(t, lv)
			})
		}
	}
}

// TestRouteOtherSingleReplicaPool: with no other replica, healthy or not,
// RouteOther refuses, so ingress retries the same replica (ADR 0014).
func TestRouteOtherSingleReplicaPool(t *testing.T) {
	disc, err := discovery.NewStatic([]config.Pool{{Name: "p", Replicas: []config.Replica{{Name: "r0", URL: "http://127.0.0.1:8001"}}}})
	if err != nil {
		t.Fatal(err)
	}
	tr := health.New(health.ConfigFrom(config.Defaults().Router), disc, nil, nil)
	lv := loadview.New(loadview.Config{LoadView: config.Defaults().Router.LoadView}, disc, nil, nil)
	ar := router.NewAffinityRouter(disc, nil, tr, lv, nil)
	ctx, r := unkeyed(t)
	for _, eject := range []bool{false, true} {
		if eject {
			tr.Eject("r0", health.ReasonTransportFailure)
		}
		if l, err := ar.RouteOther(ctx, r, "p", "r0"); !errors.Is(err, router.ErrNoReplicas) {
			t.Fatalf("ejected=%v: RouteOther = %+v, %v; want ErrNoReplicas", eject, l.Replica, err)
		}
	}
	if n := lv.InFlight("r0"); n != 0 {
		t.Fatalf("r0 in-flight = %d, want 0", n)
	}
}

// TestFallbackAllocatesNothingForTheCandidates: choosing between healthy
// and all replicas copies neither list.
func TestFallbackAllocatesNothingForTheCandidates(t *testing.T) {
	for _, ejected := range [][]string{{"r1"}, {"r0", "r1", "r2"}} {
		ar, tr, _ := fallbackRouter(t)
		for _, name := range ejected {
			tr.Eject(name, health.ReasonTransportFailure)
		}
		ctx, r := unkeyed(t)
		// The fail-open path allocates for the affinity extraction; compare
		// against the same path with every replica healthy.
		base, _, _ := fallbackRouter(t)
		route := func(a *router.AffinityRouter) func() {
			return func() {
				l, err := a.Route(ctx, r, "p")
				if err != nil {
					t.Fatal(err)
				}
				l.Release()
			}
		}
		if got, want := testing.AllocsPerRun(200, route(ar)), testing.AllocsPerRun(200, route(base)); got > want {
			t.Errorf("ejected %v: fallback allocates %.1f per route, %.1f with all healthy", ejected, got, want)
		}
	}
}

// TestKeyScanBudgetSetting: a body that costs more key-scan work than
// router.key_scan_budget lets the scan spend fails open to round-robin; the
// same body under the default budget keeps its key and so its replica.
func TestKeyScanBudgetSetting(t *testing.T) {
	// 1,500 short strings ahead of the messages: about 18,000 work units,
	// far over 1,024 and under the default 40,960.
	body := []byte(`{"tools":[` + strings.Repeat(`"a",`, 1500) + `"a"],"messages":[{"role":"user","content":"Q"}]}`)
	picks := func(budget int) map[string]int {
		ar, _, lv := fallbackRouter(t)
		ar.SetKeyScanBudget(budget)
		ctx := router.WithRequestBody(context.Background(), body)
		r, err := http.NewRequestWithContext(ctx, http.MethodPost, "/v1/chat/completions", nil)
		if err != nil {
			t.Fatal(err)
		}
		seen := map[string]int{}
		for range 12 {
			l, err := ar.Route(ctx, r, "p")
			if err != nil {
				t.Fatalf("Route: %v", err)
			}
			seen[l.Replica.Name]++
			l.Release()
		}
		assertNoInFlight(t, lv)
		return seen
	}
	if seen := picks(0); len(seen) != 1 {
		t.Errorf("default budget: picks %v, want one replica (key kept)", seen)
	}
	if seen := picks(1024); len(seen) < 2 {
		t.Errorf("budget 1024: picks %v, want round-robin over several replicas (fail open)", seen)
	}
}
