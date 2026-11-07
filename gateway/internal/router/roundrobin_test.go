package router_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

type staticDiscovery map[string][]discovery.Replica

func (s staticDiscovery) Replicas(pool string) []discovery.Replica {
	return s[pool]
}

func (s staticDiscovery) Pools() []string {
	var pools []string
	for k := range s {
		pools = append(pools, k)
	}
	return pools
}

func (s staticDiscovery) AllReplicas() []discovery.Replica {
	var all []discovery.Replica
	for _, reps := range s {
		all = append(all, reps...)
	}
	return all
}

func mustURL(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil {
		panic(err)
	}
	return u
}

func TestRoundRobinRoute(t *testing.T) {
	disc := staticDiscovery{
		"large": {
			{Pool: "large", Name: "rep1", BaseURL: mustURL("http://127.0.0.1:8001")},
			{Pool: "large", Name: "rep2", BaseURL: mustURL("http://127.0.0.1:8002")},
			{Pool: "large", Name: "rep3", BaseURL: mustURL("http://127.0.0.1:8003")},
		},
		"empty": {},
	}

	rr := router.NewRoundRobin(disc, []string{"large"})
	ctx := context.Background()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://gateway/v1/chat/completions", nil)

	// Sequential round-robin
	expected := []string{"rep1", "rep2", "rep3", "rep1", "rep2"}
	for i, want := range expected {
		rep, err := rr.Route(ctx, req, "large")
		if err != nil {
			t.Fatalf("call %d: unexpected error: %v", i, err)
		}
		if rep.Replica.Name != want {
			t.Errorf("call %d: got replica %s, want %s", i, rep.Replica.Name, want)
		}
	}

	// Empty pool
	_, err := rr.Route(ctx, req, "empty")
	if err != router.ErrNoReplicas {
		t.Errorf("empty pool: got error %v, want ErrNoReplicas", err)
	}

	// Unknown pool
	_, err = rr.Route(ctx, req, "unknown")
	if err != router.ErrNoReplicas {
		t.Errorf("unknown pool: got error %v, want ErrNoReplicas", err)
	}
}

func TestRoundRobinRouteOther(t *testing.T) {
	disc := staticDiscovery{
		"large": {
			{Pool: "large", Name: "rep1", BaseURL: mustURL("http://127.0.0.1:8001")},
			{Pool: "large", Name: "rep2", BaseURL: mustURL("http://127.0.0.1:8002")},
		},
		"single": {
			{Pool: "single", Name: "only", BaseURL: mustURL("http://127.0.0.1:8001")},
		},
	}

	rr := router.NewRoundRobin(disc, []string{"large", "single"})
	ctx := context.Background()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://gateway/v1/chat/completions", nil)

	// Multi-replica: RouteOther excludes rep1
	other, err := rr.RouteOther(ctx, req, "large", "rep1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if other.Replica.Name != "rep2" {
		t.Errorf("got %s, want rep2", other.Replica.Name)
	}

	// Single replica: RouteOther returns ErrNoReplicas
	_, err = rr.RouteOther(ctx, req, "single", "only")
	if err != router.ErrNoReplicas {
		t.Errorf("single replica RouteOther: got error %v, want ErrNoReplicas", err)
	}
}

func TestRoundRobinConcurrent(t *testing.T) {
	disc := staticDiscovery{
		"pool": {
			{Pool: "pool", Name: "rep1", BaseURL: mustURL("http://127.0.0.1:8001")},
			{Pool: "pool", Name: "rep2", BaseURL: mustURL("http://127.0.0.1:8002")},
		},
	}

	rr := router.NewRoundRobin(disc, []string{"pool"})
	ctx := context.Background()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://gateway/v1/chat/completions", nil)

	const goroutines = 20
	const iters = 1000

	var counts sync.Map
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			for range iters {
				rep, err := rr.Route(ctx, req, "pool")
				if err != nil {
					t.Errorf("Route failed: %v", err)
					return
				}
				actual, _ := counts.LoadOrStore(rep.Replica.Name, new(int))
				ptr := actual.(*int)
				// We don't need atomic inside test map if we just check total
				_ = ptr
			}
		}()
	}
	wg.Wait()
}

// Used directly, round-robin records one decision and one duration per
// route under its own policy, including a pool with no replicas.
func TestRoundRobinRecordsOneDecisionPerRoute(t *testing.T) {
	disc := staticDiscovery{"p": {{Pool: "p", Name: "a", BaseURL: mustURL("http://127.0.0.1:8001")}}}
	tm := telemetry.NewMetrics()
	rr := router.NewRoundRobin(disc, []string{"p"}, tm)
	for i := 0; i < 2; i++ {
		if _, err := rr.Route(context.Background(), nil, "p"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := rr.Route(context.Background(), nil, "none"); err == nil {
		t.Fatal("routing an unknown pool succeeded")
	}
	rec := httptest.NewRecorder()
	tm.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		`gateway_route_decisions_total{pool="p",policy="round_robin",outcome="round_robin"} 2`,
		`gateway_router_duration_seconds_count{pool="p",policy="round_robin"} 2`,
		`gateway_route_decisions_total{pool="none",policy="round_robin",outcome="no_replicas"} 1`,
		`gateway_router_duration_seconds_count{pool="none",policy="round_robin"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %s in:\n%s", want, body)
		}
	}
}
