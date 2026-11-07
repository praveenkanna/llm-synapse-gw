package router_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/health"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/loadview"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

func reloadPools(urls ...string) []config.Pool {
	p := config.Pool{Name: "p"}
	for i, u := range urls {
		p.Replicas = append(p.Replicas, config.Replica{Name: fmt.Sprintf("r%d", i), URL: u})
	}
	return []config.Pool{p}
}

func sessionRequest(t *testing.T, session string) *http.Request {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/chat/completions", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Session-Id", session)
	return req
}

// TestAffinityRouterFollowsReload: through the affinity router, a lease
// held on a replica that a reload removes releases safely, keyed traffic
// stops reaching the removed replica, and a re-pointed replica is reached
// at its new URL.
func TestAffinityRouterFollowsReload(t *testing.T) {
	ctx := context.Background()
	// Ports 1-3 refuse at once: scrapes fail fast, pruning still runs.
	disc, err := discovery.NewStatic(reloadPools("http://127.0.0.1:1", "http://127.0.0.1:2", "http://127.0.0.1:3"))
	if err != nil {
		t.Fatal(err)
	}
	tm := telemetry.NewMetrics()
	ht := health.New(health.ConfigFrom(config.Defaults().Router), disc, tm, nil)
	lv := loadview.New(loadview.Config{LoadView: config.Defaults().Router.LoadView}, disc, tm, nil)
	ar := router.NewAffinityRouter(disc, router.NewRoundRobin(disc, []string{"p"}), ht, lv, tm)

	// Find a session the ring sends to r1 and hold a lease on it.
	var onR1 string
	for i := 0; i < 1000 && onR1 == ""; i++ {
		s := fmt.Sprintf("sess-%d", i)
		l, err := ar.Route(ctx, sessionRequest(t, s), "p")
		if err != nil {
			t.Fatal(err)
		}
		if l.Replica.Name == "r1" {
			onR1 = s
		}
		l.Release()
	}
	if onR1 == "" {
		t.Fatal("no session routed to r1")
	}
	held, err := ar.Route(ctx, sessionRequest(t, onR1), "p")
	if err != nil || held.Replica.Name != "r1" {
		t.Fatalf("Route = %v, %v; want r1", held.Replica.Name, err)
	}

	// Reload removes r1; r0 and r2 keep their names and URLs.
	next := []config.Pool{{Name: "p", Replicas: []config.Replica{
		{Name: "r0", URL: "http://127.0.0.1:1"},
		{Name: "r2", URL: "http://127.0.0.1:3"},
	}}}
	if err := disc.Update(next); err != nil {
		t.Fatal(err)
	}
	ht.SyncDiscovery()
	_ = lv.ScrapeOnce(ctx) // prunes r1's state; its counter is held

	if got := lv.InFlight("r1"); got != 1 {
		t.Fatalf("r1 in-flight with the lease held = %d, want 1", got)
	}
	held.Release()
	held.Release() // idempotent
	if got := lv.InFlight("r1"); got != 0 {
		t.Fatalf("r1 in-flight after release = %d, want 0", got)
	}
	_ = lv.ScrapeOnce(ctx) // idle: the counter is pruned
	held.Release()         // still a no-op
	if got := lv.InFlight("r1"); got != 0 {
		t.Fatalf("r1 in-flight after pruning = %d", got)
	}

	for i := range 500 {
		s := onR1
		if i > 0 {
			s = fmt.Sprintf("sess-%d", i)
		}
		l, err := ar.Route(ctx, sessionRequest(t, s), "p")
		if err != nil {
			t.Fatal(err)
		}
		if l.Replica.Name == "r1" {
			t.Fatalf("session %s routed to removed replica r1", s)
		}
		l.Release()
	}

	// Re-point r0 to a new URL: keyed traffic reaches the new URL.
	repointed := []config.Pool{{Name: "p", Replicas: []config.Replica{
		{Name: "r0", URL: "http://127.0.0.1:9"},
		{Name: "r2", URL: "http://127.0.0.1:3"},
	}}}
	if err := disc.Update(repointed); err != nil {
		t.Fatal(err)
	}
	ht.SyncDiscovery()
	sawR0 := false
	for i := range 200 {
		l, err := ar.Route(ctx, sessionRequest(t, fmt.Sprintf("sess-%d", i)), "p")
		if err != nil {
			t.Fatal(err)
		}
		if l.Replica.Name == "r0" {
			sawR0 = true
			if got := l.Replica.BaseURL.String(); got != "http://127.0.0.1:9" {
				t.Fatalf("r0 routed at %s after re-point, want http://127.0.0.1:9", got)
			}
		}
		l.Release()
	}
	if !sawR0 {
		t.Fatal("no session routed to r0; re-point not exercised")
	}
	for _, name := range []string{"r0", "r1", "r2"} {
		if got := lv.InFlight(name); got != 0 {
			t.Errorf("%s in-flight = %d after every lease was released", name, got)
		}
	}
}
