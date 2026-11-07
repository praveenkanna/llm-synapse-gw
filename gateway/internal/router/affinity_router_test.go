package router_test

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/affinity"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/affinity/affinitytest"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/health"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/loadview"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/ring"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

func setupAffinityRouter(t *testing.T) (*router.AffinityRouter, *telemetry.Metrics, *loadview.View, *health.Tracker) {
	t.Helper()

	reps := []discovery.Replica{
		{Pool: "llama-pool", Name: "rep-0", BaseURL: mustURL("http://127.0.0.1:8001")},
		{Pool: "llama-pool", Name: "rep-1", BaseURL: mustURL("http://127.0.0.1:8002")},
		{Pool: "llama-pool", Name: "rep-2", BaseURL: mustURL("http://127.0.0.1:8003")},
	}
	disc := staticDiscovery{"llama-pool": reps}

	tm := telemetry.NewMetrics()
	ht := health.New(health.ConfigFrom(config.Defaults().Router), disc, tm, nil)
	lv := loadview.New(loadview.Config{LoadView: config.Defaults().Router.LoadView}, disc, tm, nil)

	rr := router.NewRoundRobin(disc, []string{"llama-pool"})
	ar := router.NewAffinityRouter(disc, rr, ht, lv, tm)

	return ar, tm, lv, ht
}

func TestAffinityRouteTier1Header(t *testing.T) {
	ar, _, lv, _ := setupAffinityRouter(t)
	ctx := context.Background()

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "/v1/chat/completions", nil)
	req.Header.Set("X-Session-Id", "sess-test-1234")

	// Pick 1
	rep1, err := ar.Route(ctx, req, "llama-pool")
	if err != nil {
		t.Fatalf("route error: %v", err)
	}
	rep1.Release()

	// Pick 2 with same session ID must route to same replica
	rep2, err := ar.Route(ctx, req, "llama-pool")
	if err != nil {
		t.Fatalf("route error: %v", err)
	}
	rep2.Release()

	if rep1.Replica.Name != rep2.Replica.Name {
		t.Errorf("expected sticky replica %s, got %s", rep1.Replica.Name, rep2.Replica.Name)
	}

	// Verify in-flight count was released back to 0
	if inFlight := lv.InFlight(rep1.Replica.Name); inFlight != 0 {
		t.Errorf("expected 0 in-flight after release, got %d", inFlight)
	}
}

func TestAffinityRouteTier2PrefixHash(t *testing.T) {
	ar, _, _, _ := setupAffinityRouter(t)

	// Turn 1 body
	bodyTurn1 := []byte(`{
		"model": "meta-llama/Llama-3.1-8B-Instruct",
		"messages": [
			{"role": "system", "content": "You are a helpful assistant."},
			{"role": "user", "content": "Hello world!"}
		]
	}`)

	// Turn 2 body: adds assistant reply and user followup
	bodyTurn2 := []byte(`{
		"model": "meta-llama/Llama-3.1-8B-Instruct",
		"messages": [
			{"role": "system", "content": "You are a helpful assistant."},
			{"role": "user", "content": "Hello world!"},
			{"role": "assistant", "content": "Hello! How can I help you today?"},
			{"role": "user", "content": "What is the capital of France?"}
		]
	}`)

	ctx1 := router.WithRequestBody(context.Background(), bodyTurn1)
	req1, _ := http.NewRequestWithContext(ctx1, http.MethodPost, "/v1/chat/completions", nil)

	ctx2 := router.WithRequestBody(context.Background(), bodyTurn2)
	req2, _ := http.NewRequestWithContext(ctx2, http.MethodPost, "/v1/chat/completions", nil)

	rep1, err := ar.Route(ctx1, req1, "llama-pool")
	if err != nil {
		t.Fatalf("turn 1 route failed: %v", err)
	}
	rep1.Release()

	rep2, err := ar.Route(ctx2, req2, "llama-pool")
	if err != nil {
		t.Fatalf("turn 2 route failed: %v", err)
	}
	rep2.Release()

	if rep1.Replica.Name != rep2.Replica.Name {
		t.Errorf("turn 2 replica %s did not match turn 1 replica %s (prefix hash affinity)", rep2.Replica.Name, rep1.Replica.Name)
	}
}

func TestAffinityRouteFailOpen(t *testing.T) {
	// Proves AC-M1-6: Unparseable body or missing key -> round-robin, request succeeds
	ar, tm, _, _ := setupAffinityRouter(t)
	ctx := context.Background()

	testCases := []struct {
		name       string
		body       []byte
		wantReason string
	}{
		{
			name:       "unparseable json body",
			body:       []byte(`{bad json`),
			wantReason: "unparseable_body",
		},
		{
			name:       "missing messages and prompt",
			body:       []byte(`{"model": "llama-3"}`),
			wantReason: "no_affinity_key",
		},
		{
			name:       "empty messages array",
			body:       []byte(`{"model": "llama-3", "messages": []}`),
			wantReason: "empty_messages",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := router.WithRequestBody(ctx, tc.body)
			req, _ := http.NewRequestWithContext(c, http.MethodPost, "/v1/chat/completions", nil)

			// Route must succeed by failing open to round-robin
			rep, err := ar.Route(c, req, "llama-pool")
			if err != nil {
				t.Fatalf("expected fail-open route success, got error: %v", err)
			}
			if rep.Replica.Name == "" {
				t.Fatalf("expected valid replica selection")
			}
			rep.Release()
		})
	}

	// Verify fail-open metric recorded
	rec := httptest.NewRecorder()
	tm.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()

	for _, tc := range testCases {
		want := fmt.Sprintf(`gateway_failopen_total{stage="affinity",reason="%s"}`, tc.wantReason)
		if !strings.Contains(body, want) {
			t.Errorf("expected fail-open metric %s in exposition:\n%s", want, body)
		}
	}
}

func TestAffinityRouteBoundedLoadOverflow(t *testing.T) {
	ar, _, lv, ht := setupAffinityRouter(t)
	ctx := context.Background()

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "/v1/chat/completions", nil)
	req.Header.Set("X-Session-Id", "sess-overflow-key")

	// Get primary replica on ring
	rRing, err := ht.Ring("llama-pool")
	if err != nil {
		t.Fatalf("ring error: %v", err)
	}
	cands := rRing.Candidates("sess-overflow-key")
	primary := cands[0]
	backup := cands[1]

	// Simulate high in-flight load on primary
	for i := 0; i < 20; i++ {
		lv.IncInFlight(primary.Name)
	}

	// Now route: primary should be overloaded -> overflow to backup
	picked, err := ar.Route(ctx, req, "llama-pool")
	if err != nil {
		t.Fatalf("route error: %v", err)
	}
	// total 20: cap = ceil(1.25 × 21 / 3) = 9, and the primary holds 20.
	if picked.Replica.Name != backup.Name {
		t.Errorf("expected overflow to %s, got %s", backup.Name, picked.Replica.Name)
	}
	if got := lv.InFlight(primary.Name); got != 20 {
		t.Errorf("primary in-flight = %d, want 20 (no slot taken and left behind)", got)
	}
	if got := lv.InFlight(backup.Name); got != 1 {
		t.Errorf("backup in-flight = %d, want 1", got)
	}
	picked.Release()
}

func TestAffinityRouteOther(t *testing.T) {
	ar, _, _, _ := setupAffinityRouter(t)
	ctx := context.Background()

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "/v1/chat/completions", nil)
	req.Header.Set("X-Session-Id", "sess-retry-key")

	rep1, err := ar.Route(ctx, req, "llama-pool")
	if err != nil {
		t.Fatalf("route error: %v", err)
	}
	rep1.Release()

	// RouteOther excluding rep1
	repOther, err := ar.RouteOther(ctx, req, "llama-pool", rep1.Replica.Name)
	if err != nil {
		t.Fatalf("routeOther error: %v", err)
	}
	repOther.Release()

	if repOther.Replica.Name == rep1.Replica.Name {
		t.Errorf("expected different replica, got same %s", repOther.Replica.Name)
	}
}

func TestAffinityRouteTelemetry(t *testing.T) {
	ar, tm, lv, ht := setupAffinityRouter(t)
	ctx := context.Background()

	// 1. Successful affinity route
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "/v1/chat/completions", nil)
	req.Header.Set("X-Session-Id", "sess-telemetry-1")

	rep, err := ar.Route(ctx, req, "llama-pool")
	if err != nil {
		t.Fatalf("route failed: %v", err)
	}

	rec := httptest.NewRecorder()
	tm.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()

	if !strings.Contains(body, `gateway_route_decisions_total{pool="llama-pool",policy="affinity",outcome="affinity"} 1`) {
		t.Errorf("missing router decision affinity: %s", body)
	}
	if !strings.Contains(body, `gateway_router_duration_seconds_count{pool="llama-pool",policy="affinity"} 1`) {
		t.Errorf("missing router duration: %s", body)
	}
	if got := lv.InFlight(rep.Replica.Name); got != 1 {
		t.Errorf("in-flight on %s = %d, want 1", rep.Replica.Name, got)
	}

	// Release replica -> in-flight should go to 0
	rep.Release()
	if got := lv.InFlight(rep.Replica.Name); got != 0 {
		t.Errorf("in-flight on %s = %d after release, want 0", rep.Replica.Name, got)
	}

	// 2. Fail-open route (no session ID or body)
	reqFailOpen, _ := http.NewRequestWithContext(ctx, http.MethodPost, "/v1/chat/completions", nil)
	repFO, err := ar.Route(ctx, reqFailOpen, "llama-pool")
	if err != nil {
		t.Fatalf("failopen route failed: %v", err)
	}
	repFO.Release()

	rec = httptest.NewRecorder()
	tm.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body = rec.Body.String()
	if !strings.Contains(body, `gateway_route_decisions_total{pool="llama-pool",policy="affinity",outcome="failopen"} 1`) {
		t.Errorf("missing router decision failopen: %s", body)
	}

	// 3. Overflow route: load the key's primary owner past the cap, so the
	// decision must leave it for the next candidate and say so.
	rRing, err := ht.Ring("llama-pool")
	if err != nil {
		t.Fatal(err)
	}
	cands := rRing.Candidates("sess-telemetry-overflow")
	for i := 0; i < 20; i++ {
		lv.IncInFlight(cands[0].Name)
	}
	reqOverflow, _ := http.NewRequestWithContext(ctx, http.MethodPost, "/v1/chat/completions", nil)
	reqOverflow.Header.Set("X-Session-Id", "sess-telemetry-overflow")
	repOF, err := ar.Route(ctx, reqOverflow, "llama-pool")
	if err != nil {
		t.Fatalf("overflow route failed: %v", err)
	}
	if repOF.Replica.Name != cands[1].Name {
		t.Fatalf("overflow picked %s, want the next candidate %s (primary %s held 20)", repOF.Replica.Name, cands[1].Name, cands[0].Name)
	}
	repOF.Release()

	rec = httptest.NewRecorder()
	tm.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body = rec.Body.String()
	for _, want := range []string{
		`gateway_route_decisions_total{pool="llama-pool",policy="affinity",outcome="overflow"} 1`,
		`gateway_route_decisions_total{pool="llama-pool",policy="affinity",outcome="affinity"} 1`,
		`gateway_router_duration_seconds_count{pool="llama-pool",policy="affinity"} 3`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %s in:\n%s", want, body)
		}
	}
}

// A fail-open decision is counted once, under the affinity policy: the inner
// round-robin records neither a decision nor a duration of its own, even
// when it was built with metrics (as cmd/gateway builds it).
func TestAffinityFailOpenCountedOnce(t *testing.T) {
	reps := []discovery.Replica{
		{Pool: "p", Name: "a", BaseURL: mustURL("http://127.0.0.1:8001")},
		{Pool: "p", Name: "b", BaseURL: mustURL("http://127.0.0.1:8002")},
	}
	disc := staticDiscovery{"p": reps}
	tm := telemetry.NewMetrics()
	rr := router.NewRoundRobin(disc, []string{"p"}, tm)
	ar := router.NewAffinityRouter(disc, rr, health.New(health.ConfigFrom(config.Defaults().Router), disc, tm, nil),
		loadview.New(loadview.Config{LoadView: config.Defaults().Router.LoadView}, disc, tm, nil), tm)

	req, _ := http.NewRequest(http.MethodPost, "/v1/chat/completions", nil) // no key
	l, err := ar.Route(context.Background(), req, "p")
	if err != nil {
		t.Fatal(err)
	}
	l.Release()

	rec := httptest.NewRecorder()
	tm.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	if !strings.Contains(body, `gateway_route_decisions_total{pool="p",policy="affinity",outcome="failopen"} 1`) ||
		!strings.Contains(body, `gateway_router_duration_seconds_count{pool="p",policy="affinity"} 1`) {
		t.Fatalf("fail-open decision or duration not counted once under affinity:\n%s", body)
	}
	if strings.Contains(body, `policy="round_robin"`) {
		t.Fatalf("inner round-robin recorded its own decision or duration:\n%s", body)
	}
	if n := strings.Count(body, "gateway_route_decisions_total{"); n != 1 {
		t.Fatalf("%d decision series, want 1:\n%s", n, body)
	}

	// A pool with no replicas: one decision, no_healthy_replicas, no "failopen" as well.
	l, err = ar.Route(context.Background(), req, "empty")
	if err == nil {
		l.Release()
		t.Fatal("routing an empty pool succeeded")
	}
	rec = httptest.NewRecorder()
	tm.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body = rec.Body.String()
	if !strings.Contains(body, `gateway_route_decisions_total{pool="empty",policy="affinity",outcome="no_healthy_replicas"} 1`) ||
		strings.Contains(body, `pool="empty",policy="affinity",outcome="failopen"`) {
		t.Fatalf("empty pool decision not counted once:\n%s", body)
	}
}

// metricValue returns the value of the exposition line starting with series, or 0.
func metricValue(t *testing.T, tm *telemetry.Metrics, series string) int {
	t.Helper()
	rec := httptest.NewRecorder()
	tm.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if rest, ok := strings.CutPrefix(line, series+" "); ok {
			v, err := strconv.Atoi(strings.TrimSpace(rest))
			if err != nil {
				t.Fatalf("parse %q: %v", line, err)
			}
			return v
		}
	}
	return 0
}

// A concurrent burst on one hot key. Each route acquires its slot and then
// re-checks the limit (cap + slack), backing off to the next candidate (or a
// fresh snapshot) when concurrent routes overshot it. After the burst, with
// every lease held, no replica may hold more than
//
//	ceil((1 + ε) × burst / n) + slack + concurrencyExcess + overCap
//
// ceil((1 + ε) × burst / n) is the cap the last decision would compute from
// a consistent snapshot (its total is at most burst - 1 other slots, held or
// tentative, since each route holds at most one slot at a time). overCap
// counts the decisions recorded as outcome="over_cap", which took the
// least-loaded replica past the limit after every round was refused; each
// adds at most one slot anywhere.
//
// concurrencyExcess is observed, not proven. A decision reads the
// per-replica counters one at a time, so a route that backs off one replica
// and takes a slot on another between two of those reads is counted twice,
// and every tentative slot a route is about to give back still counts. Both
// inflate that decision's total and so its cap, by (1 + ε)/n per extra
// count; raising it by a whole slot takes about n/(1 + ε) extra counts in one
// snapshot. The worst case provable from the reads alone is vacuous (each
// route could be counted once per replica), so the test pins what was
// measured: in about 186,000 bursts of 64 on 4 replicas for each of slack 0
// and 1 (GOMAXPROCS 1 to 8, with and without -race) the excess was 0 in all
// but 16 bursts and never passed 1.
func TestAffinityConcurrentBurstRespectsCap(t *testing.T) {
	for _, slack := range []int{0, ring.DefaultOverflowSlack} {
		t.Run(fmt.Sprintf("slack %d", slack), func(t *testing.T) { concurrentBurst(t, slack) })
	}
}

func concurrentBurst(t *testing.T, slack int) {
	const (
		burst             = 64
		concurrencyExcess = 1 // observed bound, see TestAffinityConcurrentBurstRespectsCap
		rounds            = 30
	)
	reps := []discovery.Replica{
		{Pool: "p", Name: "r-0", BaseURL: mustURL("http://127.0.0.1:8001")},
		{Pool: "p", Name: "r-1", BaseURL: mustURL("http://127.0.0.1:8002")},
		{Pool: "p", Name: "r-2", BaseURL: mustURL("http://127.0.0.1:8003")},
		{Pool: "p", Name: "r-3", BaseURL: mustURL("http://127.0.0.1:8004")},
	}
	disc := staticDiscovery{"p": reps}
	hc := health.ConfigFrom(config.Defaults().Router)
	hc.Ring.OverflowSlack = slack
	eps := hc.Ring.Epsilon
	if eps != ring.DefaultEpsilon {
		t.Fatalf("health default ε = %v, want the ring default %v", eps, ring.DefaultEpsilon)
	}

	overCapSeen, maxExcess := 0, int64(math.MinInt64)
	for round := 0; round < rounds; round++ {
		tm := telemetry.NewMetrics()
		lv := loadview.New(loadview.Config{LoadView: config.Defaults().Router.LoadView}, disc, tm, nil)
		ar := router.NewAffinityRouter(disc, nil, health.New(hc, disc, tm, nil), lv, tm)

		var (
			start  = make(chan struct{})
			wg     sync.WaitGroup
			leases = make([]router.Lease, burst)
		)
		for i := 0; i < burst; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				req, _ := http.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
				req.Header.Set("X-Session-Id", "hot-conversation")
				<-start
				l, err := ar.Route(context.Background(), req, "p")
				if err != nil {
					t.Errorf("route: %v", err)
					return
				}
				leases[i] = l
			}(i)
		}
		close(start)
		wg.Wait()

		overCap := metricValue(t, tm, `gateway_route_decisions_total{pool="p",policy="affinity",outcome="over_cap"}`)
		overCapSeen += overCap
		// ceil(1.25 × 64 / 4) = 20, exactly.
		capacity := int64(math.Ceil((1 + eps) * float64(burst) / float64(len(reps))))
		bound := capacity + int64(slack) + int64(overCap)
		var total int64
		for _, rep := range reps {
			held := lv.InFlight(rep.Name)
			total += held
			maxExcess = max(maxExcess, held-bound)
			if held > bound+concurrencyExcess {
				t.Fatalf("round %d: %s holds %d; cap %d + slack %d + %d over-cap + excess %d",
					round, rep.Name, held, capacity, slack, overCap, concurrencyExcess)
			}
		}
		if total != burst {
			t.Fatalf("round %d: %d slots held after %d routes", round, total, burst)
		}
		for i := range leases {
			leases[i].Release()
		}
		for _, rep := range reps {
			if got := lv.InFlight(rep.Name); got != 0 {
				t.Fatalf("round %d: %s in-flight %d after releasing every lease", round, rep.Name, got)
			}
		}
	}
	t.Logf("slack %d: over-cap decisions across %d bursts of %d: %d; largest excess over cap + slack + over-cap: %d",
		slack, rounds, burst, overCapSeen, maxExcess)
}

// The low-load case behind ADR 0015, through the router: on an idle
// 3-replica pool, a conversation's second request while its first is still
// in flight stays on the owner with one slot of slack (cap 1 + 1) and is
// diverted under the strict bound (cap 1).
func TestAffinityLowLoadConversationStaysWithSlack(t *testing.T) {
	reps := []discovery.Replica{
		{Pool: "p", Name: "r-0", BaseURL: mustURL("http://127.0.0.1:8001")},
		{Pool: "p", Name: "r-1", BaseURL: mustURL("http://127.0.0.1:8002")},
		{Pool: "p", Name: "r-2", BaseURL: mustURL("http://127.0.0.1:8003")},
	}
	disc := staticDiscovery{"p": reps}
	for _, tc := range []struct {
		slack    int
		wantSame bool
		outcome  string
	}{{slack: 0, wantSame: false, outcome: "overflow"}, {slack: 1, wantSame: true, outcome: "affinity"}} {
		t.Run(fmt.Sprintf("slack %d", tc.slack), func(t *testing.T) {
			tm := telemetry.NewMetrics()
			hc := health.ConfigFrom(config.Defaults().Router)
			hc.Ring.OverflowSlack = tc.slack
			lv := loadview.New(loadview.Config{LoadView: config.Defaults().Router.LoadView}, disc, tm, nil)
			ar := router.NewAffinityRouter(disc, nil, health.New(hc, disc, tm, nil), lv, tm)
			req, _ := http.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
			req.Header.Set("X-Session-Id", "low-load-conversation")

			first, err := ar.Route(context.Background(), req, "p")
			if err != nil {
				t.Fatal(err)
			}
			defer first.Release()
			second, err := ar.Route(context.Background(), req, "p")
			if err != nil {
				t.Fatal(err)
			}
			defer second.Release()
			if same := second.Replica.Name == first.Replica.Name; same != tc.wantSame {
				t.Fatalf("first on %s, second on %s; want same replica = %v", first.Replica.Name, second.Replica.Name, tc.wantSame)
			}
			series := fmt.Sprintf(`gateway_route_decisions_total{pool="p",policy="affinity",outcome="%s"}`, tc.outcome)
			want := 1
			if tc.outcome == "affinity" {
				want = 2
			}
			if got := metricValue(t, tm, series); got != want {
				t.Fatalf("%s = %d, want %d", series, got, want)
			}
		})
	}
}

// TestAffinityRoutePostKeyCap: the key-scan bounds at the router. A
// duplicate messages member beyond the post-key cap is never reached, so
// the route keeps its affinity decision and counts no fail-open. One within
// the cap fails open as duplicate_key (M1-25), and a body whose key is not
// decided within the budget as key_scan_budget, each counted once with one
// failopen decision.
func TestAffinityRoutePostKeyCap(t *testing.T) {
	far := affinitytest.DenseTools(16 << 10)
	cases := []struct {
		name, body, outcome, reason string
		failOpens                   int
	}{
		{"duplicate beyond the cap", `{"messages":[{"role":"user","content":"A"}],"tools":` + far + `,"messages":[{"role":"user","content":"B"}]}`, "affinity", "", 0},
		{"duplicate within the cap", `{"messages":[{"role":"user","content":"A"}],"messages":[{"role":"system","content":"` + affinitytest.EscapedJSON(16<<10) + `"},{"role":"user","content":"B"}]}`, "failopen", "duplicate_key", 1},
		{"no key before the budget runs out", string(affinitytest.ShortMembers(`"k":"v",`, 200_000)), "failopen", "key_scan_budget", 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ar, tm, lv, _ := setupAffinityRouter(t)
			ctx := router.WithRequestBody(context.Background(), []byte(tc.body))
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "/v1/chat/completions", nil)
			lease, err := ar.Route(ctx, req, "llama-pool")
			if err != nil || lease.Replica.Name == "" {
				t.Fatalf("route %+v, %v", lease.Replica, err)
			}
			lease.Release()
			if got := lv.InFlight(lease.Replica.Name); got != 0 {
				t.Errorf("in-flight on %s = %d after release, want 0", lease.Replica.Name, got)
			}
			rec := httptest.NewRecorder()
			tm.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			body := rec.Body.String()
			decision := fmt.Sprintf(`gateway_route_decisions_total{pool="llama-pool",policy="affinity",outcome="%s"} 1`, tc.outcome)
			if !strings.Contains(body, decision) {
				t.Errorf("want %s in the exposition", decision)
			}
			failOpen := `gateway_failopen_total{stage="affinity",reason="` + tc.reason + `"}`
			switch {
			case tc.failOpens == 0 && strings.Contains(body, `gateway_failopen_total{stage="affinity"`):
				t.Errorf("unexpected %s", failOpen)
			case tc.failOpens == 1 && !strings.Contains(body, failOpen+" 1\n"):
				t.Errorf("want %s 1 in the exposition", failOpen)
			}
		})
	}
}

// TestAffinityFailOpenOnRequestLogLine: an affinity fail-open is recorded
// on the request's telemetry record, so its reason reaches the request's
// log line, and it is counted once, as before.
func TestAffinityFailOpenOnRequestLogLine(t *testing.T) {
	reps := []discovery.Replica{
		{Pool: "p", Name: "a", BaseURL: mustURL("http://127.0.0.1:8001")},
		{Pool: "p", Name: "b", BaseURL: mustURL("http://127.0.0.1:8002")},
	}
	disc := staticDiscovery{"p": reps}
	for _, tc := range []struct {
		name, session, reason string
		tracked               discovery.Discovery // what the health tracker sees
	}{
		{name: "no affinity key", reason: affinity.ReasonNoKey, tracked: disc},
		// A key, but the tracker has no ring for the pool: round-robin still
		// finds its replicas.
		{name: "no healthy ring", session: "s1", reason: "no_healthy_ring", tracked: staticDiscovery{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs strings.Builder
			tel := telemetry.New(telemetry.Options{Logger: slog.New(slog.NewJSONHandler(&logs, nil))})
			tm := tel.Metrics()
			ar := router.NewAffinityRouter(disc, router.NewRoundRobin(disc, []string{"p"}, tm),
				health.New(health.ConfigFrom(config.Defaults().Router), tc.tracked, tm, nil), loadview.New(loadview.Config{LoadView: config.Defaults().Router.LoadView}, disc, tm, nil), tm)
			h := tel.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				l, err := ar.Route(r.Context(), r, "p")
				if err != nil {
					t.Error(err)
					return
				}
				l.Release()
				w.WriteHeader(http.StatusOK)
			}))
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
			if tc.session != "" {
				req.Header.Set(affinity.HeaderSessionID, tc.session)
			}
			h.ServeHTTP(httptest.NewRecorder(), req)
			if want := `"failopen":["affinity:` + tc.reason + `"]`; !strings.Contains(logs.String(), want) {
				t.Fatalf("log line %s; want it to carry %s", logs.String(), want)
			}
			rec := httptest.NewRecorder()
			tm.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			if want := `gateway_failopen_total{stage="affinity",reason="` + tc.reason + `"} 1` + "\n"; !strings.Contains(rec.Body.String(), want) {
				t.Fatalf("metrics lack %q (counted once)", want)
			}
		})
	}
}
