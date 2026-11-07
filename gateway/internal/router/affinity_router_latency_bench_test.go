package router_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/affinity"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/affinity/affinitytest"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/health"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/loadview"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// routeLatencyShapes are the bodies a whole route decision is timed at: a
// small chat, the AC-M1-5 sizes from M1-14 (1, 32 and 256 KiB system
// prompts, 1 MiB of tools ahead of the messages, which fails open to
// round-robin), the 1 MiB-plus shapes where the key comes first, and the
// short-token shapes that must stop within the budget too, including the
// ones that cost the most time per work unit (M1-22).
var routeLatencyShapes = []string{
	"system_1KB",
	"system_32KB",
	"system_256KB",
	"tools_1MB_before_messages_failopen",
	"tools_1MB_after_messages",
	"conversation_1.8MB",
	"members_1.6MB_before_messages_failopen",
	"members_32MB_before_messages_failopen",
	"system_messages_50k_failopen",
	"duplicate_messages_50k_failopen",
	"duplicate_prompt_tokens_1.6MB_failopen",
	"duplicate_prompt_token_lists_1.6MB_failopen",
	"escaped_near_miss_keys_1.6MB_before_messages_failopen",
	"empty_strings_1.6MB_before_messages_failopen",
	"adjacent_empty_strings_1.6MB_before_messages_failopen",
	"empty_objects_1.6MB_in_content_failopen",
	"adjacent_3byte_strings_1.6MB_before_messages_failopen",
	"short_arrays_1.6MB_before_messages_failopen",
	"empty_arrays_1.6MB_before_messages_failopen",
	"short_objects_1.6MB_before_messages_failopen",
	"bool_members_1.6MB_before_messages_failopen",
	"str15_array_1.6MB_before_messages_failopen",
	"str13_array_1.6MB_before_messages_failopen",
	"str15_prompt_1.6MB",
	"str11_prompt_1.6MB",
	"stretch4_1.6MB_before_messages_failopen",
	"stretch6_1.6MB_before_messages_failopen",
	"stretch7_1.6MB_before_messages_failopen",
	"stretch9_1.6MB_before_messages_failopen",
	"stretch9_brackets_1.6MB_before_messages_failopen",
	"stretch7_brackets_1.6MB_before_messages_failopen",
	"strict_members_1.6MB_before_messages_failopen",
}

// routeCase is one shape of a whole route decision: a body, the session
// headers it comes with, and the pool it is routed in.
type routeCase struct {
	affinitytest.Case
	session, conversation int  // bytes of X-Session-Id and X-Conversation-Id; 0: not sent
	replicas              int  // pool size; 0: latencyReplicas
	nameBytes             int  // bytes of each replica's name; 0: short names
	walk                  bool // the replicas first on the ring are full (see walkHeldShare)
	refused               bool // every acquisition is refused (AffinityRouter.RouteAllRefused)
}

// route runs the decision the case times.
func (c routeCase) route(ctx context.Context, ar *router.AffinityRouter, req *http.Request) (router.Lease, error) {
	if c.refused {
		return ar.RouteAllRefused(ctx, req, "llama-pool")
	}
	return ar.Route(ctx, req, "llama-pool")
}

const (
	kiB = 1 << 10
	miB = 1 << 20
)

// routeHeaderCases are the header tier's shapes (M1-26): keys at and under
// the limit, keys over it where the body decides, both headers, and the two
// longest reads an ignored header leads to: the other header at the limit,
// and a body that spends the whole scan budget.
func routeHeaderCases(chat, wholeBudget []byte) []routeCase {
	return []routeCase{
		{Case: affinitytest.Case{Name: "header_session_64B", Body: chat}, session: 64},
		{Case: affinitytest.Case{Name: "header_session_256B", Body: chat}, session: 256},
		{Case: affinitytest.Case{Name: "header_session_8KiB", Body: chat}, session: 8 * kiB},
		{Case: affinitytest.Case{Name: "header_session_64KiB", Body: chat}, session: 64 * kiB},
		{Case: affinitytest.Case{Name: "header_session_1MiB", Body: chat}, session: miB},
		{Case: affinitytest.Case{Name: "header_conversation_1MiB", Body: chat}, conversation: miB},
		{Case: affinitytest.Case{Name: "header_both_64B", Body: chat}, session: 64, conversation: 64},
		{Case: affinitytest.Case{Name: "header_both_1MiB", Body: chat}, session: miB, conversation: miB},
		{Case: affinitytest.Case{Name: "header_session_1MiB_conversation_256B", Body: chat}, session: miB, conversation: 256},
		{Case: affinitytest.Case{Name: "header_session_1MiB_members_32MB_before_messages_failopen", Body: wholeBudget}, session: miB},
	}
}

// routeWalkCases are the audit's shapes (M1-26): the longest ring walk at the
// default pool size and at config.MaxPoolReplicas, the latter with the
// longest names and key config and callers can set, and after the costliest
// body scan that still gives a key, then every acquisition refused.
func routeWalkCases(chat, costliestKey []byte) []routeCase {
	limit := affinity.MaxSessionHeaderBytes
	return []routeCase{
		{Case: affinitytest.Case{Name: "replicas_max_all_refused", Body: chat}, session: 64, replicas: config.MaxPoolReplicas, refused: true},
		{Case: affinitytest.Case{Name: "replicas_default_walk", Body: chat}, session: 64, walk: true},
		{Case: affinitytest.Case{Name: "replicas_max_walk", Body: chat}, session: 64, replicas: config.MaxPoolReplicas, walk: true},
		{Case: affinitytest.Case{Name: "replicas_max_long_names_walk", Body: chat}, session: limit, replicas: config.MaxPoolReplicas, nameBytes: config.MaxReplicaNameLen, walk: true},
		{Case: affinitytest.Case{Name: "str15_prompt_1.6MB_replicas_max_walk", Body: costliestKey}, replicas: config.MaxPoolReplicas, walk: true},
	}
}

// routeCases returns a small chat body, routeLatencyShapes, then the header
// and ring-walk shapes, in that order, from the shared affinity test bodies.
func routeCases(tb testing.TB) []routeCase {
	tb.Helper()
	byName := map[string][]byte{}
	for _, c := range append(affinitytest.LatencyCases(), affinitytest.WorstCases()...) {
		byName[c.Name] = c.Body
	}
	chat := []byte(`{"model":"meta-llama/Llama-3.1-8B-Instruct","messages":[{"role":"system","content":"You are a helpful assistant."},{"role":"user","content":"Benchmark message content."}],"stream":true}`)
	cases := []routeCase{{Case: affinitytest.Case{Name: "chat_small", Body: chat}}}
	for _, name := range routeLatencyShapes {
		body, ok := byName[name]
		if !ok {
			tb.Fatalf("no shared body %q", name)
		}
		cases = append(cases, routeCase{Case: affinitytest.Case{Name: name, Body: body}})
	}
	cases = append(cases, routeHeaderCases(chat, byName["members_32MB_before_messages_failopen"])...)
	return append(cases, routeWalkCases(chat, byName["str15_prompt_1.6MB"])...)
}

// request returns the case's request and the context holding its body.
func (c routeCase) request(tb testing.TB) (context.Context, *http.Request) {
	tb.Helper()
	ctx := router.WithRequestBody(context.Background(), c.Body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "/v1/chat/completions", nil)
	if err != nil {
		tb.Fatal(err)
	}
	if c.session > 0 {
		req.Header.Set(affinity.HeaderSessionID, strings.Repeat("s", c.session))
	}
	if c.conversation > 0 {
		req.Header.Set(affinity.HeaderConversationID, strings.Repeat("c", c.conversation))
	}
	return ctx, req
}

// ignoresHeader reports whether the route ignores a header: the session
// header over the limit, or the conversation header over it with no session
// header. A usable session header ends the tier, so the other is never read.
func (c routeCase) ignoresHeader() bool {
	limit := affinity.MaxSessionHeaderBytes
	return c.session > limit || (c.session == 0 && c.conversation > limit)
}

const (
	// latencyBaseInFlight is the requests each replica holds while the route
	// latency is measured.
	latencyBaseInFlight = 6
	// latencyReplicas is the pool size of the shapes that name none.
	latencyReplicas = 4
	// walkHeldShare and walkHeldInFlight load the ring-walk shapes: that
	// share of the pool, the first replicas clockwise from the key's hash,
	// holds enough to be over cap + slack. The cap is 1.25× the mean load,
	// so at most 80% of a pool can be full and 75% is close to the longest
	// walk a caller can choose a key for.
	walkHeldShare    = 0.75
	walkHeldInFlight = 128
	// latencyWaiting is the waiting requests the ring shapes' replicas report.
	latencyWaiting = 1
)

// latencyRouter is an affinity router over tc's pool at the default bounded
// load (ε 0.25, overflow slack 1), each replica already holding baseInFlight
// requests so the cap arithmetic runs on non-zero loads.
func latencyRouter(tb testing.TB, tc routeCase, baseInFlight int) (*router.AffinityRouter, *telemetry.Metrics, *loadview.View) {
	tb.Helper()
	const pool = "llama-pool"
	n := tc.replicas
	if n == 0 {
		n = latencyReplicas
	}
	reps := make([]discovery.Replica, 0, n)
	for i := range n {
		name := fmt.Sprintf("rep-%d", i)
		name = strings.Repeat("r", max(tc.nameBytes-len(name), 0)) + name
		reps = append(reps, discovery.Replica{Pool: pool, Name: name, BaseURL: mustURL(fmt.Sprintf("http://127.0.0.1:%d", 8001+i))})
	}
	disc := staticDiscovery{pool: reps}
	tm := telemetry.NewMetrics()
	ht := health.New(health.ConfigFrom(config.Defaults().Router), disc, tm, nil)
	lv := loadview.New(loadview.Config{LoadView: config.Defaults().Router.LoadView}, disc, tm, nil)
	for _, rep := range reps {
		for range baseInFlight {
			lv.IncInFlight(rep.Name)
		}
	}
	if tc.walk || tc.refused {
		// Scraped state as in production, so Load's states read hits and
		// isStale runs. Dated ahead so a long run can never go stale.
		state := loadview.ReplicaState{HasWaiting: true, WaitingRequests: latencyWaiting, LastScraped: time.Now().Add(time.Hour)}
		for _, rep := range reps {
			lv.SeedState(rep.Name, state)
		}
	}
	if tc.walk {
		rg, err := ht.Ring(pool)
		if err != nil {
			tb.Fatal(err)
		}
		_, req := tc.request(tb)
		order := rg.Candidates(affinity.Extract(req, tc.Body).Key)
		for _, rep := range order[:int(float64(n)*walkHeldShare)] {
			for range walkHeldInFlight {
				lv.IncInFlight(rep.Name)
			}
		}
	}
	return router.NewAffinityRouter(disc, router.NewRoundRobin(disc, []string{pool}), ht, lv, tm), tm, lv
}

// BenchmarkAffinityRouteLatency times every whole route decision
// (AffinityRouter.Route: key extraction, ring pick with bounded load and
// acquire, decision metrics, then the lease's Release) into a preallocated
// slice and reports p50/p99/max per shape (AC-M1-5: route decision
// ≤ 100 µs p99). Calls run one at a time, as BenchmarkExtractLatency's do.
func BenchmarkAffinityRouteLatency(b *testing.B) {
	for _, tc := range routeCases(b) {
		ar, _, _ := latencyRouter(b, tc, latencyBaseInFlight)
		b.Run(tc.Name, func(b *testing.B) {
			ctx, req := tc.request(b)
			lat := make([]time.Duration, b.N)
			b.SetBytes(int64(len(tc.Body)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := range lat {
				start := time.Now()
				lease, err := tc.route(ctx, ar, req)
				lease.Release()
				lat[i] = time.Since(start)
				if err != nil || lease.Replica.Name == "" {
					b.Fatalf("route: %v", err)
				}
			}
			b.StopTimer()
			slices.Sort(lat)
			b.ReportMetric(affinitytest.Micros(affinitytest.Quantile(lat, 0.50)), "p50-µs")
			b.ReportMetric(affinitytest.Micros(affinitytest.Quantile(lat, 0.99)), "p99-µs")
			b.ReportMetric(affinitytest.Micros(lat[len(lat)-1]), "max-µs")
		})
	}
}

// A pool past config.MaxPoolReplicas routes like any other: same key, same
// replica; the retry moves off it; every slot comes back.
func TestRouteAbovePoolCeiling(t *testing.T) {
	tc := routeCase{Case: affinitytest.Case{Name: "above", Body: []byte(boundBody)}, session: 64, replicas: config.MaxPoolReplicas + 1}
	ar, tm, lv := latencyRouter(t, tc, 0)
	ctx, req := tc.request(t)
	first, err := ar.Route(ctx, req, "llama-pool")
	if err != nil {
		t.Fatal(err)
	}
	again, err := ar.Route(ctx, req, "llama-pool")
	if err != nil {
		t.Fatal(err)
	}
	if again.Replica.Name != first.Replica.Name {
		t.Fatalf("one key routed to %s and %s", first.Replica.Name, again.Replica.Name)
	}
	other, err := ar.RouteOther(ctx, req, "llama-pool", first.Replica.Name)
	if err != nil || other.Replica.Name == first.Replica.Name {
		t.Fatalf("retry on %s, %v", other.Replica.Name, err)
	}
	first.Release()
	again.Release()
	other.Release()
	for _, rep := range []string{first.Replica.Name, other.Replica.Name} {
		if n := lv.InFlight(rep); n != 0 {
			t.Errorf("%d in flight on %s after release", n, rep)
		}
	}
	if n := decisions(t, tm, "affinity"); n != 2 {
		t.Errorf("%v affinity decisions, want 2", n)
	}
}

// TestRouteLatencyCasesRoute checks the benchmark's setup without timing
// anything: every shape routes with the decision its name promises (the
// key's primary owner, a later candidate for *_walk, or a fail-open to
// round-robin for *_failopen, counted once with its reason), counts an
// ignored header once, and the lease gives its slot back, so the benchmark
// times the path it names.
func TestRouteLatencyCasesRoute(t *testing.T) {
	for _, tc := range routeCases(t) {
		t.Run(tc.Name, func(t *testing.T) {
			ar, tm, lv := latencyRouter(t, tc, latencyBaseInFlight)
			ctx, req := tc.request(t)
			lease, err := tc.route(ctx, ar, req)
			if err != nil || lease.Replica.Name == "" {
				t.Fatalf("route %+v, %v", lease.Replica, err)
			}
			held := lv.InFlight(lease.Replica.Name)
			if tc.walk || tc.refused {
				if got, want := lv.Load(lease.Replica), held+latencyWaiting; got != want {
					t.Errorf("load on %s = %d, want %d: the scraped state did not count", lease.Replica.Name, got, want)
				}
			}
			lease.Release()
			if got := lv.InFlight(lease.Replica.Name); got != held-1 {
				t.Errorf("in-flight on %s = %d after release, want %d", lease.Replica.Name, got, held-1)
			}
			outcome := "affinity"
			switch {
			case strings.HasSuffix(tc.Name, "_failopen"):
				outcome = "failopen"
			case strings.HasSuffix(tc.Name, "_walk"):
				outcome = "overflow"
			case tc.refused:
				outcome = "over_cap"
			}
			rec := httptest.NewRecorder()
			tm.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			body := rec.Body.String()
			want := fmt.Sprintf(`gateway_route_decisions_total{pool="llama-pool",policy="affinity",outcome="%s"} 1`, outcome)
			if !strings.Contains(body, want) {
				t.Errorf("want %s in the exposition", want)
			}
			// Every *_failopen shape fails open once, with its reason (the
			// key-scan budget, unparseable for adjacent strings, or a
			// duplicate top-level key); the others count none.
			reason := "key_scan_budget"
			switch {
			case strings.HasPrefix(tc.Name, "adjacent_"): // invalid JSON: stops at once
				reason = "unparseable_body"
			case strings.HasPrefix(tc.Name, "duplicate_"): // stops at the second key (M1-25)
				reason = "duplicate_key"
			}
			failOpen := `gateway_failopen_total{stage="affinity",reason="` + reason + `"}`
			if outcome == "failopen" && !strings.Contains(body, failOpen+" 1\n") {
				t.Errorf("want %s 1 in the exposition", failOpen)
			}
			if outcome != "failopen" && strings.Contains(body, `gateway_failopen_total{stage="affinity"`) {
				t.Errorf("unexpected affinity fail-open in the exposition")
			}
			wantHeader := 0.0
			if tc.ignoresHeader() {
				wantHeader = 1
			}
			if got := headerFailOpens(t, tm); got != wantHeader {
				t.Errorf("session_header_too_long counted %v times, want %v", got, wantHeader)
			}
		})
	}
}
