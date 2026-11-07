package router_test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/affinity"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/health"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/loadview"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry/telemetrytest"
)

const (
	boundPool = "llama-pool"
	boundBody = `{"model":"m","messages":[{"role":"user","content":"hello"}]}`
)

// overLong is a header value one byte past the limit.
var overLong = strings.Repeat("s", affinity.MaxSessionHeaderBytes+1)

func headerFailOpens(t *testing.T, tm *telemetry.Metrics) float64 {
	t.Helper()
	return telemetrytest.Sum(t, tm, telemetry.MetricFailOpen, `stage="affinity_header"`, `reason="session_header_too_long"`)
}

func decisions(t *testing.T, tm *telemetry.Metrics, outcome string) float64 {
	t.Helper()
	return telemetrytest.Sum(t, tm, telemetry.MetricRouteDecisions, `outcome="`+outcome+`"`)
}

// boundRequest is a request with an X-Session-Id of session ("": none).
func boundRequest(ctx context.Context, t *testing.T, session string) *http.Request {
	t.Helper()
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, "/v1/chat/completions", nil)
	if err != nil {
		t.Fatal(err)
	}
	if session != "" {
		r.Header.Set(affinity.HeaderSessionID, session)
	}
	return r
}

// An over-long session header routes as if it were absent: to the body key's
// replica, as an affinity decision, counted once, and the connect retry of
// the same request adds nothing.
func TestOverLongSessionHeaderUsesBodyKeyCountedOnce(t *testing.T) {
	ar, tm, _, _ := setupAffinityRouter(t)
	ctx := router.WithRequestBody(context.Background(), []byte(boundBody))

	want, err := ar.Route(ctx, boundRequest(ctx, t, ""), boundPool)
	if err != nil {
		t.Fatal(err)
	}
	want.Release()

	r := boundRequest(ctx, t, overLong)
	got, err := ar.Route(ctx, r, boundPool)
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()
	if got.Replica.Name != want.Replica.Name {
		t.Fatalf("over-long header routed to %s, the body key to %s", got.Replica.Name, want.Replica.Name)
	}
	if n := headerFailOpens(t, tm); n != 1 {
		t.Fatalf("counted %v times after Route, want once", n)
	}
	if n := decisions(t, tm, "affinity"); n != 2 {
		t.Fatalf("%v affinity decisions, want 2", n)
	}

	other, err := ar.RouteOther(ctx, r, boundPool, got.Replica.Name)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Release()
	if other.Replica.Name == got.Replica.Name {
		t.Fatalf("RouteOther stayed on %s", got.Replica.Name)
	}
	if n := headerFailOpens(t, tm); n != 1 {
		t.Fatalf("counted %v times after RouteOther, want once per request", n)
	}
	if n := decisions(t, tm, "retry"); n != 1 {
		t.Fatalf("%v retry decisions, want 1: the retry keeps the body key on the ring", n)
	}
}

// A header exactly at the limit is the key and counts nothing.
func TestSessionHeaderAtTheLimitKeys(t *testing.T) {
	ar, tm, _, _ := setupAffinityRouter(t)
	ctx := context.Background()
	l, err := ar.Route(ctx, boundRequest(ctx, t, strings.Repeat("s", affinity.MaxSessionHeaderBytes)), boundPool)
	if err != nil {
		t.Fatal(err)
	}
	l.Release()
	if n := headerFailOpens(t, tm); n != 0 {
		t.Fatalf("counted %v at the limit, want 0", n)
	}
	if n := decisions(t, tm, "affinity"); n != 1 {
		t.Fatalf("%v affinity decisions, want 1: this request has no body key", n)
	}
}

// Both headers over the limit are one ignored request, not two.
func TestBothHeadersOverLongCountOnce(t *testing.T) {
	ar, tm, _, _ := setupAffinityRouter(t)
	ctx := router.WithRequestBody(context.Background(), []byte(boundBody))
	r := boundRequest(ctx, t, overLong)
	r.Header.Set(affinity.HeaderConversationID, overLong)
	l, err := ar.Route(ctx, r, boundPool)
	if err != nil {
		t.Fatal(err)
	}
	l.Release()
	if n := headerFailOpens(t, tm); n != 1 {
		t.Fatalf("counted %v, want once", n)
	}
}

// With no body key either, the two fail-opens are separate facts: the header
// was ignored, and round-robin picked because there was no key at all.
func TestOverLongHeaderWithoutABodyKey(t *testing.T) {
	ar, tm, _, _ := setupAffinityRouter(t)
	ctx := context.Background()
	l, err := ar.Route(ctx, boundRequest(ctx, t, overLong), boundPool)
	if err != nil {
		t.Fatal(err)
	}
	l.Release()
	if n := headerFailOpens(t, tm); n != 1 {
		t.Fatalf("header ignored %v times, want once", n)
	}
	if n := telemetrytest.Sum(t, tm, telemetry.MetricFailOpen, `stage="affinity"`, `reason="no_affinity_key"`); n != 1 {
		t.Fatalf("no_affinity_key counted %v times, want once", n)
	}
	if n := decisions(t, tm, "failopen"); n != 1 {
		t.Fatalf("%v failopen decisions, want 1", n)
	}
}

// On the request path the record counts it and puts it on the log line, with
// the value itself nowhere; no healthy ring adds its own reason beside it.
func TestOverLongHeaderOnTheRequestRecord(t *testing.T) {
	disc := staticDiscovery{"p": {{Pool: "p", Name: "r0", BaseURL: mustURL("http://127.0.0.1:8001")}}}
	for _, tc := range []struct {
		name    string
		tracked discovery.Discovery
		want    string
	}{
		{name: "ring", tracked: disc, want: `"failopen":["affinity_header:session_header_too_long"]`},
		{name: "no healthy ring", tracked: staticDiscovery{}, want: `"failopen":["affinity_header:session_header_too_long","affinity:no_healthy_ring"]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs strings.Builder
			tel := telemetry.New(telemetry.Options{Logger: slog.New(slog.NewJSONHandler(&logs, nil))})
			tm := tel.Metrics()
			ar := router.NewAffinityRouter(disc, router.NewRoundRobin(disc, []string{"p"}, tm),
				health.New(health.ConfigFrom(config.Defaults().Router), tc.tracked, tm, nil),
				loadview.New(loadview.Config{LoadView: config.Defaults().Router.LoadView}, disc, tm, nil), tm)
			h := tel.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				l, err := ar.Route(router.WithRequestBody(r.Context(), []byte(boundBody)), r, "p")
				if err != nil {
					t.Error(err)
					return
				}
				l.Release()
				w.WriteHeader(http.StatusOK)
			}))
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
			req.Header.Set(affinity.HeaderSessionID, overLong)
			h.ServeHTTP(httptest.NewRecorder(), req)
			if !strings.Contains(logs.String(), tc.want) {
				t.Fatalf("log line %s; want it to carry %s", logs.String(), tc.want)
			}
			if strings.Contains(logs.String(), overLong) {
				t.Fatalf("the header value reached the log line")
			}
			if n := headerFailOpens(t, tm); n != 1 {
				t.Fatalf("counted %v, want once", n)
			}
		})
	}
}

// Round-robin reads no session header, so it has nothing to ignore or count.
func TestRoundRobinCountsNoSessionHeader(t *testing.T) {
	tm := telemetry.NewMetrics()
	disc := staticDiscovery{"p": {{Pool: "p", Name: "r0", BaseURL: mustURL("http://127.0.0.1:8001")}}}
	rr := router.NewRoundRobin(disc, []string{"p"}, tm)
	ctx := context.Background()
	l, err := rr.Route(ctx, boundRequest(ctx, t, overLong), "p")
	if err != nil {
		t.Fatal(err)
	}
	l.Release()
	if n := headerFailOpens(t, tm); n != 0 {
		t.Fatalf("counted %v under round-robin", n)
	}
}

// routeCost is the fastest of n whole route decisions. Noise only adds time,
// so the minimum tracks the cost, also on a shared machine and under -race.
func routeCost(ctx context.Context, t *testing.T, ar *router.AffinityRouter, r *http.Request, n int) time.Duration {
	t.Helper()
	best := time.Duration(1<<63 - 1)
	for range n {
		start := time.Now()
		l, err := ar.Route(ctx, r, boundPool)
		d := time.Since(start)
		if err != nil {
			t.Fatal(err)
		}
		l.Release()
		best = min(best, d)
	}
	return best
}

// TestRouteCostIndependentOfHeaderLength: a route costs the same with a
// 1 KiB header as with a 1 MiB one. Hashing the header instead costs 1,024
// times as much, so the ratio is far from the limit whichever way noise leans.
func TestRouteCostIndependentOfHeaderLength(t *testing.T) {
	const maxRatio = 4
	ar, _, _, _ := setupAffinityRouter(t)
	ctx := router.WithRequestBody(context.Background(), []byte(boundBody))
	for _, header := range []string{affinity.HeaderSessionID, affinity.HeaderConversationID} {
		t.Run(header, func(t *testing.T) {
			short, long := boundRequest(ctx, t, ""), boundRequest(ctx, t, "")
			short.Header.Set(header, strings.Repeat("s", 1<<10))
			long.Header.Set(header, strings.Repeat("s", 1<<20))
			var base, huge time.Duration
			for range 5 {
				base, huge = routeCost(ctx, t, ar, short, 200), routeCost(ctx, t, ar, long, 200)
				if huge <= maxRatio*base {
					return
				}
			}
			t.Fatalf("a route took %v with a 1 MiB header, %v with 1 KiB", huge, base)
		})
	}
}
