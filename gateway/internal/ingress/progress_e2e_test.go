package ingress

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/coalesce"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/compiler"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry/telemetrytest"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

// The progression's records, end to end (AD-11): what the request
// telemetry counts for answers given before dispatch, through the handler
// and the telemetry middleware.

// requestsCounted is gateway_requests_total for pool and status.
func requestsCounted(t *testing.T, m *telemetry.Metrics, pool, status string) float64 {
	t.Helper()
	return telemetrytest.Sum(t, m, telemetry.MetricRequests, `pool="`+pool+`"`, `status="`+status+`"`)
}

// TestEarlyAnswersCountedUnderTheirPool: an answer given once the pool is
// resolved is counted under that pool; one given before it (a body over
// the limit) under "none".
func TestEarlyAnswersCountedUnderTheirPool(t *testing.T) {
	t.Run("duplicate priority", func(t *testing.T) {
		f := newFixture(t, "", Options{})
		f.h.priorityPools["large"] = true
		if resp, _ := f.post(t, PathChatCompletions, []byte(`{"model":"large-model","priority":1,"priority":2}`), nil); resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status %d, want 400", resp.StatusCode)
		}
		eventually(t, "the 400 counted under its pool", func() bool { return requestsCounted(t, f.tel.Metrics(), "large", "400") == 1 })
	})
	t.Run("body over the limit", func(t *testing.T) {
		f := newFixture(t, "", Options{MaxRequestBodyBytes: 64})
		if resp, _ := f.post(t, PathChatCompletions, []byte(strings.Repeat("a", 65)), nil); resp.StatusCode != http.StatusRequestEntityTooLarge {
			t.Fatalf("status %d, want 413", resp.StatusCode)
		}
		eventually(t, "the 413 counted under none", func() bool { return requestsCounted(t, f.tel.Metrics(), telemetry.PoolNone, "413") == 1 })
		if n := f.outcomes(t).ClientDisconnects; n != 0 {
			t.Fatalf("a refused body counted %v disconnects", n)
		}
	})
}

// TestCallerLeavingDuringCacheLookupCountedUnderItsPool: a caller that
// left while its cache lookup ran is counted as 499 under its pool, not
// "none" (B4).
func TestCallerLeavingDuringCacheLookupCountedUnderItsPool(t *testing.T) {
	f, cache := newCacheFixture(t, "", 0)
	cache.mu.Lock()
	cache.hold = true
	cache.mu.Unlock()
	ctx, leave := context.WithCancel(context.Background())
	done := make(chan answer, 1)
	go func() { done <- f.ask(ctx, faqBody("where is my order?", false, ""), faqHeaders("kb", 4)) }()
	eventually(t, "the lookup", func() bool { return cache.lookups() == 1 })
	leave()
	<-done
	eventually(t, "the 499 counted under its pool", func() bool { return requestsCounted(t, f.tel.Metrics(), "large", "499") == 1 })
	if n := f.outcomes(t).ClientDisconnects; n != 1 {
		t.Fatalf("disconnects %v, want 1", n)
	}
	if n := f.upstreamRequests(); n != 0 {
		t.Fatalf("replicas ran %d generations for a caller that left", n)
	}
}

// TestFollowerOfAFailedResponseCountedUnderItsPool: when a shared response
// fails mid-way, the leader and the follower both see it fail (a follower
// never takes a truncated body for a whole one; a stream ends in one error
// event) and both are counted under their pool with the failure's outcome,
// though a follower's abort ends its handler before any stage returns.
func TestFollowerOfAFailedResponseCountedUnderItsPool(t *testing.T) {
	full := `{"id":"cmpl-1","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}]}`
	tests := []struct {
		name, contentType, partial string
		stream                     bool
		want                       string // gateway_requests_total status for both
	}{
		{name: "body", contentType: "application/json", partial: full[:len(full)/2], want: telemetry.OutcomeAborted},
		{name: "stream", contentType: "text/event-stream", stream: true, want: telemetry.OutcomeStreamError,
			partial: "data: {\"id\":\"cmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hel\"}}]}\n\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wrote, release := make(chan struct{}), make(chan struct{})
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !strings.HasPrefix(r.URL.Path, "/v1/chat") {
					http.NotFound(w, r)
					return
				}
				w.Header().Set("Content-Type", tc.contentType)
				_, _ = io.WriteString(w, tc.partial)
				_ = http.NewResponseController(w).Flush()
				close(wrote)
				<-release
				panic(http.ErrAbortHandler) // the replica drops the connection mid-response
			}))
			t.Cleanup(up.Close)
			tel, group, gw := followerGateway(t, up.URL)

			type answer struct {
				body string
				err  error
			}
			ask := func(done chan<- answer) {
				req, _ := http.NewRequest(http.MethodPost, gw.URL+PathChatCompletions, strings.NewReader(faqBody("where is my order?", tc.stream, "")))
				req.Header.Set("Content-Type", "application/json")
				for k, v := range faqHeaders("faq", 8) {
					req.Header.Set(k, v)
				}
				resp, err := gw.Client().Do(req)
				var b []byte
				if err == nil {
					b, err = io.ReadAll(resp.Body)
					_ = resp.Body.Close()
				}
				done <- answer{string(b), err}
			}
			leader, follower := make(chan answer, 1), make(chan answer, 1)
			go ask(leader)
			<-wrote
			go ask(follower)
			eventually(t, "the follower to join", func() bool { return group.Stats().Joined == 1 })
			close(release)
			for who, ch := range map[string]chan answer{"leader": leader, "follower": follower} {
				a := <-ch
				switch {
				case !tc.stream && a.err == nil:
					t.Errorf("the %s read a whole body from a call that failed mid-body", who)
				case tc.stream && (a.err != nil || strings.Count(a.body, `{"error":`) != 1):
					t.Errorf("the %s's stream (err %v) = %q, want it to end in one error event", who, a.err, a.body)
				}
			}
			eventually(t, "both counted under their pool", func() bool {
				return requestsCounted(t, tel.Metrics(), "large", tc.want) == 2
			})
			if n := telemetrytest.Sum(t, tel.Metrics(), telemetry.MetricRequests, `pool="`+telemetry.PoolNone+`"`); n != 0 {
				t.Fatalf("%v requests counted under none", n)
			}
		})
	}
}

// followerGateway is a gateway with coalescing on in front of the replica
// at url, and the faq template.
func followerGateway(t *testing.T, url string) (*telemetry.Telemetry, *coalesce.Group, *httptest.Server) {
	t.Helper()
	cfg := parseConfig(t, fmt.Sprintf("pools:\n  - name: large\n    replicas:\n      - {name: large-0, url: %s}\nmodels:\n  large-model: large\ndefault_pool: large\nauth: {enabled: false}\n", url))
	disc, err := discovery.NewStatic(cfg.Pools)
	if err != nil {
		t.Fatal(err)
	}
	snap, errs := templates.NewSnapshot([]templates.Record{{ID: "faq", Version: 1, Status: templates.StatusActive, Spec: json.RawMessage(faqSpec)}}, time.Now())
	if len(errs) > 0 {
		t.Fatal(errs)
	}
	tel := telemetry.New(telemetry.Options{})
	group := coalesce.NewGroup(coalesce.Config{Coalescing: config.Coalescing{MaxFlights: 4, MaxSubscribers: 4, MaxReplayBytes: 1 << 20}})
	client := upstream.New(cfg.Upstream)
	t.Cleanup(client.CloseIdleConnections)
	h := New(cfg, disc, client, roundRobin(cfg, disc, client, nil), Options{Metrics: tel.Metrics(), Coalesce: group,
		Compiler: compiler.New(staticSnap{snap}, compiler.Options{MaxTokens: config.MaxTokensPolicy{Enabled: false}})})
	mux := http.NewServeMux()
	h.Register(mux)
	gw := httptest.NewServer(tel.Middleware(mux))
	t.Cleanup(gw.Close)
	return tel, group, gw
}
