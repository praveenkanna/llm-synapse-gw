package health

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/mockllm"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

// Generation probe (M1-19): a replica ejected as stuck_engine comes back
// only once M1-12's rules hold and a streaming one-token completion
// answers. Ejection times run on a fake clock; /health and the probe go to
// a real local replica (mockllm, or a scripted handler), and each test
// waits for probes in flight with tr.probes.Wait.

const (
	probeKey   = "probe-secret-upstream-key"
	probeModel = "mock-model" // mockllm's default served model
	// shortProbeTimeout keeps hanging probes quick; the bound is asserted.
	// Only a probe meant to hang gets it: under load a reply on loopback
	// can take this long, and would read as a timeout.
	shortProbeTimeout = 200 * time.Millisecond
	// answeredProbeTimeout is for a probe that gets an answer: never reached.
	answeredProbeTimeout = time.Minute
)

// replicaServer is one replica: /health always passes (as on a stuck
// engine) and generation requests go to gen, recorded first.
type replicaServer struct {
	*httptest.Server
	mu       sync.Mutex
	gens     []*http.Request
	bodies   []string
	inFlight atomic.Int64
	maxIn    atomic.Int64
}

func newReplicaServer(t *testing.T, gen http.Handler) *replicaServer {
	t.Helper()
	rs := &replicaServer{}
	rs.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(strings.NewReader(string(body)))
		rs.mu.Lock()
		rs.gens = append(rs.gens, r.Clone(context.Background()))
		rs.bodies = append(rs.bodies, string(body))
		rs.mu.Unlock()
		n := rs.inFlight.Add(1)
		defer rs.inFlight.Add(-1)
		for {
			m := rs.maxIn.Load()
			if n <= m || rs.maxIn.CompareAndSwap(m, n) {
				break
			}
		}
		gen.ServeHTTP(w, r)
	}))
	t.Cleanup(rs.Close)
	return rs
}

func (rs *replicaServer) generations() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return len(rs.gens)
}

// stuckMock is mockllm whose generations hang (X-Mock-Fault: hang, no
// headers, no tokens) while stuck is set, and answer once it is cleared.
func stuckMock(t *testing.T, stuck *atomic.Bool) http.Handler {
	t.Helper()
	cfg := mockllm.Defaults()
	cfg.TTFT = 0
	cfg.TokensPerSecond = 0
	m, err := mockllm.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if stuck.Load() {
			r.Header.Set(mockllm.HeaderFault, "hang")
		}
		m.ServeHTTP(w, r)
	})
}

// probeTracker is a tracker over pool p with replica a at rs, on a fake
// clock (min ejection 5 s, max 20 s, backoff 2), with metrics and the
// generation probe on: sent by an upstream client with probeKey, for
// probeModel, within answeredProbeTimeout.
func probeTracker(t *testing.T, rs *replicaServer, mod func(*Config)) (*Tracker, *FakeClock, *telemetry.Metrics) {
	t.Helper()
	disc, err := discovery.NewStatic(pool(map[string]string{"a": rs.URL}))
	if err != nil {
		t.Fatal(err)
	}
	up := config.Defaults().Upstream
	up.APIKey = probeKey
	clock := NewFakeClock()
	cfg := testConfig()
	cfg.MinEjection = 5 * time.Second
	cfg.MaxEjection = 20 * time.Second
	cfg.EjectionBackoffMultiplier = 2
	cfg.Now = clock.Now
	cfg.StuckProbeSender = upstream.New(up)
	cfg.StuckProbeModel = func(discovery.Replica) string { return probeModel }
	cfg.StuckProbeTimeout = answeredProbeTimeout
	if mod != nil {
		mod(&cfg)
	}
	tm := telemetry.NewMetrics()
	tr := New(cfg, disc, tm, nil)
	t.Cleanup(tr.probes.Wait)
	return tr, clock, tm
}

// poll runs one /health poll of every replica and waits for the probes it
// started.
func poll(t *testing.T, tr *Tracker) {
	t.Helper()
	if err := tr.PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	tr.probes.Wait()
}

// TestStuckReplicaStaysOutUntilGenerationProbeAnswers: a stuck engine's
// /health passes, but it stays out while its generation probe hangs (each
// timeout renewing the ejection with backoff), and comes back once a probe
// gets its first event, never before its ejection time.
func TestStuckReplicaStaysOutUntilGenerationProbeAnswers(t *testing.T) {
	var stuck atomic.Bool
	stuck.Store(true)
	rs := newReplicaServer(t, stuckMock(t, &stuck))
	tr, clock, tm := probeTracker(t, rs, func(c *Config) { c.StuckProbeTimeout = shortProbeTimeout })

	tr.Eject("a", ReasonStuckEngine)
	clock.Advance(4 * time.Second)
	poll(t, tr)
	if tr.IsHealthy("a") || rs.generations() != 0 {
		t.Fatalf("before the ejection time: healthy=%v, probes=%d; want out, none", tr.IsHealthy("a"), rs.generations())
	}

	clock.Advance(time.Second) // ejection time (5 s) over; /health passes
	start := time.Now()
	poll(t, tr)
	if elapsed := time.Since(start); elapsed > shortProbeTimeout+2*time.Second {
		t.Errorf("hanging probe took %v; its timeout is %v", elapsed, shortProbeTimeout)
	}
	if tr.IsHealthy("a") {
		t.Fatal("stuck engine re-admitted while its generation probe hangs")
	}
	if got := rs.generations(); got != 1 {
		t.Fatalf("generation probes = %d, want 1", got)
	}
	st := stateOf(tr, "a")
	if want := clock.Now().Add(10 * time.Second); !st.ejectedUntil.Equal(want) {
		t.Errorf("after a timed-out probe ejectedUntil = %v, want %v (renewed with backoff)", st.ejectedUntil, want)
	}

	poll(t, tr) // renewed: /health alone neither re-admits nor probes again
	if tr.IsHealthy("a") || rs.generations() != 1 {
		t.Fatalf("within the renewed ejection: healthy=%v, probes=%d", tr.IsHealthy("a"), rs.generations())
	}

	stuck.Store(false) // the engine recovers
	tr.cfg.StuckProbeTimeout = answeredProbeTimeout
	clock.Advance(10 * time.Second)
	poll(t, tr)
	if !tr.IsHealthy("a") {
		t.Fatal("recovered engine not re-admitted after its probe answered")
	}
	if got := rs.generations(); got != 2 {
		t.Errorf("generation probes = %d, want 2", got)
	}
	body := scrapeMetrics(t, tm)
	assertMetric(t, body, `gateway_stuck_probe_total{pool="p",outcome="timeout"} 1`)
	assertMetric(t, body, `gateway_stuck_probe_total{pool="p",outcome="pass"} 1`)
	assertMetric(t, body, `gateway_replica_ejections_total{pool="p",reason="stuck_engine"} 1`)
	if strings.Contains(body, probeKey) {
		t.Error("metrics expose the upstream key")
	}
}

// TestProbeWaitsForSuccessThreshold: the probe starts only once M1-12's
// rules hold (ejection time over and success_threshold passing /health in
// a row), and a pass re-admits only while they still hold.
func TestProbeWaitsForSuccessThreshold(t *testing.T) {
	tr, clock, _ := ejectionTracker(t, func(c *Config) {
		c.SuccessThreshold = 2
		c.StuckProbeSender = upstream.New(config.Defaults().Upstream) // never called here
	})
	a := currentReplica(t, tr, "a")
	tr.Eject("a", ReasonStuckEngine)
	clock.Advance(5 * time.Second)
	if tr.recordProbe(a, true) {
		t.Fatal("probe started after 1 of 2 passing /health")
	}
	if !tr.recordProbe(a, true) {
		t.Fatal("no probe after 2 passing /health past the ejection time")
	}
	if tr.IsHealthy("a") {
		t.Fatal("re-admitted on /health while the probe is owed")
	}
	tr.recordProbe(a, false) // /health fails while the probe runs
	tr.applyProbeOutcome(a, probeResult{outcome: ProbePass})
	if tr.IsHealthy("a") {
		t.Fatal("a pass re-admitted a replica whose /health failed meanwhile")
	}
	tr.recordProbe(a, true)
	if !tr.recordProbe(a, true) {
		t.Fatal("no new probe once the rules hold again")
	}
	tr.applyProbeOutcome(a, probeResult{outcome: ProbePass})
	if !tr.IsHealthy("a") {
		t.Fatal("a pass with the rules holding did not re-admit")
	}
	if stateOf(tr, "a").probeOwed {
		t.Error("probe still owed after re-admission")
	}
}

// TestFailedProbesBackOff: each failed probe renews the ejection for the
// next ejection time, up to max_ejection, without counting an ejection.
func TestFailedProbesBackOff(t *testing.T) {
	tr, clock, tm := stuckTracker(t, func(c *Config) {
		c.StuckProbeSender = upstream.New(config.Defaults().Upstream)
	})
	a := currentReplica(t, tr, "a")
	tr.Eject("a", ReasonStuckEngine) // 5 s
	for _, want := range []time.Duration{10 * time.Second, 20 * time.Second, 20 * time.Second} {
		clock.Advance(stateOf(tr, "a").ejectedUntil.Sub(clock.Now()))
		if !tr.recordProbe(a, true) {
			t.Fatal("no probe at the ejection time")
		}
		tr.applyProbeOutcome(a, probeResult{outcome: ProbeFail})
		if got := stateOf(tr, "a").ejectedUntil.Sub(clock.Now()); got != want {
			t.Errorf("renewed for %v, want %v", got, want)
		}
		if tr.IsHealthy("a") || tr.recordProbe(a, true) {
			t.Fatal("re-admitted or probed again within the renewed ejection")
		}
	}
	assertMetric(t, scrapeMetrics(t, tm), `gateway_replica_ejections_total{pool="p",reason="stuck_engine"} 1`)
}

// TestProbeOutcomes: what the replica answers decides the outcome; a
// rejected probe (a 4xx about the probe itself) fails open.
func TestProbeOutcomes(t *testing.T) {
	sse := func(events ...string) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			for _, e := range events {
				_, _ = io.WriteString(w, e)
			}
		}
	}
	status := func(code int, body string) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			_, _ = io.WriteString(w, body)
		}
	}
	// thenHang sends events, then nothing until the probe gives up.
	thenHang := func(events ...string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			for _, e := range events {
				_, _ = io.WriteString(w, e)
			}
			http.NewResponseController(w).Flush()
			<-r.Context().Done()
		}
	}
	tests := []struct {
		name     string
		gen      http.Handler
		want     string
		readmits bool
	}{
		{"first token", sse(`data: {"choices":[{"text":"OK"}]}`+"\n\n", "data: [DONE]\n\n"), ProbePass, true},
		{"comment and empty chunk before first token", sse(": keep-alive\n\n", `data: {"choices":[]}`+"\r\n\r\n", `data: {"choices":[{"text":" OK","finish_reason":null}]}`+"\r\n\r\n"), ProbePass, true},
		{"end of sequence only", sse(`data: {"choices":[{"index":0,"text":"","finish_reason":"stop"}]}`+"\n\n", "data: [DONE]\n\n"), ProbePass, true},
		{"chat role then content", sse(`data: {"choices":[{"delta":{"role":"assistant","content":""},"finish_reason":null}]}`+"\n\n", `data: {"choices":[{"delta":{"content":"OK"},"finish_reason":null}]}`+"\n\n"), ProbePass, true},
		{"chat reasoning", sse(`data: {"choices":[{"delta":{"reasoning_content":"hm"}}]}` + "\n\n"), ProbePass, true},
		{"chat tool call", sse(`data: {"choices":[{"delta":{"tool_calls":[{"index":0}]}}]}` + "\n\n"), ProbePass, true},
		{"chat role then hang", thenHang(`data: {"choices":[{"delta":{"role":"assistant"},"finish_reason":null}]}` + "\n\n"), ProbeTimeout, false},
		{"empty choices then hang", thenHang(`data: {"choices":[]}` + "\n\n"), ProbeTimeout, false},
		{"done only", sse("data: [DONE]\n\n"), ProbeFail, false},
		{"role chunk then done", sse(`data: {"choices":[{"delta":{"role":"assistant"}}]}`+"\n\n", "data: [DONE]\n\n"), ProbeFail, false},
		{"empty choices then end", sse(`data: {"choices":[]}` + "\n\n"), ProbeFail, false},
		{"html 200", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			_, _ = io.WriteString(w, "<html><body>OK</body></html>")
		}), ProbeFail, false},
		{"non-stream without choices", status(http.StatusOK, `{"object":"text_completion","choices":[]}`), ProbeFail, false},
		{"in-stream error event", sse(`data: {"error":{"message":"engine dead","code":500}}` + "\n\n"), ProbeFail, false},
		{"stream ends without event", sse(), ProbeFail, false},
		{"non-stream answer", status(http.StatusOK, `{"choices":[{"text":"OK"}]}`), ProbePass, true},
		{"non-stream error body", status(http.StatusOK, `{"error":{"message":"x"}}`), ProbeFail, false},
		{"server error", status(http.StatusInternalServerError, `{"error":{"message":"x"}}`), ProbeFail, false},
		{"overloaded", status(http.StatusTooManyRequests, `{}`), ProbeFail, false},
		{"request timeout", status(http.StatusRequestTimeout, `{}`), ProbeFail, false},
		{"unknown model", status(http.StatusNotFound, `{"error":{"message":"The model does not exist."}}`), ProbeRejected, true},
		{"bad credentials", status(http.StatusUnauthorized, `{"error":"Unauthorized"}`), ProbeRejected, true},
		{"bad request", status(http.StatusBadRequest, `{}`), ProbeRejected, true},
		{"forbidden", status(http.StatusForbidden, `{}`), ProbeRejected, true},
		{"method not allowed", status(http.StatusMethodNotAllowed, `{}`), ProbeRejected, true},
		{"unsupported media type", status(http.StatusUnsupportedMediaType, `{}`), ProbeRejected, true},
		{"unprocessable", status(http.StatusUnprocessableEntity, `{}`), ProbeRejected, true},
		{"conflict", status(http.StatusConflict, `{}`), ProbeFail, false},
		{"gone", status(http.StatusGone, `{}`), ProbeFail, false},
		{"payload too large", status(http.StatusRequestEntityTooLarge, `{}`), ProbeFail, false},
		{"redirect", status(http.StatusFound, `{}`), ProbeFail, false},
		{"no content", status(http.StatusNoContent, ``), ProbeFail, false},
		{"headers then nothing", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			http.NewResponseController(w).Flush()
			<-r.Context().Done()
		}), ProbeTimeout, false},
		{"no headers", http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }), ProbeTimeout, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rs := newReplicaServer(t, tc.gen)
			tr, clock, tm := probeTracker(t, rs, func(c *Config) {
				if tc.want == ProbeTimeout {
					c.StuckProbeTimeout = shortProbeTimeout
				}
			})
			tr.Eject("a", ReasonStuckEngine)
			clock.Advance(5 * time.Second)
			poll(t, tr)
			if got := tr.IsHealthy("a"); got != tc.readmits {
				t.Fatalf("re-admitted = %v, want %v", got, tc.readmits)
			}
			body := scrapeMetrics(t, tm)
			assertMetric(t, body, `gateway_stuck_probe_total{pool="p",outcome="`+tc.want+`"} 1`)
			wantFailOpen := tc.want == ProbeRejected
			if got := strings.Contains(body, `gateway_failopen_total{stage="stuck_probe",reason="probe_rejected"} 1`); got != wantFailOpen {
				t.Errorf("probe_rejected fail-open counted = %v, want %v", got, wantFailOpen)
			}
		})
	}
}

// TestNoModelFailsOpen: with no model known the probe cannot be built: the
// replica comes back on /health alone, the fail-open is counted, and no
// generation request is sent.
func TestNoModelFailsOpen(t *testing.T) {
	rs := newReplicaServer(t, http.NotFoundHandler())
	tr, clock, tm := probeTracker(t, rs, func(c *Config) {
		c.StuckProbeModel = func(discovery.Replica) string { return "" }
	})
	tr.Eject("a", ReasonStuckEngine)
	clock.Advance(4 * time.Second)
	poll(t, tr)
	if tr.IsHealthy("a") {
		t.Fatal("fail-open re-admitted before the ejection time")
	}
	clock.Advance(time.Second)
	poll(t, tr)
	if !tr.IsHealthy("a") {
		t.Fatal("no model known: not re-admitted on /health")
	}
	if got := rs.generations(); got != 0 {
		t.Errorf("generation requests = %d, want 0", got)
	}
	body := scrapeMetrics(t, tm)
	assertMetric(t, body, `gateway_failopen_total{stage="stuck_probe",reason="no_model"} 1`)
	if strings.Contains(body, "gateway_stuck_probe_total{") {
		t.Errorf("a probe counted though none was sent:\n%s", body)
	}
}

// TestNonStuckEjectionsNeverProbe: other ejection reasons keep /health-only
// re-admission.
func TestNonStuckEjectionsNeverProbe(t *testing.T) {
	for _, reason := range []string{ReasonTransportFailure, ReasonHealthCheckFail} {
		t.Run(reason, func(t *testing.T) {
			rs := newReplicaServer(t, http.NotFoundHandler())
			tr, clock, _ := probeTracker(t, rs, nil)
			tr.Eject("a", reason)
			clock.Advance(5 * time.Second)
			poll(t, tr)
			if !tr.IsHealthy("a") {
				t.Fatalf("%s ejection not re-admitted on /health", reason)
			}
			if got := rs.generations(); got != 0 {
				t.Errorf("generation probes = %d, want 0", got)
			}
		})
	}
}

// TestProbeOwedSurvivesReasonChange: once ejected as stuck, a replica owes
// its probe even if a later ejection report names another reason.
func TestProbeOwedSurvivesReasonChange(t *testing.T) {
	tr, clock, _ := ejectionTracker(t, func(c *Config) {
		c.StuckProbeSender = upstream.New(config.Defaults().Upstream)
	})
	a := currentReplica(t, tr, "a")
	tr.Eject("a", ReasonStuckEngine)
	tr.Eject("a", ReasonHealthCheckFail)
	clock.Advance(5 * time.Second)
	if !tr.recordProbe(a, true) || tr.IsHealthy("a") {
		t.Fatal("a stuck engine skipped its probe after a reason change")
	}
}

// TestOneProbeInFlightPerReplica: polls while a probe runs start no other;
// the probe takes no first-chunk wait and touches no stuck-engine counts;
// a result after Restore changes nothing.
func TestOneProbeInFlightPerReplica(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 8)
	rs := newReplicaServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"text\":\"OK\"}]}\n\n")
	}))
	tr, clock, _ := probeTracker(t, rs, func(c *Config) { c.StuckProbeTimeout = time.Minute })
	tr.Eject("a", ReasonStuckEngine)
	clock.Advance(5 * time.Second)
	for range 3 {
		if err := tr.PollOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("no generation probe started")
	}
	if err := tr.PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := rs.generations(); got != 1 {
		t.Errorf("probes sent = %d, want 1 in flight", got)
	}
	if n := tr.FirstChunkWaits(); n != 0 {
		t.Errorf("probe registered %d first-chunk waits", n)
	}
	if s, f := counts(tr, "a"); s != 0 || f != 0 {
		t.Errorf("probe changed stuck-engine counts: S=%d F=%d", s, f)
	}
	close(release)
	tr.probes.Wait()
	if !tr.IsHealthy("a") {
		t.Fatal("not re-admitted after the probe answered")
	}
	if got := rs.maxIn.Load(); got != 1 {
		t.Errorf("max probes in flight = %d, want 1", got)
	}
	if _, busy := tr.probing.Load("a"); busy {
		t.Error("probe marker left behind")
	}
}

// TestProbeResultAfterRestoreIgnored: an explicit Restore while the probe
// runs wins; the late result neither ejects nor renews anything.
func TestProbeResultAfterRestoreIgnored(t *testing.T) {
	tr, clock, _ := ejectionTracker(t, func(c *Config) {
		c.StuckProbeSender = upstream.New(config.Defaults().Upstream)
	})
	a := currentReplica(t, tr, "a")
	tr.Eject("a", ReasonStuckEngine)
	clock.Advance(5 * time.Second)
	tr.recordProbe(a, true)
	tr.Restore("a")
	tr.applyProbeOutcome(a, probeResult{outcome: ProbeTimeout})
	if !tr.IsHealthy("a") {
		t.Fatal("a late failed probe ejected a restored replica")
	}
}

// TestProbeCanceledRecordsNothing: a probe cut off by the poll loop's
// context (shutdown) ends promptly and changes no state or metric.
func TestProbeCanceledRecordsNothing(t *testing.T) {
	rs := newReplicaServer(t, http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	tr, clock, tm := probeTracker(t, rs, func(c *Config) { c.StuckProbeTimeout = time.Minute })
	tr.Eject("a", ReasonStuckEngine)
	clock.Advance(5 * time.Second)
	until := stateOf(tr, "a").ejectedUntil
	ctx, cancel := context.WithCancel(context.Background())
	if err := tr.PollOnce(ctx); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(5 * time.Second); rs.generations() == 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("no generation probe started")
		}
	}
	start := time.Now()
	cancel()
	tr.probes.Wait()
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("canceled probe took %v to end", elapsed)
	}
	if tr.IsHealthy("a") || !stateOf(tr, "a").ejectedUntil.Equal(until) {
		t.Error("a canceled probe changed the replica's state")
	}
	if strings.Contains(scrapeMetrics(t, tm), "gateway_stuck_probe_total{") {
		t.Error("a canceled probe was counted")
	}
}

// TestProbeRequestIsSyntheticAndAuthenticated: the probe is a fixed
// synthetic one-token streaming request, never caller data, sent with the
// gateway's upstream key and nothing else from any caller.
func TestProbeRequestIsSyntheticAndAuthenticated(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{
		{PathCompletions, `{"model":"mock-model","prompt":"Say OK.","max_tokens":1,"stream":true}`},
		{PathChatCompletions, `{"model":"mock-model","messages":[{"content":"Say OK.","role":"user"}],"max_tokens":1,"stream":true}`},
	}
	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			var stuck atomic.Bool
			rs := newReplicaServer(t, stuckMock(t, &stuck))
			tr, clock, _ := probeTracker(t, rs, func(c *Config) { c.StuckProbePath = tc.path })
			tr.Eject("a", ReasonStuckEngine)
			clock.Advance(5 * time.Second)
			poll(t, tr)
			if !tr.IsHealthy("a") {
				t.Fatal("mockllm's answer did not pass the probe")
			}
			rs.mu.Lock()
			defer rs.mu.Unlock()
			if len(rs.gens) != 1 {
				t.Fatalf("probes = %d, want 1", len(rs.gens))
			}
			r, body := rs.gens[0], rs.bodies[0]
			if r.Method != http.MethodPost || r.URL.Path != tc.path {
				t.Errorf("probe = %s %s, want POST %s", r.Method, r.URL.Path, tc.path)
			}
			if body != tc.want {
				t.Errorf("probe body = %s\nwant %s", body, tc.want)
			}
			if got := r.Header.Get("User-Agent"); got != upstream.ProbeUserAgent {
				t.Errorf("probe User-Agent = %q, want %q (marks the gateway's own requests)", got, upstream.ProbeUserAgent)
			}
			if got := r.Header.Get("Authorization"); got != "Bearer "+probeKey {
				t.Errorf("Authorization = %q, want the upstream key", got)
			}
			if got := r.Header.Get("Accept-Encoding"); got != "identity" {
				t.Errorf("Accept-Encoding = %q, want identity (streamed)", got)
			}
			for k := range r.Header {
				if strings.HasPrefix(strings.ToLower(k), "x-") {
					t.Errorf("probe carries header %s", k)
				}
			}
		})
	}
}

// TestReadGenerationBounded: at most maxProbeEventBytes are read; a
// stream with no generation within them fails, and a first event larger
// than the cap (cut off, unparsable) does too.
func TestReadGenerationBounded(t *testing.T) {
	empty := strings.Repeat("data: {\"choices\":[]}\n\n", maxProbeEventBytes/20)
	r := &countingReader{r: strings.NewReader(empty + "data: {\"choices\":[{\"text\":\"late\"}]}\n\n")}
	ok, err := readGeneration(r, true)
	if err != nil || ok {
		t.Fatalf("generation past the cap: ok=%v err=%v, want a fail", ok, err)
	}
	if r.n > maxProbeEventBytes {
		t.Errorf("read %d bytes, cap %d", r.n, maxProbeEventBytes)
	}
	big := "data: {\"choices\":[{\"text\":\"" + strings.Repeat("x", 2*maxProbeEventBytes) + "\"}]}\n\n"
	if ok, err := readGeneration(strings.NewReader(big), true); ok || err != nil {
		t.Errorf("cut-off giant event: ok=%v err=%v, want a fail", ok, err)
	}
	if ok, _ := readGeneration(strings.NewReader(""), false); ok {
		t.Error("empty non-stream body passed")
	}
}

// TestReadGenerationSniffsSSE: a stream whose Content-Type a proxy
// stripped is still judged as SSE.
func TestReadGenerationSniffsSSE(t *testing.T) {
	for name, tc := range map[string]struct {
		body string
		want bool
	}{
		"token":            {"data: {\"choices\":[{\"text\":\"OK\"}]}\n\ndata: [DONE]\n\n", true},
		"comment first":    {": ping\n\ndata: {\"choices\":[{\"text\":\"OK\"}]}\n\n", true},
		"done only":        {"data: [DONE]\n\n", false},
		"plain completion": {`{"choices":[{"text":"OK"}]}`, true},
	} {
		if ok, err := readGeneration(strings.NewReader(tc.body), false); ok != tc.want || err != nil {
			t.Errorf("%s: ok=%v err=%v, want %v", name, ok, err, tc.want)
		}
	}
}

// TestGeneratedToolCalls: only a non-empty tool_calls array is generation.
func TestGeneratedToolCalls(t *testing.T) {
	for body, want := range map[string]bool{
		`{"choices":[{"delta":{"tool_calls":[]}}]}`:                     false,
		`{"choices":[{"delta":{"tool_calls":[ ]}}]}`:                    false,
		`{"choices":[{"delta":{"tool_calls":null}}]}`:                   false,
		`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"c"}]}}]}`: true,
	} {
		if _, ok := judgeEvent([]byte(body)); ok != want {
			t.Errorf("%s: ok=%v, want %v", body, ok, want)
		}
	}
}

type countingReader struct {
	r io.Reader
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}

// TestRejectedProbeWarnsOncePerStatus: a rejected probe logs one warning
// per replica and status (status, path, pool, replica only: no body, no
// key); the same status again is not logged, a new one is.
func TestRejectedProbeWarnsOncePerStatus(t *testing.T) {
	var code atomic.Int64
	code.Store(http.StatusNotFound)
	rs := newReplicaServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(int(code.Load()))
		_, _ = io.WriteString(w, `{"error":{"message":"secret-body-text"}}`)
	}))
	var buf syncBuffer
	tr, clock, _ := probeTracker(t, rs, func(c *Config) {
		c.Logger = slog.New(slog.NewJSONHandler(&buf, nil))
	})
	rejectOnce := func() {
		t.Helper()
		tr.Eject("a", ReasonStuckEngine)
		clock.Advance(time.Minute)
		poll(t, tr)
		if !tr.IsHealthy("a") {
			t.Fatal("rejected probe did not fail open")
		}
	}
	rejectOnce()
	rejectOnce() // same status: not logged again
	code.Store(http.StatusUnauthorized)
	rejectOnce()
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("warnings = %d, want 2 (404 once, then 401):\n%s", len(lines), buf.String())
	}
	for i, want := range []string{`"status":404`, `"status":401`} {
		for _, part := range []string{want, `"level":"WARN"`, `"pool":"p"`, `"replica":"a"`, `"path":"/v1/completions"`} {
			if !strings.Contains(lines[i], part) {
				t.Errorf("warning %d lacks %s: %s", i, part, lines[i])
			}
		}
	}
	if out := buf.String(); strings.Contains(out, probeKey) || strings.Contains(out, "secret-body-text") || strings.Contains(out, ProbePrompt) {
		t.Errorf("warning leaks the key, body or prompt: %s", out)
	}
}

// syncBuffer is a bytes.Buffer safe for a log handler and a reader.
type syncBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// TestStopEndsProbesInFlight: the background poll loop starts the probe,
// and Stop cancels it and waits for it, promptly.
func TestStopEndsProbesInFlight(t *testing.T) {
	rs := newReplicaServer(t, http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	tr, clock, _ := probeTracker(t, rs, func(c *Config) {
		c.PollInterval = 100 * time.Millisecond
		c.StuckProbeTimeout = time.Minute
	})
	tr.Eject("a", ReasonStuckEngine)
	clock.Advance(5 * time.Second)
	tr.Start(context.Background())
	for deadline := time.Now().Add(5 * time.Second); rs.generations() == 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			tr.Stop()
			t.Fatal("poll loop started no generation probe")
		}
	}
	start := time.Now()
	tr.Stop()
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Stop took %v with a probe in flight", elapsed)
	}
	if _, busy := tr.probing.Load("a"); busy {
		t.Error("probe still marked in flight after Stop")
	}
	if got := rs.generations(); got != 1 {
		t.Errorf("probes = %d, want 1 (one in flight across polls)", got)
	}
}

// TestStaleProbeNotCounted: a probe result for a replica that a reload has
// since re-pointed is neither applied nor counted.
func TestStaleProbeNotCounted(t *testing.T) {
	tr, clock, tm := stuckTracker(t, func(c *Config) {
		c.StuckProbeSender = upstream.New(config.Defaults().Upstream)
	})
	a := currentReplica(t, tr, "a")
	tr.Eject("a", ReasonStuckEngine)
	clock.Advance(5 * time.Second)
	stale := a
	u := *a.BaseURL
	u.Host = "10.9.9.9:8000"
	stale.BaseURL = &u
	tr.applyProbeOutcome(stale, probeResult{outcome: ProbePass, status: http.StatusOK})
	if tr.IsHealthy("a") {
		t.Fatal("a probe of another address re-admitted a")
	}
	if strings.Contains(scrapeMetrics(t, tm), "gateway_stuck_probe_total{") {
		t.Error("a stale probe result was counted")
	}
}
