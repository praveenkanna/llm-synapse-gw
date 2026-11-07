package ingress

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/dispatch"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/mockllm"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/health"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// mustNotEnd is how long a stream that must stay open is watched. The
// assertion is one-sided (a stream cut by mistake ends within microseconds
// of the cut, which the tests trigger first), so it does not need to be long.
const mustNotEnd = 200 * time.Millisecond

// M1-20: when a replica is ejected as a stuck engine, its judged still-hung
// streams end with the mid-stream SSE error event instead of waiting for
// upstream.timeouts.total.

// bodyStream is a streaming request whose response headers arrived; its
// body is collected until it ends.
type bodyStream struct {
	cancel context.CancelFunc
	body   chan []byte // receives the whole body once it ends
}

func openBodyStream(t *testing.T, g *leaseGateway, hdr map[string]string) *bodyStream {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.gw.URL+PathChatCompletions, strings.NewReader(streamBody))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range g.headers(hdr) {
		req.Header.Set(k, v)
	}
	resp, err := g.gw.Client().Do(req)
	if err != nil {
		cancel()
		t.Fatalf("POST: %v", err)
	}
	s := &bodyStream{cancel: cancel, body: make(chan []byte, 1)}
	go func() {
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		s.body <- b
	}()
	t.Cleanup(cancel)
	return s
}

// ended returns the body if the stream ended within d.
func (s *bodyStream) ended(d time.Duration) ([]byte, bool) {
	select {
	case b := <-s.body:
		return b, true
	case <-time.After(d):
		return nil, false
	}
}

// assertStuckErrorEvent checks body is exactly one SSE error event naming
// the stuck-engine kind, then nothing (no [DONE], no retry output).
func assertStuckErrorEvent(t *testing.T, body []byte) {
	t.Helper()
	text := string(body)
	if strings.Count(text, "data:") != 1 || !strings.HasPrefix(text, "data: ") || !strings.HasSuffix(text, "\n\n") {
		t.Fatalf("body %q, want exactly one SSE data event", text)
	}
	var ev errorBody
	if err := json.Unmarshal([]byte(strings.TrimSuffix(strings.TrimPrefix(text, "data: "), "\n\n")), &ev); err != nil {
		t.Fatalf("event %q is not JSON: %v", text, err)
	}
	if ev.Error.Type != upstreamErrorType || ev.Error.Code != upstreamCodePrefix+"stuck_engine" {
		t.Fatalf("error event = %+v, want type %q code %q", ev.Error, upstreamErrorType, upstreamCodePrefix+"stuck_engine")
	}
}

// stuckTelemetryGateway is stuckGateway behind the telemetry middleware, so
// the request's metrics can be read. The middleware sits inside the handled
// signal: a request is counted before waitHandled returns.
func stuckTelemetryGateway(t *testing.T, urls []string, clock *fakeClock) (*leaseGateway, *telemetry.Telemetry) {
	t.Helper()
	tel := telemetry.New(telemetry.Options{Logger: slog.New(slog.DiscardHandler)})
	g := newLeaseGatewayWith(t, ringPolicy, urls, streamBody, leaseOptions{clock: clock, tel: tel})
	return g, tel
}

// metricValue returns the value of one series of tel (0 when absent).
func metricValue(t *testing.T, tel *telemetry.Telemetry, series string) float64 {
	t.Helper()
	g := &telemetryGateway{tel: tel}
	return g.metric(t, series)
}

const stuckErrorsSeries = `gateway_upstream_errors_total{replica="r0",kind="stuck_engine"}`

// TestStuckEjectionEndsHungStreams: the hung streams of an ejected replica
// get the error event promptly (far inside upstream.timeouts.total), and
// leases, waits and in-flight counts balance.
func TestStuckEjectionEndsHungStreams(t *testing.T) {
	tests := []struct {
		name    string
		hung    int  // streams hung on r0
		wantCut bool // r0 reaches the ejection threshold
	}{
		{name: "scan ejects at the threshold", hung: 3, wantCut: true},
		{name: "four hung streams, all ended", hung: 4, wantCut: true},
		{name: "below the threshold nothing is cut", hung: 2, wantCut: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clock := newFakeClock()
			g, tel := stuckTelemetryGateway(t, []string{startMock(t, nil).URL(), startMock(t, nil).URL()}, clock)
			hang := map[string]string{mockllm.HeaderTTFTMs: stuckEngineTTFT}
			var streams []*bodyStream
			for range tc.hung {
				streams = append(streams, openBodyStream(t, g, hang))
			}
			waitFor(t, "waits registered", func() bool { return g.tracker.FirstChunkWaits() == tc.hung })
			clock.Advance(health.ConfigFrom(config.Defaults().Router).StuckEngineTimeout)
			g.tracker.CheckStuckStreams()

			wait := mustNotEnd
			if tc.wantCut {
				wait = 2 * time.Second // returns as soon as the stream ends
			}
			for i, s := range streams {
				body, ended := s.ended(wait)
				switch {
				case tc.wantCut && !ended:
					t.Fatalf("stream %d still open 2 s after the ejection", i)
				case tc.wantCut:
					assertStuckErrorEvent(t, body)
				case ended:
					t.Fatalf("stream %d ended (%q) without an ejection", i, body)
				}
			}
			if !tc.wantCut {
				for _, s := range streams {
					s.cancel()
				}
			}
			g.waitHandled(t, tc.hung)
			g.assertReleased(t, tc.hung)
			if n := g.inFlight("r0"); n != 0 {
				t.Errorf("r0 in flight = %d, want 0", n)
			}
			// The headers were sent before the cut: the request ends as a
			// stream error and its upstream error is the stuck engine.
			wantCut := 0.0
			if tc.wantCut {
				wantCut = float64(tc.hung)
			}
			if got := metricValue(t, tel, stuckErrorsSeries); got != wantCut {
				t.Errorf("%s = %v, want %v", stuckErrorsSeries, got, wantCut)
			}
			if got := metricValue(t, tel, requestsSeries("stream_error")); got != wantCut {
				t.Errorf("%s = %v, want %v", requestsSeries("stream_error"), got, wantCut)
			}
		})
	}
}

// TestStuckEjectionLeavesOtherStreamsAlone: a hung stream on another
// replica is not cut by r0's ejection.
func TestStuckEjectionLeavesOtherStreamsAlone(t *testing.T) {
	clock := newFakeClock()
	g := stuckGateway(t, []string{startMock(t, nil).URL(), startMock(t, nil).URL()}, clock, nil)
	hang := map[string]string{mockllm.HeaderTTFTMs: stuckEngineTTFT}
	var cut []*bodyStream
	for range 3 {
		cut = append(cut, openBodyStream(t, g, hang))
	}
	waitFor(t, "waits registered", func() bool { return g.tracker.FirstChunkWaits() == 3 })
	other := openBodyStream(t, g, map[string]string{
		mockllm.HeaderTTFTMs: stuckEngineTTFT,
		"X-Session-Id":       sessionRoutedTo(t, g.ar, streamBody, "r1"),
	})
	waitFor(t, "4 waits registered", func() bool { return g.tracker.FirstChunkWaits() == 4 })

	clock.Advance(health.ConfigFrom(config.Defaults().Router).StuckEngineTimeout)
	g.tracker.CheckStuckStreams()
	for _, s := range cut {
		body, ok := s.ended(2 * time.Second)
		if !ok {
			t.Fatal("hung stream on the ejected replica not ended")
		}
		assertStuckErrorEvent(t, body)
	}
	if body, ok := other.ended(mustNotEnd); ok {
		t.Fatalf("stream on a healthy replica ended: %q", body)
	}
	other.cancel()
	g.waitHandled(t, 4)
	g.assertReleased(t, 4)
}

// TestClientDisconnectStillCancelsUpstream: a caller that leaves while its
// stream hangs still frees the replica; a later ejection finds nothing to
// cut and releases nothing twice.
func TestClientDisconnectStillCancelsUpstream(t *testing.T) {
	clock := newFakeClock()
	g := stuckGateway(t, []string{startMock(t, nil).URL(), startMock(t, nil).URL()}, clock, nil)
	s := openBodyStream(t, g, map[string]string{mockllm.HeaderTTFTMs: stuckEngineTTFT})
	waitFor(t, "wait registered", func() bool { return g.tracker.FirstChunkWaits() == 1 })
	s.cancel()
	g.waitHandled(t, 1)
	g.assertReleased(t, 1)
	clock.Advance(health.ConfigFrom(config.Defaults().Router).StuckEngineTimeout)
	g.tracker.CheckStuckStreams()
	g.assertReleased(t, 1)
}

// TestStuckEjectionBeforeHeadersAnswers502: a replica that sent no response
// headers at all is ejected as stuck; the callers still waiting for headers
// get a 502 with code upstream_stuck_engine (not a 504, not a dropped
// connection) and the request counts with status 502.
func TestStuckEjectionBeforeHeadersAnswers502(t *testing.T) {
	const hung = 3
	arrived := make(chan struct{}, hung)
	silent := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body) // net/http watches for the caller leaving only once the body is read
		arrived <- struct{}{}              // the request reached the replica, which never answers
		<-r.Context().Done()
	}))
	t.Cleanup(silent.Close)
	clock := newFakeClock()
	g, tel := stuckTelemetryGateway(t, []string{silent.URL, startMock(t, nil).URL()}, clock)

	type result struct {
		status int
		body   string
		err    error
	}
	results := make(chan result, hung)
	for range hung {
		// One at a time: routes racing each other can push r0 over its load
		// cap and overflow to r1, which this test does not want.
		go func() {
			req, err := http.NewRequest(http.MethodPost, g.gw.URL+PathChatCompletions, strings.NewReader(streamBody))
			if err != nil {
				results <- result{err: err}
				return
			}
			for k, v := range g.headers(nil) {
				req.Header.Set(k, v)
			}
			resp, err := g.gw.Client().Do(req)
			if err != nil {
				results <- result{err: err}
				return
			}
			defer func() { _ = resp.Body.Close() }()
			b, err := io.ReadAll(resp.Body)
			results <- result{status: resp.StatusCode, body: string(b), err: err}
		}()
		select {
		case <-arrived:
		case <-time.After(streamStepTimeout):
			t.Fatal("request never reached the silent replica")
		}
	}
	waitFor(t, "waits registered", func() bool { return g.tracker.FirstChunkWaits() == hung })
	clock.Advance(health.ConfigFrom(config.Defaults().Router).StuckEngineTimeout)
	g.tracker.CheckStuckStreams()

	for range hung {
		select {
		case res := <-results:
			if res.err != nil {
				t.Fatalf("request failed: %v", res.err)
			}
			var eb errorBody
			if res.status != http.StatusBadGateway || json.Unmarshal([]byte(res.body), &eb) != nil ||
				eb.Error.Type != upstreamErrorType || eb.Error.Code != upstreamCodePrefix+"stuck_engine" {
				t.Fatalf("response = %d %q, want 502 with code %s%s", res.status, res.body, upstreamCodePrefix, "stuck_engine")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("request still waiting for headers 2 s after the ejection")
		}
	}
	g.waitHandled(t, hung)
	g.assertReleased(t, hung)
	if got := metricValue(t, tel, requestsSeries("502")); got != hung {
		t.Errorf("%s = %v, want %d", requestsSeries("502"), got, hung)
	}
	if got := metricValue(t, tel, stuckErrorsSeries); got != hung {
		t.Errorf("%s = %v, want %d", stuckErrorsSeries, got, hung)
	}
}

// hookHealth is passive health that records the OnStuckEject hook of every
// first-chunk wait and tracks nothing else.
type hookHealth struct {
	mu    sync.Mutex
	hooks []func()
}

func (h *hookHealth) RecordTransportFailure(discovery.Replica, error) {}

func (h *hookHealth) WatchFirstChunk(context.Context, discovery.Replica) dispatch.FirstChunkWait {
	return &hookWait{FirstChunkWait: dispatch.NoFirstChunkWait, h: h}
}

func (h *hookHealth) recorded() []func() {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]func(){}, h.hooks...)
}

type hookWait struct {
	dispatch.FirstChunkWait
	h *hookHealth
}

func (w *hookWait) OnStuckEject(f func()) {
	w.h.mu.Lock()
	w.h.hooks = append(w.h.hooks, f)
	w.h.mu.Unlock()
	w.FirstChunkWait.OnStuckEject(f)
}

// TestStaleStuckHookCannotCancelRetryAttempt: a hook from the failed first
// attempt of a connect retry (a cut that was already under way when the
// attempt ended) must not cancel the second attempt on another replica.
func TestStaleStuckHookCannotCancelRetryAttempt(t *testing.T) {
	arrived, release := make(chan struct{}), make(chan struct{})
	serverSawCancel := make(chan struct{})
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body) // net/http watches for the caller leaving only once the body is read
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		close(arrived)
		select {
		case <-release:
			_, _ = w.Write([]byte("data: ok\n\n"))
		case <-r.Context().Done():
			close(serverSawCancel)
		}
	}))
	t.Cleanup(live.Close)
	cfg, disc := poolConfig(t, []string{deadURL(t), live.URL}, "")
	hr := &hookHealth{}
	disp := newTestDispatcher(t, cfg, router.NewRoundRobin(disc, []string{"large"}), hr, nil)
	g := startPoolGateway(t, cfg, disc, disp, Options{}, serving{})

	type result struct {
		status int
		body   string
		err    error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := g.gw.Client().Post(g.gw.URL+PathChatCompletions, "application/json", strings.NewReader(streamBody))
		if err != nil {
			done <- result{err: err}
			return
		}
		defer func() { _ = resp.Body.Close() }()
		b, err := io.ReadAll(resp.Body)
		done <- result{status: resp.StatusCode, body: string(b), err: err}
	}()
	<-arrived // the retry reached r1: attempt 2 is under way
	hooks := hr.recorded()
	if len(hooks) != 2 {
		t.Fatalf("hooks recorded = %d, want 2 (one per attempt)", len(hooks))
	}
	hooks[0]() // the first attempt's stale cut, late
	select {
	case <-serverSawCancel:
		t.Fatal("the stale hook of attempt 1 cancelled attempt 2")
	case <-time.After(mustNotEnd):
	}
	close(release)
	res := <-done
	if res.err != nil || res.status != http.StatusOK || res.body != "data: ok\n\n" {
		t.Fatalf("response = %d %q, err %v; want the retried stream intact", res.status, res.body, res.err)
	}
}
