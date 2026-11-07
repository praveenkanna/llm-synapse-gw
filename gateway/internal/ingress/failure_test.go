package ingress

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry/telemetrytest"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/testnet"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

// Failure table (.claude/rules/streaming-and-failure.md), one test per row:
//
//	connect fails before any byte sent   TestConnectFailureRetriedOnceOnAnotherReplica,
//	                                     TestSingleReplicaRetriesSameReplicaAfterBackoff (ADR 0014)
//	fails after send, before response    TestNoRetryAfterRequestSent
//	fails mid-stream                     TestMidStreamFailureEmitsOneErrorEvent
//	optimization stage errors            TestFailOpenToDefaultPool (ingress_test.go)
//	queue full / wait exceeded           M2 (no queue in M0)
//
// Plus: TestTruncatedNonStreamingResponseAborts, and client disconnects in
// TestClientAbortCancelsUpstreamWithinOneSecond (AC-M0-7).

// cancelDeadline is AC-M0-7's bound: a client abort must cancel the upstream
// request within this long.
const cancelDeadline = time.Second

// poolGateway is a gateway whose default pool "large" has one replica per
// URL, named r0, r1, ... in order. A fresh gateway sends its first request
// to r0.
type poolGateway struct {
	h   *Handler
	gw  *httptest.Server
	tel *telemetry.Telemetry // records every request, as in production
}

func newPoolGateway(t *testing.T, urls []string, yamlExtra string) *poolGateway {
	t.Helper()
	return newPoolGatewayWith(t, urls, yamlExtra, nil, Options{})
}

// newPoolGatewayWith is newPoolGateway with the handler options and the
// telemetry that records the requests (nil: a new one).
func newPoolGatewayWith(t *testing.T, urls []string, yamlExtra string, tel *telemetry.Telemetry, opts Options) *poolGateway {
	t.Helper()
	cfg, disc := poolConfig(t, urls, yamlExtra)
	return startPoolGateway(t, cfg, disc, nil, opts, serving{tel: tel})
}

// serving is how a test gateway is served: tel (nil: a new one) records the
// requests, as the telemetry middleware does in production; inner wraps
// the routes inside it and outer wraps it all.
type serving struct {
	tel          *telemetry.Telemetry
	inner, outer func(http.Handler) http.Handler
}

// poolConfig is the configuration and discovery behind newPoolGateway.
func poolConfig(t *testing.T, urls []string, yamlExtra string) (*config.Config, *discovery.Static) {
	t.Helper()
	var reps strings.Builder
	for i, u := range urls {
		fmt.Fprintf(&reps, "      - {name: r%d, url: %s}\n", i, u)
	}
	cfg := parseConfig(t, fmt.Sprintf("pools:\n  - name: large\n    replicas:\n%sauth: {enabled: false}\n%s", reps.String(), yamlExtra))
	disc, err := discovery.NewStatic(cfg.Pools)
	if err != nil {
		t.Fatal(err)
	}
	return cfg, disc
}

// startPoolGateway serves a Handler for cfg and disc on a test server.
// disp is nil for the round-robin dispatcher.
func startPoolGateway(t *testing.T, cfg *config.Config, disc discovery.Discovery, disp *dispatch.Dispatcher, opts Options, sv serving) *poolGateway {
	t.Helper()
	client := upstream.New(cfg.Upstream)
	t.Cleanup(client.CloseIdleConnections)
	if disp == nil {
		disp = roundRobin(cfg, disc, client, opts.Logger)
	}
	if sv.tel == nil {
		sv.tel = telemetry.New(telemetry.Options{Logger: opts.Logger})
	}
	g := &poolGateway{h: New(cfg, disc, client, disp, opts), tel: sv.tel}
	mux := http.NewServeMux()
	g.h.Register(mux)
	var root http.Handler = mux
	if sv.inner != nil {
		root = sv.inner(root)
	}
	root = sv.tel.Middleware(root)
	if sv.outer != nil {
		root = sv.outer(root)
	}
	g.gw = httptest.NewServer(root)
	t.Cleanup(g.gw.Close)
	return g
}

// do sends a request through the gateway and reads the whole response.
// readErr is the error reading the body, if any.
func (g *poolGateway) do(t *testing.T, body string, hdr map[string]string) (resp *http.Response, got []byte, readErr error) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, g.gw.URL+PathChatCompletions, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err = g.gw.Client().Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	got, readErr = io.ReadAll(resp.Body)
	return resp, got, readErr
}

// deadURL returns a URL where nothing listens: connecting is refused, and no
// server the test starts can be handed its port (see testnet.RefusedAddr).
func deadURL(t *testing.T) string {
	t.Helper()
	return testnet.RefusedURL(t)
}

const (
	chatBody   = `{"model":"mock-model","messages":[{"role":"user","content":"x"}]}`
	streamBody = `{"model":"mock-model","stream":true,"messages":[{"role":"user","content":"x"}]}`
)

// assertErrorBody checks an OpenAI-style error body with the given code.
func assertErrorBody(t *testing.T, b []byte, code string) {
	t.Helper()
	var eb errorBody
	if err := json.Unmarshal(b, &eb); err != nil || eb.Error.Code != code || eb.Error.Message == "" || eb.Error.Type == "" {
		t.Fatalf("body = %s, want an OpenAI error with code %q", b, code)
	}
}

func TestConnectFailureRetriedOnceOnAnotherReplica(t *testing.T) {
	tests := []struct {
		name        string
		dead        int // dead replicas, placed first so the first pick is dead
		live        bool
		body        string
		wantStatus  int
		wantRetries float64
		wantErrs    float64
	}{
		{name: "retry succeeds on the next replica", dead: 1, live: true, body: chatBody,
			wantStatus: http.StatusOK, wantRetries: 1, wantErrs: 1},
		{name: "streaming retry succeeds", dead: 1, live: true, body: streamBody,
			wantStatus: http.StatusOK, wantRetries: 1, wantErrs: 1},
		{name: "only one retry when every replica is down", dead: 3, body: chatBody,
			wantStatus: http.StatusBadGateway, wantRetries: 1, wantErrs: 2},
		{name: "single dead replica: exactly one retry, on the same replica", dead: 1, body: chatBody,
			wantStatus: http.StatusBadGateway, wantRetries: 1, wantErrs: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var urls []string
			for range tc.dead {
				urls = append(urls, deadURL(t))
			}
			var live *mockllm.Server
			if tc.live {
				live = startMock(t, nil)
				urls = append(urls, live.URL())
			}
			g := newPoolGateway(t, urls, "")
			resp, got, err := g.do(t, tc.body, map[string]string{mockllm.HeaderOutputTokens: "3"})
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d (%s)", resp.StatusCode, tc.wantStatus, got)
			}
			if tc.wantStatus == http.StatusBadGateway {
				assertErrorBody(t, got, "upstream_connect")
			}
			if tc.live {
				if n := len(live.Mock().Requests()); n != 1 {
					t.Fatalf("live replica got %d requests, want 1", n)
				}
				if tc.body == streamBody && !strings.HasSuffix(string(got), "data: [DONE]\n\n") {
					t.Fatalf("retried stream incomplete: %q", got)
				}
			}
			s := g.outcomes(t)
			if s.ConnectRetries != tc.wantRetries || s.UpstreamErrors != tc.wantErrs {
				t.Fatalf("stats = %+v, want %v retries and %v upstream errors", s, tc.wantRetries, tc.wantErrs)
			}
		})
	}
}

// TestSingleReplicaRetriesSameReplicaAfterBackoff covers ADR 0014: with no
// other replica, a failed connect is retried once on the same replica after
// upstream.connect_retry_backoff; with another replica there is no pause.
func TestSingleReplicaRetriesSameReplicaAfterBackoff(t *testing.T) {
	const backoff = 300 * time.Millisecond
	extra := fmt.Sprintf("upstream:\n  connect_retry_backoff: %s\n", backoff)

	t.Run("recovers from one transient connect failure", func(t *testing.T) {
		mock := startMock(t, nil)
		if err := mock.SetRefusing(true); err != nil {
			t.Fatal(err)
		}
		// Hold the backoff until the replica listens again, so the retry
		// can't race the re-listen.
		inBackoff, relisten := make(chan time.Duration, 1), make(chan struct{})
		cfg, disc := poolConfig(t, []string{mock.URL()}, extra)
		disp := newTestDispatcher(t, cfg, router.NewRoundRobin(disc, []string{"large"}), nil,
			func(ctx context.Context, d time.Duration) bool {
				inBackoff <- d
				select {
				case <-relisten:
				case <-ctx.Done():
				}
				return ctx.Err() == nil
			})
		g := startPoolGateway(t, cfg, disc, disp, Options{}, serving{})
		type result struct {
			status int
			body   []byte
		}
		done := make(chan result, 1)
		go func() {
			resp, got, _ := g.do(t, streamBody, map[string]string{mockllm.HeaderOutputTokens: "3"})
			done <- result{resp.StatusCode, got}
		}()
		// The first connect is refused; the replica comes back during the backoff.
		select {
		case d := <-inBackoff:
			if d != backoff {
				t.Fatalf("backoff = %s, want the configured %s", d, backoff)
			}
		case <-time.After(streamStepTimeout):
			t.Fatal("first connect failure never reached the backoff")
		}
		if s := g.outcomes(t); s.UpstreamErrors != 1 || s.ConnectRetries != 0 {
			t.Fatalf("stats in the backoff = %+v, want 1 upstream error and no retry yet", s)
		}
		if err := mock.SetRefusing(false); err != nil {
			t.Fatal(err)
		}
		close(relisten)
		res := <-done
		if res.status != http.StatusOK || !strings.HasSuffix(string(res.body), "data: [DONE]\n\n") {
			t.Fatalf("status %d, body %q; want a complete stream after the retry", res.status, res.body)
		}
		if s := g.outcomes(t); s.ConnectRetries != 1 || s.UpstreamErrors != 1 {
			t.Fatalf("stats = %+v, want 1 retry and 1 upstream error", s)
		}
		if n := len(mock.Mock().Requests()); n != 1 {
			t.Fatalf("replica got %d requests, want 1", n)
		}
	})

	t.Run("dead replica fails after exactly one retry", func(t *testing.T) {
		g := newPoolGateway(t, []string{deadURL(t)}, extra)
		start := time.Now()
		resp, got, err := g.do(t, chatBody, nil)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("status = %d (%s), want 502", resp.StatusCode, got)
		}
		assertErrorBody(t, got, "upstream_connect")
		if d := time.Since(start); d < backoff {
			t.Fatalf("failed after %s, want the retry to wait the %s backoff", d, backoff)
		}
		if s := g.outcomes(t); s.ConnectRetries != 1 || s.UpstreamErrors != 2 {
			t.Fatalf("stats = %+v, want 1 retry and 2 upstream errors", s)
		}
	})

	t.Run("another replica is tried at once", func(t *testing.T) {
		live := startMock(t, nil)
		g := newPoolGateway(t, []string{deadURL(t), live.URL()}, "upstream:\n  connect_retry_backoff: 5s\n")
		start := time.Now()
		resp, got, _ := g.do(t, chatBody, map[string]string{mockllm.HeaderOutputTokens: "3"})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d (%s)", resp.StatusCode, got)
		}
		if d := time.Since(start); d > 2*time.Second {
			t.Fatalf("took %s: the backoff must not apply when another replica exists", d)
		}
		if s := g.outcomes(t); s.ConnectRetries != 1 {
			t.Fatalf("stats = %+v, want 1 retry", s)
		}
	})

	t.Run("caller leaving during the backoff sends no retry", func(t *testing.T) {
		mock := startMock(t, nil)
		if err := mock.SetRefusing(true); err != nil {
			t.Fatal(err)
		}
		g := newPoolGateway(t, []string{mock.URL()}, "upstream:\n  connect_retry_backoff: 5s\n")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.gw.URL+PathChatCompletions, strings.NewReader(chatBody))
		if err != nil {
			t.Fatal(err)
		}
		errc := make(chan error, 1)
		go func() {
			resp, err := g.gw.Client().Do(req)
			if err == nil {
				_ = resp.Body.Close()
			}
			errc <- err
		}()
		waitFor(t, "first connect failed", func() bool { return g.outcomes(t).UpstreamErrors == 1 })
		if err := mock.SetRefusing(false); err != nil { // a retry would now succeed
			t.Fatal(err)
		}
		start := time.Now()
		cancel()
		if err := <-errc; err == nil {
			t.Fatal("request succeeded, want it cancelled by the caller")
		}
		waitFor(t, "disconnect counted", func() bool { return g.outcomes(t).ClientDisconnects == 1 })
		if d := time.Since(start); d > time.Second {
			t.Fatalf("handler ended %s after the caller left, want the backoff cut short", d)
		}
		if s := g.outcomes(t); s.ConnectRetries != 0 || s.UpstreamErrors != 1 {
			t.Fatalf("stats = %+v, want no retry and 1 upstream error", s)
		}
		if n := len(mock.Mock().Requests()); n != 0 {
			t.Fatalf("replica got %d requests, want 0 (no retry after the caller left)", n)
		}
	})
}

func TestNoRetryAfterRequestSent(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		fault      string
		extra      string
		wantStatus int
		wantCode   string // OpenAI error code in a gateway error body
	}{
		{name: "replica closes before responding", body: chatBody, fault: "drop",
			wantStatus: http.StatusBadGateway, wantCode: "upstream_other"},
		{name: "response header timeout", body: chatBody, fault: "hang",
			extra: "upstream:\n  timeouts: {response_header: 100ms}\n", wantStatus: http.StatusGatewayTimeout, wantCode: "upstream_timeout"},
		{name: "replica error response is relayed", body: chatBody, fault: "5xx", wantStatus: mockllm.DefaultErrorStatus},
		{name: "replica fails mid-stream", body: streamBody, fault: "drop", wantStatus: http.StatusOK},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			faulty, spare := startMock(t, nil), startMock(t, nil)
			g := newPoolGateway(t, []string{faulty.URL(), spare.URL()}, tc.extra)
			resp, got, _ := g.do(t, tc.body, map[string]string{mockllm.HeaderFault: tc.fault, mockllm.HeaderOutputTokens: "8"})
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d (%s)", resp.StatusCode, tc.wantStatus, got)
			}
			if tc.wantCode != "" {
				assertErrorBody(t, got, tc.wantCode)
			}
			if n := faulty.Mock().Stats().Requests; n != 1 {
				t.Fatalf("faulty replica got %d requests, want exactly 1", n)
			}
			if n := spare.Mock().Stats().Requests; n != 0 {
				t.Fatalf("request retried on another replica after it was sent (%d requests)", n)
			}
			if s := g.outcomes(t); s.ConnectRetries != 0 {
				t.Fatalf("stats = %+v, want no retries", s)
			}
		})
	}
}

// partialEventServer streams a whole event, then half of the next one, then
// drops the connection.
func partialEventServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", sseContentType)
		rc := http.NewResponseController(w)
		_, _ = io.WriteString(w, "data: {\"id\":\"a\"}\n\ndata: {\"id\":\"b\",\"cho")
		_ = rc.Flush()
		panic(http.ErrAbortHandler)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestMidStreamFailureEmitsOneErrorEvent(t *testing.T) {
	tests := []struct {
		name     string
		url      func(t *testing.T) string
		fault    string
		extra    string
		wantCode string
		wantTail string // exact end of the relayed body, when set
	}{
		{name: "replica drops the connection", fault: "drop", wantCode: "upstream_other",
			url: func(t *testing.T) string { return startMock(t, nil).URL() }},
		{name: "replica stalls past the idle timeout", fault: "stall", wantCode: "upstream_timeout",
			extra: "upstream:\n  timeouts: {idle_between_chunks: 100ms}\n",
			url:   func(t *testing.T) string { return startMock(t, nil).URL() }},
		{name: "replica stops mid-event", wantCode: "upstream_other",
			url: func(t *testing.T) string { return partialEventServer(t).URL },
			wantTail: "data: {\"id\":\"b\",\"cho\n\n" + `data: {"error":{"message":"` + midStreamErrorMessage +
				`","type":"upstream_error","param":null,"code":"upstream_other"}}` + "\n\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := newPoolGateway(t, []string{tc.url(t)}, tc.extra)
			resp, got, err := g.do(t, streamBody, map[string]string{mockllm.HeaderFault: tc.fault, mockllm.HeaderOutputTokens: "8"})
			if err != nil {
				t.Fatalf("stream did not end cleanly after the error event: %v", err)
			}
			if resp.StatusCode != http.StatusOK || !isEventStream(resp.Header) {
				t.Fatalf("status %d, headers %v", resp.StatusCode, resp.Header)
			}
			if tc.wantTail != "" && !strings.HasSuffix(string(got), tc.wantTail) {
				t.Fatalf("body = %q, want it to end with %q", got, tc.wantTail)
			}
			events, err := readEvents(bytes.NewReader(got))
			if err != nil {
				t.Fatal(err)
			}
			if len(events) < 2 {
				t.Fatalf("got %d events, want some tokens then the error: %q", len(events), got)
			}
			var errEvents int
			for _, ev := range events {
				if strings.Contains(ev.data, `"error"`) {
					errEvents++
				}
				if ev.data == "data: [DONE]\n" {
					t.Fatal("a failed stream must not end with [DONE]")
				}
			}
			last := events[len(events)-1].data
			if errEvents != 1 || !strings.HasPrefix(last, "data: ") {
				t.Fatalf("%d error events, last event %q; want exactly one, last", errEvents, last)
			}
			assertErrorBody(t, []byte(strings.TrimPrefix(last, "data: ")), tc.wantCode)
			waitFor(t, "relay error counted", func() bool { return g.outcomes(t).RelayErrors == 1 }) // counted when the handler returns
			if s := g.outcomes(t); s.ClientDisconnects != 0 {
				t.Fatalf("stats = %+v, want 1 relay error and no disconnect", s)
			}
		})
	}
}

// truncatingServer sends the start of a JSON body, then drops the
// connection. With declareLength it announces the full length first
// (Content-Length); otherwise the body is chunked.
func truncatingServer(t *testing.T, declareLength bool) *httptest.Server {
	t.Helper()
	full := `{"id":"cmpl-1","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"hello"}}]}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if declareLength {
			w.Header().Set("Content-Length", fmt.Sprint(len(full)))
		}
		_, _ = io.WriteString(w, full[:len(full)/2])
		_ = http.NewResponseController(w).Flush()
		panic(http.ErrAbortHandler)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestTruncatedNonStreamingResponseAborts(t *testing.T) {
	for _, declareLength := range []bool{false, true} {
		t.Run(fmt.Sprintf("content-length=%v", declareLength), func(t *testing.T) {
			g := newPoolGateway(t, []string{truncatingServer(t, declareLength).URL}, "")
			resp, got, err := g.do(t, chatBody, nil)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d", resp.StatusCode)
			}
			if err == nil {
				t.Fatalf("truncated body %q was delivered as if complete", got)
			}
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("read error = %v, want unexpected EOF", err)
			}
			waitFor(t, "relay error counted", func() bool { return g.outcomes(t).RelayErrors == 1 })
		})
	}
}

// waitFor polls cond until it holds or the test's step timeout passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(streamStepTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for: %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestClientAbortCancelsUpstreamWithinOneSecond(t *testing.T) {
	tests := []struct {
		name    string
		clients int
		body    string
		hdr     map[string]string
	}{
		{name: "before response headers", clients: 1, body: chatBody,
			hdr: map[string]string{mockllm.HeaderFault: "hang"}},
		{name: "non-streaming, while the replica decodes", clients: 1, body: chatBody,
			hdr: map[string]string{mockllm.HeaderOutputTokens: "1000"}},
		{name: "mid-stream", clients: 1, body: streamBody,
			hdr: map[string]string{mockllm.HeaderOutputTokens: "1000"}},
		{name: "burst of mid-stream aborts", clients: 100, body: streamBody,
			hdr: map[string]string{mockllm.HeaderOutputTokens: "1000"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock := startMock(t, func(c *mockllm.Config) { c.TokensPerSecond = 10 }) // 1000 tokens: ~100 s
			g := newPoolGateway(t, []string{mock.URL()}, "")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var wg sync.WaitGroup
			started := make(chan struct{}, tc.clients)
			for range tc.clients {
				wg.Go(func() { abortingClient(ctx, g.gw, tc.body, tc.hdr, started) })
			}
			stream := tc.body == streamBody
			for range tc.clients {
				select {
				case <-started:
				case <-time.After(streamStepTimeout):
					t.Fatal("clients did not start")
				}
			}
			// Non-streaming clients only know the request was sent; wait
			// until the replica holds them all.
			waitFor(t, "requests at the replica", func() bool { return mock.Mock().Stats().Running == int64(tc.clients) })

			abortAt := time.Now()
			cancel()
			want := uint64(tc.clients)
			for mock.Mock().Stats().Cancelled < want {
				if time.Since(abortAt) > cancelDeadline {
					t.Fatalf("after %s only %d of %d upstream requests were cancelled (stream=%v)",
						cancelDeadline, mock.Mock().Stats().Cancelled, want, stream)
				}
				time.Sleep(time.Millisecond)
			}
			t.Logf("%d upstream requests cancelled %s after the clients left", want, time.Since(abortAt))
			wg.Wait()
			waitFor(t, "disconnects counted", func() bool { return g.outcomes(t).ClientDisconnects == float64(want) })
			waitFor(t, "requests counted", func() bool { return telemetrytest.Sum(t, g.tel.Metrics(), telemetry.MetricRequests) == float64(want) }) // when each handler returns
			if s := g.outcomes(t); s.RelayErrors != 0 || s.UpstreamErrors != 0 {
				t.Fatalf("stats = %+v: a client abort must not count as an upstream failure", s)
			}
		})
	}
}

// abortingClient sends one request through gw with ctx and signals started
// once the request is under way: after the first body bytes for a stream,
// right after sending otherwise. It then holds the connection until ctx is
// cancelled.
func abortingClient(ctx context.Context, gw *httptest.Server, body string, hdr map[string]string, started chan<- struct{}) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, gw.URL+PathChatCompletions, strings.NewReader(body))
	if err != nil {
		started <- struct{}{}
		return
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	if !strings.Contains(body, `"stream":true`) {
		started <- struct{}{}
		if resp, err := gw.Client().Do(req); err == nil {
			_ = resp.Body.Close()
		}
		return
	}
	resp, err := gw.Client().Do(req)
	if err != nil {
		started <- struct{}{}
		return
	}
	defer func() { _ = resp.Body.Close() }()
	buf := make([]byte, 64)
	_, _ = resp.Body.Read(buf) // first event relayed
	started <- struct{}{}
	_, _ = io.Copy(io.Discard, resp.Body) // until ctx is cancelled
}

// The telemetry middleware wraps the ResponseWriter, which hides net/http's
// own close-after-413 hint from MaxBytesReader; the handler must still close.
func TestOversizedBodyClosesConnection(t *testing.T) {
	f := newFixture(t, "", Options{MaxRequestBodyBytes: 64})
	mux := http.NewServeMux()
	f.h.Register(mux)
	gw := httptest.NewServer(telemetry.New(telemetry.Options{}).Middleware(mux))
	t.Cleanup(gw.Close)
	resp, err := gw.Client().Post(gw.URL+PathCompletions, "application/json", bytes.NewReader(bytes.Repeat([]byte("a"), 1<<10)))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d (%s)", resp.StatusCode, body)
	}
	if !resp.Close {
		t.Fatal("413 response does not close the connection; the unread body could be parsed as a next request")
	}
}

func TestSSETailTerminator(t *testing.T) {
	tests := []struct {
		name   string
		writes []string
		want   string
	}{
		{name: "nothing relayed", want: ""},
		{name: "event boundary", writes: []string{"data: a\n\n"}, want: ""},
		{name: "CRLF boundary", writes: []string{"data: a\r\n\r\n"}, want: ""},
		{name: "CR boundary", writes: []string{"data: a\r\r"}, want: ""},
		{name: "boundary split across writes", writes: []string{"data: a\n", "\n"}, want: ""},
		{name: "boundary split byte by byte", writes: []string{"data: a", "\r", "\n", "\r", "\n"}, want: ""},
		{name: "line ended, event open", writes: []string{"data: a\n"}, want: "\n"},
		{name: "CRLF line ended, event open", writes: []string{"data: a\r\n"}, want: "\n"},
		{name: "mid-line", writes: []string{"data: a\n\n", "data: {\"b"}, want: "\n\n"},
		{name: "mid-line after lone CR", writes: []string{"data: a\r"}, want: "\n\n"},
		{name: "short write mid-line", writes: []string{"d"}, want: "\n\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var tail sseTail
			for _, w := range tc.writes {
				tail.observe([]byte(w))
			}
			if got := tail.terminator(); got != tc.want {
				t.Fatalf("terminator() = %q, want %q", got, tc.want)
			}
		})
	}
}
