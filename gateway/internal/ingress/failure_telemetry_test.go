package ingress

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/mockllm"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// Failure-path telemetry (M0-48): what gateway_inflight, gateway_retries_total,
// gateway_requests_total and the per-request log line say when a request is
// retried or its response fails after the status was sent.

// logBuffer collects log output from the server goroutines.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// telemetryGateway is a poolGateway behind the telemetry middleware.
type telemetryGateway struct {
	*poolGateway
	tel  *telemetry.Telemetry
	logs *logBuffer
}

func newTelemetryGateway(t *testing.T, urls []string, yamlExtra string) *telemetryGateway {
	t.Helper()
	logs := &logBuffer{}
	logger := slog.New(slog.NewJSONHandler(logs, nil))
	tel := telemetry.New(telemetry.Options{Logger: logger})
	g := newPoolGatewayWith(t, urls, yamlExtra, tel, Options{Logger: logger})
	return &telemetryGateway{poolGateway: g, tel: tel, logs: logs}
}

// metric returns the value of one series (0 when absent).
func (g *telemetryGateway) metric(t *testing.T, series string) float64 {
	t.Helper()
	rec := httptest.NewRecorder()
	g.tel.Metrics().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, telemetry.MetricsPath, nil))
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if v, ok := strings.CutPrefix(line, series+" "); ok {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				t.Fatal(err)
			}
			return f
		}
	}
	return 0
}

// requestLine waits for the single per-request log line: it is written when
// the handler returns, which can trail the client.
func (g *telemetryGateway) requestLine(t *testing.T) map[string]any {
	t.Helper()
	var line map[string]any
	waitFor(t, "request log line", func() bool {
		for _, l := range strings.Split(g.logs.String(), "\n") {
			var m map[string]any
			if json.Unmarshal([]byte(l), &m) == nil && m["msg"] == telemetry.LogMessage {
				line = m
				return true
			}
		}
		return false
	})
	return line
}

func inflightSeries(replica string) string {
	return fmt.Sprintf(`gateway_inflight{pool="large",replica=%q,class="user"}`, replica)
}

func requestsSeries(status string) string {
	return fmt.Sprintf(`gateway_requests_total{pool="large",class="user",status=%q}`, status)
}

// A connect retry moves the in-flight count to the replica actually serving,
// counts the retry, and logs the final replica.
func TestConnectRetryMovesInflightAndCountsRetry(t *testing.T) {
	arrived, release := make(chan struct{}), make(chan struct{})
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(arrived)
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"x"}`))
	}))
	t.Cleanup(live.Close)
	g := newTelemetryGateway(t, []string{deadURL(t), live.URL}, "")

	done := make(chan int, 1)
	go func() { // no t.Fatal off the test goroutine: report -1 instead
		resp, err := g.gw.Client().Post(g.gw.URL+PathChatCompletions, "application/json", strings.NewReader(chatBody))
		if err != nil {
			done <- -1
			return
		}
		_ = resp.Body.Close()
		done <- resp.StatusCode
	}()
	<-arrived
	if got := g.metric(t, inflightSeries("r0")); got != 0 {
		t.Errorf("in flight on the dead replica = %v, want 0 after the retry", got)
	}
	if got := g.metric(t, inflightSeries("r1")); got != 1 {
		t.Errorf("in flight on the serving replica = %v, want 1", got)
	}
	close(release)
	if status := <-done; status != http.StatusOK {
		t.Fatalf("status = %d", status)
	}

	line := g.requestLine(t)
	for k, want := range map[string]any{"replica": "r1", "retries": float64(1), "upstream_error": "connect", "status": float64(200)} {
		if line[k] != want {
			t.Errorf("log %s = %v, want %v", k, line[k], want)
		}
	}
	if _, ok := line["outcome"]; ok {
		t.Errorf("successful retry logged outcome %v", line["outcome"])
	}
	checks := map[string]float64{
		inflightSeries("r0"): 0,
		inflightSeries("r1"): 0,
		`gateway_retries_total{pool="large",reason="connect"}`: 1,
		requestsSeries("200"): 1,
	}
	for series, want := range checks {
		if got := g.metric(t, series); got != want {
			t.Errorf("%s = %v, want %v", series, got, want)
		}
	}
	if g.outcomes(t).ConnectRetries != 1 {
		t.Fatalf("stats = %+v, want one connect retry", g.outcomes(t))
	}
}

// With every replica down, the single retry is still counted and the
// in-flight count is released from both replicas.
func TestFailedRetryReleasesInflight(t *testing.T) {
	g := newTelemetryGateway(t, []string{deadURL(t), deadURL(t)}, "")
	resp, _, _ := g.do(t, chatBody, nil)
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	line := g.requestLine(t)
	if line["replica"] != "r1" || line["retries"] != float64(1) || line["status"] != float64(502) {
		t.Errorf("log line = %v", line)
	}
	for series, want := range map[string]float64{
		inflightSeries("r0"): 0,
		inflightSeries("r1"): 0,
		`gateway_retries_total{pool="large",reason="connect"}`: 1,
		requestsSeries("502"): 1,
	} {
		if got := g.metric(t, series); got != want {
			t.Errorf("%s = %v, want %v", series, got, want)
		}
	}
}

// A response that fails after its status was sent is counted and logged under
// a distinct outcome, never as the upstream's 200.
func TestFailedResponseHasDistinctOutcome(t *testing.T) {
	tests := []struct {
		name        string
		url         func(t *testing.T) string
		body        string
		fault       string
		wantOutcome string // "" for a complete response
	}{
		{name: "complete stream", body: streamBody,
			url: func(t *testing.T) string { return startMock(t, nil).URL() }},
		{name: "complete body", body: chatBody,
			url: func(t *testing.T) string { return startMock(t, nil).URL() }},
		{name: "replica drops mid-stream", body: streamBody, fault: "drop", wantOutcome: telemetry.OutcomeStreamError,
			url: func(t *testing.T) string { return startMock(t, nil).URL() }},
		{name: "replica stops mid-event", body: streamBody, wantOutcome: telemetry.OutcomeStreamError,
			url: func(t *testing.T) string { return partialEventServer(t).URL }},
		{name: "chunked body truncated", body: chatBody, wantOutcome: telemetry.OutcomeAborted,
			url: func(t *testing.T) string { return truncatingServer(t, false).URL }},
		{name: "declared-length body truncated", body: chatBody, wantOutcome: telemetry.OutcomeAborted,
			url: func(t *testing.T) string { return truncatingServer(t, true).URL }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := newTelemetryGateway(t, []string{tc.url(t)}, "")
			hdr := map[string]string{mockllm.HeaderOutputTokens: "8"}
			if tc.fault != "" {
				hdr[mockllm.HeaderFault] = tc.fault
			}
			resp, _, _ := g.do(t, tc.body, hdr)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status line = %d, want the upstream 200", resp.StatusCode)
			}
			line := g.requestLine(t)
			if line["status"] != float64(200) {
				t.Errorf("logged status = %v, want the 200 on the status line", line["status"])
			}
			wantLabel := tc.wantOutcome
			if wantLabel == "" {
				wantLabel = "200"
				if _, ok := line["outcome"]; ok {
					t.Errorf("complete response logged outcome %v", line["outcome"])
				}
			} else {
				if line["outcome"] != tc.wantOutcome {
					t.Errorf("logged outcome = %v, want %s", line["outcome"], tc.wantOutcome)
				}
				if got := g.metric(t, requestsSeries("200")); got != 0 {
					t.Errorf("failed response counted as status 200 (%v)", got)
				}
			}
			if got := g.metric(t, requestsSeries(wantLabel)); got != 1 {
				t.Errorf("%s = %v, want 1", requestsSeries(wantLabel), got)
			}
			if got := g.metric(t, inflightSeries("r0")); got != 0 {
				t.Errorf("in flight not released: %v", got)
			}
		})
	}
}

// A caller that goes away after the status line was sent is counted and
// logged as client_closed, never as the upstream's 200; one that goes away
// before any status keeps the 499. Neither is an upstream failure.
func TestClientDisconnectHasDistinctOutcome(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		hdr         map[string]string
		wantStatus  float64 // logged status line
		wantLabel   string  // gateway_requests_total status label
		wantOutcome string  // logged outcome, "" for none
	}{
		{name: "mid-stream after a 200", body: streamBody,
			hdr:        map[string]string{mockllm.HeaderOutputTokens: "1000"},
			wantStatus: 200, wantLabel: telemetry.OutcomeClientClosed, wantOutcome: telemetry.OutcomeClientClosed},
		{name: "before response headers", body: chatBody,
			hdr:        map[string]string{mockllm.HeaderFault: "hang"},
			wantStatus: 499, wantLabel: "499"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock := startMock(t, func(c *mockllm.Config) { c.TokensPerSecond = 10 }) // 1000 tokens: ~100 s
			g := newTelemetryGateway(t, []string{mock.URL()}, "")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			started := make(chan struct{}, 1)
			done := make(chan struct{})
			go func() {
				defer close(done)
				abortingClient(ctx, g.gw, tc.body, tc.hdr, started)
			}()
			select {
			case <-started:
			case <-time.After(streamStepTimeout):
				t.Fatal("client did not start")
			}
			waitFor(t, "request at the replica", func() bool { return mock.Mock().Stats().Running == 1 })
			cancel()
			<-done

			line := g.requestLine(t)
			if line["status"] != tc.wantStatus {
				t.Errorf("logged status = %v, want %v", line["status"], tc.wantStatus)
			}
			if got, _ := line["outcome"].(string); got != tc.wantOutcome {
				t.Errorf("logged outcome = %q, want %q", got, tc.wantOutcome)
			}
			if line["client_disconnect"] != true {
				t.Errorf("log line lacks client_disconnect: %v", line)
			}
			if _, ok := line["upstream_error"]; ok {
				t.Errorf("client disconnect logged as an upstream error: %v", line)
			}
			for series, want := range map[string]float64{
				requestsSeries(tc.wantLabel):       1,
				requestsSeries("200"):              0,
				`gateway_client_disconnects_total`: 1,
				inflightSeries("r0"):               0,
			} {
				if got := g.metric(t, series); got != want {
					t.Errorf("%s = %v, want %v", series, got, want)
				}
			}
			if s := g.outcomes(t); s.RelayErrors != 0 || s.UpstreamErrors != 0 || s.ClientDisconnects != 1 {
				t.Errorf("stats = %+v, want one disconnect and no upstream failure", s)
			}
		})
	}
}

// A /v1/models caller that goes away mid-relay is counted as a client
// disconnect, as a generation's is.
func TestModelsClientDisconnectCounted(t *testing.T) {
	sent := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"object":"list","data":[`))
		_ = http.NewResponseController(w).Flush()
		close(sent)
		<-r.Context().Done() // the rest never comes
	}))
	t.Cleanup(up.Close)
	g := newTelemetryGateway(t, []string{up.URL}, "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, g.gw.URL+PathModels, nil)
	resp, err := g.gw.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	<-sent
	cancel()
	_ = resp.Body.Close()
	line := g.requestLine(t)
	if line["client_disconnect"] != true {
		t.Errorf("log line lacks client_disconnect: %v", line)
	}
	if got := g.metric(t, `gateway_client_disconnects_total`); got != 1 {
		t.Errorf("gateway_client_disconnects_total = %v, want 1", got)
	}
}
