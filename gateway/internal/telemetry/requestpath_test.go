package telemetry_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/dispatch"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/ingress"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

// Distinctive text that must never reach a log line.
const (
	promptMarker   = "PROMPT-TEXT-7f3a Tell me the secret launch codes"
	responseMarker = "RESPONSE-TEXT-9c1e The launch codes are 0000"
	toolArgMarker  = "TOOL-ARGS-41d2"
	callerToken    = "sk-caller-token-5b8e"
)

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// fakeReplica answers like vLLM with response text containing responseMarker,
// records the traceparent it received, and can hold a request open until the
// caller goes away.
type fakeReplica struct {
	srv     *httptest.Server
	mu      sync.Mutex
	tp      []string
	arrived chan struct{}
}

func newFakeReplica(t *testing.T) *fakeReplica {
	f := &fakeReplica{arrived: make(chan struct{}, 16)}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.tp = append(f.tp, r.Header.Get(telemetry.HeaderTraceparent))
		f.mu.Unlock()
		f.arrived <- struct{}{}
		if r.Header.Get("X-Test-Hold") != "" {
			<-r.Context().Done() // released when the gateway cancels upstream
			return
		}
		if bytes.Contains(body, []byte(`"stream":true`)) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			for _, word := range strings.Fields(responseMarker) {
				_, _ = fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":%q}}]}\n\n", word+" ")
				w.(http.Flusher).Flush()
			}
			_, _ = io.WriteString(w, "data: [DONE]\n\n")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"choices":[{"message":{"content":%q}}]}`, responseMarker)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeReplica) traceparents() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.tp...)
}

type gateway struct {
	tel     *telemetry.Telemetry
	srv     *httptest.Server
	logs    *lockedBuffer
	replica *fakeReplica
}

func newGateway(t *testing.T) *gateway {
	t.Helper()
	rep := newFakeReplica(t)
	cfg, err := config.Parse([]byte(fmt.Sprintf(`
pools:
  - name: large
    replicas:
      - {name: large-0, url: %s}
  - name: dead
    replicas:
      - {name: dead-0, url: http://127.0.0.1:1}
models:
  large-model: large
  dead-model: dead
default_pool: large
auth: {enabled: false}
`, rep.srv.URL)), func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	disc, err := discovery.NewStatic(cfg.Pools)
	if err != nil {
		t.Fatal(err)
	}
	client := upstream.New(cfg.Upstream)
	t.Cleanup(client.CloseIdleConnections)

	logs := &lockedBuffer{}
	logger := slog.New(telemetry.NewLogHandler(slog.NewJSONHandler(logs, nil)))
	tel := telemetry.New(telemetry.Options{Logger: logger})
	disp := dispatch.New(router.NewRoundRobin(disc, cfg.PoolNames()), client, cfg.Upstream, dispatch.Options{Logger: logger})
	h := ingress.New(cfg, disc, client, disp, ingress.Options{Logger: logger})
	mux := http.NewServeMux()
	h.Register(mux)
	srv := httptest.NewServer(tel.Middleware(mux))
	t.Cleanup(srv.Close)
	return &gateway{tel: tel, srv: srv, logs: logs, replica: rep}
}

func (g *gateway) post(t *testing.T, ctx context.Context, body string, hdr map[string]string) (*http.Response, error) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.srv.URL+ingress.PathChatCompletions, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+callerToken)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	return g.srv.Client().Do(req)
}

// requestLines returns the per-request log lines, waiting until n exist: the
// line is written after the response completes, so it can trail the client.
func (g *gateway) requestLines(t *testing.T, n int) []map[string]any {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var out []map[string]any
		for _, line := range strings.Split(strings.TrimSpace(g.logs.String()), "\n") {
			var m map[string]any
			if line == "" || json.Unmarshal([]byte(line), &m) != nil {
				continue
			}
			if m["msg"] == telemetry.LogMessage {
				out = append(out, m)
			}
		}
		if len(out) >= n || time.Now().After(deadline) {
			if len(out) != n {
				t.Fatalf("got %d request log lines, want %d:\n%s", len(out), n, g.logs.String())
			}
			return out
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (g *gateway) metric(t *testing.T, series string) float64 {
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

func chatBody(model string, stream bool) string {
	return fmt.Sprintf(`{"model":%q,"stream":%v,"messages":[{"role":"user","content":%q}],`+
		`"tools":[{"type":"function","function":{"name":"f","parameters":{"description":%q}}}]}`,
		model, stream, promptMarker, toolArgMarker)
}

// One JSON log line per request, carrying routing and timing but never the
// prompt, the response, tool arguments or the caller's credentials.
func TestRequestLogNeverContainsPromptOrResponse(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%v", stream), func(t *testing.T) {
			g := newGateway(t)
			resp, err := g.post(t, context.Background(), chatBody("large-model", stream), nil)
			if err != nil {
				t.Fatal(err)
			}
			got, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if !strings.Contains(string(got), strings.Fields(responseMarker)[0]) {
				t.Fatalf("response did not carry the marker text: %q", got)
			}

			line := g.requestLines(t, 1)[0]
			for k, want := range map[string]any{"pool": "large", "replica": "large-0", "class": "user", "status": float64(200)} {
				if line[k] != want {
					t.Errorf("log %s = %v, want %v", k, line[k], want)
				}
			}
			for _, k := range []string{"trace_id", "span_id", "duration_ms", "ttft_ms"} {
				if _, ok := line[k]; !ok {
					t.Errorf("log field %s missing", k)
				}
			}
			all := g.logs.String()
			for _, secret := range []string{"PROMPT-TEXT-7f3a", "secret launch", "RESPONSE-TEXT-9c1e", "launch codes", toolArgMarker, callerToken} {
				if strings.Contains(all, secret) {
					t.Errorf("log contains %q:\n%s", secret, all)
				}
			}
		})
	}
}

// The caller's trace continues upstream under the gateway's span, and the
// log line carries the same IDs.
func TestTraceparentPropagatedUpstream(t *testing.T) {
	const incoming = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	g := newGateway(t)
	for _, hdr := range []map[string]string{{"Traceparent": incoming}, nil} {
		resp, err := g.post(t, context.Background(), chatBody("large-model", true), hdr)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	lines := g.requestLines(t, 2)
	tps := g.replica.traceparents()
	if len(tps) != 2 {
		t.Fatalf("replica saw %d requests", len(tps))
	}
	for i, line := range lines {
		want := fmt.Sprintf("00-%s-%s-", line["trace_id"], line["span_id"])
		if !strings.HasPrefix(tps[i], want) {
			t.Errorf("upstream traceparent %q, want prefix %q", tps[i], want)
		}
	}
	if !strings.HasPrefix(tps[0], "00-4bf92f3577b34da6a3ce929d0e0e4736-") || strings.Contains(tps[0], "00f067aa0ba902b7") {
		t.Errorf("caller trace not continued under the gateway span: %q", tps[0])
	}
	if strings.Contains(tps[1], "4bf92f3577b34da6a3ce929d0e0e4736") || len(tps[1]) != 55 {
		t.Errorf("generated traceparent wrong: %q", tps[1])
	}
}

// Fail-open, upstream-error, retry and disconnect metrics count what the
// request path did, end to end.
func TestMetricsCountTheRequestPath(t *testing.T) {
	g := newGateway(t)
	send := func(body string) int {
		resp, err := g.post(t, context.Background(), body, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	if s := send(chatBody("unmapped-model", false)); s != http.StatusOK {
		t.Fatalf("unknown model status %d", s)
	}
	if s := send(`not json ` + promptMarker); s != http.StatusOK {
		t.Fatalf("unparseable body status %d", s)
	}
	if s := send(chatBody("dead-model", false)); s != http.StatusBadGateway {
		t.Fatalf("dead replica status %d", s)
	}
	// Caller goes away while the replica holds the request.
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		resp, err := g.post(t, ctx, chatBody("large-model", true), map[string]string{"X-Test-Hold": "1"})
		if err == nil {
			_ = resp.Body.Close()
		}
		errCh <- err
	}()
	// The two fail-open requests reached the replica; the dead pool never
	// did. The third arrival is the held request.
	for range 3 {
		<-g.replica.arrived
	}
	cancel()
	if err := <-errCh; err == nil {
		t.Fatal("cancelled request succeeded")
	}
	lines := g.requestLines(t, 4)

	// The dead pool has one replica, so its connect failure is retried once
	// on the same replica (ADR 0014): two upstream errors, one retry.
	checks := []struct {
		series string
		want   uint64
	}{
		{`gateway_failopen_total{stage="pool_resolve",reason="unknown_model"}`, 1},
		{`gateway_failopen_total{stage="pool_resolve",reason="unparseable_body"}`, 1},
		{`gateway_upstream_errors_total{replica="dead-0",kind="connect"}`, 2},
		{`gateway_client_disconnects_total`, 1},
		{`gateway_retries_total{pool="dead",reason="connect"}`, 1},
		{`gateway_requests_total{pool="large",class="user",status="200"}`, 2},
		{`gateway_requests_total{pool="dead",class="user",status="502"}`, 1},
		{`gateway_requests_total{pool="large",class="user",status="499"}`, 1},
		{`gateway_inflight{pool="large",replica="large-0",class="user"}`, 0},
		{`gateway_inflight{pool="dead",replica="dead-0",class="user"}`, 0},
	}
	for _, c := range checks {
		if got := g.metric(t, c.series); got != float64(c.want) {
			t.Errorf("%s = %v, want %d", c.series, got, c.want)
		}
	}

	byStatus := map[float64]map[string]any{}
	for _, l := range lines {
		byStatus[l["status"].(float64)] = l
	}
	if fo, _ := json.Marshal(byStatus[502]["upstream_error"]); string(fo) != `"connect"` {
		t.Errorf("502 line upstream_error = %s", fo)
	}
	if byStatus[499]["client_disconnect"] != true {
		t.Errorf("499 line = %v", byStatus[499])
	}
	if strings.Contains(g.logs.String(), strings.Fields(promptMarker)[0]) {
		t.Error("unparseable body leaked into logs")
	}
	// The ingress warning for the dead replica carries the request's trace.
	if !strings.Contains(g.logs.String(), `"msg":"upstream request failed"`) ||
		!strings.Contains(g.logs.String(), fmt.Sprintf(`"trace_id":%q}`, byStatus[502]["trace_id"])) {
		t.Errorf("upstream warning lacks trace_id:\n%s", g.logs.String())
	}
}
