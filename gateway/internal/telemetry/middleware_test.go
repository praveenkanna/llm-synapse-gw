package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeClock advances by step on every reading.
type fakeClock struct {
	mu   sync.Mutex
	t    time.Time
	step time.Duration
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(c.step)
	return c.t
}

// syncBuffer is a bytes.Buffer safe for a logger and a test reading it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func newTestTelemetry(t *testing.T) (*Telemetry, *syncBuffer) {
	t.Helper()
	logs := &syncBuffer{}
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0), step: 10 * time.Millisecond}
	tel := New(Options{Logger: slog.New(slog.NewJSONHandler(logs, nil)), Now: clock.now})
	return tel, logs
}

func logLines(t *testing.T, logs *syncBuffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line is not JSON: %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

func TestMiddlewareLogsOneLinePerRequest(t *testing.T) {
	tel, logs := newTestTelemetry(t)
	h := tel.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := FromContext(r.Context())
		rec.FailOpen("pool_resolve", "unknown_model")
		rec.Route("large", "large-0")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: x\n\n"))
		rec.FirstByte()
		rec.FirstByte() // later chunks don't move TTFT
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions?secret=1", strings.NewReader(`{"model":"m"}`))
	req.Header.Set(HeaderTraceparent, validTP)
	h.ServeHTTP(httptest.NewRecorder(), req)

	lines := logLines(t, logs)
	if len(lines) != 1 {
		t.Fatalf("got %d log lines, want 1", len(lines))
	}
	got := lines[0]
	want := map[string]any{
		"msg": LogMessage, "trace_id": validTraceID, "parent_span_id": validParentID,
		"method": "POST", "path": "/v1/chat/completions", "pool": "large", "replica": "large-0",
		"class": ClassDefault, "status": float64(200), "request_bytes": float64(13), "response_bytes": float64(23),
		// clock: start=+10ms, FirstByte=+20ms, finish=+30ms
		"ttft_ms": float64(10), "duration_ms": float64(20),
		"failopen": []any{"pool_resolve:unknown_model"},
	}
	for k, v := range want {
		gb, _ := json.Marshal(got[k])
		wb, _ := json.Marshal(v)
		if !bytes.Equal(gb, wb) {
			t.Errorf("log field %s = %s, want %s", k, gb, wb)
		}
	}
	if span, _ := got["span_id"].(string); len(span) != spanIDHexLen || span == validParentID {
		t.Errorf("span_id = %q, want the gateway's own span", span)
	}
	for _, absent := range []string{"upstream_error", "client_disconnect", "outcome", "retries"} {
		if _, ok := got[absent]; ok {
			t.Errorf("unexpected field %s", absent)
		}
	}
	if strings.Contains(logs.String(), "secret") {
		t.Error("query string leaked into the log")
	}

	metrics := scrape(t, tel.Metrics())
	for _, line := range []string{
		`gateway_requests_total{pool="large",class="default",status="200"} 1`,
		`gateway_inflight{pool="large",replica="large-0",class="default"} 0`,
		`gateway_ttft_seconds_bucket{pool="large",class="default",le="0.01"} 1`,
		`gateway_ttft_seconds_count{pool="large",class="default"} 1`,
		`gateway_request_duration_seconds_count{pool="large",class="default"} 1`,
		`gateway_failopen_total{stage="pool_resolve",reason="unknown_model"} 1`,
	} {
		if !strings.Contains(metrics, line+"\n") {
			t.Errorf("metrics missing %q\n%s", line, metrics)
		}
	}
}

func TestMiddlewareFinalStatus(t *testing.T) {
	tests := []struct {
		name       string
		handler    func(w http.ResponseWriter, rec *Request)
		wantStatus string
		wantTTFT   bool
		wantFields []string
	}{
		{"handler writes nothing", func(http.ResponseWriter, *Request) {}, "200", false, nil},
		{"caller went away before any response", func(_ http.ResponseWriter, rec *Request) {
			rec.Route("large", "large-0")
			rec.ClientDisconnected()
			rec.ClientDisconnected() // counted once
		}, "499", false, []string{"client_disconnect"}},
		{"upstream error answered 502", func(w http.ResponseWriter, rec *Request) {
			rec.Route("large", "large-1")
			rec.UpstreamError("large-1", "connect")
			w.WriteHeader(http.StatusBadGateway)
		}, "502", false, []string{"upstream_error"}},
		{"error body relayed does not count as TTFT", func(w http.ResponseWriter, rec *Request) {
			rec.Route("large", "large-0")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{}}`))
			rec.FirstByte()
		}, "400", false, []string{"ttft_ms"}},
		{"informational header ignored", func(w http.ResponseWriter, rec *Request) {
			w.WriteHeader(http.StatusEarlyHints)
			w.WriteHeader(http.StatusCreated)
		}, "201", false, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tel, logs := newTestTelemetry(t)
			h := tel.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				tc.handler(w, FromContext(r.Context()))
			}))
			h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/completions", nil))
			lines := logLines(t, logs)
			if len(lines) != 1 {
				t.Fatalf("got %d log lines", len(lines))
			}
			if got := lines[0]["status"]; got != mustFloat(t, tc.wantStatus) {
				t.Errorf("logged status = %v, want %s", got, tc.wantStatus)
			}
			for _, f := range tc.wantFields {
				if _, ok := lines[0][f]; !ok {
					t.Errorf("log field %s missing", f)
				}
			}
			metrics := scrape(t, tel.Metrics())
			if !strings.Contains(metrics, `status="`+tc.wantStatus+`"} 1`) {
				t.Errorf("requests_total status %s missing\n%s", tc.wantStatus, metrics)
			}
			if has := strings.Contains(metrics, "gateway_ttft_seconds_count"); has != tc.wantTTFT {
				t.Errorf("ttft observed = %v, want %v", has, tc.wantTTFT)
			}
			if strings.Contains(metrics, "gateway_inflight{") && !strings.Contains(metrics, `class="default"} 0`) {
				t.Errorf("inflight not released\n%s", metrics)
			}
		})
	}
}

func mustFloat(t *testing.T, s string) float64 {
	t.Helper()
	var f float64
	if err := json.Unmarshal([]byte(s), &f); err != nil {
		t.Fatal(err)
	}
	return f
}

func TestMiddlewarePanicIsLoggedAndPropagated(t *testing.T) {
	tel, logs := newTestTelemetry(t)
	h := tel.Middleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		FromContext(r.Context()).Route("large", "large-0")
		panic(http.ErrAbortHandler)
	}))
	func() {
		defer func() {
			if v := recover(); v != http.ErrAbortHandler {
				t.Fatalf("recovered %v, want http.ErrAbortHandler re-panicked", v)
			}
		}()
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/completions", nil))
	}()
	lines := logLines(t, logs)
	if len(lines) != 1 || lines[0]["status"] != float64(500) {
		t.Fatalf("log lines = %v, want one line with status 500", lines)
	}
	if m := scrape(t, tel.Metrics()); !strings.Contains(m, `gateway_inflight{pool="large",replica="large-0",class="default"} 0`) {
		t.Fatalf("inflight not released after panic\n%s", m)
	}
}

func TestMiddlewareTraceContext(t *testing.T) {
	tests := []struct {
		name          string
		traceparent   string
		wantContinued bool
	}{
		{"valid traceparent continued", validTP, true},
		{"missing traceparent generated", "", false},
		{"invalid traceparent replaced", "00-zz-zz-01", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tel, logs := newTestTelemetry(t)
			var seen http.Header
			var seenTraceID string
			h := tel.Middleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				seen = r.Header.Clone()
				seenTraceID = FromContext(r.Context()).TraceID()
			}))
			req := httptest.NewRequest(http.MethodPost, "/v1/completions", nil)
			if tc.traceparent != "" {
				req.Header.Set(HeaderTraceparent, tc.traceparent)
			}
			req.Header.Set(HeaderTracestate, "vendor=abc")
			original := req.Header.Clone()
			h.ServeHTTP(httptest.NewRecorder(), req)

			if got, want := req.Header, original; !equalHeaders(got, want) {
				t.Fatalf("caller's headers modified: %v, want %v", got, want)
			}
			tp := seen.Values(HeaderTraceparent)
			if len(tp) != 1 {
				t.Fatalf("forwarded traceparent values = %v", tp)
			}
			traceID, parentID, _, ok := parseTraceparent(tp[0])
			if !ok || parentID == validParentID {
				t.Fatalf("forwarded traceparent %q must be valid with the gateway's span", tp[0])
			}
			if traceID != seenTraceID {
				t.Fatalf("context trace %q != forwarded %q", seenTraceID, traceID)
			}
			if (traceID == validTraceID) != tc.wantContinued {
				t.Fatalf("trace continued = %v, want %v", traceID == validTraceID, tc.wantContinued)
			}
			if (seen.Get(HeaderTracestate) == "vendor=abc") != tc.wantContinued {
				t.Fatalf("tracestate = %q (continued=%v)", seen.Get(HeaderTracestate), tc.wantContinued)
			}
			lines := logLines(t, logs)
			if lines[0]["trace_id"] != traceID || lines[0]["span_id"] != parentID {
				t.Fatalf("log trace %v/%v, forwarded %s/%s", lines[0]["trace_id"], lines[0]["span_id"], traceID, parentID)
			}
		})
	}
}

func equalHeaders(a, b http.Header) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if strings.Join(v, "\x00") != strings.Join(b[k], "\x00") {
			return false
		}
	}
	return true
}

func TestMiddlewareSkipsOtherPaths(t *testing.T) {
	tel, logs := newTestTelemetry(t)
	var hasRecord bool
	var tp string
	h := tel.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hasRecord = FromContext(r.Context()) != nil
		tp = r.Header.Get(HeaderTraceparent)
		w.WriteHeader(http.StatusOK)
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/livez", nil))
	if hasRecord || tp != "" || logs.String() != "" {
		t.Fatalf("health path instrumented: record=%v traceparent=%q logs=%q", hasRecord, tp, logs.String())
	}
	if strings.Contains(scrape(t, tel.Metrics()), "gateway_requests_total{") {
		t.Fatal("health request counted")
	}
}

func TestStatusWriterFlushesThrough(t *testing.T) {
	tel, _ := newTestTelemetry(t)
	h := tel.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("data: x\n\n"))
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Errorf("flush through middleware: %v", err)
		}
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/completions", nil))
	if !rec.Flushed {
		t.Fatal("flush did not reach the underlying writer")
	}
}

func TestNilRequestIsSafe(t *testing.T) {
	var r *Request
	r.Route("p", "r")
	r.Reroute("r2", "connect")
	r.ResponseFailed()
	r.FailOpen("s", "x")
	r.UpstreamError("r", "k")
	r.ClientDisconnected()
	r.FirstByte()
	r.SetCaller("c")
	r.AuthRejected("missing_api_key")
	if r.TraceID() != "" || FromContext(context.Background()) != nil {
		t.Fatal("nil record not inert")
	}
}

func TestLogHandlerAddsTraceID(t *testing.T) {
	var buf syncBuffer
	logger := slog.New(NewLogHandler(slog.NewJSONHandler(&buf, nil))).With("component", "ingress")
	rec := &Request{trace: traceContext{traceID: validTraceID}}
	logger.WarnContext(withRequest(context.Background(), rec), "upstream request failed")
	logger.Warn("no request")
	lines := logLines(t, &buf)
	if len(lines) != 2 || lines[0]["trace_id"] != validTraceID || lines[0]["component"] != "ingress" {
		t.Fatalf("first line = %v", lines)
	}
	if _, ok := lines[1]["trace_id"]; ok {
		t.Fatalf("trace_id added without a request: %v", lines[1])
	}
}

func TestMiddlewareLogsCallerAndAuthRejection(t *testing.T) {
	tests := []struct {
		name       string
		stage      func(*Request)
		status     int
		wantCaller string
		wantReject string // "" means the field is absent
	}{
		{name: "authenticated caller", stage: func(r *Request) { r.SetCaller("chat-service") }, status: http.StatusOK, wantCaller: "chat-service"},
		{name: "missing token", stage: func(r *Request) { r.AuthRejected("missing_api_key") }, status: http.StatusUnauthorized, wantReject: "missing_api_key"},
		{name: "invalid token counted once", stage: func(r *Request) {
			r.AuthRejected("invalid_api_key")
			r.AuthRejected("missing_api_key") // later calls don't count
		}, status: http.StatusUnauthorized, wantReject: "invalid_api_key"},
		{name: "no auth stage", stage: func(*Request) {}, status: http.StatusOK},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tel, logs := newTestTelemetry(t)
			h := tel.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				tc.stage(FromContext(r.Context()))
				w.WriteHeader(tc.status)
			}))
			h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil))

			lines := logLines(t, logs)
			if len(lines) != 1 {
				t.Fatalf("got %d log lines, want 1", len(lines))
			}
			if got := lines[0]["caller"]; got != tc.wantCaller {
				t.Errorf("caller = %v, want %q", got, tc.wantCaller)
			}
			got, ok := lines[0]["auth_rejection"]
			if tc.wantReject == "" && ok {
				t.Errorf("unexpected auth_rejection %v", got)
			}
			if tc.wantReject != "" && got != tc.wantReject {
				t.Errorf("auth_rejection = %v, want %q", got, tc.wantReject)
			}

			metrics := scrape(t, tel.Metrics())
			if !strings.Contains(metrics, "# TYPE "+MetricAuthRejections+" counter\n") {
				t.Errorf("%s not exported\n%s", MetricAuthRejections, metrics)
			}
			if tc.wantReject != "" {
				want := MetricAuthRejections + `{reason="` + tc.wantReject + `"} 1` + "\n"
				if !strings.Contains(metrics, want) {
					t.Errorf("metrics missing %q\n%s", want, metrics)
				}
				if n := strings.Count(metrics, MetricAuthRejections+"{"); n != 1 {
					t.Errorf("got %d rejection series, want 1\n%s", n, metrics)
				}
			}
			if strings.Contains(metrics, "chat-service") {
				t.Error("caller name used as a metric label")
			}
		})
	}
}
