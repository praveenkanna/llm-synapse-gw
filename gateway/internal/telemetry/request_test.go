package telemetry

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// A connect retry moves the in-flight count to the replica actually serving
// and is counted in gateway_retries_total; the log line names the final
// replica and the number of retries.
func TestRerouteMovesInflight(t *testing.T) {
	tel, logs := newTestTelemetry(t)
	var during string
	h := tel.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := FromContext(r.Context())
		rec.Route("large", "large-0")
		rec.UpstreamError("large-0", "connect")
		rec.Reroute("large-1", "connect")
		during = scrape(t, tel.Metrics())
		w.WriteHeader(http.StatusOK)
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/completions", nil))

	for _, line := range []string{
		`gateway_inflight{pool="large",replica="large-0",class="default"} 0`,
		`gateway_inflight{pool="large",replica="large-1",class="default"} 1`,
		`gateway_retries_total{pool="large",reason="connect"} 1`,
	} {
		if !strings.Contains(during, line+"\n") {
			t.Errorf("during the request, metrics missing %q\n%s", line, during)
		}
	}
	after := scrape(t, tel.Metrics())
	for _, line := range []string{
		`gateway_inflight{pool="large",replica="large-0",class="default"} 0`,
		`gateway_inflight{pool="large",replica="large-1",class="default"} 0`,
		`gateway_requests_total{pool="large",class="default",status="200"} 1`,
	} {
		if !strings.Contains(after, line+"\n") {
			t.Errorf("after the request, metrics missing %q\n%s", line, after)
		}
	}
	lines := logLines(t, logs)
	if len(lines) != 1 || lines[0]["replica"] != "large-1" || lines[0]["retries"] != float64(1) {
		t.Fatalf("log lines = %v, want replica large-1 with 1 retry", lines)
	}
}

func TestRouteTwiceMovesInflight(t *testing.T) {
	tests := []struct {
		name     string
		second   [2]string // pool, replica
		wantHeld map[string]int
	}{
		{"same replica keeps one count", [2]string{"large", "large-0"}, map[string]int{"large-0": 1}},
		{"other replica moves the count", [2]string{"large", "large-1"}, map[string]int{"large-0": 0, "large-1": 1}},
		{"no replica releases the count", [2]string{"large", ""}, map[string]int{"large-0": 0}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := NewMetrics()
			rec := &Request{m: m, class: ClassDefault}
			rec.Route("large", "large-0")
			rec.Route(tc.second[0], tc.second[1])
			for replica, want := range tc.wantHeld {
				if got := m.inflight.with("large", replica, ClassDefault).v.Load(); got != int64(want) {
					t.Errorf("inflight %s = %d, want %d", replica, got, want)
				}
			}
		})
	}
}

// A response that fails after its status line was sent is counted under a
// distinct outcome instead of the status, and the log line says why.
func TestMiddlewareFailedResponseOutcome(t *testing.T) {
	tests := []struct {
		name        string
		handler     func(w http.ResponseWriter, rec *Request)
		panics      bool
		wantStatus  float64 // logged status line
		wantLabel   string  // gateway_requests_total status label
		wantOutcome string  // logged outcome, "" for none
	}{
		{name: "replica failed mid-stream", handler: func(w http.ResponseWriter, rec *Request) {
			rec.Route("large", "large-0")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("data: x\n\n"))
			rec.FirstByte()
			rec.UpstreamError("large-0", "other")
			rec.ResponseFailed()
			_, _ = w.Write([]byte(`data: {"error":{}}` + "\n\n"))
		}, wantStatus: 200, wantLabel: OutcomeStreamError, wantOutcome: OutcomeStreamError},
		{name: "non-streaming body aborted", handler: func(w http.ResponseWriter, rec *Request) {
			rec.Route("large", "large-0")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":`))
			rec.ResponseFailed()
			panic(http.ErrAbortHandler)
		}, panics: true, wantStatus: 200, wantLabel: OutcomeAborted, wantOutcome: OutcomeAborted},
		{name: "panic after the status line", handler: func(w http.ResponseWriter, _ *Request) {
			w.WriteHeader(http.StatusOK)
			panic("bug")
		}, panics: true, wantStatus: 200, wantLabel: OutcomeAborted, wantOutcome: OutcomeAborted},
		{name: "client went away mid-stream", handler: func(w http.ResponseWriter, rec *Request) {
			rec.Route("large", "large-0")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("data: x\n\n"))
			rec.FirstByte()
			rec.ClientDisconnected()
		}, wantStatus: 200, wantLabel: OutcomeClientClosed, wantOutcome: OutcomeClientClosed},
		{name: "client went away mid-body", handler: func(w http.ResponseWriter, rec *Request) {
			rec.Route("large", "large-0")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":`))
			rec.ClientDisconnected()
		}, wantStatus: 200, wantLabel: OutcomeClientClosed, wantOutcome: OutcomeClientClosed},
		{name: "client went away while the stream error was written", handler: func(w http.ResponseWriter, rec *Request) {
			rec.Route("large", "large-0")
			w.WriteHeader(http.StatusOK)
			rec.UpstreamError("large-0", "other")
			rec.ResponseFailed()
			rec.ClientDisconnected()
		}, wantStatus: 200, wantLabel: OutcomeStreamError, wantOutcome: OutcomeStreamError},
		{name: "client went away before any response", handler: func(_ http.ResponseWriter, rec *Request) {
			rec.Route("large", "large-0")
			rec.ClientDisconnected()
		}, wantStatus: 499, wantLabel: "499"},
		{name: "panic before any response", handler: func(http.ResponseWriter, *Request) {
			panic(http.ErrAbortHandler)
		}, panics: true, wantStatus: 500, wantLabel: "500"},
		{name: "complete response", handler: func(w http.ResponseWriter, _ *Request) {
			_, _ = w.Write([]byte("{}"))
		}, wantStatus: 200, wantLabel: "200"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tel, logs := newTestTelemetry(t)
			h := tel.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				tc.handler(w, FromContext(r.Context()))
			}))
			func() {
				defer func() {
					if v := recover(); (v != nil) != tc.panics {
						t.Fatalf("recovered %v, want panic=%v", v, tc.panics)
					}
				}()
				h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/completions", nil))
			}()
			lines := logLines(t, logs)
			if len(lines) != 1 {
				t.Fatalf("got %d log lines", len(lines))
			}
			if lines[0]["status"] != tc.wantStatus {
				t.Errorf("logged status = %v, want %v", lines[0]["status"], tc.wantStatus)
			}
			if got, _ := lines[0]["outcome"].(string); got != tc.wantOutcome {
				t.Errorf("logged outcome = %q, want %q", got, tc.wantOutcome)
			}
			metrics := scrape(t, tel.Metrics())
			want := `gateway_requests_total{pool="` + lines[0]["pool"].(string) + `",class="default",status="` + tc.wantLabel + `"} 1`
			if !strings.Contains(metrics, want+"\n") {
				t.Errorf("metrics missing %q\n%s", want, metrics)
			}
			if strings.Contains(metrics, "gateway_inflight{") && !strings.Contains(metrics, `class="default"} 0`) {
				t.Errorf("inflight not released\n%s", metrics)
			}
		})
	}
}
