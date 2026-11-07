package auth

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
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

// telemetryStack wires the middlewares as cmd/gateway does: telemetry outside,
// auth inside it, then the request path.
func telemetryStack(t *testing.T, cfg config.Auth) (http.Handler, *telemetry.Telemetry, *lockedBuffer) {
	t.Helper()
	logs := &lockedBuffer{}
	tel := telemetry.New(telemetry.Options{Logger: slog.New(slog.NewJSONHandler(logs, nil))})
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return tel.Middleware(mustNew(t, cfg).Middleware(ok)), tel, logs
}

func scrapeMetrics(t *testing.T, tel *telemetry.Telemetry) string {
	t.Helper()
	rec := httptest.NewRecorder()
	tel.Metrics().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return rec.Body.String()
}

func TestTelemetryCountsRejectionsByReason(t *testing.T) {
	h, tel, logs := telemetryStack(t, enabledConfig())
	send := func(authorization string) {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{}`))
		if authorization != "" {
			req.Header.Set("Authorization", authorization)
		}
		h.ServeHTTP(httptest.NewRecorder(), req)
	}
	send("")                                   // missing
	send("Basic " + chatToken)                 // missing (wrong scheme)
	send("Bearer invalid-token-SECRET-3e9b77") // invalid
	send("Bearer " + chatToken)                // chat-service
	send("Bearer " + batchToken)               // batch-service

	metrics := scrapeMetrics(t, tel)
	for _, want := range []string{
		`gateway_auth_rejections_total{reason="missing_api_key"} 2`,
		`gateway_auth_rejections_total{reason="invalid_api_key"} 1`,
		`gateway_requests_total{pool="none",class="default",status="401"} 3`,
	} {
		if !strings.Contains(metrics, want+"\n") {
			t.Errorf("metrics missing %q\n%s", want, metrics)
		}
	}

	var callers, rejections []string
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line is not JSON: %q: %v", line, err)
		}
		c, _ := m["caller"].(string)
		r, _ := m["auth_rejection"].(string)
		callers = append(callers, c)
		rejections = append(rejections, r)
	}
	wantCallers := []string{"", "", "", "chat-service", "batch-service"}
	wantRejections := []string{CodeMissingToken, CodeMissingToken, CodeInvalidToken, "", ""}
	if strings.Join(callers, ",") != strings.Join(wantCallers, ",") {
		t.Errorf("logged callers = %q, want %q", callers, wantCallers)
	}
	if strings.Join(rejections, ",") != strings.Join(wantRejections, ",") {
		t.Errorf("logged rejections = %q, want %q", rejections, wantRejections)
	}

	// Neither a valid nor an invalid token reaches the logs or the metrics.
	for _, secret := range []string{chatToken, batchToken, "invalid-token-SECRET-3e9b77", "SECRET"} {
		if strings.Contains(logs.String(), secret) {
			t.Errorf("token %q logged:\n%s", secret, logs)
		}
		if strings.Contains(metrics, secret) {
			t.Errorf("token %q in metrics", secret)
		}
	}
	// Caller names are log fields, never metric labels.
	if strings.Contains(metrics, "chat-service") {
		t.Error("caller name used as a metric label")
	}
}

func TestTelemetryLogsAnonymousCallerWhenDisabled(t *testing.T) {
	h, tel, logs := telemetryStack(t, config.Auth{Enabled: false})
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer "+chatToken)
	h.ServeHTTP(httptest.NewRecorder(), req)

	var m map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(logs.String())), &m); err != nil {
		t.Fatalf("log line: %v", err)
	}
	if m["caller"] != AnonymousName {
		t.Errorf("caller = %v, want %q", m["caller"], AnonymousName)
	}
	if strings.Contains(logs.String(), chatToken) {
		t.Error("token logged")
	}
	if strings.Contains(scrapeMetrics(t, tel), `gateway_auth_rejections_total{`) {
		t.Error("rejection counted with auth disabled")
	}
}

// Without the telemetry middleware (no record in the context) auth still works.
func TestMiddlewareWithoutTelemetry(t *testing.T) {
	a := mustNew(t, enabledConfig())
	for _, hdr := range []string{"", "Bearer nope", "Bearer " + chatToken} {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		if hdr != "" {
			req.Header.Set("Authorization", hdr)
		}
		a.Middleware(echoCaller(t)).ServeHTTP(httptest.NewRecorder(), req)
	}
	if s := a.Stats(); s.MissingToken != 1 || s.InvalidToken != 1 || s.Authenticated != 1 {
		t.Fatalf("stats = %+v", s)
	}
}
