package ingress

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/affinity"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry/telemetrytest"
)

// bodyRoutedTo returns a chat body whose own key picks replica want.
func bodyRoutedTo(t *testing.T, ar *router.AffinityRouter, want string) string {
	t.Helper()
	for i := range 1000 {
		body := fmt.Sprintf(`{"model":"m","messages":[{"role":"user","content":"hello %d"}]}`, i)
		ctx := router.WithRequestBody(context.Background(), []byte(body))
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, PathChatCompletions, nil)
		if err != nil {
			t.Fatal(err)
		}
		l, err := ar.Route(ctx, req, "large")
		if err != nil {
			t.Fatal(err)
		}
		got := l.Replica.Name
		l.Release()
		if got == want {
			return body
		}
	}
	t.Fatalf("no body routes to %s", want)
	return ""
}

// An over-long session header changes routing only: the replica serving the
// connect retry gets it byte for byte, the body key picks the replica, and
// the request counts and logs it once although it was routed twice.
func TestOverLongSessionHeaderThroughTheGateway(t *testing.T) {
	session := strings.Repeat("0123456789abcdef", 64) + "!" // 1,025 bytes
	if len(session) <= affinity.MaxSessionHeaderBytes {
		t.Fatalf("a %d-byte header is not over the limit", len(session))
	}
	var seenSession, seenConversation atomic.Pointer[string]
	var seenBody atomic.Pointer[[]byte]
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s, c := r.Header.Get(affinity.HeaderSessionID), r.Header.Get(affinity.HeaderConversationID)
		seenSession.Store(&s)
		seenConversation.Store(&c)
		b, _ := io.ReadAll(r.Body)
		seenBody.Store(&b)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"x"}`))
	}))
	t.Cleanup(live.Close)

	logs := &logBuffer{}
	tel := telemetry.New(telemetry.Options{Logger: slog.New(slog.NewJSONHandler(logs, nil))})
	g := newLeaseGatewayWith(t, routingPolicy{name: "affinity_ring", affinity: true, ring: true}, []string{deadURL(t), live.URL}, chatBody, leaseOptions{tel: tel})
	body := bodyRoutedTo(t, g.ar, "r0") // the dead replica, so the request retries

	req, err := http.NewRequest(http.MethodPost, g.gw.URL+PathChatCompletions, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(affinity.HeaderSessionID, session)
	req.Header.Set(affinity.HeaderConversationID, session)
	resp, err := g.gw.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}

	if got := seenSession.Load(); got == nil || *got != session {
		t.Errorf("X-Session-Id did not reach the replica unchanged")
	}
	if got := seenConversation.Load(); got == nil || *got != session {
		t.Errorf("X-Conversation-Id did not reach the replica unchanged")
	}
	if got := seenBody.Load(); got == nil || string(*got) != body {
		t.Errorf("the body did not reach the replica unchanged")
	}
	tg := &telemetryGateway{tel: tel, logs: logs}
	line := tg.requestLine(t)
	if got := fmt.Sprint(line["failopen"]); strings.Count(got, "affinity_header:session_header_too_long") != 1 {
		t.Errorf("log failopen = %s, want the reason once", got)
	}
	if line["retries"] != float64(1) {
		t.Errorf("log retries = %v, want 1: the request must have routed twice", line["retries"])
	}
	if strings.Contains(logs.String(), session) {
		t.Errorf("the header value reached the log")
	}
	if n := telemetrytest.Sum(t, tel.Metrics(), telemetry.MetricFailOpen, `stage="affinity_header"`, `reason="session_header_too_long"`); n != 1 {
		t.Errorf("counted %v times, want once", n)
	}
}
