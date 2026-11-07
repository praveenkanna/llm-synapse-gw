package ingress

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

// benchEvent is one chat.completion.chunk event, sized like vLLM's.
const benchEvent = `data: {"id":"chatcmpl-0123456789abcdef","object":"chat.completion.chunk","created":1790000000,"model":"large-model","choices":[{"index":0,"delta":{"content":" token"},"logprobs":null,"finish_reason":null}]}` + "\n\n"

// benchEventsPerStream is the number of events each benchmark stream carries.
const benchEventsPerStream = 200

// burstUpstream streams benchEventsPerStream events, flushing each one.
func burstUpstream(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", sseContentType)
	w.WriteHeader(http.StatusOK)
	rc := http.NewResponseController(w)
	for range benchEventsPerStream {
		_, _ = io.WriteString(w, benchEvent)
		_ = rc.Flush()
	}
	_, _ = io.WriteString(w, "data: [DONE]\n\n")
}

// BenchmarkStreamRelay relays whole streams through the telemetry middleware
// and the ingress handler over loopback. Allocations include the test client
// and upstream; compare runs, or read a -memprofile for gateway frames.
func BenchmarkStreamRelay(b *testing.B) {
	up := httptest.NewServer(http.HandlerFunc(burstUpstream))
	defer up.Close()
	cfg := parseConfig(b, fmt.Sprintf("pools:\n  - name: large\n    replicas:\n      - url: %s\nauth: {enabled: false}\n", up.URL))
	disc, err := discovery.NewStatic(cfg.Pools)
	if err != nil {
		b.Fatal(err)
	}
	client := upstream.New(cfg.Upstream)
	defer client.CloseIdleConnections()
	mux := http.NewServeMux()
	New(cfg, disc, client, roundRobin(cfg, disc, client, nil), Options{}).Register(mux)
	gw := httptest.NewServer(telemetry.New(telemetry.Options{}).Middleware(mux))
	defer gw.Close()

	body := `{"model":"large-model","stream":true,"messages":[{"role":"user","content":"hi"}]}`
	hc := gw.Client()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := hc.Post(gw.URL+PathChatCompletions, "application/json", strings.NewReader(body))
			if err != nil {
				b.Error(err)
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	})
	b.ReportMetric(float64(b.N*benchEventsPerStream)/b.Elapsed().Seconds(), "events/s")
}
