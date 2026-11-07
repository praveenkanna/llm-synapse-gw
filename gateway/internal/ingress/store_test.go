package ingress

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/coalesce"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/compiler"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

// TestOnlyAWholeResponseIsStored: whether a leader's response is stored is
// decided from what its relay delivered, so a stream cut off mid-way, by
// the replica (relayed with the gateway's error event) or by its caller
// leaving, is never stored, with or without a telemetry record on the
// request; a whole one is, even one that ended on length when the gateway
// set no cap (a runaway is a cut by the gateway's own cap).
func TestOnlyAWholeResponseIsStored(t *testing.T) {
	const event = `data: {"id":"c","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"hi"}}]}` + "\n\n"
	tests := []struct {
		name       string
		replica    http.HandlerFunc
		leave      bool // the caller leaves after the first event
		wantStored int
	}{
		{name: "whole", wantStored: 1, replica: func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, event+"data: [DONE]\n\n")
		}},
		{name: "ended on length, no cap of the gateway's", wantStored: 1, replica: func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, event+`data: {"id":"c","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"length"}]}`+"\n\ndata: [DONE]\n\n")
		}},
		{name: "cut off mid-stream", replica: func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, event)
			_ = http.NewResponseController(w).Flush()
			panic(http.ErrAbortHandler) // the replica drops the connection
		}},
		{name: "its caller left mid-stream", leave: true, replica: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, event)
			_ = http.NewResponseController(w).Flush()
			<-r.Context().Done() // the rest never comes: the gateway cancels the call
		}},
	}
	for _, tc := range tests {
		for _, withTelemetry := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/telemetry=%t", tc.name, withTelemetry), func(t *testing.T) {
				up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if !strings.HasPrefix(r.URL.Path, "/v1/chat") {
						http.NotFound(w, r)
						return
					}
					tc.replica(w, r)
				}))
				t.Cleanup(up.Close)
				cache := newMemCache()
				gw, handled := cachingGateway(t, up.URL, cache, withTelemetry)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				req, _ := http.NewRequestWithContext(ctx, http.MethodPost, gw.URL+PathChatCompletions, strings.NewReader(faqBody("where is my order?", true, "")))
				req.Header.Set("Content-Type", "application/json")
				for k, v := range faqHeaders("kb", 8) {
					req.Header.Set(k, v)
				}
				resp, err := gw.Client().Do(req)
				if err != nil {
					t.Fatal(err)
				}
				if tc.leave {
					_, _ = io.ReadFull(resp.Body, make([]byte, len(event)))
					cancel()
				} else {
					_, _ = io.ReadAll(resp.Body)
				}
				_ = resp.Body.Close()
				// The body can end before the handler's deferred store runs.
				eventually(t, "the handler to return", func() bool { return handled() == 1 })
				if n, _ := cache.stored(); n != tc.wantStored {
					t.Fatalf("%d entries stored, want %d", n, tc.wantStored)
				}
			})
		}
	}
}

// TestOnlyAGatewayCutIsRunaway: a response is a runaway, never stored,
// only when a cap the gateway set cut it (finish_reason length), streamed
// or not. One that ended on its own under the gateway's cap is stored, and
// so is one the caller's own max_tokens cut: that is the answer it asked
// for.
func TestOnlyAGatewayCutIsRunaway(t *testing.T) {
	tests := []struct {
		name, extra, finish string
		stream              bool
		tokens              int
		stored              bool
	}{
		{name: "cut by the gateway's cap, streamed", stream: true, tokens: 10, finish: "length"},
		{name: "ended under the gateway's cap", tokens: 2, finish: "stop", stored: true},
		{name: "cut by the caller's own cap", extra: `"max_tokens":3,`, tokens: 10, finish: "length", stored: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f, cache := newCacheFixture(t, "", 3) // the gateway sets max_tokens 3
			a := f.ask(context.Background(), faqBody("where is my order?", tc.stream, tc.extra), faqHeaders("kb", tc.tokens))
			if a.status != http.StatusOK || !bytes.Contains(a.body, []byte(`"finish_reason":"`+tc.finish+`"`)) {
				t.Fatalf("status %d err %v body %q, want a response ending on %s", a.status, a.err, a.body, tc.finish)
			}
			if !tc.stored {
				f.assertNothingStored(t, cache)
				return
			}
			eventually(t, "the store", func() bool { n, _ := cache.stored(); return n == 1 })
		})
	}
}

// cachingGateway is a gateway with coalescing and cache in front of the
// replica at url, and the "kb" template; withTelemetry puts it behind the
// telemetry middleware, as production does. handled counts the requests
// whose handler has returned.
func cachingGateway(t *testing.T, url string, cache *memCache, withTelemetry bool) (gw *httptest.Server, handled func() int) {
	t.Helper()
	cfg := parseConfig(t, fmt.Sprintf("pools:\n  - name: large\n    replicas:\n      - {name: large-0, url: %s}\nmodels:\n  large-model: large\ndefault_pool: large\nauth: {enabled: false}\n", url))
	disc, err := discovery.NewStatic(cfg.Pools)
	if err != nil {
		t.Fatal(err)
	}
	snap, errs := templates.NewSnapshot([]templates.Record{{ID: "kb", Version: 1, Status: templates.StatusActive, Spec: json.RawMessage(kbSpec)}}, time.Now())
	if len(errs) > 0 {
		t.Fatal(errs)
	}
	client := upstream.New(cfg.Upstream)
	t.Cleanup(client.CloseIdleConnections)
	h := New(cfg, disc, client, roundRobin(cfg, disc, client, nil), Options{
		Coalesce: coalesce.NewGroup(coalesce.Config{Coalescing: config.Coalescing{MaxFlights: 4, MaxSubscribers: 4, MaxReplayBytes: 1 << 20}}),
		Cache:    cache,
		Compiler: compiler.New(staticSnap{snap}, compiler.Options{MaxTokens: config.MaxTokensPolicy{Enabled: false}}),
	})
	mux := http.NewServeMux()
	h.Register(mux)
	var handler http.Handler = mux
	if withTelemetry {
		handler = telemetry.New(telemetry.Options{}).Middleware(mux)
	}
	var n atomic.Int32
	gw = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer n.Add(1)
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(gw.Close)
	return gw, func() int { return int(n.Load()) }
}
