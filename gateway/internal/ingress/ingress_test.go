package ingress

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/dispatch"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/mockllm"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/testnet"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

func startMock(t *testing.T, mod func(*mockllm.Config)) *mockllm.Server {
	t.Helper()
	cfg := mockllm.Defaults()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.Models = []string{"large-model", "small-model", "mock-model"}
	cfg.TTFT = 0
	cfg.TokensPerSecond = 0
	if mod != nil {
		mod(&cfg)
	}
	srv, err := mockllm.Start(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return srv
}

// fixture is a gateway in front of two large replicas and one small one.
type fixture struct {
	large0, large1, small *mockllm.Server
	h                     *Handler
	gw                    *httptest.Server
	tel                   *telemetry.Telemetry // records every request, as in production
}

// outcomes is what f's telemetry counted.
func (f *fixture) outcomes(t testing.TB) outcomes {
	t.Helper()
	return countOutcomes(t, f.tel.Metrics())
}

// roundRobin is the dispatcher handler tests use unless they build their
// own: round-robin over disc, sending with client, no health tracking.
func roundRobin(cfg *config.Config, disc discovery.Discovery, client *upstream.Client, logger *slog.Logger) *dispatch.Dispatcher {
	return dispatch.New(router.NewRoundRobin(disc, cfg.PoolNames()), client, cfg.Upstream, dispatch.Options{Logger: logger})
}

func newFixture(t *testing.T, yamlExtra string, opts Options) *fixture {
	t.Helper()
	return newFixtureWith(t, yamlExtra, opts, nil)
}

// newFixtureWith is newFixture with largeMod applied to the large replicas.
func newFixtureWith(t *testing.T, yamlExtra string, opts Options, largeMod func(*mockllm.Config)) *fixture {
	t.Helper()
	f := &fixture{large0: startMock(t, largeMod), large1: startMock(t, largeMod), small: startMock(t, nil)}
	cfg := parseConfig(t, fmt.Sprintf(`
pools:
  - name: large
    replicas:
      - {name: large-0, url: %s}
      - {name: large-1, url: %s}
  - name: small
    replicas:
      - {name: small-0, url: %s}
models:
  large-model: large
  small-model: small
default_pool: large
auth: {enabled: false}
%s`, f.large0.URL(), f.large1.URL(), f.small.URL(), yamlExtra))
	disc, err := discovery.NewStatic(cfg.Pools)
	if err != nil {
		t.Fatal(err)
	}
	client := upstream.New(cfg.Upstream)
	t.Cleanup(client.CloseIdleConnections)
	f.h = New(cfg, disc, client, roundRobin(cfg, disc, client, opts.Logger), opts)
	f.tel = telemetry.New(telemetry.Options{Logger: opts.Logger})
	mux := http.NewServeMux()
	f.h.Register(mux)
	f.gw = httptest.NewServer(f.tel.Middleware(mux))
	t.Cleanup(f.gw.Close)
	return f
}

func parseConfig(t testing.TB, y string) *config.Config {
	t.Helper()
	cfg, err := config.Parse([]byte(y), func(string) string { return "" })
	if err != nil {
		t.Fatalf("config.Parse: %v", err)
	}
	return cfg
}

func (f *fixture) post(t *testing.T, path string, body []byte, hdr map[string]string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, f.gw.URL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := f.gw.Client().Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return resp, b
}

func count(s *mockllm.Server) int { return len(s.Mock().Requests()) }

func TestForwardsBodyBytesUntouchedToMappedPool(t *testing.T) {
	f := newFixture(t, "", Options{})
	// Odd spacing, key order, unicode escapes and extra fields must survive.
	bodies := map[string][]byte{
		PathChatCompletions: []byte(`{ "messages":[{"role":"user","content":"café \"q\""}],"model":"small-model","max_tokens":2,"response_format":{"type":"json_object"},"guided_choice":["a","b"] }`),
		PathCompletions:     []byte(`{"model":"small-model","prompt":"hi","max_tokens":2,"logprobs":1,"n":2}`),
	}
	for path, body := range bodies {
		t.Run(path, func(t *testing.T) {
			f.small.Mock().ResetRequests()
			resp, got := f.post(t, path, body, map[string]string{mockllm.HeaderOutputTokens: "2"})
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, body %s", resp.StatusCode, got)
			}
			if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
				t.Fatalf("Content-Type = %q", ct)
			}
			var parsed map[string]any
			if err := json.Unmarshal(got, &parsed); err != nil || parsed["object"] == nil {
				t.Fatalf("response not an OpenAI object: %s", got)
			}
			reqs := f.small.Mock().Requests()
			if len(reqs) != 1 {
				t.Fatalf("small pool saw %d requests, want 1", len(reqs))
			}
			if !bytes.Equal(reqs[0].Body, body) {
				t.Fatalf("upstream body differs:\n got %q\nwant %q", reqs[0].Body, body)
			}
			if reqs[0].Path != path || reqs[0].Header.Get(mockllm.HeaderOutputTokens) != "2" {
				t.Fatalf("path %q / X-Mock header %v not forwarded", reqs[0].Path, reqs[0].Header)
			}
		})
	}
	if count(f.large0)+count(f.large1) != 0 {
		t.Fatal("large pool received small-model traffic")
	}
	if s := f.outcomes(t); s.UnknownModel != 0 || s.UnparseableBody != 0 {
		t.Fatalf("stats = %+v, want no fail-open", s)
	}
}

func TestRoundRobinAcrossPoolReplicas(t *testing.T) {
	f := newFixture(t, "", Options{})
	body := []byte(`{"model":"large-model","prompt":"x","max_tokens":1}`)
	const n = 10
	for range n {
		if resp, b := f.post(t, PathCompletions, body, nil); resp.StatusCode != http.StatusOK {
			t.Fatalf("status %d: %s", resp.StatusCode, b)
		}
	}
	if a, b := count(f.large0), count(f.large1); a != n/2 || b != n/2 {
		t.Fatalf("round robin split = %d/%d, want %d/%d", a, b, n/2, n/2)
	}
	if count(f.small) != 0 {
		t.Fatal("small pool received large-model traffic")
	}
}

func TestFailOpenToDefaultPool(t *testing.T) {
	tests := []struct {
		name            string
		body            string
		wantUnknown     float64
		wantUnparseable float64
	}{
		{name: "unknown model", body: `{"model":"mock-model","prompt":"x","max_tokens":1}`, wantUnknown: 1},
		{name: "missing model", body: `{"prompt":"x","max_tokens":1}`, wantUnknown: 1},
		{name: "model not a string", body: `{"model":42,"prompt":"x"}`, wantUnparseable: 1},
		{name: "invalid json", body: `{"model":"small-model",`, wantUnparseable: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, "", Options{})
			resp, _ := f.post(t, PathCompletions, []byte(tc.body), nil)
			// Whatever the replica answers (400 for broken bodies) is relayed;
			// the gateway itself never rejects.
			if resp.StatusCode >= 500 {
				t.Fatalf("status = %d, want the replica's answer", resp.StatusCode)
			}
			if got := count(f.large0) + count(f.large1); got != 1 || count(f.small) != 0 {
				t.Fatalf("default pool got %d, small got %d; want 1, 0", got, count(f.small))
			}
			var rec mockllm.RecordedRequest
			if count(f.large0) == 1 {
				rec = f.large0.Mock().Requests()[0]
			} else {
				rec = f.large1.Mock().Requests()[0]
			}
			if string(rec.Body) != tc.body {
				t.Fatalf("body changed: %q", rec.Body)
			}
			s := f.outcomes(t)
			if s.UnknownModel != tc.wantUnknown || s.UnparseableBody != tc.wantUnparseable {
				t.Fatalf("stats = %+v", s)
			}
		})
	}
}

func TestRelaysUpstreamStatusAndHeaders(t *testing.T) {
	f := newFixture(t, "", Options{})
	resp, body := f.post(t, PathChatCompletions, []byte(`{"model":"large-model","messages":[{"role":"user","content":"x"}]}`),
		map[string]string{mockllm.HeaderFault: "5xx"})
	if resp.StatusCode != mockllm.DefaultErrorStatus {
		t.Fatalf("status = %d, want the replica's %d", resp.StatusCode, mockllm.DefaultErrorStatus)
	}
	if !strings.Contains(string(body), "error") {
		t.Fatalf("replica error body not relayed: %s", body)
	}
	if f.outcomes(t).UpstreamErrors != 0 {
		t.Fatal("a replica's own error response counted as a gateway upstream error")
	}
}

func TestModelsForwardedToDefaultPool(t *testing.T) {
	f := newFixture(t, "", Options{})
	resp, err := f.gw.Client().Get(f.gw.URL + PathModels)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var list struct {
		Object string `json:"object"`
		Data   []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/models: status %d, err %v", resp.StatusCode, err)
	}
	if list.Object != "list" || len(list.Data) == 0 {
		t.Fatalf("models = %+v", list)
	}
}

func TestRequestBodyLimit(t *testing.T) {
	f := newFixture(t, "", Options{MaxRequestBodyBytes: 64})
	resp, body := f.post(t, PathCompletions, bytes.Repeat([]byte("a"), 65), nil)
	if resp.StatusCode != http.StatusRequestEntityTooLarge || !strings.Contains(string(body), "request_too_large") {
		t.Fatalf("status %d body %s, want 413", resp.StatusCode, body)
	}
	if count(f.large0)+count(f.large1)+count(f.small) != 0 {
		t.Fatal("oversized body reached a replica")
	}
}

// emptyDiscovery has no replicas for any pool.
type emptyDiscovery struct{}

func (emptyDiscovery) Replicas(string) []discovery.Replica { return nil }

func TestGatewayErrors(t *testing.T) {
	dead := testnet.RefusedURL(t)
	hang := startMock(t, nil)

	tests := []struct {
		name     string
		url      string
		extra    string
		disc     discovery.Discovery
		hdr      map[string]string
		want     int
		wantCode string
	}{
		{name: "connect refused", url: dead, want: http.StatusBadGateway, wantCode: "upstream_connect"},
		{name: "response header timeout", url: hang.URL(), hdr: map[string]string{mockllm.HeaderFault: "hang"},
			extra: "upstream:\n  timeouts: {response_header: 100ms}\n", want: http.StatusGatewayTimeout, wantCode: "upstream_timeout"},
		{name: "no replicas", url: dead, disc: emptyDiscovery{}, want: http.StatusServiceUnavailable, wantCode: "no_replicas"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := parseConfig(t, fmt.Sprintf("pools:\n  - name: large\n    replicas:\n      - url: %s\nauth: {enabled: false}\n%s", tc.url, tc.extra))
			disc := tc.disc
			if disc == nil {
				s, err := discovery.NewStatic(cfg.Pools)
				if err != nil {
					t.Fatal(err)
				}
				disc = s
			}
			client := upstream.New(cfg.Upstream)
			defer client.CloseIdleConnections()
			h := New(cfg, disc, client, roundRobin(cfg, disc, client, nil), Options{})
			mux := http.NewServeMux()
			h.Register(mux)
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, PathChatCompletions, strings.NewReader(`{"model":"mock-model"}`))
			for k, v := range tc.hdr {
				req.Header.Set(k, v)
			}
			mux.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d (%s)", rec.Code, tc.want, rec.Body)
			}
			var eb errorBody
			if err := json.Unmarshal(rec.Body.Bytes(), &eb); err != nil || eb.Error.Code != tc.wantCode || eb.Error.Message == "" {
				t.Fatalf("error body = %s, want OpenAI error with code %q", rec.Body, tc.wantCode)
			}
			if rec.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("Content-Type = %q", rec.Header().Get("Content-Type"))
			}
		})
	}
}

func TestClientDisconnectCancelsUpstream(t *testing.T) {
	f := newFixture(t, "", Options{})
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.gw.URL+PathCompletions,
		strings.NewReader(`{"model":"large-model","prompt":"x"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(mockllm.HeaderFault, "hang")
	done := make(chan struct{})
	go func() {
		defer close(done)
		if resp, err := f.gw.Client().Do(req); err == nil {
			_ = resp.Body.Close()
		}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for f.large0.Mock().Stats().Requests+f.large1.Mock().Stats().Requests == 0 {
		if time.Now().After(deadline) {
			t.Fatal("request never reached a replica")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	for f.large0.Mock().Stats().Cancelled+f.large1.Mock().Stats().Cancelled == 0 {
		if time.Now().After(deadline) {
			t.Fatal("upstream request was not cancelled after the client left")
		}
		time.Sleep(5 * time.Millisecond)
	}
	for f.outcomes(t).ClientDisconnects == 0 {
		if time.Now().After(deadline) {
			t.Fatal("client disconnect not counted")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestReadBody(t *testing.T) {
	tests := []struct {
		body       string
		wantModel  string
		wantStream bool
		wantOK     bool
	}{
		{`{"model":"a"}`, "a", false, true},
		{`{"messages":[{"model":"inner"}],"model":"outer"}`, "outer", false, true},
		{`{"prompt":"x"}`, "", false, true},
		{`{"model":null}`, "", false, true},
		{`{"model":1}`, "", false, false},
		{`[1,2]`, "", false, false},
		{``, "", false, false},
		{`{"model":"a","stream":true}`, "a", true, true},
		{`{"stream":true,"model":"a"}`, "a", true, true},
		{`{"model":"a","stream":false}`, "a", false, true},
		{`{"model":"a","stream":"true"}`, "a", false, true},
		{`{"model":"a","stream":null}`, "a", false, true},
		{`{"messages":[{"stream":true}],"model":"a"}`, "a", false, true},
		{`{"model":1,"stream":true}`, "", true, false},
		// Keys match case-sensitively and a repeated key keeps its last value (vLLM's parser).
		{`{"MODEL":"upper","model":"lower"}`, "lower", false, true},
		{`{"model":"lower","MODEL":"upper"}`, "lower", false, true},
		{`{"Model":"a"}`, "", false, true},
		{`{"model":"a","STREAM":true}`, "a", false, true},
		{`{"model":"first","model":"second"}`, "second", false, true},
		{`{"model":"a","stream":true,"stream":false}`, "a", false, true},
		// Only one JSON object is accepted.
		{`{"model":"a"} {"model":"b"}`, "", false, false},
		{`{"model":"a"}x`, "", false, false},
		{`{"model":"a"}` + " \n", "a", false, true},
		{`{"model":"a"`, "", false, false},
		{`"model"`, "", false, false},
		{`null`, "", false, false},
		{`{"model":"a",}`, "", false, false},
		// Escaped keys decode like any JSON string; values keep their nesting.
		{`{"model":"esc"}`, "esc", false, true},
		{`{"x":"}\"{","model":"a","stream":true,"y":[1,{"model":"z"}]}`, "a", true, true},
		{` { "model" : "a" , "stream" : true } `, "a", true, true},
		{`{"a":1.5e3,"b":-2,"c":null,"d":false,"model":"a"}`, "a", false, true},
	}
	for _, tc := range tests {
		got, ok := readBody(new(bodyindex.Index), []byte(tc.body))
		if got.model != tc.wantModel || got.stream != tc.wantStream || ok != tc.wantOK {
			t.Errorf("readBody(%s) = %+v, %v; want model %q stream %v, %v",
				tc.body, got, ok, tc.wantModel, tc.wantStream, tc.wantOK)
		}
	}
}
