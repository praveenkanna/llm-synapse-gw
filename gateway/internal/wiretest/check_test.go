package wiretest

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/mockllm"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

func TestSenderPrepareRenamesOnlyTheServedModel(t *testing.T) {
	g := Golden{Body: []byte(`{"messages":[{"content":"say mock-model"}], "model" : "mock-model"}`)}
	tests := []struct {
		name, model, want string
	}{
		{"no model set", "", string(g.Body)},
		{"same model", GoldenModel, string(g.Body)},
		{"renamed", "large-model", `{"messages":[{"content":"say mock-model"}], "model" : "large-model"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Sender{Model: tc.model}.Prepare(g)
			if string(got.Body) != tc.want {
				t.Errorf("body = %s, want %s", got.Body, tc.want)
			}
		})
	}
	if bytes.Contains(g.Body, []byte("large-model")) {
		t.Error("Prepare changed the caller's golden")
	}
	unknown := Golden{Body: []byte(`{"model":"not-a-served-model"}`)}
	if got := (Sender{Model: "large-model"}).Prepare(unknown); !bytes.Equal(got.Body, unknown.Body) {
		t.Errorf("unknown-model body changed: %s", got.Body)
	}
	if got := (Sender{Model: "large-model"}).Prepare(Golden{}); got.Body != nil {
		t.Errorf("bodyless golden gained a body: %q", got.Body)
	}
}

func TestHTTPRecorderReadsAndResetsCapture(t *testing.T) {
	cfg := mockllm.Defaults()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.Models = []string{GoldenModel}
	cfg.TTFT, cfg.TokensPerSecond = 0, 0
	mock, err := mockllm.Start(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mock.Close() })
	rec := HTTPRecorder{Client: &http.Client{}, BaseURL: mock.URL() + "/"}

	body := "{\"model\":\"mock-model\",\"max_tokens\":2,\"messages\":[{\"role\":\"user\",\"content\":\"x\\u00e9\"}]}\r\n"
	resp, err := http.Post(mock.URL()+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	recs, err := rec.Requests()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || string(recs[0].Body) != body || recs[0].Path != "/v1/chat/completions" {
		t.Fatalf("captured %+v, want the one request byte for byte", recs)
	}
	if err := rec.ResetRequests(); err != nil {
		t.Fatal(err)
	}
	if recs, err = rec.Requests(); err != nil || len(recs) != 0 {
		t.Fatalf("after reset: %d requests, err %v", len(recs), err)
	}
}

func TestHTTPRecorderReportsDisabledControl(t *testing.T) {
	cfg := mockllm.Defaults()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.ControlEnabled = false
	mock, err := mockllm.Start(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mock.Close() })
	rec := HTTPRecorder{Client: &http.Client{}, BaseURL: mock.URL()}
	if _, err := rec.Requests(); err == nil {
		t.Error("Requests succeeded against a mockllm without control endpoints")
	}
	if err := rec.ResetRequests(); err == nil {
		t.Error("ResetRequests succeeded against a mockllm without control endpoints")
	}
}

// fakeTB records failures; Fatal ends the calling goroutine like testing.T.
type fakeTB struct {
	testing.TB
	mu     sync.Mutex
	errors []string
}

func (f *fakeTB) Helper() {}
func (f *fakeTB) Errorf(format string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errors = append(f.errors, fmt.Sprintf(format, args...))
}
func (f *fakeTB) Fatalf(format string, args ...any) {
	f.Errorf(format, args...)
	runtime.Goexit()
}
func (f *fakeTB) Fatal(args ...any) {
	f.Errorf("%s", fmt.Sprint(args...))
	runtime.Goexit()
}

// run calls check with a fakeTB and returns the failures it reported.
func run(check func(testing.TB)) []string {
	f := &fakeTB{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		check(f)
	}()
	<-done
	return f.errors
}

// streamExchange builds a matching direct/gateway pair for a streamed golden.
func streamExchange(gwHeader http.Header, upHeader http.Header) (Golden, Exchange, Exchange) {
	body := []byte(`{"model":"mock-model","stream":true}`)
	sse := []byte("data: {\"object\":\"chat.completion.chunk\"}\n\ndata: [DONE]\n\n")
	g := Golden{Case: Case{Name: "s", Method: http.MethodPost, Path: "/v1/chat/completions", Stream: true}, Body: body}
	rec := func(h http.Header) *mockllm.RecordedRequest {
		return &mockllm.RecordedRequest{Method: g.Method, Path: g.Path, Header: h, Body: body, BodyBytes: len(body)}
	}
	respHeader := http.Header{"Content-Type": {"text/event-stream; charset=utf-8"}}
	direct := Exchange{
		Resp:     &http.Response{StatusCode: http.StatusOK, Header: respHeader, ContentLength: -1},
		Body:     sse,
		Upstream: rec(http.Header{"Traceparent": {"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}}),
	}
	viaGW := Exchange{
		Resp:     &http.Response{StatusCode: http.StatusOK, Header: gwHeader, ContentLength: -1},
		Body:     sse,
		Upstream: rec(upHeader),
	}
	return g, direct, viaGW
}

func TestCompareExpectations(t *testing.T) {
	const gwTP = "00-4bf92f3577b34da6a3ce929d0e0e4736-1111111111111111-01"
	nginx := Expect{
		ProxyRequestHeaders:  []string{"X-Real-Ip"},
		ProxyResponseHeaders: []string{"Server"},
		ProxyConsumesXAccel:  true,
	}
	tests := []struct {
		name     string
		expect   Expect
		gwResp   http.Header
		upstream http.Header
		wantErr  string // substring of the first failure; "" for none
	}{
		{
			name:     "in-process path passes",
			expect:   Expect{UpstreamAuth: 1},
			gwResp:   http.Header{"Content-Type": {"text/event-stream; charset=utf-8"}, "X-Accel-Buffering": {"no"}},
			upstream: http.Header{"Traceparent": {gwTP}, "Accept-Encoding": {"identity"}, "Authorization": {"x"}},
		},
		{
			name:     "nginx path passes",
			expect:   nginx,
			gwResp:   http.Header{"Content-Type": {"text/event-stream; charset=utf-8"}, "Server": {"nginx"}},
			upstream: http.Header{"Traceparent": {gwTP}, "Accept-Encoding": {"identity"}, "X-Real-Ip": {"10.0.0.1"}},
		},
		{
			name:     "caller credential reaching the replica fails",
			expect:   nginx,
			gwResp:   http.Header{"Content-Type": {"text/event-stream; charset=utf-8"}, "Server": {"nginx"}},
			upstream: http.Header{"Traceparent": {gwTP}, "Accept-Encoding": {"identity"}, "X-Real-Ip": {"1"}, "Authorization": {"x"}},
			wantErr:  "Authorization values = 1, want 0",
		},
		{
			name:     "missing proxy request header fails",
			expect:   nginx,
			gwResp:   http.Header{"Content-Type": {"text/event-stream; charset=utf-8"}, "Server": {"nginx"}},
			upstream: http.Header{"Traceparent": {gwTP}, "Accept-Encoding": {"identity"}},
			wantErr:  "X-Real-Ip the proxy adds is missing",
		},
		{
			name:     "X-Accel-Buffering leaking past the proxy fails",
			expect:   nginx,
			gwResp:   http.Header{"Content-Type": {"text/event-stream; charset=utf-8"}, "Server": {"nginx"}, "X-Accel-Buffering": {"no"}},
			upstream: http.Header{"Traceparent": {gwTP}, "Accept-Encoding": {"identity"}, "X-Real-Ip": {"1"}},
			wantErr:  "X-Accel-Buffering",
		},
		{
			name:     "unexpected extra response header fails",
			expect:   Expect{},
			gwResp:   http.Header{"Content-Type": {"text/event-stream; charset=utf-8"}, "X-Accel-Buffering": {"no"}, "Server": {"nginx"}},
			upstream: http.Header{"Traceparent": {gwTP}, "Accept-Encoding": {"identity"}},
			wantErr:  "response header Server",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g, direct, viaGW := streamExchange(tc.gwResp, tc.upstream)
			errs := run(func(tb testing.TB) { Compare(tb, g, direct, viaGW, tc.expect) })
			switch {
			case tc.wantErr == "" && len(errs) > 0:
				t.Errorf("unexpected failures: %q", errs)
			case tc.wantErr != "" && (len(errs) == 0 || !strings.Contains(strings.Join(errs, "\n"), tc.wantErr)):
				t.Errorf("failures %q, want one containing %q", errs, tc.wantErr)
			}
		})
	}
}

// TestSendSkipsTheGatewaysOwnProbes: a probe the gateway sends on its own
// (priority or stuck-engine) can reach the replica while a golden is in
// flight; the check takes the golden's request, not the probe.
func TestSendSkipsTheGatewaysOwnProbes(t *testing.T) {
	cfg := mockllm.Defaults()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.Models = []string{GoldenModel}
	cfg.TTFT, cfg.TokensPerSecond = 0, 0
	mock, err := mockllm.Start(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mock.Close() })
	// A stand-in gateway: it probes the replica, then forwards the call.
	gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probe, _ := http.NewRequest(http.MethodPost, mock.URL()+"/v1/completions", strings.NewReader(`{"model":"mock-model","prompt":"Say OK.","max_tokens":1}`))
		probe.Header.Set("Content-Type", "application/json")
		probe.Header.Set("User-Agent", upstream.ProbeUserAgent)
		if resp, err := http.DefaultClient.Do(probe); err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
		fwd, _ := http.NewRequest(r.Method, mock.URL()+r.URL.Path, r.Body)
		fwd.Header = r.Header.Clone()
		resp, err := http.DefaultClient.Do(fwd)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer func() { _ = resp.Body.Close() }()
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))
	t.Cleanup(gw.Close)

	s := Sender{Client: &http.Client{}, Recorder: MockRecorder{Mock: mock.Mock()}}
	g := Golden{Case: Case{Method: http.MethodPost, Path: "/v1/chat/completions"},
		Body: []byte(`{"model":"mock-model","max_tokens":2,"messages":[{"role":"user","content":"x"}]}`)}
	ex := s.Send(t, gw.URL, g)
	if ex.Upstream == nil || ex.Upstream.Path != "/v1/chat/completions" || !bytes.Equal(ex.Upstream.Body, g.Body) {
		t.Fatalf("upstream = %+v, want the golden's request", ex.Upstream)
	}
}
