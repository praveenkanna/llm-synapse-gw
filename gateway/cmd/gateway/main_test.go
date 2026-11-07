package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/health"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/ingress"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/mockllm"
)

// Tests never bind the default metrics port (:9090), which a local
// Prometheus may hold.
const (
	envMetricsAddr = "GATEWAY_METRICS_LISTEN_ADDR"
	anyPort        = "127.0.0.1:0"
)

// testConfig returns a minimal valid config (one pool, auth off).
func testConfig(t *testing.T) *config.Config {
	t.Helper()
	cfg, err := config.Parse([]byte(`
pools:
  - name: large
    replicas:
      - url: http://127.0.0.1:1
auth:
  enabled: false
`), func(string) string { return "" })
	if err != nil {
		t.Fatalf("config.Parse: %v", err)
	}
	return cfg
}

func TestRunFailsFastOnInvalidConfig(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "gateway.yaml")
	if err := os.WriteFile(bad, []byte("pools:\n  - name: large\n    replicas: []\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		env  map[string]string
		want string
	}{
		{"no config file", map[string]string{}, "at least one pool is required"},
		{"invalid file", map[string]string{config.EnvConfigPath: bad}, "needs at least one replica"},
		{"invalid env", map[string]string{config.EnvConfigPath: bad, "GATEWAY_READ_HEADER_TIMEOUT": "x"}, "GATEWAY_READ_HEADER_TIMEOUT"},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := run(ctx, func(k string) string { return tc.env[k] }, logger)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("run() = %v, want error containing %q", err, tc.want)
			}
		})
	}
}

func TestRunServesOnConfiguredAddr(t *testing.T) {
	// Reserve a free port, then hand it to run via the env override.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	_ = probe.Close()

	dir := t.TempDir()
	path := filepath.Join(dir, "gateway.yaml")
	yaml := "pools:\n  - name: large\n    replicas:\n      - url: http://127.0.0.1:1\nauth:\n  enabled: false\n"
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{config.EnvConfigPath: path, "GATEWAY_LISTEN_ADDR": addr, envMetricsAddr: anyPort}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, func(k string) string { return env[k] }, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()

	client := &http.Client{Timeout: 2 * time.Second}
	var resp *http.Response
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err = client.Get("http://" + addr + health.LivenessPath)
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		cancel()
		t.Fatalf("GET liveness on configured addr %s: %v", addr, err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("liveness status = %d", resp.StatusCode)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after context cancel")
	}
}

func TestRoutes(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
		wantBody   string
	}{
		{name: "liveness ok", method: http.MethodGet, path: health.LivenessPath, wantStatus: http.StatusOK, wantBody: "ok\n"},
		{name: "liveness HEAD ok", method: http.MethodHead, path: health.LivenessPath, wantStatus: http.StatusOK},
		{name: "liveness wrong method", method: http.MethodPost, path: health.LivenessPath, wantStatus: http.StatusMethodNotAllowed},
		{name: "readiness ok", method: http.MethodGet, path: health.ReadinessPath, wantStatus: http.StatusOK, wantBody: "ready\n"},
		{name: "unknown path", method: http.MethodGet, path: "/nope", wantStatus: http.StatusNotFound},
	}
	mux := newMux(nil, health.New(), nil)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, nil))
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
			if tc.wantBody != "" && rec.Body.String() != tc.wantBody {
				t.Fatalf("body = %q, want %q", rec.Body.String(), tc.wantBody)
			}
			if tc.wantStatus == http.StatusOK && rec.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("Cache-Control = %q, want no-store", rec.Header().Get("Cache-Control"))
			}
		})
	}
}

func TestNewServerTimeouts(t *testing.T) {
	cfg := testConfig(t)
	srv := newServer(cfg, http.NewServeMux(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if srv.WriteTimeout != 0 {
		t.Errorf("WriteTimeout = %v, want 0 (SSE streams run for minutes)", srv.WriteTimeout)
	}
	if srv.IdleTimeout != cfg.Server.IdleTimeout || srv.IdleTimeout != config.DefaultIdleTimeout {
		t.Errorf("IdleTimeout = %v, want %v", srv.IdleTimeout, cfg.Server.IdleTimeout)
	}
	if srv.ReadHeaderTimeout != cfg.Server.ReadHeaderTimeout {
		t.Errorf("ReadHeaderTimeout = %v, want %v", srv.ReadHeaderTimeout, cfg.Server.ReadHeaderTimeout)
	}
	// The whole header block stays where it was: lowering it would refuse
	// requests the gateway forwards today (M1-27, docs/results).
	if srv.MaxHeaderBytes != 1<<20 {
		t.Errorf("MaxHeaderBytes = %d, want 1 MiB set by name", srv.MaxHeaderBytes)
	}
}

// TestConfigDefaultsMatchPackages keeps config's copies of the ingress and
// discovery defaults (config cannot import either package) in step.
func TestConfigDefaultsMatchPackages(t *testing.T) {
	if got, want := int64(config.DefaultMaxRequestBodyBytes), int64(ingress.DefaultMaxRequestBodyBytes); got != want {
		t.Errorf("config.DefaultMaxRequestBodyBytes = %d, ingress default = %d", got, want)
	}
	if got, want := config.DefaultReloadInterval, discovery.DefaultWatchInterval; got != want {
		t.Errorf("config.DefaultReloadInterval = %s, discovery default = %s", got, want)
	}
}

func TestIngressOptionsFromConfig(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := testConfig(t)
	if got := ingressOptions(cfg, logger); got.MaxRequestBodyBytes != ingress.DefaultMaxRequestBodyBytes || got.Logger != logger {
		t.Errorf("default options = %+v, want body limit %d and the given logger", got, ingress.DefaultMaxRequestBodyBytes)
	}
	cfg.Server.MaxRequestBodyBytes = 4 << 10
	if got := ingressOptions(cfg, logger).MaxRequestBodyBytes; got != 4<<10 {
		t.Errorf("MaxRequestBodyBytes = %d, want configured 4096", got)
	}
}

// TestRunEnforcesConfiguredBodyLimit proves server.max_request_body_bytes
// reaches the request path: a body over the limit is answered 413 and never
// forwarded.
func TestRunEnforcesConfiguredBodyLimit(t *testing.T) {
	mcfg := mockllm.Defaults()
	mcfg.ListenAddr = anyPort
	mcfg.TTFT = 0
	mcfg.TokensPerSecond = 0
	mock, err := mockllm.Start(mcfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mock.Close() }()

	probe, err := net.Listen("tcp", anyPort)
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	_ = probe.Close()
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	yaml := "server:\n  max_request_body_bytes: 1KiB\npools:\n  - name: large\n    replicas:\n      - url: " + mock.URL() + "\nauth:\n  enabled: false\n"
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{config.EnvConfigPath: path, "GATEWAY_LISTEN_ADDR": addr, envMetricsAddr: anyPort}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, func(k string) string { return env[k] }, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("run returned error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("run did not return after context cancel")
		}
	}()

	body := `{"model":"mock-model","prompt":"` + strings.Repeat("x", 2048) + `","max_tokens":2}`
	client := &http.Client{Timeout: 2 * time.Second}
	var resp *http.Response
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err = client.Post("http://"+addr+"/v1/completions", "application/json", strings.NewReader(body))
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("POST through gateway: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413 for a 2 KiB body over a 1 KiB limit", resp.StatusCode)
	}
	if n := len(mock.Mock().Requests()); n != 0 {
		t.Fatalf("upstream saw %d requests, want none", n)
	}
}

func TestRunForwardsToUpstream(t *testing.T) {
	mcfg := mockllm.Defaults()
	mcfg.ListenAddr = "127.0.0.1:0"
	mcfg.TTFT = 0
	mcfg.TokensPerSecond = 0
	mock, err := mockllm.Start(mcfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mock.Close() }()

	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	_ = probe.Close()
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	yaml := "pools:\n  - name: large\n    replicas:\n      - url: " + mock.URL() + "\nauth:\n  enabled: false\n"
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{config.EnvConfigPath: path, "GATEWAY_LISTEN_ADDR": addr, envMetricsAddr: anyPort}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, func(k string) string { return env[k] }, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("run returned error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("run did not return after context cancel")
		}
	}()

	body := `{"model":"mock-model","prompt":"hi","max_tokens":2}`
	client := &http.Client{Timeout: 2 * time.Second}
	var resp *http.Response
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err = client.Post("http://"+addr+"/v1/completions", "application/json", strings.NewReader(body))
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("POST through gateway: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	reqs := mock.Mock().Requests()
	if len(reqs) != 1 || string(reqs[0].Body) != body {
		t.Fatalf("upstream saw %d requests (%v), want the caller's bytes", len(reqs), reqs)
	}
}

func TestRunServesMetricsAndInstrumentsRequests(t *testing.T) {
	mcfg := mockllm.Defaults()
	mcfg.ListenAddr = anyPort
	mcfg.TTFT = 0
	mcfg.TokensPerSecond = 0
	mock, err := mockllm.Start(mcfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mock.Close() }()

	free := func() string {
		probe, err := net.Listen("tcp", anyPort)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = probe.Close() }()
		return probe.Addr().String()
	}
	addr, metricsAddr := free(), free()
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	yaml := "pools:\n  - name: large\n    replicas:\n      - url: " + mock.URL() + "\nauth:\n  enabled: false\n"
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{config.EnvConfigPath: path, "GATEWAY_LISTEN_ADDR": addr, envMetricsAddr: metricsAddr}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, func(k string) string { return env[k] }, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("run returned error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("run did not return after context cancel")
		}
	}()

	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	client := &http.Client{Timeout: 2 * time.Second}
	var resp *http.Response
	deadline := time.Now().Add(5 * time.Second)
	for {
		req, _ := http.NewRequest(http.MethodPost, "http://"+addr+"/v1/completions",
			strings.NewReader(`{"model":"mock-model","prompt":"hi","max_tokens":2}`))
		req.Header.Set("Traceparent", "00-"+traceID+"-00f067aa0ba902b7-01")
		resp, err = client.Do(req)
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("POST through gateway: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	reqs := mock.Mock().Requests()
	if len(reqs) != 1 || !strings.HasPrefix(reqs[0].Header.Get("Traceparent"), "00-"+traceID+"-") {
		t.Fatalf("upstream did not receive the caller's trace: %v", reqs)
	}

	// The request is recorded after the handler returns, which can be just after
	// the client has read the whole response, so poll until it shows up.
	const want = `gateway_requests_total{pool="large",class="user",status="200"} 1`
	var body []byte
	deadline = time.Now().Add(5 * time.Second)
	for {
		mresp, err := client.Get("http://" + metricsAddr + "/metrics")
		if err != nil {
			t.Fatalf("GET metrics on %s: %v", metricsAddr, err)
		}
		body, _ = io.ReadAll(mresp.Body)
		_ = mresp.Body.Close()
		if strings.Contains(string(body), want) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Metrics are not served on the request port.
	if r, err := client.Get("http://" + addr + "/metrics"); err == nil {
		_ = r.Body.Close()
		if r.StatusCode != http.StatusNotFound {
			t.Errorf("request port /metrics status = %d, want 404", r.StatusCode)
		}
	}
}

func TestRunAuthenticatesCallersAndInjectsUpstreamKey(t *testing.T) {
	const (
		callerToken = "chat-token-0123456789"
		upstreamKey = "sk-upstream-0123456789"
	)
	// A plain httptest upstream records the exact Authorization header; the
	// mockllm capture redacts it, so it can only prove presence.
	type seenReq struct {
		authz string
		body  string
	}
	var (
		mu      sync.Mutex
		seen    []seenReq
		scrapes []string // Authorization of the load view's /metrics scrapes
	)
	upstreamSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			mu.Lock()
			scrapes = append(scrapes, r.Header.Get("Authorization"))
			mu.Unlock()
		}
		if r.URL.Path == "/v1/completions" {
			b, _ := io.ReadAll(r.Body)
			mu.Lock()
			seen = append(seen, seenReq{authz: r.Header.Get("Authorization"), body: string(b)})
			mu.Unlock()
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"cmpl-1","object":"text_completion","choices":[]}`))
	}))
	defer upstreamSrv.Close()

	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	_ = probe.Close()
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	yaml := "pools:\n  - name: large\n    replicas:\n      - url: " + upstreamSrv.URL +
		"\nauth:\n  callers:\n    - name: chat-service\n      token_env: GATEWAY_TOKEN_CHAT\n"
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{
		config.EnvConfigPath:       path,
		"GATEWAY_LISTEN_ADDR":      addr,
		"GATEWAY_TOKEN_CHAT":       callerToken,
		"GATEWAY_UPSTREAM_API_KEY": upstreamKey,
		envMetricsAddr:             anyPort,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, func(k string) string { return env[k] }, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("run returned error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("run did not return after context cancel")
		}
	}()

	client := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	// Liveness needs no token; poll it until the server is up.
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := client.Get("http://" + addr + health.LivenessPath)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("liveness without token = %d, want 200", resp.StatusCode)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("gateway did not start: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	body := `{"model":"mock-model","prompt":"hi","max_tokens":2}`
	send := func(method, path, authz string) int {
		t.Helper()
		req, err := http.NewRequest(method, "http://"+addr+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		if authz != "" {
			req.Header.Set("Authorization", authz)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	post := func(authz string) int { return send(http.MethodPost, "/v1/completions", authz) }

	tests := []struct {
		name       string
		authz      string
		wantStatus int
	}{
		{"missing token", "", http.StatusUnauthorized},
		{"invalid token", "Bearer wrong-token-0123456789", http.StatusUnauthorized},
		{"valid token", "Bearer " + callerToken, http.StatusOK},
	}
	for _, tc := range tests {
		if got := post(tc.authz); got != tc.wantStatus {
			t.Errorf("%s: status = %d, want %d", tc.name, got, tc.wantStatus)
		}
	}

	// ADR 0012: every /v1/ path is behind auth before routing, so an
	// unauthenticated caller gets 401 for unknown paths and wrong methods
	// (route names are not revealed); a valid token then sees 404 / 405.
	routing := []struct {
		name       string
		method     string
		path       string
		authz      string
		wantStatus int
	}{
		{"unknown v1 path, no token", http.MethodPost, "/v1/embeddings", "", http.StatusUnauthorized},
		{"unknown v1 path, invalid token", http.MethodPost, "/v1/embeddings", "Bearer wrong-token-0123456789", http.StatusUnauthorized},
		{"unknown v1 path, valid token", http.MethodPost, "/v1/embeddings", "Bearer " + callerToken, http.StatusNotFound},
		{"wrong method, no token", http.MethodGet, "/v1/completions", "", http.StatusUnauthorized},
		{"wrong method, valid token", http.MethodGet, "/v1/completions", "Bearer " + callerToken, http.StatusMethodNotAllowed},
		{"path outside v1, no token", http.MethodPost, "/nope", "", http.StatusNotFound},
	}
	for _, tc := range routing {
		if got := send(tc.method, tc.path, tc.authz); got != tc.wantStatus {
			t.Errorf("%s: %s %s status = %d, want %d", tc.name, tc.method, tc.path, got, tc.wantStatus)
		}
	}

	mu.Lock()
	reqs := append([]seenReq(nil), seen...)
	mu.Unlock()
	if len(reqs) != 1 {
		t.Fatalf("upstream saw %d requests, want 1 (rejected requests must not reach a replica)", len(reqs))
	}
	if got, want := reqs[0].authz, "Bearer "+upstreamKey; got != want {
		t.Fatalf("upstream Authorization = %q, want %q (the gateway's key, never the caller token)", got, want)
	}
	if reqs[0].body != body {
		t.Fatalf("upstream body = %q, want the caller's bytes", reqs[0].body)
	}
	// The load view scrapes with the same key: run passes upstream.api_key
	// to it.
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		mu.Lock()
		got := append([]string(nil), scrapes...)
		mu.Unlock()
		if len(got) > 0 {
			if got[0] != "Bearer "+upstreamKey {
				t.Fatalf("load view scrape Authorization = %q, want the upstream key", got[0])
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the load view never scraped the replica")
		}
	}
}
