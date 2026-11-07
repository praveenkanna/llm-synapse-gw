package upstream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/mockllm"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/testnet"
)

func testUpstream(mod func(*config.Upstream)) config.Upstream {
	u := config.Defaults().Upstream
	u.Timeouts = config.UpstreamTimeouts{
		Connect:              time.Second,
		ResponseHeader:       config.DefaultResponseHeaderTimeout, // 0: bounded by Total
		StreamResponseHeader: 2 * time.Second,
		IdleBetweenChunks:    2 * time.Second,
		Total:                5 * time.Second,
	}
	if mod != nil {
		mod(&u)
	}
	return u
}

func replicaFor(t *testing.T, raw string) discovery.Replica {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return discovery.Replica{Pool: "large", Name: u.Host, BaseURL: u}
}

func startMock(t *testing.T) *mockllm.Server {
	t.Helper()
	cfg := mockllm.Defaults()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.TTFT = 0
	cfg.TokensPerSecond = 0
	srv, err := mockllm.Start(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return srv
}

func TestForwardHeaders(t *testing.T) {
	in := http.Header{
		"Authorization":       {"Bearer caller-token"},
		"Proxy-Authorization": {"Basic x"},
		"Connection":          {"keep-alive, X-Drop-Me"},
		"X-Drop-Me":           {"1"},
		"Keep-Alive":          {"timeout=5"},
		"Te":                  {"trailers"},
		"Transfer-Encoding":   {"chunked"},
		"Upgrade":             {"h2c"},
		"Content-Length":      {"12"},
		"Content-Type":        {"application/json"},
		"X-Mock-Fault":        {"hang"},
		"X-Mock-Ttft-Ms":      {"5"},
		"Traceparent":         {"00-abc-def-01"},
		"Accept":              {"text/event-stream", "application/json"},
	}
	orig := in.Clone()
	out := forwardHeaders(in)
	for _, k := range []string{"Authorization", "Proxy-Authorization", "Connection", "X-Drop-Me", "Keep-Alive", "Te", "Transfer-Encoding", "Upgrade", "Content-Length"} {
		if _, ok := out[k]; ok {
			t.Errorf("%s forwarded, want dropped", k)
		}
	}
	for _, k := range []string{"Content-Type", "X-Mock-Fault", "X-Mock-Ttft-Ms", "Traceparent", "Accept"} {
		if strings.Join(out[k], "|") != strings.Join(orig[k], "|") {
			t.Errorf("%s = %v, want %v", k, out[k], orig[k])
		}
	}
	if strings.Join(in["Authorization"], "") != "Bearer caller-token" || len(in) != len(orig) {
		t.Error("forwardHeaders modified its input")
	}
	if got := forwardHeaders(nil); got == nil || len(got) != 0 {
		t.Errorf("forwardHeaders(nil) = %v, want empty map", got)
	}
}

func TestCopyResponseHeaders(t *testing.T) {
	src := http.Header{
		"Content-Type":      {"application/json"},
		"Connection":        {"close, X-Hop"},
		"X-Hop":             {"1"},
		"Transfer-Encoding": {"chunked"},
		"X-Request-Id":      {"abc"},
	}
	dst := http.Header{}
	CopyResponseHeaders(dst, src)
	if dst.Get("Content-Type") != "application/json" || dst.Get("X-Request-Id") != "abc" {
		t.Fatalf("end-to-end headers lost: %v", dst)
	}
	for _, k := range []string{"Connection", "X-Hop", "Transfer-Encoding"} {
		if dst.Get(k) != "" {
			t.Errorf("%s copied, want dropped", k)
		}
	}
	src.Set("Content-Type", "changed")
	if dst.Get("Content-Type") != "application/json" {
		t.Error("dst shares value slices with src")
	}
}

func TestDoForwardsBytesHeadersAndAPIKey(t *testing.T) {
	mock := startMock(t)
	tests := []struct {
		name     string
		apiKey   config.Secret
		wantAuth string
	}{
		// mockllm redacts credential values in its capture, so this test can
		// only see presence; TestDoInjectsExactAPIKey checks the real value.
		{name: "gateway key replaces caller token", apiKey: "sk-upstream-key", wantAuth: mockllm.RedactedValue},
		{name: "no key: caller token never forwarded", apiKey: "", wantAuth: ""},
	}
	body := []byte(`{"model":"mock-model",  "messages":[{"role":"user","content":"héllo é"}],"max_tokens":3}`)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock.Mock().ResetRequests()
			c := New(testUpstream(func(u *config.Upstream) { u.APIKey = tc.apiKey }))
			defer c.CloseIdleConnections()
			resp, err := c.Do(context.Background(), replicaFor(t, mock.URL()), Request{
				Method:   http.MethodPost,
				Path:     "/v1/chat/completions",
				RawQuery: "a=1",
				Header: http.Header{
					"Content-Type":         {"application/json"},
					"Authorization":        {"Bearer caller-token"},
					"X-Mock-Output-Tokens": {"2"},
				},
				Body: body,
			})
			if err != nil {
				t.Fatalf("Do: %v", err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d", resp.StatusCode)
			}
			reqs := mock.Mock().Requests()
			if len(reqs) != 1 {
				t.Fatalf("mock saw %d requests, want 1", len(reqs))
			}
			got := reqs[0]
			if !bytes.Equal(got.Body, body) {
				t.Fatalf("body changed:\n got %q\nwant %q", got.Body, body)
			}
			if got.Path != "/v1/chat/completions" || got.Query != "a=1" {
				t.Fatalf("path/query = %s?%s", got.Path, got.Query)
			}
			if a := got.Header.Get("Authorization"); a != tc.wantAuth {
				t.Fatalf("Authorization upstream = %q, want %q", a, tc.wantAuth)
			}
			if got.Header.Get(mockllm.HeaderOutputTokens) != "2" {
				t.Fatalf("X-Mock header not forwarded: %v", got.Header)
			}
		})
	}
}

func TestDoStreamAsksForIdentityEncoding(t *testing.T) {
	got := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- r.Header.Get("Accept-Encoding")
	}))
	t.Cleanup(srv.Close)
	c := New(testUpstream(nil))
	t.Cleanup(c.CloseIdleConnections)
	tests := []struct {
		name   string
		stream bool
		caller string
		want   string
	}{
		{name: "stream overrides caller encoding", stream: true, caller: "gzip, br", want: "identity"},
		{name: "stream without caller encoding", stream: true, caller: "", want: "identity"},
		{name: "non-stream keeps caller encoding", stream: false, caller: "gzip", want: "gzip"},
		{name: "non-stream without caller encoding", stream: false, caller: "", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			hdr := http.Header{"Content-Type": {"application/json"}}
			if tc.caller != "" {
				hdr.Set("Accept-Encoding", tc.caller)
			}
			before := hdr.Clone()
			resp, err := c.Do(context.Background(), replicaFor(t, srv.URL), Request{
				Method: http.MethodPost, Path: "/v1/chat/completions", Header: hdr, Body: []byte("{}"), Stream: tc.stream,
			})
			if err != nil {
				t.Fatalf("Do: %v", err)
			}
			_ = resp.Body.Close()
			if g := <-got; g != tc.want {
				t.Errorf("upstream Accept-Encoding = %q, want %q", g, tc.want)
			}
			if !reflect.DeepEqual(hdr, before) {
				t.Errorf("caller headers modified: %v, want %v", hdr, before)
			}
		})
	}
}

func TestDoInjectsExactAPIKey(t *testing.T) {
	tests := []struct {
		name     string
		apiKey   config.Secret
		wantAuth string
	}{
		{name: "gateway key replaces caller token", apiKey: "sk-upstream-key", wantAuth: "Bearer sk-upstream-key"},
		{name: "no key: caller token never forwarded", apiKey: "", wantAuth: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			authSeen := make(chan string, 1)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				authSeen <- r.Header.Get("Authorization")
				_, _ = w.Write([]byte("ok"))
			}))
			defer srv.Close()
			c := New(testUpstream(func(u *config.Upstream) { u.APIKey = tc.apiKey }))
			defer c.CloseIdleConnections()
			resp, err := c.Do(context.Background(), replicaFor(t, srv.URL), Request{
				Method: http.MethodPost,
				Path:   "/v1/chat/completions",
				Header: http.Header{"Authorization": {"Bearer caller-token"}},
				Body:   []byte(`{}`),
			})
			if err != nil {
				t.Fatalf("Do: %v", err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if got := <-authSeen; got != tc.wantAuth {
				t.Fatalf("Authorization upstream = %q, want %q", got, tc.wantAuth)
			}
		})
	}
}

func TestDoReusesConnections(t *testing.T) {
	var conns int
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns++
		}
	}
	srv.Start()
	defer srv.Close()
	c := New(testUpstream(nil))
	defer c.CloseIdleConnections()
	for range 5 {
		resp, err := c.Do(context.Background(), replicaFor(t, srv.URL), Request{Method: http.MethodGet, Path: "/v1/models"})
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	srv.Close() // waits for handlers; ConnState writes happen-before here
	if conns != 1 {
		t.Fatalf("opened %d connections for 5 sequential requests, want 1 (keep-alive)", conns)
	}
}

func TestDoFailureKinds(t *testing.T) {
	// A port with nothing listening: connect refused.
	deadURL := testnet.RefusedURL(t)

	block := make(chan struct{})
	defer close(block)
	slowHeaders := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		// Drain the body so the server notices the client leaving.
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-block:
		case <-r.Context().Done():
		}
	}))
	defer slowHeaders.Close()

	tests := []struct {
		name string
		url  string
		ctx  func() (context.Context, context.CancelFunc)
		mod  func(*config.Upstream)
		want Kind
	}{
		{name: "connect refused", url: deadURL, want: KindConnect},
		{name: "response header timeout", url: slowHeaders.URL, want: KindTimeout,
			mod: func(u *config.Upstream) { u.Timeouts.ResponseHeader = 50 * time.Millisecond }},
		{name: "total timeout before headers", url: slowHeaders.URL, want: KindTimeout,
			mod: func(u *config.Upstream) { u.Timeouts.Total = 50 * time.Millisecond }},
		{name: "caller cancels", url: slowHeaders.URL, want: KindCanceled,
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 50*time.Millisecond)
			}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.Background(), context.CancelFunc(func() {})
			if tc.ctx != nil {
				ctx, cancel = tc.ctx()
			}
			defer cancel()
			c := New(testUpstream(tc.mod))
			defer c.CloseIdleConnections()
			start := time.Now()
			resp, err := c.Do(ctx, replicaFor(t, tc.url), Request{Method: http.MethodPost, Path: "/v1/completions", Body: []byte(`{}`)})
			if err == nil {
				_ = resp.Body.Close()
				t.Fatal("Do succeeded, want error")
			}
			if k := KindOf(err); k != tc.want {
				t.Fatalf("kind = %s (%v), want %s", k, err, tc.want)
			}
			if got, want := Retryable(err), tc.want == KindConnect || tc.want == KindDNS; got != want {
				t.Fatalf("Retryable(%v) = %v, want %v: only connect failures may be retried", err, got, want)
			}
			var ue *Error
			if !errors.As(err, &ue) || ue.Replica == "" {
				t.Fatalf("error %v is not an *Error with a replica", err)
			}
			if d := time.Since(start); d > 2*time.Second {
				t.Fatalf("failure took %s", d)
			}
		})
	}
}

// chunkServer sends headers at once, then n chunks every gap, then stalls
// until the client leaves (if stall).
func chunkServer(t *testing.T, n int, gap time.Duration, stall bool) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		fl.Flush() // headers at once, like vLLM
		for i := range n {
			if i > 0 {
				select {
				case <-time.After(gap):
				case <-r.Context().Done():
					return
				}
			}
			_, _ = w.Write([]byte("data: x\n\n"))
			fl.Flush()
		}
		if stall {
			<-r.Context().Done()
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestBodyTimeouts(t *testing.T) {
	tests := []struct {
		name    string
		chunks  int
		gap     time.Duration
		stall   bool
		mod     func(*config.Upstream)
		wantErr error
	}{
		{name: "slow but steady generation outlives idle timeout", chunks: 6, gap: 30 * time.Millisecond,
			mod: func(u *config.Upstream) { u.Timeouts.IdleBetweenChunks = 100 * time.Millisecond }},
		{name: "hung replica hits idle timeout", chunks: 2, gap: 10 * time.Millisecond, stall: true,
			mod: func(u *config.Upstream) { u.Timeouts.IdleBetweenChunks = 100 * time.Millisecond }, wantErr: ErrIdleTimeout},
		{name: "headers then a long wait for the first chunk is bounded by total only", chunks: 0, stall: true,
			mod: func(u *config.Upstream) {
				u.Timeouts.IdleBetweenChunks = 50 * time.Millisecond // must not apply before the first byte
				u.Timeouts.Total = 300 * time.Millisecond
			}, wantErr: ErrTotalTimeout},
		{name: "explicit first chunk limit", chunks: 0, stall: true,
			mod: func(u *config.Upstream) { u.Timeouts.FirstChunk = 50 * time.Millisecond }, wantErr: ErrFirstChunkTimeout},
		{name: "hung stream caught by idle timeout with no header limit", chunks: 1, stall: true,
			mod: func(u *config.Upstream) {
				u.Timeouts.ResponseHeader = 0
				u.Timeouts.IdleBetweenChunks = 100 * time.Millisecond
			}, wantErr: ErrIdleTimeout},
		{name: "total timeout cuts a steady stream", chunks: 1000, gap: 10 * time.Millisecond,
			mod: func(u *config.Upstream) {
				u.Timeouts.IdleBetweenChunks = 100 * time.Millisecond
				u.Timeouts.Total = 200 * time.Millisecond
			}, wantErr: ErrTotalTimeout},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := chunkServer(t, tc.chunks, tc.gap, tc.stall)
			c := New(testUpstream(tc.mod))
			defer c.CloseIdleConnections()
			resp, err := c.Do(context.Background(), replicaFor(t, srv.URL), Request{Method: http.MethodPost, Path: "/v1/chat/completions"})
			if err != nil {
				t.Fatalf("Do: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()
			n, err := io.Copy(io.Discard, resp.Body)
			if tc.wantErr == nil {
				if err != nil || n != int64(tc.chunks*len("data: x\n\n")) {
					t.Fatalf("read %d bytes, err %v; want all chunks", n, err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) || KindOf(err) != KindTimeout {
				t.Fatalf("read err = %v (kind %s), want %v", err, KindOf(err), tc.wantErr)
			}
		})
	}
}

// TestDoResponseHeaderWait: vLLM sends a non-streaming response's headers
// only after the whole generation (and queues requests under load), so by
// default the gateway waits for those as long as the total timeout allows,
// like a direct call. A streaming response's headers come at once, so a
// streaming request has its own, shorter, header limit.
func TestDoResponseHeaderWait(t *testing.T) {
	const generation = 300 * time.Millisecond
	// slowHeaders answers after the whole "generation" (non-streaming vLLM).
	slowHeaders := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-time.After(generation):
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[]}`))
	}))
	defer slowHeaders.Close()
	// quickHeaders sends stream headers at once, then the first event after
	// the "TTFT" (streaming vLLM).
	quickHeaders := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		select {
		case <-time.After(generation):
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write([]byte(`{"choices":[]}`))
	}))
	defer quickHeaders.Close()

	defaults := config.Defaults().Upstream.Timeouts
	tests := []struct {
		name     string
		url      string
		stream   bool
		mod      func(*config.Upstream)
		wantKind Kind // "" for success
	}{
		{name: "non-stream default (no header limit) waits for a slow generation", url: slowHeaders.URL,
			mod: func(u *config.Upstream) {
				u.Timeouts.ResponseHeader = defaults.ResponseHeader
				u.Timeouts.IdleBetweenChunks = 100 * time.Millisecond // does not apply before headers
			}},
		{name: "non-stream explicit header limit shorter than the generation", url: slowHeaders.URL, wantKind: KindTimeout,
			mod: func(u *config.Upstream) { u.Timeouts.ResponseHeader = 50 * time.Millisecond }},
		{name: "non-stream default still bounded by total", url: slowHeaders.URL, wantKind: KindTimeout,
			mod: func(u *config.Upstream) { u.Timeouts.Total = 100 * time.Millisecond }},
		{name: "non-stream ignores the stream header limit", url: slowHeaders.URL,
			mod: func(u *config.Upstream) { u.Timeouts.StreamResponseHeader = 50 * time.Millisecond }},
		{name: "stream header limit catches a replica hung before headers", url: slowHeaders.URL, stream: true, wantKind: KindTimeout,
			mod: func(u *config.Upstream) { u.Timeouts.StreamResponseHeader = 50 * time.Millisecond }},
		{name: "stream header limit does not cut a slow first token", url: quickHeaders.URL, stream: true,
			mod: func(u *config.Upstream) { u.Timeouts.StreamResponseHeader = 50 * time.Millisecond }},
		{name: "stream header limit 0 means bounded by total", url: slowHeaders.URL, stream: true,
			mod: func(u *config.Upstream) { u.Timeouts.StreamResponseHeader = 0 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := New(testUpstream(tc.mod))
			defer c.CloseIdleConnections()
			resp, err := c.Do(context.Background(), replicaFor(t, tc.url),
				Request{Method: http.MethodPost, Path: "/v1/chat/completions", Body: []byte(`{}`), Stream: tc.stream})
			if tc.wantKind != "" {
				if err == nil {
					_ = resp.Body.Close()
					t.Fatal("Do succeeded, want a timeout")
				}
				if KindOf(err) != tc.wantKind || !errors.Is(err, ErrResponseHeaderTimeout) && !errors.Is(err, ErrTotalTimeout) {
					t.Fatalf("kind = %s (%v), want %s from a gateway timeout", KindOf(err), err, tc.wantKind)
				}
				return
			}
			if err != nil {
				t.Fatalf("Do: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()
			body, err := io.ReadAll(resp.Body)
			if err != nil || string(body) != `{"choices":[]}` {
				t.Fatalf("body %q, err %v", body, err)
			}
		})
	}
}

func TestRetryableOnlyForConnectErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "connect", err: &Error{Kind: KindConnect, Replica: "r", Err: errors.New("refused")}, want: true},
		{name: "wrapped connect", err: fmt.Errorf("forward: %w", &Error{Kind: KindConnect, Replica: "r", Err: errors.New("refused")}), want: true},
		{name: "dns", err: &Error{Kind: KindDNS, Replica: "r", Err: &net.DNSError{Err: "no such host", Name: "r"}}, want: true},
		{name: "timeout", err: &Error{Kind: KindTimeout, Replica: "r", Err: ErrIdleTimeout}},
		{name: "canceled", err: &Error{Kind: KindCanceled, Replica: "r", Err: context.Canceled}},
		{name: "other after send", err: &Error{Kind: KindOther, Replica: "r", Err: io.ErrUnexpectedEOF}},
		{name: "bare body read error", err: io.ErrUnexpectedEOF},
		{name: "nil", err: nil},
	}
	for _, tc := range tests {
		if got := Retryable(tc.err); got != tc.want {
			t.Errorf("%s: Retryable = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestTLSSessionCacheAndHTTP2(t *testing.T) {
	resumed := make(chan bool, 4)
	protos := make(chan string, 4)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resumed <- r.TLS != nil && r.TLS.DidResume
		protos <- r.Proto
		_, _ = w.Write([]byte("ok"))
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	roots := srv.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs

	t.Run("default HTTP/1.1 with TLS session resumption", func(t *testing.T) {
		c := New(testUpstream(func(u *config.Upstream) {
			u.IdleConnTimeout = 45 * time.Second
		}))
		defer c.CloseIdleConnections()
		tr := c.hc.Transport.(*http.Transport)
		if tr.TLSClientConfig == nil || tr.TLSClientConfig.ClientSessionCache == nil {
			t.Fatal("TLSClientConfig.ClientSessionCache not configured")
		}
		if tr.IdleConnTimeout != 45*time.Second {
			t.Fatalf("IdleConnTimeout = %s, want 45s", tr.IdleConnTimeout)
		}
		if c.ProbeClient(time.Second).Transport != c.hc.Transport {
			t.Fatal("ProbeClient does not share upstream Transport")
		}
		tr.TLSClientConfig.RootCAs = roots

		// First connection: full handshake over HTTP/1.1.
		resp, err := c.Do(context.Background(), replicaFor(t, srv.URL), Request{Method: http.MethodGet, Path: "/v1/models"})
		if err != nil {
			t.Fatalf("first Do: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		<-resumed
		if p := <-protos; p != "HTTP/1.1" {
			t.Fatalf("first proto = %q, want HTTP/1.1", p)
		}

		// Force a fresh TCP+TLS connection: ClientSessionCache resumes TLS 1.3.
		c.CloseIdleConnections()
		resp, err = c.Do(context.Background(), replicaFor(t, srv.URL), Request{Method: http.MethodGet, Path: "/v1/models"})
		if err != nil {
			t.Fatalf("second Do: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if !<-resumed {
			t.Fatal("second TLS connection did not resume session")
		}
		if p := <-protos; p != "HTTP/1.1" {
			t.Fatalf("second proto = %q, want HTTP/1.1", p)
		}
	})

	t.Run("opt-in HTTP/2 ALPN negotiation", func(t *testing.T) {
		c := New(testUpstream(func(u *config.Upstream) {
			u.EnableHTTP2 = true
		}))
		defer c.CloseIdleConnections()
		tr := c.hc.Transport.(*http.Transport)
		tr.TLSClientConfig.RootCAs = roots

		resp, err := c.Do(context.Background(), replicaFor(t, srv.URL), Request{Method: http.MethodGet, Path: "/v1/models"})
		if err != nil {
			t.Fatalf("Do HTTP/2: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		<-resumed
		if p := <-protos; p != "HTTP/2.0" {
			t.Fatalf("proto = %q, want HTTP/2.0", p)
		}
	})
}
