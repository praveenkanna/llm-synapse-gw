package health

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestEndpoints(t *testing.T) {
	tests := []struct {
		name       string
		draining   bool
		method     string
		path       string
		wantStatus int
		wantBody   string
	}{
		{"livez ok", false, http.MethodGet, LivenessPath, http.StatusOK, "ok\n"},
		{"livez HEAD", false, http.MethodHead, LivenessPath, http.StatusOK, ""},
		{"livez stays ok while draining", true, http.MethodGet, LivenessPath, http.StatusOK, "ok\n"},
		{"livez wrong method", false, http.MethodPost, LivenessPath, http.StatusMethodNotAllowed, ""},
		{"readyz ready", false, http.MethodGet, ReadinessPath, http.StatusOK, "ready\n"},
		{"readyz HEAD", false, http.MethodHead, ReadinessPath, http.StatusOK, ""},
		{"readyz draining", true, http.MethodGet, ReadinessPath, http.StatusServiceUnavailable, "draining\n"},
		{"readyz wrong method", false, http.MethodPost, ReadinessPath, http.StatusMethodNotAllowed, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := New()
			if tc.draining {
				c.SetDraining()
			}
			mux := http.NewServeMux()
			c.Register(mux)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, nil))
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
			if tc.wantBody != "" && rec.Body.String() != tc.wantBody {
				t.Fatalf("body = %q, want %q", rec.Body.String(), tc.wantBody)
			}
			if tc.wantStatus != http.StatusMethodNotAllowed && rec.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("Cache-Control = %q, want no-store", rec.Header().Get("Cache-Control"))
			}
		})
	}
}

func TestSetDrainingIsIdempotent(t *testing.T) {
	c := New()
	if c.Draining() {
		t.Fatal("new checker is draining")
	}
	c.SetDraining()
	c.SetDraining()
	if !c.Draining() {
		t.Fatal("checker not draining after SetDraining")
	}
}

// streamHandler serves /stream as SSE: it sends one event, signals started,
// then holds the stream open until release is closed (sending a final event)
// or the request context ends (recording that on cancelled).
type streamHandler struct {
	started   chan struct{}
	release   chan struct{}
	cancelled chan struct{}
}

func newStreamHandler() *streamHandler {
	return &streamHandler{
		started:   make(chan struct{}),
		release:   make(chan struct{}),
		cancelled: make(chan struct{}),
	}
}

func (s *streamHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "data: first\n\n")
	w.(http.Flusher).Flush()
	close(s.started)
	select {
	case <-s.release:
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		w.(http.Flusher).Flush()
	case <-r.Context().Done():
		close(s.cancelled)
	}
}

type harness struct {
	addr   string
	ck     *Checker
	stream *streamHandler
	cancel context.CancelFunc
	// done is closed when Serve returns; err holds its result (read only
	// after done is closed).
	done chan struct{}
	err  error
}

// startServe runs Serve on a loopback listener with /stream and health routes.
func startServe(t *testing.T, drainTimeout time.Duration) *harness {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ck := New()
	sh := newStreamHandler()
	mux := http.NewServeMux()
	ck.Register(mux)
	mux.Handle("GET /stream", sh)
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	ctx, cancel := context.WithCancel(context.Background())
	h := &harness{addr: ln.Addr().String(), ck: ck, stream: sh, cancel: cancel, done: make(chan struct{})}
	go func() {
		h.err = Serve(ctx, srv, ln, ck, drainTimeout, 0, discardLogger())
		close(h.done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-h.done:
		case <-time.After(5 * time.Second):
			t.Error("Serve did not return during cleanup")
		}
	})
	return h
}

// openStream starts GET /stream and returns once the first event arrived.
func (h *harness) openStream(t *testing.T) (*http.Response, *bufio.Reader) {
	t.Helper()
	resp, err := http.Get("http://" + h.addr + "/stream")
	if err != nil {
		t.Fatalf("GET /stream: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	br := bufio.NewReader(resp.Body)
	line, err := br.ReadString('\n')
	if err != nil || line != "data: first\n" {
		t.Fatalf("first line = %q, %v", line, err)
	}
	<-h.stream.started
	return resp, br
}

// waitRefused polls until a new TCP connection to addr is refused.
func waitRefused(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err != nil {
			return
		}
		_ = conn.Close()
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("listener still accepting connections after drain began")
}

func waitDone(t *testing.T, h *harness, within time.Duration) {
	t.Helper()
	select {
	case <-h.done:
		if h.err != nil {
			t.Fatalf("Serve returned error: %v", h.err)
		}
	case <-time.After(within):
		t.Fatalf("Serve did not return within %v", within)
	}
}

func TestServeReadyBeforeDrain(t *testing.T) {
	h := startServe(t, time.Second)
	resp, err := http.Get("http://" + h.addr + ReadinessPath)
	if err != nil {
		t.Fatalf("GET readyz: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("readyz = %d, want 200", resp.StatusCode)
	}
}

func TestServeDrainsOpenStream(t *testing.T) {
	h := startServe(t, 10*time.Second)
	_, br := h.openStream(t)

	h.cancel() // SIGTERM equivalent

	// New connections are refused while the stream is still open.
	waitRefused(t, h.addr)
	if !h.ck.Draining() {
		t.Fatal("readiness still true while draining")
	}
	select {
	case <-h.done:
		t.Fatalf("Serve returned (%v) while a stream was still open", h.err)
	case <-time.After(100 * time.Millisecond):
	}

	// The open stream finishes normally and Serve then returns.
	close(h.stream.release)
	rest, err := io.ReadAll(br)
	if err != nil {
		t.Fatalf("reading rest of stream: %v", err)
	}
	if !strings.Contains(string(rest), "data: [DONE]\n\n") {
		t.Fatalf("stream tail = %q, want [DONE] event", rest)
	}
	select {
	case <-h.stream.cancelled:
		t.Fatal("stream request context was cancelled during a drain that did not time out")
	default:
	}
	waitDone(t, h, 5*time.Second)
}

func TestServeClosesStreamsAfterDrainTimeout(t *testing.T) {
	const drainTimeout = 150 * time.Millisecond
	h := startServe(t, drainTimeout)
	_, br := h.openStream(t)

	start := time.Now()
	h.cancel()
	waitDone(t, h, 5*time.Second)
	if elapsed := time.Since(start); elapsed < drainTimeout {
		t.Fatalf("Serve returned after %v, before the %v drain timeout", elapsed, drainTimeout)
	}

	// The stuck stream was cut: its request context was cancelled (so an
	// upstream request tied to it would be too) and the client sees the end.
	select {
	case <-h.stream.cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("stream request context not cancelled after drain timeout")
	}
	rest, _ := io.ReadAll(br)
	if strings.Contains(string(rest), "[DONE]") {
		t.Fatalf("stream completed (%q), want it cut", rest)
	}
}

func TestServeWithNoOpenRequestsReturnsPromptly(t *testing.T) {
	h := startServe(t, 10*time.Second)
	start := time.Now()
	h.cancel()
	waitDone(t, h, 5*time.Second)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("idle drain took %v, want prompt return", elapsed)
	}
	if !h.ck.Draining() {
		t.Fatal("checker not draining after Serve returned")
	}
}

func TestServeReturnsErrorOnClosedListener(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	_ = ln.Close()
	ck := New()
	done := make(chan error, 1)
	go func() {
		done <- Serve(context.Background(), &http.Server{Handler: http.NewServeMux()}, ln, ck, time.Second, 0, discardLogger())
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Serve returned nil, want error for closed listener")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return for closed listener")
	}
	if !ck.Draining() {
		t.Fatal("readiness still true after the server failed")
	}
}

func TestServePreDrainDelay(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	ck := New()
	mux := http.NewServeMux()
	ck.Register(mux)
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	const preDrain = 60 * time.Millisecond
	go func() {
		done <- Serve(ctx, srv, ln, ck, time.Second, preDrain, discardLogger())
	}()

	addr := ln.Addr().String()
	// Before cancel: readyz is 200.
	resp, err := http.Get("http://" + addr + ReadinessPath)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("readyz before cancel = %v, err %v", resp, err)
	}
	_ = resp.Body.Close()

	start := time.Now()
	cancel()

	// During pre-drain: readiness immediately reports 503 while listener still accepts requests.
	time.Sleep(10 * time.Millisecond)
	if !ck.Draining() {
		t.Fatal("readiness not draining during pre-drain window")
	}
	resp, err = http.Get("http://" + addr + ReadinessPath)
	if err != nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("readyz during pre-drain = %v, err %v, want 503", resp, err)
	}
	_ = resp.Body.Close()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve error: %v", err)
		}
		if elapsed := time.Since(start); elapsed < preDrain {
			t.Fatalf("Serve returned in %v, expected at least preDrain %v", elapsed, preDrain)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return")
	}
}
