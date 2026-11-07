package mockllm

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// failingWriter accepts headers but fails every body write, like a client
// that disconnected after the handler started answering.
type failingWriter struct{ *httptest.ResponseRecorder }

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("broken pipe") }

func newTestMock(t *testing.T) *Mock {
	t.Helper()
	cfg := Defaults()
	cfg.Models = []string{testModel}
	cfg.TTFT = 0
	cfg.TokensPerSecond = 0
	cfg.OutputTokens = LengthDist{Kind: DistFixed, A: 4}
	cfg.Seed = 1
	m, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return m
}

func TestNonStreamingCountsCompletedOnlyWhenDelivered(t *testing.T) {
	const body = `{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	tests := []struct {
		name          string
		ctx           context.Context
		writer        func() http.ResponseWriter
		wantCompleted uint64
		wantCancelled uint64
	}{
		{"delivered", context.Background(), func() http.ResponseWriter { return httptest.NewRecorder() }, 1, 0},
		{"write fails", context.Background(), func() http.ResponseWriter { return failingWriter{httptest.NewRecorder()} }, 0, 1},
		{"client already gone", cancelled, func() http.ResponseWriter { return httptest.NewRecorder() }, 0, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newTestMock(t)
			r := httptest.NewRequestWithContext(tt.ctx, http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
			m.ServeHTTP(tt.writer(), r)
			s := m.Stats()
			if s.Completed != tt.wantCompleted || s.Cancelled != tt.wantCancelled || s.Running != 0 {
				t.Fatalf("stats = completed %d cancelled %d running %d, want completed %d cancelled %d running 0",
					s.Completed, s.Cancelled, s.Running, tt.wantCompleted, tt.wantCancelled)
			}
		})
	}
}

// TestNonStreamingHasContentLength guards the flush in sendJSON: the body must
// still arrive with a Content-Length, not chunked, as vLLM sends it.
func TestNonStreamingHasContentLength(t *testing.T) {
	srv := startMock(t, nil)
	resp, err := post(t, context.Background(), srv.URL()+"/v1/completions", `{"model":"test-model","prompt":"hi"}`, nil)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if resp.StatusCode != http.StatusOK || len(resp.TransferEncoding) != 0 || resp.ContentLength != int64(len(b)) {
		t.Fatalf("status %d transfer-encoding %v content-length %d body %d bytes",
			resp.StatusCode, resp.TransferEncoding, resp.ContentLength, len(b))
	}
}
