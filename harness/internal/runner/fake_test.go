package runner

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// fakeUpstream is a tiny OpenAI-style server for tests. The harness module
// cannot import the gateway's mockllm (separate module, internal package).
type fakeUpstream struct {
	ttft time.Duration
	// roleGap delays the first token after the role-only first chunk, so the
	// first body byte and the first token arrive apart.
	roleGap time.Duration
	gap     time.Duration
	tokens  int
	fault   string // "", "500", "stream_error", "truncate", "hang", "bad_json"
	cache   string // X-Gateway-Cache response header; "" sends none

	mu          sync.Mutex
	inflight    int
	maxInflight int
	requests    int
	auth        []string
	traces      []string
	priorities  [][]string // X-Priority values of each request
	headers     []http.Header
	bodies      [][]byte
}

func (f *fakeUpstream) start(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return srv
}

func (f *fakeUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	f.inflight++
	f.maxInflight = max(f.maxInflight, f.inflight)
	f.requests++
	f.auth = append(f.auth, r.Header.Get("Authorization"))
	f.traces = append(f.traces, r.Header.Get("traceparent"))
	f.priorities = append(f.priorities, r.Header.Values("X-Priority"))
	f.headers = append(f.headers, r.Header.Clone())
	f.bodies = append(f.bodies, body)
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.inflight--
		f.mu.Unlock()
	}()

	var req struct {
		Stream bool `json:"stream"`
	}
	_ = json.Unmarshal(body, &req)
	if f.cache != "" {
		w.Header().Set("X-Gateway-Cache", f.cache)
	}
	switch f.fault {
	case "500":
		http.Error(w, `{"error":{"message":"boom"}}`, http.StatusInternalServerError)
		return
	case "hang":
		<-r.Context().Done()
		return
	}
	if !sleepCtx(r, f.ttft) {
		return
	}
	if !req.Stream {
		if f.fault == "bad_json" {
			_, _ = io.WriteString(w, "not json")
			return
		}
		fmt.Fprintf(w, `{"choices":[{"message":{"role":"assistant","content":"x"},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":%d}}`, f.tokens)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	fl := w.(http.Flusher)
	// Role-only first chunk, like vLLM: not a token.
	_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}]}\n\n")
	fl.Flush()
	if !sleepCtx(r, f.roleGap) {
		return
	}
	for i := range f.tokens {
		if i > 0 && !sleepCtx(r, f.gap) {
			return
		}
		if f.fault == "stream_error" && i == 1 {
			_, _ = io.WriteString(w, "data: {\"error\":{\"message\":\"upstream died\",\"code\":502}}\n\n")
			fl.Flush()
			return
		}
		finish := "null"
		if i == f.tokens-1 {
			finish = `"stop"`
		}
		fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":\"tok\"},\"finish_reason\":%s}]}\n\n", finish)
		fl.Flush()
	}
	if f.fault == "truncate" {
		return
	}
	fmt.Fprintf(w, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":%d}}\n\ndata: [DONE]\n\n", f.tokens)
	fl.Flush()
}

func sleepCtx(r *http.Request, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	select {
	case <-time.After(d):
		return true
	case <-r.Context().Done():
		return false
	}
}

// snapshot returns the recorded Authorization headers and request count.
func (f *fakeUpstream) snapshot() (auth []string, requests, maxInflight int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.auth...), f.requests, f.maxInflight
}

// requestHeaders returns the headers of every request, in arrival order.
func (f *fakeUpstream) requestHeaders() []http.Header {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]http.Header(nil), f.headers...)
}

// priorityHeaders returns the X-Priority values of every request, in arrival order.
func (f *fakeUpstream) priorityHeaders() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]string(nil), f.priorities...)
}

// traceparents returns the traceparent header of every request, in arrival order.
func (f *fakeUpstream) traceparents() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.traces...)
}
