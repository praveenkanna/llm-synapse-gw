package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/harness/internal/outcome"
)

func sha(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// TestResponseOutcomes: each response is judged by the outcome rules (ADR
// 0021): complete needs [DONE] after a finish_reason with no error event for
// a stream, and a finish_reason on every choice for a body; ok also needs a
// 2xx status.
func TestResponseOutcomes(t *testing.T) {
	tests := []struct {
		name             string
		stream           bool
		status           int
		body             string
		complete, ok, ev bool
		wantErr          string
	}{
		{name: "body with finish_reason", body: `{"choices":[{"finish_reason":"stop","message":{"content":"x"}}]}`, complete: true, ok: true},
		{name: "body without finish_reason", body: `{"choices":[{"message":{"content":"x"}}]}`, wantErr: "truncated"},
		{name: "body cut short", body: `{"choices":[{"finish_reason":"st`, wantErr: "bad_response"},
		{name: "stream to [DONE]", stream: true, body: "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\ndata: {\"choices\":[{\"delta\":{},\"finish_reason\":\"length\"}]}\n\ndata: [DONE]\n\n", complete: true, ok: true},
		{name: "stream without [DONE]", stream: true, body: "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n", wantErr: "truncated"},
		{name: "stream without finish_reason", stream: true, body: "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\ndata: [DONE]\n\n", wantErr: "truncated"},
		{name: "stream with an error event", stream: true, body: "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: {\"error\":{\"message\":\"aborted\"}}\n\ndata: [DONE]\n\n", ev: true, wantErr: "stream_error"},
		{name: "a 503", status: http.StatusServiceUnavailable, body: `{"error":{"message":"busy"}}`, wantErr: "http_status"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.stream {
					w.Header().Set("Content-Type", "text/event-stream")
				}
				if tc.status != 0 {
					w.WriteHeader(tc.status)
				}
				fmt.Fprint(w, tc.body)
			}))
			defer srv.Close()
			rep := send(context.Background(), options{url: srv.URL, path: "/", n: 1, timeout: 5 * time.Second, slowRate: 1}, []byte(`{}`))
			r := rep.Responses[0]
			if r.Complete != tc.complete || r.OK != tc.ok || r.ErrorEvent != tc.ev || r.Err != tc.wantErr {
				t.Fatalf("complete %v ok %v error_event %v error %q; want %v %v %v %q", r.Complete, r.OK, r.ErrorEvent, r.Err, tc.complete, tc.ok, tc.ev, tc.wantErr)
			}
			if r.SHA256 != sha(tc.body) || r.Bytes != len(tc.body) {
				t.Fatalf("bytes %d sha %s, want every byte hashed (%d, %s)", r.Bytes, r.SHA256, len(tc.body), sha(tc.body))
			}
			if tc.ok && (r.FirstMS <= 0 || r.TTFTMS < r.FirstMS) {
				t.Fatalf("first byte %v ms, TTFT %v ms; want both, TTFT not before the first byte", r.FirstMS, r.TTFTMS)
			}
		})
	}
}

// TestSendSameBytesCounted: every response is hashed, identical ones count
// as one, and the leader goes first when -lead-delay is set.
func TestSendSameBytesCounted(t *testing.T) {
	const leadDelay = 400 * time.Millisecond
	var mu sync.Mutex
	var arrived []time.Time
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		arrived = append(arrived, time.Now())
		mu.Unlock()
		if r.Header.Get("X-Test") != "yes" {
			http.Error(w, "missing header", http.StatusBadRequest)
			return
		}
		w.Header().Set("X-Gateway-Cache", "hit")
		fmt.Fprint(w, `{"choices":[{"finish_reason":"stop"}]}`)
	}))
	defer srv.Close()
	o := options{url: srv.URL, path: "/", n: 5, headers: headerFlags{"X-Test: yes"}, leadDelay: leadDelay, timeout: 5 * time.Second, slow: 1, slowRate: 1 << 20}
	start := time.Now()
	rep := send(context.Background(), o, []byte(`{}`))
	if rep.OK != 5 || rep.Complete != 5 || rep.Distinct != 1 {
		t.Fatalf("ok %d complete %d distinct %d, want 5 5 1", rep.OK, rep.Complete, rep.Distinct)
	}
	if !rep.Responses[4].Slow || rep.Responses[0].Slow || rep.Responses[0].Cache != "hit" {
		t.Fatalf("responses = %+v", rep.Responses)
	}
	// The lead delay is long next to scheduling delays under load: the
	// leader arrives well inside it, and the rest never before it ends.
	mu.Lock()
	defer mu.Unlock()
	slices.SortFunc(arrived, time.Time.Compare)
	if d := arrived[0].Sub(start); d > leadDelay*3/4 {
		t.Fatalf("first request after %v, want it well before the %v lead delay", d, leadDelay)
	}
	if d := arrived[1].Sub(start); d < leadDelay {
		t.Fatalf("second request after %v, want none before the %v lead delay", d, leadDelay)
	}
}

// TestEveryByteHashed: the hash covers the whole body as sent, including
// bytes after an error event or after [DONE] that the outcome rules ignore,
// for a fast reader and for a slow one whose reads split events.
func TestEveryByteHashed(t *testing.T) {
	const (
		tok    = "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n"
		finish = "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"
		done   = "data: [DONE]\n\n"
	)
	tests := []struct {
		name    string
		events  []string
		wantErr string
	}{
		// A replica died mid-event: the gateway closes the half-sent event,
		// then sends its error event and [DONE].
		{name: "error event then more bytes", events: []string{tok, "data: {\"choi\n\n", "data: {\"error\":{\"code\":\"upstream_error\"}}\n\n", done, ": trailing\n\n"}, wantErr: outcome.ErrStreamError},
		{name: "bytes after [DONE]", events: []string{tok, finish, done, ": trailing\n\n"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var full string
			for _, e := range tc.events {
				full += e
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				for _, e := range tc.events {
					fmt.Fprint(w, e)
					w.(http.Flusher).Flush()
					time.Sleep(5 * time.Millisecond)
				}
			}))
			defer srv.Close()
			// 300 B/s reads 30 bytes at a time, so events split across reads.
			rep := send(context.Background(), options{url: srv.URL, path: "/", n: 2, timeout: 10 * time.Second, slow: 1, slowRate: 300}, []byte(`{}`))
			for _, r := range rep.Responses {
				if r.SHA256 != sha(full) || r.Bytes != len(full) {
					t.Fatalf("slow %v: bytes %d sha %s, want %d %s", r.Slow, r.Bytes, r.SHA256, len(full), sha(full))
				}
				if r.Err != tc.wantErr || r.OK != (tc.wantErr == "") || r.ErrorEvent != (tc.wantErr != "") {
					t.Fatalf("slow %v: error %q ok %v error_event %v, want %q", r.Slow, r.Err, r.OK, r.ErrorEvent, tc.wantErr)
				}
			}
		})
	}
}

// hang holds a request until its client leaves. The body is read first:
// net/http notices a client leaving only once the handler has read it.
func hang(r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
	<-r.Context().Done()
}

// TestTransportKinds: failures before and during the response are named by
// the outcome rules, the same as the load generator names them.
func TestTransportKinds(t *testing.T) {
	refused := func(t *testing.T) string {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		url := "http://" + ln.Addr().String()
		_ = ln.Close()
		return url
	}
	server := func(h http.HandlerFunc) func(t *testing.T) string {
		return func(t *testing.T) string {
			srv := httptest.NewServer(h)
			t.Cleanup(srv.Close)
			return srv.URL
		}
	}
	tests := []struct {
		name string
		url  func(t *testing.T) string
		want string
	}{
		{"connection refused", refused, outcome.ErrConnect},
		{"closed before the response", server(func(w http.ResponseWriter, _ *http.Request) {
			c, _, _ := w.(http.Hijacker).Hijack()
			_ = c.Close()
		}), outcome.ErrRequest},
		{"timeout before the headers", server(func(_ http.ResponseWriter, r *http.Request) {
			hang(r)
		}), outcome.ErrTimeout},
		{"timeout while reading", server(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n")
			w.(http.Flusher).Flush()
			hang(r)
		}), outcome.ErrTimeout},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rep := send(context.Background(), options{url: tc.url(t), path: "/", n: 1, timeout: 200 * time.Millisecond, slowRate: 1}, []byte(`{}`))
			if r := rep.Responses[0]; r.Err != tc.want || r.OK {
				t.Fatalf("error %q ok %v, want %q", r.Err, r.OK, tc.want)
			}
		})
	}
}
