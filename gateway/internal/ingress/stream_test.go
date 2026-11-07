package ingress

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/mockllm"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

const (
	// streamStepTimeout bounds each wait for a relayed chunk. A gateway that
	// buffered the stream would never deliver the chunk while the replica
	// holds back the next one, so the test fails instead of hanging.
	streamStepTimeout = 5 * time.Second
	// maxTTFBOverhead is the largest acceptable median time-to-first-byte the
	// gateway may add over a direct call on loopback. AC-M0-3's real target
	// (5 ms p50) is proven by the harness; this catches buffering or a missing
	// flush, which cost a whole generation, not milliseconds.
	maxTTFBOverhead = 20 * time.Millisecond
	sseContentType  = "text/event-stream; charset=utf-8"
)

// newGatewayTo returns a gateway whose default pool has one replica at url.
func newGatewayTo(t *testing.T, url string) *httptest.Server {
	t.Helper()
	cfg := parseConfig(t, fmt.Sprintf("pools:\n  - name: large\n    replicas:\n      - url: %s\nauth: {enabled: false}\n", url))
	disc, err := discovery.NewStatic(cfg.Pools)
	if err != nil {
		t.Fatal(err)
	}
	client := upstream.New(cfg.Upstream)
	t.Cleanup(client.CloseIdleConnections)
	mux := http.NewServeMux()
	New(cfg, disc, client, roundRobin(cfg, disc, client, nil), Options{}).Register(mux)
	gw := httptest.NewServer(mux)
	t.Cleanup(gw.Close)
	return gw
}

// within runs fn and fails the test if it does not return in time.
func within(t *testing.T, what string, fn func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	case <-time.After(streamStepTimeout):
		t.Fatalf("%s: nothing relayed within %s (stream buffered?)", what, streamStepTimeout)
	}
}

// gatedUpstream is a replica that sends one SSE event each time the test
// releases it, so the test controls exactly what the replica has sent.
type gatedUpstream struct {
	events [][]byte
	next   chan struct{}
	quit   chan struct{}
	reqHdr chan http.Header
}

func (u *gatedUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	u.reqHdr <- r.Header.Clone()
	w.Header().Set("Content-Type", sseContentType)
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Request-Id", "req-123")
	w.WriteHeader(http.StatusOK)
	rc := http.NewResponseController(w)
	_ = rc.Flush()
	for _, ev := range u.events {
		select {
		case <-u.next:
		case <-u.quit:
			return
		case <-r.Context().Done():
			return
		}
		if _, err := w.Write(ev); err != nil {
			return
		}
		_ = rc.Flush()
	}
}

func TestStreamRelaysEachChunkBeforeTheNextIsSent(t *testing.T) {
	events := [][]byte{
		[]byte(`data: {"id":"cmpl-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant"}}]}` + "\n\n"),
		[]byte(`data: {"id":"cmpl-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"café "}}]}` + "\n\n"),
		[]byte(": keep-alive comment\n\n"),
		[]byte(`data: {"id":"cmpl-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}` + "\n\n"),
		[]byte("data: [DONE]\n\n"),
	}
	up := &gatedUpstream{events: events, next: make(chan struct{}), quit: make(chan struct{}), reqHdr: make(chan http.Header, 1)}
	upSrv := httptest.NewServer(up)
	t.Cleanup(upSrv.Close)
	gw := newGatewayTo(t, upSrv.URL)
	// Registered last so it runs first: releases a held-back replica handler,
	// letting the gateway and replica servers close promptly on failure.
	t.Cleanup(func() { close(up.quit) })

	req, err := http.NewRequest(http.MethodPost, gw.URL+PathChatCompletions,
		strings.NewReader(`{"model":"large-model","stream":true,"messages":[{"role":"user","content":"x"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept-Encoding", "gzip, br")

	// Headers must arrive while the replica has sent no event yet.
	var resp *http.Response
	within(t, "response headers", func() (err error) {
		resp, err = gw.Client().Do(req)
		return err
	})
	defer func() { _ = resp.Body.Close() }()

	if got := <-up.reqHdr; got.Get("Accept-Encoding") != "identity" {
		t.Fatalf("upstream Accept-Encoding = %q, want %q for a stream", got.Get("Accept-Encoding"), "identity")
	}
	wantHeaders := map[string]string{
		"Content-Type":      sseContentType,
		"Cache-Control":     "no-cache",
		"X-Request-Id":      "req-123",
		"X-Accel-Buffering": "no",
		"Content-Encoding":  "",
		"Content-Length":    "",
	}
	for k, want := range wantHeaders {
		if got := resp.Header.Get(k); got != want {
			t.Errorf("header %s = %q, want %q", k, got, want)
		}
	}
	if resp.StatusCode != http.StatusOK || resp.ContentLength != -1 {
		t.Fatalf("status %d, content length %d; want 200 with no length", resp.StatusCode, resp.ContentLength)
	}

	// Release one event at a time; each must arrive, byte for byte, while the
	// replica is still holding back the next one.
	for i, ev := range events {
		up.next <- struct{}{}
		got := make([]byte, len(ev))
		within(t, fmt.Sprintf("event %d", i), func() error {
			_, err := io.ReadFull(resp.Body, got)
			return err
		})
		if !bytes.Equal(got, ev) {
			t.Fatalf("event %d relayed as %q, want %q", i, got, ev)
		}
	}
	within(t, "end of stream", func() error {
		rest, err := io.ReadAll(resp.Body)
		if err == nil && len(rest) != 0 {
			return fmt.Errorf("unexpected trailing bytes %q", rest)
		}
		return err
	})
}

func TestNonStreamingResponseUnchanged(t *testing.T) {
	f := newFixture(t, "", Options{})
	body := []byte(`{"model":"small-model","prompt":"x","max_tokens":1,"stream":false}`)
	resp, got := f.post(t, PathCompletions, body, map[string]string{"Accept-Encoding": "gzip"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %s", resp.StatusCode, got)
	}
	if v := resp.Header.Get(headerXAccelBuffering); v != "" {
		t.Fatalf("non-stream response got %s: %q", headerXAccelBuffering, v)
	}
	reqs := f.small.Mock().Requests()
	if len(reqs) != 1 || reqs[0].Header.Get("Accept-Encoding") != "gzip" {
		t.Fatalf("non-stream request should forward the caller's Accept-Encoding; got %v", reqs)
	}
}

// sseEvent is one relayed event and when the caller received it.
type sseEvent struct {
	data string
	at   time.Time
}

// readEvents reads SSE events from r until EOF.
func readEvents(r io.Reader) ([]sseEvent, error) {
	br := bufio.NewReader(r)
	var out []sseEvent
	var cur strings.Builder
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF && line == "" {
			if cur.Len() != 0 {
				return out, fmt.Errorf("stream ended mid-event: %q", cur.String())
			}
			return out, nil
		}
		if err != nil {
			return out, err
		}
		if line == "\n" {
			out = append(out, sseEvent{data: cur.String(), at: time.Now()})
			cur.Reset()
			continue
		}
		cur.WriteString(line)
	}
}

func TestStreamFromMockllmArrivesIncrementally(t *testing.T) {
	const (
		tokens          = 6
		tokensPerSecond = 20 // one token every 50ms
		interval        = time.Second / tokensPerSecond
	)
	mock := startMock(t, func(c *mockllm.Config) { c.TokensPerSecond = tokensPerSecond })
	gw := newGatewayTo(t, mock.URL())

	for _, path := range []string{PathCompletions, PathChatCompletions} {
		t.Run(path, func(t *testing.T) {
			body := `{"model":"mock-model","stream":true,"prompt":"x","messages":[{"role":"user","content":"x"}]}`
			req, err := http.NewRequest(http.MethodPost, gw.URL+path, strings.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set(mockllm.HeaderOutputTokens, fmt.Sprint(tokens))
			resp, err := gw.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.Header.Get("Content-Type") != sseContentType || resp.Header.Get(headerXAccelBuffering) != "no" {
				t.Fatalf("stream headers = %v", resp.Header)
			}
			events, err := readEvents(resp.Body)
			if err != nil {
				t.Fatal(err)
			}

			// Framing: every event is one "data: ..." line, the last is [DONE].
			if len(events) < tokens+1 {
				t.Fatalf("got %d events, want at least %d", len(events), tokens+1)
			}
			for i, ev := range events {
				if !strings.HasPrefix(ev.data, "data: ") || strings.Count(ev.data, "\n") != 1 {
					t.Fatalf("event %d malformed: %q", i, ev.data)
				}
			}
			if last := events[len(events)-1].data; last != "data: [DONE]\n" {
				t.Fatalf("last event = %q, want [DONE]", last)
			}

			// Incremental: the caller sees tokens spread over the decode time,
			// not in one burst at the end.
			spread := events[len(events)-1].at.Sub(events[0].at)
			if minSpread := (tokens - 1) * interval / 2; spread < minSpread {
				t.Fatalf("events arrived within %s, want spread over at least %s (buffered?)", spread, minSpread)
			}
			distinct := 1
			for i := 1; i < len(events); i++ {
				if events[i].at.Sub(events[i-1].at) >= interval/4 {
					distinct++
				}
			}
			if distinct < tokens-1 {
				t.Fatalf("only %d separate arrivals for %d tokens", distinct, tokens)
			}
		})
	}
}

// timeToFirstByte returns how long a streaming request to base takes to
// deliver its first body byte, then drains the response.
func timeToFirstByte(t *testing.T, client *http.Client, base string) time.Duration {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, base+PathCompletions,
		strings.NewReader(`{"model":"mock-model","stream":true,"prompt":"x"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(mockllm.HeaderOutputTokens, "2")
	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var one [1]byte
	if _, err := io.ReadFull(resp.Body, one[:]); err != nil {
		t.Fatalf("first byte: %v", err)
	}
	ttfb := time.Since(start)
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatalf("drain: %v", err)
	}
	return ttfb
}

func median(ds []time.Duration) time.Duration {
	s := slices.Clone(ds)
	slices.Sort(s)
	return s[len(s)/2]
}

func TestStreamTTFBCloseToDirect(t *testing.T) {
	const (
		ttft    = 60 * time.Millisecond
		samples = 7
	)
	mock := startMock(t, func(c *mockllm.Config) { c.TTFT = ttft })
	gw := newGatewayTo(t, mock.URL())
	client := &http.Client{Transport: &http.Transport{}}
	t.Cleanup(client.CloseIdleConnections)

	// Warm keep-alive connections on both paths so dials are not measured.
	timeToFirstByte(t, client, mock.URL())
	timeToFirstByte(t, client, gw.URL)

	var direct, viaGateway []time.Duration
	for range samples { // interleaved so load drift hits both paths alike
		direct = append(direct, timeToFirstByte(t, client, mock.URL()))
		viaGateway = append(viaGateway, timeToFirstByte(t, client, gw.URL))
	}
	d, g := median(direct), median(viaGateway)
	if g < ttft {
		t.Fatalf("gateway TTFB %s is below the replica's TTFT %s; measurement is wrong", g, ttft)
	}
	if overhead := g - d; overhead > maxTTFBOverhead {
		t.Fatalf("median TTFB direct %s, via gateway %s: overhead %s > %s", d, g, overhead, maxTTFBOverhead)
	}
	t.Logf("median TTFB direct %s, via gateway %s", d, g)
}
