package ingress

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/mockllm"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/health"
)

// Stuck-engine detection through the relay (M1-13): every streaming
// attempt registers a first-chunk wait on its lease's replica, the relay
// ends it on the first body byte, and every other exit ends it too. The
// health tracker runs on a fake clock and is scanned by hand.

// stuckEngineTTFT is a first token so late that, to the test, the replica
// sent its headers and then nothing: the M0-76 failure mode.
const stuckEngineTTFT = "3600000" // ms

var ringPolicy = routingPolicy{name: "affinity_ring", affinity: true, ring: true}

// fakeClock is a manual clock for the health tracker.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// hungStream is a streaming request whose response headers arrived and
// whose body is still open.
type hungStream struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// openStream sends a streaming request with hdr and returns once the
// gateway relayed the response headers; the body is drained in the
// background until the stream ends or close is called.
func openStream(t *testing.T, g *leaseGateway, hdr map[string]string) *hungStream {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.gw.URL+PathChatCompletions, strings.NewReader(streamBody))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range g.headers(hdr) {
		req.Header.Set(k, v)
	}
	resp, err := g.gw.Client().Do(req)
	if err != nil {
		cancel()
		t.Fatalf("POST: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		cancel()
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	s := &hungStream{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(s.done)
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	t.Cleanup(s.close) // a failed test must not leave the gateway's Close waiting on it
	return s
}

// close disconnects the caller and waits for the body reader. Safe to
// call more than once.
func (s *hungStream) close() {
	s.cancel()
	<-s.done
}

func stuckGateway(t *testing.T, urls []string, clock *fakeClock, mod func(*health.Config)) *leaseGateway {
	t.Helper()
	return newLeaseGatewayWith(t, ringPolicy, urls, streamBody, leaseOptions{health: mod, clock: clock})
}

// TestStuckReplicaEjectedAndReadmittedPerRules: r0 sends headers and no
// tokens. One stalled stream does not eject it; three within the window
// do (stuck_engine); a passing /health does not re-admit it before its
// ejection time, and does after. Meanwhile the conversation moves to r1.
func TestStuckReplicaEjectedAndReadmittedPerRules(t *testing.T) {
	clock := newFakeClock()
	stuck := startMock(t, nil)
	g := stuckGateway(t, []string{stuck.URL(), startMock(t, nil).URL()}, clock, nil) // defaults: 30 s, 3 streams, 1 min
	hang := map[string]string{mockllm.HeaderTTFTMs: stuckEngineTTFT}
	timeout := health.ConfigFrom(config.Defaults().Router).StuckEngineTimeout

	first := openStream(t, g, hang)
	waitFor(t, "the first wait registered", func() bool { return g.tracker.FirstChunkWaits() == 1 })
	clock.Advance(timeout)
	g.tracker.CheckStuckStreams()
	if !g.tracker.IsHealthy("r0") {
		t.Fatal("a single stalled stream ejected r0")
	}

	streams := []*hungStream{first, openStream(t, g, hang), openStream(t, g, hang)}
	waitFor(t, "3 waits registered", func() bool { return g.tracker.FirstChunkWaits() == 3 })
	clock.Advance(timeout - time.Second)
	g.tracker.CheckStuckStreams()
	if !g.tracker.IsHealthy("r0") {
		t.Fatal("r0 ejected before the 2nd and 3rd streams reached stuck_engine_timeout")
	}
	clock.Advance(time.Second)
	g.tracker.CheckStuckStreams()
	if g.tracker.IsHealthy("r0") {
		t.Fatal("r0 still in after 3 stalled streams within the window")
	}
	if got := g.counter.waitsStarted("r0"); got != 3 {
		t.Errorf("waits started on r0 = %d, want 3", got)
	}

	// /health still passes (the mock's front end is fine): not enough alone.
	if err := g.tracker.PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if g.tracker.IsHealthy("r0") {
		t.Fatal("a passing /health re-admitted a stuck engine before its ejection time")
	}

	// The same conversation now goes to r1 and streams normally.
	resp, got, err := g.do(t, streamBody, g.headers(map[string]string{mockllm.HeaderOutputTokens: "3"}))
	if err != nil || resp.StatusCode != http.StatusOK || !strings.Contains(string(got), "data: [DONE]") {
		t.Fatalf("request after the ejection: status %d, err %v, body %q", resp.StatusCode, err, got)
	}
	g.waitHandled(t, 1)
	if n := g.counter.held("r1"); n != 0 || g.counter.waitsStarted("r1") != 1 {
		t.Errorf("r1: %d leases held, %d waits started; want 0 and 1", n, g.counter.waitsStarted("r1"))
	}

	clock.Advance(health.ConfigFrom(config.Defaults().Router).MinEjection)
	if err := g.tracker.PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !g.tracker.IsHealthy("r0") {
		t.Fatal("r0 not re-admitted by a good /health after its ejection time")
	}

	for _, s := range streams {
		s.close()
	}
	g.waitHandled(t, len(streams))
	g.assertReleased(t, len(streams)+1)
}

// TestStuckReplicaWithFastStreamsEjected: while most streams to r0 hang,
// a few newer ones get their first byte at once (as a stuck engine
// answering some requests with an in-stream error would). Answers to
// younger streams excuse nothing: 4 stalls against 2 first chunks eject.
func TestStuckReplicaWithFastStreamsEjected(t *testing.T) {
	clock := newFakeClock()
	g := stuckGateway(t, []string{startMock(t, nil).URL(), startMock(t, nil).URL()}, clock, nil)
	timeout := health.ConfigFrom(config.Defaults().Router).StuckEngineTimeout
	var hung []*hungStream
	for range 4 {
		hung = append(hung, openStream(t, g, map[string]string{mockllm.HeaderTTFTMs: stuckEngineTTFT}))
	}
	waitFor(t, "4 waits registered", func() bool { return g.tracker.FirstChunkWaits() == 4 })
	clock.Advance(timeout * 2 / 3)
	for range 2 {
		resp, got, err := g.do(t, streamBody, g.headers(map[string]string{mockllm.HeaderOutputTokens: "1"}))
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("fast stream: status %d, err %v (%s)", resp.StatusCode, err, got)
		}
		g.waitHandled(t, 1)
	}
	clock.Advance(timeout - timeout*2/3)
	g.tracker.CheckStuckStreams()
	if g.tracker.IsHealthy("r0") {
		t.Fatal("fast streams on younger requests masked a stuck engine")
	}
	for _, s := range hung {
		s.close()
	}
	g.waitHandled(t, len(hung))
	g.assertReleased(t, len(hung)+2)
}

// TestFIFOBacklogNotEjected: r0 works through a backlog in arrival order:
// the oldest streams get their first token late (here 3 s of real time,
// 20 s on the tracker's clock), the newer ones are still queued when they
// pass stuck_engine_timeout. They are excused (an older stream was
// answered recently), so r0 stays in although S ≫ F.
func TestFIFOBacklogNotEjected(t *testing.T) {
	clock := newFakeClock()
	g := stuckGateway(t, []string{startMock(t, nil).URL(), startMock(t, nil).URL()}, clock, nil)
	timeout := health.ConfigFrom(config.Defaults().Router).StuckEngineTimeout
	done := make(chan int, 2)
	for range 2 { // the head of the queue: answered after a while
		go func() {
			resp, _, _ := g.do(t, streamBody, g.headers(map[string]string{mockllm.HeaderTTFTMs: "3000", mockllm.HeaderOutputTokens: "1"}))
			done <- resp.StatusCode
		}()
	}
	waitFor(t, "the head of the queue registered", func() bool { return g.tracker.FirstChunkWaits() == 2 })
	var queued []*hungStream
	for range 6 { // queued behind it: not answered within the test
		queued = append(queued, openStream(t, g, map[string]string{mockllm.HeaderTTFTMs: stuckEngineTTFT}))
	}
	waitFor(t, "8 waits registered", func() bool { return g.tracker.FirstChunkWaits() == 8 })
	clock.Advance(timeout * 2 / 3) // before the head's first tokens arrive (3 s real)
	for range 2 {
		if s := <-done; s != http.StatusOK {
			t.Fatalf("head of the queue: status %d", s)
		}
	}
	g.waitHandled(t, 2)
	clock.Advance(timeout - timeout*2/3)
	g.tracker.CheckStuckStreams()
	if !g.tracker.IsHealthy("r0") {
		t.Fatal("a replica serving its backlog in arrival order was ejected")
	}
	for _, s := range queued {
		s.close()
	}
	g.waitHandled(t, len(queued))
	g.assertReleased(t, len(queued)+2)
}

// TestSlowFirstTokenNeverEjects: a replica whose first token comes later
// than usual but before stuck_engine_timeout is never ejected, even with a
// threshold of one stream.
func TestSlowFirstTokenNeverEjects(t *testing.T) {
	clock := newFakeClock()
	g := stuckGateway(t, []string{startMock(t, nil).URL(), startMock(t, nil).URL()}, clock,
		func(c *health.Config) { c.StuckEngineStreams = 1 })
	timeout := health.ConfigFrom(config.Defaults().Router).StuckEngineTimeout
	const requests = 3
	for range requests {
		done := make(chan int, 1)
		go func() {
			resp, _, _ := g.do(t, streamBody, g.headers(map[string]string{mockllm.HeaderTTFTMs: "200", mockllm.HeaderOutputTokens: "3"}))
			done <- resp.StatusCode
		}()
		waitFor(t, "the wait registered", func() bool { return g.tracker.FirstChunkWaits() == 1 })
		clock.Advance(timeout - time.Second) // the first token is still to come
		g.tracker.CheckStuckStreams()
		if s := <-done; s != http.StatusOK {
			t.Fatalf("status = %d, want 200", s)
		}
		g.waitHandled(t, 1)
		clock.Advance(time.Minute)
		g.tracker.CheckStuckStreams()
		if !g.tracker.IsHealthy("r0") {
			t.Fatal("a slow but healthy replica was ejected")
		}
	}
	if got := g.counter.waitsStarted("r0"); got != requests {
		t.Errorf("waits started on r0 = %d, want %d", got, requests)
	}
	g.assertReleased(t, requests)
}

// TestFirstChunkWaitEndsOnEveryExit: whatever way a streaming attempt
// ends, its wait is unregistered (assertReleased checks the registry is
// empty), and waits were really started.
func TestFirstChunkWaitEndsOnEveryExit(t *testing.T) {
	live := func(t *testing.T) string { return startMock(t, nil).URL() }
	tests := []struct {
		name       string
		urls       func(t *testing.T) []string
		hdr        map[string]string
		wantStatus int
		wantLeases int
		wantWaits  int
	}{
		{name: "success", wantStatus: http.StatusOK, wantLeases: 1, wantWaits: 1,
			urls: func(t *testing.T) []string { return []string{live(t), live(t)} }},
		{name: "replica answers 5xx", wantStatus: http.StatusInternalServerError, wantLeases: 1, wantWaits: 1,
			hdr:  map[string]string{mockllm.HeaderFault: "5xx"},
			urls: func(t *testing.T) []string { return []string{live(t), live(t)} }},
		{name: "replica fails mid-stream", wantStatus: http.StatusOK, wantLeases: 1, wantWaits: 1,
			hdr:  map[string]string{mockllm.HeaderFault: "drop"},
			urls: func(t *testing.T) []string { return []string{live(t), live(t)} }},
		{name: "connect retry on another replica", wantStatus: http.StatusOK, wantLeases: 2, wantWaits: 2,
			urls: func(t *testing.T) []string { return []string{deadURL(t), live(t)} }},
		{name: "same-replica retry fails", wantStatus: http.StatusBadGateway, wantLeases: 1, wantWaits: 2,
			urls: func(t *testing.T) []string { return []string{deadURL(t)} }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := newLeaseGatewayWith(t, ringPolicy, tc.urls(t), streamBody, leaseOptions{backoff: immediateBackoff})
			hdr := g.headers(tc.hdr)
			hdr[mockllm.HeaderOutputTokens] = "4"
			resp, got, _ := g.do(t, streamBody, hdr)
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d (%s)", resp.StatusCode, tc.wantStatus, got)
			}
			g.waitHandled(t, 1)
			if n := g.counter.waitsStarted("r0") + g.counter.waitsStarted("r1"); n != tc.wantWaits {
				t.Errorf("waits started = %d, want %d", n, tc.wantWaits)
			}
			g.assertReleased(t, tc.wantLeases)
		})
	}
}

// TestFirstChunkWaitEndsOnClientDisconnect: a caller that leaves while
// the replica has sent no token ends the wait.
func TestFirstChunkWaitEndsOnClientDisconnect(t *testing.T) {
	g := stuckGateway(t, []string{startMock(t, nil).URL(), startMock(t, nil).URL()}, newFakeClock(), nil)
	s := openStream(t, g, map[string]string{mockllm.HeaderTTFTMs: stuckEngineTTFT})
	waitFor(t, "the wait registered", func() bool { return g.tracker.FirstChunkWaits() == 1 })
	s.close()
	g.waitHandled(t, 1)
	g.assertReleased(t, 1)
}

// panicOnWriteHeader makes the handler panic as it starts the response,
// after the attempt's wait was registered and before any body byte.
func panicOnWriteHeader(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(panickyWriter{w}, r)
	})
}

type panickyWriter struct{ http.ResponseWriter }

func (panickyWriter) WriteHeader(int) { panic("test: response writer failed") }

// TestFirstChunkWaitEndsOnPanic: a panic on the request path still ends
// the wait (and releases the lease).
func TestFirstChunkWaitEndsOnPanic(t *testing.T) {
	g := newLeaseGatewayWith(t, ringPolicy, []string{startMock(t, nil).URL(), startMock(t, nil).URL()}, streamBody,
		leaseOptions{wrap: panicOnWriteHeader})
	req, err := http.NewRequest(http.MethodPost, g.gw.URL+PathChatCompletions, strings.NewReader(streamBody))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range g.headers(nil) {
		req.Header.Set(k, v)
	}
	if resp, err := g.gw.Client().Do(req); err == nil {
		_ = resp.Body.Close()
		t.Fatal("request succeeded, want the connection aborted")
	}
	g.waitHandled(t, 1)
	if g.counter.waitsStarted("r0") != 1 {
		t.Fatalf("waits started = %d, want 1", g.counter.waitsStarted("r0"))
	}
	g.assertReleased(t, 1)
}

// TestFirstChunkRegistryFullFailsOpen: with the registry full, a stream is
// relayed normally, just not watched.
func TestFirstChunkRegistryFullFailsOpen(t *testing.T) {
	g := stuckGateway(t, []string{startMock(t, nil).URL(), startMock(t, nil).URL()}, newFakeClock(),
		func(c *health.Config) { c.StuckEngineMaxWaits = 1 })
	hung := openStream(t, g, map[string]string{mockllm.HeaderTTFTMs: stuckEngineTTFT})
	waitFor(t, "the wait registered", func() bool { return g.tracker.FirstChunkWaits() == 1 })

	resp, got, err := g.do(t, streamBody, g.headers(map[string]string{mockllm.HeaderOutputTokens: "3"}))
	if err != nil || resp.StatusCode != http.StatusOK || !strings.Contains(string(got), "data: [DONE]") {
		t.Fatalf("stream past the registry cap: status %d, err %v, body %q", resp.StatusCode, err, got)
	}
	g.waitHandled(t, 1)
	if n := g.tracker.FirstChunkWaits(); n != 1 {
		t.Fatalf("waits = %d, want 1 (the cap)", n)
	}
	hung.close()
	g.waitHandled(t, 1)
	g.assertReleased(t, 2)
}

// TestNonStreamingRequestsNotWatched: vLLM answers a non-streaming request
// only once it is generated, so no wait is started for one.
func TestNonStreamingRequestsNotWatched(t *testing.T) {
	g := newLeaseGateway(t, ringPolicy, []string{startMock(t, nil).URL(), startMock(t, nil).URL()}, chatBody)
	resp, got, _ := g.do(t, chatBody, g.headers(nil))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d (%s)", resp.StatusCode, got)
	}
	g.waitHandled(t, 1)
	if n := g.counter.waitsStarted("r0"); n != 0 {
		t.Fatalf("waits started for a non-streaming request: %d", n)
	}
	g.assertReleased(t, 1)
}
