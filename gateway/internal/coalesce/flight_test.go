package coalesce

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

func testGroup(max config.Coalescing) *Group {
	if max.MaxFlights == 0 {
		max.MaxFlights = 8
	}
	if max.MaxSubscribers == 0 {
		max.MaxSubscribers = 64
	}
	if max.MaxReplayBytes == 0 {
		max.MaxReplayBytes = MinReplayBytes
	}
	return NewGroup(Config{Coalescing: max})
}

// lead acquires a joinable flight that must be led.
func lead(t *testing.T, g *Group, key string) *Flight {
	t.Helper()
	f, sub, reason := g.Acquire(key, true)
	if f == nil || sub != nil || reason != "" {
		t.Fatalf("Acquire(%q): f=%v sub=%v reason=%q, want a new flight", key, f, sub, reason)
	}
	return f
}

func join(t *testing.T, g *Group, key string) *Subscription {
	t.Helper()
	f, sub, reason := g.Acquire(key, true)
	if sub == nil {
		t.Fatalf("Acquire(%q): want a subscription, got f=%v reason=%q", key, f, reason)
	}
	return sub
}

// result is what one subscriber's Serve produced.
type result struct {
	status int
	header http.Header
	body   string
	err    error
}

func serve(sub *Subscription, ctx context.Context) <-chan result {
	out := make(chan result, 1)
	go func() {
		rec := httptest.NewRecorder()
		err := sub.Serve(ctx, rec, nil)
		sub.Close() // before reporting, so a test that checks for leaks after get() sees it
		out <- result{status: rec.Code, header: rec.Header(), body: rec.Body.String(), err: err}
	}()
	return out
}

func get(t *testing.T, ch <-chan result) result {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("subscriber did not finish")
		return result{}
	}
}

// settled returns the Group's stats once it holds nothing, waiting briefly:
// serve sends its result and only then closes its subscription, so a check
// made right after get can see a subscriber that is a moment from leaving.
// A real leak still fails, after the wait.
func settled(g *Group) Stats {
	deadline := time.Now().Add(2 * time.Second)
	for {
		st := g.Stats()
		if (st.Flights == 0 && st.RetainedBytes == 0) || time.Now().After(deadline) {
			return st
		}
		time.Sleep(time.Millisecond)
	}
}

func writeAll(t *testing.T, w http.ResponseWriter, parts ...string) {
	t.Helper()
	for _, p := range parts {
		if _, err := io.WriteString(w, p); err != nil {
			t.Fatalf("leader write: %v", err)
		}
		w.(http.Flusher).Flush()
	}
}

func TestSubscribersGetTheLeadersExactResponse(t *testing.T) {
	g := testGroup(config.Coalescing{})
	f := lead(t, g, "k")
	leaderRec := httptest.NewRecorder()
	w := f.Writer(leaderRec)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("X-Accel-Buffering", "no")

	// 50 identical requests: one led the flight, 49 subscribe at different times.
	var results []<-chan result
	join49 := func(n int) {
		for range n {
			results = append(results, serve(join(t, g, "k"), t.Context()))
		}
	}
	join49(10) // before any header
	w.WriteHeader(http.StatusOK)
	writeAll(t, w, "data: 1\n\n")
	join49(20) // after the first event
	writeAll(t, w, "data: 2\n\n", "data: 3\n\n")
	join49(19) // mid-stream
	writeAll(t, w, "data: [DONE]\n\n")
	f.Finish(true, false)

	want := "data: 1\n\ndata: 2\n\ndata: 3\n\ndata: [DONE]\n\n"
	if got := leaderRec.Body.String(); got != want {
		t.Fatalf("leader got %q", got)
	}
	if len(results) != 49 {
		t.Fatalf("%d subscribers", len(results))
	}
	for i, ch := range results {
		r := get(t, ch)
		if r.err != nil || r.status != 200 || r.body != want {
			t.Fatalf("subscriber %d: status=%d err=%v body=%q", i, r.status, r.err, r.body)
		}
		if r.header.Get("Content-Type") != "text/event-stream" || r.header.Get("X-Accel-Buffering") != "no" {
			t.Fatalf("subscriber %d: headers %v", i, r.header)
		}
	}
	st := settled(g)
	if st.Led != 1 || st.Joined != 49 {
		t.Fatalf("stats %+v: want 1 led and 49 joined", st)
	}
	if st.Flights != 0 || st.RetainedBytes != 0 {
		t.Fatalf("after every subscriber left: %+v, want nothing held", st)
	}
	// The key is free again.
	if f2, _, _ := g.Acquire("k", true); f2 == nil {
		t.Fatal("a finished flight is still joinable")
	} else {
		f2.Finish(true, false)
	}
}

func TestLateJoinerGetsEventsSoFarThenLive(t *testing.T) {
	g := testGroup(config.Coalescing{})
	f := lead(t, g, "k")
	w := f.Writer(httptest.NewRecorder())
	w.WriteHeader(http.StatusOK)
	writeAll(t, w, "a", "b", "c")
	ch := serve(join(t, g, "k"), t.Context())
	time.Sleep(20 * time.Millisecond) // the joiner has replayed and now waits
	writeAll(t, w, "d", "e")
	f.Finish(true, false)
	if r := get(t, ch); r.err != nil || r.body != "abcde" {
		t.Fatalf("got %q err=%v, want the replay then the live chunks", r.body, r.err)
	}
}

func TestNonStreamingBodyIsSharedWhole(t *testing.T) {
	g := testGroup(config.Coalescing{})
	f := lead(t, g, "k")
	ch := serve(join(t, g, "k"), t.Context()) // joins before any byte exists
	w := f.Writer(httptest.NewRecorder())
	w.Header().Set("Content-Type", "application/json")
	body := strings.Repeat(`{"x":1}`, 1700) // 11.9 KB: several chunks, under the bound
	if _, err := io.WriteString(w, body); err != nil {
		t.Fatal(err)
	}
	f.Finish(true, false)
	if r := get(t, ch); r.err != nil || r.body != body || r.header.Get("Content-Type") != "application/json" {
		t.Fatalf("err=%v len=%d, want the whole body", r.err, len(r.body))
	}
}

func TestErrorResponsesAreSharedToo(t *testing.T) {
	g := testGroup(config.Coalescing{})
	f := lead(t, g, "k")
	ch := serve(join(t, g, "k"), t.Context())
	w := f.Writer(httptest.NewRecorder())
	w.Header().Set("Retry-After", "2")
	w.WriteHeader(http.StatusTooManyRequests)
	_, _ = io.WriteString(w, `{"error":{"code":"queue_full"}}`)
	f.Finish(true, false)
	r := get(t, ch)
	if r.status != 429 || r.header.Get("Retry-After") != "2" || !strings.Contains(r.body, "queue_full") {
		t.Fatalf("got %d %v %q: a refusal is shared fate", r.status, r.header, r.body)
	}
}

func TestUnjoinableFlightsAreNotShared(t *testing.T) {
	g := testGroup(config.Coalescing{})
	a, _, _ := g.Acquire("k", false)
	b, sub, _ := g.Acquire("k", false)
	if a == nil || b == nil || sub != nil {
		t.Fatal("a cache-only flight must never take subscribers")
	}
	// A joinable flight is not joined by a request that does not want to share.
	c := lead(t, g, "k")
	d, sub, _ := g.Acquire("k", false)
	if d == nil || sub != nil {
		t.Fatal("a request that is not joinable must lead its own flight")
	}
	for _, f := range []*Flight{a, b, c, d} {
		f.Finish(true, false)
	}
}

func TestFlightsAreBounded(t *testing.T) {
	g := testGroup(config.Coalescing{MaxFlights: 2})
	a, _, _ := g.Acquire("a", true)
	b, _, _ := g.Acquire("b", false)
	if f, s, reason := g.Acquire("c", true); f != nil || s != nil || reason != telemetry.ReasonFlightsFull {
		t.Fatalf("third flight: f=%v s=%v reason=%q, want flights_full", f, s, reason)
	}
	// Joining an existing flight costs no flight.
	if s := join(t, g, "a"); s == nil {
		t.Fatal("join failed")
	} else {
		s.Close()
	}
	a.Finish(true, false)
	if f, _, reason := g.Acquire("c", true); f == nil {
		t.Fatalf("a finished flight must free its slot: %q", reason)
	} else {
		f.Finish(true, false)
	}
	b.Finish(true, false)
	if n := g.Stats().Flights; n != 0 {
		t.Fatalf("%d flights held after all finished", n)
	}
}

func TestSubscribersAreBoundedAndTheOverflowStartsANewFlight(t *testing.T) {
	g := testGroup(config.Coalescing{MaxSubscribers: 2})
	f := lead(t, g, "k")
	s1, s2 := join(t, g, "k"), join(t, g, "k")
	f2, sub, _ := g.Acquire("k", true)
	if f2 == nil || sub != nil {
		t.Fatal("a full flight must give the next request a flight of its own")
	}
	// The new flight now takes the joiners.
	s3 := join(t, g, "k")
	if s3.f != f2 {
		t.Fatal("later joiners must find the newer flight")
	}
	for _, s := range []*Subscription{s1, s2, s3} {
		s.Close()
	}
	f.Finish(true, false)
	f2.Finish(true, false)
	if st := g.Stats(); st.Flights != 0 || st.RetainedBytes != 0 {
		t.Fatalf("leaked: %+v", st)
	}
}

func TestFinishKeepReturnsTheWholeResponse(t *testing.T) {
	g := testGroup(config.Coalescing{})
	f := lead(t, g, "k")
	w := f.Writer(httptest.NewRecorder())
	w.Header().Set("Content-Type", "application/json")
	body := strings.Repeat("0123456789", 1500)
	_, _ = io.WriteString(w, body)
	rec, ok := f.Finish(true, true)
	if !ok || rec.Status != 200 || string(rec.Body) != body || rec.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("ok=%v status=%d len=%d", ok, rec.Status, len(rec.Body))
	}
	// Not for an aborted exchange, or when not asked.
	for name, run := range map[string]func(*Flight) bool{
		"incomplete": func(f *Flight) bool { _, ok := f.Finish(false, true); return ok },
		"not asked":  func(f *Flight) bool { _, ok := f.Finish(true, false); return ok },
	} {
		f := lead(t, g, name)
		w := f.Writer(httptest.NewRecorder())
		_, _ = io.WriteString(w, "x")
		if run(f) {
			t.Errorf("%s: Finish returned a response", name)
		}
	}
	// Not when no response was written.
	f = lead(t, g, "silent")
	if _, ok := f.Finish(true, true); ok {
		t.Error("a flight with no response returned one")
	}
}

func TestAbortedLeaderEndsSubscribersWithAnError(t *testing.T) {
	g := testGroup(config.Coalescing{})
	// Before any header.
	f := lead(t, g, "k")
	ch := serve(join(t, g, "k"), t.Context())
	f.Finish(false, false)
	if r := get(t, ch); !errors.Is(r.err, ErrFlightFailed) || r.body != "" {
		t.Fatalf("before headers: err=%v body=%q", r.err, r.body)
	}
	// After part of the response.
	f = lead(t, g, "k")
	w := f.Writer(httptest.NewRecorder())
	w.WriteHeader(http.StatusOK)
	writeAll(t, w, "part")
	ch = serve(join(t, g, "k"), t.Context())
	time.Sleep(10 * time.Millisecond)
	f.Finish(false, false)
	if r := get(t, ch); !errors.Is(r.err, ErrFlightFailed) || r.body != "part" {
		t.Fatalf("mid-response: err=%v body=%q, want the bytes written, then the error", r.err, r.body)
	}
}

// A response that failed after it started, with its failure written into
// it, reaches every subscriber whole, ending in ErrResponseFailed; nothing
// is recorded for the cache.
func TestFailedResponseReachesSubscribersWhole(t *testing.T) {
	g := testGroup(config.Coalescing{})
	f := lead(t, g, "k")
	w := f.Writer(httptest.NewRecorder())
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	writeAll(t, w, "data: {}\n\n")
	early := serve(join(t, g, "k"), t.Context())
	writeAll(t, w, `data: {"error":{}}`+"\n\n")
	late := serve(join(t, g, "k"), t.Context()) // attached once join returns
	f.FinishFailed()
	want := "data: {}\n\n" + `data: {"error":{}}` + "\n\n"
	for name, ch := range map[string]<-chan result{"joined early": early, "joined late": late} {
		if r := get(t, ch); !errors.Is(r.err, ErrResponseFailed) || r.body != want {
			t.Errorf("%s: err=%v body=%q, want all of it, then ErrResponseFailed", name, r.err, r.body)
		}
	}
	if g.Join("k") != nil {
		t.Error("a finished flight took a joiner")
	}
}

// gatedWriter is a client that reads only as fast as the test allows.
type gatedWriter struct {
	hdr  http.Header
	gate chan struct{} // one receive per Write
	got  bytes.Buffer
	mu   sync.Mutex
}

func newGated() *gatedWriter {
	return &gatedWriter{hdr: http.Header{}, gate: make(chan struct{}, 1<<20)}
}

func (g *gatedWriter) Header() http.Header { return g.hdr }
func (g *gatedWriter) WriteHeader(int)     {}
func (g *gatedWriter) Write(p []byte) (int, error) {
	<-g.gate
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.got.Write(p)
}
func (g *gatedWriter) Flush() {}

// waitCaughtUp blocks until sub has read everything written so far.
func waitCaughtUp(t *testing.T, sub *Subscription) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		sub.f.mu.Lock()
		caught := sub.pos >= sub.f.head
		sub.f.mu.Unlock()
		if caught {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the fast reader never caught up")
		}
		time.Sleep(20 * time.Microsecond)
	}
}

// AC-M4-5 (replay side): a response far over the bound never makes the
// flight hold more than the bound plus one piece, a reader that stalls is
// evicted, and the leader is never slowed by it.
func TestReplayBufferIsBoundedAndASlowReaderIsEvicted(t *testing.T) {
	const max = MinReplayBytes
	g := testGroup(config.Coalescing{MaxReplayBytes: max})
	f := lead(t, g, "k")
	w := f.Writer(httptest.NewRecorder())
	w.WriteHeader(http.StatusOK)

	stalled := newGated() // never opened: its first Write blocks
	sub := join(t, g, "k")
	served := make(chan error, 1)
	go func() { served <- sub.Serve(t.Context(), stalled, nil) }()

	// A fast reader that keeps up.
	fastSub := join(t, g, "k")
	fast := serve(fastSub, t.Context())

	piece := strings.Repeat("x", 1000)
	var peak int64
	start := time.Now()
	for range 400 { // 400 KB, 25 times the bound
		_, _ = io.WriteString(w, piece)
		if r := g.Stats().RetainedBytes; r > peak {
			peak = r
		}
		// The fast reader keeps pace; the stalled one cannot. (The leader is
		// not waiting for either: it only lets the reader's goroutine run so
		// the test does not depend on scheduling.)
		waitCaughtUp(t, fastSub)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("the leader took %v: a stalled reader must not slow it", d)
	}
	if limit := int64(max + chunkMax); peak > limit {
		t.Fatalf("retained peaked at %d bytes, over the bound %d plus one piece", peak, limit)
	}
	st := g.Stats()
	if st.Sealed != 1 || st.Evicted != 1 {
		t.Fatalf("stats %+v: want the flight sealed and the stalled reader evicted", st)
	}
	f.Finish(true, true)

	// The stalled client unblocks; its Serve ends with the eviction.
	close(stalled.gate)
	select {
	case err := <-served:
		if !errors.Is(err, ErrEvicted) {
			t.Fatalf("stalled reader ended with %v, want ErrEvicted", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the evicted reader never ended")
	}
	sub.Close()
	if r := get(t, fast); r.err != nil || len(r.body) != 400*1000 {
		t.Fatalf("the fast reader got %d bytes, err=%v: it must get the whole response", len(r.body), r.err)
	}
	if st := settled(g); st.Flights != 0 || st.RetainedBytes != 0 {
		t.Fatalf("leaked after everyone left: %+v", st)
	}
}

// A subscriber evicted before it was sent anything can still be served as an
// ordinary request: the handler tells by Started.
func TestEvictedBeforeAnyByteIsNotStarted(t *testing.T) {
	g := testGroup(config.Coalescing{MaxReplayBytes: MinReplayBytes})
	f := lead(t, g, "k")
	w := f.Writer(httptest.NewRecorder())
	sub := join(t, g, "k")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, strings.Repeat("z", 4*MinReplayBytes)) // one write, over the bound, before the subscriber reads
	err := sub.Serve(t.Context(), httptest.NewRecorder(), nil)
	if !errors.Is(err, ErrEvicted) || sub.Started() {
		t.Fatalf("err=%v started=%v, want ErrEvicted and not started", err, sub.Started())
	}
	sub.Close()
	f.Finish(true, false)

	// A subscriber that was sent part of the response is started.
	g = testGroup(config.Coalescing{MaxReplayBytes: MinReplayBytes})
	f = lead(t, g, "k")
	w = f.Writer(httptest.NewRecorder())
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "head")
	sub = join(t, g, "k")
	stalled := newGated()
	done := make(chan error, 1)
	go func() { done <- sub.Serve(t.Context(), stalled, nil) }()
	time.Sleep(20 * time.Millisecond) // it has the headers and is blocked writing "head"
	_, _ = io.WriteString(w, strings.Repeat("z", 4*MinReplayBytes))
	close(stalled.gate)
	if err := <-done; !errors.Is(err, ErrEvicted) || !sub.Started() {
		t.Fatalf("err=%v started=%v, want ErrEvicted after the headers", err, sub.Started())
	}
	sub.Close()
	f.Finish(true, false)
}

func TestASealedFlightIsReplacedForNewRequests(t *testing.T) {
	g := testGroup(config.Coalescing{MaxReplayBytes: MinReplayBytes})
	f := lead(t, g, "k")
	w := f.Writer(httptest.NewRecorder())
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, strings.Repeat("y", 3*MinReplayBytes))
	if g.Stats().Sealed != 1 {
		t.Fatal("the flight should have sealed")
	}
	// It cannot give a late joiner the start, so a new request leads a flight of its own.
	f2, sub, _ := g.Acquire("k", true)
	if f2 == nil || sub != nil {
		t.Fatalf("a sealed flight must not take subscribers (f=%v sub=%v)", f2, sub)
	}
	// And a sealed response is not handed to the cache.
	if _, ok := f.Finish(true, true); ok {
		t.Fatal("a response over the bound was kept")
	}
	f2.Finish(true, false)
}

func TestLeaderLeavingDoesNotInterruptSubscribers(t *testing.T) {
	g := testGroup(config.Coalescing{})
	f := lead(t, g, "k")
	parent, leave := context.WithCancel(t.Context())
	up := f.Context(parent)
	w := f.Writer(httptest.NewRecorder())
	w.WriteHeader(http.StatusOK)
	subs := make([]<-chan result, 49)
	for i := range subs {
		subs[i] = serve(join(t, g, "k"), t.Context())
	}
	writeAll(t, w, "one")

	leave() // the first caller disconnects
	time.Sleep(30 * time.Millisecond)
	if up.Err() != nil {
		t.Fatal("the upstream call was cancelled while 49 subscribers remain")
	}
	writeAll(t, w, "two", "three")
	f.Finish(true, false)
	for i, ch := range subs {
		if r := get(t, ch); r.err != nil || r.body != "onetwothree" {
			t.Fatalf("subscriber %d: err=%v body=%q", i, r.err, r.body)
		}
	}
	if c := g.Stats().Cancelled; c != 0 {
		t.Fatalf("cancelled %d", c)
	}
}

func TestUpstreamIsCancelledOnlyWhenTheLastClientLeaves(t *testing.T) {
	g := testGroup(config.Coalescing{})
	f := lead(t, g, "k")
	parent, leave := context.WithCancel(t.Context())
	up := f.Context(parent)
	s1, s2 := join(t, g, "k"), join(t, g, "k")

	s1.Close() // a subscriber leaves: the leader and s2 remain
	if up.Err() != nil {
		t.Fatal("cancelled with the leader and a subscriber still here")
	}
	leave() // now the leader leaves: s2 remains
	time.Sleep(30 * time.Millisecond)
	if up.Err() != nil {
		t.Fatal("cancelled with a subscriber still here")
	}
	s2.Close() // the last one
	select {
	case <-up.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("not cancelled after the last client left")
	}
	if !errors.Is(context.Cause(up), errNoSubscribers) {
		t.Fatalf("cause %v", context.Cause(up))
	}
	if g.Stats().Cancelled != 1 {
		t.Fatalf("stats %+v", g.Stats())
	}
	// A request arriving now is not attached to the dying flight.
	f2, sub, _ := g.Acquire("k", true)
	if f2 == nil || sub != nil {
		t.Fatalf("attached to a cancelled flight (f=%v sub=%v)", f2, sub)
	}
	f2.Finish(true, false)
	f.Finish(false, false)
	if st := g.Stats(); st.Flights != 0 || st.RetainedBytes != 0 {
		t.Fatalf("leaked: %+v", st)
	}
}

func TestLastSubscriberLeavingFirstThenLeader(t *testing.T) {
	g := testGroup(config.Coalescing{})
	f := lead(t, g, "k")
	parent, leave := context.WithCancel(t.Context())
	up := f.Context(parent)
	s := join(t, g, "k")
	s.Close()
	if up.Err() != nil {
		t.Fatal("cancelled while the leader's client is still here")
	}
	leave()
	select {
	case <-up.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("not cancelled after everyone left")
	}
	f.Finish(false, false)
}

func TestNormalCompletionIsNotACancellation(t *testing.T) {
	g := testGroup(config.Coalescing{})
	f := lead(t, g, "k")
	parent, finish := context.WithCancel(t.Context())
	up := f.Context(parent)
	w := f.Writer(httptest.NewRecorder())
	_, _ = io.WriteString(w, "ok")
	f.Finish(true, false)
	finish() // net/http cancels the request context when the handler returns
	time.Sleep(20 * time.Millisecond)
	if st := g.Stats(); st.Cancelled != 0 {
		t.Fatalf("a finished request was counted as a cancellation: %+v", st)
	}
	_ = up
}

// failingWriter is a leader's client that goes away after n bytes.
type failingWriter struct {
	hdr  http.Header
	left int
}

func (f *failingWriter) Header() http.Header { return f.hdr }
func (f *failingWriter) WriteHeader(int)     {}
func (f *failingWriter) Write(p []byte) (int, error) {
	if f.left < len(p) {
		return 0, errors.New("broken pipe")
	}
	f.left -= len(p)
	return len(p), nil
}
func (f *failingWriter) Flush() {}

func TestLeaderClientFailureKeepsTheFlightWhileSubscribersRemain(t *testing.T) {
	g := testGroup(config.Coalescing{})
	f := lead(t, g, "k")
	up := f.Context(t.Context())
	cl := &failingWriter{hdr: http.Header{}, left: 3}
	w := f.Writer(cl)
	ch := serve(join(t, g, "k"), t.Context())
	w.WriteHeader(http.StatusOK)
	if _, err := io.WriteString(w, "abc"); err != nil {
		t.Fatal(err)
	}
	// The client breaks; a subscriber remains, so the handler is told nothing.
	if n, err := io.WriteString(w, "def"); err != nil || n != 3 {
		t.Fatalf("write = %d, %v: the exchange must go on for the subscriber", n, err)
	}
	if !w.ClientFailed() {
		t.Fatal("ClientFailed not set")
	}
	if up.Err() != nil {
		t.Fatal("cancelled with a subscriber remaining")
	}
	f.Finish(true, false)
	if r := get(t, ch); r.err != nil || r.body != "abcdef" {
		t.Fatalf("subscriber got %q err=%v", r.body, r.err)
	}
}

func TestLeaderClientFailureWithNobodyElseEndsTheExchange(t *testing.T) {
	g := testGroup(config.Coalescing{})
	f := lead(t, g, "k")
	up := f.Context(t.Context())
	w := f.Writer(&failingWriter{hdr: http.Header{}, left: 0})
	w.WriteHeader(http.StatusOK)
	if _, err := io.WriteString(w, "abc"); err == nil {
		t.Fatal("the failed write must reach the handler when nobody else is attached")
	}
	select {
	case <-up.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("upstream not cancelled")
	}
	f.Finish(false, false)
}

// A request is never attached to a flight whose upstream call is being
// cancelled: whenever Acquire hands out a subscription, the call stays alive
// until that subscriber closes.
func TestNeverAttachedToACancellingFlight(t *testing.T) {
	g := testGroup(config.Coalescing{MaxFlights: 1 << 10, MaxSubscribers: 1 << 10})
	for iter := range 300 {
		key := fmt.Sprintf("k%d", iter)
		f := lead(t, g, key)
		parent, leave := context.WithCancel(t.Context())
		up := f.Context(parent)
		w := f.Writer(httptest.NewRecorder())
		w.WriteHeader(http.StatusOK)

		var wg sync.WaitGroup
		bad := make(chan string, 64)
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 20 {
					fl, sub, _ := g.Acquire(key, true)
					if sub == nil {
						if fl != nil {
							fl.Finish(true, false)
						}
						return
					}
					// A joiner may attach to a replacement flight for the key;
					// only f's own subscribers are bound to f's call.
					own := sub.f == f
					if own && up.Err() != nil {
						bad <- "attached to a flight whose upstream call was already cancelled"
					}
					time.Sleep(time.Duration(iter%3) * 50 * time.Microsecond)
					if own && up.Err() != nil {
						bad <- "upstream cancelled while a subscriber was attached"
					}
					sub.Close()
				}
			}()
		}
		leave()
		wg.Wait()
		close(bad)
		for msg := range bad {
			t.Fatalf("iteration %d: %s", iter, msg)
		}
		f.Finish(false, false)
	}
	if st := g.Stats(); st.Flights != 0 || st.RetainedBytes != 0 {
		t.Fatalf("leaked: %+v", st)
	}
}

func TestWriterRecordsWhatTheHandlerSets(t *testing.T) {
	g := testGroup(config.Coalescing{})
	f := lead(t, g, "k")
	rec := httptest.NewRecorder()
	w := f.Writer(rec)
	w.Header().Set("X-Test", "1")
	w.WriteHeader(http.StatusEarlyHints) // informational: not the response
	if rec.Code != http.StatusEarlyHints && rec.Code != 200 {
		t.Fatalf("code %d", rec.Code)
	}
	ch := serve(join(t, g, "k"), t.Context())
	_, _ = io.WriteString(w, "body") // implicit 200
	f.Finish(true, false)
	r := get(t, ch)
	if r.status != 200 || r.body != "body" || r.header.Get("X-Test") != "1" {
		t.Fatalf("got %d %v %q", r.status, r.header, r.body)
	}
	// A second WriteHeader is ignored, as net/http does.
	f2 := lead(t, g, "k2")
	w2 := f2.Writer(httptest.NewRecorder())
	w2.WriteHeader(http.StatusAccepted)
	w2.WriteHeader(http.StatusInternalServerError)
	ch = serve(join(t, g, "k2"), t.Context())
	f2.Finish(true, false)
	if r := get(t, ch); r.status != http.StatusAccepted {
		t.Fatalf("status %d", r.status)
	}
}

func TestSubscriberClientGoneEndsServe(t *testing.T) {
	g := testGroup(config.Coalescing{})
	f := lead(t, g, "k")
	w := f.Writer(httptest.NewRecorder())
	w.WriteHeader(http.StatusOK)
	ctx, gone := context.WithCancel(t.Context())
	ch := serve(join(t, g, "k"), ctx)
	time.Sleep(10 * time.Millisecond)
	gone()
	if r := get(t, ch); !errors.Is(r.err, context.Canceled) {
		t.Fatalf("err = %v", r.err)
	}
	f.Finish(true, false)
	if st := settled(g); st.Flights != 0 {
		t.Fatalf("leaked: %+v", st)
	}
}

func TestSubscriberWriteErrorEndsServe(t *testing.T) {
	g := testGroup(config.Coalescing{})
	f := lead(t, g, "k")
	w := f.Writer(httptest.NewRecorder())
	w.WriteHeader(http.StatusOK)
	writeAll(t, w, "data")
	sub := join(t, g, "k")
	err := sub.Serve(t.Context(), &failingWriter{hdr: http.Header{}}, nil)
	if err == nil || errors.Is(err, ErrEvicted) {
		t.Fatalf("err = %v, want the write error", err)
	}
	sub.Close()
	f.Finish(true, false)
}

func TestObserveSeesEveryByte(t *testing.T) {
	g := testGroup(config.Coalescing{})
	f := lead(t, g, "k")
	w := f.Writer(httptest.NewRecorder())
	w.WriteHeader(http.StatusOK)
	writeAll(t, w, "ab", "cd")
	f.Finish(true, false)
	// done and drained before the subscriber arrives is impossible (the flight
	// is closed), so take a subscription first.
	g2 := testGroup(config.Coalescing{})
	f2 := lead(t, g2, "k")
	w2 := f2.Writer(httptest.NewRecorder())
	w2.WriteHeader(http.StatusOK)
	writeAll(t, w2, "ab", "cd")
	sub := join(t, g2, "k")
	f2.Finish(true, false)
	var seen bytes.Buffer
	if err := sub.Serve(t.Context(), httptest.NewRecorder(), func(b []byte) { seen.Write(b) }); err != nil {
		t.Fatal(err)
	}
	sub.Close()
	if seen.String() != "abcd" {
		t.Fatalf("observed %q", seen.String())
	}
}
