package dispatch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry/telemetrytest"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/testnet"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

// stepTimeout bounds every wait for something that must happen.
const stepTimeout = 5 * time.Second

const streamBody = "data: hi\n\ndata: [DONE]\n\n"

// pool is a dispatcher over replicas r0, r1, … at urls, with a router that
// counts leases and a health that records reports and waits.
type pool struct {
	cfg    *config.Config
	d      *Dispatcher
	router *countingRouter
	health *fakeHealth
}

// newPool builds a pool "large" over urls. yaml is extra gateway
// configuration; backoff, if set, waits out the connect-retry backoff.
func newPool(t *testing.T, urls []string, yaml string, backoff func(context.Context, time.Duration) bool) *pool {
	t.Helper()
	return newPoolWith(t, urls, yaml, backoff, nil)
}

// newPoolWith is newPool with hl as the dispatcher's health (nil: the
// recording fakeHealth).
func newPoolWith(t *testing.T, urls []string, yaml string, backoff func(context.Context, time.Duration) bool, hl Health) *pool {
	t.Helper()
	var reps strings.Builder
	for i, u := range urls {
		fmt.Fprintf(&reps, "      - {name: r%d, url: %s}\n", i, u)
	}
	cfg, err := config.Parse([]byte(fmt.Sprintf("pools:\n  - name: large\n    replicas:\n%sauth: {enabled: false}\n%s", reps.String(), yaml)), func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	disc, err := discovery.NewStatic(cfg.Pools)
	if err != nil {
		t.Fatal(err)
	}
	client := upstream.New(cfg.Upstream)
	t.Cleanup(client.CloseIdleConnections)
	p := &pool{cfg: cfg, router: newCountingRouter(router.NewRoundRobin(disc, []string{"large"})), health: &fakeHealth{}}
	if hl == nil {
		hl = p.health
	}
	p.d = New(p.router, client, cfg.Upstream, Options{Health: hl, BackoffWait: backoff})
	return p
}

// send dispatches a request to the pool for a caller with context ctx.
func (p *pool) send(ctx context.Context, stream bool) (*Exchange, error) {
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil).WithContext(ctx)
	return p.sendRequest(r, stream)
}

func (p *pool) sendRequest(r *http.Request, stream bool) (*Exchange, error) {
	return p.d.Send(r, "large", upstream.Request{
		Method: http.MethodPost, Path: "/v1/chat/completions", Header: http.Header{},
		Body: []byte(`{"model":"m"}`), Stream: stream,
	})
}

// sendCounted is send inside the telemetry middleware, as the gateway runs
// Send: what the request counted (retries, upstream errors, a caller that
// left) is in the returned metrics, the one ledger (AD-12).
func (p *pool) sendCounted(t *testing.T, stream bool) (*Exchange, *telemetry.Metrics, error) {
	t.Helper()
	tel := telemetry.New(telemetry.Options{})
	var ex *Exchange
	var err error
	tel.Middleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		ex, err = p.sendRequest(r, stream)
	})).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil))
	return ex, tel.Metrics(), err
}

// counts is what a request counted: connect retries, upstream errors and
// callers that left.
func counts(t *testing.T, m *telemetry.Metrics) (retries, upstreamErrors, disconnects float64) {
	t.Helper()
	return telemetrytest.Sum(t, m, telemetry.MetricRetries), telemetrytest.Sum(t, m, telemetry.MetricUpstreamErrors),
		telemetrytest.Sum(t, m, telemetry.MetricClientDisconnects)
}

// assertReleased checks that every lease taken was given back.
func (p *pool) assertReleased(t *testing.T, wantLeases int) {
	t.Helper()
	acquired, released := p.router.totals()
	if acquired != wantLeases || released != acquired {
		t.Fatalf("leases acquired %d, released %d; want %d acquired, all released", acquired, released, wantLeases)
	}
}

// countingRouter wraps a Router and counts the leases it hands out and gets back.
type countingRouter struct {
	inner              router.Router
	mu                 sync.Mutex
	acquired, released int
}

func newCountingRouter(inner router.Router) *countingRouter { return &countingRouter{inner: inner} }

func (c *countingRouter) Route(ctx context.Context, r *http.Request, pool string) (router.Lease, error) {
	l, err := c.inner.Route(ctx, r, pool)
	return c.wrap(l, err)
}

func (c *countingRouter) RouteOther(ctx context.Context, r *http.Request, pool, exclude string) (router.Lease, error) {
	l, err := c.inner.RouteOther(ctx, r, pool, exclude)
	return c.wrap(l, err)
}

func (c *countingRouter) wrap(l router.Lease, err error) (router.Lease, error) {
	if err != nil {
		return l, err
	}
	c.mu.Lock()
	c.acquired++
	c.mu.Unlock()
	return router.NewLease(l.Replica, c), nil
}

func (c *countingRouter) Release(discovery.Replica) {
	c.mu.Lock()
	c.released++
	c.mu.Unlock()
}

func (c *countingRouter) totals() (acquired, released int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.acquired, c.released
}

// fakeHealth records the replicas reported, the waits started and the
// contexts they were started with.
type fakeHealth struct {
	mu       sync.Mutex
	reported []string
	waits    []*fakeWait
	ctxs     []context.Context
}

func (h *fakeHealth) RecordTransportFailure(rep discovery.Replica, _ error) {
	h.mu.Lock()
	h.reported = append(h.reported, rep.Name)
	h.mu.Unlock()
}

func (h *fakeHealth) WatchFirstChunk(ctx context.Context, rep discovery.Replica) FirstChunkWait {
	w := &fakeWait{replica: rep.Name}
	h.mu.Lock()
	h.waits = append(h.waits, w)
	h.ctxs = append(h.ctxs, ctx)
	h.mu.Unlock()
	return w
}

func (h *fakeHealth) reports() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.reported)
}

func (h *fakeHealth) started() []*fakeWait {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.waits)
}

// fakeWait counts every Received and End call and keeps the stuck-eject hook.
type fakeWait struct {
	replica         string
	mu              sync.Mutex
	received, ended int
	hook            func()
}

func (w *fakeWait) Received() { w.mu.Lock(); w.received++; w.mu.Unlock() }
func (w *fakeWait) End()      { w.mu.Lock(); w.ended++; w.mu.Unlock() }
func (w *fakeWait) OnStuckEject(f func()) {
	w.mu.Lock()
	w.hook = f
	w.mu.Unlock()
}

func (w *fakeWait) counts() (received, ended int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.received, w.ended
}

func (w *fakeWait) stuckHook() func() {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.hook
}

// liveServer answers every request with status and body.
func liveServer(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(s.Close)
	return s
}

// immediate skips the same-replica retry pause.
func immediate(ctx context.Context, _ time.Duration) bool { return ctx.Err() == nil }

// readAll reads and closes an exchange's body through the Exchange.
func readAll(t *testing.T, ex *Exchange) string {
	t.Helper()
	defer ex.Close()
	b, err := io.ReadAll(ex.Response.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(b)
}

// TestSendRetriesOnceOnAnotherReplica: a refused connect is retried once on
// the pool's other replica, reported once against the refused one, and the
// request ends holding no lease.
func TestSendRetriesOnceOnAnotherReplica(t *testing.T) {
	live := liveServer(t, http.StatusOK, streamBody)
	p := newPool(t, []string{testnet.RefusedURL(t), live.URL}, "", func(context.Context, time.Duration) bool {
		t.Error("same-replica backoff used although the pool has another replica")
		return false
	})
	ex, m, err := p.sendCounted(t, true)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if ex.Replica.Name != "r1" || ex.Response.StatusCode != http.StatusOK {
		t.Fatalf("answered by %s with %d, want r1 with 200", ex.Replica.Name, ex.Response.StatusCode)
	}
	if got := readAll(t, ex); got != streamBody {
		t.Fatalf("body = %q", got)
	}
	if retries, errs, gone := counts(t, m); retries != 1 || errs != 1 || gone != 0 {
		t.Fatalf("counted %v retries, %v upstream errors, %v disconnects; want 1 retry and 1 upstream error", retries, errs, gone)
	}
	if got := p.health.reports(); !slices.Equal(got, []string{"r0"}) {
		t.Fatalf("reported = %v, want [r0]", got)
	}
	p.assertReleased(t, 2)
}

// TestSendSameReplicaRetryAfterBackoff: with no other replica the retry
// goes to the same one after the configured backoff, and one request is
// reported once however many of its attempts were refused.
func TestSendSameReplicaRetryAfterBackoff(t *testing.T) {
	var waited []time.Duration
	p := newPool(t, []string{testnet.RefusedURL(t)}, "upstream:\n  connect_retry_backoff: 300ms\n",
		func(ctx context.Context, d time.Duration) bool {
			waited = append(waited, d)
			return ctx.Err() == nil
		})
	_, m, err := p.sendCounted(t, false)
	var failed *Error
	if !errors.As(err, &failed) || failed.Replica.Name != "r0" || upstream.KindOf(failed.Err) != upstream.KindConnect {
		t.Fatalf("Send error = %v, want an *Error for a connect failure on r0", err)
	}
	if !slices.Equal(waited, []time.Duration{300 * time.Millisecond}) {
		t.Fatalf("backoffs = %v, want one of the configured 300ms", waited)
	}
	if retries, errs, _ := counts(t, m); retries != 1 || errs != 2 {
		t.Fatalf("counted %v retries, %v upstream errors; want 1 retry and 2 upstream errors", retries, errs)
	}
	if got := p.health.reports(); !slices.Equal(got, []string{"r0"}) {
		t.Fatalf("reported = %v, want [r0] once", got)
	}
	p.assertReleased(t, 1)
}

// TestSendCallerLeavesDuringBackoff: no retry is sent, the disconnect is
// counted, and the slot kept through the backoff is released once.
func TestSendCallerLeavesDuringBackoff(t *testing.T) {
	p := newPool(t, []string{testnet.RefusedURL(t)}, "", func(context.Context, time.Duration) bool { return false })
	_, m, err := p.sendCounted(t, false)
	if !errors.Is(err, ErrCallerGone) {
		t.Fatalf("Send error = %v, want ErrCallerGone", err)
	}
	if retries, errs, gone := counts(t, m); retries != 0 || gone != 1 || errs != 1 {
		t.Fatalf("counted %v retries, %v upstream errors, %v disconnects; want no retry, 1 disconnect, 1 upstream error", retries, errs, gone)
	}
	p.assertReleased(t, 1)
}

// TestSendNeverRetriesAfterTheRequestWasSent: a replica that drops the
// connection after reading the request is neither retried nor reported.
func TestSendNeverRetriesAfterTheRequestWasSent(t *testing.T) {
	drop := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		c, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = c.Close()
		}
	}))
	t.Cleanup(drop.Close)
	p := newPool(t, []string{drop.URL, liveServer(t, http.StatusOK, "{}").URL}, "", immediate)
	_, m, err := p.sendCounted(t, false)
	var failed *Error
	if !errors.As(err, &failed) || failed.Replica.Name != "r0" {
		t.Fatalf("Send error = %v, want an *Error on r0", err)
	}
	if retries, errs, _ := counts(t, m); retries != 0 || errs != 1 {
		t.Fatalf("counted %v retries, %v upstream errors; want no retry and 1 upstream error", retries, errs)
	}
	if got := p.health.reports(); len(got) != 0 {
		t.Fatalf("reported = %v, want nothing", got)
	}
	p.assertReleased(t, 1)
}

// TestSendNoReplicas: a pool without replicas takes no lease.
func TestSendNoReplicas(t *testing.T) {
	p := newPool(t, []string{testnet.RefusedURL(t)}, "", immediate)
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	if _, err := p.d.Send(r, "missing", upstream.Request{Method: http.MethodPost, Path: "/v1/chat/completions"}); !errors.Is(err, ErrNoReplicas) {
		t.Fatalf("Send error = %v, want ErrNoReplicas", err)
	}
	p.assertReleased(t, 0)
}

// TestExchangeWatchesStreamingAttempts: a streaming attempt's wait hears of
// the first body byte once and is ended by Close; a non-streaming attempt is
// not watched.
func TestExchangeWatchesStreamingAttempts(t *testing.T) {
	live := liveServer(t, http.StatusOK, streamBody)
	p := newPool(t, []string{live.URL}, "", immediate)

	ex, err := p.send(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	readAll(t, ex)
	if n := len(p.health.started()); n != 0 {
		t.Fatalf("waits started for a non-streaming request: %d", n)
	}

	ex, err = p.send(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	waits := p.health.started()
	if len(waits) != 1 {
		t.Fatalf("waits started = %d, want 1", len(waits))
	}
	if received, _ := waits[0].counts(); received != 0 {
		t.Fatalf("first chunk reported before any read: %d", received)
	}
	buf := make([]byte, 4)
	for {
		if _, err := ex.Response.Body.Read(buf); err != nil {
			break
		}
	}
	ex.Close()
	ex.Close() // idempotent
	if received, ended := waits[0].counts(); received != 1 || ended == 0 {
		t.Fatalf("wait told received %d, ended %d times; want received once, then ended", received, ended)
	}
	p.assertReleased(t, 2)
}

// TestExchangeErrorStatusIsNoFirstChunk: an error response ends the wait at
// once and its body never counts as a first chunk.
func TestExchangeErrorStatusIsNoFirstChunk(t *testing.T) {
	p := newPool(t, []string{liveServer(t, http.StatusServiceUnavailable, `{"error":{}}`).URL}, "", immediate)
	ex, err := p.send(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	waits := p.health.started()
	if received, ended := waits[0].counts(); received != 0 || ended != 1 {
		t.Fatalf("after headers: received %d, ended %d; want 0 and 1", received, ended)
	}
	readAll(t, ex)
	if received, _ := waits[0].counts(); received != 0 {
		t.Fatalf("an error body counted as a first chunk")
	}
	p.assertReleased(t, 1)
}

// TestStuckEjectCutsTheAttempt: the wait's stuck-eject hook cancels a
// streaming attempt still waiting for headers, which fails as stuck_engine,
// unreported (not a connect failure).
func TestStuckEjectCutsTheAttempt(t *testing.T) {
	arrived := make(chan struct{})
	hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(arrived)
		<-r.Context().Done()
	}))
	t.Cleanup(hang.Close)
	p := newPool(t, []string{hang.URL}, "", immediate)
	errc := make(chan error, 1)
	go func() {
		_, err := p.send(context.Background(), true)
		errc <- err
	}()
	select {
	case <-arrived:
	case <-time.After(stepTimeout):
		t.Fatal("request never reached the replica")
	}
	p.health.started()[0].stuckHook()()
	var err error
	select {
	case err = <-errc:
	case <-time.After(stepTimeout):
		t.Fatal("Send still waiting after the stuck ejection")
	}
	var failed *Error
	if !errors.As(err, &failed) || upstream.KindOf(failed.Err) != upstream.KindStuck {
		t.Fatalf("Send error = %v, want an *Error of kind %s", err, upstream.KindStuck)
	}
	if got := p.health.reports(); len(got) != 0 {
		t.Fatalf("reported = %v, want nothing", got)
	}
	p.assertReleased(t, 1)
}

// TestStaleStuckCutCannotReachTheRetry: a cut from the failed first attempt
// of a connect retry, arriving late, must not cancel the attempt on the
// other replica.
func TestStaleStuckCutCannotReachTheRetry(t *testing.T) {
	release, sawCancel := make(chan struct{}), make(chan struct{})
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body) // net/http watches for the caller leaving only once the body is read
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		select {
		case <-release:
			_, _ = io.WriteString(w, "data: ok\n\n")
		case <-r.Context().Done():
			close(sawCancel)
		}
	}))
	t.Cleanup(live.Close)
	p := newPool(t, []string{testnet.RefusedURL(t), live.URL}, "", immediate)
	ex, err := p.send(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	waits := p.health.started()
	if len(waits) != 2 || waits[0].replica != "r0" || waits[1].replica != "r1" {
		t.Fatalf("waits = %d, want one per attempt (r0, r1)", len(waits))
	}
	waits[0].stuckHook()() // the first attempt's cut, late
	select {
	case <-sawCancel:
		t.Fatal("the first attempt's stale cut cancelled the retry")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if got := readAll(t, ex); got != "data: ok\n\n" {
		t.Fatalf("body = %q, want the retried stream intact", got)
	}
	p.assertReleased(t, 2)
}

// panicHealth is passive health that panics when a wait starts, or when
// one ends.
type panicHealth struct{ onStart, onEnd bool }

func (panicHealth) RecordTransportFailure(discovery.Replica, error) {}

func (h panicHealth) WatchFirstChunk(context.Context, discovery.Replica) FirstChunkWait {
	if h.onStart {
		panic("health: start")
	}
	return panicWait{onEnd: h.onEnd}
}

type panicWait struct{ onEnd bool }

func (panicWait) Received()           {}
func (panicWait) OnStuckEject(func()) {}
func (w panicWait) End() {
	if w.onEnd {
		panic("health: end")
	}
}

// mustPanic runs f and fails unless it panics.
func mustPanic(t *testing.T, what string, f func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatalf("%s did not panic", what)
		}
	}()
	f()
}

// TestLeaseReleasedWhenHealthPanics: a panic in passive health, when a
// wait starts or ends, still gives back the replica's lease, on every path
// it can hit: Send, an error response, and Close.
func TestLeaseReleasedWhenHealthPanics(t *testing.T) {
	ok := liveServer(t, http.StatusOK, streamBody)
	failing := liveServer(t, http.StatusServiceUnavailable, `{"error":{}}`)
	t.Run("wait start panics in Send", func(t *testing.T) {
		p := newPoolWith(t, []string{ok.URL}, "", immediate, panicHealth{onStart: true})
		mustPanic(t, "Send", func() { _, _ = p.send(context.Background(), true) })
		p.assertReleased(t, 1)
	})
	t.Run("wait end panics on an error response", func(t *testing.T) {
		p := newPoolWith(t, []string{failing.URL}, "", immediate, panicHealth{onEnd: true})
		mustPanic(t, "Send", func() { _, _ = p.send(context.Background(), true) })
		p.assertReleased(t, 1)
	})
	t.Run("wait end panics in Close", func(t *testing.T) {
		p := newPoolWith(t, []string{ok.URL}, "", immediate, panicHealth{onEnd: true})
		ex, err := p.send(context.Background(), true)
		if err != nil {
			t.Fatal(err)
		}
		mustPanic(t, "Close", ex.Close)
		p.assertReleased(t, 1)
	})
}

// countingWait counts Received calls.
type countingWait struct{ received, ended int }

func (w *countingWait) Received()           { w.received++ }
func (w *countingWait) End()                { w.ended++ }
func (w *countingWait) OnStuckEject(func()) {}

// chunkReader returns chunk left times, then io.EOF.
type chunkReader struct {
	chunk []byte
	left  int
}

func (c *chunkReader) Read(p []byte) (int, error) {
	if c.left == 0 {
		return 0, io.EOF
	}
	c.left--
	return copy(p, c.chunk), nil
}

func (c *chunkReader) Close() error { return nil }

// TestFirstChunkBodyTellsWaitOnceWithoutPerReadAllocation: the body tells
// the wait about the first chunk once, and reading through it allocates
// nothing per read.
func TestFirstChunkBodyTellsWaitOnceWithoutPerReadAllocation(t *testing.T) {
	const chunks = 1000
	src := &chunkReader{chunk: []byte(streamBody)}
	wait := &countingWait{}
	body := &firstChunkBody{ReadCloser: src, wait: wait}
	buf := make([]byte, 64)
	src.left = chunks
	for {
		if _, err := body.Read(buf); err != nil {
			break
		}
	}
	if wait.received != 1 || wait.ended != 0 {
		t.Fatalf("wait told received %d, ended %d times; want 1 and 0", wait.received, wait.ended)
	}
	allocs := testing.AllocsPerRun(20, func() {
		src.left = chunks
		for {
			if _, err := body.Read(buf); err != nil {
				return
			}
		}
	})
	if allocs > 0 {
		t.Fatalf("reading %d chunks made %.0f allocations; want none", chunks, allocs)
	}
}

// goneWithoutSignal is a context that is already cancelled but whose Done
// channel never closes, standing in for a cancellation that races the
// backoff timer.
type goneWithoutSignal struct{ context.Context }

func (goneWithoutSignal) Done() <-chan struct{} { return nil }
func (goneWithoutSignal) Err() error            { return context.Canceled }

// TestSleepCtx: the backoff never reports a completed wait for a caller that
// has left (ctx is checked again after the timer, and for a zero backoff).
func TestSleepCtx(t *testing.T) {
	live := context.Background()
	gone, cancel := context.WithCancel(context.Background())
	cancel()
	tests := []struct {
		name string
		ctx  context.Context
		d    time.Duration
		want bool
	}{
		{name: "elapsed", ctx: live, d: time.Millisecond, want: true},
		{name: "zero backoff", ctx: live, want: true},
		{name: "caller gone, zero backoff", ctx: gone},
		{name: "caller gone before the timer", ctx: gone, d: time.Hour},
		// Done never fires, so only the re-check after the timer sees the
		// caller gone: the timer and the cancellation landing together.
		{name: "caller gone when the timer fires", ctx: goneWithoutSignal{context.Background()}, d: time.Millisecond},
	}
	for _, tc := range tests {
		if got := sleepCtx(tc.ctx, tc.d); got != tc.want {
			t.Errorf("%s: sleepCtx = %v, want %v", tc.name, got, tc.want)
		}
	}
}

type requestMarker struct{}

// TestWatchGetsTheRequestContext: a wait is started with the caller's
// request context (what health records a full registry on), for the first
// attempt and for the retry.
func TestWatchGetsTheRequestContext(t *testing.T) {
	live := liveServer(t, http.StatusOK, streamBody)
	p := newPool(t, []string{testnet.RefusedURL(t), live.URL}, "", immediate)
	ctx := context.WithValue(context.Background(), requestMarker{}, "this request")
	ex, err := p.send(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	readAll(t, ex)
	p.health.mu.Lock()
	defer p.health.mu.Unlock()
	if len(p.health.ctxs) != 2 {
		t.Fatalf("waits started with %d contexts, want 2 (attempt and retry)", len(p.health.ctxs))
	}
	for i, c := range p.health.ctxs {
		if c.Value(requestMarker{}) != "this request" {
			t.Fatalf("wait %d started without the request's context", i)
		}
	}
}
