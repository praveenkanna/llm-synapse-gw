package ingress

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/dispatch"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/mockllm"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/health"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/loadview"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

// In-flight accounting (M1-10): every lease a route hands out is released
// exactly once, on the replica it names, whatever way the request ends. Each
// test runs under every routing policy and checks, once the handler has
// returned, that leases acquired == leases released per replica and that the
// load view's in-flight count is 0 on every replica.

// routingPolicy builds the router a leaseGateway runs with.
type routingPolicy struct {
	name string
	// affinity selects the AffinityRouter (with a load view); otherwise
	// round-robin, which tracks no load of its own.
	affinity bool
	// ring routes through the consistent-hash ring with an X-Session-Id key
	// chosen so the first pick is r0. Without it the affinity router has no
	// health tracker and fails open to round-robin, still tracking load.
	ring bool
}

var routingPolicies = []routingPolicy{
	{name: "round_robin"},
	{name: "affinity_ring", affinity: true, ring: true},
	{name: "affinity_failopen", affinity: true},
}

// leaseCounter wraps a Router and counts the leases it hands out and gets
// back, per replica. It is also the dispatcher's passive health: it records
// every failure reported and every first-chunk wait started, and passes
// them on to health (nil: nothing behind it, as with round-robin).
type leaseCounter struct {
	inner  router.Router
	health dispatch.Health

	mu       sync.Mutex
	acquired map[string]int
	released map[string]int
	failures []string       // replicas reported to RecordTransportFailure, in order
	waits    map[string]int // first-chunk waits started, per replica
}

func newLeaseCounter(inner router.Router, health dispatch.Health) *leaseCounter {
	return &leaseCounter{inner: inner, health: health, acquired: map[string]int{}, released: map[string]int{}, waits: map[string]int{}}
}

func (c *leaseCounter) Route(ctx context.Context, r *http.Request, pool string) (router.Lease, error) {
	l, err := c.inner.Route(ctx, r, pool)
	if err != nil {
		return l, err
	}
	return c.wrap(l), nil
}

func (c *leaseCounter) RouteOther(ctx context.Context, r *http.Request, pool, exclude string) (router.Lease, error) {
	l, err := c.inner.RouteOther(ctx, r, pool, exclude)
	if err != nil {
		return l, err
	}
	return c.wrap(l), nil
}

func (c *leaseCounter) RecordTransportFailure(replica discovery.Replica, err error) {
	c.mu.Lock()
	c.failures = append(c.failures, replica.Name)
	c.mu.Unlock()
	if c.health != nil {
		c.health.RecordTransportFailure(replica, err)
	}
}

func (c *leaseCounter) WatchFirstChunk(ctx context.Context, replica discovery.Replica) dispatch.FirstChunkWait {
	c.mu.Lock()
	c.waits[replica.Name]++
	c.mu.Unlock()
	if c.health == nil {
		return dispatch.NoFirstChunkWait
	}
	return c.health.WatchFirstChunk(ctx, replica)
}

// waitsStarted returns the first-chunk waits started on replica so far.
func (c *leaseCounter) waitsStarted(replica string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.waits[replica]
}

// reported returns the replicas reported to RecordTransportFailure so far.
func (c *leaseCounter) reported() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.failures)
}

func (c *leaseCounter) wrap(l router.Lease) router.Lease {
	c.mu.Lock()
	c.acquired[l.Replica.Name]++
	c.mu.Unlock()
	return router.NewLease(l.Replica, &countedRelease{c: c, inner: l})
}

// countedRelease counts a release, then gives back the inner router's slot.
type countedRelease struct {
	c     *leaseCounter
	inner router.Lease
}

func (cr *countedRelease) Release(rep discovery.Replica) {
	cr.c.mu.Lock()
	cr.c.released[rep.Name]++
	cr.c.mu.Unlock()
	cr.inner.Release()
}

// held returns the leases outstanding on replica.
func (c *leaseCounter) held(replica string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.acquired[replica] - c.released[replica]
}

func (c *leaseCounter) totals() (acquired, released int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, n := range c.acquired {
		acquired += n
	}
	for _, n := range c.released {
		released += n
	}
	return acquired, released
}

// leaseGateway is a poolGateway whose router counts leases. Every request's
// handler signals handled when it returns, however it ends, so the counts
// are checked only after all of the request's releases have run.
type leaseGateway struct {
	*poolGateway
	counter  *leaseCounter
	lv       *loadview.View         // nil for round-robin
	tracker  *health.Tracker        // nil unless the policy routes through the ring
	ar       *router.AffinityRouter // nil for round-robin
	replicas []string
	hdr      map[string]string // X-Session-Id for the ring policy
	handled  chan struct{}
}

func newLeaseGateway(t *testing.T, p routingPolicy, urls []string, body string) *leaseGateway {
	t.Helper()
	return newLeaseGatewayWith(t, p, urls, body, leaseOptions{})
}

// leaseOptions adjusts a leaseGateway.
type leaseOptions struct {
	yaml   string               // extra gateway configuration
	health func(*health.Config) // adjusts the ring policy's health tracker
	clock  *fakeClock           // the ring policy's tracker reads time from it
	// backoff waits out the connect-retry backoff (nil: a real timer).
	backoff func(ctx context.Context, d time.Duration) bool
	// wrap, when set, is a middleware inside the telemetry middleware.
	wrap func(http.Handler) http.Handler
	// tel records the requests (nil: a new one).
	tel *telemetry.Telemetry
}

func newLeaseGatewayWith(t *testing.T, p routingPolicy, urls []string, body string, o leaseOptions) *leaseGateway {
	t.Helper()
	cfg, disc := poolConfig(t, urls, o.yaml)
	rr := router.NewRoundRobin(disc, []string{"large"})
	g := &leaseGateway{handled: make(chan struct{}, 16), hdr: map[string]string{}}
	for i := range urls {
		g.replicas = append(g.replicas, fmt.Sprintf("r%d", i))
	}
	var inner router.Router = rr
	if p.affinity {
		g.lv = loadview.New(loadview.Config{LoadView: config.Defaults().Router.LoadView}, disc, nil, nil) // never started: in-flight only
		if p.ring {
			hc := health.ConfigFrom(config.Defaults().Router)
			if o.clock != nil {
				hc.Now = o.clock.Now
			}
			if o.health != nil {
				o.health(&hc)
			}
			g.tracker = health.New(hc, disc, nil, nil) // never started: passive only
		}
		ar := router.NewAffinityRouter(disc, rr, g.tracker, g.lv, nil)
		g.ar = ar
		if p.ring {
			g.hdr["X-Session-Id"] = sessionRoutedTo(t, ar, body, "r0")
		}
		inner = ar
	}
	g.counter = newLeaseCounter(inner, dispatch.TrackerHealth(g.tracker))
	done := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() { g.handled <- struct{}{} }()
			next.ServeHTTP(w, r)
		})
	}
	disp := newTestDispatcher(t, cfg, g.counter, g.counter, o.backoff)
	g.poolGateway = startPoolGateway(t, cfg, disc, disp, Options{}, serving{tel: o.tel, inner: o.wrap, outer: done})
	return g
}

// newTestDispatcher builds the dispatcher a test gateway sends with: rtr
// picks replicas, hl is its passive health (nil: none) and backoff waits out
// the connect-retry backoff (nil: a real timer).
func newTestDispatcher(t *testing.T, cfg *config.Config, rtr router.Router, hl dispatch.Health, backoff func(context.Context, time.Duration) bool) *dispatch.Dispatcher {
	t.Helper()
	client := upstream.New(cfg.Upstream)
	t.Cleanup(client.CloseIdleConnections)
	return dispatch.New(rtr, client, cfg.Upstream, dispatch.Options{Health: hl, BackoffWait: backoff})
}

// sessionRoutedTo returns an X-Session-Id whose ring pick for body is want.
func sessionRoutedTo(t *testing.T, ar *router.AffinityRouter, body, want string) string {
	t.Helper()
	for i := range 1000 {
		key := fmt.Sprintf("sess-%d", i)
		ctx := router.WithRequestBody(context.Background(), []byte(body))
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, PathChatCompletions, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-Session-Id", key)
		l, err := ar.Route(ctx, req, "large")
		if err != nil {
			t.Fatal(err)
		}
		got := l.Replica.Name
		l.Release()
		if got == want {
			return key
		}
	}
	t.Fatalf("no session key routes to %s", want)
	return ""
}

// headers returns the policy's routing headers plus extra.
func (g *leaseGateway) headers(extra map[string]string) map[string]string {
	h := make(map[string]string, len(g.hdr)+len(extra))
	for k, v := range g.hdr {
		h[k] = v
	}
	for k, v := range extra {
		h[k] = v
	}
	return h
}

// waitHandled waits until n more requests' handlers have returned.
func (g *leaseGateway) waitHandled(t *testing.T, n int) {
	t.Helper()
	for range n {
		select {
		case <-g.handled:
		case <-time.After(streamStepTimeout):
			t.Fatal("handler did not return")
		}
	}
}

// inFlight is the load view's count on replica (0 for round-robin).
func (g *leaseGateway) inFlight(replica string) int64 {
	if g.lv == nil {
		return 0
	}
	return g.lv.InFlight(replica)
}

// assertReleased checks that wantLeases leases were acquired and every one
// was released, that no replica has a request in flight and that no
// first-chunk wait is left registered.
func (g *leaseGateway) assertReleased(t *testing.T, wantLeases int) {
	t.Helper()
	if g.tracker != nil {
		if n := g.tracker.FirstChunkWaits(); n != 0 {
			t.Errorf("first-chunk waits still registered: %d, want 0", n)
		}
	}
	acquired, released := g.counter.totals()
	if acquired != wantLeases || released != acquired {
		t.Fatalf("leases acquired %d, released %d; want %d acquired, all released", acquired, released, wantLeases)
	}
	for _, rep := range g.replicas {
		if n := g.counter.held(rep); n != 0 {
			t.Errorf("replica %s: %d leases outstanding, want 0", rep, n)
		}
		if n := g.inFlight(rep); n != 0 {
			t.Errorf("replica %s: in-flight %d, want 0", rep, n)
		}
	}
}

// immediateBackoff skips the same-replica retry pause.
func immediateBackoff(ctx context.Context, _ time.Duration) bool { return ctx.Err() == nil }

func TestInFlightReleasedOnEveryExit(t *testing.T) {
	type urlsFn func(t *testing.T) []string
	live := func(t *testing.T) string { return startMock(t, nil).URL() }
	tests := []struct {
		name       string
		urls       urlsFn
		body       string
		hdr        map[string]string
		wantStatus int
		wantLeases int
	}{
		{name: "success", body: chatBody, wantStatus: http.StatusOK, wantLeases: 1,
			urls: func(t *testing.T) []string { return []string{live(t)} }},
		{name: "streaming success", body: streamBody, wantStatus: http.StatusOK, wantLeases: 1,
			urls: func(t *testing.T) []string { return []string{live(t), live(t)} }},
		{name: "connect failure on r0, retry succeeds on r1", body: chatBody, wantStatus: http.StatusOK, wantLeases: 2,
			urls: func(t *testing.T) []string { return []string{deadURL(t), live(t)} }},
		{name: "streaming connect retry on another replica", body: streamBody, wantStatus: http.StatusOK, wantLeases: 2,
			urls: func(t *testing.T) []string { return []string{deadURL(t), live(t)} }},
		{name: "retry on another replica fails too", body: chatBody, wantStatus: http.StatusBadGateway, wantLeases: 2,
			urls: func(t *testing.T) []string { return []string{deadURL(t), deadURL(t)} }},
		{name: "same-replica retry fails", body: chatBody, wantStatus: http.StatusBadGateway, wantLeases: 1,
			urls: func(t *testing.T) []string { return []string{deadURL(t)} }},
		{name: "upstream fails after the request is sent", body: chatBody, wantStatus: http.StatusBadGateway, wantLeases: 1,
			hdr:  map[string]string{mockllm.HeaderFault: "drop"},
			urls: func(t *testing.T) []string { return []string{live(t), live(t)} }},
		{name: "upstream fails mid-stream", body: streamBody, wantStatus: http.StatusOK, wantLeases: 1,
			hdr:  map[string]string{mockllm.HeaderFault: "drop"},
			urls: func(t *testing.T) []string { return []string{live(t)} }},
		{name: "non-streaming body cut after headers (aborted)", body: chatBody, wantStatus: http.StatusOK, wantLeases: 1,
			urls: func(t *testing.T) []string { return []string{truncatingServer(t, false).URL} }},
	}
	for _, p := range routingPolicies {
		for _, tc := range tests {
			t.Run(p.name+"/"+tc.name, func(t *testing.T) {
				g := newLeaseGatewayWith(t, p, tc.urls(t), tc.body, leaseOptions{backoff: immediateBackoff})
				hdr := g.headers(tc.hdr)
				hdr[mockllm.HeaderOutputTokens] = "3"
				resp, got, _ := g.do(t, tc.body, hdr)
				if resp.StatusCode != tc.wantStatus {
					t.Fatalf("status = %d, want %d (%s)", resp.StatusCode, tc.wantStatus, got)
				}
				g.waitHandled(t, 1)
				g.assertReleased(t, tc.wantLeases)
			})
		}
	}
}

// TestInFlightSameReplicaRetry: in a one-replica pool the retry keeps the
// replica's slot through the backoff (ADR 0014), and the count still ends
// at 0.
func TestInFlightSameReplicaRetry(t *testing.T) {
	for _, p := range routingPolicies {
		t.Run(p.name, func(t *testing.T) {
			mock := startMock(t, nil)
			if err := mock.SetRefusing(true); err != nil {
				t.Fatal(err)
			}
			inBackoff, resume := make(chan struct{}), make(chan struct{})
			g := newLeaseGatewayWith(t, p, []string{mock.URL()}, streamBody, leaseOptions{
				backoff: func(ctx context.Context, _ time.Duration) bool {
					close(inBackoff)
					<-resume
					return ctx.Err() == nil
				},
			})
			status := make(chan int, 1)
			go func() {
				resp, _, _ := g.do(t, streamBody, g.headers(map[string]string{mockllm.HeaderOutputTokens: "3"}))
				status <- resp.StatusCode
			}()
			select {
			case <-inBackoff:
			case <-time.After(streamStepTimeout):
				t.Fatal("first connect failure never reached the backoff")
			}
			if n := g.counter.held("r0"); n != 1 {
				t.Errorf("leases held in the backoff = %d, want 1 (the slot is kept for the retry)", n)
			}
			if g.tracker != nil {
				if n := g.tracker.FirstChunkWaits(); n != 0 || g.counter.waitsStarted("r0") != 1 {
					t.Errorf("in the backoff: %d waits registered of %d started, want 0 of 1 (the failed attempt's wait ended)", n, g.counter.waitsStarted("r0"))
				}
			}
			if p.affinity {
				if n := g.inFlight("r0"); n != 1 {
					t.Errorf("in-flight in the backoff = %d, want 1", n)
				}
			}
			if err := mock.SetRefusing(false); err != nil {
				t.Fatal(err)
			}
			close(resume)
			if s := <-status; s != http.StatusOK {
				t.Fatalf("status = %d, want 200 after the retry", s)
			}
			g.waitHandled(t, 1)
			if s := g.outcomes(t); s.ConnectRetries != 1 {
				t.Fatalf("stats = %+v, want 1 retry", s)
			}
			g.assertReleased(t, 1)
		})
	}
}

// TestInFlightCallerLeavesDuringBackoff: no retry is sent and the held slot
// is released once.
func TestInFlightCallerLeavesDuringBackoff(t *testing.T) {
	for _, p := range routingPolicies {
		t.Run(p.name, func(t *testing.T) {
			mock := startMock(t, nil)
			if err := mock.SetRefusing(true); err != nil {
				t.Fatal(err)
			}
			inBackoff := make(chan struct{})
			g := newLeaseGatewayWith(t, p, []string{mock.URL()}, chatBody, leaseOptions{
				backoff: func(ctx context.Context, _ time.Duration) bool {
					close(inBackoff)
					<-ctx.Done() // only the caller leaving ends this backoff
					return false
				},
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.gw.URL+PathChatCompletions, strings.NewReader(chatBody))
			if err != nil {
				t.Fatal(err)
			}
			for k, v := range g.headers(nil) {
				req.Header.Set(k, v)
			}
			errc := make(chan error, 1)
			go func() {
				resp, err := g.gw.Client().Do(req)
				if err == nil {
					_ = resp.Body.Close()
				}
				errc <- err
			}()
			select {
			case <-inBackoff:
			case <-time.After(streamStepTimeout):
				t.Fatal("first connect failure never reached the backoff")
			}
			cancel()
			if err := <-errc; err == nil {
				t.Fatal("request succeeded, want it cancelled by the caller")
			}
			g.waitHandled(t, 1)
			if s := g.outcomes(t); s.ConnectRetries != 0 || s.ClientDisconnects != 1 {
				t.Fatalf("stats = %+v, want no retry and 1 disconnect", s)
			}
			g.assertReleased(t, 1)
		})
	}
}

// TestInFlightMidStreamClientDisconnect: a caller leaving mid-stream releases
// the stream's slot once.
func TestInFlightMidStreamClientDisconnect(t *testing.T) {
	for _, p := range routingPolicies {
		t.Run(p.name, func(t *testing.T) {
			mock := startMock(t, func(c *mockllm.Config) { c.TokensPerSecond = 10 }) // 1000 tokens: ~100 s
			g := newLeaseGateway(t, p, []string{mock.URL(), startMock(t, nil).URL()}, streamBody)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.gw.URL+PathChatCompletions, strings.NewReader(streamBody))
			if err != nil {
				t.Fatal(err)
			}
			for k, v := range g.headers(map[string]string{mockllm.HeaderOutputTokens: "1000"}) {
				req.Header.Set(k, v)
			}
			resp, err := g.gw.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			buf := make([]byte, 64)
			if _, err := resp.Body.Read(buf); err != nil {
				t.Fatalf("first event: %v", err)
			}
			if n := g.counter.held("r0"); n != 1 {
				t.Errorf("leases held mid-stream = %d, want 1", n)
			}
			cancel()
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			g.waitHandled(t, 1)
			if s := g.outcomes(t); s.ClientDisconnects != 1 {
				t.Fatalf("stats = %+v, want 1 client disconnect", s)
			}
			g.assertReleased(t, 1)
		})
	}
}
