package upstream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

var (
	addrA = netip.MustParseAddr("10.0.0.1")
	addrB = netip.MustParseAddr("10.0.0.2")
	local = netip.MustParseAddr("127.0.0.1")
)

// fakeClock is a settable clock safe for concurrent use.
type fakeClock struct{ ns atomic.Int64 }

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.ns.Store(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())
	return c
}

func (c *fakeClock) now() time.Time          { return time.Unix(0, c.ns.Load()) }
func (c *fakeClock) advance(d time.Duration) { c.ns.Add(int64(d)) }

// fakeLookup answers from a settable result and counts calls.
type fakeLookup struct {
	mu    sync.Mutex
	addrs []netip.Addr
	err   error
	delay time.Duration // honours ctx: returns ctx.Err() if it ends first
	calls atomic.Int64
	hosts []string
}

func (f *fakeLookup) set(addrs []netip.Addr, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.addrs, f.err = addrs, err
}

func (f *fakeLookup) setDelay(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.delay = d
}

func (f *fakeLookup) lookedUp() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.hosts...)
}

func (f *fakeLookup) lookup(ctx context.Context, host string) ([]netip.Addr, error) {
	f.calls.Add(1)
	f.mu.Lock()
	addrs, err, delay := f.addrs, f.err, f.delay
	f.hosts = append(f.hosts, host)
	f.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return addrs, err
}

func dnsConfig(mod func(*config.UpstreamDNS)) config.UpstreamDNS {
	d := config.Defaults().Upstream.DNS
	d.TTL = 30 * time.Second
	d.LookupTimeout = time.Second
	if mod != nil {
		mod(&d)
	}
	return d
}

// failOpenCounter records WithFailOpen calls.
type failOpenCounter struct{ n atomic.Int64 }

func (f *failOpenCounter) record(stage, reason string) {
	if stage == telemetry.StageUpstreamDNS && reason == ReasonLookupFailed {
		f.n.Add(1)
	}
}

func newTestResolver(fl *fakeLookup, fo *failOpenCounter, mod func(*config.UpstreamDNS)) (*resolver, *fakeClock) {
	var onFailOpen func(string, string)
	if fo != nil {
		onFailOpen = fo.record
	}
	r := newResolver(dnsConfig(mod), fl.lookup, onFailOpen)
	clk := newFakeClock()
	r.now = clk.now
	return r, clk
}

func mustResolve(t *testing.T, r *resolver, host string) []netip.Addr {
	t.Helper()
	addrs, err := r.resolve(context.Background(), host)
	if err != nil {
		t.Fatalf("resolve %s: %v", host, err)
	}
	return addrs
}

// waitIdle waits until host has no lookup in flight.
func waitIdle(t *testing.T, r *resolver, host string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		r.mu.Lock()
		e := r.hosts[host]
		busy := e != nil && e.call != nil
		r.mu.Unlock()
		if !busy {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("lookup of %s still in flight", host)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestResolverCachesUntilTTLThenRefreshesInBackground(t *testing.T) {
	fl := &fakeLookup{}
	fl.set([]netip.Addr{addrA}, nil)
	r, clk := newTestResolver(fl, nil, nil)

	for range 3 {
		if got := mustResolve(t, r, "replica"); got[0] != addrA {
			t.Fatalf("addrs = %v, want %v", got, addrA)
		}
	}
	if n := fl.calls.Load(); n != 1 {
		t.Fatalf("lookups within TTL = %d, want 1 (cached)", n)
	}

	fl.set([]netip.Addr{addrB}, nil)
	fl.setDelay(200 * time.Millisecond) // a slow DNS server must not delay dials
	clk.advance(29 * time.Second)
	if got := mustResolve(t, r, "replica"); got[0] != addrA || fl.calls.Load() != 1 {
		t.Fatalf("before TTL: addrs = %v, lookups %d; want cached %v, 1", got, fl.calls.Load(), addrA)
	}
	clk.advance(2 * time.Second)
	start := time.Now()
	if got := mustResolve(t, r, "replica"); got[0] != addrA {
		t.Fatalf("expired: addrs = %v, want %v served while refreshing", got, addrA)
	}
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Fatalf("expired resolve waited %s for the refresh, want stale-while-revalidate", d)
	}
	waitIdle(t, r, "replica")
	if got := mustResolve(t, r, "replica"); got[0] != addrB {
		t.Fatalf("after refresh: addrs = %v, want %v", got, addrB)
	}
	if n := fl.calls.Load(); n != 2 {
		t.Fatalf("lookups = %d, want 2 (one background refresh)", n)
	}
}

func TestResolverConcurrentColdDialsShareOneLookup(t *testing.T) {
	fl := &fakeLookup{}
	fl.set([]netip.Addr{addrA}, nil)
	fl.setDelay(50 * time.Millisecond)
	r, _ := newTestResolver(fl, nil, nil)

	var wg sync.WaitGroup
	errs := make(chan error, 100)
	for range 100 {
		wg.Go(func() {
			addrs, err := r.resolve(context.Background(), "replica")
			if err == nil && (len(addrs) != 1 || addrs[0] != addrA) {
				err = fmt.Errorf("addrs = %v", addrs)
			}
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if n := fl.calls.Load(); n != 1 {
		t.Fatalf("lookups for 100 concurrent cold dials = %d, want 1", n)
	}
}

func TestResolverTemporaryFailureUsesStaleEntry(t *testing.T) {
	tests := []struct {
		name  string
		err   error
		delay time.Duration
	}{
		{name: "server error", err: &net.DNSError{Err: "server misbehaving", Name: "replica", IsTemporary: true}},
		{name: "plain error", err: errors.New("boom")},
		{name: "lookup timeout", delay: 5 * time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fl := &fakeLookup{}
			fl.set([]netip.Addr{addrA}, nil)
			fo := &failOpenCounter{}
			r, clk := newTestResolver(fl, fo, func(d *config.UpstreamDNS) { d.LookupTimeout = 50 * time.Millisecond })
			mustResolve(t, r, "replica")

			fl.set(nil, tc.err)
			fl.setDelay(tc.delay)
			clk.advance(31 * time.Second)
			if got := mustResolve(t, r, "replica"); got[0] != addrA {
				t.Fatalf("addrs = %v, want stale %v", got, addrA)
			}
			waitIdle(t, r, "replica")
			if n := fo.n.Load(); n != 1 {
				t.Fatalf("fail-open count = %d, want 1", n)
			}
			// The stale entry is reused without hammering DNS until the retry floor.
			for range 5 {
				if got := mustResolve(t, r, "replica"); got[0] != addrA {
					t.Fatalf("addrs = %v, want stale %v", got, addrA)
				}
			}
			if n := fl.calls.Load(); n != 2 {
				t.Fatalf("lookups = %d, want 2 (no retry before %s)", n, minRefreshInterval)
			}
			fl.set([]netip.Addr{addrB}, nil)
			fl.setDelay(0)
			clk.advance(minRefreshInterval)
			mustResolve(t, r, "replica") // starts the retry
			waitIdle(t, r, "replica")
			if got := mustResolve(t, r, "replica"); got[0] != addrB {
				t.Fatalf("after recovery: addrs = %v, want %v", got, addrB)
			}
			if n := fo.n.Load(); n != 1 {
				t.Fatalf("fail-open count = %d after a good lookup, want still 1", n)
			}
		})
	}
}

// TestResolverNotFoundAfterSuccessDropsAddresses: a replica removed from DNS
// must not be dialled at its old address (which may now be someone else).
func TestResolverNotFoundAfterSuccessDropsAddresses(t *testing.T) {
	fl := &fakeLookup{}
	fl.set([]netip.Addr{addrA}, nil)
	fo := &failOpenCounter{}
	r, clk := newTestResolver(fl, fo, nil)
	mustResolve(t, r, "replica")

	fl.set(nil, &net.DNSError{Err: "no such host", Name: "replica", IsNotFound: true})
	clk.advance(31 * time.Second)
	mustResolve(t, r, "replica") // stale while the refresh runs
	waitIdle(t, r, "replica")

	for range 3 {
		_, err := r.resolve(context.Background(), "replica")
		var de *net.DNSError
		if !errors.As(err, &de) || !de.IsNotFound {
			t.Fatalf("err = %v, want the cached not-found answer", err)
		}
	}
	if n := fl.calls.Load(); n != 2 {
		t.Fatalf("lookups = %d, want 2 (not-found cached for %s)", n, minRefreshInterval)
	}
	if fo.n.Load() != 0 {
		t.Fatal("not-found counted as a fail-open: the stale address must not be used")
	}
	fl.set([]netip.Addr{addrB}, nil)
	clk.advance(minRefreshInterval)
	if got := mustResolve(t, r, "replica"); got[0] != addrB {
		t.Fatalf("after the host is back: addrs = %v, want %v", got, addrB)
	}
}

func TestResolverStaleAgeExpiry(t *testing.T) {
	fl := &fakeLookup{}
	fl.set([]netip.Addr{addrA}, nil)
	fo := &failOpenCounter{}
	r, clk := newTestResolver(fl, fo, func(d *config.UpstreamDNS) { d.MaxStale = time.Minute })
	mustResolve(t, r, "replica")

	fl.set(nil, &net.DNSError{Err: "i/o timeout", Name: "replica", IsTimeout: true})
	clk.advance(30*time.Second + 50*time.Second) // 50s past TTL: within max_stale
	if got := mustResolve(t, r, "replica"); got[0] != addrA {
		t.Fatalf("within max_stale: addrs = %v, want stale %v", got, addrA)
	}
	waitIdle(t, r, "replica")
	clk.advance(11 * time.Second) // 61s past TTL
	_, err := r.resolve(context.Background(), "replica")
	var de *net.DNSError
	if !errors.As(err, &de) || !de.IsTimeout {
		t.Fatalf("past max_stale: err = %v, want the lookup's failure (stale addresses dropped)", err)
	}
}

func TestResolverColdFailure(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		delay        time.Duration
		wantTimeout  bool
		wantNotFound bool // authoritative: cached for minRefreshInterval
	}{
		{name: "not found", err: &net.DNSError{Err: "no such host", Name: "replica", IsNotFound: true}, wantNotFound: true},
		{name: "empty answer", wantNotFound: true},
		{name: "plain error", err: errors.New("boom")},
		{name: "lookup timeout", delay: 5 * time.Second, wantTimeout: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fl := &fakeLookup{}
			fl.set(nil, tc.err)
			fl.setDelay(tc.delay)
			fo := &failOpenCounter{}
			r, _ := newTestResolver(fl, fo, func(d *config.UpstreamDNS) { d.LookupTimeout = 50 * time.Millisecond })
			for range 2 {
				_, err := r.resolve(context.Background(), "replica")
				var de *net.DNSError
				if !errors.As(err, &de) {
					t.Fatalf("err = %v (%T), want *net.DNSError", err, err)
				}
				if de.IsTimeout != tc.wantTimeout || de.IsNotFound != tc.wantNotFound {
					t.Fatalf("IsTimeout = %v, IsNotFound = %v; want %v, %v", de.IsTimeout, de.IsNotFound, tc.wantTimeout, tc.wantNotFound)
				}
			}
			wantLookups, wantCached := int64(2), 0 // temporary: not cached, looked up again
			if tc.wantNotFound {
				wantLookups, wantCached = 1, 1
			}
			if n := fl.calls.Load(); n != wantLookups {
				t.Fatalf("lookups = %d, want %d", n, wantLookups)
			}
			if n := r.len(); n != wantCached {
				t.Fatalf("cached hosts = %d, want %d", n, wantCached)
			}
			if fo.n.Load() != 0 {
				t.Fatal("cold failure counted as fail-open: there was nothing to fall back on")
			}
		})
	}
}

// TestResolverWaiterHonoursContext: a dial that gives up does not cancel
// the shared lookup, which still fills the cache for the next dial.
func TestResolverWaiterHonoursContext(t *testing.T) {
	fl := &fakeLookup{}
	fl.set([]netip.Addr{addrA}, nil)
	fl.setDelay(100 * time.Millisecond)
	r, _ := newTestResolver(fl, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := r.resolve(ctx, "replica"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the caller's context error", err)
	}
	waitIdle(t, r, "replica")
	if got := mustResolve(t, r, "replica"); got[0] != addrA {
		t.Fatalf("addrs = %v, want %v from the detached lookup", got, addrA)
	}
	if n := fl.calls.Load(); n != 1 {
		t.Fatalf("lookups = %d, want 1 (cached by the detached lookup)", n)
	}
}

func TestResolverCeiling(t *testing.T) {
	fl := &fakeLookup{}
	fl.set([]netip.Addr{addrA}, nil)
	r, clk := newTestResolver(fl, nil, func(d *config.UpstreamDNS) { d.MaxHosts = 4 })

	mustResolve(t, r, "keep")
	for i := range 1000 {
		clk.advance(time.Millisecond)
		mustResolve(t, r, fmt.Sprintf("host-%d", i))
		mustResolve(t, r, "keep") // most recently used: never the eviction victim
		if n := r.len(); n > 4 {
			t.Fatalf("cached hosts = %d after %d inserts, want <= 4", n, i+1)
		}
	}
	before := fl.calls.Load()
	mustResolve(t, r, "keep")
	if fl.calls.Load() != before {
		t.Fatal("recently used host was evicted")
	}
}

func TestResolverCeilingFullOfInflightLookupsStillResolves(t *testing.T) {
	fl := &fakeLookup{}
	fl.set([]netip.Addr{addrA}, nil)
	fl.setDelay(200 * time.Millisecond)
	r, _ := newTestResolver(fl, nil, func(d *config.UpstreamDNS) { d.MaxHosts = 1 })

	done := make(chan error, 1)
	go func() {
		_, err := r.resolve(context.Background(), "slow")
		done <- err
	}()
	for r.len() == 0 {
		time.Sleep(time.Millisecond)
	}
	if got := mustResolve(t, r, "other"); got[0] != addrA {
		t.Fatalf("addrs = %v", got)
	}
	if n := r.len(); n != 1 {
		t.Fatalf("cached hosts = %d, want 1 (ceiling)", n)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestResolverMarkFailedWaitsForFreshLookup(t *testing.T) {
	fl := &fakeLookup{}
	fl.set([]netip.Addr{addrA}, nil)
	r, clk := newTestResolver(fl, nil, nil)
	mustResolve(t, r, "replica")

	fl.set([]netip.Addr{addrB}, nil) // the replica moved
	r.markFailed("replica")
	if got := mustResolve(t, r, "replica"); got[0] != addrA || fl.calls.Load() != 1 {
		t.Fatalf("addrs = %v, lookups %d; want %v and no lookup before the retry floor", got, fl.calls.Load(), addrA)
	}
	clk.advance(minRefreshInterval)
	if got := mustResolve(t, r, "replica"); got[0] != addrB {
		t.Fatalf("addrs = %v, want the fresh %v (a suspect address is not reused)", got, addrB)
	}
	r.markFailed("unknown") // no entry: no-op
}

// TestResolverMarkFailedDuringRefresh exercises markFailed, resolve and
// prewarm racing with in-flight lookups (run with -race).
func TestResolverMarkFailedDuringRefresh(t *testing.T) {
	fl := &fakeLookup{}
	fl.set([]netip.Addr{addrA}, nil)
	fl.setDelay(time.Millisecond)
	r, clk := newTestResolver(fl, nil, func(d *config.UpstreamDNS) { d.TTL = time.Second })
	mustResolve(t, r, "replica")

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 200 {
				switch (g + i) % 4 {
				case 0:
					r.markFailed("replica")
				case 1:
					clk.advance(100 * time.Millisecond)
				case 2:
					r.prewarm("replica")
				default:
					if _, err := r.resolve(context.Background(), "replica"); err != nil {
						t.Error(err)
						return
					}
				}
			}
		})
	}
	wg.Wait()
	waitIdle(t, r, "replica")
	r.mu.Lock()
	e := r.hosts["replica"]
	r.mu.Unlock()
	if e == nil || len(e.addrs) != 1 || e.addrs[0] != addrA {
		t.Fatalf("entry after the race = %+v, want addrs [%v]", e, addrA)
	}
}

func TestResolverPrewarmPools(t *testing.T) {
	fl := &fakeLookup{}
	fl.set([]netip.Addr{addrA}, nil)
	fl.setDelay(200 * time.Millisecond)
	r, _ := newTestResolver(fl, nil, nil)
	pools := []config.Pool{
		{Name: "large", Replicas: []config.Replica{{URL: "http://vllm-large:8000"}, {URL: "http://10.1.2.3:8000"}}},
		{Name: "small", Replicas: []config.Replica{{URL: "http://[::1]:8000"}, {URL: "http://vllm-small:8000"}, {URL: "://bad"}}},
	}
	start := time.Now()
	r.prewarmPools(pools)
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Fatalf("prewarm blocked for %s, want it never to wait on DNS", d)
	}
	waitIdle(t, r, "vllm-large")
	waitIdle(t, r, "vllm-small")
	if hosts := fl.lookedUp(); len(hosts) != 2 {
		t.Fatalf("looked up %q, want only the two host names", hosts)
	}
	start = time.Now()
	mustResolve(t, r, "vllm-large")
	mustResolve(t, r, "vllm-small")
	if d := time.Since(start); d > 100*time.Millisecond || fl.calls.Load() != 2 {
		t.Fatalf("resolve after prewarm took %s with %d lookups, want cached", d, fl.calls.Load())
	}
	r.prewarmPools(pools) // fresh: no new lookups
	if n := fl.calls.Load(); n != 2 {
		t.Fatalf("lookups = %d after prewarming fresh hosts, want 2", n)
	}
}

// clientFor returns a Client whose replica host names resolve through fl.
func clientFor(fl *fakeLookup, fo *failOpenCounter, mod func(*config.Upstream)) *Client {
	opts := []Option{withLookup(fl.lookup)}
	if fo != nil {
		opts = append(opts, WithFailOpen(fo.record))
	}
	return New(testUpstream(mod), opts...)
}

// namedReplica points a replica at srv through host name "replica.test".
func namedReplica(t *testing.T, srv *httptest.Server) discovery.Replica {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	u.Host = net.JoinHostPort("replica.test", u.Port())
	return discovery.Replica{Pool: "large", Name: "replica-0", BaseURL: u}
}

func okServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func doGet(c *Client, replica discovery.Replica) error {
	resp, err := c.Do(context.Background(), replica, Request{Method: http.MethodGet, Path: "/v1/models"})
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, err = io.Copy(io.Discard, resp.Body)
	return err
}

// TestDoSlowResolverDoesNotEatConnectTimeout is the M0-75 failure: a lookup
// slower than the connect timeout used to fail the dial.
func TestDoSlowResolverDoesNotEatConnectTimeout(t *testing.T) {
	srv := okServer(t)
	fl := &fakeLookup{}
	fl.set([]netip.Addr{local}, nil)
	fl.setDelay(300 * time.Millisecond)
	c := clientFor(fl, nil, func(u *config.Upstream) {
		u.Timeouts.Connect = 100 * time.Millisecond
		u.DNS.LookupTimeout = 2 * time.Second
	})
	defer c.CloseIdleConnections()
	if err := doGet(c, namedReplica(t, srv)); err != nil {
		t.Fatalf("Do with a 300ms lookup and a 100ms connect timeout: %v", err)
	}
	if hosts := fl.lookedUp(); len(hosts) != 1 || hosts[0] != "replica.test" {
		t.Fatalf("looked up %q, want the replica host once", hosts)
	}
}

func TestDoFailingResolverUsesStaleEntry(t *testing.T) {
	srv := okServer(t)
	fl := &fakeLookup{}
	fl.set([]netip.Addr{local}, nil)
	fo := &failOpenCounter{}
	c := clientFor(fl, fo, func(u *config.Upstream) { u.DNS.TTL = time.Millisecond })
	defer c.CloseIdleConnections()
	replica := namedReplica(t, srv)
	if err := doGet(c, replica); err != nil {
		t.Fatal(err)
	}

	fl.set(nil, &net.DNSError{Err: "i/o timeout", Name: "replica.test", IsTimeout: true})
	for range 2 {
		c.CloseIdleConnections() // force a new dial
		time.Sleep(5 * time.Millisecond)
		if err := doGet(c, replica); err != nil {
			t.Fatalf("Do after a failed refresh: %v, want the stale address used", err)
		}
		waitIdle(t, c.res, "replica.test")
	}
	if fl.calls.Load() < 2 || fo.n.Load() < 1 {
		t.Fatalf("lookups = %d, fail-opens = %d; want >= 2 and >= 1", fl.calls.Load(), fo.n.Load())
	}
}

func TestDoUnresolvableReplicaIsRetryableDNSKind(t *testing.T) {
	fl := &fakeLookup{}
	fl.set(nil, &net.DNSError{Err: "no such host", Name: "replica.test", IsNotFound: true})
	c := clientFor(fl, nil, nil)
	defer c.CloseIdleConnections()
	err := doGet(c, namedReplica(t, okServer(t)))
	if KindOf(err) != KindDNS {
		t.Fatalf("kind = %s (%v), want %s", KindOf(err), err, KindDNS)
	}
	if !Retryable(err) {
		t.Fatalf("Retryable(%v) = false: nothing reached a replica", err)
	}
}

func TestDoIPLiteralSkipsResolver(t *testing.T) {
	srv := okServer(t) // http://127.0.0.1:port
	fl := &fakeLookup{}
	fl.set(nil, errors.New("must not be called"))
	c := clientFor(fl, nil, nil)
	defer c.CloseIdleConnections()
	if err := doGet(c, replicaFor(t, srv.URL)); err != nil {
		t.Fatal(err)
	}
	if n := fl.calls.Load(); n != 0 {
		t.Fatalf("lookups for an IP-literal replica = %d, want 0", n)
	}
}

func TestDoFailedDialReresolves(t *testing.T) {
	srv := okServer(t) // listens on 127.0.0.1 only
	// The first answer is an address where nothing listens on srv's port.
	fl := &fakeLookup{}
	fl.set([]netip.Addr{netip.MustParseAddr("::1")}, nil)
	c := clientFor(fl, nil, nil)
	defer c.CloseIdleConnections()
	replica := namedReplica(t, srv)
	if err := doGet(c, replica); KindOf(err) != KindConnect {
		t.Fatalf("first Do: kind %s (%v), want connect", KindOf(err), err)
	}
	fl.set([]netip.Addr{local}, nil)
	time.Sleep(minRefreshInterval)
	if err := doGet(c, replica); err != nil {
		t.Fatalf("Do after the replica moved: %v", err)
	}
	if n := fl.calls.Load(); n != 2 {
		t.Fatalf("lookups = %d, want 2 (a failed dial re-resolves before the TTL)", n)
	}
}
