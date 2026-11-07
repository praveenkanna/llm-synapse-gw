package upstream

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"net/url"
	"sync"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// Fail-open reason for gateway_failopen_total{stage="upstream_dns",reason}: a
// replica host's lookup failed and its last known addresses were used.
const (
	ReasonLookupFailed = "lookup_failed"
)

const (
	// minRefreshInterval is the shortest gap between lookups of one host
	// after a failed lookup or a failed dial, and how long an authoritative
	// "no such host" answer is cached, so a DNS or replica outage costs at
	// most one lookup per host per interval.
	minRefreshInterval  = time.Second
	errNoAddressesFound = "no addresses found"
)

// lookupFunc resolves a host name to IP addresses.
type lookupFunc func(ctx context.Context, host string) ([]netip.Addr, error)

func systemLookup(ctx context.Context, host string) ([]netip.Addr, error) {
	return net.DefaultResolver.LookupNetIP(ctx, "ip", host)
}

// resolver caches replica host addresses so name resolution happens outside
// the TCP connect timeout and, once a host is known, off the request path.
//
//   - Fresh addresses (younger than ttl) are used as they are.
//   - Expired addresses are still used at once while one background lookup
//     refreshes them (stale-while-revalidate). Only a host with no known
//     address makes a dial wait, for one lookup shared by every waiting dial
//     and bounded by its own timeout.
//   - A temporary lookup failure (timeout, server error) keeps the known
//     addresses in use (fail open, counted), but never longer than maxStale
//     past their ttl.
//   - An authoritative "no such host" (or empty) answer drops the known
//     addresses: the replica is gone, and dialling its old address could
//     reach whatever reuses it. The answer is cached for minRefreshInterval.
//   - A failed dial marks the addresses suspect: the next dial (no sooner
//     than minRefreshInterval after the last lookup) waits for a fresh
//     lookup instead of reusing them, in case the replica moved.
//
// The cache holds at most maxHosts hosts (least recently used evicted).
// Lookups run on goroutines owned by the resolver that end within the lookup
// timeout. Safe for concurrent use.
type resolver struct {
	lookup   lookupFunc
	ttl      time.Duration
	maxStale time.Duration
	timeout  time.Duration
	maxHosts int
	now      func() time.Time
	// onFailOpen is called once per failed lookup that left stale addresses
	// in use. It runs on the lookup goroutine, so it must be concurrency-safe.
	onFailOpen func(stage, reason string)

	mu    sync.Mutex
	hosts map[string]*hostEntry
}

type hostEntry struct {
	addrs      []netip.Addr // last good answer; nil if none usable
	resolvedAt time.Time    // when addrs were looked up
	refreshAt  time.Time    // a dial at or after this starts a refresh
	lookedUp   time.Time    // when the last lookup (good or not) finished
	used       time.Time    // last use, for eviction
	suspect    bool         // a dial to addrs failed: the next due dial waits for a lookup
	notFound   error        // cached "no such host" answer, until notFoundAt
	notFoundAt time.Time    // when notFound expires
	call       *lookupCall  // in-flight lookup, or nil
}

// lookupCall is one lookup shared by every dial waiting on it.
type lookupCall struct {
	done  chan struct{} // closed when addrs and err are set
	addrs []netip.Addr
	err   error
}

func newResolver(cfg config.UpstreamDNS, lookup lookupFunc, onFailOpen func(stage, reason string)) *resolver {
	return &resolver{
		lookup:     lookup,
		ttl:        cfg.TTL,
		maxStale:   cfg.MaxStale,
		timeout:    cfg.LookupTimeout,
		maxHosts:   cfg.MaxHosts,
		now:        time.Now,
		onFailOpen: onFailOpen,
		hosts:      make(map[string]*hostEntry),
	}
}

// resolve returns host's addresses. It waits for a lookup only when host
// has no usable address, or when a dial to its addresses failed and a
// refresh is due. Errors are *net.DNSError, or ctx's error if ctx ends while
// waiting.
func (r *resolver) resolve(ctx context.Context, host string) ([]netip.Addr, error) {
	r.mu.Lock()
	now := r.now()
	e := r.hosts[host]
	if e == nil {
		if e = r.insertLocked(host); e == nil {
			r.mu.Unlock()
			return r.lookupUncached(ctx, host)
		}
	}
	e.used = now
	if e.notFound != nil && now.Before(e.notFoundAt) {
		err := e.notFound
		r.mu.Unlock()
		return nil, err
	}
	r.dropTooStaleLocked(e, now)
	if e.addrs != nil && (now.Before(e.refreshAt) || !e.suspect) {
		addrs := e.addrs
		if !now.Before(e.refreshAt) {
			r.startRefreshLocked(host, e) // stale-while-revalidate
		}
		r.mu.Unlock()
		return addrs, nil
	}
	// No usable address, or a dial to the known ones failed: wait for the
	// lookup (which falls back to the known addresses if it fails).
	c := r.startRefreshLocked(host, e)
	r.mu.Unlock()

	select {
	case <-c.done:
		return c.addrs, c.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// prewarm starts a background lookup for host unless its addresses are
// fresh or a lookup is in flight. It never blocks on DNS.
func (r *resolver) prewarm(host string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	e := r.hosts[host]
	if e == nil {
		if e = r.insertLocked(host); e == nil {
			return
		}
		e.used = now
	}
	if e.notFound != nil && now.Before(e.notFoundAt) {
		return
	}
	r.dropTooStaleLocked(e, now)
	if e.addrs == nil || !now.Before(e.refreshAt) {
		r.startRefreshLocked(host, e)
	}
}

// dropTooStaleLocked forgets addresses more than maxStale past their ttl.
func (r *resolver) dropTooStaleLocked(e *hostEntry, now time.Time) {
	if e.addrs != nil && now.Sub(e.resolvedAt) > r.ttl+r.maxStale {
		e.addrs = nil
	}
}

// startRefreshLocked returns host's in-flight lookup, starting one if none.
func (r *resolver) startRefreshLocked(host string, e *hostEntry) *lookupCall {
	if e.call == nil {
		e.call = &lookupCall{done: make(chan struct{})}
		go r.refresh(host, e, e.call) // ends within r.timeout
	}
	return e.call
}

// insertLocked adds an empty entry for host, evicting the least recently
// used idle entry when the cache is full. It returns nil if every entry has
// a lookup in flight (the caller then resolves without caching).
func (r *resolver) insertLocked(host string) *hostEntry {
	if len(r.hosts) >= r.maxHosts {
		var victim string
		var oldest time.Time
		for h, e := range r.hosts {
			if e.call == nil && (victim == "" || e.used.Before(oldest)) {
				victim, oldest = h, e.used
			}
		}
		if victim == "" {
			return nil
		}
		delete(r.hosts, victim)
	}
	e := &hostEntry{}
	r.hosts[host] = e
	return e
}

// refresh runs one lookup for e and publishes the result to c's waiters.
func (r *resolver) refresh(host string, e *hostEntry, c *lookupCall) {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	addrs, err := r.lookupAddrs(ctx, host)
	cancel()

	r.mu.Lock()
	now := r.now()
	e.lookedUp = now
	staleUsed := false
	switch {
	case err == nil:
		e.addrs, e.resolvedAt, e.notFound = addrs, now, nil
		e.refreshAt = now.Add(r.ttl)
	case isNotFound(err):
		// Authoritative: the host is gone. Never dial its old address.
		e.addrs, e.notFound, e.notFoundAt = nil, err, now.Add(minRefreshInterval)
	default:
		r.dropTooStaleLocked(e, now)
		if e.addrs != nil {
			staleUsed = true
			addrs, err = e.addrs, nil // waiters (after a failed dial) get the stale answer
			e.refreshAt = now.Add(min(r.ttl, minRefreshInterval))
		} else if r.hosts[host] == e {
			// Nothing to fall back on and nothing authoritative to cache:
			// free the slot; the next dial looks up again.
			delete(r.hosts, host)
		}
	}
	e.call = nil
	e.suspect = false
	c.addrs, c.err = addrs, err
	r.mu.Unlock()
	close(c.done)

	if staleUsed && r.onFailOpen != nil {
		r.onFailOpen(telemetry.StageUpstreamDNS, ReasonLookupFailed)
	}
}

func isNotFound(err error) bool {
	var de *net.DNSError
	return errors.As(err, &de) && de.IsNotFound
}

// lookupUncached resolves host without the cache (only when the cache is
// full of in-flight lookups), still within the lookup timeout.
func (r *resolver) lookupUncached(ctx context.Context, host string) ([]netip.Addr, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	return r.lookupAddrs(ctx, host)
}

// lookupAddrs calls the lookup function and normalises its failures to
// *net.DNSError, so callers can tell a failed lookup from a failed connect.
// An empty answer counts as "no such host".
func (r *resolver) lookupAddrs(ctx context.Context, host string) ([]netip.Addr, error) {
	addrs, err := r.lookup(ctx, host)
	if err == nil && len(addrs) == 0 {
		err = &net.DNSError{Err: errNoAddressesFound, Name: host, IsNotFound: true}
	}
	if err == nil {
		return addrs, nil
	}
	var de *net.DNSError
	if errors.As(err, &de) {
		return nil, err
	}
	return nil, &net.DNSError{
		Err:       err.Error(),
		Name:      host,
		IsTimeout: errors.Is(err, context.DeadlineExceeded),
	}
}

// markFailed marks host's addresses suspect after a failed dial: the next
// dial (no sooner than minRefreshInterval after the last lookup) waits for a
// fresh lookup instead of reusing them, in case the replica moved.
func (r *resolver) markFailed(host string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.hosts[host]
	if e == nil {
		return
	}
	e.suspect = true
	if at := e.lookedUp.Add(minRefreshInterval); at.Before(e.refreshAt) {
		e.refreshAt = at
	}
}

// len reports the number of cached hosts (for tests).
func (r *resolver) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.hosts)
}

// prewarmPools starts background lookups for every replica host name in
// pools (IP literals and unparsable URLs are skipped).
func (r *resolver) prewarmPools(pools []config.Pool) {
	for _, p := range pools {
		for _, rep := range p.Replicas {
			u, err := url.Parse(rep.URL)
			if err != nil {
				continue
			}
			if host := u.Hostname(); host != "" && !isIPLiteral(host) {
				r.prewarm(host)
			}
		}
	}
}

func isIPLiteral(host string) bool {
	_, err := netip.ParseAddr(host)
	return err == nil
}

// dialFn is net.Dialer.DialContext's shape.
type dialFn func(ctx context.Context, network, address string) (net.Conn, error)

// dialFunc returns a DialContext that resolves host names through r, then
// connects to the resolved addresses within one connect timeout (see
// dialAddrs). IP-literal hosts skip the resolver.
func (r *resolver) dialFunc(dial dialFn, connect time.Duration) dialFn {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil || isIPLiteral(host) {
			return dial(ctx, network, address) // a bad address is reported by dial
		}
		addrs, err := r.resolve(ctx, host)
		if err != nil {
			return nil, &net.OpError{Op: "dial", Net: network, Err: err}
		}
		conn, err := dialAddrs(ctx, dial, connect, network, addrs, port)
		if err != nil && ctx.Err() == nil {
			r.markFailed(host)
		}
		return conn, err
	}
}
