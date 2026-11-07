// Package discovery tells the request path which replicas serve each pool.
//
// The request path depends only on the Discovery interface, so the static
// list used under Docker Compose can later be replaced by DNS or Kubernetes
// endpoints without touching ingress or routing (ADR 0002). Lookups read an
// immutable snapshot through an atomic pointer: no locks, no I/O, never
// blocking a request.
package discovery

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/url"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
)

// Replica is one upstream server. Values are immutable once published.
type Replica struct {
	// Pool is the pool this replica belongs to.
	Pool string
	// Name is the replica's metric label (unique across pools).
	Name string
	// BaseURL is the replica's base URL (scheme and host, no path).
	BaseURL *url.URL
}

// Discovery returns the current replicas of a pool.
type Discovery interface {
	// Replicas returns the pool's replicas, or nil for an unknown pool.
	// Callers must not modify the returned slice.
	Replicas(pool string) []Replica
}

// Inventory is a Discovery that can also list every replica and says when
// the replica set changed. Background components (health tracking, the load
// view) use it to follow hot reloads: they wait on Changed, compare Version
// with the one they last applied and re-read the set only when it moved.
// The request path needs only Discovery.
//
// Every method is a non-blocking read of in-memory state (no I/O, no
// waiting on locks held across I/O): callers may hold their own locks
// while calling them.
type Inventory interface {
	Discovery
	// Pools returns the pool names, sorted.
	Pools() []string
	// AllReplicas returns every replica of every pool, sorted by pool and
	// then in configured order. Callers must not modify the returned slice.
	AllReplicas() []Replica
	// Version identifies the current replica set. It increases whenever the
	// set changes (it may also increase when the set does not), and never
	// decreases. Read Version before listing: a listing is then never older
	// than the version recorded for it, so a change can be applied late but
	// never missed.
	Version() uint64
	// Changed returns a channel that is closed when the replica set next
	// changes (when Version next increases). Take the channel before reading
	// Version or listing, so a change made in between still closes it.
	Changed() <-chan struct{}
}

// Static serves the replica list from configuration. Update swaps the whole
// set atomically, which is how hot reload applies a changed config file.
type Static struct {
	snap atomic.Pointer[snapshot]
	mu   sync.Mutex // serialises Update so versions never repeat
}

// snapshot is an immutable pool -> replicas view.
type snapshot struct {
	pools   map[string][]Replica
	version uint64
	// replaced is closed when a newer snapshot is published (Changed).
	replaced chan struct{}
}

var _ Inventory = (*Static)(nil)

// NewStatic builds a Static from configured pools. The pools must already be
// validated by config (names resolved, URLs well-formed).
func NewStatic(pools []config.Pool) (*Static, error) {
	snap, err := buildSnapshot(pools)
	if err != nil {
		return nil, err
	}
	snap.version = 1
	s := &Static{}
	s.snap.Store(snap)
	return s, nil
}

// Replicas implements Discovery.
func (s *Static) Replicas(pool string) []Replica {
	return s.snap.Load().pools[pool]
}

// Changed implements Inventory: the returned channel is closed by the next
// successful Update.
func (s *Static) Changed() <-chan struct{} {
	return s.snap.Load().replaced
}

// Version implements Inventory. It starts at 1 and grows by one on every
// successful Update.
func (s *Static) Version() uint64 {
	return s.snap.Load().version
}

// Pools returns the pool names, sorted.
func (s *Static) Pools() []string {
	return slices.Sorted(maps.Keys(s.snap.Load().pools))
}

// WarnOversized logs one warning for each pool larger than
// config.MaxPoolReplicas, the size AC-M1-5 is proven for. Such a pool still
// runs: each route allocates a little and costs more. Call it at startup;
// Watch calls it on every reload that changes the replica set.
func (s *Static) WarnOversized(logger *slog.Logger) {
	pools := s.snap.Load().pools
	for _, pool := range slices.Sorted(maps.Keys(pools)) {
		if n := len(pools[pool]); n > config.MaxPoolReplicas {
			logger.Warn("pool is larger than the route latency target is proven for",
				"pool", pool, "replicas", n, "proven_replicas", config.MaxPoolReplicas)
		}
	}
}

// AllReplicas implements Inventory: every replica, sorted by pool, then in
// configured order.
func (s *Static) AllReplicas() []Replica {
	snap := s.snap.Load()
	if snap == nil {
		return nil
	}
	var all []Replica
	for _, pool := range slices.Sorted(maps.Keys(snap.pools)) {
		all = append(all, snap.pools[pool]...)
	}
	return all
}

// Update replaces the replica set. It refuses a set whose pool names differ
// from the current ones: model -> pool mapping is fixed at startup, so adding
// or removing a pool needs a restart. On error the current set is kept.
// Every successful Update advances Version and closes the Changed channel.
func (s *Static) Update(pools []config.Pool) error {
	next, err := buildSnapshot(pools)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.snap.Load()
	if !slices.Equal(slices.Sorted(maps.Keys(cur.pools)), slices.Sorted(maps.Keys(next.pools))) {
		return errors.New("discovery: pool names changed; restart the gateway to add or remove pools")
	}
	next.version = cur.version + 1
	s.snap.Store(next)
	close(cur.replaced) // after Store: a woken waiter sees the new set
	return nil
}

// equal reports whether pools describe exactly the current replica set.
func (s *Static) equal(pools []config.Pool) bool {
	cur := s.snap.Load().pools
	if len(cur) != len(pools) {
		return false
	}
	for _, p := range pools {
		have, ok := cur[p.Name]
		if !ok || len(have) != len(p.Replicas) {
			return false
		}
		for i, r := range p.Replicas {
			u, err := url.Parse(r.URL)
			if err != nil || have[i].Name != r.Name || have[i].BaseURL.String() != u.String() {
				return false
			}
		}
	}
	return true
}

func buildSnapshot(pools []config.Pool) (*snapshot, error) {
	if len(pools) == 0 {
		return nil, errors.New("discovery: no pools configured")
	}
	out := make(map[string][]Replica, len(pools))
	for _, p := range pools {
		if _, dup := out[p.Name]; dup {
			return nil, fmt.Errorf("discovery: duplicate pool %q", p.Name)
		}
		if len(p.Replicas) == 0 {
			return nil, fmt.Errorf("discovery: pool %q has no replicas", p.Name)
		}
		reps := make([]Replica, 0, len(p.Replicas))
		for _, r := range p.Replicas {
			u, err := url.Parse(r.URL)
			if err != nil || u.Scheme == "" || u.Host == "" {
				return nil, fmt.Errorf("discovery: pool %q: invalid replica URL %q", p.Name, r.URL)
			}
			reps = append(reps, Replica{Pool: p.Name, Name: r.Name, BaseURL: u})
		}
		out[p.Name] = reps
	}
	return &snapshot{pools: out, replaced: make(chan struct{})}, nil
}
