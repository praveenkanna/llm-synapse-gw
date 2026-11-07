// Package health manages active and passive health checks, stuck engine detection,
// and deterministic consistent-hash ring rebuilding for upstream replicas.
//
// The tracker follows discovery hot reloads (SyncDiscovery): when replicas
// are added, removed or re-pointed, the affected pools' rings are rebuilt
// from the new set and health state is kept for exactly its replicas.
//
// Ejection and re-admission (ejection.go) are damped: an ejected replica
// stays out for at least an ejection time that grows on repeated ejections,
// and only a good /health probe after that time brings it back. A replica
// ejected as a stuck engine also needs a passing generation probe
// (probe.go), since a stuck engine's /health passes.
package health

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/ring"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

const (
	// Ejection reasons for telemetry and logging.
	ReasonTransportFailure = "transport_failure"
	ReasonHealthCheckFail  = "health_check_failed"
	ReasonStuckEngine      = "stuck_engine"
)

var (
	// ErrNoHealthyReplicas indicates all replicas in the pool are currently unhealthy.
	ErrNoHealthyReplicas = errors.New("no healthy replicas in pool")
)

// Config configures health monitoring, stuck-engine detection and
// ejection. Its settings are router.health and the ring's router.epsilon
// and router.overflow_slack, validated by config, which owns their
// defaults and bounds (ADR 0022); the rest are collaborators.
type Config struct {
	// RouterHealth is router.health. StuckProbeEnabled and
	// StuckProbeModels are read by whoever sets StuckProbeSender and
	// StuckProbeModel, not by the tracker.
	config.RouterHealth
	// Ring is the hash ring's settings (router.epsilon and
	// router.overflow_slack).
	Ring ring.Config
	// OutOfOrderPools names the pools whose replicas serve out of arrival
	// order (vLLM --scheduling-policy priority, pools[].priority_scheduling).
	// On them any recent first chunk on the replica excuses a judged wait,
	// since an older low-priority stream may legitimately wait behind newer
	// high-priority ones (stuck.go).
	OutOfOrderPools map[string]bool
	// StuckProbeSender sends the generation probe that gates re-admission
	// after a stuck_engine ejection (probe.go); *upstream.Client, which
	// injects the gateway's upstream API key. nil turns the probe off:
	// such a replica comes back on /health alone, like any other.
	StuckProbeSender ProbeSender
	// StuckProbeModel names the model a probe of rep asks for, or "" when
	// none is known (the probe fails open). nil knows none.
	StuckProbeModel func(rep discovery.Replica) string
	// Logger receives the tracker's warnings (a rejected generation
	// probe); nil discards them.
	Logger *slog.Logger
	// Now is the clock; nil means time.Now. Tests set a fake clock here
	// before New.
	Now func() time.Time
}

// ConfigFrom returns the tracker's settings from the validated router
// section r, with no collaborators set.
func ConfigFrom(r config.RouterConfig) Config {
	rc := ring.DefaultConfig()
	rc.Epsilon, rc.OverflowSlack = r.Epsilon, r.OverflowSlack
	if r.VirtualNodes > 0 {
		rc.VirtualNodes = r.VirtualNodes
	}
	return Config{RouterHealth: r.Health, Ring: rc}
}

// replicaHealth is one replica's health state. Values in Tracker.states are
// never modified: changes store a modified copy, under statesMu.
type replicaHealth struct {
	name           string
	pool           string
	healthy        bool
	ejectionReason string
	lastChecked    time.Time
	// lastTransition is when the replica was last ejected or (re-)admitted.
	lastTransition time.Time
	// ejectedUntil is the earliest re-admission while ejected.
	ejectedUntil time.Time
	// ejections counts ejections in a row; it sets the next ejection's
	// length and starts over once the replica stays in for MaxEjection.
	ejections int
	// activeFails and activePasses are consecutive /health probe results.
	activeFails  int
	activePasses int
	// passiveFails counts connect or DNS failures of requests since the
	// last good probe or re-admission. Kept apart from activeFails so that
	// one kind of failure never completes the other's threshold.
	passiveFails int
	// probeOwed is set by a stuck_engine ejection and cleared by
	// re-admission: while set (and a probe sender is configured), the
	// replica also needs a passing generation probe to come back.
	probeOwed bool
	// rejectedStatus is the HTTP status of the last rejected generation
	// probe (0: none since a probe that was not rejected), so a rejection
	// is logged once per status.
	rejectedStatus int
}

// poolView is a pool's routing view: its healthy replicas as a ring and as a
// list. It is built under statesMu after every health or membership change
// and published whole, so the request path reads it without locks or
// allocation, and an all-ejected pool's empty view is built once, not per
// request.
type poolView struct {
	ring    *ring.Ring          // healthy replicas; Len() == 0 when all are ejected
	healthy []discovery.Replica // the same replicas in discovery order; never modified
}

// Tracker monitors active replica health, passively ejects broken replicas,
// detects stuck engines, and maintains healthy consistent-hash rings.
type Tracker struct {
	cfg        Config
	disc       discovery.Discovery
	lister     replicaLister       // nil: disc cannot list (rings from disc.Replicas)
	inv        discovery.Inventory // nil: no version; every sync compares in full
	metrics    *telemetry.Metrics
	httpClient *http.Client
	now        func() time.Time // Config.Now
	// epoch is when the tracker was built; stuck-engine times are offsets
	// from it (monotonic, see stuck.go).
	epoch        time.Time
	bucketWidth  time.Duration // StuckEngineWindow / stuckWindowBuckets
	receiptWidth time.Duration // StuckEngineTimeout / excuseBucketsPerTimeout

	states   sync.Map // replicaName (string) -> *replicaHealth
	statesMu sync.Mutex
	views    sync.Map // poolName (string) -> *atomic.Pointer[poolView]
	// waits are the registered first-chunk waits (stuck.go).
	waits waitRegistry
	// members is the replica set last applied from discovery (nil without a
	// lister). Written under statesMu, read lock-free.
	members atomic.Pointer[membership]
	// probing holds the names of replicas with a generation probe in
	// flight (probe.go): at most one each, removed when it ends.
	probing sync.Map // replicaName (string) -> struct{}
	// probes tracks generation probe goroutines; Stop waits for them.
	probes sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New creates a new health Tracker. When disc can list its replicas (a
// discovery.Inventory such as *discovery.Static), every replica starts
// healthy, rings are built at once and the tracker follows reloads;
// otherwise rings are built lazily from disc.Replicas.
func New(cfg Config, disc discovery.Discovery, tm *telemetry.Metrics, client *http.Client) *Tracker {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if client == nil {
		client = &http.Client{Timeout: cfg.PollTimeout}
	}

	t := &Tracker{
		cfg:        cfg,
		disc:       disc,
		metrics:    tm,
		httpClient: client,
		now:        cfg.Now,
		epoch:      cfg.Now(),
		// at least 1ns, so a tiny window never divides by zero
		bucketWidth:  max(cfg.StuckEngineWindow/stuckWindowBuckets, 1),
		receiptWidth: max(cfg.StuckEngineTimeout/excuseBucketsPerTimeout, 1),
	}

	t.lister, _ = disc.(replicaLister)
	t.inv, _ = disc.(discovery.Inventory)
	t.SyncDiscovery() // every replica healthy, rings built

	return t
}

// Start begins background health polling and stuck stream inspection.
func (t *Tracker) Start(ctx context.Context) {
	t.ctx, t.cancel = context.WithCancel(ctx)

	t.wg.Add(3)
	go t.pollLoop()
	go t.stuckLoop()
	go t.syncLoop()
}

// Stop stops all background goroutines, generation probes included.
func (t *Tracker) Stop() {
	if t.cancel != nil {
		t.cancel()
	}
	t.wg.Wait()     // the poll loop, which starts probes, has returned
	t.probes.Wait() // probes in flight end on the cancelled context
}

// IsHealthy returns true if the replica is currently healthy.
func (t *Tracker) IsHealthy(replicaName string) bool {
	val, ok := t.states.Load(replicaName)
	if !ok {
		// Untracked replicas default to healthy until discovered/probed
		return true
	}
	return val.(*replicaHealth).healthy
}

// Healthy returns pool's currently healthy replicas in discovery order, or
// nil when none is. The slice is shared and must not be modified; reading
// it allocates nothing.
func (t *Tracker) Healthy(pool string) []discovery.Replica {
	if v := t.view(pool); v != nil {
		return v.healthy
	}
	return nil
}

// Ring returns the active consistent-hash ring of healthy replicas for pool,
// or ErrNoHealthyReplicas when every replica of the pool is ejected (or the
// pool is unknown). It reads the published view and never rebuilds it.
func (t *Tracker) Ring(pool string) (*ring.Ring, error) {
	v := t.view(pool)
	if v == nil || v.ring.Len() == 0 {
		return nil, ErrNoHealthyReplicas
	}
	return v.ring, nil
}

// view returns pool's published view. With a lister, every pool of the
// applied set has one (SyncDiscovery builds them; health changes rebuild
// them) and any other pool has none. Without a lister, a pool's first view
// is built here, once, under statesMu; later changes rebuild it.
func (t *Tracker) view(pool string) *poolView {
	if val, ok := t.views.Load(pool); ok {
		if v := val.(*atomic.Pointer[poolView]).Load(); v != nil {
			return v
		}
	}
	if t.members.Load() != nil || t.disc == nil {
		return nil
	}
	t.statesMu.Lock()
	defer t.statesMu.Unlock()
	ptr := t.viewPtr(pool)
	if v := ptr.Load(); v != nil {
		return v // built while this call waited for the lock
	}
	v := t.buildView(pool)
	ptr.Store(v)
	return v
}

// countEjection counts a replica leaving its pool's ring in
// gateway_replica_ejections_total. It is not a fail-open: requests are still
// routed, to the remaining replicas.
func (t *Tracker) countEjection(pool, reason string) {
	if t.metrics == nil {
		return
	}
	if pool == "" {
		pool = telemetry.PoolNone
	}
	t.metrics.ReplicaEjected(pool, reason)
}

func (t *Tracker) pollLoop() {
	defer t.wg.Done()
	ticker := time.NewTicker(t.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
			_ = t.PollOnce(t.ctx)
		}
	}
}

// PollOnce applies any discovery change (SyncDiscovery), then queries
// /health across all replicas and updates health state.
func (t *Tracker) PollOnce(ctx context.Context) error {
	t.SyncDiscovery()
	m := t.members.Load()
	if m == nil || len(m.byName) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	for _, rep := range m.byName {
		wg.Add(1)
		go func(r discovery.Replica) {
			defer wg.Done()
			t.pollReplica(ctx, r)
		}(rep)
	}
	wg.Wait()
	return nil
}

// pollReplica probes rep's /health and records the result. When that
// result leaves a stuck_engine ejection waiting only on its generation
// probe, the probe starts in the background (probe.go).
func (t *Tracker) pollReplica(ctx context.Context, rep discovery.Replica) {
	healthURL := rep.BaseURL.JoinPath("health").String()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
	var isOK bool
	if err == nil {
		resp, doErr := t.httpClient.Do(req)
		if doErr == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			isOK = (resp.StatusCode == http.StatusOK)
		}
	}
	if t.recordProbe(rep, isOK) {
		t.startGenerationProbe(ctx, rep)
	}
}

// rebuildAllPoolsForReplica rebuilds the view of the pool replicaName
// belongs to, for state recorded without a pool. Without a lister the pool
// is unknown, so every view built so far is rebuilt. Callers hold statesMu.
func (t *Tracker) rebuildAllPoolsForReplica(replicaName string) {
	if m := t.members.Load(); m != nil {
		if rep, ok := m.byName[replicaName]; ok {
			t.rebuildPoolRing(rep.Pool)
		}
		return
	}
	t.views.Range(func(key, _ any) bool {
		t.rebuildPoolRing(key.(string))
		return true
	})
}

// rebuildPoolRing publishes a new view for pool. Callers hold statesMu.
func (t *Tracker) rebuildPoolRing(pool string) {
	t.viewPtr(pool).Store(t.buildView(pool))
}

// buildView builds pool's view from its currently healthy replicas, with
// the replica values (base URLs included) of the applied set.
func (t *Tracker) buildView(pool string) *poolView {
	var healthy []discovery.Replica
	for _, rep := range t.poolReplicas(pool) {
		if t.IsHealthy(rep.Name) {
			healthy = append(healthy, rep)
		}
	}
	return &poolView{ring: ring.New(healthy, t.cfg.Ring), healthy: healthy}
}

func (t *Tracker) viewPtr(pool string) *atomic.Pointer[poolView] {
	if val, ok := t.views.Load(pool); ok {
		return val.(*atomic.Pointer[poolView])
	}
	newPtr := &atomic.Pointer[poolView]{}
	actual, _ := t.views.LoadOrStore(pool, newPtr)
	return actual.(*atomic.Pointer[poolView])
}
