package health_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/health"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/ring"
)

// rep is a named replica for pool "p" in a reload test.
type rep struct{ name, url string }

func poolsOf(reps ...rep) []config.Pool {
	p := config.Pool{Name: "p"}
	for _, r := range reps {
		p.Replicas = append(p.Replicas, config.Replica{Name: r.name, URL: r.url})
	}
	return []config.Pool{p}
}

func newStatic(t *testing.T, reps ...rep) *discovery.Static {
	t.Helper()
	s, err := discovery.NewStatic(poolsOf(reps...))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func update(t *testing.T, s *discovery.Static, reps ...rep) {
	t.Helper()
	if err := s.Update(poolsOf(reps...)); err != nil {
		t.Fatal(err)
	}
}

func healthyServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// ringOf returns pool p's ring members as name=url, sorted.
func ringOf(t *testing.T, tr *health.Tracker) []string {
	t.Helper()
	r, err := tr.Ring("p")
	if err != nil {
		t.Fatalf("Ring(p): %v", err)
	}
	var out []string
	for _, rep := range r.Replicas() {
		out = append(out, rep.Name+"="+rep.BaseURL.String())
	}
	slices.Sort(out)
	return out
}

func want(reps ...rep) []string {
	var out []string
	for _, r := range reps {
		out = append(out, r.name+"="+r.url)
	}
	slices.Sort(out)
	return out
}

// TestReloadRebuildsRingOnPoll is the review's reproduction (B2): pool [a,b]
// reloaded to [a,c] must leave the ring as [a,c] after a poll, not [a,b].
func TestReloadRebuildsRingOnPoll(t *testing.T) {
	srv := healthyServer(t)
	a := rep{"a", srv.URL}
	b := rep{"b", srv.URL + "/b"}
	c := rep{"c", srv.URL + "/c"}
	disc := newStatic(t, a, b)
	tr := health.New(health.ConfigFrom(config.Defaults().Router), disc, nil, srv.Client())

	if got := ringOf(t, tr); !slices.Equal(got, want(a, b)) {
		t.Fatalf("initial ring = %v", got)
	}
	update(t, disc, a, c)
	if err := tr.PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := ringOf(t, tr); !slices.Equal(got, want(a, c)) {
		t.Fatalf("ring after reload and one poll = %v, want %v", got, want(a, c))
	}
}

// TestReloadAddRemoveRepoint drives each kind of reload through
// SyncDiscovery and checks ring membership and base URLs.
func TestReloadAddRemoveRepoint(t *testing.T) {
	a := rep{"a", "http://10.0.0.1:8000"}
	b := rep{"b", "http://10.0.0.2:8000"}
	c := rep{"c", "http://10.0.0.3:8000"}
	b2 := rep{"b", "http://10.0.0.9:9000"} // b re-pointed

	tests := []struct {
		name   string
		before []rep
		after  []rep
	}{
		{"add", []rep{a, b}, []rep{a, b, c}},
		{"remove", []rep{a, b, c}, []rep{a, c}},
		{"replace", []rep{a, b}, []rep{a, c}},
		{"re-point", []rep{a, b}, []rep{a, b2}},
		{"re-point and add", []rep{a, b}, []rep{a, b2, c}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			disc := newStatic(t, tc.before...)
			tr := health.New(health.ConfigFrom(config.Defaults().Router), disc, nil, nil)
			if got := ringOf(t, tr); !slices.Equal(got, want(tc.before...)) {
				t.Fatalf("initial ring = %v", got)
			}
			update(t, disc, tc.after...)
			if !tr.SyncDiscovery() {
				t.Error("SyncDiscovery reported no change")
			}
			if got := ringOf(t, tr); !slices.Equal(got, want(tc.after...)) {
				t.Fatalf("ring after reload = %v, want %v", got, want(tc.after...))
			}
			if tr.SyncDiscovery() {
				t.Error("second SyncDiscovery with no new version reported a change")
			}
		})
	}
}

// TestKeyedTrafficLeavesRemovedReplica: after b is removed, no key routes to
// it, and keys owned by surviving replicas mostly stay put (consistent
// hashing: only b's keys move).
func TestKeyedTrafficLeavesRemovedReplica(t *testing.T) {
	a := rep{"a", "http://10.0.0.1:8000"}
	b := rep{"b", "http://10.0.0.2:8000"}
	c := rep{"c", "http://10.0.0.3:8000"}
	disc := newStatic(t, a, b, c)
	tr := health.New(health.ConfigFrom(config.Defaults().Router), disc, nil, nil)

	const keys = 1000
	before := make([]string, keys)
	toB := 0
	r, _ := tr.Ring("p")
	for i := range keys {
		pick, err := r.Route(fmt.Sprintf("conv-%d", i), nil, "")
		if err != nil {
			t.Fatal(err)
		}
		before[i] = pick.Name
		if pick.Name == "b" {
			toB++
		}
	}
	if toB == 0 {
		t.Fatal("no key routed to b before the reload; test proves nothing")
	}

	update(t, disc, a, c)
	tr.SyncDiscovery()
	r, _ = tr.Ring("p")
	moved := 0
	for i := range keys {
		pick, err := r.Route(fmt.Sprintf("conv-%d", i), nil, "")
		if err != nil {
			t.Fatal(err)
		}
		if pick.Name == "b" {
			t.Fatalf("key %d still routed to removed replica b", i)
		}
		if before[i] != "b" && pick.Name != before[i] {
			moved++
		}
	}
	if moved != 0 {
		t.Errorf("%d keys owned by surviving replicas moved; only b's keys should", moved)
	}
}

// TestReloadKeepsEjectionsOfRemainingReplicas: a replica that stays at the
// same address keeps its ejection; a re-pointed one starts healthy at its
// new address; a new one starts healthy.
func TestReloadKeepsEjectionsOfRemainingReplicas(t *testing.T) {
	a := rep{"a", "http://10.0.0.1:8000"}
	b := rep{"b", "http://10.0.0.2:8000"}
	c := rep{"c", "http://10.0.0.3:8000"}
	d := rep{"d", "http://10.0.0.4:8000"}
	b2 := rep{"b", "http://10.0.0.9:8000"}
	disc := newStatic(t, a, b, c)
	tr := health.New(health.ConfigFrom(config.Defaults().Router), disc, nil, nil)
	tr.Eject("a", health.ReasonTransportFailure)
	tr.Eject("b", health.ReasonTransportFailure)

	update(t, disc, a, b2, c, d)
	tr.SyncDiscovery()

	if tr.IsHealthy("a") {
		t.Error("a (same address) lost its ejection across the reload")
	}
	if !tr.IsHealthy("b") {
		t.Error("b re-pointed to a new address is still ejected")
	}
	if got := ringOf(t, tr); !slices.Equal(got, want(b2, c, d)) {
		t.Fatalf("ring = %v, want %v", got, want(b2, c, d))
	}
}

// TestLateReportsForRemovedReplicaChangeNothing: a passive failure, stuck
// stream or probe result arriving for a replica after the reload removed
// it neither ejects anything nor brings it back.
func TestLateReportsForRemovedReplicaChangeNothing(t *testing.T) {
	a := rep{"a", "http://10.0.0.1:8000"}
	b := rep{"b", "http://10.0.0.2:8000"}
	disc := newStatic(t, a, b)
	cfg := health.ConfigFrom(config.Defaults().Router)
	cfg.PassiveFailureThreshold = 1 // one late report would eject b if it were applied
	tr := health.New(cfg, disc, nil, nil)
	removedB := disc.Replicas("p")[1]
	update(t, disc, a)
	tr.SyncDiscovery()

	tr.RecordTransportFailure(removedB, errors.New("dial tcp: connection refused"))
	tr.Eject("b", health.ReasonStuckEngine)
	tr.Restore("b")
	if got := ringOf(t, tr); !slices.Equal(got, want(a)) {
		t.Fatalf("ring = %v, want %v", got, want(a))
	}
}

// fakeInventory is a discovery.Inventory other than *discovery.Static.
type fakeInventory struct {
	mu      sync.Mutex
	reps    []discovery.Replica
	version uint64
	changed chan struct{}
}

var _ discovery.Inventory = (*fakeInventory)(nil)

func (f *fakeInventory) set(reps ...discovery.Replica) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reps = reps
	f.version++
	if f.changed != nil {
		close(f.changed)
	}
	f.changed = make(chan struct{})
}

func (f *fakeInventory) Changed() <-chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.changed
}

func (f *fakeInventory) Replicas(pool string) []discovery.Replica {
	var out []discovery.Replica
	for _, r := range f.AllReplicas() {
		if r.Pool == pool {
			out = append(out, r)
		}
	}
	return out
}
func (f *fakeInventory) Pools() []string { return []string{"p"} }
func (f *fakeInventory) AllReplicas() []discovery.Replica {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reps
}
func (f *fakeInventory) Version() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.version
}

func replica(name, raw string) discovery.Replica {
	u, err := url.Parse(raw)
	if err != nil {
		panic(err)
	}
	return discovery.Replica{Pool: "p", Name: name, BaseURL: u}
}

// TestTrackerFollowsAnyInventory: the tracker works through
// discovery.Inventory, not only *discovery.Static.
func TestTrackerFollowsAnyInventory(t *testing.T) {
	inv := &fakeInventory{}
	inv.set(replica("a", "http://10.0.0.1:8000"), replica("b", "http://10.0.0.2:8000"))
	tr := health.New(health.ConfigFrom(config.Defaults().Router), inv, nil, nil)
	if got := ringOf(t, tr); !slices.Equal(got, []string{"a=http://10.0.0.1:8000", "b=http://10.0.0.2:8000"}) {
		t.Fatalf("initial ring = %v", got)
	}
	inv.set(replica("b", "http://10.0.0.5:8000"))
	tr.SyncDiscovery()
	if got := ringOf(t, tr); !slices.Equal(got, []string{"b=http://10.0.0.5:8000"}) {
		t.Fatalf("ring after change = %v", got)
	}
}

// TestBackgroundLoopAppliesReload: with the tracker started, a reload
// reaches the ring on the next poll tick, with no explicit sync and while
// every /health probe hangs (the sync never waits on a probe).
func TestBackgroundLoopAppliesReload(t *testing.T) {
	release := make(chan struct{})
	reached := make(chan struct{}, 16)
	hang := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case reached <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer hang.Close()
	defer close(release)
	a := rep{"a", hang.URL}
	b := rep{"b", hang.URL + "/b"}
	c := rep{"c", hang.URL + "/c"}
	disc := newStatic(t, a, b)
	cfg := health.ConfigFrom(config.Defaults().Router)
	cfg.PollInterval = 10 * time.Millisecond
	cfg.PollTimeout = time.Hour
	tr := health.New(cfg, disc, nil, &http.Client{})
	tr.Start(context.Background())
	defer tr.Stop()
	select { // the poll loop is now stuck in a probe
	case <-reached:
	case <-time.After(5 * time.Second):
		t.Fatal("health poll never reached the replica")
	}

	update(t, disc, a, c)
	deadline := time.After(5 * time.Second)
	for !slices.Equal(ringOf(t, tr), want(a, c)) {
		select {
		case <-deadline:
			t.Fatalf("ring still %v 5s after the reload (poll interval 10ms)", ringOf(t, tr))
		case <-time.After(cfg.PollInterval):
		}
	}
}

// TestRouteConcurrentWithReloads: requests read rings while reloads swap
// them; every pick is a replica of some published set and no read fails.
// Run with -race.
func TestRouteConcurrentWithReloads(t *testing.T) {
	a := rep{"a", "http://10.0.0.1:8000"}
	b := rep{"b", "http://10.0.0.2:8000"}
	c := rep{"c", "http://10.0.0.3:8000"}
	disc := newStatic(t, a, b)
	tr := health.New(health.ConfigFrom(config.Defaults().Router), disc, nil, nil)
	valid := map[string]bool{"a": true, "b": true, "c": true}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var routed atomic.Int64
	for w := range 8 {
		wg.Go(func() {
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				r, err := tr.Ring("p")
				if err != nil {
					t.Errorf("Ring during reload: %v", err)
					return
				}
				pick, err := r.Route(fmt.Sprintf("k-%d-%d", w, i), func(discovery.Replica) int64 { return 0 }, "")
				if err != nil && !errors.Is(err, ring.ErrNoReplicas) {
					t.Errorf("Route during reload: %v", err)
					return
				}
				if err == nil && !valid[pick.Name] {
					t.Errorf("routed to unknown replica %q", pick.Name)
					return
				}
				routed.Add(1)
			}
		})
	}
	sets := [][]rep{{a, c}, {a, b, c}, {b}, {a, b}}
	for i := range 200 {
		update(t, disc, sets[i%len(sets)]...)
		tr.SyncDiscovery()
	}
	close(stop)
	wg.Wait()
	if routed.Load() == 0 {
		t.Fatal("no routes ran concurrently with the reloads")
	}
	if got := ringOf(t, tr); !slices.Equal(got, want(sets[199%len(sets)]...)) {
		t.Fatalf("final ring = %v", got)
	}
}

// TestReloadRebuildsRingWithoutWaitingForTick: a Static.Update reaches the
// ring through Inventory.Changed, not the poll tick. The tick here is an
// hour, so only the change signal can explain a rebuild.
func TestReloadRebuildsRingWithoutWaitingForTick(t *testing.T) {
	a := rep{"a", "http://10.0.0.1:8000"}
	b := rep{"b", "http://10.0.0.2:8000"}
	c := rep{"c", "http://10.0.0.3:8000"}
	cfg := health.ConfigFrom(config.Defaults().Router)
	cfg.PollInterval = time.Hour
	cfg.PollTimeout = time.Hour

	disc := newStatic(t, a, b)
	tr := health.New(cfg, disc, nil, nil)
	tr.Start(context.Background())
	defer tr.Stop()
	waitRing(t, tr, want(a, b), 5*time.Second)

	for i, set := range [][]rep{{a, c}, {a, b, c}, {c}} {
		start := time.Now()
		update(t, disc, set...)
		waitRing(t, tr, want(set...), 5*time.Second)
		t.Logf("reload %d reached the ring in %v", i, time.Since(start))
	}

	inv := &fakeInventory{}
	inv.set(replica("a", "http://10.0.0.1:8000"))
	tr2 := health.New(cfg, inv, nil, nil)
	tr2.Start(context.Background())
	defer tr2.Stop()
	inv.set(replica("b", "http://10.0.0.2:8000"))
	waitRing(t, tr2, []string{"b=http://10.0.0.2:8000"}, 5*time.Second)
}

// TestFileEditReachesRingWithinReloadInterval: end to end from a changed
// config (as discovery.Watch sees it) to the ring, the worst case is the
// reload interval plus a small margin, with the health tick out of play.
func TestFileEditReachesRingWithinReloadInterval(t *testing.T) {
	const reloadInterval = 300 * time.Millisecond
	const margin = 700 * time.Millisecond
	a := rep{"a", "http://10.0.0.1:8000"}
	b := rep{"b", "http://10.0.0.2:8000"}
	disc := newStatic(t, a, b)
	var file atomic.Pointer[config.Config]
	file.Store(&config.Config{Pools: poolsOf(a, b)})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		discovery.Watch(ctx, disc, func() (*config.Config, error) { return file.Load(), nil }, reloadInterval, nil)
	}()
	defer func() { cancel(); <-watchDone }()

	cfg := health.ConfigFrom(config.Defaults().Router)
	cfg.PollInterval = time.Hour
	cfg.PollTimeout = time.Hour
	tr := health.New(cfg, disc, nil, nil)
	tr.Start(ctx)
	defer tr.Stop()

	for i, set := range [][]rep{{a}, {a, b}, {b}} {
		start := time.Now()
		file.Store(&config.Config{Pools: poolsOf(set...)}) // the "file edit"
		waitRing(t, tr, want(set...), reloadInterval+margin)
		t.Logf("edit %d reached the ring in %v (reload interval %v)", i, time.Since(start), reloadInterval)
	}
}

// waitRing waits until pool p's ring is exactly wantRing, failing after limit.
func waitRing(t *testing.T, tr *health.Tracker, wantRing []string, limit time.Duration) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for {
		got := ringOf(t, tr)
		if slices.Equal(got, wantRing) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("ring = %v after %v, want %v", got, limit, wantRing)
		}
		time.Sleep(time.Millisecond)
	}
}
