package health

import (
	"errors"
	"net/url"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/ring"
)

var errRefused = errors.New("dial tcp: connection refused")

// ejectionTracker is a tracker over pool "p" with replicas a and b, on a
// fake clock, never started: probes are applied with recordProbe.
func ejectionTracker(t *testing.T, mod func(*Config)) (*Tracker, *FakeClock, *discovery.Static) {
	t.Helper()
	disc, err := discovery.NewStatic(pool(map[string]string{"a": "http://10.0.0.1:8000", "b": "http://10.0.0.2:8000"}))
	if err != nil {
		t.Fatal(err)
	}
	cfg := testConfig()
	cfg.MinEjection = 5 * time.Second
	cfg.MaxEjection = 20 * time.Second
	cfg.EjectionBackoffMultiplier = 2
	clock := NewFakeClock()
	cfg.Now = clock.Now
	if mod != nil {
		mod(&cfg)
	}
	tr := New(cfg, disc, nil, nil)
	return tr, clock, disc
}

func replicaOf(t *testing.T, disc *discovery.Static, name string) discovery.Replica {
	t.Helper()
	for _, rep := range disc.AllReplicas() {
		if rep.Name == name {
			return rep
		}
	}
	t.Fatalf("no replica %s", name)
	return discovery.Replica{}
}

func stateOf(tr *Tracker, name string) replicaHealth {
	val, _ := tr.states.Load(name)
	return *val.(*replicaHealth)
}

// TestPassiveFailuresNeedThresholdInARow: connect failures eject only at
// the threshold, and a good probe in between starts the count over.
func TestPassiveFailuresNeedThresholdInARow(t *testing.T) {
	tr, _, disc := ejectionTracker(t, nil) // passive threshold 3 (default)
	a := replicaOf(t, disc, "a")

	tr.RecordTransportFailure(a, errRefused)
	tr.RecordTransportFailure(a, errRefused)
	tr.recordProbe(a, true)
	tr.RecordTransportFailure(a, errRefused)
	tr.RecordTransportFailure(a, errRefused)
	if !tr.IsHealthy("a") {
		t.Fatal("a ejected by 2 + 2 connect failures with a good probe between them")
	}
	tr.RecordTransportFailure(a, errRefused)
	if tr.IsHealthy("a") {
		t.Fatal("a still in after 3 connect failures in a row")
	}
	if got := stateOf(tr, "a").ejectionReason; got != ReasonTransportFailure {
		t.Errorf("reason = %q, want %q", got, ReasonTransportFailure)
	}
}

// TestPassiveAndActiveFailuresCountedApart: neither kind of failure
// completes the other's threshold.
func TestPassiveAndActiveFailuresCountedApart(t *testing.T) {
	tr, _, disc := ejectionTracker(t, nil) // active 2, passive 3
	a := replicaOf(t, disc, "a")

	tr.recordProbe(a, false)
	tr.RecordTransportFailure(a, errRefused)
	tr.RecordTransportFailure(a, errRefused)
	if !tr.IsHealthy("a") {
		t.Fatal("1 failed probe + 2 connect failures ejected a: the counts are shared")
	}
	if st := stateOf(tr, "a"); st.activeFails != 1 || st.passiveFails != 2 {
		t.Fatalf("activeFails = %d, passiveFails = %d; want 1 and 2", st.activeFails, st.passiveFails)
	}
	tr.recordProbe(a, false)
	if tr.IsHealthy("a") {
		t.Fatal("a still in after 2 failed probes in a row")
	}
	if got := stateOf(tr, "a").ejectionReason; got != ReasonHealthCheckFail {
		t.Errorf("reason = %q, want %q", got, ReasonHealthCheckFail)
	}
}

// TestEjectionTimeAndBackoff: an ejected replica is re-admitted by the
// first good probe at its ejection time, never before; the time doubles on
// every ejection in a row up to the cap, and starts over once the replica
// stayed in for MaxEjection.
func TestEjectionTimeAndBackoff(t *testing.T) {
	tr, clock, disc := ejectionTracker(t, func(c *Config) { c.PassiveFailureThreshold = 1 })
	a := replicaOf(t, disc, "a")

	for i, want := range []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second, 20 * time.Second} {
		tr.RecordTransportFailure(a, errRefused)
		if tr.IsHealthy("a") {
			t.Fatalf("ejection %d: a not ejected", i+1)
		}
		clock.Advance(want - time.Millisecond)
		tr.recordProbe(a, true)
		if tr.IsHealthy("a") {
			t.Fatalf("ejection %d: re-admitted %v after ejection, want %v", i+1, want-time.Millisecond, want)
		}
		clock.Advance(time.Millisecond)
		if tr.IsHealthy("a") {
			t.Fatalf("ejection %d: re-admitted by time alone, without a probe", i+1)
		}
		tr.recordProbe(a, true)
		if !tr.IsHealthy("a") {
			t.Fatalf("ejection %d: not re-admitted by a good probe at %v", i+1, want)
		}
	}

	// In for MaxEjection: the next ejection starts over at MinEjection.
	clock.Advance(20 * time.Second)
	tr.RecordTransportFailure(a, errRefused)
	clock.Advance(5 * time.Second)
	tr.recordProbe(a, true)
	if !tr.IsHealthy("a") {
		t.Fatal("after staying in for max_ejection, the ejection time did not start over")
	}
}

// TestFlappingIsDamped: a replica alternating failed and good probes every
// poll is out for its whole ejection time each time, instead of entering
// and leaving the ring on every probe.
func TestFlappingIsDamped(t *testing.T) {
	tr, clock, disc := ejectionTracker(t, func(c *Config) { c.FailureThreshold = 1 })
	a := replicaOf(t, disc, "a")
	transitions := 0
	was := true
	for range 40 { // 40 s of 1 s polls, alternating fail / pass
		for _, ok := range []bool{false, true} {
			tr.recordProbe(a, ok)
			if h := tr.IsHealthy("a"); h != was {
				transitions++
				was = h
			}
			clock.Advance(500 * time.Millisecond)
		}
	}
	// Undamped: 80 transitions. Damped: out at 0 s for 5 s, in at 5.5 s,
	// out at 6 s for 10 s, in at 16.5 s, out at 17 s for 20 s, in at
	// 37.5 s, out at 38 s: 7.
	if transitions != 7 {
		t.Fatalf("%d ring transitions in 40 s of flapping, want 7", transitions)
	}
}

// TestRequiresSuccessThresholdInARowAfterEjection: good probes before a
// failed one don't count, and re-admission still waits for the threshold.
func TestRequiresSuccessThresholdInARowAfterEjection(t *testing.T) {
	tr, clock, disc := ejectionTracker(t, func(c *Config) {
		c.SuccessThreshold = 2
		c.PassiveFailureThreshold = 1
	})
	a := replicaOf(t, disc, "a")
	tr.RecordTransportFailure(a, errRefused)
	tr.recordProbe(a, true)
	tr.recordProbe(a, false)
	clock.Advance(5 * time.Second)
	tr.recordProbe(a, true)
	if tr.IsHealthy("a") {
		t.Fatal("re-admitted with 1 good probe in a row, threshold 2")
	}
	tr.recordProbe(a, true)
	if !tr.IsHealthy("a") {
		t.Fatal("not re-admitted with 2 good probes in a row after the ejection time")
	}
}

// TestStuckEngineNotReadmittedByHealthAlone: a stuck engine's /health
// keeps passing; that alone never brings it back before its ejection time.
func TestStuckEngineNotReadmittedByHealthAlone(t *testing.T) {
	tr, clock, disc := ejectionTracker(t, nil)
	a := replicaOf(t, disc, "a")
	tr.Eject("a", ReasonStuckEngine)
	for range 4 {
		clock.Advance(time.Second)
		tr.recordProbe(a, true)
		if tr.IsHealthy("a") {
			t.Fatal("stuck engine re-admitted by /health before its ejection time")
		}
	}
	clock.Advance(time.Second)
	if tr.IsHealthy("a") {
		t.Fatal("stuck engine re-admitted by time alone")
	}
	tr.recordProbe(a, true)
	if !tr.IsHealthy("a") {
		t.Fatal("stuck engine not re-admitted by a good probe after its ejection time")
	}
}

// TestFailureOnOldURLAfterRepointIgnored: once a reload re-points a
// replica, connect failures reported against its old address eject
// nothing; failures on the new address still count.
func TestFailureOnOldURLAfterRepointIgnored(t *testing.T) {
	tr, _, disc := ejectionTracker(t, func(c *Config) { c.PassiveFailureThreshold = 1 })
	oldA := replicaOf(t, disc, "a")
	if err := disc.Update(pool(map[string]string{"a": "http://10.0.0.9:8000", "b": "http://10.0.0.2:8000"})); err != nil {
		t.Fatal(err)
	}
	tr.SyncDiscovery()
	for range 5 {
		tr.RecordTransportFailure(oldA, errRefused)
	}
	if !tr.IsHealthy("a") {
		t.Fatal("a failure on a's old URL ejected its new endpoint")
	}
	if st := stateOf(tr, "a"); st.passiveFails != 0 {
		t.Fatalf("old-URL failures counted: passiveFails = %d", st.passiveFails)
	}
	tr.RecordTransportFailure(replicaOf(t, disc, "a"), errRefused)
	if tr.IsHealthy("a") {
		t.Fatal("a failure on a's current URL did not count")
	}
}

// TestAllEjectedRingIsCachedNotRebuilt: with every replica out, Ring
// answers from the published empty view: no rebuild, no allocation.
func TestAllEjectedRingIsCachedNotRebuilt(t *testing.T) {
	tr, _, _ := ejectionTracker(t, nil)
	tr.Eject("a", ReasonStuckEngine)
	tr.Eject("b", ReasonStuckEngine)
	v := tr.view("p")
	if v == nil || v.ring.Len() != 0 || len(v.healthy) != 0 {
		t.Fatalf("view = %+v, want the empty all-ejected view", v)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		if _, err := tr.Ring("p"); !errors.Is(err, ErrNoHealthyReplicas) {
			t.Fatalf("Ring = %v, want ErrNoHealthyReplicas", err)
		}
		if h := tr.Healthy("p"); h != nil {
			t.Fatalf("Healthy = %v, want nil", h)
		}
	})
	if allocs != 0 {
		t.Errorf("all-ejected Ring+Healthy allocate %.1f times per call, want 0", allocs)
	}
	if tr.view("p") != v {
		t.Error("the all-ejected view was rebuilt by a request-path read")
	}
}

// TestRingReadsRaceProbesSafely: request-path reads during concurrent
// ejections and re-admissions see only published views, and the final view
// matches the final state (run with -race).
func TestRingReadsRaceProbesSafely(t *testing.T) {
	tr, clock, disc := ejectionTracker(t, func(c *Config) { c.FailureThreshold = 1 })
	a, b := replicaOf(t, disc, "a"), replicaOf(t, disc, "b")
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				if r, err := tr.Ring("p"); err == nil && r.Len() == 0 {
					t.Error("Ring returned an empty ring without an error")
				}
				_ = tr.Healthy("p")
			}
		})
	}
	for i := range 200 {
		tr.recordProbe(a, i%2 == 0)
		tr.recordProbe(b, i%3 == 0)
		clock.Advance(time.Second)
	}
	close(stop)
	wg.Wait()

	clock.Advance(time.Minute)
	tr.recordProbe(a, true)
	tr.recordProbe(b, true)
	if r, err := tr.Ring("p"); err != nil || r.Len() != 2 {
		t.Fatalf("final ring = %v, %v; want both replicas", r, err)
	}
}

// noListDiscovery cannot list its replicas: the tracker builds views lazily.
type noListDiscovery map[string][]discovery.Replica

func (d noListDiscovery) Replicas(pool string) []discovery.Replica { return d[pool] }

// TestLazyViewBuiltOnce: without a lister, a pool's view is built on first
// read and then reused, also when every replica is out.
func TestLazyViewBuiltOnce(t *testing.T) {
	u, _ := url.Parse("http://10.0.0.1:8000")
	a := discovery.Replica{Pool: "p", Name: "a", BaseURL: u}
	cfg := testConfig()
	cfg.PassiveFailureThreshold = 1
	tr := New(cfg, noListDiscovery{"p": {a}}, nil, nil)
	v := tr.view("p")
	if v == nil || v.ring.Len() != 1 || tr.view("p") != v {
		t.Fatalf("lazy view = %+v, want one replica, built once", v)
	}
	tr.RecordTransportFailure(a, errRefused)
	empty := tr.view("p")
	if empty == v || empty.ring.Len() != 0 {
		t.Fatalf("view after ejecting the only replica = %+v", empty)
	}
	if allocs := testing.AllocsPerRun(100, func() { _, _ = tr.Ring("p") }); allocs != 0 || tr.view("p") != empty {
		t.Fatalf("all-ejected lazy view rebuilt or allocating (%.1f allocs)", allocs)
	}
}

func TestEjectionDuration(t *testing.T) {
	tests := []struct {
		name     string
		min, max time.Duration
		mult     float64
		prior    int
		want     time.Duration
	}{
		{"first", 5 * time.Second, time.Minute, 2, 0, 5 * time.Second},
		{"second", 5 * time.Second, time.Minute, 2, 1, 10 * time.Second},
		{"fourth", 5 * time.Second, time.Minute, 2, 3, 40 * time.Second},
		{"capped", 5 * time.Second, time.Minute, 2, 4, time.Minute},
		{"far past the cap", 5 * time.Second, time.Minute, 2, maxEjectionSteps, time.Minute},
		{"multiplier one", 5 * time.Second, time.Minute, 1, 10, 5 * time.Second},
		{"fractional", 4 * time.Second, time.Minute, 1.5, 2, 9 * time.Second},
		{"min equals max", time.Second, time.Second, 10, 3, time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tr := &Tracker{cfg: Config{RouterHealth: config.RouterHealth{MinEjection: tc.min, MaxEjection: tc.max, EjectionBackoffMultiplier: tc.mult}}}
			if got := tr.ejectionDuration(tc.prior); got != tc.want {
				t.Errorf("ejectionDuration(%d) = %v, want %v", tc.prior, got, tc.want)
			}
		})
	}
}

// TestConfigFromRouterSection: the tracker's settings are config's router
// section as validated (ADR 0022), the ring's from router.epsilon and
// router.overflow_slack; New sets the collaborators config does not own.
func TestConfigFromRouterSection(t *testing.T) {
	r := config.Defaults().Router
	r.Epsilon, r.OverflowSlack = 0.6, 4
	// Every field away from its default, so a field reset to the default
	// shows.
	r.Health = config.RouterHealth{
		PollInterval: 3 * time.Second, PollTimeout: 2 * time.Second,
		FailureThreshold: 4, SuccessThreshold: 3, PassiveFailureThreshold: 2,
		StuckEngineTimeout: time.Minute, StuckEngineStreams: 5, StuckEngineRatio: 0.7, StuckEngineWindow: 2 * time.Minute, StuckEngineMaxWaits: 512,
		MinEjection: 2 * time.Second, MaxEjection: 90 * time.Second, EjectionBackoffMultiplier: 3,
		StuckProbeEnabled: false, StuckProbeTimeout: 4 * time.Second, StuckProbePath: config.StuckProbePathChatCompletions,
		StuckProbeModels: map[string]string{"p": "m"},
	}
	c := ConfigFrom(r)
	if !reflect.DeepEqual(c.RouterHealth, r.Health) || c.StuckEngineMaxWaits != 512 {
		t.Errorf("settings = %+v, want router.health as given", c.RouterHealth)
	}
	if want := (ring.Config{VirtualNodes: ring.DefaultVirtualNodes, Epsilon: 0.6, OverflowSlack: 4}); c.Ring != want {
		t.Errorf("ring = %+v, want %+v", c.Ring, want)
	}
	if c.Now != nil || c.Logger != nil || c.StuckProbeSender != nil || c.StuckProbeModel != nil || c.OutOfOrderPools != nil {
		t.Errorf("ConfigFrom set a collaborator: %+v", c)
	}
	disc, err := discovery.NewStatic([]config.Pool{{Name: "p", Replicas: []config.Replica{{Name: "a", URL: "http://127.0.0.1:1"}}}})
	if err != nil {
		t.Fatal(err)
	}
	if tr := New(c, disc, nil, nil); tr.cfg.Now == nil || tr.cfg.Logger == nil {
		t.Error("New left the clock or the logger nil; want time.Now and a discarding logger")
	}
}

// TestEjectAgainExtendsNeverShortens: ejecting an ejected replica again
// (a stuck engine reported after a passive ejection) keeps it out for a
// full ejection time from the later report, counts no new ejection, and a
// report whose time would end sooner changes nothing.
func TestEjectAgainExtendsNeverShortens(t *testing.T) {
	tr, clock, disc := ejectionTracker(t, func(c *Config) { c.PassiveFailureThreshold = 1 })
	a := replicaOf(t, disc, "a")
	tr.RecordTransportFailure(a, errRefused) // out until +5 s
	start := stateOf(tr, "a").ejectedUntil
	clock.Advance(4 * time.Second)
	tr.Eject("a", ReasonStuckEngine) // out until +9 s
	st := stateOf(tr, "a")
	if st.ejectionReason != ReasonStuckEngine || !st.ejectedUntil.Equal(start.Add(4*time.Second)) || st.ejections != 1 {
		t.Fatalf("after a second ejection: reason %q, until +%v, ejections %d; want stuck_engine, +9s, 1",
			st.ejectionReason, st.ejectedUntil.Sub(start)+5*time.Second, st.ejections)
	}
	clock.Advance(time.Second) // +5 s: the first ejection's end
	tr.recordProbe(a, true)
	if tr.IsHealthy("a") {
		t.Fatal("re-admitted at the earlier deadline")
	}

	// A report that would not end later leaves the deadline where it is.
	tr.Eject("b", ReasonStuckEngine)
	longUntil := stateOf(tr, "b").ejectedUntil
	tr.Eject("b", ReasonTransportFailure) // same length, from the same instant: not later
	if got := stateOf(tr, "b").ejectedUntil; !got.Equal(longUntil) {
		t.Fatalf("deadline moved from %v to %v", longUntil, got)
	}

	clock.Advance(4 * time.Second) // +9 s
	tr.recordProbe(a, true)
	if !tr.IsHealthy("a") {
		t.Fatal("not re-admitted after the extended deadline")
	}
}

// testConfig is the tracker configuration from config's defaults.
func testConfig() Config { return ConfigFrom(config.Defaults().Router) }
