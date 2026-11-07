package arbitrator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

func TestShareOf(t *testing.T) {
	for _, tc := range []struct {
		limit, own, total, members, want int
	}{
		{limit: 10, own: 0, total: 0, members: 1, want: 10},  // alone and idle: everything
		{limit: 10, own: 2, total: 4, members: 2, want: 5},   // 2 + (10-4)/2
		{limit: 10, own: 0, total: 4, members: 2, want: 3},   // idle gateway gets its split
		{limit: 10, own: 15, total: 20, members: 2, want: 7}, // over: 10*15/20
		{limit: 10, own: 1, total: 100, members: 3, want: 1}, // floor of one slot
		{limit: 10, own: 0, total: 30, members: 2, want: 1},
		{limit: 10, own: 5, total: 0, members: 0, want: 10}, // stale view: own demand counts
	} {
		if got := shareOf(tc.limit, tc.own, tc.total, tc.members); got != tc.want {
			t.Errorf("shareOf(%d, own %d, total %d, members %d) = %d, want %d",
				tc.limit, tc.own, tc.total, tc.members, got, tc.want)
		}
	}
}

// fakeStore keeps demand per pool and member, with no leases.
type fakeStore struct {
	mu     sync.Mutex
	demand map[string]map[string]int
	err    error
	calls  int
}

func (s *fakeStore) Sync(_ context.Context, pool, member string, own int, _ time.Duration) (int, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.err != nil {
		return 0, 0, s.err
	}
	if s.demand[pool] == nil {
		s.demand[pool] = map[string]int{}
	}
	s.demand[pool][member] = own
	total := 0
	for _, d := range s.demand[pool] {
		total += d
	}
	return total, len(s.demand[pool]), nil
}

func (s *fakeStore) setErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

type failOpens struct {
	mu sync.Mutex
	n  map[string]int
}

func (f *failOpens) FailOpen(stage, reason string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n[stage+":"+reason]++
}

func TestCoordinatorSharesAndFallsBack(t *testing.T) {
	arb := newTest(t, testConfig(), 10)
	store := &fakeStore{demand: map[string]map[string]int{
		"p": {"other": 6}, // another gateway using 6 of 10
	}}
	fo := &failOpens{n: map[string]int{}}
	c := NewCoordinator(CoordinatorConfig{Member: "me", Interval: time.Hour, Timeout: time.Second, LeaseTTL: time.Second}, arb, store, fo)
	now := time.Unix(1000, 0)
	c.now = func() time.Time { return now }

	c.Sync(context.Background())
	if got := arb.pools["p"].share.Load(); got != 2 { // 0 + (10-6)/2
		t.Fatalf("share = %d, want 2", got)
	}
	// Local admission obeys the share: two slots, then a queue.
	s1, _ := arb.Acquire(context.Background(), "p", "interactive")
	s2, _ := arb.Acquire(context.Background(), "p", "interactive")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	if _, err := arb.Acquire(ctx, "p", "interactive"); err == nil {
		t.Fatal("third request admitted beyond a share of 2")
	}
	cancel()

	// Redis fails: the share holds for one lease, then local limits apply.
	store.setErr(errors.New("connection refused"))
	now = now.Add(500 * time.Millisecond)
	c.Sync(context.Background())
	if got := arb.pools["p"].share.Load(); got != 2 {
		t.Fatalf("share dropped within the lease: %d", got)
	}
	now = now.Add(time.Second)
	c.Sync(context.Background())
	if got := arb.pools["p"].share.Load(); got != -1 {
		t.Fatalf("share = %d after the lease ran out, want -1 (local)", got)
	}
	if fo.n[telemetry.StageAdmissionRedis+":"+telemetry.ReasonRedisError] != 2 {
		t.Fatalf("fail-opens = %v, want one per failed round", fo.n)
	}
	s3, err := arb.Acquire(context.Background(), "p", "interactive")
	if err != nil {
		t.Fatalf("local fallback refused a request with room: %v", err)
	}

	// Redis is back: shares again.
	store.setErr(nil)
	c.Sync(context.Background())
	if got := arb.pools["p"].share.Load(); got != 3 { // own 3 + (10-9)/2
		t.Fatalf("share = %d after recovery, want 3", got)
	}
	s1.Release()
	s2.Release()
	s3.Release()
	assertDrained(t, arb)
}

func TestShareDemandCapsClassesAtTheirShare(t *testing.T) {
	cfg := testConfig()
	cfg.Classes[2].MaxShare = 0.25 // background: at most 2 of 8 slots
	a := newTest(t, cfg, 8)
	var held []*Slot
	for range 2 {
		held = append(held, mustAcquire(t, a, "background"))
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for range 5 { // a backlog the class could never run
		go func() { _, _ = a.Acquire(ctx, "p", "background") }()
	}
	waitQueued(t, a, 5)
	held = append(held, mustAcquire(t, a, "interactive"))
	if got := a.ShareDemand("p"); got != 3 { // background 2 (capped from 7) + interactive 1
		t.Fatalf("ShareDemand = %d, want 3", got)
	}
	if in, q := a.Demand("p"); in != 3 || q != 5 {
		t.Fatalf("Demand = %d, %d; want 3 in flight, 5 queued", in, q)
	}
	cancel()
	waitQueued(t, a, 0)
	for _, s := range held {
		s.Release()
	}
	assertDrained(t, a)
}

func TestCoordinatorTimeoutReason(t *testing.T) {
	arb := newTest(t, testConfig(), 4)
	fo := &failOpens{n: map[string]int{}}
	block := StoreFunc(func(ctx context.Context, _, _ string, _ int, _ time.Duration) (int, int, error) {
		<-ctx.Done()
		return 0, 0, ctx.Err()
	})
	c := NewCoordinator(CoordinatorConfig{Member: "me", Interval: time.Hour, Timeout: 5 * time.Millisecond, LeaseTTL: time.Second}, arb, block, fo)
	start := time.Now()
	c.Sync(context.Background())
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Fatalf("sync took %s with a 5ms timeout", took)
	}
	if fo.n[telemetry.StageAdmissionRedis+":"+ReasonRedisTimeout] != 1 {
		t.Fatalf("fail-opens = %v, want one redis_timeout", fo.n)
	}
}

func TestCoordinatorStartStop(t *testing.T) {
	arb := newTest(t, testConfig(), 4)
	store := &fakeStore{demand: map[string]map[string]int{}}
	c := NewCoordinator(CoordinatorConfig{Member: "me", Interval: time.Millisecond, Timeout: time.Second, LeaseTTL: time.Second}, arb, store, nil)
	c.Start(context.Background())
	deadline := time.Now().Add(2 * time.Second)
	for {
		store.mu.Lock()
		n := store.calls
		store.mu.Unlock()
		if n >= 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d syncs", n)
		}
		time.Sleep(time.Millisecond)
	}
	c.Stop()
}

// StoreFunc adapts a function to Store.
type StoreFunc func(ctx context.Context, pool, member string, demand int, lease time.Duration) (int, int, error)

func (f StoreFunc) Sync(ctx context.Context, pool, member string, demand int, lease time.Duration) (int, int, error) {
	return f(ctx, pool, member, demand, lease)
}
