package templates

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeLoader is an in-memory Loader.
type fakeLoader struct {
	mu       sync.Mutex
	stamp    string
	recs     []Record
	stampErr error
	loadErr  error
	block    bool // calls wait for their context and return plainErr
	plainErr error
	panicky  bool
	stamps   int
	loads    int
	limits   []int
}

func (f *fakeLoader) Stamp(ctx context.Context) (string, error) {
	f.mu.Lock()
	f.stamps++
	stamp, err, block, plain, panicky := f.stamp, f.stampErr, f.block, f.plainErr, f.panicky
	f.mu.Unlock()
	if panicky {
		panic("loader bug")
	}
	if block {
		<-ctx.Done()
		if plain != nil {
			return "", plain
		}
		return "", ctx.Err()
	}
	return stamp, err
}

func (f *fakeLoader) LoadServed(_ context.Context, limit int) ([]Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loads++
	f.limits = append(f.limits, limit)
	return f.recs, f.loadErr
}

func (f *fakeLoader) set(fn func(f *fakeLoader)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

func (f *fakeLoader) counts() (stamps, loads int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stamps, f.loads
}

// fakeClock is a settable clock.
type fakeClock struct{ ns atomic.Int64 }

func (c *fakeClock) Now() time.Time          { return time.Unix(0, c.ns.Load()) }
func (c *fakeClock) Advance(d time.Duration) { c.ns.Add(int64(d)) }

type registryTest struct {
	loader  *fakeLoader
	clock   *fakeClock
	reg     *Registry
	mu      sync.Mutex
	reasons []string
	invalid []string
}

func newRegistryTest(t *testing.T, o RegistryOptions, recs ...Record) *registryTest {
	t.Helper()
	rt := &registryTest{loader: &fakeLoader{stamp: "s1", recs: recs}, clock: &fakeClock{}}
	rt.clock.ns.Store(time.Unix(1000, 0).UnixNano())
	o.Now = rt.clock.Now
	o.OnFailOpen = func(reason string) {
		rt.mu.Lock()
		defer rt.mu.Unlock()
		rt.reasons = append(rt.reasons, reason)
	}
	o.OnInvalid = func(id string, version int, err error) {
		rt.mu.Lock()
		defer rt.mu.Unlock()
		rt.invalid = append(rt.invalid, fmt.Sprintf("%s/%d", id, version))
	}
	rt.reg = NewRegistry(rt.loader, o)
	return rt
}

func (rt *registryTest) failOpens() []string {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return append([]string(nil), rt.reasons...)
}

func (rt *registryTest) mustRefresh(t *testing.T) {
	t.Helper()
	if err := rt.reg.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
}

func TestRegistryFirstLoad(t *testing.T) {
	rt := newRegistryTest(t, RegistryOptions{MaxVersions: 10}, record(t, "a", 1, StatusActive, matchSpec()))
	if rt.reg.Snapshot() != nil {
		t.Fatal("Snapshot before the first load is not nil")
	}
	rt.clock.Advance(3 * time.Second)
	if got := rt.reg.Age(); got != 3*time.Second {
		t.Fatalf("Age before a load = %v, want time since NewRegistry", got)
	}
	rt.mustRefresh(t)
	snap := rt.reg.Snapshot()
	if snap.Len() != 1 || !snap.LoadedAt().Equal(rt.clock.Now()) {
		t.Fatalf("snapshot Len = %d, LoadedAt = %v", snap.Len(), snap.LoadedAt())
	}
	if rt.reg.Age() != 0 {
		t.Fatalf("Age after a load = %v", rt.reg.Age())
	}
	if _, loads := rt.loader.counts(); loads != 1 || rt.loader.limits[0] != 10 {
		t.Fatalf("loads = %d, limits = %v", loads, rt.loader.limits)
	}
	if len(rt.failOpens()) != 0 {
		t.Fatalf("fail-opens = %v", rt.failOpens())
	}
}

func TestRegistryUnchangedStampSkipsLoad(t *testing.T) {
	rt := newRegistryTest(t, RegistryOptions{}, record(t, "a", 1, StatusActive, matchSpec()))
	rt.mustRefresh(t)
	first := rt.reg.Snapshot()
	rt.clock.Advance(time.Minute)
	rt.mustRefresh(t)
	if stamps, loads := rt.loader.counts(); stamps != 2 || loads != 1 {
		t.Fatalf("stamps = %d, loads = %d; want 2, 1", stamps, loads)
	}
	if rt.reg.Snapshot() != first {
		t.Fatal("snapshot swapped on an unchanged stamp")
	}
	if rt.reg.Age() != 0 {
		t.Fatalf("Age = %v, want 0 after a successful check", rt.reg.Age())
	}
}

func TestRegistryChangeSwaps(t *testing.T) {
	rt := newRegistryTest(t, RegistryOptions{}, record(t, "a", 1, StatusActive, matchSpec()))
	rt.mustRefresh(t)
	first := rt.reg.Snapshot()
	rt.loader.set(func(f *fakeLoader) {
		f.stamp = "s2"
		f.recs = append(f.recs, record(t, "a", 2, StatusActive, matchSpec()))
	})
	rt.mustRefresh(t)
	second := rt.reg.Snapshot()
	if second == first || second.Len() != 2 || first.Len() != 1 {
		t.Fatalf("Len first = %d, second = %d", first.Len(), second.Len())
	}
	if _, ok := second.Lookup("a", 2); !ok {
		t.Fatal("new version not in the new snapshot")
	}
}

func TestRegistryLoaderErrorKeepsSnapshot(t *testing.T) {
	rt := newRegistryTest(t, RegistryOptions{}, record(t, "a", 1, StatusActive, matchSpec()))
	rt.mustRefresh(t)
	first := rt.reg.Snapshot()

	rt.loader.set(func(f *fakeLoader) { f.stampErr = errors.New("connection refused") })
	rt.clock.Advance(10 * time.Second)
	for range 2 {
		if err := rt.reg.Refresh(context.Background()); err == nil {
			t.Fatal("Refresh: nil error")
		}
	}
	rt.loader.set(func(f *fakeLoader) { f.stampErr, f.stamp, f.loadErr = nil, "s2", errors.New("relation missing") })
	if err := rt.reg.Refresh(context.Background()); err == nil {
		t.Fatal("Refresh: nil error")
	}
	if rt.reg.Snapshot() != first {
		t.Fatal("failure replaced the snapshot")
	}
	if got := rt.failOpens(); fmt.Sprint(got) != "[postgres_error postgres_error postgres_error]" {
		t.Fatalf("fail-opens = %v, want one postgres_error per round", got)
	}
	if rt.reg.Age() != 10*time.Second {
		t.Fatalf("Age = %v, want time since the last success", rt.reg.Age())
	}

	rt.loader.set(func(f *fakeLoader) { f.loadErr = nil })
	rt.mustRefresh(t)
	if rt.reg.Snapshot() == first {
		t.Fatal("recovered round did not load the changed stamp")
	}
}

func TestRegistryErrorBeforeFirstLoad(t *testing.T) {
	rt := newRegistryTest(t, RegistryOptions{})
	rt.loader.set(func(f *fakeLoader) { f.stampErr = errors.New("down") })
	if err := rt.reg.Refresh(context.Background()); err == nil {
		t.Fatal("Refresh: nil error")
	}
	if rt.reg.Snapshot() != nil {
		t.Fatal("snapshot after a failed first load")
	}
	if _, reason := rt.reg.Snapshot().Match("a", "1", nil); reason != ReasonRegistryUnavailable {
		t.Fatalf("Match reason = %q", reason)
	}
}

func TestRegistryTimeout(t *testing.T) {
	for _, plain := range []error{nil, errors.New("conn closed")} {
		t.Run(fmt.Sprint(plain), func(t *testing.T) {
			rt := newRegistryTest(t, RegistryOptions{QueryTimeout: 10 * time.Millisecond})
			rt.loader.set(func(f *fakeLoader) { f.block, f.plainErr = true, plain })
			err := rt.reg.Refresh(context.Background())
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Refresh = %v, want a deadline error", err)
			}
			if got := rt.failOpens(); fmt.Sprint(got) != "[postgres_timeout]" {
				t.Fatalf("fail-opens = %v", got)
			}
		})
	}
}

func TestRegistryCancelledIsNotAFailedRound(t *testing.T) {
	rt := newRegistryTest(t, RegistryOptions{QueryTimeout: time.Minute})
	rt.loader.set(func(f *fakeLoader) { f.block = true })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := rt.reg.Refresh(ctx); err == nil {
		t.Fatal("Refresh: nil error")
	}
	if got := rt.failOpens(); len(got) != 0 {
		t.Fatalf("fail-opens = %v, want none on shutdown", got)
	}
}

func TestRegistryOverLimitKeepsSnapshot(t *testing.T) {
	rt := newRegistryTest(t, RegistryOptions{MaxVersions: 2}, record(t, "a", 1, StatusActive, matchSpec()))
	rt.mustRefresh(t)
	first := rt.reg.Snapshot()
	rt.loader.set(func(f *fakeLoader) {
		f.stamp = "s2"
		for v := 2; v <= 3; v++ {
			f.recs = append(f.recs, record(t, "a", v, StatusActive, matchSpec()))
		}
	})
	for range 2 {
		if err := rt.reg.Refresh(context.Background()); err == nil {
			t.Fatal("Refresh: nil error")
		}
	}
	if rt.reg.Snapshot() != first {
		t.Fatal("over-limit load replaced the snapshot")
	}
	if got := rt.failOpens(); fmt.Sprint(got) != "[snapshot_too_large snapshot_too_large]" {
		t.Fatalf("fail-opens = %v", got)
	}
	if rt.loader.limits[len(rt.loader.limits)-1] != 2 {
		t.Fatalf("limits = %v", rt.loader.limits)
	}
}

func TestRegistryInvalidRecordSkipped(t *testing.T) {
	bad := matchSpec()
	bad.Model = ""
	rt := newRegistryTest(t, RegistryOptions{},
		record(t, "a", 1, StatusActive, matchSpec()),
		record(t, "a", 2, StatusActive, bad))
	rt.mustRefresh(t)
	if snap := rt.reg.Snapshot(); snap.Len() != 1 {
		t.Fatalf("Len = %d, want 1", snap.Len())
	}
	if fmt.Sprint(rt.invalid) != "[a/2]" {
		t.Fatalf("OnInvalid calls = %v", rt.invalid)
	}
	rt.mustRefresh(t) // unchanged stamp: not reported again
	if len(rt.invalid) != 1 {
		t.Fatalf("OnInvalid calls = %v", rt.invalid)
	}
	if len(rt.failOpens()) != 0 {
		t.Fatalf("an invalid record failed the round: %v", rt.failOpens())
	}
}

func TestRegistryRunSurvivesPanic(t *testing.T) {
	rt := newRegistryTest(t, RegistryOptions{Interval: time.Millisecond})
	rt.loader.set(func(f *fakeLoader) { f.panicky = true })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); rt.reg.Run(ctx) }()
	waitFor(t, func() bool { return len(rt.failOpens()) >= 2 })
	cancel()
	<-done
	if got := rt.failOpens(); got[0] != ReasonPostgresError {
		t.Fatalf("fail-opens = %v", got)
	}
}

func TestRegistryRunKickAndStop(t *testing.T) {
	rt := newRegistryTest(t, RegistryOptions{Interval: time.Hour}, record(t, "a", 1, StatusActive, matchSpec()))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); rt.reg.Run(ctx) }()
	waitFor(t, func() bool { return rt.reg.Snapshot() != nil })

	rt.loader.set(func(f *fakeLoader) {
		f.stamp = "s2"
		f.recs = append(f.recs, record(t, "b", 1, StatusActive, matchSpec()))
	})
	rt.reg.Kick()
	rt.reg.Kick() // never blocks
	waitFor(t, func() bool { return rt.reg.Snapshot().Len() == 2 })
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestRegistryConcurrentReaders(t *testing.T) {
	rt := newRegistryTest(t, RegistryOptions{Interval: time.Millisecond}, record(t, "a", 1, StatusActive, matchSpec()))
	body := chatBody([2]any{"system", "x"})
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() { rt.reg.Run(ctx) })
	wg.Go(func() {
		for i := 2; ctx.Err() == nil; i++ {
			rt.loader.set(func(f *fakeLoader) {
				f.stamp = fmt.Sprint(i)
				f.recs = []Record{record(t, "a", 1, StatusActive, matchSpec()), record(t, "a", i, StatusActive, matchSpec())}
			})
			rt.reg.Kick()
			time.Sleep(100 * time.Microsecond)
		}
	})
	var reads atomic.Int64
	for range 4 {
		wg.Go(func() {
			for ctx.Err() == nil {
				snap := rt.reg.Snapshot()
				snap.Match("a", "1", body)
				_ = snap.Versions()
				_ = rt.reg.Age()
				reads.Add(1)
			}
		})
	}
	waitFor(t, func() bool {
		stamps, _ := rt.loader.counts()
		return stamps > 20 && reads.Load() > 1000
	})
	cancel()
	wg.Wait()
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(time.Millisecond)
	}
}
