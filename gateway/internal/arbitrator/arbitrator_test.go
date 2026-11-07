package arbitrator

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

func testConfig() Config {
	return Config{
		Classes: []Class{
			{Name: "interactive", MaxWait: 2 * time.Second, MaxQueue: 8},
			{Name: "user", MaxWait: 2 * time.Second, MaxQueue: 8},
			{Name: "background", MaxWait: 2 * time.Second, MaxQueue: 8},
		},
		RetryAfterMax: 30 * time.Second,
	}
}

func newTest(t *testing.T, cfg Config, perReplica int) *Arbitrator {
	t.Helper()
	return New(cfg, []Pool{{Name: "p", PerReplica: perReplica}}, func(string) int { return 1 }, telemetry.NewMetrics())
}

func mustAcquire(t *testing.T, a *Arbitrator, class string) *Slot {
	t.Helper()
	s, err := a.Acquire(context.Background(), "p", class)
	if err != nil {
		t.Fatalf("Acquire(%s): %v", class, err)
	}
	return s
}

// waitQueued blocks until the pool has n queued requests.
func waitQueued(t *testing.T, a *Arbitrator, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, q := a.Demand("p"); q == n {
			return
		}
		if time.Now().After(deadline) {
			_, q := a.Demand("p")
			t.Fatalf("queued = %d, want %d", q, n)
		}
		time.Sleep(time.Millisecond)
	}
}

func assertDrained(t *testing.T, a *Arbitrator) {
	t.Helper()
	if in, q := a.Demand("p"); in != 0 || q != 0 {
		t.Fatalf("Demand = %d in flight, %d queued; want 0, 0", in, q)
	}
}

func TestAdmitUpToLimitThenServeHighestClassFirst(t *testing.T) {
	a := newTest(t, testConfig(), 1)
	held := mustAcquire(t, a, "background")

	order := make(chan string, 4)
	var wg sync.WaitGroup
	enqueue := func(class, label string, queued int) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := mustAcquire(t, a, class)
			order <- label
			s.Release()
		}()
		waitQueued(t, a, queued)
	}
	enqueue("background", "bg", 1)
	enqueue("user", "user-1", 2)
	enqueue("interactive", "int", 3)
	enqueue("user", "user-2", 4)

	held.Release()
	wg.Wait()
	close(order)
	var got []string
	for l := range order {
		got = append(got, l)
	}
	want := []string{"int", "user-1", "user-2", "bg"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("service order = %v, want %v", got, want)
		}
	}
	assertDrained(t, a)
}

func TestQueueFullAnsweredAtOnceWith429(t *testing.T) {
	cfg := testConfig()
	cfg.Classes[2].MaxQueue = 1
	a := newTest(t, cfg, 1)
	held := mustAcquire(t, a, "user")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		s, _ := a.Acquire(ctx, "p", "background")
		s.Release()
	}()
	waitQueued(t, a, 1)

	start := time.Now()
	_, err := a.Acquire(context.Background(), "p", "background")
	var rej *Rejection
	if !errors.As(err, &rej) || rej.Status != http.StatusTooManyRequests || rej.Outcome != telemetry.AdmissionQueueFull {
		t.Fatalf("err = %v, want a 429 queue_full rejection", err)
	}
	if el := time.Since(start); el > 50*time.Millisecond {
		t.Fatalf("rejection took %s, want within 50ms (AC-M2-3)", el)
	}
	if rej.RetryAfterSeconds() < 1 {
		t.Fatalf("Retry-After = %d, want >= 1", rej.RetryAfterSeconds())
	}
	cancel()
	held.Release()
	waitQueued(t, a, 0)
	waitDrained(t, a)
}

// waitDrained waits for goroutines still releasing slots, then asserts.
func waitDrained(t *testing.T, a *Arbitrator) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if in, q := a.Demand("p"); in == 0 && q == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	assertDrained(t, a)
}

func TestMaxWaitAnswered503WithinBound(t *testing.T) {
	cfg := testConfig()
	cfg.Classes[1].MaxWait = 150 * time.Millisecond
	a := newTest(t, cfg, 1)
	held := mustAcquire(t, a, "user")
	start := time.Now()
	_, err := a.Acquire(context.Background(), "p", "user")
	el := time.Since(start)
	var rej *Rejection
	if !errors.As(err, &rej) || rej.Status != http.StatusServiceUnavailable || rej.Outcome != telemetry.AdmissionQueueTimeout {
		t.Fatalf("err = %v, want a 503 queue_timeout rejection", err)
	}
	if el < 150*time.Millisecond || el > 250*time.Millisecond {
		t.Fatalf("waited %s, want max wait 150ms (+100ms at most, AC-M2-3)", el)
	}
	held.Release()
	assertDrained(t, a)
}

func TestMaxWaitZeroNeverQueues(t *testing.T) {
	cfg := testConfig()
	cfg.Classes[2].MaxWait = 0
	a := newTest(t, cfg, 1)
	held := mustAcquire(t, a, "user")
	_, err := a.Acquire(context.Background(), "p", "background")
	var rej *Rejection
	if !errors.As(err, &rej) || rej.Status != http.StatusTooManyRequests {
		t.Fatalf("err = %v, want 429", err)
	}
	held.Release()
	assertDrained(t, a)
}

func TestCallerLeavingWhileQueuedLeaksNothing(t *testing.T) {
	a := newTest(t, testConfig(), 1)
	held := mustAcquire(t, a, "user")
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		_, err := a.Acquire(ctx, "p", "user")
		errc <- err
	}()
	waitQueued(t, a, 1)
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	held.Release()
	assertDrained(t, a)
}

// TestThousandAbortsLeaveNoSlots covers AC-M2-4 at unit level: 1,000
// callers that leave at random points (queued, just granted, admitted)
// leave no slot or queue entry behind.
func TestThousandAbortsLeaveNoSlots(t *testing.T) {
	cfg := testConfig()
	for i := range cfg.Classes {
		cfg.Classes[i].MaxQueue = 2000
	}
	a := newTest(t, cfg, 4)
	var wg sync.WaitGroup
	classes := []string{"interactive", "user", "background"}
	for i := range 1000 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(i%7)*time.Millisecond)
			defer cancel()
			s, err := a.Acquire(ctx, "p", classes[i%3])
			if err != nil {
				return
			}
			if i%2 == 0 {
				time.Sleep(time.Millisecond)
			}
			s.Release()
			s.Release() // idempotent
		}()
	}
	wg.Wait()
	assertDrained(t, a)
}

func TestMaxShareKeepsRoomForHigherClasses(t *testing.T) {
	cfg := testConfig()
	cfg.Classes[2].MaxShare = 0.5
	a := newTest(t, cfg, 4)
	b1, b2 := mustAcquire(t, a, "background"), mustAcquire(t, a, "background")
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		s, err := a.Acquire(ctx, "p", "background")
		s.Release()
		errc <- err
	}()
	waitQueued(t, a, 1) // a third background request waits: share is 2 of 4

	// A newly arrived interactive request must not queue behind it.
	start := time.Now()
	ia := mustAcquire(t, a, "interactive")
	if time.Since(start) > 50*time.Millisecond {
		t.Fatal("interactive request waited although slots were free")
	}
	b1.Release() // the queued background request takes the freed share
	if err := <-errc; err != nil {
		t.Fatalf("queued background: %v", err)
	}
	cancel()
	b2.Release()
	ia.Release()
	assertDrained(t, a)
}

func TestUnknownPoolFailsOpen(t *testing.T) {
	a := newTest(t, testConfig(), 1)
	s, err := a.Acquire(context.Background(), "other", "user")
	if s != nil || err != nil {
		t.Fatalf("Acquire(unknown pool) = %v, %v; want nil, nil", s, err)
	}
	s.Release() // nil slot is valid
	s.ObserveTTFT(time.Second)
}

func TestUnlimitedPoolNeverQueues(t *testing.T) {
	a := newTest(t, testConfig(), 0)
	var slots []*Slot
	for range 100 {
		slots = append(slots, mustAcquire(t, a, "background"))
	}
	if in, _ := a.Demand("p"); in != 100 {
		t.Fatalf("in flight = %d, want 100", in)
	}
	for _, s := range slots {
		s.Release()
	}
	assertDrained(t, a)
}

func TestCapacityFollowsReplicaCount(t *testing.T) {
	n := 1
	var mu sync.Mutex
	a := New(testConfig(), []Pool{{Name: "p", PerReplica: 2}}, func(string) int { mu.Lock(); defer mu.Unlock(); return n }, nil)
	s1, s2 := mustAcquire(t, a, "user"), mustAcquire(t, a, "user")
	done := make(chan *Slot, 1)
	go func() { done <- mustAcquire(t, a, "user") }()
	waitQueued(t, a, 1)
	mu.Lock()
	n = 2
	mu.Unlock()
	a.Kick() // discovery change
	s3 := <-done
	if got := a.Capacity("p"); got != 4 {
		t.Fatalf("Capacity = %d, want 4", got)
	}
	for _, s := range []*Slot{s1, s2, s3} {
		s.Release()
	}
	assertDrained(t, a)
}

func TestShareLimitsAndGrowthDispatches(t *testing.T) {
	a := newTest(t, testConfig(), 4)
	a.SetShare("p", 1)
	s1 := mustAcquire(t, a, "user")
	done := make(chan *Slot, 1)
	go func() { done <- mustAcquire(t, a, "user") }()
	waitQueued(t, a, 1)
	a.SetShare("p", 3) // the Redis view gave this gateway more
	s2 := <-done
	a.SetShare("p", -1) // view lost: full capacity again
	s3, s4 := mustAcquire(t, a, "user"), mustAcquire(t, a, "user")
	for _, s := range []*Slot{s1, s2, s3, s4} {
		s.Release()
	}
	assertDrained(t, a)
}

func TestRetryAfterFromReleaseRateCapped(t *testing.T) {
	p := &pool{a: &Arbitrator{retryAfterMax: 5 * time.Second}}
	now := time.Unix(1000, 0)
	for range 2 {
		p.rate.add(now)
	}
	p.queuedTotal = 9 // 10 ahead at 2 releases/s: 5s
	if got := p.rejectLocked(503, "x", now.Add(time.Second)).RetryAfter; got != 5*time.Second {
		t.Fatalf("RetryAfter = %s, want 5s", got)
	}
	p.queuedTotal = 99 // capped
	if got := p.rejectLocked(503, "x", now.Add(time.Second)).RetryAfter; got != 5*time.Second {
		t.Fatalf("RetryAfter = %s, want the 5s cap", got)
	}
	empty := &pool{a: &Arbitrator{retryAfterMax: 5 * time.Second}}
	if got := empty.rejectLocked(503, "x", now).RetryAfterSeconds(); got != 1 {
		t.Fatalf("RetryAfterSeconds with no history = %d, want 1", got)
	}
}

func TestRetryAfterIgnoresLowerPriorityQueues(t *testing.T) {
	cfg := Config{
		Classes: []Class{
			{Name: "interactive", MaxWait: 0, MaxQueue: 0, MaxShare: 1},
			{Name: "user", MaxWait: 10 * time.Second, MaxQueue: 16, MaxShare: 1},
			{Name: "background", MaxWait: 10 * time.Second, MaxQueue: 256, MaxShare: 1},
		},
		RetryAfterMax: 30 * time.Second,
	}
	a := newTest(t, cfg, 1)
	now := time.Unix(2000, 0)
	a.now = func() time.Time { return now }
	p := a.pools["p"]
	// 1 release/s over the last second.
	p.rate.add(now)
	now = now.Add(time.Second)

	held := mustAcquire(t, a, "interactive")
	// Queue 20 background waiters behind the 1 held slot.
	ctxs := make([]context.CancelFunc, 20)
	var wg sync.WaitGroup
	for i := range 20 {
		ctx, cancel := context.WithCancel(context.Background())
		ctxs[i] = cancel
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = a.Acquire(ctx, "p", "background")
		}()
	}
	waitQueued(t, a, 20)

	// An interactive arrival (rank 0) has 0 interactive waiters ahead (ahead=1),
	// so Retry-After must be 1s rather than 21s from the background backlog.
	_, err := a.Acquire(context.Background(), "p", "interactive")
	var rej *Rejection
	if !errors.As(err, &rej) || rej.RetryAfterSeconds() != 1 {
		t.Fatalf("interactive Retry-After = %v, want 1s despite 20 background waiters", err)
	}
	for _, cancel := range ctxs {
		cancel()
	}
	wg.Wait()
	held.Release()
	assertDrained(t, a)
}

func TestEarlyRejectUnderSustainedOverload(t *testing.T) {
	cfg := Config{
		Classes: []Class{
			{Name: "interactive", MaxWait: 500 * time.Millisecond, MaxQueue: 32, MaxShare: 1},
		},
		RetryAfterMax: 30 * time.Second,
		EarlyReject:   true,
	}
	a := newTest(t, cfg, 1)
	cur := time.Unix(3000, 0)
	var mu sync.Mutex
	a.now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return cur
	}
	advance := func(d time.Duration) {
		mu.Lock()
		cur = cur.Add(d)
		mu.Unlock()
	}

	// Warm EWMA with 10 releases spaced 400ms apart (ewmaInterval = 0.4s).
	for range 10 {
		s := mustAcquire(t, a, "interactive")
		advance(400 * time.Millisecond)
		s.Release()
	}

	held := mustAcquire(t, a, "interactive")
	// First two waiters enqueue: ahead=1 -> est=0, ahead=2 -> est=1.5*0.4s=600ms > 500ms!
	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan struct{})
	go func() {
		defer close(done1)
		_, _ = a.Acquire(ctx1, "p", "interactive")
	}()
	waitQueued(t, a, 1)

	// Next arrival sees ahead=2, est=(2-0.5)*400ms = 600ms > MaxWait(500ms),
	// so it rejects immediately with 503 queue_timeout instead of stalling 500ms.
	t0 := time.Now()
	_, err := a.Acquire(context.Background(), "p", "interactive")
	var rej *Rejection
	if !errors.As(err, &rej) || rej.Status != http.StatusServiceUnavailable || rej.Outcome != telemetry.AdmissionQueueTimeout {
		t.Fatalf("got %v, want 503 queue_timeout early rejection", err)
	}
	if took := time.Since(t0); took > 50*time.Millisecond {
		t.Fatalf("early reject took %s, want immediate", took)
	}

	cancel1()
	<-done1
	held.Release()
	assertDrained(t, a)
}
