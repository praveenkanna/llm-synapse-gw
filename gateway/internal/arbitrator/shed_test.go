package arbitrator

import (
	"context"
	"errors"
	"math/rand/v2"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

func shedCfg() ShedConfig {
	return ShedConfig{
		Enabled:      true,
		Threshold:    100 * time.Millisecond,
		Window:       10 * time.Second,
		MinSamples:   5,
		RecoverRatio: 0.5,
		Classes:      []string{"background"},
	}
}

func TestShedTrackerHysteresis(t *testing.T) {
	tr := newShedTracker(shedCfg())
	now := time.Unix(1000, 0)
	feed := func(d time.Duration, n int) {
		for range n {
			now = now.Add(shedEvalInterval)
			tr.observe(now, d)
		}
	}
	feed(500*time.Millisecond, 4)
	if tr.on() {
		t.Fatal("shedding with fewer than min_samples samples")
	}
	feed(500*time.Millisecond, 2)
	if !tr.on() {
		t.Fatal("not shedding with p95 above the threshold")
	}
	// Between threshold × recover_ratio (50ms) and the threshold: stays on.
	feed(80*time.Millisecond, 200)
	if !tr.on() {
		t.Fatal("stopped shedding above threshold × recover_ratio")
	}
	feed(10*time.Millisecond, 200)
	if tr.on() {
		t.Fatal("still shedding with p95 below threshold × recover_ratio")
	}
}

func TestShedTrackerSamplesAgeOut(t *testing.T) {
	tr := newShedTracker(shedCfg())
	now := time.Unix(1000, 0)
	for range 10 {
		now = now.Add(shedEvalInterval)
		tr.observe(now, time.Second)
	}
	if !tr.on() {
		t.Fatal("not shedding")
	}
	if !tr.evaluate(now.Add(time.Minute)) || tr.on() {
		t.Fatal("still shedding after every sample left the window")
	}
}

func TestShedTrackerBoundedMemory(t *testing.T) {
	tr := newShedTracker(shedCfg())
	now := time.Unix(1000, 0)
	for range 3 * shedMaxSamples {
		tr.observe(now, time.Millisecond)
	}
	if tr.n != shedMaxSamples || cap(tr.scratch) != shedMaxSamples {
		t.Fatalf("n = %d, scratch cap = %d; want both %d", tr.n, cap(tr.scratch), shedMaxSamples)
	}
}

func TestShedRejectsAndEvictsShedClass(t *testing.T) {
	cfg := testConfig()
	cfg.Shed = shedCfg()
	a := newTest(t, cfg, 1)
	held := mustAcquire(t, a, "interactive")
	queued := make(chan error, 1)
	go func() {
		_, err := a.Acquire(context.Background(), "p", "background")
		queued <- err
	}()
	waitQueued(t, a, 1)

	for range 10 {
		held.ObserveTTFT(time.Second)
		a.pools["p"].shed.lastEval = time.Time{} // evaluate on every sample
	}
	var rej *Rejection
	if err := <-queued; !errors.As(err, &rej) || rej.Status != http.StatusTooManyRequests || rej.Outcome != telemetry.AdmissionShed {
		t.Fatalf("queued background got %v, want a 429 shed rejection", err)
	}
	if _, err := a.Acquire(context.Background(), "p", "background"); !errors.As(err, &rej) || rej.Outcome != telemetry.AdmissionShed {
		t.Fatalf("new background got %v, want shed", err)
	}
	// Other classes still queue and get served.
	done := make(chan error, 1)
	go func() {
		s, err := a.Acquire(context.Background(), "p", "user")
		s.Release()
		done <- err
	}()
	waitQueued(t, a, 1)
	held.Release()
	if err := <-done; err != nil {
		t.Fatalf("user while shedding: %v", err)
	}
	assertDrained(t, a)
}

func TestOnlyHighestClassFeedsShedding(t *testing.T) {
	cfg := testConfig()
	cfg.Shed = shedCfg()
	a := newTest(t, cfg, 0)
	s := mustAcquire(t, a, "user")
	for range 20 {
		s.ObserveTTFT(time.Hour)
	}
	if n := a.pools["p"].shed.n; n != 0 {
		t.Fatalf("user TTFT recorded %d samples, want 0", n)
	}
	s.Release()
}

func TestQuickselectMatchesSort(t *testing.T) {
	rng := rand.New(rand.NewPCG(42, 99))
	for _, n := range []int{1, 2, 5, 20, 24, 25, 100, 513, 4096} {
		cases := [][]time.Duration{
			make([]time.Duration, n), // all equal
			make([]time.Duration, n), // ascending
			make([]time.Duration, n), // descending
			make([]time.Duration, n), // few unique values
			make([]time.Duration, n), // pseudo-random
		}
		for i := range n {
			cases[0][i] = 42 * time.Millisecond
			cases[1][i] = time.Duration(i) * time.Millisecond
			cases[2][i] = time.Duration(n-i) * time.Millisecond
			cases[3][i] = time.Duration(i%5) * time.Millisecond
			cases[4][i] = time.Duration(rng.IntN(100_000)) * time.Microsecond
		}
		k := (n*95+99)/100 - 1
		for ci, orig := range cases {
			a := slices.Clone(orig)
			b := slices.Clone(orig)
			slices.Sort(b)
			if got, want := quickselectDuration(a, k), b[k]; got != want {
				t.Fatalf("n=%d case=%d k=%d: quickselect=%s, sort=%s", n, ci, k, got, want)
			}
		}
	}
}
