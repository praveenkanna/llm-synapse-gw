package arbitrator

import (
	"sync"
	"sync/atomic"
	"time"
)

// ShedConfig configures shedding on the highest class's TTFT p95.
type ShedConfig struct {
	Enabled bool
	// Threshold is the p95 TTFT over Window above which shedding starts;
	// it stops below Threshold × RecoverRatio.
	Threshold    time.Duration
	Window       time.Duration
	MinSamples   int
	RecoverRatio float64
	// Classes are shed; the highest class never is.
	Classes []string
}

const (
	// shedMaxSamples bounds the samples a tracker keeps (the newest win),
	// so its memory is fixed whatever the request rate.
	shedMaxSamples = 4096
	// shedEvalInterval spaces p95 evaluations: each sorts up to
	// shedMaxSamples values (~100 µs).
	shedEvalInterval = 250 * time.Millisecond
)

type ttftSample struct {
	at time.Time
	d  time.Duration
}

// shedTracker keeps a pool's recent highest-class TTFT samples and decides
// whether the pool sheds, with hysteresis. Fewer than MinSamples samples
// in the window never shed (fail open).
type shedTracker struct {
	cfg      ShedConfig
	shedding atomic.Bool

	mu       sync.Mutex
	ring     [shedMaxSamples]ttftSample
	head, n  int
	lastEval time.Time
	scratch  []time.Duration
}

func newShedTracker(cfg ShedConfig) *shedTracker {
	return &shedTracker{cfg: cfg, scratch: make([]time.Duration, 0, shedMaxSamples)}
}

func (t *shedTracker) on() bool { return t.shedding.Load() }

// observe records a sample and re-evaluates if due; it reports whether
// the shedding state changed.
func (t *shedTracker) observe(now time.Time, d time.Duration) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.ring[t.head] = ttftSample{at: now, d: d}
	t.head = (t.head + 1) % shedMaxSamples
	t.n = min(t.n+1, shedMaxSamples)
	return t.evaluateLocked(now)
}

// evaluate re-evaluates if due and reports whether the state changed.
func (t *shedTracker) evaluate(now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.evaluateLocked(now)
}

func (t *shedTracker) evaluateLocked(now time.Time) bool {
	if now.Sub(t.lastEval) < shedEvalInterval {
		return false
	}
	t.lastEval = now
	want := false
	if p95, ok := t.p95Locked(now); ok {
		limit := t.cfg.Threshold
		if t.shedding.Load() {
			limit = time.Duration(float64(limit) * t.cfg.RecoverRatio)
		}
		want = p95 > limit
	}
	return t.shedding.Swap(want) != want
}

// p95Locked returns the p95 of the samples within the window, and false
// with fewer than MinSamples of them.
func (t *shedTracker) p95Locked(now time.Time) (time.Duration, bool) {
	from := now.Add(-t.cfg.Window)
	s := t.scratch[:0]
	for i := range t.n {
		if smp := t.ring[i]; !smp.at.Before(from) {
			s = append(s, smp.d)
		}
	}
	if len(s) == 0 || len(s) < t.cfg.MinSamples {
		return 0, false
	}
	return quickselectDuration(s, (len(s)*95+99)/100-1), true
}

// quickselectDuration partitions a in place so that the k-th smallest duration
// (0-indexed) is returned in O(N) time using 3-way Dutch-National-Flag
// partitioning with median-of-three pivot selection.
func quickselectDuration(a []time.Duration, k int) time.Duration {
	lo, hi := 0, len(a)-1
	for hi-lo > 24 {
		mid := lo + (hi-lo)/2
		if a[mid] < a[lo] {
			a[lo], a[mid] = a[mid], a[lo]
		}
		if a[hi] < a[lo] {
			a[lo], a[hi] = a[hi], a[lo]
		}
		if a[hi] < a[mid] {
			a[mid], a[hi] = a[hi], a[mid]
		}
		pivot := a[mid]

		lt, i, gt := lo, lo, hi
		for i <= gt {
			v := a[i]
			if v < pivot {
				a[lt], a[i] = a[i], a[lt]
				lt++
				i++
			} else if v > pivot {
				a[i], a[gt] = a[gt], a[i]
				gt--
			} else {
				i++
			}
		}
		switch {
		case k < lt:
			hi = lt - 1
		case k > gt:
			lo = gt + 1
		default:
			return pivot
		}
	}
	for i := lo + 1; i <= hi; i++ {
		v := a[i]
		j := i - 1
		for j >= lo && a[j] > v {
			a[j+1] = a[j]
			j--
		}
		a[j+1] = v
	}
	return a[k]
}
