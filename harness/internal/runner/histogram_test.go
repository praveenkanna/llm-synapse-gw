package runner

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/harness/internal/outcome"
)

func TestHistogramBucketsCoverRange(t *testing.T) {
	if got := bucketFor(histMax); got >= histBuckets-1 {
		t.Fatalf("histMax lands in bucket %d, want below the overflow bucket %d", got, histBuckets-1)
	}
	if bucketUpper(bucketFor(histMax)) < histMax {
		t.Fatalf("bucket for histMax has upper bound %v < %v", bucketUpper(bucketFor(histMax)), histMax)
	}
	if got := bucketFor(10 * time.Hour); got != histBuckets-1 {
		t.Fatalf("overflow value in bucket %d, want %d", got, histBuckets-1)
	}
	for _, d := range []time.Duration{0, time.Nanosecond, histMin - 1} {
		if got := bucketFor(d); got != 0 {
			t.Errorf("bucketFor(%v) = %d, want 0", d, got)
		}
	}
}

func TestHistogramQuantiles(t *testing.T) {
	tests := []struct {
		name   string
		values func(i int) time.Duration
		n      int
		want   map[float64]time.Duration
	}{
		{
			name:   "uniform 1..10000 ms",
			values: func(i int) time.Duration { return time.Duration(i+1) * time.Millisecond },
			n:      10000,
			want:   map[float64]time.Duration{0.5: 5000 * time.Millisecond, 0.95: 9500 * time.Millisecond, 0.99: 9900 * time.Millisecond, 1: 10000 * time.Millisecond},
		},
		{
			name:   "constant is exact",
			values: func(int) time.Duration { return 1234567 * time.Nanosecond },
			n:      100,
			want:   map[float64]time.Duration{0.5: 1234567 * time.Nanosecond, 0.99: 1234567 * time.Nanosecond},
		},
		{
			name:   "sub-microsecond clamps to min",
			values: func(int) time.Duration { return 300 * time.Nanosecond },
			n:      10,
			want:   map[float64]time.Duration{0.5: 300 * time.Nanosecond},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var h Histogram
			for i := range tc.n {
				h.Record(tc.values(i))
			}
			if h.Count() != uint64(tc.n) {
				t.Fatalf("count = %d, want %d", h.Count(), tc.n)
			}
			for q, want := range tc.want {
				got := h.Quantile(q)
				if rel := math.Abs(float64(got-want)) / float64(want); rel > 0.011 {
					t.Errorf("q%.2f = %v, want %v (±1%%), off by %.2f%%", q, got, want, 100*rel)
				}
			}
		})
	}
}

func TestHistogramEmptyAndNegative(t *testing.T) {
	var h Histogram
	if got := h.Quantile(0.99); got != 0 {
		t.Fatalf("empty quantile = %v, want 0", got)
	}
	if s := h.Snapshot(); s != (outcome.Stats{}) {
		t.Fatalf("empty snapshot = %+v", s)
	}
	h.Record(-time.Second)
	if got := h.Quantile(1); got != 0 {
		t.Fatalf("negative recorded as %v, want 0", got)
	}
}

func TestHistogramConcurrent(t *testing.T) {
	var h Histogram
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 1000 {
				h.Record(time.Duration(g*1000+i+1) * time.Microsecond)
			}
		})
	}
	wg.Wait()
	s := h.Snapshot()
	if s.Count != 8000 || s.MaxMS != 8 {
		t.Fatalf("snapshot = %+v, want count 8000 and max 8ms", s)
	}
	if got := h.Quantile(0); got < time.Microsecond || got > 1010*time.Nanosecond {
		t.Fatalf("lowest quantile = %v, want 1µs (+1%%)", got)
	}
}
