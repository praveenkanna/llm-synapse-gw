package runner

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/harness/internal/outcome"
)

// Histogram bounds. Buckets grow geometrically by histGrowth, so a reported
// quantile is within 1% of the true value; values below histMin share bucket 0
// and values above histMax share the last bucket. Memory is fixed
// (histBuckets counters) no matter how many values are recorded.
const (
	histGrowth  = 1.01
	histMin     = time.Microsecond
	histMax     = 30 * time.Minute
	histBuckets = 2144 // 1 (< histMin) + ceil(ln(histMax/histMin)/ln(histGrowth)) = 2142, + 1 overflow; checked by a test
)

var logHistGrowth = math.Log(histGrowth)

// Histogram is a fixed-size, lock-free latency histogram. All methods are safe
// for concurrent use. The zero value is ready to use.
type Histogram struct {
	counts [histBuckets]atomic.Uint64
	n      atomic.Uint64
	sum    atomic.Int64 // nanoseconds; overflows only after ~292 years of summed latency
	max    atomic.Int64
	min    atomic.Int64 // stored as value+1 so that 0 means "unset"
}

func bucketFor(d time.Duration) int {
	if d < histMin {
		return 0
	}
	i := 1 + int(math.Log(float64(d)/float64(histMin))/logHistGrowth)
	if i >= histBuckets {
		return histBuckets - 1
	}
	return i
}

// bucketUpper is the largest value that falls in bucket i.
func bucketUpper(i int) time.Duration {
	if i == 0 {
		return histMin
	}
	return time.Duration(float64(histMin) * math.Pow(histGrowth, float64(i)))
}

// Record adds one observation. Negative values are recorded as zero.
func (h *Histogram) Record(d time.Duration) {
	if d < 0 {
		d = 0
	}
	h.counts[bucketFor(d)].Add(1)
	h.n.Add(1)
	h.sum.Add(int64(d))
	for {
		cur := h.max.Load()
		if int64(d) <= cur || h.max.CompareAndSwap(cur, int64(d)) {
			break
		}
	}
	for {
		cur := h.min.Load()
		if (cur != 0 && int64(d)+1 >= cur) || h.min.CompareAndSwap(cur, int64(d)+1) {
			break
		}
	}
}

// Count returns the number of recorded values.
func (h *Histogram) Count() uint64 { return h.n.Load() }

// Quantile returns the q-quantile (0 < q <= 1) of recorded values, accurate to
// within 1% and clamped to the observed min and max. It returns 0 when empty.
func (h *Histogram) Quantile(q float64) time.Duration {
	n := h.n.Load()
	if n == 0 {
		return 0
	}
	rank := uint64(math.Ceil(q * float64(n)))
	if rank < 1 {
		rank = 1
	}
	var seen uint64
	v := time.Duration(h.max.Load())
	for i := range h.counts {
		seen += h.counts[i].Load()
		if seen >= rank {
			v = bucketUpper(i)
			break
		}
	}
	lo, hi := time.Duration(h.min.Load()-1), time.Duration(h.max.Load())
	return min(max(v, lo), hi)
}

// Snapshot summarises the histogram.
func (h *Histogram) Snapshot() outcome.Stats {
	n := h.n.Load()
	if n == 0 {
		return outcome.Stats{}
	}
	return outcome.Stats{
		Count:  n,
		MeanMS: ms(time.Duration(h.sum.Load() / int64(n))),
		P50MS:  ms(h.Quantile(0.50)),
		P95MS:  ms(h.Quantile(0.95)),
		P99MS:  ms(h.Quantile(0.99)),
		MaxMS:  ms(time.Duration(h.max.Load())),
	}
}

// ms converts a duration to fractional milliseconds (outcome.MS).
func ms(d time.Duration) float64 { return outcome.MS(d) }
