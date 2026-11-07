//go:build !race

package templates

import (
	"slices"
	"testing"
	"time"
)

// TestMatchLatencyP95 is AC-M3-3's unit-level proof: matching the
// ~4,900-character prompt takes at most 1 ms at p95.
func TestMatchLatencyP95(t *testing.T) {
	const n, limit = 2000, time.Millisecond
	snap, body := benchSnapshot(t), benchBody(t)
	for range 100 { // warm up
		snap.Match("support-answer", "1", body)
	}
	d := make([]time.Duration, n)
	for i := range d {
		start := time.Now()
		v, reason := snap.Match("support-answer", "1", body)
		d[i] = time.Since(start)
		if v == nil {
			t.Fatalf("no match: %s", reason)
		}
	}
	slices.Sort(d)
	p50, p95, p99 := d[n*50/100], d[n*95/100], d[n*99/100]
	t.Logf("Match over %d runs: p50 %v, p95 %v, p99 %v, max %v", n, p50, p95, p99, d[n-1])
	if p95 > limit {
		t.Fatalf("Match p95 = %v, over %v", p95, limit)
	}
}
