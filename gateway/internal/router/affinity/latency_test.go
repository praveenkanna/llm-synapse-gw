//go:build !race

package affinity_test

import (
	"runtime"
	"testing"
	"time"
)

// p99Limit is AC-M1-5's route-decision budget; key extraction is most of it.
const p99Limit = 100 * time.Microsecond

// TestExtractP99 proves AC-M1-5 for key extraction: p99 ≤ 100 µs at 1, 32
// and 256 KiB system prompts, 1 MiB of tools ahead of the messages, and the
// worst shapes. Timing needs a build without the race detector and a quiet
// machine, so it is skipped under -race and -short; each case gets five
// attempts so one burst of contention from other processes does not fail it.
func TestExtractP99(t *testing.T) {
	if testing.Short() {
		t.Skip("timing test skipped under -short")
	}
	for _, tc := range append(latencyCases(), worstCases()...) {
		t.Run(tc.Name, func(t *testing.T) {
			measure(tc.Body, 50) // warm up
			best := time.Duration(1<<63 - 1)
			for range 5 {
				runtime.GC()
				lat := measure(tc.Body, 1000)
				p99 := quantile(lat, 0.99)
				best = min(best, p99)
				if p99 <= p99Limit {
					break
				}
			}
			t.Logf("p99 %.1f µs (%d bytes)", micros(best), len(tc.Body))
			if best > p99Limit {
				t.Errorf("p99 %v > %v", best, p99Limit)
			}
		})
	}
}
