package templates

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestIDBoundMatchesThePattern(t *testing.T) {
	if MaxIDBytes != 128 {
		t.Errorf("MaxIDBytes = %d: a change moves what is looked up, and the results file", MaxIDBytes)
	}
	if !ValidID(strings.Repeat("a", MaxIDBytes)) {
		t.Errorf("an ID of %d bytes is not valid, so the bound is past what a snapshot holds", MaxIDBytes)
	}
	if ValidID(strings.Repeat("a", MaxIDBytes+1)) {
		t.Errorf("an ID of %d bytes is valid: a snapshot could hold one the bound never finds", MaxIDBytes+1)
	}
}

func TestMatchIDAtTheBound(t *testing.T) {
	atLimit := strings.Repeat("a", MaxIDBytes)
	snap := mustSnapshot(t, record(t, atLimit, 1, StatusActive, matchSpec()))
	body := chatBody([2]any{"system", "x"})
	if _, reason := snap.Match(atLimit, "1", body); reason == ReasonUnknownTemplate {
		t.Errorf("an ID at the limit was not looked up")
	}
	if v, reason := snap.Match(atLimit+"a", "1", body); v != nil || reason != ReasonUnknownTemplate {
		t.Errorf("one over: %v, %q", v, reason)
	}
	var none *Snapshot
	if _, reason := none.Match(atLimit+"a", "1", body); reason != ReasonRegistryUnavailable {
		t.Errorf("no snapshot: %q, want registry_unavailable as for any ID", reason)
	}
}

func matchCost(s *Snapshot, id string, runs int) time.Duration {
	best := time.Hour
	for range runs {
		start := time.Now()
		s.Match(id, "1", nil)
		if d := time.Since(start); d < best {
			best = d
		}
	}
	return best
}

// manyTemplates is a snapshot past the eight entries a Go map scans by
// length without hashing the key.
func manyTemplates(t testing.TB) *Snapshot {
	t.Helper()
	recs := make([]Record, 0, 32)
	for i := range 32 {
		recs = append(recs, record(t, "support-"+strconv.Itoa(i), 1, StatusActive, matchSpec()))
	}
	return mustSnapshot(t, recs...)
}

// TestMatchCostStopsAtTheBound: an unknown ID costs the same at 1 KiB as at
// 1 MiB. A lookup hashes it, 1,024 times as much.
func TestMatchCostStopsAtTheBound(t *testing.T) {
	const maxRatio = 8
	snap := manyTemplates(t)
	short, long := strings.Repeat("a", 1<<10), strings.Repeat("a", 1<<20)
	var base, huge time.Duration
	for range 5 {
		base, huge = matchCost(snap, short, 500), matchCost(snap, long, 500)
		if huge <= maxRatio*max(base, 50*time.Nanosecond) {
			return
		}
	}
	t.Fatalf("a match took %v with a 1 MiB ID, %v with 1 KiB", huge, base)
}

func TestVersionHeaderBound(t *testing.T) {
	if maxVersionHeaderBytes != 32 {
		t.Errorf("maxVersionHeaderBytes = %d: a change moves what is read, and the results file", maxVersionHeaderBytes)
	}
	atLimit := strings.Repeat("0", maxVersionHeaderBytes-1) + "7"
	if n, reason := parseVersion(atLimit); n != 7 || reason != "" {
		t.Errorf("at the limit: %d, %q", n, reason)
	}
	if n, reason := parseVersion("0" + atLimit); n != 0 || reason != ReasonBadVersion {
		t.Errorf("one over: %d, %q; want bad_version", n, reason)
	}
}

// TestVersionCostStopsAtTheBound: digits for the scan, non-ASCII space for
// the trim. The sizes are 256 times apart.
func TestVersionCostStopsAtTheBound(t *testing.T) {
	const maxRatio = 8
	cost := func(h string) time.Duration {
		best := time.Hour
		for range 100 {
			start := time.Now()
			parseVersion(h)
			if d := time.Since(start); d < best {
				best = d
			}
		}
		return best
	}
	for name, unit := range map[string]string{"digits": "00", "space": " "} {
		t.Run(name, func(t *testing.T) {
			short, long := strings.Repeat(unit, 2<<10), strings.Repeat(unit, 512<<10)
			var base, huge time.Duration
			for range 5 {
				base, huge = cost(short), cost(long)
				if huge <= maxRatio*max(base, 50*time.Nanosecond) {
					return
				}
			}
			t.Fatalf("a parse took %v at 1 MiB, %v at 4 KiB", huge, base)
		})
	}
}

// BenchmarkMatchUnknownID times the lookup at and past the limit
// (docs/results). Sizes are literal, so a run before the bound compares
// like for like.
func BenchmarkMatchUnknownID(b *testing.B) {
	snap := manyTemplates(b)
	for _, size := range []int{128, 8 << 10, 1 << 20} {
		id := strings.Repeat("a", size)
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				snap.Match(id, "1", nil)
			}
		})
	}
}
