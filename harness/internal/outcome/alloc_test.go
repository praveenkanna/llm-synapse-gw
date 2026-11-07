//go:build !race

package outcome

import "testing"

// TestStreamMeasuringAllocations: an event split across reads costs what a
// whole one does, its JSON decode, plus a small constant to put it back
// together. The decode's own count depends on the toolchain, so the whole
// event is the baseline, with a ceiling above any toolchain's. Not under
// -race, which adds allocations of its own (sync.Pool drops items on
// purpose).
func TestStreamMeasuringAllocations(t *testing.T) {
	const reassembly, wholeCeiling = 2, 16
	b := NewBody(true, t0, nil)
	whole := []byte(tokChunk)
	half := len(tokChunk) / 2
	first, rest := whole[:half], whole[half:]
	wholeEvent := testing.AllocsPerRun(1000, func() {
		b.Write(whole, at(1))
	})
	splitEvent := testing.AllocsPerRun(1000, func() {
		b.Write(first, at(1))
		b.Write(rest, at(2))
	})
	if wholeEvent > wholeCeiling {
		t.Fatalf("%.1f allocations per whole event; want at most %d", wholeEvent, wholeCeiling)
	}
	if splitEvent > wholeEvent+reassembly {
		t.Fatalf("%.1f allocations per split event, %.1f for a whole one; want at most %d more", splitEvent, wholeEvent, reassembly)
	}
}
