package health

import (
	"sync/atomic"
	"testing"
	"time"
)

// M1-20: waits judged still hung on a replica ejected as stuck_engine get
// their OnStuckEject hook run, once, outside the tracker's locks.

type hook struct{ n atomic.Int32 }

func (h *hook) fn() { h.n.Add(1) }

// TestStuckEjectionRunsHooksOfHungWaits: which waits are cut.
func TestStuckEjectionRunsHooksOfHungWaits(t *testing.T) {
	tests := []struct {
		name string
		// setup returns the waits that must be cut and those that must not.
		setup func(t *testing.T, tr *Tracker, clock *FakeClock) (cut, kept []*FirstChunkWait)
		eject func(tr *Tracker)
	}{
		{
			name: "scan ejection cuts stalled waits only on that replica",
			setup: func(t *testing.T, tr *Tracker, clock *FakeClock) (cut, kept []*FirstChunkWait) {
				a, b := currentReplica(t, tr, "a"), currentReplica(t, tr, "b")
				cut = startN(t, tr, a, 3)
				kept = startN(t, tr, b, 1)
				answered := tr.StartFirstChunkWait(a, nil)
				answered.Received() // past its first chunk: never cut
				kept = append(kept, answered)
				clock.Advance(stuckTimeout)
				return cut, kept
			},
			eject: func(tr *Tracker) { tr.CheckStuckStreams() },
		},
		{
			name: "a wait younger than the timeout is not judged hung",
			setup: func(t *testing.T, tr *Tracker, clock *FakeClock) (cut, kept []*FirstChunkWait) {
				a := currentReplica(t, tr, "a")
				cut = startN(t, tr, a, 3)
				clock.Advance(stuckTimeout - time.Second)
				kept = startN(t, tr, a, 1)
				clock.Advance(time.Second)
				return cut, kept
			},
			eject: func(tr *Tracker) { tr.CheckStuckStreams() },
		},
		{
			name: "manual stuck_engine ejection cuts old waits",
			setup: func(t *testing.T, tr *Tracker, clock *FakeClock) (cut, kept []*FirstChunkWait) {
				cut = startN(t, tr, currentReplica(t, tr, "a"), 2)
				clock.Advance(stuckTimeout)
				kept = startN(t, tr, currentReplica(t, tr, "a"), 1)
				return cut, kept
			},
			eject: func(tr *Tracker) { tr.Eject("a", ReasonStuckEngine) },
		},
		{
			name: "manual ejection keeps pending waits that arrival order excuses",
			setup: func(t *testing.T, tr *Tracker, clock *FakeClock) (cut, kept []*FirstChunkWait) {
				a := currentReplica(t, tr, "a")
				cut = startN(t, tr, a, 2) // older than the answered wait: not excused
				clock.Advance(time.Second)
				kept = startN(t, tr, a, 20) // same age as the answered wait: excused
				answered := tr.StartFirstChunkWait(a, nil)
				clock.Advance(stuckTimeout)
				answered.Received() // the queue is moving
				kept = append(kept, answered)
				return cut, kept
			},
			eject: func(tr *Tracker) { tr.Eject("a", ReasonStuckEngine) },
		},
		{
			name: "scan ejection keeps pending waits that arrival order excuses",
			setup: func(t *testing.T, tr *Tracker, clock *FakeClock) (cut, kept []*FirstChunkWait) {
				a := currentReplica(t, tr, "a")
				cut = startN(t, tr, a, 3)
				clock.Advance(time.Second)
				kept = startN(t, tr, a, 20)
				answered := tr.StartFirstChunkWait(a, nil)
				clock.Advance(stuckTimeout)
				answered.Received()
				kept = append(kept, answered)
				return cut, kept
			},
			eject: func(tr *Tracker) { tr.CheckStuckStreams() },
		},
		{
			name: "another ejection reason cuts nothing",
			setup: func(t *testing.T, tr *Tracker, clock *FakeClock) (cut, kept []*FirstChunkWait) {
				kept = startN(t, tr, currentReplica(t, tr, "a"), 3)
				clock.Advance(stuckTimeout)
				return nil, kept
			},
			eject: func(tr *Tracker) { tr.Eject("a", ReasonHealthCheckFail) },
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tr, clock, _ := stuckTracker(t, nil)
			cut, kept := tc.setup(t, tr, clock)
			hooks := map[*FirstChunkWait]*hook{}
			for _, w := range append(append([]*FirstChunkWait{}, cut...), kept...) {
				h := &hook{}
				hooks[w] = h
				w.OnStuckEject(h.fn)
			}
			tc.eject(tr)
			tc.eject(tr) // a second scan or report never cuts twice
			for _, w := range cut {
				if n := hooks[w].n.Load(); n != 1 {
					t.Errorf("hung wait hook ran %d times, want 1", n)
				}
			}
			for _, w := range kept {
				if n := hooks[w].n.Load(); n != 0 {
					t.Errorf("wait that is not hung had its hook run %d times", n)
				}
			}
			// Waits still balance: ending every wait empties the registry.
			endAll(cut)
			endAll(kept)
			if n := tr.FirstChunkWaits(); n != 0 {
				t.Fatalf("waits = %d after every wait ended, want 0", n)
			}
		})
	}
}

// TestOnStuckEjectNeverRunsAfterEnd: Received and End drop the hook, so a
// finished request is never cancelled later, and a nil wait is safe.
func TestOnStuckEjectNeverRunsAfterEnd(t *testing.T) {
	tr, clock, _ := stuckTracker(t, nil)
	a := currentReplica(t, tr, "a")
	ws := startN(t, tr, a, 5)
	h := &hook{}
	for _, w := range ws {
		w.OnStuckEject(h.fn)
	}
	ws[0].Received()
	ws[1].End()
	ws[0].OnStuckEject(h.fn) // after the end: ignored
	var none *FirstChunkWait
	none.OnStuckEject(h.fn)
	clock.Advance(stuckTimeout)
	tr.CheckStuckStreams()
	if tr.IsHealthy("a") {
		t.Fatal("a not ejected by 3 stalls")
	}
	if n := h.n.Load(); n != 3 {
		t.Fatalf("hooks run = %d, want 3 (the waits still hung)", n)
	}
	for _, w := range ws {
		if w.onEject != nil {
			t.Error("a wait kept its hook after the cut or its end: the closure would leak")
		}
	}
	endAll(ws)
}
