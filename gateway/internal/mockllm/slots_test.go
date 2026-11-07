package mockllm

import (
	"context"
	"math/rand/v2"
	"runtime"
	"sync"
	"testing"
)

func isReady(w *waiter) bool {
	select {
	case <-w.ready:
		return true
	default:
		return false
	}
}

func TestSlotsAdmissionOrder(t *testing.T) {
	tests := []struct {
		name       string
		priorities []int // queued in this arrival order behind one held slot
		want       []int // waiter indices in the order they get the slot
	}{
		{name: "fifo when all equal", priorities: []int{0, 0, 0}, want: []int{0, 1, 2}},
		{name: "lower value first", priorities: []int{10, 1}, want: []int{1, 0}},
		{name: "fifo among equal priorities", priorities: []int{5, 1, 1, 5, -2}, want: []int{4, 1, 2, 0, 3}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newSlots(1)
			if w := s.take(0); w != nil {
				t.Fatal("first take queued with a free slot")
			}
			ws := make([]*waiter, len(tc.priorities))
			for i, p := range tc.priorities {
				if ws[i] = s.take(p); ws[i] == nil {
					t.Fatalf("take %d got a slot while all were held", i)
				}
			}
			for step := range tc.want {
				s.release() // the current holder finishes; the slot moves on
				for i, w := range ws {
					if admitted := indexOf(tc.want[:step+1], i) >= 0; isReady(w) != admitted {
						t.Fatalf("after release %d: waiter %d ready=%v, want admission order %v", step, i, isReady(w), tc.want)
					}
				}
			}
			s.release()
			if inUse, queued := s.state(); inUse != 0 || queued != 0 {
				t.Fatalf("after draining: inUse %d queued %d, want 0 0", inUse, queued)
			}
		})
	}
}

func indexOf(xs []int, x int) int {
	for i, v := range xs {
		if v == x {
			return i
		}
	}
	return -1
}

func TestSlotsCancelledWaiterHoldsNothing(t *testing.T) {
	s := newSlots(1)
	s.take(0)
	gone, next := s.take(1), s.take(2)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if s.wait(ctx, gone) {
		t.Fatal("wait with a cancelled context reported admission")
	}
	if _, queued := s.state(); queued != 1 {
		t.Fatalf("queued = %d after a waiter left, want 1", queued)
	}
	s.release()
	if !isReady(next) {
		t.Fatal("slot skipped the remaining waiter")
	}
	s.release()
	if inUse, queued := s.state(); inUse != 0 || queued != 0 {
		t.Fatalf("inUse %d queued %d, want 0 0", inUse, queued)
	}
}

// TestSlotsHandoffToLeavingWaiter: a slot handed to a waiter whose context
// ended at the same moment must reach the next waiter, not vanish.
func TestSlotsHandoffToLeavingWaiter(t *testing.T) {
	s := newSlots(1)
	s.take(0)
	leaving, next := s.take(0), s.take(0)
	s.release() // hands the slot to leaving
	if !isReady(leaving) {
		t.Fatal("release did not hand the slot over")
	}
	s.abandon(leaving)
	if !isReady(next) {
		t.Fatal("slot handed to a leaving waiter was lost")
	}
	s.release()
	if inUse, queued := s.state(); inUse != 0 || queued != 0 {
		t.Fatalf("inUse %d queued %d, want 0 0", inUse, queued)
	}
}

// TestSlotsConcurrentCancelsDoNotLeak races admissions, releases and
// cancellations; afterwards every slot must be free and the queue empty.
func TestSlotsConcurrentCancelsDoNotLeak(t *testing.T) {
	const limit, workers = 3, 200
	s := newSlots(limit)
	var mu sync.Mutex
	running, peak := 0, 0
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(i), 1))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if r.IntN(3) == 0 {
				cancel()
			}
			if w := s.take(r.IntN(4)); w != nil && !s.wait(ctx, w) {
				return
			}
			mu.Lock()
			running++
			peak = max(peak, running)
			mu.Unlock()
			runtime.Gosched()
			mu.Lock()
			running--
			mu.Unlock()
			s.release()
		}()
	}
	wg.Wait()
	if inUse, queued := s.state(); inUse != 0 || queued != 0 {
		t.Fatalf("inUse %d queued %d after all workers finished, want 0 0", inUse, queued)
	}
	if peak > limit {
		t.Fatalf("%d ran at once, limit %d", peak, limit)
	}
}
