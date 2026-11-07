package mockllm

import (
	"container/heap"
	"context"
	"sync"
)

// slots admits up to limit running requests, like vLLM's --max-num-seqs.
// Waiters are admitted lowest priority value first, ties in arrival order;
// under fcfs every request has priority 0, so the order is plain arrival.
// The queue holds at most one waiter per open request.
type slots struct {
	mu      sync.Mutex
	limit   int
	inUse   int
	seq     uint64 // arrival counter, breaks priority ties
	waiters waitQueue
}

func newSlots(limit int) *slots { return &slots{limit: limit} }

// waiter is one queued request. ready is closed when a slot is handed to it.
type waiter struct {
	priority int
	seq      uint64
	ready    chan struct{}
	index    int // position in the heap; -1 once popped or removed
}

// take claims a free slot and returns nil, or queues a waiter for one. While
// waiters are queued every slot is in use (release hands slots to waiters
// before freeing them), so a newcomer never overtakes the queue.
func (s *slots) take(priority int) *waiter {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inUse < s.limit {
		s.inUse++
		return nil
	}
	w := &waiter{priority: priority, seq: s.seq, ready: make(chan struct{})}
	s.seq++
	heap.Push(&s.waiters, w)
	return w
}

// wait blocks until w is handed a slot (true) or ctx ends (false). A waiter
// that gives up holds no slot afterwards.
func (s *slots) wait(ctx context.Context, w *waiter) bool {
	select {
	case <-w.ready:
		return true
	case <-ctx.Done():
		s.abandon(w)
		return false
	}
}

// abandon withdraws w from the queue. If a slot was already handed to it
// (release won the race with ctx), the slot is passed on rather than lost.
func (s *slots) abandon(w *waiter) {
	s.mu.Lock()
	if w.index >= 0 {
		heap.Remove(&s.waiters, w.index)
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	s.release()
}

// release gives a held slot to the first waiter, or frees it.
func (s *slots) release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.waiters.Len() > 0 {
		close(heap.Pop(&s.waiters).(*waiter).ready)
		return
	}
	s.inUse--
}

// state returns slots in use and waiters queued, for tests.
func (s *slots) state() (inUse, queued int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inUse, s.waiters.Len()
}

// waitQueue is a container/heap of waiters ordered by (priority, seq).
type waitQueue []*waiter

func (q waitQueue) Len() int { return len(q) }

func (q waitQueue) Less(i, j int) bool {
	if q[i].priority != q[j].priority {
		return q[i].priority < q[j].priority
	}
	return q[i].seq < q[j].seq
}

func (q waitQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].index = i
	q[j].index = j
}

func (q *waitQueue) Push(x any) {
	w := x.(*waiter)
	w.index = len(*q)
	*q = append(*q, w)
}

func (q *waitQueue) Pop() any {
	old := *q
	n := len(old)
	w := old[n-1]
	old[n-1] = nil
	w.index = -1
	*q = old[:n-1]
	return w
}
