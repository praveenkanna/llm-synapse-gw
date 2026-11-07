// Package arbitrator decides when a request may go upstream (M2): per-pool
// admission slots, bounded priority queues with a max wait per class, and
// shedding of the lowest classes while the highest class's time to first
// token is too high.
//
// A pool's capacity is its max_inflight_per_replica (mirroring vLLM's
// --max-num-seqs) times its replica count, or this gateway's share of it
// while the Redis view is fresh (redis.go). A request takes a slot when one
// is free and nobody waits; otherwise it waits in its class's queue, and a
// freed slot goes to the oldest waiter of the highest class. Each class may
// hold at most its max_share of the slots, so long background work cannot
// fill a pool and leave a newly arrived interactive request waiting for it
// to finish.
//
// Answers never hang: a full queue or a shed class is answered at once with
// 429, a wait past the class max wait with 503, both carrying Retry-After
// (Rejection). A caller that leaves while queued gives its place up; a slot
// is released exactly once (Slot.Release is idempotent), so slots return to
// zero when load stops. Every queue is bounded by its class max_queue.
//
// The request path takes one per-pool mutex for a few list operations; it
// never waits on Redis or any I/O.
package arbitrator

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// Class is one priority class's admission bounds.
type Class struct {
	Name string
	// MaxWait is the longest a request waits in the queue; 0 never queues.
	MaxWait time.Duration
	// MaxQueue bounds the class's queue per pool; 0 never queues.
	MaxQueue int
	// MaxShare is the largest fraction of the pool's slots the class may
	// hold, in (0, 1]; 0 means 1.
	MaxShare float64
}

// Config configures an Arbitrator.
type Config struct {
	// Classes lists the priority classes, highest first. A request of a
	// class not listed gets the lowest class's bounds.
	Classes []Class
	// RetryAfterMax caps the Retry-After of a rejection (at least 1s).
	RetryAfterMax time.Duration
	// EarlyReject rejects arrivals with 503 + Retry-After immediately when
	// the EWMA slot-release interval predicts that requests queued ahead in
	// [0..rank] will exceed the class MaxWait.
	EarlyReject bool
	Shed        ShedConfig
}

// Pool is one pool's admission capacity.
type Pool struct {
	Name string
	// PerReplica is the slots per replica; 0 means no limit (never queued).
	PerReplica int
}

// Rejection is an admission refusal, answered with Status and a
// Retry-After header. It is returned as the error of Acquire.
type Rejection struct {
	Status     int
	Outcome    string // telemetry.Admission* outcome
	RetryAfter time.Duration
}

func (r *Rejection) Error() string {
	return fmt.Sprintf("admission: %s (status %d, retry after %s)", r.Outcome, r.Status, r.RetryAfter)
}

// RetryAfterSeconds is the Retry-After header value: whole seconds,
// rounded up, at least 1.
func (r *Rejection) RetryAfterSeconds() int {
	return max(1, int(math.Ceil(r.RetryAfter.Seconds())))
}

// Arbitrator admits requests per pool. It is safe for concurrent use.
type Arbitrator struct {
	classes       []Class
	rank          map[string]int
	retryAfterMax time.Duration
	earlyReject   bool
	shedCfg       ShedConfig
	shedRanks     []bool // by rank
	pools         map[string]*pool
	replicas      func(pool string) int
	m             *telemetry.Metrics
	now           func() time.Time
}

// New builds an Arbitrator. replicas returns a pool's current replica
// count (discovery); m may be nil.
func New(cfg Config, pools []Pool, replicas func(pool string) int, m *telemetry.Metrics) *Arbitrator {
	if len(cfg.Classes) == 0 {
		cfg.Classes = []Class{{Name: telemetry.ClassDefault}}
	}
	a := &Arbitrator{
		classes:       cfg.Classes,
		rank:          make(map[string]int, len(cfg.Classes)),
		retryAfterMax: max(cfg.RetryAfterMax, time.Second),
		earlyReject:   cfg.EarlyReject,
		shedCfg:       cfg.Shed,
		shedRanks:     make([]bool, len(cfg.Classes)),
		pools:         make(map[string]*pool, len(pools)),
		replicas:      replicas,
		m:             m,
		now:           time.Now,
	}
	for i, c := range cfg.Classes {
		a.rank[c.Name] = i
	}
	for _, name := range cfg.Shed.Classes {
		if r, ok := a.rank[name]; ok && r > 0 {
			a.shedRanks[r] = true
		}
	}
	for _, p := range pools {
		a.pools[p.Name] = a.newPool(p)
	}
	return a
}

func (a *Arbitrator) newPool(p Pool) *pool {
	pl := &pool{
		a:          a,
		name:       p.Name,
		perReplica: p.PerReplica,
		queues:     make([]waiterQueue, len(a.classes)),
		inflightBy: make([]int, len(a.classes)),
		lastLimit:  math.MinInt,
	}
	pl.share.Store(-1)
	if a.shedCfg.Enabled {
		pl.shed = newShedTracker(a.shedCfg)
	}
	return pl
}

// rankOf returns class's rank (0 is highest); unknown classes rank lowest.
func (a *Arbitrator) rankOf(class string) int {
	if r, ok := a.rank[class]; ok {
		return r
	}
	return len(a.classes) - 1
}

// Acquire admits a request of class to pool, waiting in the class queue
// if the pool is full. It returns the request's slot, a *Rejection, or
// ctx's error if the caller left while queued. A pool it does not know is
// admitted at once with a nil slot (fail open). The slot must be released.
func (a *Arbitrator) Acquire(ctx context.Context, poolName, class string) (*Slot, error) {
	p := a.pools[poolName]
	if p == nil {
		return nil, nil
	}
	rank := a.rankOf(class)
	start := a.now()
	p.mu.Lock()
	if p.shed != nil && a.shedRanks[rank] && p.shed.evaluate(start) {
		// Shed-class arrivals also re-evaluate, so shedding ends once the
		// highest class's samples age out even if it sends nothing more.
		p.applyShedLocked()
	}
	if p.shedding && a.shedRanks[rank] {
		rej := p.rejectRankLocked(rank, http.StatusTooManyRequests, telemetry.AdmissionShed, start)
		p.mu.Unlock()
		a.m.AdmissionDecision(poolName, class, rej.Outcome)
		return nil, rej
	}
	// After dispatch, whoever still waits has no room (the pool is full or
	// its class is at its share), so a request whose class has room and
	// nobody of its own class ahead takes a slot at once.
	p.dispatchLocked()
	if p.queues[rank].Len() == 0 && p.hasRoomLocked(rank) {
		s := p.admitLocked(rank, class)
		p.mu.Unlock()
		a.m.AdmissionDecision(poolName, class, telemetry.AdmissionAdmitted)
		return s, nil
	}
	c := a.classes[rank]
	q := &p.queues[rank]
	if c.MaxWait <= 0 || q.Len() >= c.MaxQueue {
		rej := p.rejectRankLocked(rank, http.StatusTooManyRequests, telemetry.AdmissionQueueFull, start)
		p.mu.Unlock()
		a.m.AdmissionDecision(poolName, class, rej.Outcome)
		return nil, rej
	}
	if a.earlyReject && p.shouldEarlyRejectLocked(rank, c.MaxWait, start) {
		rej := p.rejectRankLocked(rank, http.StatusServiceUnavailable, telemetry.AdmissionQueueTimeout, start)
		p.mu.Unlock()
		a.m.ObserveQueueWait(poolName, class, 0)
		a.m.AdmissionDecision(poolName, class, rej.Outcome)
		return nil, rej
	}
	w := waiterPool.Get().(*waiter)
	*w = waiter{pool: p, rank: rank, class: class, enqueued: start, ch: w.ch}
	q.pushBack(w)
	p.queuedTotal++
	p.mu.Unlock()
	a.m.QueueDepthAdd(poolName, class, 1)

	timer := acquireTimer(c.MaxWait)
	defer releaseTimer(timer)
	defer func() {
		*w = waiter{ch: w.ch}
		waiterPool.Put(w)
	}()
	select {
	case s := <-w.ch:
		return a.granted(s, w, ctx)
	case <-timer.C:
		if !p.leave(w) {
			return a.granted(<-w.ch, w, ctx)
		}
		a.m.ObserveQueueWait(poolName, class, a.now().Sub(start).Seconds())
		p.mu.Lock()
		rej := p.rejectRankLocked(rank, http.StatusServiceUnavailable, telemetry.AdmissionQueueTimeout, a.now())
		p.mu.Unlock()
		a.m.AdmissionDecision(poolName, class, rej.Outcome)
		return nil, rej
	case <-ctx.Done():
		if !p.leave(w) {
			<-w.ch // granted or rejected meanwhile; give a slot straight back
			if w.slot != nil {
				w.slot.Release()
			}
		}
		a.m.ObserveQueueWait(poolName, class, a.now().Sub(start).Seconds())
		a.m.AdmissionDecision(poolName, class, telemetry.AdmissionClientGone)
		return nil, ctx.Err()
	}
}

// granted finishes a wait that ended with a grant (s) or a rejection
// (s == nil, w.rej set) handed over by the pool.
func (a *Arbitrator) granted(s *Slot, w *waiter, ctx context.Context) (*Slot, error) {
	p := w.pool
	wait := a.now().Sub(w.enqueued)
	a.m.ObserveQueueWait(p.name, w.class, wait.Seconds())
	if s == nil {
		a.m.AdmissionDecision(p.name, w.class, w.rej.Outcome)
		return nil, w.rej
	}
	s.queueWait = wait
	if ctx.Err() != nil {
		s.Release()
		a.m.AdmissionDecision(p.name, w.class, telemetry.AdmissionClientGone)
		return nil, ctx.Err()
	}
	a.m.AdmissionDecision(p.name, w.class, telemetry.AdmissionAdmitted)
	return s, nil
}

// Kick re-runs dispatch on every pool, after a capacity change (replicas
// added, a new Redis share).
func (a *Arbitrator) Kick() {
	for _, p := range a.pools {
		p.mu.Lock()
		p.dispatchLocked()
		p.mu.Unlock()
	}
}

// Demand returns pool's slots in use and requests queued.
func (a *Arbitrator) Demand(poolName string) (inflight, queued int) {
	p := a.pools[poolName]
	if p == nil {
		return 0, 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.inflight, p.queuedTotal
}

// ShareDemand is pool's demand for a share of the capacity across gateways:
// per class, slots in use plus queued, but no more than the class could
// hold (its max_share of the full capacity). A deep background backlog then
// cannot win a gateway slots that only higher classes could use.
func (a *Arbitrator) ShareDemand(poolName string) int {
	p := a.pools[poolName]
	if p == nil {
		return 0
	}
	limit := p.capacity()
	p.mu.Lock()
	defer p.mu.Unlock()
	total := 0
	for rank := range a.classes {
		d := p.inflightBy[rank] + p.queues[rank].Len()
		if s := a.classes[rank].MaxShare; limit >= 0 && s > 0 && s < 1 {
			d = min(d, max(1, int(s*float64(limit))))
		}
		total += d
	}
	return total
}

// Capacity returns pool's full capacity (all gateways together), or -1
// for no limit.
func (a *Arbitrator) Capacity(poolName string) int {
	p := a.pools[poolName]
	if p == nil {
		return -1
	}
	return p.capacity()
}

// SetShare sets this gateway's share of pool's capacity (the Redis view);
// a negative share clears it, so the full capacity applies. The pool is
// dispatched at once, since the share may have grown.
func (a *Arbitrator) SetShare(poolName string, share int) {
	p := a.pools[poolName]
	if p == nil {
		return
	}
	p.share.Store(int64(max(share, -1)))
	p.mu.Lock()
	p.dispatchLocked()
	p.mu.Unlock()
}

// Pools returns the names of the pools with a slot limit.
func (a *Arbitrator) Pools() []string {
	var names []string
	for name, p := range a.pools {
		if p.perReplica > 0 {
			names = append(names, name)
		}
	}
	return names
}

// pool is one pool's admission state, guarded by mu.
type pool struct {
	a          *Arbitrator
	name       string
	perReplica int
	share      atomic.Int64 // this gateway's share; -1 none

	mu          sync.Mutex
	inflight    int
	inflightBy  []int         // by rank
	queues      []waiterQueue // of *waiter, by rank
	queuedTotal int
	shedding    bool
	shed        *shedTracker // nil when shedding is off
	rate        rateWindow   // slot releases per second, for Retry-After
	lastLimit   int

	// Multi-second EWMA of inter-release interval (seconds), used for
	// Retry-After fallback on slow models and opt-in EarlyReject sojourn estimation.
	lastRelease  time.Time
	ewmaInterval float64
	ewmaSamples  int
}

// waiter is a queued request. Once done (under the pool's mutex) it has
// been handed exactly one value on ch: its slot, or nil with rej set.
type waiter struct {
	pool       *pool
	rank       int
	class      string
	enqueued   time.Time
	prev, next *waiter
	ch         chan *Slot
	done       bool
	slot       *Slot
	rej        *Rejection
}

type waiterQueue struct {
	head, tail *waiter
	len        int
}

func (q *waiterQueue) Len() int { return q.len }

func (q *waiterQueue) pushBack(w *waiter) {
	w.prev, w.next = q.tail, nil
	if q.tail != nil {
		q.tail.next = w
	} else {
		q.head = w
	}
	q.tail = w
	q.len++
}

func (q *waiterQueue) popFront() *waiter {
	w := q.head
	if w == nil {
		return nil
	}
	q.remove(w)
	return w
}

func (q *waiterQueue) remove(w *waiter) {
	if w.prev != nil {
		w.prev.next = w.next
	} else {
		q.head = w.next
	}
	if w.next != nil {
		w.next.prev = w.prev
	} else {
		q.tail = w.prev
	}
	w.prev, w.next = nil, nil
	q.len--
}

var waiterPool = sync.Pool{
	New: func() any {
		return &waiter{ch: make(chan *Slot, 1)}
	},
}

var timerPool sync.Pool

func acquireTimer(d time.Duration) *time.Timer {
	if v := timerPool.Get(); v != nil {
		t := v.(*time.Timer)
		t.Reset(d)
		return t
	}
	return time.NewTimer(d)
}

func releaseTimer(t *time.Timer) {
	t.Stop()
	timerPool.Put(t)
}

// capacity is the pool's full capacity, or -1 for no limit.
func (p *pool) capacity() int {
	if p.perReplica <= 0 {
		return -1
	}
	n := 1
	if p.a.replicas != nil {
		n = max(p.a.replicas(p.name), 1)
	}
	return p.perReplica * n
}

// limitLocked is the pool's effective limit on this gateway, or -1.
func (p *pool) limitLocked() int {
	limit := p.capacity()
	if s := int(p.share.Load()); limit >= 0 && s >= 0 && s < limit {
		limit = s
	}
	if limit != p.lastLimit {
		p.lastLimit = limit
		p.a.m.SetAdmissionLimit(p.name, int64(limit))
	}
	return limit
}

// hasRoomLocked reports whether a request of rank may take a slot now.
func (p *pool) hasRoomLocked(rank int) bool {
	limit := p.limitLocked()
	if limit < 0 {
		return true
	}
	if p.inflight >= limit {
		return false
	}
	share := p.a.classes[rank].MaxShare
	if share <= 0 || share >= 1 {
		return true
	}
	return p.inflightBy[rank] < max(1, int(share*float64(limit)))
}

func (p *pool) admitLocked(rank int, class string) *Slot {
	p.inflight++
	p.inflightBy[rank]++
	p.a.m.AdmissionSlotsAdd(p.name, 1)
	return &Slot{pool: p, rank: rank, class: class}
}

// dispatchLocked hands free slots to waiters: per slot, the oldest waiter
// of the highest class that has room under its share.
func (p *pool) dispatchLocked() {
	for p.queuedTotal > 0 {
		granted := false
		for rank := range p.queues {
			q := &p.queues[rank]
			if q.Len() == 0 || !p.hasRoomLocked(rank) {
				continue
			}
			w := q.popFront()
			p.queuedTotal--
			w.done = true
			w.slot = p.admitLocked(rank, w.class)
			p.a.m.QueueDepthAdd(p.name, w.class, -1)
			w.ch <- w.slot
			granted = true
			break
		}
		if !granted {
			return
		}
	}
}

// leave takes w out of its queue if it is still waiting, and reports
// whether it did; false means the pool already handed it a value.
func (p *pool) leave(w *waiter) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if w.done {
		return false
	}
	w.done = true
	p.queues[w.rank].remove(w)
	p.queuedTotal--
	p.a.m.QueueDepthAdd(p.name, w.class, -1)
	return true
}

const (
	ewmaMinSamples = 8
	ewmaMaxIdleGap = 60 * time.Second
	ewmaFreshLimit = 30 * time.Second
	ewmaAlpha      = 0.2
)

func (p *pool) release(s *Slot) {
	now := p.a.now()
	p.mu.Lock()
	p.inflight--
	p.inflightBy[s.rank]--
	p.rate.add(now)
	if !p.lastRelease.IsZero() {
		if gap := now.Sub(p.lastRelease); gap <= ewmaMaxIdleGap {
			dt := max(gap.Seconds(), 1e-4)
			if p.ewmaSamples == 0 {
				p.ewmaInterval = dt
			} else {
				p.ewmaInterval = ewmaAlpha*dt + (1-ewmaAlpha)*p.ewmaInterval
			}
			p.ewmaSamples++
		}
	}
	p.lastRelease = now
	p.a.m.AdmissionSlotsAdd(p.name, -1)
	p.dispatchLocked()
	p.mu.Unlock()
}

func (p *pool) aheadForRankLocked(rank int) int {
	if rank >= 0 && rank < len(p.queues)-1 {
		ahead := 1
		for r := 0; r <= rank; r++ {
			ahead += p.queues[r].Len()
		}
		return ahead
	}
	return p.queuedTotal + 1
}

// shouldEarlyRejectLocked reports whether an arriving request of rank is
// predicted to wait longer than maxWait based on the EWMA inter-release
// interval across queues[0..rank]. Requires a warm EWMA (>= 8 samples) and at
// least one waiter already ahead in [0..rank].
func (p *pool) shouldEarlyRejectLocked(rank int, maxWait time.Duration, now time.Time) bool {
	if p.ewmaSamples < ewmaMinSamples || p.ewmaInterval <= 0 || now.Sub(p.lastRelease) > ewmaFreshLimit {
		return false
	}
	ahead := 1
	for r := 0; r <= rank && r < len(p.queues); r++ {
		ahead += p.queues[r].Len()
	}
	if ahead < 2 {
		return false
	}
	est := time.Duration((float64(ahead) - 0.5) * p.ewmaInterval * float64(time.Second))
	return est > maxWait
}

// rejectLocked builds a rejection whose Retry-After estimates when all
// requests queued ahead will have been served, capped at RetryAfterMax.
func (p *pool) rejectLocked(status int, outcome string, now time.Time) *Rejection {
	return p.rejectRankLocked(-1, status, outcome, now)
}

// rejectRankLocked builds a rank-aware rejection whose Retry-After counts only
// requests queued in [0..rank] (lower-priority classes do not delay rank).
func (p *pool) rejectRankLocked(rank, status int, outcome string, now time.Time) *Rejection {
	ahead := p.aheadForRankLocked(rank)
	ra := time.Second
	if r := p.rate.perSecond(now); r > 0 {
		ra = time.Duration(float64(ahead) / r * float64(time.Second))
	} else if p.ewmaSamples >= ewmaMinSamples && p.ewmaInterval > 0 && now.Sub(p.lastRelease) <= ewmaFreshLimit {
		ra = time.Duration(float64(ahead) * p.ewmaInterval * float64(time.Second))
	}
	return &Rejection{Status: status, Outcome: outcome, RetryAfter: min(max(ra, time.Second), p.a.retryAfterMax)}
}

// applyShedLocked brings the pool's shedding in line with its tracker;
// turning it on answers every queued request of a shed class with 429.
func (p *pool) applyShedLocked() {
	on := p.shed.on()
	if p.shedding == on {
		return
	}
	p.shedding = on
	p.a.m.SetShedding(p.name, on)
	if !on {
		return
	}
	now := p.a.now()
	for rank := range p.queues {
		if !p.a.shedRanks[rank] {
			continue
		}
		q := &p.queues[rank]
		for q.Len() > 0 {
			w := q.popFront()
			p.queuedTotal--
			w.done = true
			w.rej = p.rejectRankLocked(rank, http.StatusTooManyRequests, telemetry.AdmissionShed, now)
			p.a.m.QueueDepthAdd(p.name, w.class, -1)
			w.ch <- nil
		}
	}
}

// observeTTFT feeds a highest-class TTFT sample to the shed tracker.
func (p *pool) observeTTFT(d time.Duration) {
	if p.shed == nil {
		return
	}
	if !p.shed.observe(p.a.now(), d) {
		return
	}
	p.mu.Lock()
	p.applyShedLocked()
	p.mu.Unlock()
}

// Slot is an admitted request's hold on its pool. A nil *Slot (admission
// off, or a pool without admission state) is valid: every method no-ops.
type Slot struct {
	pool      *pool
	rank      int
	class     string
	queueWait time.Duration
	released  atomic.Bool
}

// Release gives the slot back; only the first call counts.
func (s *Slot) Release() {
	if s == nil || !s.released.CompareAndSwap(false, true) {
		return
	}
	s.pool.release(s)
}

// QueueWait is how long the request waited in the queue (0 if admitted at
// once).
func (s *Slot) QueueWait() time.Duration {
	if s == nil {
		return 0
	}
	return s.queueWait
}

// ObserveTTFT reports the request's time to first token; requests of the
// highest class feed the pool's shedding decision.
func (s *Slot) ObserveTTFT(d time.Duration) {
	if s == nil || s.rank != 0 {
		return
	}
	s.pool.observeTTFT(d)
}

// rateWindow counts events in the current and previous second.
type rateWindow struct {
	sec       int64
	cur, prev int
	havePrev  bool
}

func (r *rateWindow) add(now time.Time) {
	r.roll(now)
	r.cur++
}

func (r *rateWindow) roll(now time.Time) {
	sec := now.Unix()
	switch {
	case sec == r.sec:
	case sec == r.sec+1:
		r.prev, r.cur, r.sec, r.havePrev = r.cur, 0, sec, true
	default:
		r.prev, r.cur, r.sec, r.havePrev = 0, 0, sec, true
	}
}

// perSecond is the events per second over the last full second, or 0.
func (r *rateWindow) perSecond(now time.Time) float64 {
	r.roll(now)
	if !r.havePrev {
		return float64(r.cur)
	}
	return float64(r.prev)
}
