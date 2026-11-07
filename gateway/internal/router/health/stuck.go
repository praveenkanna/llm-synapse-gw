package health

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// Stuck-engine detection.
//
// A vLLM engine can stop scheduling while its HTTP front end still answers
// /health with 200 and opens streams (the M0-76 failure mode): the replica
// sends response headers, then nothing. The relay watches for this: before
// each streaming upstream attempt it starts a first-chunk wait
// (StartFirstChunkWait) under a gateway-generated ID, and ends it when the
// replica's first body byte arrives (Received) or the attempt ends any other
// way (End): success, error, retry, client disconnect or panic.
//
// A wait older than StuckEngineTimeout is judged once, by the background
// scan (every StuckEngineTimeout/10, at least 100 ms) or by End, whichever
// comes first.
//
// Arrival order first. vLLM serves a backlog in arrival order: a burst of
// long prompts, or a steady queue longer than the timeout, leaves many
// waits past it while the replica is working through older ones. So a
// judged wait is excused when some wait that started no later than it
// received its first chunk within the last StuckEngineTimeout/2: the queue
// is moving and this wait's turn has not come yet. Answers to younger
// waits do not excuse: a stuck engine that still sends fast in-stream
// errors (vLLM answers some errors as HTTP 200 with data: {"error":...},
// each a first body byte), or a multi-engine endpoint whose other engine
// answers new requests at once, still has its old waits stalled. Pools
// with priority scheduling (vLLM --scheduling-policy priority,
// Config.OutOfOrderPools) serve out of arrival order: there any first chunk
// on the replica within the last StuckEngineTimeout/2 excuses, so a stuck
// engine is still caught (it answers nothing) but an error-masked one that
// answers new requests fast is not.
//
// Known limits: StuckEngineTimeout must exceed the slowest legitimate first
// token on an idle replica, or a burst of very long prompts is judged
// before any receipt exists and is ejected. A multi-engine endpoint whose
// healthy engine is itself queueing (time to first token of at least
// StuckEngineTimeout/2) answers waits older than the stuck ones and masks
// them. An excused wait is judged once: if the engine sticks right after,
// its hung stream does not count and detection waits for new traffic. The
// re-admission probe (probe.go) is one request: behind a multi-engine
// endpoint (data parallel, or a load balancer over several engines) a
// healthy engine can answer it while a stuck one stays stuck, so the
// endpoint comes back and is ejected again by its stalls. After a reload
// re-points a replica, the probe can name the old endpoint's model until
// the next /v1/models scrape; a stuck ejection in that window gets 404,
// counts as rejected and fails open (re-admits), as before M1-19.
//
// A wait that is not excused is a stall, recorded at the time it is
// judged and taken back if its first chunk comes after all. Per replica,
// the tracker counts over the last StuckEngineWindow (a sliding window of
// stuckWindowBuckets buckets) the first chunks F and the stalls S, where S
// is the larger of the stalls in the window and the stalled waits still
// open (so a stuck replica with little traffic, whose requests hang for up
// to upstream.timeouts.total, is still caught), and ejects it as
// stuck_engine when
//
//	S ≥ StuckEngineStreams  and  S / (S + F) ≥ StuckEngineRatio.
//
// Ejection goes through the same damped state machine as every ejection
// (ejection.go): a passing /health re-admits it only after its ejection
// time, and only once a generation probe answers (probe.go), since a
// stuck engine's /health passes. Ejection and re-admission start all
// counts over. Streams already hung on the replica (waits judged stalled,
// or older than the timeout and not yet judged) are ended at ejection
// through the wait's OnStuckEject hook, which the relay maps to the
// mid-stream SSE error event (M1-20); waits past the registry cap, streams
// past their first chunk, and streams routed onto an already-ejected
// replica (fail-open routing when every replica is ejected) are not cut:
// they still wait for upstream.timeouts.
//
// A stall on a replica that a reload has since removed or re-pointed is
// ignored (the wait named the old endpoint), as is one on a replica already
// ejected. A replica that is not current when the wait would start is not
// watched at all. The registry holds at most StuckEngineMaxWaits waits; past
// that a wait is not registered, so the scan never sees it (fail open,
// counted as gateway_failopen_total{stage="stuck_engine",reason="registry_full"}),
// but it still records its first chunk and is judged by End, so a full
// registry does not blind the counts.
//
// Times are offsets from the tracker's epoch (Tracker.since), which use the
// monotonic clock: a wall-clock step cannot fake a stall.

// Fail-open reason for a full first-chunk wait registry (stage
// telemetry.StageStuckEngine).
const (
	ReasonRegistryFull = "registry_full"
)

const (
	// stuckScanDivisor sets the scan period as a fraction of
	// StuckEngineTimeout, so a stall is noticed at most 10% late.
	stuckScanDivisor = 10
	// minStuckScanInterval keeps a short timeout from turning the scan into
	// a busy loop.
	minStuckScanInterval = 100 * time.Millisecond
	// stuckWindowBuckets splits StuckEngineWindow into buckets: the
	// counts cover the last 9 to 10 buckets, between 90% and 100% of it.
	stuckWindowBuckets = 10
	// excuseBucketsPerTimeout splits StuckEngineTimeout into receipt
	// buckets for the arrival-order excuse; excuseRecentBuckets of them
	// (half the timeout) count as recent, and excuseBuckets slots hold them.
	excuseBucketsPerTimeout = 30
	excuseRecentBuckets     = excuseBucketsPerTimeout / 2
	excuseBuckets           = excuseRecentBuckets + 1
)

// stuckScanInterval is how often the registry is scanned for stalls.
func (c Config) stuckScanInterval() time.Duration {
	return max(c.StuckEngineTimeout/stuckScanDivisor, minStuckScanInterval)
}

// windowCounter counts events per time bucket over the last
// stuckWindowBuckets buckets, without locks or allocation. Each slot packs
// its bucket number (low 32 bits of it) and its count into one word, so a
// slot moving to a newer bucket and a count landing in it never race.
type windowCounter struct {
	slots [stuckWindowBuckets]atomic.Uint64
}

const countMask = 1<<32 - 1

// add counts one event in bucket b.
func (c *windowCounter) add(b int64) {
	s := &c.slots[b%stuckWindowBuckets]
	want := uint32(b)
	for {
		old := s.Load()
		epoch, n := uint32(old>>32), old&countMask
		var next uint64
		switch age := int32(want - epoch); {
		case age == 0 && old != 0:
			if n == countMask {
				return // saturated
			}
			next = old + 1
		case age > 0 || old == 0:
			next = uint64(want)<<32 | 1 // the slot's bucket is older: start it over
		default:
			return // the slot already holds a newer bucket: b is out of the window
		}
		if s.CompareAndSwap(old, next) {
			return
		}
	}
}

// remove takes back one event counted in bucket b, if b is still held.
func (c *windowCounter) remove(b int64) {
	s := &c.slots[b%stuckWindowBuckets]
	want := uint32(b)
	for {
		old := s.Load()
		if old == 0 || uint32(old>>32) != want || old&countMask == 0 {
			return
		}
		if s.CompareAndSwap(old, old-1) {
			return
		}
	}
}

// sum returns the events in the window ending with bucket b.
func (c *windowCounter) sum(b int64) int64 {
	var total int64
	for i := range c.slots {
		v := c.slots[i].Load()
		if v == 0 {
			continue
		}
		if age := int32(uint32(b) - uint32(v>>32)); age >= 0 && age < stuckWindowBuckets {
			total += int64(v & countMask)
		}
	}
	return total
}

func (c *windowCounter) reset() {
	for i := range c.slots {
		c.slots[i].Store(0)
	}
}

// receiptLog remembers, per receipt bucket, the earliest start of a wait
// that received its first chunk in that bucket, for the arrival-order
// excuse. Each slot packs its bucket number (low 32 bits, plus one so that
// 0 means empty) and the start in milliseconds (low 32 bits; compared with
// wraparound, valid within ±24 days) into one word, updated by CAS.
type receiptLog struct {
	slots [excuseBuckets]atomic.Uint64
}

// record notes a first chunk received in receipt bucket rb by a wait that
// started at startMs.
func (l *receiptLog) record(rb int64, startMs uint32) {
	s := &l.slots[rb%excuseBuckets]
	tag := uint32(rb) + 1
	next := uint64(tag)<<32 | uint64(startMs)
	for {
		old := s.Load()
		if old != 0 {
			switch age := int32(tag - uint32(old>>32)); {
			case age < 0:
				return // the slot holds a newer bucket
			case age == 0 && int32(startMs-uint32(old)) >= 0:
				return // an earlier start is already recorded
			}
		}
		if s.CompareAndSwap(old, next) {
			return
		}
	}
}

// receiptSnapshot is a copy of a receiptLog at one instant. Ejection resets
// the log, so the stuck-ejection cut (cancelHung) reads the receipts as they
// were when it decided to eject.
type receiptSnapshot [excuseBuckets]uint64

func (l *receiptLog) snapshot() (s receiptSnapshot) {
	for i := range l.slots {
		s[i] = l.slots[i].Load()
	}
	return s
}

// excuses reports whether a wait that started at or before startMs
// received its first chunk in the recent buckets ending with rb.
func (l *receiptLog) excuses(rb int64, startMs uint32) bool {
	return l.snapshot().excuses(rb, startMs)
}

func (s receiptSnapshot) excuses(rb int64, startMs uint32) bool {
	tag := uint32(rb) + 1
	for _, v := range s {
		if v == 0 {
			continue
		}
		if age := int32(tag - uint32(v>>32)); age < 0 || age >= excuseRecentBuckets {
			continue
		}
		if int32(startMs-uint32(v)) >= 0 {
			return true
		}
	}
	return false
}

func (l *receiptLog) reset() {
	for i := range l.slots {
		l.slots[i].Store(0)
	}
}

// replicaProgress holds a replica endpoint's first-chunk and stall counts.
// It lives in the applied membership, one per replica endpoint.
type replicaProgress struct {
	firsts   windowCounter // first body bytes received
	stalls   windowCounter // stalls not taken back
	receipts receiptLog    // earliest start answered, per recent receipt bucket
	// openStalls counts stalled waits still open; gen moves on every reset,
	// so a wait stalled before a reset does not decrement the new count.
	openStalls atomic.Int64
	gen        atomic.Uint64
}

func (p *replicaProgress) reset() {
	p.gen.Add(1)
	p.firsts.reset()
	p.stalls.reset()
	p.receipts.reset()
	p.openStalls.Store(0)
}

// Wait states. A wait is judged at most once (pending → stalled or judged)
// and ends once (→ ended).
const (
	waitPending uint8 = iota
	waitStalled       // judged past the timeout and counted as a stall
	waitJudged        // judged past the timeout, not counted (replica out or stale)
	waitEnded
)

// FirstChunkWait is one upstream attempt waiting for its replica's first
// body byte. The nil *FirstChunkWait watches nothing. Received and End are
// safe to call more than once and in any order: only the first call counts.
// Neither allocates.
type FirstChunkWait struct {
	t          *Tracker
	id         uint64 // gateway-generated; never caller-controlled
	registered bool   // in the registry (scanned); false when it was full
	shardIdx   uint8
	inList     bool
	prev, next *FirstChunkWait
	replica    discovery.Replica
	start      time.Duration // Tracker.since at registration
	progress   *replicaProgress

	mu          sync.Mutex // per wait: never contended beyond its request and one scan
	state       uint8
	stallBucket int64  // the bucket its stall was counted in (waitStalled)
	stallOpen   bool   // counted in openStalls (judged by a scan, still open)
	stallGen    uint64 // progress.gen when it was counted
	onEject     func() // ends the stream when the replica is ejected as stuck; nil once used or ended
}

const (
	waitShards    = 32
	waitShardMask = waitShards - 1
)

type waitShard struct {
	mu   sync.Mutex
	head *FirstChunkWait
	_    [40]byte // pad shard to 64B cache line
}

// waitRegistry holds the registered first-chunk waits across cache-line-padded
// intrusive doubly-linked list shards, bounded strictly by size.
type waitRegistry struct {
	nextID atomic.Uint64
	size   atomic.Int64
	shards [waitShards]waitShard
}

// reserve takes a slot for one more wait, or reports false at limit.
func (r *waitRegistry) reserve(limit int) bool {
	if limit <= 0 {
		return false
	}
	for {
		cur := r.size.Load()
		if cur >= int64(limit) {
			return false
		}
		if r.size.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

func (r *waitRegistry) add(w *FirstChunkWait) {
	id := r.nextID.Add(1)
	w.id = id
	idx := uint8(id & waitShardMask)
	w.shardIdx = idx
	sh := &r.shards[idx]
	sh.mu.Lock()
	w.inList = true
	w.prev = nil
	w.next = sh.head
	if sh.head != nil {
		sh.head.prev = w
	}
	sh.head = w
	sh.mu.Unlock()
}

func (r *waitRegistry) remove(w *FirstChunkWait) {
	sh := &r.shards[w.shardIdx]
	sh.mu.Lock()
	removed := w.inList
	if removed {
		if w.prev != nil {
			w.prev.next = w.next
		} else {
			sh.head = w.next
		}
		if w.next != nil {
			w.next.prev = w.prev
		}
		w.prev, w.next = nil, nil
		w.inList = false
	}
	sh.mu.Unlock()
	if removed {
		r.size.Add(-1)
	}
}

func (r *waitRegistry) forEach(fn func(*FirstChunkWait)) {
	var stack [64]*FirstChunkWait
	for i := range r.shards {
		sh := &r.shards[i]
		sh.mu.Lock()
		if sh.head == nil {
			sh.mu.Unlock()
			continue
		}
		list := stack[:0]
		for cur := sh.head; cur != nil; cur = cur.next {
			list = append(list, cur)
		}
		sh.mu.Unlock()
		for _, w := range list {
			fn(w)
		}
	}
}

// since returns the monotonic time from the tracker's epoch to now.
func (t *Tracker) since(now time.Time) time.Duration {
	return max(now.Sub(t.epoch), 0)
}

// bucket returns the window bucket of offset d.
func (t *Tracker) bucket(d time.Duration) int64 {
	return int64(d / t.bucketWidth)
}

// receiptBucket returns the arrival-order receipt bucket of offset d.
func (t *Tracker) receiptBucket(d time.Duration) int64 {
	return int64(d / t.receiptWidth)
}

// millis is offset d in milliseconds, truncated to 32 bits (receiptLog).
func millis(d time.Duration) uint32 {
	return uint32(d / time.Millisecond)
}

// excuseStart is the start the arrival-order excuse compares receipts
// with: the wait's own start, or now on a pool that serves out of arrival
// order, where any recent first chunk on the replica shows the engine works.
func (t *Tracker) excuseStart(w *FirstChunkWait, now time.Duration) uint32 {
	if t.cfg.OutOfOrderPools[w.replica.Pool] {
		return millis(now)
	}
	return millis(w.start)
}

// StartFirstChunkWait starts watching an upstream attempt on rep (the
// lease's replica) for its first body byte. It returns nil, watching
// nothing, when rep is no longer the current replica of its name at that
// address, or without a replica set (a discovery that cannot list). When
// the registry is full the wait is returned unregistered, a fail-open
// recorded on rec, the attempt's request (so it reaches the request's log
// line), or as a metric when rec is nil: the scan does not see the wait,
// but Received and End still count. The caller ends the wait exactly as the
// attempt ends: Received on the first body byte, End on every other exit.
func (t *Tracker) StartFirstChunkWait(rep discovery.Replica, rec *telemetry.Request) *FirstChunkWait {
	progress, ok := t.current(rep)
	if !ok || progress == nil {
		return nil
	}
	w := &FirstChunkWait{
		t:        t,
		replica:  rep,
		start:    t.since(t.now()),
		progress: progress,
	}
	if !t.waits.reserve(t.cfg.StuckEngineMaxWaits) {
		switch {
		case rec != nil:
			rec.FailOpen(telemetry.StageStuckEngine, ReasonRegistryFull)
		case t.metrics != nil:
			t.metrics.FailOpen(telemetry.StageStuckEngine, ReasonRegistryFull)
		}
		return w
	}
	w.registered = true
	t.waits.add(w)
	return w
}

// Received ends the wait because the replica's first body byte arrived:
// it counts a first chunk for the replica, and takes back the wait's stall
// if it was judged one.
func (w *FirstChunkWait) Received() {
	if w == nil {
		return
	}
	w.mu.Lock()
	prev := w.state
	w.state = waitEnded
	w.onEject = nil
	w.mu.Unlock()
	if prev == waitEnded {
		return
	}
	w.unregister()
	now := w.t.since(w.t.now())
	w.progress.firsts.add(w.t.bucket(now))
	w.progress.receipts.record(w.t.receiptBucket(now), millis(w.start))
	if prev == waitStalled {
		w.progress.stalls.remove(w.stallBucket)
		w.closeStall()
	}
}

// closeStall takes a stalled wait out of openStalls as it ends.
func (w *FirstChunkWait) closeStall() {
	if w.stallOpen && w.progress.gen.Load() == w.stallGen {
		w.progress.openStalls.Add(-1)
	}
}

// End ends the wait without a first chunk: the attempt failed, was retried,
// the caller left, or the response was an error. A wait already past
// StuckEngineTimeout that no scan judged yet is judged now, so a timeout
// that ends the attempt between scans still counts.
func (w *FirstChunkWait) End() {
	if w == nil {
		return
	}
	t := w.t
	now := t.since(t.now())
	w.mu.Lock()
	prev := w.state
	w.state = waitEnded
	w.onEject = nil
	stalled := prev == waitPending && now-w.start >= t.cfg.StuckEngineTimeout && w.countStall(now)
	w.mu.Unlock()
	if prev == waitEnded {
		return
	}
	w.unregister()
	if prev == waitStalled {
		w.closeStall() // the stall still counts in the window; it is just no longer open
	}
	if stalled {
		t.maybeEjectStuck(w.replica, now)
	}
}

// OnStuckEject sets f to run once, from the goroutine that ejects the
// wait's replica as a stuck engine, if the wait is then still without its
// first body byte and judged hung (see cancelHung). f must not block: it
// cancels the attempt's context. It never runs after Received or End, which
// drop it, and a nil wait ignores it. A second call replaces the first.
func (w *FirstChunkWait) OnStuckEject(f func()) {
	if w == nil {
		return
	}
	w.mu.Lock()
	if w.state != waitEnded {
		w.onEject = f
	}
	w.mu.Unlock()
}

func (w *FirstChunkWait) unregister() {
	if w.registered {
		w.t.waits.remove(w)
	}
}

// judge records a stall for a pending wait older than StuckEngineTimeout.
// It reports whether one was counted.
func (w *FirstChunkWait) judge(now time.Duration) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.state != waitPending || now-w.start < w.t.cfg.StuckEngineTimeout {
		return false
	}
	if w.countStall(now) {
		w.state = waitStalled
		w.stallOpen = true
		w.stallGen = w.progress.gen.Load()
		w.progress.openStalls.Add(1)
		return true
	}
	w.state = waitJudged
	return false
}

// countStall counts the wait's stall at now unless its replica is no
// longer current or in its pool, or arrival order excuses it (see the
// package comment above). Callers hold w.mu.
func (w *FirstChunkWait) countStall(now time.Duration) bool {
	t := w.t
	if !t.isCurrent(w.replica) || !t.IsHealthy(w.replica.Name) {
		return false
	}
	if w.progress.receipts.excuses(t.receiptBucket(now), t.excuseStart(w, now)) {
		return false // an older or same-age wait was answered recently: the queue moves
	}
	w.stallBucket = t.bucket(now)
	w.progress.stalls.add(w.stallBucket)
	t.countStallMetric(w.replica.Pool)
	return true
}

// FirstChunkWaits returns the number of first-chunk waits registered now.
func (t *Tracker) FirstChunkWaits() int {
	return int(t.waits.size.Load())
}

// CheckStuckStreams judges every registered wait older than
// StuckEngineTimeout, once, and ejects replicas whose stalls reach the
// thresholds. The background loop calls it periodically; tests call it
// after advancing a fake clock.
func (t *Tracker) CheckStuckStreams() {
	now := t.since(t.now())
	t.waits.forEach(func(w *FirstChunkWait) {
		if w.judge(now) {
			t.maybeEjectStuck(w.replica, now)
		}
	})
}

// maybeEjectStuck ejects rep as a stuck engine when its stalls (in the
// window, or still open if more) reach StuckEngineStreams and
// StuckEngineRatio of stalls plus first chunks in the window.
func (t *Tracker) maybeEjectStuck(rep discovery.Replica, now time.Duration) {
	if snap, ejected := t.ejectStuck(rep, now); ejected {
		t.cancelHung(rep, now, snap) // after statesMu is released: the callbacks are the callers'
	}
}

// ejectStuck is maybeEjectStuck's decision; it reports whether it ejected
// rep, with rep's receipts as they were before the ejection reset them.
func (t *Tracker) ejectStuck(rep discovery.Replica, now time.Duration) (receiptSnapshot, bool) {
	t.statesMu.Lock()
	defer t.statesMu.Unlock()
	p, ok := t.current(rep)
	if !ok || p == nil {
		return receiptSnapshot{}, false // re-pointed or removed since the wait began
	}
	rh := t.stateFor(rep.Name, rep.Pool)
	if !rh.healthy {
		return receiptSnapshot{}, false // already out; re-admission alone decides when it is back
	}
	b := t.bucket(now)
	s, f := max(p.stalls.sum(b), p.openStalls.Load()), p.firsts.sum(b)
	if s < int64(t.cfg.StuckEngineStreams) || float64(s) < t.cfg.StuckEngineRatio*float64(s+f) {
		return receiptSnapshot{}, false
	}
	snap := p.receipts.snapshot()
	t.eject(rh, ReasonStuckEngine)
	return snap, true
}

// receiptsOf returns replica's receipts now (empty without a replica set).
// Callers hold statesMu.
func (t *Tracker) receiptsOf(replica string) receiptSnapshot {
	if m := t.members.Load(); m != nil {
		if p := m.progress[replica]; p != nil {
			return p.receipts.snapshot()
		}
	}
	return receiptSnapshot{}
}

// cancelHung runs the OnStuckEject hook of every registered wait on rep
// (same name and address) that is judged still hung: counted as a stall, or
// older than StuckEngineTimeout, not yet judged and not excused by arrival
// order (snap holds the receipts from before the ejection reset them, and
// the check is the one countStall makes). Younger waits are not judged hung
// (they may be answered), and a wait excused by arrival order was judged
// working through a backlog, so neither is touched. Each hook runs at most
// once, outside every tracker lock. The registry holds at most
// StuckEngineMaxWaits waits, so the walk is bounded; a wait past that cap is
// not registered and ends by upstream.timeouts as before.
func (t *Tracker) cancelHung(rep discovery.Replica, now time.Duration, snap receiptSnapshot) {
	rb := t.receiptBucket(now)
	t.waits.forEach(func(w *FirstChunkWait) {
		if w.replica.Name != rep.Name || !sameEndpoint(w.replica, rep) {
			return
		}
		var f func()
		w.mu.Lock()
		hung := w.state == waitStalled ||
			(w.state == waitPending && now-w.start >= t.cfg.StuckEngineTimeout && !snap.excuses(rb, t.excuseStart(w, now)))
		if hung {
			f, w.onEject = w.onEject, nil
		}
		w.mu.Unlock()
		// A first byte can land between the check above and this call; the
		// stream then relays that chunk and gets the stuck error event
		// right after. That is acceptable: the replica is out anyway.
		if f != nil {
			f()
		}
	})
}

// resetStuckCounts starts replica's first-chunk and stall counts over, on
// ejection and re-admission. Callers hold statesMu.
func (t *Tracker) resetStuckCounts(replica string) {
	m := t.members.Load()
	if m == nil {
		return
	}
	if p := m.progress[replica]; p != nil {
		p.reset()
	}
}

// countStallMetric counts a first-chunk stall in
// gateway_first_chunk_stalls_total.
func (t *Tracker) countStallMetric(pool string) {
	if t.metrics == nil {
		return
	}
	if pool == "" {
		pool = telemetry.PoolNone
	}
	t.metrics.FirstChunkStall(pool)
}

// stuckLoop scans for stalled first-chunk waits every stuckScanInterval.
func (t *Tracker) stuckLoop() {
	defer t.wg.Done()
	ticker := time.NewTicker(t.cfg.stuckScanInterval())
	defer ticker.Stop()
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
			if t.FirstChunkWaits() > 0 {
				t.CheckStuckStreams()
			}
		}
	}
}
