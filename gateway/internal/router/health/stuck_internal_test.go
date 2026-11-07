package health

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// Stuck-engine detection (M1-13): first-chunk waits past
// StuckEngineTimeout are stalls (taken back by a late first chunk); a
// replica is ejected when its stalls S and first chunks F over the sliding
// window reach S ≥ StuckEngineStreams and S/(S+F) ≥ StuckEngineRatio.
// Every test runs on a fake clock and scans by hand.

const stuckTimeout = config.DefaultHealthStuckEngineTimeout // the tests run on config's defaults (testConfig)

// stuckTracker is ejectionTracker (pool p: a, b; min ejection 5 s) with
// metrics, default stuck-engine settings (3 streams, ratio 0.5, 1 min
// window) unless mod changes them.
func stuckTracker(t *testing.T, mod func(*Config)) (*Tracker, *FakeClock, *telemetry.Metrics) {
	t.Helper()
	tr, clock, _ := ejectionTracker(t, mod)
	tm := telemetry.NewMetrics()
	tr.metrics = tm
	return tr, clock, tm
}

func scrapeMetrics(t *testing.T, tm *telemetry.Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	tm.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return rec.Body.String()
}

func assertMetric(t *testing.T, body, line string) {
	t.Helper()
	if !strings.Contains(body, line+"\n") {
		t.Errorf("metrics lack %q:\n%s", line, body)
	}
}

// currentReplica returns name's replica from tr's applied set.
func currentReplica(t *testing.T, tr *Tracker, name string) discovery.Replica {
	t.Helper()
	rep, ok := tr.members.Load().byName[name]
	if !ok {
		t.Fatalf("no replica %s", name)
	}
	return rep
}

// counts returns name's stalls and first chunks in the window now.
func counts(tr *Tracker, name string) (stalls, firsts int64) {
	p := tr.members.Load().progress[name]
	b := tr.bucket(tr.since(tr.now()))
	return p.stalls.sum(b), p.firsts.sum(b)
}

// startN starts n waits on rep.
func startN(t *testing.T, tr *Tracker, rep discovery.Replica, n int) []*FirstChunkWait {
	t.Helper()
	ws := make([]*FirstChunkWait, n)
	for i := range ws {
		if ws[i] = tr.StartFirstChunkWait(rep, nil); ws[i] == nil {
			t.Fatal("wait not started")
		}
	}
	return ws
}

// serve starts n waits on rep that get their first chunk at once.
func serve(t *testing.T, tr *Tracker, rep discovery.Replica, n int) {
	t.Helper()
	for _, w := range startN(t, tr, rep, n) {
		w.Received()
	}
}

func endAll(ws []*FirstChunkWait) {
	for _, w := range ws {
		w.End()
	}
}

func TestStuckEngineEjectedAfterStreamsInWindow(t *testing.T) {
	tr, clock, tm := stuckTracker(t, nil)
	a := currentReplica(t, tr, "a")
	waits := startN(t, tr, a, 3)
	clock.Advance(stuckTimeout - time.Second)
	tr.CheckStuckStreams()
	if s, _ := counts(tr, "a"); !tr.IsHealthy("a") || s != 0 {
		t.Fatal("waits younger than stuck_engine_timeout counted as stalls")
	}
	clock.Advance(time.Second)
	tr.CheckStuckStreams()
	if tr.IsHealthy("a") {
		t.Fatal("a still in after 3 stalled streams and no first chunk")
	}
	if got := stateOf(tr, "a").ejectionReason; got != ReasonStuckEngine {
		t.Errorf("reason = %q, want %q", got, ReasonStuckEngine)
	}
	if !tr.IsHealthy("b") {
		t.Error("b ejected by a's stalls")
	}
	body := scrapeMetrics(t, tm)
	assertMetric(t, body, `gateway_first_chunk_stalls_total{pool="p"} 3`)
	assertMetric(t, body, `gateway_replica_ejections_total{pool="p",reason="stuck_engine"} 1`)
	if s, f := counts(tr, "a"); s != 0 || f != 0 {
		t.Errorf("counts after ejection: S=%d F=%d, want both started over", s, f)
	}

	// The waits stay registered until their requests end, and are judged
	// once: neither another scan nor End counts them again.
	if n := tr.FirstChunkWaits(); n != 3 {
		t.Fatalf("waits = %d, want 3 until their requests end", n)
	}
	clock.Advance(time.Minute)
	tr.CheckStuckStreams()
	endAll(waits)
	assertMetric(t, scrapeMetrics(t, tm), `gateway_first_chunk_stalls_total{pool="p"} 3`)
	if n := tr.FirstChunkWaits(); n != 0 {
		t.Fatalf("waits = %d after every request ended, want 0", n)
	}
}

// TestSingleStallDoesNotEject: one stall, or stalls that never sit in the
// sliding window three at a time, leave the replica in.
func TestSingleStallDoesNotEject(t *testing.T) {
	tr, clock, _ := stuckTracker(t, nil) // 3 streams, 1 min window (6 s buckets)
	a := currentReplica(t, tr, "a")
	// stallAt starts n waits at the clock's offset start and judges them at
	// start+30 s; it returns S then.
	stallAt := func(start time.Duration, n int) int64 {
		t.Helper()
		clock.Advance(start - tr.since(tr.now()))
		ws := startN(t, tr, a, n)
		clock.Advance(stuckTimeout)
		tr.CheckStuckStreams()
		tr.CheckStuckStreams() // judged once
		endAll(ws)
		s, _ := counts(tr, "a")
		return s
	}
	if s := stallAt(0, 1); s != 1 || !tr.IsHealthy("a") { // stall at +30 s
		t.Fatalf("after one stall: S %d, healthy %v; want 1, in", s, tr.IsHealthy("a"))
	}
	if s := stallAt(50*time.Second, 1); s != 2 || !tr.IsHealthy("a") { // +80 s
		t.Fatalf("after two stalls: S %d, healthy %v; want 2, in", s, tr.IsHealthy("a"))
	}
	if s := stallAt(100*time.Second, 1); s != 2 || !tr.IsHealthy("a") { // +130 s: +30 s slid out
		t.Fatalf("at +130 s: S %d, healthy %v; want 2 (the +30 s stall slid out), in", s, tr.IsHealthy("a"))
	}
	if s := stallAt(140*time.Second, 1); s != 2 || !tr.IsHealthy("a") { // +170 s: +80 s slid out
		t.Fatalf("at +170 s: S %d, healthy %v; want 2, in", s, tr.IsHealthy("a"))
	}
	stallAt(170*time.Second, 2) // +200 s: +170 s and two more
	if tr.IsHealthy("a") {
		t.Fatal("3 stalls within the sliding window did not eject a")
	}
}

// TestOverloadedServingReplicaNotEjected: an overloaded replica in FIFO
// order sends first chunks steadily (F large) and its late streams get
// their first chunk after all (their stalls are taken back), so it stays.
func TestOverloadedServingReplicaNotEjected(t *testing.T) {
	tr, clock, _ := stuckTracker(t, nil)
	a := currentReplica(t, tr, "a")
	for round := range 4 {
		queued := startN(t, tr, a, 4) // will wait past the timeout
		for range 3 {
			clock.Advance(10 * time.Second)
			serve(t, tr, a, 5) // first chunks keep coming
		}
		clock.Advance(time.Second)
		tr.CheckStuckStreams() // the 4 queued are stalls now: S=4 ≥ 3, but F ≥ 15
		if s, f := counts(tr, "a"); s != 4 || f < 15 {
			t.Fatalf("round %d: S=%d F=%d, want S=4 and F ≥ 15", round, s, f)
		}
		if !tr.IsHealthy("a") {
			t.Fatalf("round %d: an overloaded replica that keeps serving was ejected", round)
		}
		for _, w := range queued {
			w.Received() // late, but they come
		}
		if s, _ := counts(tr, "a"); s != 0 {
			t.Fatalf("round %d: S=%d after the late first chunks, want 0 (taken back)", round, s)
		}
	}
	if n := tr.FirstChunkWaits(); n != 0 {
		t.Fatalf("waits = %d, want 0", n)
	}
}

// TestSlowFirstTokenNeverStalls: a first token before the timeout is
// never judged, even with a threshold of one stream.
func TestSlowFirstTokenNeverStalls(t *testing.T) {
	tr, clock, tm := stuckTracker(t, func(c *Config) { c.StuckEngineStreams = 1 })
	a := currentReplica(t, tr, "a")
	for range 3 {
		w := tr.StartFirstChunkWait(a, nil)
		clock.Advance(stuckTimeout - time.Second)
		tr.CheckStuckStreams()
		w.Received()
		clock.Advance(time.Minute)
		tr.CheckStuckStreams()
	}
	if !tr.IsHealthy("a") {
		t.Fatal("a slow but healthy replica was ejected")
	}
	if strings.Contains(scrapeMetrics(t, tm), "gateway_first_chunk_stalls_total{") {
		t.Error("stalls counted for first chunks before the timeout")
	}
}

// TestErrorTrickleDoesNotMaskStuckEngine: most streams hang while a few
// newer ones get a fast first body byte (vLLM sends some errors as 200 +
// an SSE error event). Answers to younger waits excuse nothing, so the
// stuck engine is ejected: S=6, F=2.
func TestErrorTrickleDoesNotMaskStuckEngine(t *testing.T) {
	tr, clock, _ := stuckTracker(t, nil)
	a := currentReplica(t, tr, "a")
	hung := startN(t, tr, a, 6)
	clock.Advance(20 * time.Second)
	serve(t, tr, a, 2) // in-stream errors, recent, on waits younger than the hung ones
	clock.Advance(stuckTimeout - 20*time.Second)
	tr.CheckStuckStreams()
	if tr.IsHealthy("a") {
		t.Fatal("a stuck engine masked by a trickle of fast errors stayed in")
	}
	endAll(hung)
}

// TestPriorityPoolExcusesYoungerAnswers: on a pool with priority
// scheduling, newer high-priority streams answered while older
// low-priority ones wait is the scheduler at work, not a stuck engine.
func TestPriorityPoolExcusesYoungerAnswers(t *testing.T) {
	tr, clock, _ := stuckTracker(t, func(c *Config) { c.OutOfOrderPools = map[string]bool{"p": true} })
	a := currentReplica(t, tr, "a")
	waiting := startN(t, tr, a, 6) // low priority, waiting behind newer ones
	clock.Advance(20 * time.Second)
	serve(t, tr, a, 2) // newer high-priority streams answered
	clock.Advance(stuckTimeout - 20*time.Second)
	tr.CheckStuckStreams()
	if !tr.IsHealthy("a") {
		t.Fatal("a priority-scheduled replica serving newer streams was ejected")
	}
	if s, _ := counts(tr, "a"); s != 0 {
		t.Fatalf("S = %d, want 0 (excused)", s)
	}
	endAll(waiting)
}

// TestPriorityPoolStillEjectsSilentEngine: with nothing answered at all, a
// priority-scheduled replica is ejected as before.
func TestPriorityPoolStillEjectsSilentEngine(t *testing.T) {
	tr, clock, _ := stuckTracker(t, func(c *Config) { c.OutOfOrderPools = map[string]bool{"p": true} })
	a := currentReplica(t, tr, "a")
	hung := startN(t, tr, a, 3)
	clock.Advance(stuckTimeout)
	tr.CheckStuckStreams()
	if tr.IsHealthy("a") {
		t.Fatal("a silent engine on a priority pool stayed in")
	}
	endAll(hung)
}

// fifoReplica simulates a replica serving its waits in arrival order:
// every step it answers the oldest open waits, as many as it serves per
// step. Abandoned waits leave the queue (vLLM aborts them).
type fifoReplica struct {
	t     *testing.T
	tr    *Tracker
	rep   discovery.Replica
	queue []*FirstChunkWait
}

func (f *fifoReplica) arrive(n int) {
	f.queue = append(f.queue, startN(f.t, f.tr, f.rep, n)...)
}

func (f *fifoReplica) serve(n int) {
	for ; n > 0 && len(f.queue) > 0; n-- {
		f.queue[0].Received()
		f.queue = f.queue[1:]
	}
}

// abandonOlderThan ends the open waits older than age (callers giving up).
func (f *fifoReplica) abandonOlderThan(age time.Duration) {
	now := f.tr.since(f.tr.now())
	kept := f.queue[:0]
	for _, w := range f.queue {
		if now-w.start > age {
			w.End()
			continue
		}
		kept = append(kept, w)
	}
	f.queue = kept
}

// TestFIFOBurstNotEjected: a burst of long prompts served in arrival order
// (first chunks at ~11 s, ~21 s, ...) leaves most waits past the timeout
// with S ≫ F, but every one of them is excused by an older or same-age
// wait answered recently, so the replica stays in.
func TestFIFOBurstNotEjected(t *testing.T) {
	tr, clock, tm := stuckTracker(t, nil)
	f := &fifoReplica{t: t, tr: tr, rep: currentReplica(t, tr, "a")}
	f.arrive(10)
	for sec := 1; len(f.queue) > 0; sec++ {
		clock.Advance(time.Second)
		if sec%10 == 1 && sec > 1 {
			f.serve(1) // one prefill every 10 s
		}
		tr.CheckStuckStreams()
		if !tr.IsHealthy("a") {
			t.Fatalf("+%ds: a replica serving a burst in arrival order was ejected", sec)
		}
	}
	if strings.Contains(scrapeMetrics(t, tm), "gateway_first_chunk_stalls_total{") {
		t.Error("stalls counted for a burst served in arrival order")
	}
}

// TestLongQueueNotEjected: a steady queue wait of ~100 s (above timeout +
// window, 90 s) on a replica serving in arrival order never ejects it,
// with or without callers abandoning queued streams.
func TestLongQueueNotEjected(t *testing.T) {
	for _, abandon := range []bool{false, true} {
		tr, clock, tm := stuckTracker(t, nil)
		f := &fifoReplica{t: t, tr: tr, rep: currentReplica(t, tr, "a")}
		f.arrive(20) // a backlog of 20 at one served per 5 s: ~100 s of queue
		for sec := 1; sec <= 400; sec++ {
			clock.Advance(time.Second)
			if sec%5 == 0 {
				f.arrive(1)
				f.serve(1)
			}
			if abandon && sec%15 == 0 {
				f.abandonOlderThan(60 * time.Second) // some callers give up after 60 s
			}
			tr.CheckStuckStreams()
			if !tr.IsHealthy("a") {
				t.Fatalf("abandon=%v, +%ds: a replica with a long FIFO queue was ejected", abandon, sec)
			}
		}
		if strings.Contains(scrapeMetrics(t, tm), "gateway_first_chunk_stalls_total{") {
			t.Errorf("abandon=%v: stalls counted for a moving FIFO queue", abandon)
		}
		endAll(f.queue)
	}
}

// TestPartlyStuckEndpointEjected: a multi-engine endpoint with one engine
// stuck answers half its streams at once (the healthy engine) and never
// the other half. The fast answers go to young waits, so they excuse none
// of the stalled ones, and the stalls reach the ratio.
func TestPartlyStuckEndpointEjected(t *testing.T) {
	tr, clock, _ := stuckTracker(t, nil)
	a := currentReplica(t, tr, "a")
	var hung []*FirstChunkWait
	for sec := 1; sec <= 300 && tr.IsHealthy("a"); sec++ {
		clock.Advance(time.Second)
		if sec%2 == 0 {
			if sec%4 == 0 {
				serve(t, tr, a, 1) // the healthy engine
			} else {
				hung = append(hung, startN(t, tr, a, 1)...) // the stuck engine
			}
		}
		tr.CheckStuckStreams()
	}
	if tr.IsHealthy("a") {
		s, f := counts(tr, "a")
		t.Fatalf("a partly stuck endpoint stayed in (S=%d, F=%d)", s, f)
	}
	endAll(hung)
}

// TestHungStreamsCountWhileOpen: a stuck replica with little traffic (one
// request per 40 s, each hanging until the upstream total timeout) never
// has 3 stalls in the 1 min window, but its stalled streams are still
// open: that counts. Callers that give up close them, and then it doesn't.
func TestHungStreamsCountWhileOpen(t *testing.T) {
	for _, giveUp := range []bool{false, true} {
		tr, clock, _ := stuckTracker(t, nil)
		a := currentReplica(t, tr, "a")
		var open []*FirstChunkWait
		for sec := 1; sec <= 200; sec++ {
			clock.Advance(time.Second)
			if sec%40 == 1 {
				open = append(open, startN(t, tr, a, 1)...)
			}
			tr.CheckStuckStreams()
			if giveUp {
				for _, w := range open {
					if tr.since(tr.now())-w.start > stuckTimeout+5*time.Second {
						w.End()
					}
				}
			}
		}
		if got := !tr.IsHealthy("a"); got != !giveUp {
			t.Errorf("give up %v: ejected %v, want %v", giveUp, got, !giveUp)
		}
		endAll(open)
	}
}

func TestReceiptLog(t *testing.T) {
	var l receiptLog
	l.record(10, 5000)
	l.record(10, 7000) // later start: the earlier one stays
	if !l.excuses(10, 5000) || !l.excuses(10, 9000) {
		t.Fatal("a wait started at or after the recorded start is not excused")
	}
	if l.excuses(10, 4999) {
		t.Fatal("an older wait is excused by a younger one's answer")
	}
	l.record(12, 3000)
	if !l.excuses(12, 4000) {
		t.Fatal("earlier start in a later bucket not seen")
	}
	if !l.excuses(12+excuseRecentBuckets-1, 3000) {
		t.Fatal("receipt at the edge of the recent buckets not seen")
	}
	if l.excuses(12+excuseRecentBuckets, 6000) {
		t.Fatal("receipts older than half the timeout still excuse")
	}
	l.record(10+excuseBuckets, 9000) // reuses bucket 10's slot
	l.record(10, 1000)               // older than the slot's bucket: dropped
	if l.excuses(10+excuseBuckets, 1500) {
		t.Fatal("a stale record overwrote a newer bucket")
	}
	l.reset()
	if l.excuses(12, 1<<31) {
		t.Fatal("reset left a record")
	}
}

// TestRatioKnob: a stricter ratio keeps the replica in at a share a
// looser one ejects.
func TestRatioKnob(t *testing.T) {
	for _, tc := range []struct {
		ratio float64
		eject bool
	}{{0.9, false}, {0.6, true}} {
		tr, clock, _ := stuckTracker(t, func(c *Config) { c.StuckEngineRatio = tc.ratio })
		a := currentReplica(t, tr, "a")
		stalled := startN(t, tr, a, 4)
		clock.Advance(20 * time.Second)
		serve(t, tr, a, 2) // younger waits: no excuse; S/(S+F) = 4/6
		clock.Advance(stuckTimeout - 20*time.Second)
		tr.CheckStuckStreams()
		if got := !tr.IsHealthy("a"); got != tc.eject {
			t.Errorf("ratio %v: ejected %v, want %v", tc.ratio, got, tc.eject)
		}
		endAll(stalled)
	}
}

// TestEndJudgesWaitBetweenScans: an attempt that ends (an upstream
// timeout, a caller giving up) after StuckEngineTimeout but before a scan
// still counts as a stall.
func TestEndJudgesWaitBetweenScans(t *testing.T) {
	tr, clock, _ := stuckTracker(t, nil)
	a := currentReplica(t, tr, "a")
	ws := startN(t, tr, a, 3)
	clock.Advance(stuckTimeout + time.Second)
	endAll(ws) // no scan ran
	if tr.IsHealthy("a") {
		t.Fatal("3 stalled attempts ended between scans did not eject a")
	}

	// Ended before the timeout: nothing.
	tr2, clock2, _ := stuckTracker(t, nil)
	b := currentReplica(t, tr2, "b")
	ws = startN(t, tr2, b, 5)
	clock2.Advance(stuckTimeout - time.Second)
	endAll(ws)
	if s, _ := counts(tr2, "b"); s != 0 || !tr2.IsHealthy("b") {
		t.Fatalf("attempts ended before the timeout counted: S=%d", s)
	}
}

// TestStuckEjectionReadmittedOnlyPerEjectionRules: a stuck engine's /health
// keeps passing; it is re-admitted only by a good probe after its ejection
// time (M1-12), and stalls while it is out change nothing.
func TestStuckEjectionReadmittedOnlyPerEjectionRules(t *testing.T) {
	tr, clock, tm := stuckTracker(t, func(c *Config) { c.StuckEngineStreams = 1 })
	a := currentReplica(t, tr, "a")
	w1, w2 := tr.StartFirstChunkWait(a, nil), tr.StartFirstChunkWait(a, nil)
	clock.Advance(stuckTimeout)
	tr.CheckStuckStreams() // one ejects; the other's stall comes while a is out
	if tr.IsHealthy("a") {
		t.Fatal("a not ejected")
	}
	until := stateOf(tr, "a").ejectedUntil
	tr.recordProbe(a, true)
	if tr.IsHealthy("a") {
		t.Fatal("a passing /health re-admitted a stuck engine before its ejection time")
	}
	clock.Advance(4 * time.Second)
	tr.recordProbe(a, true)
	if tr.IsHealthy("a") {
		t.Fatal("re-admitted before min_ejection (5 s)")
	}
	if got := stateOf(tr, "a").ejectedUntil; !got.Equal(until) {
		t.Errorf("ejection time moved by stalls while out: %v -> %v", until, got)
	}
	if s, _ := counts(tr, "a"); s != 0 {
		t.Errorf("stall counted while a was out: S=%d", s)
	}
	clock.Advance(time.Second)
	tr.recordProbe(a, true)
	if !tr.IsHealthy("a") {
		t.Fatal("a good probe after the ejection time did not re-admit a")
	}
	assertMetric(t, scrapeMetrics(t, tm), `gateway_replica_ejections_total{pool="p",reason="stuck_engine"} 1`)
	endAll([]*FirstChunkWait{w1, w2})
}

// TestStallAfterRepointIgnored: a wait that began on a replica's old
// address blames nothing once a reload re-points it, and a wait on a
// replica no longer current is not started.
func TestStallAfterRepointIgnored(t *testing.T) {
	tr, clock, disc := ejectionTracker(t, func(c *Config) { c.StuckEngineStreams = 1 })
	oldA := replicaOf(t, disc, "a")
	w := tr.StartFirstChunkWait(oldA, nil)
	if w == nil {
		t.Fatal("wait on the current replica not started")
	}
	if err := disc.Update(pool(map[string]string{"a": "http://10.0.0.9:8000", "b": "http://10.0.0.2:8000"})); err != nil {
		t.Fatal(err)
	}
	tr.SyncDiscovery()
	if stale := tr.StartFirstChunkWait(oldA, nil); stale != nil {
		t.Fatal("a wait on a's old address was started")
	}
	clock.Advance(stuckTimeout)
	tr.CheckStuckStreams()
	if !tr.IsHealthy("a") {
		t.Fatal("a stall on a's old address ejected its new endpoint")
	}
	if s, _ := counts(tr, "a"); s != 0 {
		t.Fatalf("old-address stall counted on the new endpoint: S = %d", s)
	}
	w.End()

	// Removed altogether: the stall is dropped too.
	b := replicaOf(t, disc, "b")
	wb := tr.StartFirstChunkWait(b, nil)
	if err := disc.Update(pool(map[string]string{"a": "http://10.0.0.9:8000"})); err != nil {
		t.Fatal(err)
	}
	tr.SyncDiscovery()
	clock.Advance(stuckTimeout)
	tr.CheckStuckStreams()
	if _, ok := tr.states.Load("b"); ok {
		t.Fatal("a stall on a removed replica recreated its state")
	}
	wb.End()
	if n := tr.FirstChunkWaits(); n != 0 {
		t.Fatalf("waits = %d, want 0", n)
	}
}

// TestCountsFollowReload: a re-pointed replica's new endpoint does not
// inherit the old one's counts; a replica at the same address keeps them.
func TestCountsFollowReload(t *testing.T) {
	tr, _, disc := ejectionTracker(t, nil)
	tr.StartFirstChunkWait(replicaOf(t, disc, "a"), nil).Received()
	tr.StartFirstChunkWait(replicaOf(t, disc, "b"), nil).Received()
	if err := disc.Update(pool(map[string]string{"a": "http://10.0.0.9:8000", "b": "http://10.0.0.2:8000"})); err != nil {
		t.Fatal(err)
	}
	tr.SyncDiscovery()
	if _, f := counts(tr, "a"); f != 0 {
		t.Error("re-pointed a kept its old endpoint's first chunks")
	}
	if _, f := counts(tr, "b"); f != 1 {
		t.Error("b, at the same address, lost its first chunks")
	}
}

// TestFullRegistryStillCounts: past StuckEngineMaxWaits a wait is not
// registered (fail open, counted), but its first chunk still counts (F),
// and its end past the timeout still counts as a stall; a slot frees when
// a registered wait ends.
func TestFullRegistryStillCounts(t *testing.T) {
	tr, clock, tm := stuckTracker(t, func(c *Config) { c.StuckEngineMaxWaits = 2 })
	a := currentReplica(t, tr, "a")
	w1, w2 := tr.StartFirstChunkWait(a, nil), tr.StartFirstChunkWait(a, nil)
	extra := tr.StartFirstChunkWait(a, nil)
	if w1 == nil || w2 == nil || extra == nil {
		t.Fatal("wait not started")
	}
	if extra.registered || tr.FirstChunkWaits() != 2 {
		t.Fatalf("registered %v, waits %d: want the third unregistered and 2 (the cap)", extra.registered, tr.FirstChunkWaits())
	}
	assertMetric(t, scrapeMetrics(t, tm), `gateway_failopen_total{stage="stuck_engine",reason="registry_full"} 1`)
	extra.Received()
	if _, f := counts(tr, "a"); f != 1 {
		t.Fatalf("F = %d, want 1: an unregistered wait's first chunk still counts", f)
	}
	late := tr.StartFirstChunkWait(a, nil) // also unregistered
	clock.Advance(stuckTimeout)
	late.End()
	if s, _ := counts(tr, "a"); s != 1 {
		t.Fatalf("S = %d, want 1: an unregistered wait ended past the timeout still counts", s)
	}
	w1.End()
	if w3 := tr.StartFirstChunkWait(a, nil); w3 == nil || !w3.registered {
		t.Fatal("no slot after a wait ended")
	} else {
		w3.End()
	}
	w2.End()
	if n := tr.FirstChunkWaits(); n != 0 {
		t.Fatalf("waits = %d, want 0", n)
	}
}

// TestFirstChunkWaitEndsOnce: Received and End in any order and number end
// the wait once and count once; the nil wait is a no-op.
func TestFirstChunkWaitEndsOnce(t *testing.T) {
	tr, _, _ := stuckTracker(t, nil)
	a := currentReplica(t, tr, "a")
	keep := tr.StartFirstChunkWait(a, nil)
	w := tr.StartFirstChunkWait(a, nil)
	w.Received()
	w.End()
	w.Received()
	w.End()
	if n := tr.FirstChunkWaits(); n != 1 {
		t.Fatalf("waits = %d, want 1", n)
	}
	if _, f := counts(tr, "a"); f != 1 {
		t.Fatalf("F = %d, want 1", f)
	}
	var none *FirstChunkWait
	none.Received()
	none.End()
	keep.End()
	if n := tr.FirstChunkWaits(); n != 0 {
		t.Fatalf("waits = %d, want 0", n)
	}
}

// TestReceivedAndEndAllocateNothing: ending a wait is allocation-free.
func TestReceivedAndEndAllocateNothing(t *testing.T) {
	tr, _, _ := stuckTracker(t, func(c *Config) { c.StuckEngineMaxWaits = 1 << 12 })
	a := currentReplica(t, tr, "a")
	ws := startN(t, tr, a, 2*101)
	i := 0
	if n := testing.AllocsPerRun(100, func() { ws[i].Received(); i++ }); n != 0 {
		t.Errorf("Received allocates %v times", n)
	}
	if n := testing.AllocsPerRun(100, func() { ws[i].End(); i++ }); n != 0 {
		t.Errorf("End allocates %v times", n)
	}
}

// TestFirstChunkWaitsConcurrent: waits started and ended from many
// goroutines while scans run leave the registry empty (run with -race).
func TestFirstChunkWaitsConcurrent(t *testing.T) {
	tr, clock, _ := stuckTracker(t, func(c *Config) { c.StuckEngineMaxWaits = 16 })
	a, b := currentReplica(t, tr, "a"), currentReplica(t, tr, "b")
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			for j := range 200 {
				rep := a
				if (i+j)%2 == 0 {
					rep = b
				}
				w := tr.StartFirstChunkWait(rep, nil)
				if j%3 == 0 {
					w.Received()
				}
				w.End()
				if n := tr.FirstChunkWaits(); n > 16 {
					t.Errorf("waits = %d past the cap 16", n)
				}
			}
		})
	}
	wg.Go(func() {
		for range 50 {
			clock.Advance(time.Second)
			tr.CheckStuckStreams()
		}
	})
	wg.Wait()
	if n := tr.FirstChunkWaits(); n != 0 {
		t.Fatalf("waits = %d, want 0", n)
	}
}

func TestWindowCounter(t *testing.T) {
	var c windowCounter
	c.add(0)
	c.add(0)
	c.add(3)
	if got := c.sum(3); got != 3 {
		t.Fatalf("sum = %d, want 3", got)
	}
	c.remove(0)
	if got := c.sum(9); got != 2 {
		t.Fatalf("sum at the window's far edge = %d, want 2", got)
	}
	if got := c.sum(10); got != 1 {
		t.Fatalf("sum once bucket 0 slid out = %d, want 1", got)
	}
	c.add(10) // reuses bucket 0's slot
	if got := c.sum(10); got != 2 {
		t.Fatalf("sum after slot reuse = %d, want 2", got)
	}
	c.add(0)    // older than the slot's bucket: out of the window, dropped
	c.remove(0) // likewise
	if got := c.sum(10); got != 2 {
		t.Fatalf("stale add/remove changed the sum to %d", got)
	}
	c.remove(3)
	c.remove(3) // never below zero
	if got := c.sum(10); got != 1 {
		t.Fatalf("sum = %d, want 1", got)
	}
	c.reset()
	if got := c.sum(10); got != 0 {
		t.Fatalf("sum after reset = %d", got)
	}
}

func TestStuckScanIntervalDerivesFromTimeout(t *testing.T) {
	for _, tc := range []struct{ timeout, want time.Duration }{
		{30 * time.Second, 3 * time.Second},
		{5 * time.Minute, 30 * time.Second},
		{time.Second, 100 * time.Millisecond},
	} {
		c := testConfig()
		c.StuckEngineTimeout = tc.timeout
		if got := c.stuckScanInterval(); got != tc.want {
			t.Errorf("timeout %v: scan every %v, want %v", tc.timeout, got, tc.want)
		}
	}
}

// TestStuckLoopScansInBackground: with the tracker started on the real
// clock (Config.Now unset), a stalled stream is found without a manual scan.
func TestStuckLoopScansInBackground(t *testing.T) {
	disc, err := discovery.NewStatic(pool(map[string]string{"a": "http://10.0.0.1:8000", "b": "http://10.0.0.2:8000"}))
	if err != nil {
		t.Fatal(err)
	}
	cfg := testConfig()
	cfg.StuckEngineTimeout = time.Second // scans every 100 ms
	cfg.StuckEngineStreams = 1
	cfg.PollInterval = time.Hour // no probes
	tr := New(cfg, disc, nil, nil)
	w := tr.StartFirstChunkWait(currentReplica(t, tr, "a"), nil)
	defer w.End()
	tr.Start(t.Context())
	defer tr.Stop()
	deadline := time.Now().Add(5 * time.Second)
	for tr.IsHealthy("a") {
		if time.Now().After(deadline) {
			t.Fatal("background scan never ejected the stuck replica")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestFullRegistryOnRequestLogLine: a wait the full registry cannot watch is
// a fail-open on the attempt's request, so it reaches the request's log line
// and is counted once.
func TestFullRegistryOnRequestLogLine(t *testing.T) {
	var logs strings.Builder
	tel := telemetry.New(telemetry.Options{Logger: slog.New(slog.NewJSONHandler(&logs, nil))})
	tr, _, _ := stuckTracker(t, func(c *Config) { c.StuckEngineMaxWaits = 1 })
	tr.metrics = tel.Metrics() // the requests' metrics, as main wires them
	a := currentReplica(t, tr, "a")
	held := tr.StartFirstChunkWait(a, nil) // fills the registry
	defer held.End()
	h := tel.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w2 := tr.StartFirstChunkWait(a, telemetry.FromContext(r.Context()))
		if w2 == nil || w2.registered {
			t.Error("want an unregistered wait from the full registry")
		} else {
			w2.End()
		}
		w.WriteHeader(http.StatusOK)
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil))
	if want := `"failopen":["stuck_engine:registry_full"]`; !strings.Contains(logs.String(), want) {
		t.Fatalf("log line %s; want it to carry %s", logs.String(), want)
	}
	assertMetric(t, scrapeMetrics(t, tel.Metrics()), `gateway_failopen_total{stage="stuck_engine",reason="registry_full"} 1`)
}
