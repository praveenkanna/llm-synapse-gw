package coalesce

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// Reasons a request runs as an ordinary request instead of through a
// flight, for gateway_failopen_total{stage="coalesce",reason}.
const (
	ReasonEvicted     = "evicted"
	ReasonFlightError = "flight_failed"
)

// chunkMax is the largest piece a response is recorded in. Writes are split
// at it so the replay bound holds to within one piece.
const chunkMax = 4 << 10

// MinReplayBytes is the smallest replay bound that keeps the slack of one
// piece small next to the bound (config's lower bound on
// coalescing.max_replay_bytes).
const MinReplayBytes = config.MinCoalesceMaxReplayBytes

// Errors a subscriber's Serve can end with, besides a write error to its
// own client.
var (
	// ErrEvicted: the subscriber fell further behind than the replay bound.
	ErrEvicted = errors.New("coalesce: subscriber evicted: further behind than the replay bound")
	// ErrFlightFailed: the leader's exchange ended without completing
	// (it was aborted); the subscriber has what was written so far.
	ErrFlightFailed = errors.New("coalesce: the shared upstream call did not complete")
	// ErrResponseFailed: the leader's response failed after it started and
	// what was written says so (a stream's error event); the subscriber has
	// all of it.
	ErrResponseFailed = errors.New("coalesce: the shared response failed")
	// errNoSubscribers cancels the upstream call of a flight nobody wants.
	errNoSubscribers = errors.New("coalesce: no client left to serve")
)

// Events reported through Config.Observe.
const (
	EventSealed    = "sealed"    // a response outgrew the replay bound
	EventEvicted   = "evicted"   // a subscriber fell further behind than the bound
	EventCancelled = "cancelled" // the upstream call was cancelled: no client left
)

// Config bounds a Group with the coalescing section, validated by config,
// which owns its defaults and bounds (ADR 0022): MaxFlights flights at
// once, shared or not; MaxSubscribers subscribers per flight besides its
// leader; MaxReplayBytes response bytes a flight keeps, plus at most
// chunkMax. MaxKeyBodyBytes is read by whoever builds the keys.
type Config struct {
	config.Coalescing
	// Observe, if set, is told of each event, possibly with a flight's lock
	// held, so it must be quick and must not call back into the Group.
	Observe func(event string)
}

func (g *Group) observe(event string) {
	if g.cfg.Observe != nil {
		g.cfg.Observe(event)
	}
}

// Stats is a point-in-time view of a Group.
type Stats struct {
	Flights       int    // flights holding memory now
	RetainedBytes int64  // bytes held across all flights
	Led           uint64 // flights started
	Joined        uint64 // requests that subscribed to a running flight
	Sealed        uint64 // flights that outgrew the replay bound
	Evicted       uint64 // subscribers cut off for falling too far behind
	Cancelled     uint64 // upstream calls cancelled because nobody was left
}

const (
	groupShards    = 16
	groupShardMask = groupShards - 1
)

type flightShard struct {
	mu      sync.Mutex
	flights map[string]*Flight
	_       [48]byte // pad shard to cache line
}

// Group is the table of flights. It is safe for concurrent use.
type Group struct {
	cfg    Config
	shards [groupShards]flightShard

	active                                  atomic.Int64 // flights that have not released their memory
	retained                                atomic.Int64
	led, joined, sealed, evicted, cancelled atomic.Uint64
}

// NewGroup returns an empty Group. cfg's fields must be positive.
func NewGroup(cfg Config) *Group {
	g := &Group{cfg: cfg}
	for i := range g.shards {
		g.shards[i].flights = make(map[string]*Flight)
	}
	return g
}

func (g *Group) shardFor(key string) *flightShard {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= prime64
	}
	return &g.shards[h&groupShardMask]
}

func (g *Group) reserveFlight() bool {
	if g.cfg.MaxFlights <= 0 {
		return false
	}
	for {
		cur := g.active.Load()
		if int(cur) >= g.cfg.MaxFlights {
			return false
		}
		if g.active.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

// Stats returns the Group's counters.
func (g *Group) Stats() Stats {
	return Stats{
		Flights:       int(g.active.Load()),
		RetainedBytes: g.retained.Load(),
		Led:           g.led.Load(),
		Joined:        g.joined.Load(),
		Sealed:        g.sealed.Load(),
		Evicted:       g.evicted.Load(),
		Cancelled:     g.cancelled.Load(),
	}
}

// Acquire registers a request under key. Exactly one of f, sub and reason
// is set:
//
//   - sub: a flight with this key is running and can take another
//     subscriber; the caller must Serve it and Close it.
//   - f: the caller leads a new flight and must call Finish exactly once. A
//     joinable flight is found by later requests with the same key; one that
//     is not joinable (cache recording only) is not. A running flight that
//     cannot take more subscribers (sealed, full or being cancelled) is
//     replaced for new joiners by the new flight.
//   - reason: too many flights; the request runs on its own.
func (g *Group) Acquire(key string, joinable bool) (f *Flight, sub *Subscription, reason string) {
	sh := g.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if joinable {
		if s := g.joinLocked(sh, key); s != nil {
			return nil, s, ""
		}
	}
	if !g.reserveFlight() {
		return nil, nil, telemetry.ReasonFlightsFull
	}
	f = &Flight{g: g, sh: sh, key: key}
	f.chunks = f.chunkBuf[:0]
	g.led.Add(1)
	if joinable {
		sh.flights[key] = f
	}
	return f, nil, ""
}

// Join subscribes to the running flight with key when it can take another
// subscriber, and returns nil otherwise. It never starts a flight.
func (g *Group) Join(key string) *Subscription {
	sh := g.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	return g.joinLocked(sh, key)
}

func (g *Group) joinLocked(sh *flightShard, key string) *Subscription {
	cur := sh.flights[key]
	if cur == nil {
		return nil
	}
	s := cur.tryJoin(g.cfg.MaxSubscribers)
	if s != nil {
		g.joined.Add(1)
	}
	return s
}

// unregister stops new requests from finding f. It must not be called with
// f.mu held (the shard's lock is taken first everywhere else).
func (g *Group) unregister(f *Flight) {
	sh := f.sh
	sh.mu.Lock()
	if sh.flights[f.key] == f {
		delete(sh.flights, f.key)
	}
	sh.mu.Unlock()
}

var slabPool = sync.Pool{
	New: func() any { return new([chunkMax]byte) },
}

type slab struct {
	off  int64
	used int
	buf  *[chunkMax]byte
}

// Flight is one upstream exchange whose response is recorded as it is
// written. Its leader's own handler drives the exchange: see Context,
// Writer and Finish.
type Flight struct {
	g   *Group
	sh  *flightShard
	key string

	mu        sync.Mutex
	notify    chan struct{} // lazily allocated when a subscriber waits; closed on bump
	hasWaiter bool

	hdrReady bool
	status   int
	header   http.Header

	chunkBuf   [4]slab
	chunks     []slab
	base       int64 // offset of the first retained byte
	head       int64 // offset past the last byte written
	readers    int   // subscribers currently writing pending slices outside f.mu
	sealed     bool  // outgrew the replay bound: no new joiners
	anyEvicted bool  // at least one subscriber was evicted
	subs       map[*Subscription]struct{}
	done       bool // the leader's exchange has ended
	failed     bool // ... without completing
	respFail   bool // ... on a response that failed, as what was written says
	closed     bool // no new joiners: cancelling, or done
	released   bool // memory returned to the Group

	leaderGone bool // the leader's own client is gone
	cancelUp   context.CancelCauseFunc
	stopAfter  func() bool
}

// bump wakes every waiting subscriber, allocating nothing when nobody waits.
// f.mu must be held.
func (f *Flight) bump() {
	if f.hasWaiter {
		close(f.notify)
		f.notify = nil
		f.hasWaiter = false
	}
}

func (f *Flight) waitChanLocked() <-chan struct{} {
	if f.notify == nil {
		f.notify = make(chan struct{})
	}
	f.hasWaiter = true
	return f.notify
}

// Context returns the context the upstream call must use. It carries
// parent's values but not its cancellation: the call is cancelled when the
// leader's client has gone and no subscriber is left (ADR 0019), or by
// Finish. It must be called once, before the exchange starts.
func (f *Flight) Context(parent context.Context) context.Context {
	ctx, cancel := context.WithCancelCause(context.WithoutCancel(parent))
	f.mu.Lock()
	f.cancelUp = cancel
	closed := f.closed
	f.mu.Unlock()
	if closed {
		cancel(errNoSubscribers)
	}
	stop := context.AfterFunc(parent, func() { f.leaderLeft() })
	f.mu.Lock()
	f.stopAfter = stop
	f.mu.Unlock()
	return ctx
}

// leaderLeft records that the leader's client is gone. It returns whether
// subscribers remain that still need the response. With none, the flight
// closes and its upstream call is cancelled.
func (f *Flight) leaderLeft() (keepGoing bool) {
	f.mu.Lock()
	f.leaderGone = true
	keepGoing = len(f.subs) > 0 || f.done
	cancelNow := !keepGoing && !f.closed
	if cancelNow {
		f.closed = true
	}
	cancel := f.cancelUp
	f.mu.Unlock()
	if cancelNow {
		f.g.unregister(f)
		f.g.cancelled.Add(1)
		f.g.observe(EventCancelled)
		if cancel != nil {
			cancel(errNoSubscribers)
		}
	}
	return keepGoing
}

// tryJoin adds a subscriber if the flight can serve one more. Called with
// the shard's lock held.
func (f *Flight) tryJoin(max int) *Subscription {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed || f.sealed || f.done || len(f.subs) >= max {
		return nil
	}
	if f.subs == nil {
		f.subs = make(map[*Subscription]struct{}, 4)
	}
	s := &Subscription{f: f}
	f.subs[s] = struct{}{}
	return s
}

// setHeader records the response status and headers. Called once, by the
// leader's Writer.
func (f *Flight) setHeader(status int, header http.Header) {
	f.mu.Lock()
	f.hdrReady, f.status, f.header = true, status, header
	f.bump()
	f.mu.Unlock()
}

// append packs p into pooled 4 KiB slabs and enforces the replay bound.
func (f *Flight) append(p []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.released {
		return
	}
	for len(p) > 0 {
		if len(f.chunks) > 0 {
			last := &f.chunks[len(f.chunks)-1]
			if last.used < chunkMax {
				n := copy(last.buf[last.used:], p)
				last.used += n
				f.head += int64(n)
				f.g.retained.Add(int64(n))
				p = p[n:]
				continue
			}
		}
		buf := slabPool.Get().(*[chunkMax]byte)
		n := copy(buf[:], p)
		f.chunks = append(f.chunks, slab{off: f.head, used: n, buf: buf})
		f.head += int64(n)
		f.g.retained.Add(int64(n))
		p = p[n:]
	}
	if f.head-f.base > int64(f.g.cfg.MaxReplayBytes) {
		f.enforceBound()
	}
	f.bump()
}

// enforceBound runs when the retained bytes exceed the bound. It seals the
// flight (a late joiner could no longer be given the start), drops what
// every subscriber has read, and evicts any subscriber still further
// behind than the bound. f.mu must be held.
func (f *Flight) enforceBound() {
	if !f.sealed {
		f.sealed = true
		f.g.sealed.Add(1)
		f.g.observe(EventSealed)
	}
	max := int64(f.g.cfg.MaxReplayBytes)
	f.trim(f.minPos())
	if f.head-f.base <= max {
		return
	}
	for s := range f.subs {
		if s.pos < f.head-max && !s.evicted {
			s.evicted = true
			f.anyEvicted = true
			delete(f.subs, s)
			f.g.evicted.Add(1)
			f.g.observe(EventEvicted)
		}
	}
	f.trim(f.minPos())
}

// minPos is the offset the slowest subscriber has read to; with none, every
// byte written so far.
func (f *Flight) minPos() int64 {
	min := f.head
	for s := range f.subs {
		if s.pos < min {
			min = s.pos
		}
	}
	return min
}

// trim drops whole slabs that end at or before upto. f.mu must be held.
func (f *Flight) trim(upto int64) {
	i := 0
	for i < len(f.chunks) && f.chunks[i].off+int64(f.chunks[i].used) <= upto {
		f.g.retained.Add(-int64(f.chunks[i].used))
		i++
	}
	if i == 0 {
		return
	}
	clear(f.chunks[:i])
	f.chunks = f.chunks[i:]
	if len(f.chunks) > 0 {
		f.base = f.chunks[0].off
	} else {
		f.base = f.head
	}
}

// release returns the flight's memory once its exchange has ended and no
// subscriber can still read it. f.mu must be held.
func (f *Flight) release() {
	if f.released || !f.done || len(f.subs) > 0 || f.readers > 0 {
		return
	}
	f.released = true
	f.g.retained.Add(-(f.head - f.base))
	if !f.sealed && !f.anyEvicted {
		for i := range f.chunks {
			if f.chunks[i].buf != nil {
				slabPool.Put(f.chunks[i].buf)
				f.chunks[i].buf = nil
			}
		}
	} else {
		clear(f.chunks)
	}
	f.chunks, f.base = nil, f.head
	f.g.active.Add(-1)
}

// Recorded is a finished response as the leader wrote it.
type Recorded struct {
	Status int
	Header http.Header
	Body   []byte
}

// Finish ends the flight: the leader's exchange is over. complete is false
// when it ended without finishing (it was aborted); subscribers then get
// ErrFlightFailed after what was written. When keep is set and the whole
// response was recorded (it never outgrew the bound and the exchange
// completed), Finish returns it, for the response cache.
func (f *Flight) Finish(complete, keep bool) (Recorded, bool) {
	return f.finish(complete, false, keep)
}

// FinishFailed ends the flight on a response that failed after it started
// and whose failure is part of what was written (a stream's error event):
// subscribers get all of it, then ErrResponseFailed. Nothing is recorded.
func (f *Flight) FinishFailed() {
	f.finish(true, true, false)
}

func (f *Flight) finish(complete, respFailed, keep bool) (Recorded, bool) {
	var rec Recorded
	var ok bool
	f.mu.Lock()
	if f.stopAfter != nil {
		f.stopAfter()
	}
	if complete && keep && f.hdrReady && !f.sealed && f.base == 0 {
		rec = Recorded{Status: f.status, Header: f.header.Clone(), Body: make([]byte, 0, f.head)}
		for _, c := range f.chunks {
			rec.Body = append(rec.Body, c.buf[:c.used]...)
		}
		ok = true
	}
	wasClosed := f.closed
	f.done, f.failed, f.respFail, f.closed = true, !complete, respFailed, true
	cancel := f.cancelUp
	f.release()
	f.bump()
	f.mu.Unlock()
	if !wasClosed {
		f.g.unregister(f)
	}
	if cancel != nil {
		cancel(nil)
	}
	return rec, ok
}

// Writer wraps the leader's own response writer: everything written goes
// to the leader's client and, as chunks, to the flight. The leader never
// waits on a subscriber. If the leader's client fails, writes keep going
// to the flight for as long as subscribers remain.
type Writer struct {
	f           *Flight
	client      http.ResponseWriter
	flusher     http.Flusher
	wroteHeader bool
	failed      bool
	swallowed   bool
}

func unwrapFlusher(w http.ResponseWriter) http.Flusher {
	for cur := w; cur != nil; {
		if fl, ok := cur.(http.Flusher); ok {
			return fl
		}
		u, ok := cur.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			return nil
		}
		cur = u.Unwrap()
	}
	return nil
}

// Writer returns the leader's writer for client.
func (f *Flight) Writer(client http.ResponseWriter) *Writer {
	return &Writer{f: f, client: client, flusher: unwrapFlusher(client)}
}

// ClientFailed reports whether a write to the leader's client failed. Call
// it from the leader's goroutine, after the exchange.
func (w *Writer) ClientFailed() bool { return w.failed }

// Header returns the leader's client's header map, so what the handler sets
// is what is recorded.
func (w *Writer) Header() http.Header { return w.client.Header() }

// WriteHeader records the status and headers and passes them on.
func (w *Writer) WriteHeader(code int) {
	if code < http.StatusOK { // informational: not the response
		w.client.WriteHeader(code)
		return
	}
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	w.f.setHeader(code, w.client.Header().Clone())
	if !w.failed {
		w.client.WriteHeader(code)
	}
}

// Write records p and writes it to the leader's client. A failed client
// write is reported to the handler only when no subscriber remains: the
// handler then ends the exchange as a client disconnect.
func (w *Writer) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	w.f.append(p)
	if w.failed {
		return len(p), nil
	}
	n, err := w.client.Write(p)
	if err == nil {
		return n, nil
	}
	w.failed = true
	if w.f.leaderLeft() {
		w.swallowed = true
		return len(p), nil
	}
	return n, err
}

// Swallowed reports whether a client write failure was hidden from the
// handler because subscribers remained. The handler never saw that
// disconnect, so it must account for it itself. Call it from the leader's
// goroutine, after the exchange.
func (w *Writer) Swallowed() bool { return w.swallowed }

// Flush flushes the leader's client.
func (w *Writer) Flush() {
	if !w.failed {
		if w.flusher != nil {
			w.flusher.Flush()
		} else {
			_ = http.NewResponseController(w.client).Flush()
		}
	}
}

// Unwrap lets http.ResponseController reach the client's writer.
func (w *Writer) Unwrap() http.ResponseWriter { return w.client }

// Subscription is one request attached to a flight.
type Subscription struct {
	f       *Flight
	pos     int64 // offset read up to, guarded by f.mu
	evicted bool  // guarded by f.mu
	closed  bool
	started bool // Serve has written the response's headers
}

// Started reports whether Serve wrote any of the response to its client.
// When Serve fails before that (the subscriber was evicted, or the flight
// failed before a response existed), the client has been sent nothing and
// the handler may serve it as an ordinary request. Call it from the
// goroutine that called Serve, after it returned.
func (s *Subscription) Started() bool { return s.started }

// Close detaches the subscriber. When it was the last one and the leader's
// client is gone, the upstream call is cancelled. It is idempotent.
func (s *Subscription) Close() {
	f := s.f
	f.mu.Lock()
	if s.closed {
		f.mu.Unlock()
		return
	}
	s.closed = true
	delete(f.subs, s)
	cancelNow := f.leaderGone && len(f.subs) == 0 && !f.done && !f.closed
	if cancelNow {
		f.closed = true
	}
	cancel := f.cancelUp
	f.release()
	f.mu.Unlock()
	if cancelNow {
		f.g.unregister(f)
		f.g.cancelled.Add(1)
		f.g.observe(EventCancelled)
		if cancel != nil {
			cancel(errNoSubscribers)
		}
	}
}

// takeMax bounds what one Serve step takes from the buffer, so a client
// blocked in a write holds at most this much beyond the replay bound.
const takeMax = 64 << 10

// Serve writes the flight's response to w: the headers, then every chunk
// from the start, then each new chunk as the leader writes it, flushing
// after each. observe, if not nil, sees every byte written. It returns nil
// when the response is complete, ErrResponseFailed when it is complete but
// failed, ErrEvicted or ErrFlightFailed if the subscriber cannot finish it,
// ctx's error if the client went away, or the
// write error.
//
// The subscriber's read position moves only once a chunk is written, so a
// client that is slow to read looks as far behind as it is and is evicted
// when it falls past the replay bound.
func (s *Subscription) Serve(ctx context.Context, w http.ResponseWriter, observe func([]byte)) error {
	f := s.f
	fl := unwrapFlusher(w)
	var rc *http.ResponseController
	if fl == nil {
		rc = http.NewResponseController(w)
	}
	flush := func() {
		if fl != nil {
			fl.Flush()
		} else {
			_ = rc.Flush()
		}
	}
	var pieceBuf [8]piece
	wroteHeader := false
	for {
		f.mu.Lock()
		switch {
		case s.evicted:
			f.mu.Unlock()
			return ErrEvicted
		case !f.hdrReady && f.done:
			f.mu.Unlock()
			return ErrFlightFailed
		case !f.hdrReady:
			ch := f.waitChanLocked()
			f.mu.Unlock()
			if err := wait(ctx, ch); err != nil {
				return err
			}
			continue
		}
		status, header := f.status, f.header
		pending := f.piecesFrom(pieceBuf[:0], s.pos, takeMax)
		caughtUp := s.pos+pendingBytes(pending) >= f.head
		done, failed, respFailed := f.done, f.failed, f.respFail
		if len(pending) > 0 {
			f.readers++
		}
		f.mu.Unlock()

		if !wroteHeader {
			wroteHeader = true
			s.started = true
			for k, vv := range header {
				w.Header()[k] = append([]string(nil), vv...)
			}
			w.WriteHeader(status)
			flush()
		}
		if len(pending) > 0 {
			var writeErr error
			for _, p := range pending {
				if _, err := w.Write(p.data); err != nil {
					writeErr = err
					break
				}
				if observe != nil {
					observe(p.data)
				}
				flush()
				f.mu.Lock()
				s.pos = p.end
				if f.sealed { // a subscriber past the start is the only reader that needs these bytes
					f.trim(f.minPos())
				}
				f.mu.Unlock()
			}
			f.mu.Lock()
			f.readers--
			f.release()
			f.mu.Unlock()
			if writeErr != nil {
				return writeErr
			}
		}
		if !caughtUp {
			continue // more is buffered than one step takes
		}
		if done {
			f.mu.Lock()
			more := s.pos < f.head
			f.mu.Unlock()
			if more {
				continue
			}
			switch {
			case failed:
				return ErrFlightFailed
			case respFailed:
				return ErrResponseFailed
			}
			return nil
		}
		f.mu.Lock()
		if s.evicted {
			f.mu.Unlock()
			return ErrEvicted
		}
		if s.pos < f.head || f.done {
			f.mu.Unlock()
			continue
		}
		ch := f.waitChanLocked()
		f.mu.Unlock()
		if err := wait(ctx, ch); err != nil {
			return err
		}
	}
}

// piece is data and the offset just past it.
type piece struct {
	data []byte
	end  int64
}

func pendingBytes(ps []piece) int64 {
	var n int64
	for _, p := range ps {
		n += int64(len(p.data))
	}
	return n
}

// piecesFrom appends onto dst the data written at or after off, at most max
// bytes of it (always at least one piece when any is available). f.mu must be
// held.
func (f *Flight) piecesFrom(dst []piece, off int64, max int) []piece {
	i := sort.Search(len(f.chunks), func(i int) bool {
		c := f.chunks[i]
		return c.off+int64(c.used) > off
	})
	out := dst
	taken := 0
	for ; i < len(f.chunks); i++ {
		c := f.chunks[i]
		data := c.buf[:c.used:c.used]
		if off > c.off {
			data = data[off-c.off:]
		}
		if taken > 0 && taken+len(data) > max {
			break
		}
		out = append(out, piece{data: data, end: c.off + int64(c.used)})
		taken += len(data)
	}
	return out
}

func wait(ctx context.Context, ch <-chan struct{}) error {
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
