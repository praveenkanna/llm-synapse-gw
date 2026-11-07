// Package runner drives an OpenAI-compatible endpoint with synthetic load and
// measures every request: TTFT, inter-token latency, duration and status.
//
// Two load models are supported. Rate mode is open loop: requests arrive on a
// seeded Poisson schedule (optionally with periodic bursts) whether or not
// earlier requests finished, which avoids coordinated omission. Concurrency
// mode is closed loop: N workers each send the next request as soon as their
// previous one ends. In both modes the request sequence comes from a Source
// and is fixed by the seed.
package runner

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/harness/internal/outcome"
)

// Config controls one run. Exactly one of Rate and Concurrency is set, and at
// least one of Duration and MaxRequests bounds the run.
type Config struct {
	Rate        float64       `json:"rate,omitempty"`        // open loop: mean arrivals per second
	Burst       Burst         `json:"burst"`                 // open loop only
	Concurrency int           `json:"concurrency,omitempty"` // closed loop: workers
	Duration    time.Duration `json:"duration_ns"`           // stop issuing after this; 0 = unbounded
	MaxRequests int64         `json:"max_requests"`          // stop after this many; 0 = unbounded
	MaxInFlight int           `json:"max_in_flight"`         // open loop: arrivals beyond this are dropped and recorded
	Seed        uint64        `json:"seed"`
}

// Validate checks the configuration.
func (c Config) Validate() error {
	switch {
	case (c.Rate > 0) == (c.Concurrency > 0):
		return errors.New("set exactly one of rate and concurrency")
	case c.Rate < 0 || c.Concurrency < 0 || c.Duration < 0 || c.MaxRequests < 0:
		return errors.New("rate, concurrency, duration and requests must not be negative")
	case c.Duration == 0 && c.MaxRequests == 0:
		return errors.New("set a duration or a request count")
	case c.Rate > 0 && c.MaxInFlight < 1:
		return errors.New("max in-flight must be >= 1 in rate mode")
	case c.Concurrency > 0 && c.Burst.Enabled():
		return errors.New("bursts apply to rate mode only")
	}
	if c.Rate > 0 {
		_, err := NewArrivals(c.Rate, c.Burst, rand.New(rand.NewPCG(c.Seed, 0)))
		return err
	}
	return nil
}

// Runner executes one run.
type Runner struct {
	cfg    Config
	src    Source
	client *Client
	agg    *Aggregator

	outMu  sync.Mutex
	out    *bufio.Writer
	enc    *json.Encoder
	outErr error

	start    time.Time
	lastDone atomic.Int64 // ns since start of the latest completion
}

// New returns a runner that writes one JSON line per request to raw.
func New(cfg Config, src Source, client *Client, raw io.Writer) (*Runner, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if src == nil || client == nil || client.HTTP == nil {
		return nil, errors.New("source and client are required")
	}
	w := bufio.NewWriterSize(raw, 64<<10)
	return &Runner{cfg: cfg, src: src, client: client, agg: NewAggregator(), out: w, enc: json.NewEncoder(w)}, nil
}

// Run drives load until the run's bounds are reached or ctx is cancelled,
// waits for in-flight requests, and returns the summary. Cancelling ctx also
// cancels in-flight requests; they are recorded as canceled.
func (r *Runner) Run(ctx context.Context) (Summary, error) {
	r.start = time.Now()
	if r.cfg.Rate > 0 {
		r.runOpenLoop(ctx)
	} else {
		r.runClosedLoop(ctx)
	}
	elapsed := time.Duration(r.lastDone.Load())
	if elapsed == 0 {
		elapsed = time.Since(r.start)
	}
	r.outMu.Lock()
	err := errors.Join(r.outErr, r.out.Flush())
	r.outMu.Unlock()
	return r.agg.Summary(elapsed), err
}

func (r *Runner) runOpenLoop(ctx context.Context) {
	arr, _ := NewArrivals(r.cfg.Rate, r.cfg.Burst, rand.New(rand.NewPCG(r.cfg.Seed, 0x5eed)))
	var wg sync.WaitGroup
	defer wg.Wait()
	var inflight atomic.Int64
	timer := time.NewTimer(0)
	defer timer.Stop()
	<-timer.C
	for seq := int64(0); r.cfg.MaxRequests == 0 || seq < r.cfg.MaxRequests; seq++ {
		at := arr.Next()
		if r.cfg.Duration > 0 && at >= r.cfg.Duration {
			return
		}
		// Drawn in sequence order, so the sequence is fixed by the seed.
		var req Request
		if ts, ok := r.src.(TimedSource); ok {
			req = ts.NextAt(at)
		} else {
			req = r.src.Next()
		}
		if wait := time.Until(r.start.Add(at)); wait > 0 {
			timer.Reset(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		} else if ctx.Err() != nil {
			return
		}
		if inflight.Load() >= int64(r.cfg.MaxInFlight) {
			r.finish(Result{Seq: seq, Class: req.Class, Stream: req.Stream, Scheduled: at, Start: time.Since(r.start), Err: outcome.ErrDropped})
			continue
		}
		inflight.Add(1)
		wg.Add(1)
		go func(seq int64, at time.Duration, req Request) {
			defer wg.Done()
			defer inflight.Add(-1)
			r.send(ctx, seq, at, req)
		}(seq, at, req)
	}
}

func (r *Runner) runClosedLoop(ctx context.Context) {
	var mu sync.Mutex
	var seq int64
	next := func() (int64, Request, bool) {
		mu.Lock()
		defer mu.Unlock()
		if ctx.Err() != nil || (r.cfg.MaxRequests > 0 && seq >= r.cfg.MaxRequests) ||
			(r.cfg.Duration > 0 && time.Since(r.start) >= r.cfg.Duration) {
			return 0, Request{}, false
		}
		s := seq
		seq++
		return s, r.src.Next(), true
	}
	var wg sync.WaitGroup
	for range r.cfg.Concurrency {
		wg.Go(func() {
			for {
				s, req, ok := next()
				if !ok {
					return
				}
				r.send(ctx, s, time.Since(r.start), req)
			}
		})
	}
	wg.Wait()
}

func (r *Runner) send(ctx context.Context, seq int64, scheduled time.Duration, req Request) {
	startOff := time.Since(r.start)
	res := r.client.Do(ctx, req, r.agg.GapRecorder(req.Class))
	res.Seq, res.Scheduled, res.Start = seq, scheduled, startOff
	r.finish(res)
}

func (r *Runner) finish(res Result) {
	done := int64(time.Since(r.start))
	for {
		cur := r.lastDone.Load()
		if done <= cur || r.lastDone.CompareAndSwap(cur, done) {
			break
		}
	}
	r.agg.Record(res)
	rec := recordOf(res, r.cfg.Burst)
	r.outMu.Lock()
	defer r.outMu.Unlock()
	if r.outErr == nil {
		r.outErr = r.enc.Encode(rec)
	}
}

// recordOf is the results line for res. A request is in a burst window when
// its scheduled arrival is (ADR 0021), whenever it actually started.
func recordOf(res Result, burst Burst) outcome.Record {
	return outcome.Record{
		Seq: res.Seq, Class: res.Class, Stream: res.Stream, TraceID: res.TraceID,
		ScheduledMS: ms(res.Scheduled), StartMS: ms(res.Start), LagMS: ms(res.Start - res.Scheduled),
		Status: res.Status, Error: res.Err, OK: res.OK(), Complete: res.Complete,
		InBurst: burst.Windows().Contains(res.Scheduled),
		TTFTMS:  ms(res.TTFT), FirstByteMS: ms(res.FirstByte), DurationMS: ms(res.Duration),
		ITLMeanMS: ms(res.ITLMean), ITLMaxMS: ms(res.ITLMax),
		OutputTokens: res.OutputTokens, PromptTokens: res.PromptTokens, Chunks: res.Chunks,
		Cache: res.Cache,
	}
}
