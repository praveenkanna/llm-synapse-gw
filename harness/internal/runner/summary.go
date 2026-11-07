package runner

import (
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/harness/internal/outcome"
)

// AllClasses is the summary row covering every class.
const AllClasses = "all"

// classStats accumulates one class's measurements; safe for concurrent use.
type classStats struct {
	ttft, itl, duration Histogram
	lag                 Histogram // send time minus scheduled time, every request

	mu           sync.Mutex
	requests     int64
	ok           int64
	outputTokens int64
	promptTokens int64
	statuses     map[string]int64 // "200", "500", "error:connect", ...
}

func newClassStats() *classStats { return &classStats{statuses: map[string]int64{}} }

func (s *classStats) record(r Result) {
	s.lag.Record(r.Start - r.Scheduled)
	if r.OK() {
		s.ttft.Record(r.TTFT)
		s.duration.Record(r.Duration)
	}
	key := outcome.StatusKey(r.Status, r.Err)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests++
	if r.OK() {
		s.ok++
		s.outputTokens += int64(r.OutputTokens)
		s.promptTokens += int64(r.PromptTokens)
	}
	s.statuses[key]++
}

// Aggregator collects per-class and overall statistics in bounded memory.
type Aggregator struct {
	all     *classStats
	mu      sync.RWMutex
	classes map[string]*classStats
}

// NewAggregator returns an empty aggregator.
func NewAggregator() *Aggregator {
	return &Aggregator{all: newClassStats(), classes: map[string]*classStats{}}
}

func (a *Aggregator) class(name string) *classStats {
	a.mu.RLock()
	s, ok := a.classes[name]
	a.mu.RUnlock()
	if ok {
		return s
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if s, ok = a.classes[name]; !ok {
		s = newClassStats()
		a.classes[name] = s
	}
	return s
}

// GapRecorder returns a callback that records inter-token gaps for class.
func (a *Aggregator) GapRecorder(class string) func(time.Duration) {
	s := a.class(class)
	return func(d time.Duration) {
		s.itl.Record(d)
		a.all.itl.Record(d)
	}
}

// Record adds a finished request.
func (a *Aggregator) Record(r Result) {
	a.class(r.Class).record(r)
	a.all.record(r)
}

// ClassSummary is one row of the run summary. Latencies cover successful
// requests only; ITL covers every inter-token gap of streaming requests.
type ClassSummary struct {
	Class           string           `json:"class"`
	Requests        int64            `json:"requests"`
	OK              int64            `json:"ok"`
	Errors          int64            `json:"errors"`
	ErrorRate       float64          `json:"error_rate"`
	Statuses        map[string]int64 `json:"statuses"`
	ReqPerSec       float64          `json:"req_per_sec"`
	OutputTokens    int64            `json:"output_tokens"`
	PromptTokens    int64            `json:"prompt_tokens"`
	OutputTokPerSec float64          `json:"output_tok_per_sec"`
	TTFT            outcome.Stats    `json:"ttft"`
	ITL             outcome.Stats    `json:"itl"`
	Duration        outcome.Stats    `json:"duration"`
	// Lag is send delay against the schedule, over every request including
	// failed and dropped ones. A high lag means the generator fell behind and
	// the run under-delivered its intended load. In closed-loop mode requests
	// are scheduled when sent, so lag is near zero by construction.
	Lag outcome.Stats `json:"lag"`
}

func (s *classStats) summary(name string, elapsed time.Duration) ClassSummary {
	s.mu.Lock()
	cs := ClassSummary{
		Class:        name,
		Requests:     s.requests,
		OK:           s.ok,
		Errors:       s.requests - s.ok,
		Statuses:     maps.Clone(s.statuses),
		OutputTokens: s.outputTokens,
		PromptTokens: s.promptTokens,
	}
	s.mu.Unlock()
	if cs.Requests > 0 {
		cs.ErrorRate = float64(cs.Errors) / float64(cs.Requests)
	}
	if sec := elapsed.Seconds(); sec > 0 {
		cs.ReqPerSec = float64(cs.OK) / sec
		cs.OutputTokPerSec = float64(cs.OutputTokens) / sec
	}
	cs.TTFT, cs.ITL, cs.Duration, cs.Lag = s.ttft.Snapshot(), s.itl.Snapshot(), s.duration.Snapshot(), s.lag.Snapshot()
	return cs
}

// Summary is the whole-run summary written to summary.json.
type Summary struct {
	Elapsed   string         `json:"elapsed"`
	ElapsedMS float64        `json:"elapsed_ms"`
	Classes   []ClassSummary `json:"classes"` // sorted by name, then the "all" row
}

// Summary returns per-class rows plus an "all" row. Throughputs are divided
// by elapsed, the wall time from run start to the last completion.
func (a *Aggregator) Summary(elapsed time.Duration) Summary {
	a.mu.RLock()
	names := slices.Sorted(maps.Keys(a.classes))
	rows := make([]ClassSummary, 0, len(names)+1)
	for _, n := range names {
		rows = append(rows, a.classes[n].summary(n, elapsed))
	}
	a.mu.RUnlock()
	rows = append(rows, a.all.summary(AllClasses, elapsed))
	return Summary{Elapsed: elapsed.Round(time.Millisecond).String(), ElapsedMS: ms(elapsed), Classes: rows}
}

// WriteTable prints the summary as an aligned text table.
func (s Summary) WriteTable(w io.Writer) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintf(tw, "class\treqs\tok\terr%%\treq/s\tout tok/s\tTTFT p50\tp95\tp99\tITL p50\tp95\tp99\tdur p50\tp95\tp99\tlag p50\tp95\tp99\t\n")
	for _, c := range s.Classes {
		fmt.Fprintf(tw, "%s\t%d\t%d\t%.2f\t%.1f\t%.0f\t%.1f\t%.1f\t%.1f\t%.2f\t%.2f\t%.2f\t%.1f\t%.1f\t%.1f\t%.2f\t%.2f\t%.2f\t\n",
			c.Class, c.Requests, c.OK, 100*c.ErrorRate, c.ReqPerSec, c.OutputTokPerSec,
			c.TTFT.P50MS, c.TTFT.P95MS, c.TTFT.P99MS,
			c.ITL.P50MS, c.ITL.P95MS, c.ITL.P99MS,
			c.Duration.P50MS, c.Duration.P95MS, c.Duration.P99MS,
			c.Lag.P50MS, c.Lag.P95MS, c.Lag.P99MS)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	_, err := fmt.Fprintf(w, "latencies in ms (TTFT and duration over successful requests; lag = send delay vs schedule, all requests); elapsed %s\n", s.Elapsed)
	return err
}
