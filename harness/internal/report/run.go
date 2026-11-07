// Package report reads loadgen run directories back, compares a baseline run
// (direct to the upstream) with a target run (through the gateway) from the
// same session, and renders a results file from docs/results/TEMPLATE.md.
//
// Statistics are recomputed from results.jsonl, so a warm-up window can be
// cut off and scheduling lag and concurrency measured; summary.json is used
// only for what the per-request records cannot give.
package report

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/harness/internal/metrics"
	"github.com/praveenkanna/llm-synapse-gw/harness/internal/outcome"
	"github.com/praveenkanna/llm-synapse-gw/harness/internal/runner"
)

// Run directory file names, as written by loadgen.
const (
	FileRun     = "run.json"
	FileSummary = "summary.json"
	FileResults = "results.jsonl"
	FileMetrics = "metrics.json"
)

// maxRecords bounds how many results.jsonl lines are read. Per record the
// report keeps two sweep events and up to five latency values (~80 bytes), so
// the ceiling holds memory near 160 MB; a 5,000-stream or 1,000 req/s run of
// a few minutes is well under 1 M records.
const maxRecords = 2_000_000

// RunInfo is run.json.
type RunInfo struct {
	URL    string                 `json:"url"`
	Label  string                 `json:"label"`
	Args   []string               `json:"args"`
	Run    runner.Config          `json:"run"`
	Source runner.SyntheticConfig `json:"source"`
}

// Run is one loadgen run read back from its directory.
type Run struct {
	Dir     string
	Info    RunInfo
	Summary runner.Summary
	Metrics *metrics.Result // nil when the run captured nothing
	Window  Window
}

// Name is the run's label, or its directory name.
func (r *Run) Name() string {
	if r.Info.Label != "" {
		return r.Info.Label
	}
	return filepath.Base(r.Dir)
}

// ClassStats are one class's statistics over the measurement window.
type ClassStats struct {
	Class           string           `json:"class"`
	Requests        int64            `json:"requests"`
	OK              int64            `json:"ok"`
	Errors          int64            `json:"errors"`
	Status5xx       int64            `json:"status_5xx"`
	Shed            int64            `json:"shed"`    // HTTP 429
	Dropped         int64            `json:"dropped"` // not sent: harness in-flight ceiling
	Statuses        map[string]int64 `json:"statuses"`
	OutputTokens    int64            `json:"output_tokens"` // of successful completions in the window
	ReqPerSec       float64          `json:"req_per_sec"`   // successful completions per second
	OutputTokPerSec float64          `json:"output_tok_per_sec"`
	TTFT            outcome.Stats    `json:"ttft"`
	Duration        outcome.Stats    `json:"duration"`
}

// ErrorRate is errors over requests.
func (c ClassStats) ErrorRate() float64 { return ratio(c.Errors, c.Requests) }

// ShedRate is 429s over requests.
func (c ClassStats) ShedRate() float64 { return ratio(c.Shed, c.Requests) }

func ratio(a, b int64) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
}

// Concurrency is the number of requests in flight over the window.
type Concurrency struct {
	Mean float64 `json:"mean"` // time-weighted
	Peak int64   `json:"peak"`
}

// Window holds a run's steady-state statistics over [warm-up, end], where end
// is the run's issue cutoff (its duration) or its last completion, whichever
// is earlier, so the drain after the last request was issued is left out.
// Latency, errors and lag cover requests that started in the window;
// throughput counts successful completions in it; concurrency is
// time-weighted over it.
type Window struct {
	Warmup   time.Duration `json:"warmup_ns"`
	EndMS    float64       `json:"end_ms"`
	Seconds  float64       `json:"seconds"`
	Excluded int64         `json:"excluded"` // records that started during warm-up
	// RunStatuses counts every record of the run, warm-up included, by
	// outcome (see record.runKey); Run5xx counts those with a 5xx status.
	RunStatuses map[string]int64 `json:"run_statuses"`
	Run5xx      int64            `json:"run_5xx"`
	Classes     []ClassStats     `json:"classes"` // sorted by name, then "all"
	HasStream   bool             `json:"has_stream"`
	HasNoStream bool             `json:"has_nostream"`
	Lag         outcome.Stats    `json:"lag"` // actual minus scheduled send time
	InFlight    Concurrency      `json:"in_flight"`
	OpenStreams Concurrency      `json:"open_streams"`
}

// Class returns the named class row, or false.
func (w Window) Class(name string) (ClassStats, bool) {
	for _, c := range w.Classes {
		if c.Class == name {
			return c, true
		}
	}
	return ClassStats{}, false
}

// All returns the row covering every class.
func (w Window) All() ClassStats {
	c, _ := w.Class(runner.AllClasses)
	return c
}

// runKey names a record's outcome for whole-run status counts. Unlike the
// per-class keys (outcome.StatusKey) it keeps the HTTP status of a failed
// request: "502" for a non-2xx response, "200 error:stream_error" for a
// stream that failed after its headers, "error:connect" when no response
// arrived.
func runKey(r outcome.Record) string {
	switch {
	case r.Status == 0:
		return "error:" + r.Error
	case r.Error == "" || r.Error == outcome.ErrHTTPStatus:
		return strconv.Itoa(r.Status)
	default:
		return strconv.Itoa(r.Status) + " error:" + r.Error
	}
}

// Load reads a run directory. run.json and results.jsonl are required;
// summary.json and metrics.json are optional.
func Load(dir string, warmup time.Duration) (*Run, error) {
	r := &Run{Dir: dir}
	if err := readJSON(filepath.Join(dir, FileRun), &r.Info); err != nil {
		return nil, err
	}
	if err := readJSON(filepath.Join(dir, FileSummary), &r.Summary); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	var m metrics.Result
	switch err := readJSON(filepath.Join(dir, FileMetrics), &m); {
	case err == nil:
		r.Metrics = &m
	case !errors.Is(err, os.ErrNotExist):
		return nil, err
	}
	w, err := readWindow(filepath.Join(dir, FileResults), warmup, r.Info.Run.Duration)
	if err != nil {
		return nil, err
	}
	r.Window = w
	return r, nil
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

// acc accumulates one class. Latencies are kept exactly (not in a bucketed
// histogram) so percentile differences of a few milliseconds are resolved
// even on second-long requests.
type acc struct {
	stats          ClassStats
	ttft, duration []float64 // ms, successful requests
	done, tokens   int64     // successful completions in the window, their output tokens
}

// completion is a successful request's end, kept until the window end is known.
type completion struct {
	acc    *acc
	end    float64
	tokens int64
}

func (a *acc) add(r outcome.Record) {
	s := &a.stats
	s.Requests++
	s.Statuses[outcome.StatusKey(r.Status, r.Error)]++
	switch {
	case r.Succeeded():
		s.OK++
		a.ttft = append(a.ttft, r.TTFTMS)
		a.duration = append(a.duration, r.DurationMS)
	default:
		s.Errors++
	}
	switch {
	case r.Status == 429:
		s.Shed++
	case r.Status >= 500:
		s.Status5xx++
	case r.Error == outcome.ErrDropped:
		s.Dropped++
	}
}

// event is a start (+1) or end (-1) of a request at t milliseconds.
type event struct {
	t      float64
	delta  int8
	stream bool
}

func readWindow(path string, warmup, cutoff time.Duration) (Window, error) {
	f, err := os.Open(path)
	if err != nil {
		return Window{}, err
	}
	defer f.Close()
	w := Window{Warmup: warmup, RunStatuses: map[string]int64{}}
	cut := float64(warmup) / float64(time.Millisecond)
	accs := map[string]*acc{}
	all := &acc{stats: ClassStats{Class: runner.AllClasses, Statuses: map[string]int64{}}}
	var lag []float64
	var events []event
	var done []completion
	lastEnd := cut
	err = outcome.ReadRecords(f, maxRecords, func(r outcome.Record) error {
		endMS := r.StartMS + r.DurationMS
		if r.Error != outcome.ErrDropped && r.DurationMS > 0 {
			events = append(events, event{r.StartMS, 1, r.Stream}, event{endMS, -1, r.Stream})
		}
		lastEnd = max(lastEnd, endMS)
		w.RunStatuses[runKey(r)]++
		if r.Status >= 500 {
			w.Run5xx++
		}
		a := accs[r.Class]
		if a == nil {
			a = &acc{stats: ClassStats{Class: r.Class, Statuses: map[string]int64{}}}
			accs[r.Class] = a
		}
		if r.Succeeded() {
			done = append(done, completion{a, endMS, int64(r.OutputTokens)})
		}
		if r.StartMS < cut {
			w.Excluded++
			return nil
		}
		a.add(r)
		all.add(r)
		lag = append(lag, r.LagMS)
		w.HasStream = w.HasStream || r.Stream
		w.HasNoStream = w.HasNoStream || !r.Stream
		return nil
	})
	if err != nil {
		return Window{}, fmt.Errorf("%s: %w", path, err)
	}
	end := lastEnd
	if c := float64(cutoff) / float64(time.Millisecond); c > cut && c < end {
		end = c
	}
	w.EndMS, w.Seconds = end, (end-cut)/1000
	for _, d := range done {
		if d.end >= cut && d.end <= end {
			d.acc.done++
			d.acc.tokens += d.tokens
			all.done++
			all.tokens += d.tokens
		}
	}
	for _, name := range slices.Sorted(maps.Keys(accs)) {
		if accs[name].stats.Requests == 0 { // every request was in the warm-up
			continue
		}
		w.Classes = append(w.Classes, accs[name].finish(w.Seconds))
	}
	w.Classes = append(w.Classes, all.finish(w.Seconds))
	w.Lag = outcome.Percentiles(lag)
	w.InFlight, w.OpenStreams = sweep(events, cut, end)
	return w, nil
}

func (a *acc) finish(seconds float64) ClassStats {
	s := a.stats
	s.OutputTokens = a.tokens
	if seconds > 0 {
		s.ReqPerSec = float64(a.done) / seconds
		s.OutputTokPerSec = float64(a.tokens) / seconds
	}
	s.TTFT, s.Duration = outcome.Percentiles(a.ttft), outcome.Percentiles(a.duration)
	return s
}

// sweep computes in-flight requests (all, and streams only) over [from, to].
// Ends sort before starts at the same instant, so back-to-back requests on
// one closed-loop worker do not count twice.
func sweep(events []event, from, to float64) (all, streams Concurrency) {
	slices.SortFunc(events, func(a, b event) int {
		return cmp.Or(cmp.Compare(a.t, b.t), cmp.Compare(a.delta, b.delta))
	})
	var cur, curS int64
	var area, areaS float64
	prev, inWindow := from, false
	for _, e := range events {
		if e.t > to {
			break
		}
		if e.t >= from {
			if !inWindow { // requests already in flight when the window opens
				inWindow = true
				all.Peak, streams.Peak = cur, curS
			}
			area += float64(cur) * (e.t - prev)
			areaS += float64(curS) * (e.t - prev)
			prev = e.t
		}
		cur += int64(e.delta)
		if e.stream {
			curS += int64(e.delta)
		}
		if inWindow {
			all.Peak, streams.Peak = max(all.Peak, cur), max(streams.Peak, curS)
		}
	}
	if !inWindow {
		all.Peak, streams.Peak = cur, curS
	}
	area += float64(cur) * (to - prev)
	areaS += float64(curS) * (to - prev)
	if span := to - from; span > 0 {
		all.Mean, streams.Mean = area/span, areaS/span
	}
	return all, streams
}
