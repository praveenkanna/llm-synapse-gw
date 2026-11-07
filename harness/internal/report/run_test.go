package report

import (
	"encoding/json"
	"maps"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/harness/internal/metrics"
	"github.com/praveenkanna/llm-synapse-gw/harness/internal/outcome"
	"github.com/praveenkanna/llm-synapse-gw/harness/internal/runner"
)

// rec builds one results.jsonl line, as the load generator writes it.
func rec(class string, stream bool, startMS, durMS, ttftMS float64, status int, errKind string) outcome.Record {
	return outcome.Record{Class: class, Stream: stream, StartMS: startMS, Status: status, Error: errKind,
		OK: outcome.OK(status, errKind), Complete: errKind == "" && status != 0,
		TTFTMS: ttftMS, DurationMS: durMS, OutputTokens: 10, LagMS: 0.5}
}

// writeRun writes a run directory; m may be nil.
func writeRun(t *testing.T, label string, recs []outcome.Record, m *metrics.Result) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), label)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	info := RunInfo{
		URL: "http://" + label, Label: label, Args: []string{"-label", label, "-rate", "10"},
		Run:    runner.Config{Rate: 10, Duration: 10 * time.Second, Seed: 3},
		Source: runner.SyntheticConfig{Classes: []runner.ClassSpec{{Name: "interactive", Weight: 1, Stream: true, MaxTokens: 16, PromptWords: 8}}},
	}
	write := func(name string, v any) {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(FileRun, info)
	if m != nil {
		write(FileMetrics, m)
	}
	var b strings.Builder
	for _, r := range recs {
		line, _ := json.Marshal(r)
		b.Write(line)
		b.WriteByte('\n')
	}
	if err := os.WriteFile(filepath.Join(dir, FileResults), []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestLoadWindow(t *testing.T) {
	recs := []outcome.Record{
		rec("interactive", true, 0, 1000, 50, 200, ""),    // warm-up: excluded
		rec("interactive", true, 1000, 1000, 10, 200, ""), // ends 2000
		rec("interactive", true, 1500, 1000, 20, 200, ""), // ends 2500
		rec("background", false, 1500, 500, 500, 429, "http_status"),
		rec("background", false, 1600, 400, 400, 503, "http_status"),
		rec("background", false, 1700, 0, 0, 0, outcome.ErrDropped),
		rec("background", false, 2000, 1000, 1000, 200, ""), // ends 3000
	}
	dir := writeRun(t, "gw", recs, nil)
	r, err := Load(dir, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	w := r.Window
	if w.Excluded != 1 || w.Seconds != 2 || !w.HasStream || !w.HasNoStream || r.Metrics != nil || r.Name() != "gw" {
		t.Fatalf("window = %+v", w)
	}
	all := w.All()
	if all.Requests != 6 || all.OK != 3 || all.Errors != 3 || all.Shed != 1 || all.Status5xx != 1 || all.Dropped != 1 {
		t.Fatalf("all = %+v", all)
	}
	// Successful completions in [1000, 3000]: the warm-up request (ends 1000) and three more.
	if all.ReqPerSec != 2 || all.OutputTokPerSec != 20 || all.OutputTokens != 40 {
		t.Fatalf("throughput = %v req/s, %v tok/s", all.ReqPerSec, all.OutputTokPerSec)
	}
	if all.Statuses["429"] != 0 || all.Statuses["error:http_status"] != 2 || all.Statuses["error:dropped"] != 1 || all.Statuses["200"] != 3 {
		t.Fatalf("statuses = %v", all.Statuses)
	}
	inter, ok := w.Class("interactive")
	if !ok || inter.Requests != 2 || math.Abs(inter.TTFT.P50MS-10) > 0.1 || math.Abs(inter.TTFT.P95MS-20) > 0.2 {
		t.Fatalf("interactive = %+v", inter)
	}
	if w.Lag.Count != 6 || math.Abs(w.Lag.P99MS-0.5) > 0.01 {
		t.Fatalf("lag = %+v", w.Lag)
	}
	// In flight over [1000, 3000]: 1000-1500:1, 1500-1600:3, 1600-2000:4, 2000:−3 +1 → 2,
	// 2000-2500:2 (req 1500 and 2000), 2500-3000:1. Area = 500+300+1600+1000+500 = 3900.
	if w.InFlight.Peak != 4 || math.Abs(w.InFlight.Mean-1.95) > 1e-9 {
		t.Fatalf("in flight = %+v", w.InFlight)
	}
	if w.OpenStreams.Peak != 2 {
		t.Fatalf("open streams = %+v", w.OpenStreams)
	}
	if cls := []string{w.Classes[0].Class, w.Classes[1].Class, w.Classes[2].Class}; cls[0] != "background" || cls[2] != runner.AllClasses {
		t.Fatalf("class order = %v", cls)
	}
}

func TestLoadErrors(t *testing.T) {
	if _, err := Load(t.TempDir(), 0); err == nil {
		t.Fatal("empty dir: want error")
	}
	dir := writeRun(t, "x", nil, nil)
	if err := os.WriteFile(filepath.Join(dir, FileResults), []byte("{not json\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(dir, 0); err == nil || !strings.Contains(err.Error(), "line 1") {
		t.Fatalf("bad results line: err = %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, FileMetrics), []byte("[]"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(dir, 0); err == nil {
		t.Fatal("bad metrics.json: want error")
	}
}

func TestSweep(t *testing.T) {
	ev := func(start, end float64, stream bool) []event {
		return []event{{start, 1, stream}, {end, -1, stream}}
	}
	join := func(es ...[]event) []event {
		var out []event
		for _, e := range es {
			out = append(out, e...)
		}
		return out
	}
	tests := []struct {
		name       string
		events     []event
		from, to   float64
		mean       float64
		peak       int64
		streamPeak int64
	}{
		{"back to back on one worker counts once", join(ev(0, 10, true), ev(10, 20, true)), 0, 20, 1, 1, 1},
		{"overlap", join(ev(0, 10, true), ev(5, 10, false)), 0, 10, 1.5, 2, 1},
		{"already in flight when window opens", join(ev(0, 100, true), ev(0, 100, true)), 50, 60, 2, 2, 2},
		{"window cuts the tail", join(ev(0, 10, false), ev(20, 30, false)), 5, 25, 0.5, 1, 0},
		{"empty", nil, 0, 10, 0, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			all, streams := sweep(tt.events, tt.from, tt.to)
			if math.Abs(all.Mean-tt.mean) > 1e-9 || all.Peak != tt.peak || streams.Peak != tt.streamPeak {
				t.Fatalf("all = %+v, streams = %+v; want mean %v peak %d stream peak %d", all, streams, tt.mean, tt.peak, tt.streamPeak)
			}
		})
	}
}

func TestWindowEndsAtIssueCutoff(t *testing.T) {
	recs := []outcome.Record{
		rec("a", true, 0, 1000, 5, 200, ""),    // warm-up, ends 1000
		rec("a", true, 1000, 1000, 5, 200, ""), // ends 2000
		rec("a", true, 1500, 1000, 5, 200, ""), // ends 2500
		rec("a", true, 2000, 1000, 5, 200, ""), // ends 3000: in the drain, after the cutoff
	}
	dir := writeRun(t, "cut", recs, nil)
	w, err := readWindow(filepath.Join(dir, FileResults), time.Second, 2500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	all := w.All()
	if w.EndMS != 2500 || w.Seconds != 1.5 || all.Requests != 3 || all.ReqPerSec != 2 {
		t.Fatalf("window end %v, %v s, %d requests, %v req/s", w.EndMS, w.Seconds, all.Requests, all.ReqPerSec)
	}
	// In flight over [1000, 2500]: 1 until 1500, 2 until 2000, 2 until 2500 → mean 5/3.
	if math.Abs(w.InFlight.Mean-2500.0/1500) > 1e-9 || w.InFlight.Peak != 2 {
		t.Fatalf("in flight = %+v", w.InFlight)
	}
	// A cutoff past the last completion changes nothing.
	w2, _ := readWindow(filepath.Join(dir, FileResults), time.Second, time.Hour)
	if w2.EndMS != 3000 {
		t.Fatalf("end = %v, want last completion 3000", w2.EndMS)
	}
}

// Whole-run status counts include the warm-up, so a 5xx burst while
// connections open is never cut off with it.
func TestRunStatusesIncludeWarmup(t *testing.T) {
	recs := []outcome.Record{
		rec("a", true, 0, 100, 0, 502, outcome.ErrHTTPStatus),   // warm-up
		rec("a", true, 10, 0, 0, 0, outcome.ErrConnect),         // warm-up
		rec("a", true, 20, 100, 5, 200, outcome.ErrStreamError), // warm-up: failed after headers
		rec("a", true, 30, 100, 5, 200, ""),                     // warm-up
		rec("a", true, 1000, 100, 5, 200, ""),
		rec("a", true, 1100, 100, 0, 504, outcome.ErrHTTPStatus),
	}
	r, err := Load(writeRun(t, "gw", recs, nil), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	w := r.Window
	want := map[string]int64{"200": 2, "502": 1, "504": 1, "200 error:stream_error": 1, "error:connect": 1}
	if !maps.Equal(w.RunStatuses, want) || w.Run5xx != 2 {
		t.Fatalf("run statuses = %v, 5xx %d; want %v, 2", w.RunStatuses, w.Run5xx, want)
	}
	if all := w.All(); all.Requests != 2 || all.Status5xx != 1 || w.Excluded != 4 {
		t.Fatalf("window: %d requests, %d 5xx, %d excluded", all.Requests, all.Status5xx, w.Excluded)
	}
	line := runStatusLine(r)
	wantLine := "- gw, whole run (warm-up included): 6 requests, 2 5xx; outcomes: 200 × 2, 200 error:stream_error × 1, 502 × 1, 504 × 1, error:connect × 1."
	if line != wantLine {
		t.Fatalf("status line = %q\nwant          %q", line, wantLine)
	}
}
