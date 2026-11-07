package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/harness/internal/outcome"
)

func twoClassSource(t *testing.T, seed uint64) *Synthetic {
	t.Helper()
	s, err := NewSynthetic(SyntheticConfig{
		Model: "m", SystemWords: 4,
		Classes: []ClassSpec{
			{Name: "interactive", Weight: 3, Stream: true, MaxTokens: 4, PromptWords: 3},
			{Name: "background", Weight: 1, Stream: false, MaxTokens: 4, PromptWords: 3},
		},
	}, seed)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func readRecords(t *testing.T, raw *bytes.Buffer) []outcome.Record {
	t.Helper()
	var out []outcome.Record
	if err := outcome.ReadRecords(raw, 0, func(r outcome.Record) error { out = append(out, r); return nil }); err != nil {
		t.Fatalf("bad results: %v", err)
	}
	return out
}

func runOnce(t *testing.T, cfg Config, f *fakeUpstream, seed uint64) (Summary, []outcome.Record) {
	t.Helper()
	srv := f.start(t)
	var raw bytes.Buffer
	r, err := New(cfg, twoClassSource(t, seed), newClient(srv.URL), &raw)
	if err != nil {
		t.Fatal(err)
	}
	sum, err := r.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return sum, readRecords(t, &raw)
}

func row(t *testing.T, s Summary, class string) ClassSummary {
	t.Helper()
	for _, c := range s.Classes {
		if c.Class == class {
			return c
		}
	}
	t.Fatalf("no summary row for %q in %+v", class, s.Classes)
	return ClassSummary{}
}

func TestRunOpenLoopRate(t *testing.T) {
	f := &fakeUpstream{ttft: 5 * time.Millisecond, gap: time.Millisecond, tokens: 4}
	cfg := Config{Rate: 200, Duration: 500 * time.Millisecond, MaxInFlight: 1000, Seed: 9}
	sum, recs := runOnce(t, cfg, f, 9)

	all := row(t, sum, AllClasses)
	// 100 expected arrivals; Poisson with a fixed seed, so a loose band.
	if all.Requests < 60 || all.Requests > 140 || all.Requests != int64(len(recs)) {
		t.Fatalf("requests = %d (records %d), want ~100", all.Requests, len(recs))
	}
	if all.OK != all.Requests || all.ErrorRate != 0 {
		t.Fatalf("ok %d of %d, statuses %v", all.OK, all.Requests, all.Statuses)
	}
	inter, bg := row(t, sum, "interactive"), row(t, sum, "background")
	if inter.Requests+bg.Requests != all.Requests || inter.Requests <= bg.Requests {
		t.Fatalf("class split %d/%d of %d", inter.Requests, bg.Requests, all.Requests)
	}
	if inter.TTFT.Count != uint64(inter.OK) || inter.TTFT.P50MS < 5 || inter.TTFT.P99MS < inter.TTFT.P50MS {
		t.Fatalf("interactive TTFT = %+v", inter.TTFT)
	}
	if inter.ITL.Count != uint64(3*inter.OK) {
		t.Fatalf("interactive ITL gaps = %d, want %d", inter.ITL.Count, 3*inter.OK)
	}
	if bg.ITL.Count != 0 || bg.Duration.Count != uint64(bg.OK) {
		t.Fatalf("background ITL/duration = %+v/%+v", bg.ITL, bg.Duration)
	}
	if all.OutputTokens != 4*all.OK || all.ReqPerSec <= 0 || all.OutputTokPerSec <= 0 {
		t.Fatalf("throughput row = %+v", all)
	}
	for i, r := range recs {
		if r.ScheduledMS < 0 || r.ScheduledMS >= 500 || r.Status != 200 || r.TTFTMS <= 0 || r.DurationMS < r.TTFTMS {
			t.Fatalf("record %d = %+v", i, r)
		}
	}
	// Lag covers every request, per class and overall, and is bounded by the
	// worst per-record lag (histogram quantiles are clamped to the max).
	if all.Lag.Count != uint64(all.Requests) || inter.Lag.Count != uint64(inter.Requests) || bg.Lag.Count != uint64(bg.Requests) {
		t.Fatalf("lag counts all/interactive/background = %d/%d/%d, want %d/%d/%d",
			all.Lag.Count, inter.Lag.Count, bg.Lag.Count, all.Requests, inter.Requests, bg.Requests)
	}
	var worst float64
	for _, r := range recs {
		worst = max(worst, r.LagMS)
	}
	if all.Lag.MaxMS != worst || all.Lag.P99MS > worst || all.Lag.P50MS > all.Lag.P99MS {
		t.Fatalf("lag stats %+v, worst record lag %v", all.Lag, worst)
	}
}

func TestRunDeterministicSequence(t *testing.T) {
	cfg := Config{Rate: 500, MaxRequests: 50, MaxInFlight: 1000, Seed: 5}
	seq := func(seed uint64) map[int64]string {
		_, recs := runOnce(t, Config{Rate: cfg.Rate, MaxRequests: cfg.MaxRequests, MaxInFlight: cfg.MaxInFlight, Seed: seed},
			&fakeUpstream{tokens: 2}, seed)
		m := map[int64]string{}
		for _, r := range recs {
			m[r.Seq] = fmt.Sprintf("%s@%v", r.Class, r.ScheduledMS)
		}
		return m
	}
	a, b, c := seq(5), seq(5), seq(6)
	if len(a) != 50 {
		t.Fatalf("got %d requests, want 50", len(a))
	}
	for k, v := range a {
		if b[k] != v {
			t.Fatalf("seq %d: %s vs %s with the same seed", k, v, b[k])
		}
	}
	same := true
	for k, v := range a {
		same = same && c[k] == v
	}
	if same {
		t.Fatal("different seeds gave the same schedule")
	}
}

func TestRunClosedLoopConcurrency(t *testing.T) {
	f := &fakeUpstream{ttft: 10 * time.Millisecond, tokens: 2}
	sum, recs := runOnce(t, Config{Concurrency: 4, MaxRequests: 40, Seed: 1}, f, 1)
	if all := row(t, sum, AllClasses); all.Requests != 40 || all.OK != 40 || len(recs) != 40 {
		t.Fatalf("all row = %+v, records %d", all, len(recs))
	}
	if _, n, maxIn := f.snapshot(); n != 40 || maxIn != 4 {
		t.Fatalf("upstream saw %d requests with max concurrency %d, want 40 and 4", n, maxIn)
	}
	seen := map[int64]bool{}
	for _, r := range recs {
		seen[r.Seq] = true
	}
	if len(seen) != 40 {
		t.Fatalf("sequence numbers not unique: %d distinct", len(seen))
	}
}

func TestRunDropsBeyondMaxInFlight(t *testing.T) {
	f := &fakeUpstream{ttft: 300 * time.Millisecond, tokens: 1}
	sum, _ := runOnce(t, Config{Rate: 1000, MaxRequests: 50, MaxInFlight: 5, Seed: 2}, f, 2)
	all := row(t, sum, AllClasses)
	if dropped := all.Statuses["error:"+outcome.ErrDropped]; dropped < 30 || all.Requests != 50 {
		t.Fatalf("dropped %d of %d, statuses %v", dropped, all.Requests, all.Statuses)
	}
	if all.Lag.Count != 50 {
		t.Fatalf("lag count %d, want 50: dropped requests must count", all.Lag.Count)
	}
	if _, _, maxIn := f.snapshot(); maxIn > 5 {
		t.Fatalf("upstream concurrency %d exceeds max in-flight 5", maxIn)
	}
}

func TestRunCancelRecordsInFlight(t *testing.T) {
	f := &fakeUpstream{fault: "hang"}
	srv := f.start(t)
	var raw bytes.Buffer
	r, err := New(Config{Rate: 100, Duration: time.Minute, MaxInFlight: 100, Seed: 1}, twoClassSource(t, 1), newClient(srv.URL), &raw)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	sum, err := r.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("run did not stop promptly on cancel")
	}
	all := row(t, sum, AllClasses)
	if all.Requests == 0 || all.Statuses["error:"+outcome.ErrCanceled] != all.Requests {
		t.Fatalf("statuses %v, want all canceled", all.Statuses)
	}
	if n := len(readRecords(t, &raw)); int64(n) != all.Requests {
		t.Fatalf("%d records for %d requests", n, all.Requests)
	}
}

func TestConfigValidate(t *testing.T) {
	burst := Burst{Every: time.Second, Length: time.Millisecond, Factor: 2}
	tests := []struct {
		name string
		cfg  Config
		ok   bool
	}{
		{"rate", Config{Rate: 1, Duration: time.Second, MaxInFlight: 1}, true},
		{"rate with burst", Config{Rate: 1, Burst: burst, MaxRequests: 1, MaxInFlight: 1}, true},
		{"concurrency", Config{Concurrency: 1, MaxRequests: 1}, true},
		{"neither", Config{Duration: time.Second}, false},
		{"both", Config{Rate: 1, Concurrency: 1, Duration: time.Second, MaxInFlight: 1}, false},
		{"unbounded", Config{Rate: 1, MaxInFlight: 1}, false},
		{"no in-flight ceiling", Config{Rate: 1, Duration: time.Second}, false},
		{"burst in closed loop", Config{Concurrency: 1, Burst: burst, Duration: time.Second}, false},
		{"bad burst", Config{Rate: 1, Burst: Burst{Every: time.Second}, Duration: time.Second, MaxInFlight: 1}, false},
		{"negative requests", Config{Concurrency: 1, Duration: time.Second, MaxRequests: -1}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.cfg.Validate(); (err == nil) != tc.ok {
				t.Fatalf("Validate() = %v, want ok=%v", err, tc.ok)
			}
		})
	}
}

func TestSummaryTable(t *testing.T) {
	a := NewAggregator()
	a.Record(Result{Class: "x", Status: 200, TTFT: time.Millisecond, Duration: 2 * time.Millisecond, OutputTokens: 3})
	a.Record(Result{Class: "x", Status: 500, Err: outcome.ErrHTTPStatus})
	s := a.Summary(time.Second)
	x := row(t, s, "x")
	if x.Requests != 2 || x.OK != 1 || x.ErrorRate != 0.5 || x.Statuses["200"] != 1 || x.Statuses["error:http_status"] != 1 {
		t.Fatalf("row = %+v", x)
	}
	if s.Classes[len(s.Classes)-1].Class != AllClasses {
		t.Fatal("all row must be last")
	}
	var buf bytes.Buffer
	if err := s.WriteTable(&buf); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(buf.String(), "\n")
	if !strings.Contains(lines[0], "TTFT p50") || !strings.Contains(lines[0], "lag p50") ||
		strings.Fields(lines[1])[0] != "x" || strings.Fields(lines[2])[0] != AllClasses {
		t.Fatalf("table:\n%s", buf.String())
	}
	// Header and rows have the same number of columns ("lag p50" is two words).
	if h, r := len(strings.Fields(lines[0])), len(strings.Fields(lines[1])); h-5 != r {
		t.Fatalf("header has %d fields, row has %d:\n%s", h, r, buf.String())
	}
}

func TestSummaryLagPerClass(t *testing.T) {
	a := NewAggregator()
	// Class "fast": 100 requests, lag 0..99 µs. Class "slow": 100 requests,
	// lag 1..100 ms, one of them dropped and one failed: lag counts them all.
	for i := range 100 {
		a.Record(Result{Class: "fast", Status: 200, Scheduled: time.Second, Start: time.Second + time.Duration(i)*time.Microsecond})
		r := Result{Class: "slow", Status: 200, Scheduled: time.Second, Start: time.Second + time.Duration(i+1)*time.Millisecond}
		switch i {
		case 10:
			r.Status, r.Err = 0, outcome.ErrDropped
		case 20:
			r.Status, r.Err = 503, outcome.ErrHTTPStatus
		}
		a.Record(r)
	}
	a.Record(Result{Class: "fast", Status: 200, Scheduled: 2 * time.Second, Start: time.Second}) // clock skew: clamped to 0

	s := a.Summary(time.Second)
	near := func(got, want float64) bool { return got >= want*0.98 && got <= want*1.02 }
	fast, slow, all := row(t, s, "fast"), row(t, s, "slow"), row(t, s, AllClasses)
	if fast.Lag.Count != 101 || fast.Lag.MaxMS != 0.099 || fast.Lag.P99MS > 0.099 {
		t.Fatalf("fast lag = %+v", fast.Lag)
	}
	if slow.Lag.Count != 100 || !near(slow.Lag.P50MS, 50) || !near(slow.Lag.P95MS, 95) || !near(slow.Lag.P99MS, 99) || slow.Lag.MaxMS != 100 {
		t.Fatalf("slow lag = %+v, want p50/p95/p99 ~ 50/95/99 ms", slow.Lag)
	}
	if all.Lag.Count != 201 || all.Lag.MaxMS != 100 {
		t.Fatalf("all lag = %+v", all.Lag)
	}

	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var back struct {
		Classes []struct {
			Class string `json:"class"`
			Lag   struct {
				P50 *float64 `json:"p50_ms"`
				P95 *float64 `json:"p95_ms"`
				P99 *float64 `json:"p99_ms"`
			} `json:"lag"`
		} `json:"classes"`
	}
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	for _, c := range back.Classes {
		if c.Lag.P50 == nil || c.Lag.P95 == nil || c.Lag.P99 == nil {
			t.Fatalf("summary.json row %q lacks lag p50/p95/p99: %s", c.Class, b)
		}
	}
}

// TestRecordsCarryOutcomeFacts: each record carries the derived facts readers
// use (ADR 0021): ok and complete as judged, the first byte apart from the
// first token, and in_burst by the scheduled arrival, not the actual start.
func TestRecordsCarryOutcomeFacts(t *testing.T) {
	f := &fakeUpstream{roleGap: 20 * time.Millisecond, gap: time.Millisecond, tokens: 3}
	burst := Burst{Every: 100 * time.Millisecond, Length: 50 * time.Millisecond, Factor: 2}
	cfg := Config{Rate: 200, Burst: burst, Duration: 600 * time.Millisecond, MaxInFlight: 1000, Seed: 4}
	_, recs := runOnce(t, cfg, f, 4)
	var in, out, apart int
	for _, r := range recs {
		scheduled := time.Duration(r.ScheduledMS * float64(time.Millisecond))
		if r.InBurst != burst.Windows().Contains(scheduled) {
			t.Fatalf("record %d: in_burst %v for scheduled %v ms", r.Seq, r.InBurst, r.ScheduledMS)
		}
		if r.InBurst {
			in++
		} else {
			out++
		}
		if !r.OK || !r.Complete || r.OK != r.Succeeded() {
			t.Fatalf("record %d: ok %v complete %v, want a judged success: %+v", r.Seq, r.OK, r.Complete, r)
		}
		// The role chunk comes a roleGap before the first token. A late read
		// under load can take both at once, so a record may show them
		// together; never the token first.
		if r.Stream && r.TTFTMS < r.FirstByteMS {
			t.Fatalf("record %d: first byte %v ms, TTFT %v ms; want the first byte (the role chunk) no later than the first token", r.Seq, r.FirstByteMS, r.TTFTMS)
		}
		if r.Stream && r.TTFTMS > r.FirstByteMS {
			apart++
		}
	}
	if in == 0 || out == 0 {
		t.Fatalf("%d records in bursts, %d outside; the run needs both", in, out)
	}
	if apart == 0 {
		t.Fatal("no record has its first byte apart from its first token")
	}
}

// TestRecordBurstWindowByScheduledArrival: in_burst follows the scheduled
// arrival, not the actual start, so a request the generator sent late is
// still counted in the window it was meant for.
func TestRecordBurstWindowByScheduledArrival(t *testing.T) {
	burst := Burst{Every: 100 * time.Millisecond, Length: 50 * time.Millisecond, Factor: 2}
	late := Result{Scheduled: 149 * time.Millisecond, Start: 151 * time.Millisecond, Status: 200}
	if !recordOf(late, burst).InBurst {
		t.Fatal("scheduled in the window, started after it: want in_burst")
	}
	early := Result{Scheduled: 99 * time.Millisecond, Start: 101 * time.Millisecond, Status: 200}
	if recordOf(early, burst).InBurst {
		t.Fatal("scheduled before the window, started in it: want not in_burst")
	}
}
