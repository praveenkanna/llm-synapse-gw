package report

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/harness/internal/metrics"
	"github.com/praveenkanna/llm-synapse-gw/harness/internal/outcome"
)

// closedLoop builds n back-to-back requests on each of workers streams.
func closedLoop(class string, stream bool, workers, n int, durMS, ttftMS float64, status int) []outcome.Record {
	var out []outcome.Record
	for w := range workers {
		for i := range n {
			errKind := ""
			if status != 200 {
				errKind = "http_status"
			}
			out = append(out, rec(class, stream, float64(i)*durMS+float64(w%7)*0.001, durMS, ttftMS, status, errKind))
		}
	}
	return out
}

func hit(r float64) *float64 { return &r }

func loadPairT(t *testing.T, base, target []outcome.Record, bm, tm *metrics.Result) Comparison {
	t.Helper()
	b, err := Load(writeRun(t, "direct", base, bm), 0)
	if err != nil {
		t.Fatal(err)
	}
	g, err := Load(writeRun(t, "gateway", target, tm), 0)
	if err != nil {
		t.Fatal(err)
	}
	return Comparison{Name: "pair", Baseline: b, Target: g}
}

func TestRows(t *testing.T) {
	up := func(rate float64, preempt bool) *metrics.Result {
		tot := metrics.Upstream{Target: "total", PrefixCacheHitRate: hit(rate), Preemptions: metrics.Counter{Delta: 2, Exported: preempt}}
		return &metrics.Result{Total: &tot, Upstreams: []metrics.Upstream{tot}}
	}
	gwm := up(0.5, false)
	gwm.Process = &metrics.ProcessStats{Source: "pid 9", Samples: 5, RSSMaxMB: 123.4, CPUCoresMean: 1.25, CPUCoresMax: 2}
	c := loadPairT(t,
		closedLoop("interactive", true, 2, 10, 100, 10, 200),
		closedLoop("interactive", true, 2, 10, 100, 12, 200),
		up(0.25, false), gwm)

	tests := []struct {
		metric, base, target, delta string
	}{
		{"TTFT p50 interactive (ms)", "10.0", "12.0", "+2.0"},
		{"TTFT p95 all (ms)", "10.0", "12.0", "+2.0"},
		{"TTFT p95 background (ms)", na, na, na}, // no such class
		{"Throughput (req/s)", "20.0", "20.0", "+0.0 (+0.0%)"},
		{"Error rate (%)", "0.00", "0.00", "+0.00"},
		{"Prefix-cache hit rate (%)", "25.0", "50.0", "+25.0"},
		{"Preemptions", na, na, na},
		{"Gateway RSS max (MB)", "—", "123", "—"},
		{"Gateway CPU mean / max (cores)", "—", "1.25 / 2.00", "—"},
		{"In flight mean / peak", "2 / 2", "2 / 2", "—"},
		{"Open streams mean / peak", "2 / 2", "2 / 2", "—"},
		{"5xx responses", "0", "0", "+0"},
		{"Something new", na, na, na},
	}
	for _, tt := range tests {
		r := c.Row(tt.metric)
		if r.Baseline != tt.base || r.Target != tt.target || r.Delta != tt.delta {
			t.Errorf("Row(%q) = %+v; want %s | %s | %s", tt.metric, r, tt.base, tt.target, tt.delta)
		}
	}
}

func checkMap(cs []Check) map[string]Check {
	m := map[string]Check{}
	for _, c := range cs {
		m[c.AC+" "+c.What] = c
	}
	return m
}

func TestChecks(t *testing.T) {
	tests := []struct {
		name     string
		gwTTFT   float64
		gwStatus int
		pass     map[string]bool
	}{
		{"within limits", 14, 200, map[string]bool{
			"AC-M0-3 TTFT overhead, class all": true, "AC-M0-4 req/s vs direct": true, "AC-M0-5 5xx through the gateway": true,
		}},
		{"p50 overhead too high", 16, 200, map[string]bool{"AC-M0-3 TTFT overhead, class all": false}},
		{"gateway 5xx", 10, 502, map[string]bool{"AC-M0-5 5xx through the gateway": false, "AC-M0-4 req/s vs direct": false}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := loadPairT(t, closedLoop("interactive", true, 3, 20, 100, 10, 200), closedLoop("interactive", true, 3, 20, 100, tt.gwTTFT, tt.gwStatus), nil, nil)
			got := checkMap(c.Checks([]string{CheckOverhead, CheckThroughput, CheckErrors, "AC-X"}))
			for k, want := range tt.pass {
				ch, ok := got[k]
				if !ok || ch.Pass != want {
					t.Errorf("%s: got %+v (present %v), want pass=%v", k, ch, ok, want)
				}
			}
			if ch := got["AC-X unknown check"]; ch.Pass {
				t.Error("unknown check passed")
			}
		})
	}
}

func TestScaleChecks(t *testing.T) {
	proc := &metrics.Result{Process: &metrics.ProcessStats{Source: "pid 1", Samples: 3, RSSMaxMB: 80}}
	streams := func(workers int) []outcome.Record { return closedLoop("s", true, workers, 3, 1000, 50, 200) }
	c := loadPairT(t, streams(5000), streams(5000), nil, proc)
	got := checkMap(c.Checks([]string{CheckScale}))
	for _, k := range []string{"AC-M0-6 concurrent open streams, one gateway", "AC-M0-6 TTFT overhead, class all", "AC-M0-6 gateway CPU and RSS recorded"} {
		if !got[k].Pass {
			t.Errorf("%s: %+v", k, got[k])
		}
	}
	if len(got) != 3 {
		t.Errorf("checks = %v", got)
	}

	c = loadPairT(t, streams(4000), streams(4000), nil, nil)
	got = checkMap(c.Checks([]string{CheckScale}))
	if got["AC-M0-6 concurrent open streams, one gateway"].Pass || got["AC-M0-6 gateway CPU and RSS recorded"].Pass {
		t.Errorf("4000 streams without process samples passed: %v", got)
	}

	// Non-streaming: 1,100 req/s over 2 s passes; 900 req/s fails.
	for _, tc := range []struct {
		workers int
		pass    bool
	}{{110, true}, {90, false}} {
		recs := closedLoop("n", false, tc.workers, 20, 100, 100, 200)
		c = loadPairT(t, recs, recs, nil, proc)
		ch := checkMap(c.Checks([]string{CheckScale}))["AC-M0-6 non-streaming req/s, one gateway"]
		if ch.Pass != tc.pass {
			t.Errorf("%d workers: %+v", tc.workers, ch)
		}
	}
}

func TestRenderTemplate(t *testing.T) {
	tmpl := readTemplate(t)
	gm := &metrics.Result{
		Upstreams: []metrics.Upstream{
			{Target: "http://m:8001", PrefixCacheHitRate: hit(0), PrefixCacheQueries: metrics.Counter{Delta: 640, Exported: true}, PrefixCacheHits: metrics.Counter{Exported: true}},
			{Target: "http://m:8002", Error: "status 503"},
		},
		Process: &metrics.ProcessStats{Source: "pid 7", Samples: 10, RSSMaxMB: 64, CPUCoresMean: 0.5, CPUCoresMax: 0.9},
	}
	gm.Total = &metrics.Upstream{Target: "total", PrefixCacheHitRate: hit(0)}
	c := loadPairT(t, closedLoop("chat", true, 2, 10, 100, 10, 200), closedLoop("chat", true, 2, 10, 100, 11, 200), nil, gm)
	d := Doc{
		Date: "2026-09-27", Milestone: "M0", Profile: "scale", Load: "5000 streams",
		Setup:       map[string]string{"Git SHA": "abc123", "Upstream": "mockllm"},
		Commands:    "loadgen -label direct\nloadgen -label gateway",
		Comparisons: []Comparison{c, {Name: "second", Baseline: c.Baseline, Target: c.Target}},
		Checks:      []string{CheckOverhead, CheckErrors},
		Verdict:     "Supports AC-M0-3.",
		Notes:       "Mock upstream.",
	}
	md, err := Render(tmpl, d)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"# 2026-09-27 · M0 · scale · 5000 streams\n",
		"- Git SHA: abc123\n",
		"- Upstream: mockllm\n",
		"- Hardware: <server, GPUs>\n", // left for the human when not given
		"loadgen -label direct\nloadgen -label gateway\n```",
		"| Metric | Baseline (direct) | Target (gateway) | Delta |",
		"| TTFT p50 interactive (ms) | n/a | n/a | n/a |",
		"| TTFT p95 all (ms) | 10.0 | 11.0 | +1.0 |",
		"| Gateway RSS max (MB) | — | 64 | — |",
		"| Gateway CPU mean / max (cores) | — | 0.50 / 0.90 | — |",
		"n/a means the run has no class of that name",
		"## Details: pair\n",
		"## Details: second\n",
		"- gateway, whole run (warm-up included): 20 requests, 0 5xx; outcomes: 200 × 20.\n",
		"| http://m:8002 | scrape failed: status 503 |",
		"| gateway | http://m:8001 | 0.0 | 640 | 0 | not exported | not exported |",
		"| AC-M0-3 | pair | TTFT overhead, class chat | **PASS** |",
		"| AC-M0-5 | second | 5xx through the gateway | **PASS** |",
		"Supports AC-M0-3.\n",
		"## Notes\n\nMock upstream.\n",
	} {
		if !strings.Contains(md, want) {
			t.Errorf("rendered file lacks %q", want)
		}
	}
	for _, gone := range []string{"<exact commands>", "YYYY-MM-DD", "Which acceptance criterion", "Anything surprising", "<direct / round-robin>"} {
		if strings.Contains(md, gone) {
			t.Errorf("placeholder %q left in output", gone)
		}
	}
	if strings.Index(md, "## Details: pair") > strings.Index(md, "## Verdict") {
		t.Error("details must come before the verdict")
	}
}

func TestRenderErrors(t *testing.T) {
	tmpl := readTemplate(t)
	c := loadPairT(t, closedLoop("a", true, 1, 1, 10, 1, 200), closedLoop("a", true, 1, 1, 10, 1, 200), nil, nil)
	if _, err := Render(tmpl, Doc{}); err == nil {
		t.Error("no comparisons: want error")
	}
	for _, drop := range []string{"## Notes", "## Verdict", "## Results"} {
		broken := strings.Replace(tmpl, drop, "## Other", 1)
		if _, err := Render(broken, Doc{Comparisons: []Comparison{c}}); err == nil {
			t.Errorf("template without %s: want error", drop)
		}
	}
	noTable := strings.ReplaceAll(tmpl, "|", "")
	if _, err := Render(noTable, Doc{Comparisons: []Comparison{c}}); err == nil {
		t.Error("template without a table: want error")
	}
}

func readTemplate(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "results", "TEMPLATE.md"))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
