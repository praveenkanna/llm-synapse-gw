package report

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/praveenkanna/llm-synapse-gw/harness/internal/metrics"
	"github.com/praveenkanna/llm-synapse-gw/harness/internal/outcome"
	"github.com/praveenkanna/llm-synapse-gw/harness/internal/runner"
)

// Acceptance targets from docs/plan/phase-0-transparent.md. Change them there
// (with a reason) first, never to make a run pass.
const (
	overheadP50MaxMS    = 5.0    // AC-M0-3
	overheadP95MaxMS    = 15.0   // AC-M0-3, AC-M0-6
	throughputMinRatio  = 0.99   // AC-M0-4
	scaleOpenStreams    = 5000   // AC-M0-6
	scaleStreamsMeanMin = 0.99   // AC-M0-6: time-weighted mean >= 99% of scaleOpenStreams
	scaleNoStreamRPS    = 1000.0 // AC-M0-6
)

// Check names accepted by Checks.
const (
	CheckOverhead   = "AC-M0-3"
	CheckThroughput = "AC-M0-4"
	CheckErrors     = "AC-M0-5"
	CheckScale      = "AC-M0-6"
)

// Comparison pairs a baseline run with a target run from the same session.
type Comparison struct {
	Name     string
	Baseline *Run
	Target   *Run
}

// Row is one line of the results table.
type Row struct {
	Metric, Baseline, Target, Delta string
}

const na = "n/a"

var ttftRow = regexp.MustCompile(`^TTFT (p50|p95|p99) (\S+) \(ms\)$`)

// ExtraRows are appended to the template's results table.
var ExtraRows = []string{
	"TTFT p95 all (ms)",
	"5xx responses",
	"In flight mean / peak",
	"Open streams mean / peak",
	"Scheduling lag p99 (ms)",
	"Gateway CPU mean / max (cores)",
}

// Row fills the named results-table metric. Unknown metrics yield n/a.
func (c Comparison) Row(metric string) Row {
	row := Row{Metric: metric, Baseline: na, Target: na, Delta: na}
	b, t := c.Baseline.Window, c.Target.Window
	if m := ttftRow.FindStringSubmatch(metric); m != nil { // a class the run lacks stays n/a
		bs, _ := b.Class(m[2])
		ts, _ := t.Class(m[2])
		return numeric(row, pct(bs.TTFT, m[1]), bs.TTFT.Count > 0, pct(ts.TTFT, m[1]), ts.TTFT.Count > 0, "%.1f", false)
	}
	ba, ta := b.All(), t.All()
	switch metric {
	case "Throughput (req/s)":
		return numeric(row, ba.ReqPerSec, true, ta.ReqPerSec, true, "%.1f", true)
	case "Output tokens/s":
		return numeric(row, ba.OutputTokPerSec, true, ta.OutputTokPerSec, true, "%.0f", true)
	case "Error rate (%)":
		return numeric(row, 100*ba.ErrorRate(), true, 100*ta.ErrorRate(), true, "%.2f", false)
	case "Shed / 429 rate (%)":
		return numeric(row, 100*ba.ShedRate(), true, 100*ta.ShedRate(), true, "%.2f", false)
	case "5xx responses":
		return numeric(row, float64(ba.Status5xx), true, float64(ta.Status5xx), true, "%.0f", false)
	case "Prefix-cache hit rate (%)":
		bv, bok := hitRate(c.Baseline)
		tv, tok := hitRate(c.Target)
		return numeric(row, 100*bv, bok, 100*tv, tok, "%.1f", false)
	case "Preemptions":
		bv, bok := preemptions(c.Baseline)
		tv, tok := preemptions(c.Target)
		return numeric(row, bv, bok, tv, tok, "%.0f", false)
	case "Gateway RSS max (MB)":
		if p := process(c.Target); p != nil {
			row.Target, row.Baseline, row.Delta = fmt.Sprintf("%.0f", p.RSSMaxMB), "—", "—"
		}
	case "Gateway CPU mean / max (cores)":
		if p := process(c.Target); p != nil {
			row.Target, row.Baseline, row.Delta = fmt.Sprintf("%.2f / %.2f", p.CPUCoresMean, p.CPUCoresMax), "—", "—"
		}
	case "In flight mean / peak":
		row.Baseline, row.Target, row.Delta = conc(b.InFlight), conc(t.InFlight), "—"
	case "Open streams mean / peak":
		if b.HasStream || t.HasStream {
			row.Baseline, row.Target, row.Delta = conc(b.OpenStreams), conc(t.OpenStreams), "—"
		}
	case "Scheduling lag p99 (ms)":
		return numeric(row, b.Lag.P99MS, b.Lag.Count > 0, t.Lag.P99MS, t.Lag.Count > 0, "%.1f", false)
	}
	return row
}

func pct(s outcome.Stats, p string) float64 {
	switch p {
	case "p50":
		return s.P50MS
	case "p95":
		return s.P95MS
	}
	return s.P99MS
}

func numeric(row Row, b float64, bok bool, t float64, tok bool, format string, relative bool) Row {
	if bok {
		row.Baseline = fmt.Sprintf(format, b)
	}
	if tok {
		row.Target = fmt.Sprintf(format, t)
	}
	if bok && tok {
		row.Delta = fmt.Sprintf("%+"+strings.TrimPrefix(format, "%"), t-b)
		if relative && b != 0 {
			row.Delta += fmt.Sprintf(" (%+.1f%%)", 100*(t-b)/b)
		}
	}
	return row
}

func conc(c Concurrency) string { return fmt.Sprintf("%.0f / %d", c.Mean, c.Peak) }

func total(r *Run) *metrics.Upstream {
	if r.Metrics == nil || r.Metrics.Total == nil {
		return nil
	}
	return r.Metrics.Total
}

func hitRate(r *Run) (float64, bool) {
	if t := total(r); t != nil && t.PrefixCacheHitRate != nil {
		return *t.PrefixCacheHitRate, true
	}
	return 0, false
}

func preemptions(r *Run) (float64, bool) {
	if t := total(r); t != nil && t.Preemptions.Exported {
		return t.Preemptions.Delta, true
	}
	return 0, false
}

func process(r *Run) *metrics.ProcessStats {
	if r.Metrics == nil || r.Metrics.Process == nil || r.Metrics.Process.Samples == 0 {
		return nil
	}
	return r.Metrics.Process
}

// Check is one acceptance check against a comparison.
type Check struct {
	AC     string
	What   string
	Pass   bool
	Detail string
}

// Checks evaluates the named acceptance criteria (CheckOverhead, ...).
func (c Comparison) Checks(names []string) []Check {
	var out []Check
	for _, n := range names {
		switch n {
		case CheckOverhead:
			out = append(out, c.overheadChecks(overheadP50MaxMS, overheadP95MaxMS, CheckOverhead)...)
		case CheckThroughput:
			out = append(out, c.throughputChecks()...)
		case CheckErrors:
			out = append(out, c.errorCheck())
		case CheckScale:
			out = append(out, c.scaleChecks()...)
		default:
			out = append(out, Check{AC: n, What: "unknown check", Detail: "no evaluator for this criterion"})
		}
	}
	return out
}

func (c Comparison) overheadChecks(p50Max, p95Max float64, ac string) []Check {
	var out []Check
	for _, tc := range c.Target.Window.Classes {
		bc, ok := c.Baseline.Window.Class(tc.Class)
		if !ok || tc.TTFT.Count == 0 || bc.TTFT.Count == 0 {
			out = append(out, Check{AC: ac, What: "TTFT overhead, class " + tc.Class, Detail: "no successful requests to compare in both runs"})
			continue
		}
		d50, d95 := tc.TTFT.P50MS-bc.TTFT.P50MS, tc.TTFT.P95MS-bc.TTFT.P95MS
		pass := d95 <= p95Max && (p50Max <= 0 || d50 <= p50Max)
		limit := fmt.Sprintf("p95 ≤ %.0f ms", p95Max)
		if p50Max > 0 {
			limit = fmt.Sprintf("p50 ≤ %.0f ms, %s", p50Max, limit)
		}
		out = append(out, Check{
			AC: ac, What: "TTFT overhead, class " + tc.Class, Pass: pass,
			Detail: fmt.Sprintf("p50 %+.1f ms (%.1f → %.1f), p95 %+.1f ms (%.1f → %.1f); limit %s",
				d50, bc.TTFT.P50MS, tc.TTFT.P50MS, d95, bc.TTFT.P95MS, tc.TTFT.P95MS, limit),
		})
	}
	return out
}

func (c Comparison) throughputChecks() []Check {
	b, t := c.Baseline.Window.All(), c.Target.Window.All()
	mk := func(what string, bv, tv float64) Check {
		ch := Check{AC: CheckThroughput, What: what}
		if bv <= 0 {
			ch.Detail = "baseline is zero"
			return ch
		}
		r := tv / bv
		ch.Pass = r >= throughputMinRatio
		ch.Detail = fmt.Sprintf("%.1f vs %.1f direct = %.2f%% (limit ≥ %.0f%%)", tv, bv, 100*r, 100*throughputMinRatio)
		return ch
	}
	return []Check{mk("req/s vs direct", b.ReqPerSec, t.ReqPerSec), mk("output tok/s vs direct", b.OutputTokPerSec, t.OutputTokPerSec)}
}

func (c Comparison) errorCheck() Check {
	b, t := c.Baseline.Window.All(), c.Target.Window.All()
	return Check{
		AC: CheckErrors, What: "5xx through the gateway", Pass: t.Status5xx == 0,
		Detail: fmt.Sprintf("%d 5xx via gateway, %d direct (a 5xx seen on both paths comes from the upstream)", t.Status5xx, b.Status5xx),
	}
}

func (c Comparison) scaleChecks() []Check {
	w := c.Target.Window
	var out []Check
	if w.HasStream {
		minMean := scaleStreamsMeanMin * scaleOpenStreams
		out = append(out, Check{
			AC: CheckScale, What: "concurrent open streams, one gateway",
			Pass:   w.OpenStreams.Peak >= scaleOpenStreams && w.OpenStreams.Mean >= minMean,
			Detail: fmt.Sprintf("mean %.0f, peak %d over %.0f s (limit: peak ≥ %d, mean ≥ %.0f)", w.OpenStreams.Mean, w.OpenStreams.Peak, w.Seconds, scaleOpenStreams, minMean),
		})
	}
	if w.HasNoStream && !w.HasStream {
		rps := w.All().ReqPerSec
		out = append(out, Check{
			AC: CheckScale, What: "non-streaming req/s, one gateway", Pass: rps >= scaleNoStreamRPS,
			Detail: fmt.Sprintf("%.1f req/s succeeded over %.0f s (limit ≥ %.0f)", rps, w.Seconds, scaleNoStreamRPS),
		})
	}
	for _, ch := range c.overheadChecks(0, overheadP95MaxMS, CheckScale) {
		if strings.HasSuffix(ch.What, " "+runner.AllClasses) {
			out = append(out, ch)
		}
	}
	p := process(c.Target)
	ch := Check{AC: CheckScale, What: "gateway CPU and RSS recorded", Pass: p != nil, Detail: "no process samples (run loadgen with -watch-pid or -watch-metrics)"}
	if p != nil {
		ch.Detail = fmt.Sprintf("CPU mean %.2f / max %.2f cores, RSS max %.0f MB (%d samples from %s)", p.CPUCoresMean, p.CPUCoresMax, p.RSSMaxMB, p.Samples, p.Source)
	}
	return append(out, ch)
}
