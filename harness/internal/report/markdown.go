package report

import (
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/praveenkanna/llm-synapse-gw/harness/internal/metrics"
)

// Doc is everything a results file holds beyond the runs themselves.
type Doc struct {
	Date, Milestone, Profile, Load string
	// Setup fills "- Key:" lines of the Setup section, by key.
	Setup map[string]string
	// Commands replaces the <exact commands> placeholder.
	Commands string
	// Comparisons: the first fills the results table; all get a details section.
	Comparisons []Comparison
	// Checks names the acceptance criteria to evaluate (CheckOverhead, ...).
	Checks []string
	// Verdict and Notes are free text added after the generated content.
	Verdict, Notes string
}

var (
	setupLine      = regexp.MustCompile(`^- ([^:]+):\s*(.*)$`)
	baselineHeader = regexp.MustCompile(`Baseline \([^)]*\)`)
	targetHeader   = regexp.MustCompile(`Target \([^)]*\)`)
)

// Render fills template (docs/results/TEMPLATE.md) with d.
func Render(template string, d Doc) (string, error) {
	if len(d.Comparisons) == 0 {
		return "", errors.New("render: no comparisons")
	}
	var out strings.Builder
	w := func(s string) { out.WriteString(s + "\n") }
	lines := strings.Split(strings.TrimRight(template, "\n"), "\n")
	section, title, inTable, sawTable := "", false, false, false
	seen := map[string]bool{}
	for _, line := range lines {
		if inTable && !strings.HasPrefix(line, "|") {
			inTable = false
			writeExtraRows(w, d.Comparisons[0])
		}
		switch {
		case strings.HasPrefix(line, "# ") && !title:
			title = true
			w(fmt.Sprintf("# %s · %s · %s · %s", d.Date, d.Milestone, d.Profile, d.Load))
			continue
		case strings.HasPrefix(line, "## "):
			section = strings.TrimSpace(strings.TrimPrefix(line, "## "))
			seen[section] = true
			switch section {
			case "Verdict":
				writeDetails(w, d.Comparisons)
				w(line)
				w("")
				writeVerdict(w, d)
			case "Notes":
				w(line)
				w("")
				writeNotes(w, d)
			default:
				w(line)
			}
			continue
		}
		switch section {
		case "Verdict", "Notes":
			// The template's placeholder text is replaced by the generated content.
		case "Setup":
			if m := setupLine.FindStringSubmatch(line); m != nil && d.Setup[m[1]] != "" {
				w(fmt.Sprintf("- %s: %s", m[1], d.Setup[m[1]]))
			} else if strings.TrimSpace(line) == "<exact commands>" {
				w(strings.TrimRight(d.Commands, "\n"))
			} else {
				w(line)
			}
		case "Results":
			if !strings.HasPrefix(line, "|") {
				w(line)
				continue
			}
			if !inTable {
				inTable, sawTable = true, true
				w(tableHeader(line, d.Comparisons[0]))
				continue
			}
			w(tableRow(line, d.Comparisons[0]))
		default:
			w(line)
		}
	}
	if inTable {
		writeExtraRows(w, d.Comparisons[0])
	}
	for _, s := range []string{"Setup", "Results", "Verdict", "Notes"} {
		if !seen[s] {
			return "", fmt.Errorf("render: template has no %q section", s)
		}
	}
	if !sawTable {
		return "", errors.New("render: template has no results table")
	}
	return out.String(), nil
}

func tableHeader(line string, c Comparison) string {
	line = baselineHeader.ReplaceAllLiteralString(line, "Baseline ("+c.Baseline.Name()+")")
	return targetHeader.ReplaceAllLiteralString(line, "Target ("+c.Target.Name()+")")
}

func tableRow(line string, c Comparison) string {
	cells := strings.Split(strings.Trim(strings.TrimSpace(line), "|"), "|")
	metric := strings.TrimSpace(cells[0])
	if metric == "" || strings.Trim(metric, "-: ") == "" { // separator row
		return line
	}
	return formatRow(c.Row(metric))
}

func formatRow(r Row) string {
	return fmt.Sprintf("| %s | %s | %s | %s |", r.Metric, r.Baseline, r.Target, r.Delta)
}

func writeExtraRows(w func(string), c Comparison) {
	for _, m := range ExtraRows {
		w(formatRow(c.Row(m)))
	}
	w("")
	w(fmt.Sprintf("Comparison shown: **%s** (baseline `%s`, target `%s`). TTFT and throughput cover requests started after the %s warm-up; delta is target minus baseline, a difference of percentiles.",
		c.Name, filepath.Base(c.Baseline.Dir), filepath.Base(c.Target.Dir), c.Target.Window.Warmup))
	w("")
	w("TTFT rows name a traffic class; n/a means the run has no class of that name (see `TTFT p95 all` and the details below).")
}

func writeDetails(w func(string), comps []Comparison) {
	for _, c := range comps {
		w("## Details: " + c.Name)
		w("")
		for _, r := range []*Run{c.Baseline, c.Target} {
			w(fmt.Sprintf("- %s: `%s` → %s; window %.1f s after %s warm-up (%d warm-up requests excluded); scheduling lag p50/p99/max %.1f/%.1f/%.1f ms.",
				r.Name(), filepath.Base(r.Dir), r.Info.URL, r.Window.Seconds, r.Window.Warmup, r.Window.Excluded,
				r.Window.Lag.P50MS, r.Window.Lag.P99MS, r.Window.Lag.MaxMS))
		}
		for _, r := range []*Run{c.Baseline, c.Target} {
			w(runStatusLine(r))
		}
		w("")
		w("| Class | Run | Requests | OK | Err % | 5xx | 429 | req/s | out tok/s | TTFT p50 | p95 | p99 | Duration p50 | p95 |")
		w("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
		for _, tc := range c.Target.Window.Classes {
			bc, _ := c.Baseline.Window.Class(tc.Class)
			for _, x := range []struct {
				run string
				s   ClassStats
			}{{c.Baseline.Name(), bc}, {c.Target.Name(), tc}} {
				s := x.s
				w(fmt.Sprintf("| %s | %s | %d | %d | %.2f | %d | %d | %.1f | %.0f | %.1f | %.1f | %.1f | %.1f | %.1f |",
					tc.Class, x.run, s.Requests, s.OK, 100*s.ErrorRate(), s.Status5xx, s.Shed, s.ReqPerSec, s.OutputTokPerSec,
					s.TTFT.P50MS, s.TTFT.P95MS, s.TTFT.P99MS, s.Duration.P50MS, s.Duration.P95MS))
			}
		}
		w("")
		writeUpstreams(w, c)
		writeProcesses(w, c)
	}
}

func writeUpstreams(w func(string), c Comparison) {
	var rows []string
	for _, r := range []*Run{c.Baseline, c.Target} {
		if r.Metrics == nil {
			continue
		}
		for _, u := range r.Metrics.Upstreams {
			if u.Error != "" {
				rows = append(rows, fmt.Sprintf("| %s | %s | scrape failed: %s | | | | |", r.Name(), u.Target, u.Error))
				continue
			}
			hr := na
			if u.PrefixCacheHitRate != nil {
				hr = fmt.Sprintf("%.1f", 100**u.PrefixCacheHitRate)
			}
			rows = append(rows, fmt.Sprintf("| %s | %s | %s | %s | %s | %s | %s |", r.Name(), u.Target, hr,
				counter(u.PrefixCacheQueries), counter(u.PrefixCacheHits), counter(u.Preemptions), counter(u.GenerationTokens)))
		}
	}
	if len(rows) == 0 {
		w("No upstream `/metrics` were scraped for these runs.")
		w("")
		return
	}
	w("Upstream counters, scraped before and after each run:")
	w("")
	w("| Run | Upstream | Prefix-cache hit % | Queried tokens | Hit tokens | Preemptions | Generation tokens |")
	w("| --- | --- | --- | --- | --- | --- | --- |")
	for _, r := range rows {
		w(r)
	}
	w("")
}

func counter(c metrics.Counter) string {
	switch {
	case !c.Exported:
		return "not exported"
	case c.Reset:
		return fmt.Sprintf("%.0f (counter reset)", c.Delta)
	}
	return fmt.Sprintf("%.0f", c.Delta)
}

func writeProcesses(w func(string), c Comparison) {
	for _, r := range []*Run{c.Baseline, c.Target} {
		p := process(r)
		if p == nil {
			continue
		}
		w(fmt.Sprintf("Process sampled during %s (%s, %d samples, %d errors): CPU %.1f s = mean %.2f / max %.2f cores; RSS start %.0f, end %.0f, mean %.0f, max %.0f MB.",
			r.Name(), p.Source, p.Samples, p.Errors, p.CPUSeconds, p.CPUCoresMean, p.CPUCoresMax, p.RSSStartMB, p.RSSEndMB, p.RSSMeanMB, p.RSSMaxMB))
		w("")
	}
}

func writeVerdict(w func(string), d Doc) {
	var all []string
	for _, c := range d.Comparisons {
		for _, ch := range c.Checks(d.Checks) {
			res := "FAIL"
			if ch.Pass {
				res = "PASS"
			}
			all = append(all, fmt.Sprintf("| %s | %s | %s | **%s** | %s |", ch.AC, c.Name, ch.What, res, ch.Detail))
		}
	}
	if len(all) > 0 {
		w("Computed by `harness/cmd/report`:")
		w("")
		w("| Criterion | Comparison | Check | Result | Detail |")
		w("| --- | --- | --- | --- | --- |")
		for _, l := range all {
			w(l)
		}
		w("")
	}
	if v := strings.TrimSpace(d.Verdict); v != "" {
		w(v)
		w("")
	}
}

func writeNotes(w func(string), d Doc) {
	if n := strings.TrimSpace(d.Notes); n != "" {
		w(n)
	}
}

// runStatusLine counts every request of the run by outcome, warm-up included,
// so errors during the warm-up (such as a connect burst) are never hidden.
func runStatusLine(r *Run) string {
	var total int64
	var parts []string
	for _, k := range slices.Sorted(maps.Keys(r.Window.RunStatuses)) {
		n := r.Window.RunStatuses[k]
		total += n
		parts = append(parts, fmt.Sprintf("%s × %d", k, n))
	}
	if len(parts) == 0 {
		parts = []string{"none"}
	}
	return fmt.Sprintf("- %s, whole run (warm-up included): %d requests, %d 5xx; outcomes: %s.",
		r.Name(), total, r.Window.Run5xx, strings.Join(parts, ", "))
}
