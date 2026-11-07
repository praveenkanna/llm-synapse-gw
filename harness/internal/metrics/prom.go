// Package metrics captures what the load generator cannot see from its own
// requests: counters exported by the upstream (vLLM or mockllm /metrics,
// scraped before and after a run) and the CPU and memory of the process under
// test (the gateway, sampled during a run).
//
// Everything here is bounded: scrape bodies, series counts and sampler state
// have fixed ceilings, and a sampler keeps running aggregates, not a series.
package metrics

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Scrape ceilings. A vLLM /metrics page is ~100 KiB; these leave ample room.
const (
	maxScrapeBytes  = 16 << 20
	maxSeries       = 200_000
	maxLineBytes    = 1 << 20
	defaultScrapeTO = 10 * time.Second
)

// Sample is one series of a Prometheus text exposition: the metric name, its
// raw label set (without braces) and value.
type Sample struct {
	Name   string
	Labels string
	Value  float64
}

// Snapshot is a parsed /metrics page, taken at At.
type Snapshot struct {
	At      time.Time
	Samples []Sample
}

// Sum adds every series of the named metric. ok is false when the metric is
// absent, so callers can tell "zero" from "not exported".
func (s Snapshot) Sum(name string) (sum float64, ok bool) {
	for _, smp := range s.Samples {
		if smp.Name == name {
			sum += smp.Value
			ok = true
		}
	}
	return sum, ok
}

// SumAny returns the sum of the first of names that is present, and that name.
// vLLM has renamed metrics across versions; callers list current names first.
func (s Snapshot) SumAny(names ...string) (sum float64, name string, ok bool) {
	for _, n := range names {
		if v, found := s.Sum(n); found {
			return v, n, true
		}
	}
	return 0, "", false
}

// Parse reads the Prometheus text exposition format. Comments and blank lines
// are skipped; histogram and summary series are kept as plain samples.
func Parse(r io.Reader) (Snapshot, error) {
	sc := bufio.NewScanner(io.LimitReader(r, maxScrapeBytes))
	sc.Buffer(make([]byte, 0, 64<<10), maxLineBytes)
	var snap Snapshot
	for line := 1; sc.Scan(); line++ {
		text := strings.TrimSpace(sc.Text())
		if text == "" || text[0] == '#' {
			continue
		}
		smp, err := parseLine(text)
		if err != nil {
			return Snapshot{}, fmt.Errorf("line %d: %w", line, err)
		}
		if len(snap.Samples) >= maxSeries {
			return Snapshot{}, fmt.Errorf("more than %d series", maxSeries)
		}
		snap.Samples = append(snap.Samples, smp)
	}
	if err := sc.Err(); err != nil {
		return Snapshot{}, err
	}
	return snap, nil
}

// parseLine parses `name{labels} value [timestamp]`.
func parseLine(text string) (Sample, error) {
	var smp Sample
	end := strings.IndexAny(text, "{ \t")
	if end <= 0 {
		return smp, errors.New("missing value")
	}
	smp.Name = text[:end]
	rest := text[end:]
	if rest[0] == '{' {
		closeAt := labelsEnd(rest)
		if closeAt < 0 {
			return smp, errors.New("unterminated label set")
		}
		smp.Labels = rest[1:closeAt]
		rest = rest[closeAt+1:]
	}
	fields := strings.Fields(rest)
	if len(fields) == 0 || len(fields) > 2 {
		return smp, fmt.Errorf("want value [timestamp], got %q", rest)
	}
	v, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return smp, fmt.Errorf("value: %w", err)
	}
	smp.Value = v
	return smp, nil
}

// labelsEnd returns the index of the '}' closing the label set that s opens,
// skipping braces inside quoted label values, or -1.
func labelsEnd(s string) int {
	inQuote := false
	for i := 1; i < len(s); i++ {
		switch c := s[i]; {
		case inQuote && c == '\\':
			i++ // escaped character
		case c == '"':
			inQuote = !inQuote
		case !inQuote && c == '}':
			return i
		}
	}
	return -1
}

// Scraper fetches /metrics pages.
type Scraper struct {
	HTTP    *http.Client // nil: a private client bounded by Timeout
	Timeout time.Duration
}

// MetricsURL returns target with the path /metrics when it has no path, so a
// base URL such as http://127.0.0.1:8001 can be given.
func MetricsURL(target string) (string, error) {
	u, err := url.Parse(target)
	if err != nil {
		return "", fmt.Errorf("metrics url %q: %w", target, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" || u.Host == "" {
		return "", fmt.Errorf("metrics url %q: want http(s)://host[:port][/path]", target)
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = "/metrics"
	}
	return u.String(), nil
}

// Scrape fetches and parses one page.
func (s Scraper) Scrape(ctx context.Context, target string) (Snapshot, error) {
	u, err := MetricsURL(target)
	if err != nil {
		return Snapshot{}, err
	}
	to := s.Timeout
	if to <= 0 {
		to = defaultScrapeTO
	}
	ctx, cancel := context.WithTimeout(ctx, to)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return Snapshot{}, err
	}
	req.Header.Set("Accept", "text/plain;version=0.0.4")
	hc := s.HTTP
	if hc == nil {
		hc = &http.Client{}
	}
	resp, err := hc.Do(req)
	if err != nil {
		return Snapshot{}, fmt.Errorf("scrape %s: %w", u, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		return Snapshot{}, fmt.Errorf("scrape %s: status %d", u, resp.StatusCode)
	}
	snap, err := Parse(resp.Body)
	if err != nil {
		return Snapshot{}, fmt.Errorf("scrape %s: %w", u, err)
	}
	snap.At = time.Now()
	return snap, nil
}
