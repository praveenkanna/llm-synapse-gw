package metrics

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// CaptureConfig says what to capture around a run.
type CaptureConfig struct {
	// Upstreams are /metrics URLs (or base URLs) of vLLM or mockllm replicas,
	// scraped before and after the run.
	Upstreams []string
	// PID is a local process to sample (the gateway); 0 for none.
	PID int
	// ProcessMetrics is a /metrics URL exporting process_* metrics, sampled
	// when PID is 0 (the gateway in another container).
	ProcessMetrics string
	// Interval between process samples.
	Interval time.Duration
	// HTTP client for scrapes; nil uses a client with Timeout.
	HTTP    *http.Client
	Timeout time.Duration
}

// Enabled reports whether there is anything to capture.
func (c CaptureConfig) Enabled() bool {
	return len(c.Upstreams) > 0 || c.PID > 0 || c.ProcessMetrics != ""
}

// Result is written to metrics.json in the run directory.
type Result struct {
	Upstreams []Upstream    `json:"upstreams,omitempty"`
	Total     *Upstream     `json:"total,omitempty"`
	Process   *ProcessStats `json:"process,omitempty"`
}

// Capture is a capture in progress.
type Capture struct {
	cfg     CaptureConfig
	scraper Scraper
	before  []Snapshot

	sampler *Sampler
	cancel  context.CancelFunc
	done    chan struct{}
	once    sync.Once
	result  Result
}

// Start scrapes every upstream once and starts the process sampler. A failed
// "before" scrape is an error: the comparison would be meaningless.
func Start(ctx context.Context, cfg CaptureConfig) (*Capture, error) {
	if cfg.Interval <= 0 {
		cfg.Interval = time.Second
	}
	hc := cfg.HTTP
	if hc == nil {
		hc = &http.Client{Timeout: max(cfg.Timeout, defaultScrapeTO)}
	}
	c := &Capture{cfg: cfg, scraper: Scraper{HTTP: hc, Timeout: cfg.Timeout}}
	for _, u := range cfg.Upstreams {
		snap, err := c.scraper.Scrape(ctx, u)
		if err != nil {
			return nil, fmt.Errorf("before run: %w", err)
		}
		c.before = append(c.before, snap)
	}

	var probe Probe
	var source string
	switch {
	case cfg.PID > 0:
		probe, source = PIDProbe(cfg.PID), fmt.Sprintf("pid %d", cfg.PID)
	case cfg.ProcessMetrics != "":
		probe, source = PromProbe(c.scraper, cfg.ProcessMetrics), cfg.ProcessMetrics
	}
	if probe != nil {
		if _, err := probe(ctx); err != nil {
			return nil, fmt.Errorf("process sampling: %w", err)
		}
		s, err := NewSampler(source, probe, cfg.Interval)
		if err != nil {
			return nil, err
		}
		sctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		c.sampler, c.cancel, c.done = s, cancel, make(chan struct{})
		go func() {
			defer close(c.done)
			s.Run(sctx)
		}()
	}
	return c, nil
}

// Stop ends sampling, scrapes every upstream again and returns the result.
// A failed "after" scrape is recorded in that upstream's Error, not returned,
// so the run's own results are still written. Stop is idempotent.
func (c *Capture) Stop(ctx context.Context) Result {
	c.once.Do(func() {
		if c.sampler != nil {
			c.cancel()
			<-c.done
			st := c.sampler.Stats()
			c.result.Process = &st
		}
		for i, u := range c.cfg.Upstreams {
			after, err := c.scraper.Scrape(ctx, u)
			if err != nil {
				c.result.Upstreams = append(c.result.Upstreams, Upstream{Target: u, Error: errorText(err)})
				continue
			}
			c.result.Upstreams = append(c.result.Upstreams, Diff(u, c.before[i], after))
		}
		if len(c.result.Upstreams) > 0 {
			t := Total(c.result.Upstreams)
			c.result.Total = &t
		}
	})
	return c.result
}

func errorText(err error) string {
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	return err.Error()
}
