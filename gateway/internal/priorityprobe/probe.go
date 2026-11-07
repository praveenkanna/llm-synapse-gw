// Package priorityprobe checks, in the background, that the replicas of
// every pool flagged with priority_scheduling accept a non-zero vLLM
// `priority` (ADR 0009: vLLM rejects one with 400 when it runs without
// --scheduling-policy priority). The gateway sends a priority to a pool only
// while its last probe round passed on every replica; until the first pass,
// after a rejection, or with no model to probe, it sends none (fail open:
// requests stay valid, they lose only the ordering), counted as
// gateway_failopen_total{stage="upstream_priority",reason} once per round.
//
// A probe is one non-streaming completion of one token with a fixed
// synthetic prompt, sent through the upstream client (with the upstream
// API key). 2xx passes; a 4xx fails; a transport error, timeout or 5xx is
// inconclusive and keeps the replica's previous result. Neither request nor
// response text is logged.
package priorityprobe

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

// Fail-open reasons (stage telemetry.StageUpstreamPriority).
const (
	ReasonPending = "unverified"
)

// PathCompletions is the path probed.
const PathCompletions = "/v1/completions"

// maxDrainBytes bounds the response body read before closing.
const maxDrainBytes = 64 << 10

// retryInterval is the round period while some replica has no conclusive
// result yet (not started, unreachable), so priority starts soon after it
// comes up rather than one Interval later.
const retryInterval = 5 * time.Second

// Sender sends one upstream request (*upstream.Client).
type Sender interface {
	Do(ctx context.Context, replica discovery.Replica, req upstream.Request) (*http.Response, error)
}

// FailOpener counts a background fail-open (*telemetry.Metrics).
type FailOpener interface {
	FailOpen(stage, reason string)
}

// Config configures a Prober.
type Config struct {
	// Pools are the flagged pools.
	Pools []string
	// Priority is the non-zero value probed (the largest configured).
	Priority int
	// Model returns the model to probe on a replica, or "".
	Model func(rep discovery.Replica) string
	// PriorityProbe is priority.probe: the probe's interval and timeout,
	// validated by config (ADR 0022).
	config.PriorityProbe
	Logger *slog.Logger
}

// Prober tracks which flagged pools accept a priority.
type Prober struct {
	cfg      Config
	disc     discovery.Discovery
	sender   Sender
	failOpen FailOpener
	verified map[string]*atomic.Bool

	mu      sync.Mutex
	results map[string]bool // replica name -> last conclusive result

	cancel context.CancelFunc
	done   chan struct{}
}

// New builds a Prober; Start runs it.
func New(cfg Config, disc discovery.Discovery, sender Sender, failOpen FailOpener) *Prober {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	p := &Prober{cfg: cfg, disc: disc, sender: sender, failOpen: failOpen,
		verified: make(map[string]*atomic.Bool, len(cfg.Pools)), results: make(map[string]bool)}
	for _, pool := range cfg.Pools {
		p.verified[pool] = new(atomic.Bool)
	}
	return p
}

// Verified reports whether pool's replicas accepted a priority in the last
// round. Unflagged pools are never verified.
func (p *Prober) Verified(pool string) bool {
	if p == nil {
		return false
	}
	v := p.verified[pool]
	return v != nil && v.Load()
}

// Start runs a round at once, then every Interval and whenever the replica
// set changes (when disc is a discovery.Inventory), until Stop or ctx ends.
func (p *Prober) Start(ctx context.Context) {
	if len(p.cfg.Pools) == 0 {
		return
	}
	ctx, p.cancel = context.WithCancel(ctx)
	p.done = make(chan struct{})
	inv, _ := p.disc.(discovery.Inventory)
	go func() {
		defer close(p.done)
		t := time.NewTimer(p.cfg.Interval)
		defer t.Stop()
		for {
			var changed <-chan struct{} // nil without an inventory: never ready
			if inv != nil {
				changed = inv.Changed() // before listing: see discovery.Inventory
			}
			next := p.cfg.Interval
			if p.Round(ctx) {
				next = min(next, retryInterval)
			}
			t.Reset(next)
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			case <-changed:
			}
		}
	}()
}

// Stop ends the background loop and waits for it.
func (p *Prober) Stop() {
	if p.cancel == nil {
		return
	}
	p.cancel()
	<-p.done
}

// Round probes every replica of every flagged pool once, one at a time,
// and updates each pool's verdict. pending reports that some replica has
// no conclusive result yet.
func (p *Prober) Round(ctx context.Context) (pending bool) {
	for _, pool := range p.cfg.Pools {
		reps := p.disc.Replicas(pool)
		if !p.allPassed(reps) {
			p.verified[pool].Store(false) // a replica not yet probed gets no priority meanwhile
		}
		ok, reason := len(reps) > 0, ReasonPending
		for _, rep := range reps {
			pass, why := p.probe(ctx, rep)
			if !pass {
				ok, reason = false, why
				pending = pending || why == ReasonPending
			}
		}
		if ctx.Err() != nil {
			return false
		}
		if prev := p.verified[pool].Swap(ok); prev != ok {
			p.cfg.Logger.Info("vLLM priority pass-through changed", "pool", pool, "enabled", ok, "reason", reasonIf(!ok, reason))
		}
		if !ok && p.failOpen != nil {
			p.failOpen.FailOpen(telemetry.StageUpstreamPriority, reason)
		}
	}
	return pending
}

func reasonIf(cond bool, r string) string {
	if cond {
		return r
	}
	return ""
}

func (p *Prober) allPassed(reps []discovery.Replica) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, rep := range reps {
		if !p.results[rep.Name] {
			return false
		}
	}
	return true
}

// probe returns rep's result: its fresh one if conclusive, else its last.
func (p *Prober) probe(ctx context.Context, rep discovery.Replica) (bool, string) {
	model := ""
	if p.cfg.Model != nil {
		model = p.cfg.Model(rep)
	}
	if model == "" {
		return false, telemetry.ReasonNoModel
	}
	pctx, cancel := context.WithTimeout(ctx, p.cfg.Timeout)
	defer cancel()
	resp, err := p.sender.Do(pctx, rep, upstream.Request{
		Method: http.MethodPost,
		Path:   PathCompletions,
		Header: http.Header{"Content-Type": {"application/json"}, "User-Agent": {upstream.ProbeUserAgent}},
		Body:   probeBody(model, p.cfg.Priority),
	})
	conclusive, pass := false, false
	status := 0
	if err == nil {
		status = resp.StatusCode
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrainBytes))
		_ = resp.Body.Close()
		switch {
		case status >= 200 && status < 300:
			conclusive, pass = true, true
		case status >= 400 && status < 500:
			conclusive = true
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if conclusive {
		if prev, seen := p.results[rep.Name]; !seen || prev != pass {
			p.cfg.Logger.Info("vLLM priority probe", "pool", rep.Pool, "replica", rep.Name, "status", status, "pass", pass)
		}
		p.results[rep.Name] = pass
		if len(p.results) > maxTrackedReplicas {
			clear(p.results) // bounded: replicas come from config; start over
		}
	}
	if p.results[rep.Name] {
		return true, ""
	}
	if conclusive {
		return false, telemetry.ReasonProbeRejected
	}
	return false, ReasonPending
}

// maxTrackedReplicas bounds the per-replica results.
const maxTrackedReplicas = 4096

func probeBody(model string, priority int) []byte {
	m, _ := json.Marshal(model)
	return fmt.Appendf(nil, `{"model":%s,"prompt":"Hi","max_tokens":1,"priority":%d}`, m, priority)
}
