package main

import (
	"context"
	"log/slog"
	"maps"
	"os"
	"slices"
	"strconv"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/arbitrator"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/priorityprobe"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/redis"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// newAdmission builds the arbitrator from admission and priority config, or
// returns nil when admission is off. A pool's capacity follows its replica
// count in disc.
func newAdmission(cfg *config.Config, disc discovery.Discovery, m *telemetry.Metrics) *arbitrator.Arbitrator {
	if !cfg.Admission.Enabled {
		return nil
	}
	pools := make([]arbitrator.Pool, 0, len(cfg.Pools))
	for _, p := range cfg.Pools {
		pools = append(pools, arbitrator.Pool{Name: p.Name, PerReplica: p.MaxInflightPerReplica})
	}
	return arbitrator.New(arbitrator.ConfigFrom(cfg), pools, func(pool string) int { return len(disc.Replicas(pool)) }, m)
}

// followReplicas re-dispatches arb's queues whenever inv's replica set
// changes (capacity may have grown), until ctx ends.
func followReplicas(ctx context.Context, inv discovery.Inventory, arb *arbitrator.Arbitrator) {
	for {
		changed := inv.Changed() // before Kick lists replicas: see discovery.Inventory
		arb.Kick()
		select {
		case <-ctx.Done():
			return
		case <-changed:
		}
	}
}

// startAdmissionSync starts the Redis demand sync when admission.redis is
// on, and returns its stop function (a no-op when off).
func startAdmissionSync(ctx context.Context, cfg *config.Config, arb *arbitrator.Arbitrator, m *telemetry.Metrics, logger *slog.Logger) func() {
	r := cfg.Admission.Redis
	if arb == nil || !r.Enabled {
		return func() {}
	}
	client := redis.New(redis.Options{
		Addr:        r.Addr,
		Username:    r.Username,
		Password:    string(r.Password),
		DB:          r.DB,
		DialTimeout: r.Timeout,
	})
	c := arbitrator.NewCoordinator(arbitrator.CoordinatorConfig{
		Member:   memberName(),
		Interval: r.SyncInterval,
		Timeout:  r.Timeout,
		LeaseTTL: r.LeaseTTL,
		Logger:   logger,
	}, arb, arbitrator.RedisStore{Client: client, Prefix: r.KeyPrefix}, m)
	c.Start(ctx)
	logger.Info("admission shared through Redis", "addr", r.Addr, "sync_interval", r.SyncInterval.String())
	return func() {
		c.Stop()
		_ = client.Close()
	}
}

// memberName identifies this gateway process in Redis.
func memberName() string {
	host, _ := os.Hostname()
	return host + ":" + strconv.Itoa(os.Getpid())
}

// newPriorityProber builds the probe that verifies each priority-scheduled
// pool accepts a vLLM priority before the gateway sends one, with the model
// cfg's probe-model rule picks (config.ProbeModels). It returns nil when no
// pool is flagged or every value is 0.
func newPriorityProber(cfg *config.Config, disc discovery.Discovery, sender priorityprobe.Sender,
	served func(replica string) []string, m *telemetry.Metrics, logger *slog.Logger) *priorityprobe.Prober {
	flagged := slices.Sorted(maps.Keys(cfg.PriorityPools()))
	// Any non-zero value tells whether priority scheduling is on; the largest
	// in magnitude (positive on a tie) keeps the choice deterministic.
	probe := 0
	for _, v := range cfg.Priority.UpstreamValues {
		if a, b := abs(v), abs(probe); a > b || (a == b && v > probe) {
			probe = v
		}
	}
	if len(flagged) == 0 || probe == 0 {
		return nil
	}
	return priorityprobe.New(priorityprobe.Config{
		Pools:         flagged,
		Priority:      probe,
		Model:         probeModel(cfg.ProbeModels(), served),
		PriorityProbe: cfg.Priority.Probe,
		Logger:        logger,
	}, disc, sender, m)
}

func abs(v int) int {
	if v < 0 {
		return -v
	}
	return v
}
