package main

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/compiler"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates/pgstore"
)

// Migration retry backoff while Postgres is unreachable at startup.
const (
	migrateBackoffMin = time.Second
	migrateBackoffMax = 30 * time.Second
)

// templateRegistry is the M3 registry as the gateway runs it: the store,
// the refresher keeping the request path's snapshot, and the compiler
// that reads it.
type templateRegistry struct {
	store    *pgstore.Store      // nil when the registry is off
	registry *templates.Registry // nil when the registry is off
	compiler *compiler.Compiler
	stop     func()
}

// startTemplates starts the template registry when templates.postgres.dsn
// is set. Startup never waits on Postgres: the schema migration retries in
// the background, and until the first snapshot loads every request naming
// a template fails open (registry_unavailable). Without a DSN the compiler
// still runs, so such requests are counted the same way.
func startTemplates(ctx context.Context, cfg *config.Config, m *telemetry.Metrics, logger *slog.Logger) (*templateRegistry, error) {
	if !cfg.Templates.Enabled() {
		return &templateRegistry{compiler: compiler.FromConfig(cfg, nil), stop: func() {}}, nil
	}
	pg := cfg.Templates.Postgres
	store, err := pgstore.Open(ctx, pg.DSN.Reveal(), pgstore.Options{
		ConnectTimeout: pg.ConnectTimeout,
		QueryTimeout:   pg.QueryTimeout,
		MaxConns:       int32(pg.MaxConns),
	})
	if err != nil {
		return nil, err
	}
	reg := templates.NewRegistry(store, templates.RegistryOptions{
		Interval:     cfg.Templates.RefreshInterval,
		QueryTimeout: pg.QueryTimeout,
		MaxVersions:  cfg.Templates.MaxVersions,
		OnFailOpen:   func(reason string) { m.FailOpen(telemetry.StageTemplateRegistry, reason) },
		OnInvalid: func(id string, version int, err error) {
			logger.Warn("template version skipped from the snapshot", "template", id, "version", version, "err", err)
		},
		Logger: logger,
	})
	comp := compiler.FromConfig(cfg, reg)
	m.SetTemplateSnapshotSource(func() (int, time.Duration, bool) {
		return reg.Snapshot().Len(), reg.Age(), true
	})

	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); migrate(runCtx, store, reg, logger) }()
	go func() { defer wg.Done(); reg.Run(runCtx) }()
	go func() {
		defer wg.Done()
		followSnapshot(runCtx, reg, cfg.Templates.RefreshInterval, func(s *templates.Snapshot) {
			comp.Retain(s)
			m.RetainTemplates(func(id, version string) bool {
				v, err := strconv.Atoi(version)
				if err != nil {
					return false
				}
				_, ok := s.Lookup(id, v)
				return ok
			})
		})
	}()
	return &templateRegistry{store: store, registry: reg, compiler: comp, stop: func() {
		cancel()
		wg.Wait()
		store.Close()
	}}, nil
}

// migrate applies the store's schema, retrying with backoff until it
// succeeds or ctx ends, then asks the registry for an immediate refresh.
func migrate(ctx context.Context, store *pgstore.Store, reg *templates.Registry, logger *slog.Logger) {
	backoff := migrateBackoffMin
	for {
		err := store.Migrate(ctx)
		if err == nil {
			reg.Kick()
			return
		}
		if ctx.Err() != nil {
			return
		}
		logger.Warn("template store migration failed; retrying", "err", err, "retry_in", backoff.String())
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, migrateBackoffMax)
	}
}

// followSnapshot calls changed with every new registry snapshot, checking
// every interval, so per-version state (length statistics, metric series)
// of versions that left it is dropped.
func followSnapshot(ctx context.Context, reg *templates.Registry, interval time.Duration, changed func(*templates.Snapshot)) {
	t := time.NewTicker(interval)
	defer t.Stop()
	var last *templates.Snapshot
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if s := reg.Snapshot(); s != nil && s != last {
			last = s
			changed(s)
		}
	}
}
