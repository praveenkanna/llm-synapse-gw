// Command gateway is the LLM gateway: an OpenAI-wire-compatible HTTP service in
// front of a pool of vLLM replicas.
//
// It loads configuration (internal/config), builds the request path
// (internal/auth in front of internal/ingress over internal/upstream and
// internal/discovery), watches the config file for replica changes every
// discovery.reload_interval, and serves until SIGINT/SIGTERM, then drains
// gracefully (internal/health). A second SIGINT/SIGTERM during drain
// force-quits. internal/telemetry instruments the request path and serves
// Prometheus metrics on server.metrics_listen_addr; the metrics listener
// outlives the drain so it stays observable.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/auth"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/coalesce"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/dispatch"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/health"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/ingress"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/respcache"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router"
	routerhealth "github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/health"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/loadview"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

func main() {
	logOut := telemetry.NewBufferedWriter(os.Stdout, 64<<10, 25*time.Millisecond)
	logger := slog.New(slog.NewJSONHandler(logOut, nil))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// Restore default signal handling as soon as the first signal arrives, so
	// a second SIGINT/SIGTERM during drain terminates the process immediately.
	context.AfterFunc(ctx, stop)

	if err := run(ctx, os.Getenv, logger); err != nil {
		logger.Error("gateway stopped with error", "err", err)
		_ = logOut.Close()
		stop()
		os.Exit(1)
	}
	_ = logOut.Close()
}

// run loads configuration, fails fast if it is invalid, then serves until ctx
// is cancelled.
func run(ctx context.Context, getenv func(string) string, logger *slog.Logger) error {
	cfg, err := config.Load(getenv)
	if err != nil {
		return err
	}
	cfg.Runtime.Apply()
	if cfg.Runtime != (config.Runtime{}) {
		logger.Info("go runtime tuned", "gc_percent", cfg.Runtime.GCPercent, "memory_limit", cfg.Runtime.MemoryLimit.String())
	}
	authn, err := auth.New(cfg.Auth)
	if err != nil {
		return err
	}
	disc, err := discovery.NewStatic(cfg.Pools)
	if err != nil {
		return err
	}
	disc.WarnOversized(logger)
	logger = slog.New(telemetry.NewLogHandler(logger.Handler())) // trace_id on request-scoped lines
	tel := telemetry.New(telemetry.Options{Logger: logger})
	client := upstream.New(cfg.Upstream, upstream.WithFailOpen(tel.Metrics().FailOpen))
	defer client.CloseIdleConnections()
	client.Prewarm(cfg.Pools) // background lookups; startup never waits on DNS

	// Health tracker for active/passive ejection and ring rebuilding, and the
	// load view for in-flight tracking and vLLM metric scraping. Both start
	// background goroutines and return at once: startup and liveness never
	// wait on a replica. Both follow the hot reload below through disc (a
	// discovery.Inventory): the tracker rebuilds affected rings within one
	// health poll interval, and the load view prunes removed replicas' state
	// and telemetry series on its next scrape or probe. A replica ejected as
	// a stuck engine is re-admitted only after a generation probe, sent
	// through the upstream client (with the upstream key) for the model
	// config's probe-model rule picks: the pool's configured one, else the
	// first the load view saw on the replica's /v1/models, else the pool's
	// first mapped model.
	loadView := loadview.New(loadview.Config{LoadView: cfg.Router.LoadView, APIKey: cfg.Upstream.APIKey}, disc, tel.Metrics(), client.ProbeClient(0))
	healthTracker := routerhealth.New(trackerConfig(cfg, client, servedModels(loadView), logger), disc, tel.Metrics(), client.ProbeClient(cfg.Router.Health.PollTimeout))
	healthTracker.Start(ctx)
	defer healthTracker.Stop()

	loadView.Start(ctx)
	defer loadView.Stop()

	// Routing: round-robin or affinity, and the passive health that goes with it.
	rtr, routeHealth := newRouting(cfg, disc, healthTracker, loadView, tel.Metrics())
	disp := dispatch.New(rtr, client, cfg.Upstream, dispatch.Options{Health: routeHealth, Logger: logger})

	// Admission (M2): per-pool slots and priority queues before routing,
	// shared across gateways through Redis when admission.redis is on (the
	// request path never waits on Redis), and the probe that turns on vLLM
	// priority for flagged pools once their replicas accept it.
	arb := newAdmission(cfg, disc, tel.Metrics())
	if arb != nil {
		go followReplicas(ctx, disc, arb)
	}
	stopSync := startAdmissionSync(ctx, cfg, arb, tel.Metrics(), logger)
	defer stopSync()
	prober := newPriorityProber(cfg, disc, client, servedModels(loadView), tel.Metrics(), logger)
	if prober != nil {
		prober.Start(ctx)
		defer prober.Stop()
	}

	// Template registry (M3): an in-memory snapshot of Postgres refreshed
	// out of band; the request path only reads the snapshot.
	tmpl, err := startTemplates(ctx, cfg, tel.Metrics(), logger)
	if err != nil {
		return err
	}
	defer tmpl.stop()
	stopAdmin, err := startAdmin(cfg, tmpl, disc, client, logger)
	if err != nil {
		return err
	}
	defer stopAdmin() // runs before tmpl.stop closes the store

	opts := ingressOptions(cfg, logger)
	opts.Admission = arb
	opts.Compiler = tmpl.compiler
	if prober != nil {
		opts.PriorityVerified = prober.Verified
	}
	opts.Metrics = tel.Metrics()
	opts.HealthyReplicas = healthTracker.Healthy
	opts.Coalesce = newCoalesceGroup(cfg, tel.Metrics())
	if cfg.Cache.Enabled {
		rc := newResponseCache(cfg, tel.Metrics())
		defer rc.Close() // drains the write queue, a few stores at most
		opts.Cache = rc
	}
	gw := ingress.New(cfg, disc, client, disp, opts)
	if !authn.Enabled() {
		logger.Warn("caller auth disabled: every request is accepted as the anonymous caller")
	}

	ln, err := net.Listen("tcp", cfg.Server.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.Server.ListenAddr, err)
	}
	mln, err := net.Listen("tcp", cfg.Server.MetricsListenAddr)
	if err != nil {
		_ = ln.Close()
		return fmt.Errorf("listen %s: %w", cfg.Server.MetricsListenAddr, err)
	}

	// Metrics outlive the request server so the drain stays observable. A
	// metrics failure is logged, never fatal: the request path fails open.
	metricsCtx, stopMetrics := context.WithCancel(context.Background())
	metricsDone := make(chan struct{})
	go func() {
		defer close(metricsDone)
		if err := telemetry.ServeMetrics(metricsCtx, mln, tel.Metrics(), logger); err != nil {
			logger.Error("metrics server stopped", "err", err)
		}
	}()
	defer func() {
		stopMetrics()
		<-metricsDone
	}()

	// Hot-reload the replica list from the mounted config file.
	watchCtx, stopWatch := context.WithCancel(ctx)
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		if getenv(config.EnvConfigPath) == "" {
			return // nothing to watch
		}
		load := func() (*config.Config, error) {
			c, err := config.Load(getenv)
			if err == nil {
				client.Prewarm(c.Pools) // keeps replica addresses fresh off the request path
			}
			return c, err
		}
		discovery.Watch(watchCtx, disc, load, cfg.Discovery.ReloadInterval, logger)
	}()
	defer func() {
		stopWatch()
		<-watchDone
	}()

	checker := health.New()
	srv := newServer(cfg, tel.Middleware(newMux(gw, checker, authn)), logger)
	return health.Serve(ctx, srv, ln, checker, cfg.Shutdown.DrainTimeout, cfg.Shutdown.PreDrainDelay, logger)
}

// newCoalesceGroup builds the flight table that shares generations between
// identical requests of templates that opted in (ADR 0019), and exports its
// size and events.
func newCoalesceGroup(cfg *config.Config, m *telemetry.Metrics) *coalesce.Group {
	g := coalesce.NewGroup(coalesce.Config{Coalescing: cfg.Coalescing, Observe: m.CoalesceEvent})
	m.SetCoalesceSource(func() (int, int64) {
		s := g.Stats()
		return s.Flights, s.RetainedBytes
	})
	return g
}

// newResponseCache builds the Redis response cache (ADR 0019) over its own
// connections; its store results are counted in gateway_cache_stores_total.
func newResponseCache(cfg *config.Config, m *telemetry.Metrics) *respcache.Cache {
	return respcache.New(respcache.Config{Cache: cfg.Cache, OnStore: m.CacheStore})
}

// newRouting builds the router for cfg.Router.Policy and the passive health
// the dispatcher reports to, which go together: the affinity router steers
// by tracker's view, and the dispatcher feeds tracker the hard connect
// failures and first-chunk waits that view is built from. Round-robin picks
// ignore health, so under round_robin nothing is reported or watched (nil
// Health).
func newRouting(cfg *config.Config, disc discovery.Discovery, tracker *routerhealth.Tracker, lv *loadview.View, m *telemetry.Metrics) (router.Router, dispatch.Health) {
	rr := router.NewRoundRobin(disc, cfg.PoolNames(), m)
	if cfg.Router.Policy == "round_robin" {
		return rr, nil
	}
	ar := router.NewAffinityRouter(disc, rr, tracker, lv, m)
	ar.SetKeyScanBudget(cfg.Router.KeyScanBudget)
	return ar, dispatch.TrackerHealth(tracker)
}

// ingressOptions maps configuration onto the request path's options.
func ingressOptions(cfg *config.Config, logger *slog.Logger) ingress.Options {
	return ingress.Options{
		MaxRequestBodyBytes: int64(cfg.Server.MaxRequestBodyBytes),
		Logger:              logger,
	}
}

// trackerConfig is the health tracker's configuration: the router section,
// the stuck-engine probe (sent by sender, its model from what served lists),
// logger for rejected probes (status, path, pool, replica only), and the
// pools whose replicas serve out of order, since vLLM priority reorders
// first chunks there.
func trackerConfig(cfg *config.Config, sender routerhealth.ProbeSender, served func(replica string) []string, logger *slog.Logger) routerhealth.Config {
	hc := withStuckProbe(routerhealth.ConfigFrom(cfg.Router), cfg, sender, served)
	hc.Logger = logger
	hc.OutOfOrderPools = cfg.PriorityPools()
	return hc
}

// withStuckProbe turns on the stuck-engine generation probe when c's
// router.health.stuck_probe_enabled is set: sender sends it, with the model
// cfg's probe-model rule picks (config.ProbeModels) from what served lists
// for the replica.
func withStuckProbe(c routerhealth.Config, cfg *config.Config, sender routerhealth.ProbeSender, served func(replica string) []string) routerhealth.Config {
	if !c.StuckProbeEnabled {
		return c
	}
	c.StuckProbeSender = sender
	c.StuckProbeModel = probeModel(cfg.ProbeModels(), served)
	return c
}

// probeModel is the model a probe of rep sends, by models' rule over what
// served lists for it (served may be nil: nothing listed).
func probeModel(models config.ProbeModels, served func(replica string) []string) func(rep discovery.Replica) string {
	return func(rep discovery.Replica) string {
		var listed []string
		if served != nil {
			listed = served(rep.Name)
		}
		return models.For(rep.Pool, listed)
	}
}

// servedModels returns the models each replica listed on /v1/models, as
// last probed by lv (none before its first probe).
func servedModels(lv *loadview.View) func(replica string) []string {
	return func(replica string) []string {
		st, _ := lv.State(replica)
		return st.Models
	}
}

// apiPrefix covers every OpenAI-wire route; all of it sits behind caller auth.
const apiPrefix = "/v1/"

// maxHeaderBytes is the most a client's request headers may take, net/http's
// own default set by name. It stays this high on purpose: the values the
// request path reads have their own bounds (M1-27), and past those a longer
// header costs nothing and is forwarded, so a lower ceiling would refuse
// requests without saving any work on them.
const maxHeaderBytes = http.DefaultMaxHeaderBytes

// newMux builds the gateway's HTTP routes: health endpoints (never behind
// auth), then the request path behind authn. gw may be nil (health only), in
// which case authn is unused.
func newMux(gw *ingress.Handler, checker *health.Checker, authn *auth.Authenticator) *http.ServeMux {
	mux := http.NewServeMux()
	checker.Register(mux)
	if gw != nil {
		api := http.NewServeMux()
		gw.Register(api)
		mux.Handle(apiPrefix, authn.Middleware(api))
	}
	return mux
}

// newServer builds the client-facing HTTP server. WriteTimeout is deliberately
// unset: SSE streams may legitimately run for minutes. IdleTimeout closes idle
// keep-alive connections.
func newServer(cfg *config.Config, handler http.Handler, logger *slog.Logger) *http.Server {
	return &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
		IdleTimeout:       cfg.Server.IdleTimeout,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}
}
