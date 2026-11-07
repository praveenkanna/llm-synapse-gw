package config

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

// envOverride maps one GATEWAY_* environment variable onto a config field.
type envOverride struct {
	name  string
	apply func(c *Config, v string) error
}

// envOverrides lists every scalar environment override. Structured settings
// (pools, models, callers) come only from the YAML file.
var envOverrides = []envOverride{
	{"GATEWAY_LISTEN_ADDR", setString(func(c *Config) *string { return &c.Server.ListenAddr })},
	{"GATEWAY_METRICS_LISTEN_ADDR", setString(func(c *Config) *string { return &c.Server.MetricsListenAddr })},
	{"GATEWAY_READ_HEADER_TIMEOUT", setDuration(func(c *Config) *time.Duration { return &c.Server.ReadHeaderTimeout })},
	{"GATEWAY_IDLE_TIMEOUT", setDuration(func(c *Config) *time.Duration { return &c.Server.IdleTimeout })},
	{"GATEWAY_MAX_REQUEST_BODY_BYTES", setByteSize(func(c *Config) *ByteSize { return &c.Server.MaxRequestBodyBytes })},
	{"GATEWAY_SHUTDOWN_DRAIN_TIMEOUT", setDuration(func(c *Config) *time.Duration { return &c.Shutdown.DrainTimeout })},
	{"GATEWAY_SHUTDOWN_PRE_DRAIN_DELAY", setDuration(func(c *Config) *time.Duration { return &c.Shutdown.PreDrainDelay })},
	{"GATEWAY_DISCOVERY_RELOAD_INTERVAL", setDuration(func(c *Config) *time.Duration { return &c.Discovery.ReloadInterval })},
	{"GATEWAY_UPSTREAM_MAX_IDLE_CONNS_PER_HOST", setInt(func(c *Config) *int { return &c.Upstream.MaxIdleConnsPerHost })},
	{"GATEWAY_UPSTREAM_IDLE_CONN_TIMEOUT", setDuration(func(c *Config) *time.Duration { return &c.Upstream.IdleConnTimeout })},
	{"GATEWAY_UPSTREAM_ENABLE_HTTP2", setBool(func(c *Config) *bool { return &c.Upstream.EnableHTTP2 })},
	{"GATEWAY_UPSTREAM_CONNECT_RETRY_BACKOFF", setDuration(func(c *Config) *time.Duration { return &c.Upstream.ConnectRetryBackoff })},
	{"GATEWAY_UPSTREAM_CONNECT_TIMEOUT", setDuration(func(c *Config) *time.Duration { return &c.Upstream.Timeouts.Connect })},
	{"GATEWAY_UPSTREAM_RESPONSE_HEADER_TIMEOUT", setDuration(func(c *Config) *time.Duration { return &c.Upstream.Timeouts.ResponseHeader })},
	{"GATEWAY_UPSTREAM_STREAM_RESPONSE_HEADER_TIMEOUT", setDuration(func(c *Config) *time.Duration { return &c.Upstream.Timeouts.StreamResponseHeader })},
	{"GATEWAY_UPSTREAM_FIRST_CHUNK_TIMEOUT", setDuration(func(c *Config) *time.Duration { return &c.Upstream.Timeouts.FirstChunk })},
	{"GATEWAY_UPSTREAM_IDLE_BETWEEN_CHUNKS_TIMEOUT", setDuration(func(c *Config) *time.Duration { return &c.Upstream.Timeouts.IdleBetweenChunks })},
	{"GATEWAY_UPSTREAM_TOTAL_TIMEOUT", setDuration(func(c *Config) *time.Duration { return &c.Upstream.Timeouts.Total })},
	{"GATEWAY_UPSTREAM_DNS_TTL", setDuration(func(c *Config) *time.Duration { return &c.Upstream.DNS.TTL })},
	{"GATEWAY_UPSTREAM_DNS_MAX_STALE", setDuration(func(c *Config) *time.Duration { return &c.Upstream.DNS.MaxStale })},
	{"GATEWAY_UPSTREAM_DNS_LOOKUP_TIMEOUT", setDuration(func(c *Config) *time.Duration { return &c.Upstream.DNS.LookupTimeout })},
	{"GATEWAY_UPSTREAM_DNS_MAX_HOSTS", setInt(func(c *Config) *int { return &c.Upstream.DNS.MaxHosts })},
	{"GATEWAY_UPSTREAM_API_KEY", func(c *Config, v string) error { c.Upstream.APIKey = Secret(v); return nil }},
	{"GATEWAY_DEFAULT_POOL", setString(func(c *Config) *string { return &c.DefaultPool })},
	{"GATEWAY_AUTH_ENABLED", setBool(func(c *Config) *bool { return &c.Auth.Enabled })},
	{"GATEWAY_RUNTIME_GC_PERCENT", setInt(func(c *Config) *int { return &c.Runtime.GCPercent })},
	{"GATEWAY_RUNTIME_MEMORY_LIMIT", setByteSize(func(c *Config) *ByteSize { return &c.Runtime.MemoryLimit })},
	{"GATEWAY_ROUTER_POLICY", setString(func(c *Config) *string { return &c.Router.Policy })},
	{"GATEWAY_ROUTER_EPSILON", setFloat(func(c *Config) *float64 { return &c.Router.Epsilon })},
	{"GATEWAY_ROUTER_OVERFLOW_SLACK", setInt(func(c *Config) *int { return &c.Router.OverflowSlack })},
	{"GATEWAY_ROUTER_KEY_SCAN_BUDGET", setInt(func(c *Config) *int { return &c.Router.KeyScanBudget })},
	{"GATEWAY_ROUTER_HEALTH_POLL_INTERVAL", setDuration(func(c *Config) *time.Duration { return &c.Router.Health.PollInterval })},
	{"GATEWAY_ROUTER_HEALTH_POLL_TIMEOUT", setDuration(func(c *Config) *time.Duration { return &c.Router.Health.PollTimeout })},
	{"GATEWAY_ROUTER_HEALTH_FAILURE_THRESHOLD", setInt(func(c *Config) *int { return &c.Router.Health.FailureThreshold })},
	{"GATEWAY_ROUTER_HEALTH_SUCCESS_THRESHOLD", setInt(func(c *Config) *int { return &c.Router.Health.SuccessThreshold })},
	{"GATEWAY_ROUTER_HEALTH_PASSIVE_FAILURE_THRESHOLD", setInt(func(c *Config) *int { return &c.Router.Health.PassiveFailureThreshold })},
	{"GATEWAY_ROUTER_HEALTH_STUCK_ENGINE_TIMEOUT", setDuration(func(c *Config) *time.Duration { return &c.Router.Health.StuckEngineTimeout })},
	{"GATEWAY_ROUTER_HEALTH_STUCK_ENGINE_STREAMS", setInt(func(c *Config) *int { return &c.Router.Health.StuckEngineStreams })},
	{"GATEWAY_ROUTER_HEALTH_STUCK_ENGINE_RATIO", setFloat(func(c *Config) *float64 { return &c.Router.Health.StuckEngineRatio })},
	{"GATEWAY_ROUTER_HEALTH_STUCK_ENGINE_WINDOW", setDuration(func(c *Config) *time.Duration { return &c.Router.Health.StuckEngineWindow })},
	{"GATEWAY_ROUTER_HEALTH_STUCK_ENGINE_MAX_WAITS", setInt(func(c *Config) *int { return &c.Router.Health.StuckEngineMaxWaits })},
	{"GATEWAY_ROUTER_HEALTH_MIN_EJECTION", setDuration(func(c *Config) *time.Duration { return &c.Router.Health.MinEjection })},
	{"GATEWAY_ROUTER_HEALTH_MAX_EJECTION", setDuration(func(c *Config) *time.Duration { return &c.Router.Health.MaxEjection })},
	{"GATEWAY_ROUTER_HEALTH_EJECTION_BACKOFF_MULTIPLIER", setFloat(func(c *Config) *float64 { return &c.Router.Health.EjectionBackoffMultiplier })},
	{"GATEWAY_ROUTER_HEALTH_STUCK_PROBE_ENABLED", setBool(func(c *Config) *bool { return &c.Router.Health.StuckProbeEnabled })},
	{"GATEWAY_ROUTER_HEALTH_STUCK_PROBE_TIMEOUT", setDuration(func(c *Config) *time.Duration { return &c.Router.Health.StuckProbeTimeout })},
	{"GATEWAY_ROUTER_HEALTH_STUCK_PROBE_PATH", setString(func(c *Config) *string { return &c.Router.Health.StuckProbePath })},
	{"GATEWAY_ROUTER_LOAD_VIEW_SCRAPE_INTERVAL", setDuration(func(c *Config) *time.Duration { return &c.Router.LoadView.ScrapeInterval })},
	{"GATEWAY_ROUTER_LOAD_VIEW_SCRAPE_TIMEOUT", setDuration(func(c *Config) *time.Duration { return &c.Router.LoadView.ScrapeTimeout })},
	{"GATEWAY_ROUTER_LOAD_VIEW_PROBE_INTERVAL", setDuration(func(c *Config) *time.Duration { return &c.Router.LoadView.ProbeInterval })},
	{"GATEWAY_ROUTER_LOAD_VIEW_STALE_THRESHOLD", setDuration(func(c *Config) *time.Duration { return &c.Router.LoadView.StaleThreshold })},
	{"GATEWAY_PRIORITY_DEFAULT_CLASS", setString(func(c *Config) *string { return &c.Priority.DefaultClass })},
	{"GATEWAY_PRIORITY_ANONYMOUS_MAX_CLASS", setString(func(c *Config) *string { return &c.Priority.AnonymousMaxClass })},
	{"GATEWAY_PRIORITY_DEFAULT_MAX_CLASS", setString(func(c *Config) *string { return &c.Priority.DefaultMaxClass })},
	{"GATEWAY_PRIORITY_PROBE_INTERVAL", setDuration(func(c *Config) *time.Duration { return &c.Priority.Probe.Interval })},
	{"GATEWAY_PRIORITY_PROBE_TIMEOUT", setDuration(func(c *Config) *time.Duration { return &c.Priority.Probe.Timeout })},
	{"GATEWAY_ADMISSION_ENABLED", setBool(func(c *Config) *bool { return &c.Admission.Enabled })},
	{"GATEWAY_ADMISSION_MAX_WAIT", setDuration(func(c *Config) *time.Duration { return &c.Admission.MaxWait })},
	{"GATEWAY_ADMISSION_MAX_QUEUE", setInt(func(c *Config) *int { return &c.Admission.MaxQueue })},
	{"GATEWAY_ADMISSION_RETRY_AFTER_MAX", setDuration(func(c *Config) *time.Duration { return &c.Admission.RetryAfterMax })},
	{"GATEWAY_ADMISSION_EARLY_REJECT", setBool(func(c *Config) *bool { return &c.Admission.EarlyReject })},
	{"GATEWAY_ADMISSION_SHED_ENABLED", setBool(func(c *Config) *bool { return &c.Admission.Shed.Enabled })},
	{"GATEWAY_ADMISSION_SHED_TTFT_P95_THRESHOLD", setDuration(func(c *Config) *time.Duration { return &c.Admission.Shed.TTFTP95Threshold })},
	{"GATEWAY_ADMISSION_SHED_WINDOW", setDuration(func(c *Config) *time.Duration { return &c.Admission.Shed.Window })},
	{"GATEWAY_ADMISSION_SHED_MIN_SAMPLES", setInt(func(c *Config) *int { return &c.Admission.Shed.MinSamples })},
	{"GATEWAY_ADMISSION_SHED_RECOVER_RATIO", setFloat(func(c *Config) *float64 { return &c.Admission.Shed.RecoverRatio })},
	{"GATEWAY_ADMISSION_REDIS_ENABLED", setBool(func(c *Config) *bool { return &c.Admission.Redis.Enabled })},
	{"GATEWAY_ADMISSION_REDIS_ADDR", setString(func(c *Config) *string { return &c.Admission.Redis.Addr })},
	{"GATEWAY_ADMISSION_REDIS_USERNAME", setString(func(c *Config) *string { return &c.Admission.Redis.Username })},
	{"GATEWAY_ADMISSION_REDIS_PASSWORD", func(c *Config, v string) error { c.Admission.Redis.Password = Secret(v); return nil }},
	{"GATEWAY_ADMISSION_REDIS_DB", setInt(func(c *Config) *int { return &c.Admission.Redis.DB })},
	{"GATEWAY_ADMISSION_REDIS_SYNC_INTERVAL", setDuration(func(c *Config) *time.Duration { return &c.Admission.Redis.SyncInterval })},
	{"GATEWAY_ADMISSION_REDIS_TIMEOUT", setDuration(func(c *Config) *time.Duration { return &c.Admission.Redis.Timeout })},
	{"GATEWAY_ADMISSION_REDIS_LEASE_TTL", setDuration(func(c *Config) *time.Duration { return &c.Admission.Redis.LeaseTTL })},
	{"GATEWAY_ADMISSION_REDIS_KEY_PREFIX", setString(func(c *Config) *string { return &c.Admission.Redis.KeyPrefix })},
	{"GATEWAY_COALESCING_MAX_FLIGHTS", setInt(func(c *Config) *int { return &c.Coalescing.MaxFlights })},
	{"GATEWAY_COALESCING_MAX_SUBSCRIBERS", setInt(func(c *Config) *int { return &c.Coalescing.MaxSubscribers })},
	{"GATEWAY_COALESCING_MAX_REPLAY_BYTES", setInt(func(c *Config) *int { return &c.Coalescing.MaxReplayBytes })},
	{"GATEWAY_COALESCING_MAX_KEY_BODY_BYTES", setInt(func(c *Config) *int { return &c.Coalescing.MaxKeyBodyBytes })},
	{"GATEWAY_CACHE_ENABLED", setBool(func(c *Config) *bool { return &c.Cache.Enabled })},
	{"GATEWAY_CACHE_REDIS_ADDR", setString(func(c *Config) *string { return &c.Cache.Redis.Addr })},
	{"GATEWAY_CACHE_REDIS_USERNAME", setString(func(c *Config) *string { return &c.Cache.Redis.Username })},
	{"GATEWAY_CACHE_REDIS_PASSWORD", func(c *Config, v string) error { c.Cache.Redis.Password = Secret(v); return nil }},
	{"GATEWAY_CACHE_REDIS_DB", setInt(func(c *Config) *int { return &c.Cache.Redis.DB })},
	{"GATEWAY_CACHE_CONNECTIONS", setInt(func(c *Config) *int { return &c.Cache.Connections })},
	{"GATEWAY_CACHE_TIMEOUT", setDuration(func(c *Config) *time.Duration { return &c.Cache.Timeout })},
	{"GATEWAY_CACHE_MAX_TTL", setDuration(func(c *Config) *time.Duration { return &c.Cache.MaxTTL })},
	{"GATEWAY_CACHE_MAX_ENTRY_BYTES", setInt(func(c *Config) *int { return &c.Cache.MaxEntryBytes })},
	{"GATEWAY_CACHE_WRITE_QUEUE", setInt(func(c *Config) *int { return &c.Cache.WriteQueue })},
	{"GATEWAY_CACHE_WRITE_WORKERS", setInt(func(c *Config) *int { return &c.Cache.WriteWorkers })},
	{"GATEWAY_CACHE_KEY_PREFIX", setString(func(c *Config) *string { return &c.Cache.KeyPrefix })},
	{"GATEWAY_CONTEXT_ENABLED", setBool(func(c *Config) *bool { return &c.Context.Enabled })},
	{"GATEWAY_CONTEXT_MAX_PROBES", setInt(func(c *Config) *int { return &c.Context.MaxProbes })},
	{"GATEWAY_CONTEXT_TOKENIZE_TIMEOUT", setDuration(func(c *Config) *time.Duration { return &c.Context.TokenizeTimeout })},
	{"GATEWAY_CONTEXT_ASSEMBLE_TIMEOUT", setDuration(func(c *Config) *time.Duration { return &c.Context.AssembleTimeout })},
	{"GATEWAY_CONTEXT_MAX_BODY_BYTES", setInt(func(c *Config) *int { return &c.Context.MaxBodyBytes })},
	{"GATEWAY_TEMPLATES_POSTGRES_DSN", func(c *Config, v string) error { c.Templates.Postgres.DSN = Secret(v); return nil }},
	{"GATEWAY_TEMPLATES_POSTGRES_CONNECT_TIMEOUT", setDuration(func(c *Config) *time.Duration { return &c.Templates.Postgres.ConnectTimeout })},
	{"GATEWAY_TEMPLATES_POSTGRES_QUERY_TIMEOUT", setDuration(func(c *Config) *time.Duration { return &c.Templates.Postgres.QueryTimeout })},
	{"GATEWAY_TEMPLATES_POSTGRES_MAX_CONNS", setInt(func(c *Config) *int { return &c.Templates.Postgres.MaxConns })},
	{"GATEWAY_TEMPLATES_REFRESH_INTERVAL", setDuration(func(c *Config) *time.Duration { return &c.Templates.RefreshInterval })},
	{"GATEWAY_TEMPLATES_MAX_VERSIONS", setInt(func(c *Config) *int { return &c.Templates.MaxVersions })},
	{"GATEWAY_TEMPLATES_MAX_TOKENS_ENABLED", setBool(func(c *Config) *bool { return &c.Templates.MaxTokens.Enabled })},
	{"GATEWAY_TEMPLATES_MAX_TOKENS_MARGIN", setFloat(func(c *Config) *float64 { return &c.Templates.MaxTokens.Margin })},
	{"GATEWAY_TEMPLATES_MAX_TOKENS_MIN_SAMPLES", setInt(func(c *Config) *int { return &c.Templates.MaxTokens.MinSamples })},
	{"GATEWAY_ADMIN_LISTEN_ADDR", setString(func(c *Config) *string { return &c.Admin.ListenAddr })},
	{"GATEWAY_ADMIN_TOKENIZE_TIMEOUT", setDuration(func(c *Config) *time.Duration { return &c.Admin.TokenizeTimeout })},
}

// applyEnv overlays every set (non-empty) override onto cfg.
func applyEnv(cfg *Config, getenv func(string) string) error {
	var errs []error
	for _, o := range envOverrides {
		v := getenv(o.name)
		if v == "" {
			continue
		}
		if err := o.apply(cfg, v); err != nil {
			errs = append(errs, fmt.Errorf("env %s: %w", o.name, err))
		}
	}
	return errors.Join(errs...)
}

func setString(field func(*Config) *string) func(*Config, string) error {
	return func(c *Config, v string) error {
		*field(c) = v
		return nil
	}
}

func setDuration(field func(*Config) *time.Duration) func(*Config, string) error {
	return func(c *Config, v string) error {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid duration %q (want e.g. 10s, 2m)", v)
		}
		*field(c) = d
		return nil
	}
}

func setInt(field func(*Config) *int) func(*Config, string) error {
	return func(c *Config, v string) error {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid integer %q", v)
		}
		*field(c) = n
		return nil
	}
}

func setFloat(field func(*Config) *float64) func(*Config, string) error {
	return func(c *Config, v string) error {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return fmt.Errorf("invalid float %q", v)
		}
		*field(c) = f
		return nil
	}
}

func setBool(field func(*Config) *bool) func(*Config, string) error {
	return func(c *Config, v string) error {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("invalid boolean %q (want true or false)", v)
		}
		*field(c) = b
		return nil
	}
}

func setByteSize(field func(*Config) *ByteSize) func(*Config, string) error {
	return func(c *Config, v string) error {
		b, err := ParseByteSize(v)
		if err != nil {
			return err
		}
		*field(c) = b
		return nil
	}
}
