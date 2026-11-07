package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

const examplePath = "../../../deploy/compose/config/gateway.example.yaml"

// minimalYAML is the smallest valid config: one pool, auth off.
const minimalYAML = `
pools:
  - name: large
    replicas:
      - url: http://vllm-large-0:8000
auth:
  enabled: false
`

func envMap(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func mustParse(t *testing.T, data string, env map[string]string) *Config {
	t.Helper()
	cfg, err := Parse([]byte(data), envMap(env))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return cfg
}

func TestDefaultsApplied(t *testing.T) {
	cfg := mustParse(t, minimalYAML, nil)

	want := Defaults()
	if cfg.Server != want.Server {
		t.Errorf("Server = %+v, want %+v", cfg.Server, want.Server)
	}
	if cfg.Shutdown != want.Shutdown {
		t.Errorf("Shutdown = %+v, want %+v", cfg.Shutdown, want.Shutdown)
	}
	if cfg.Upstream != want.Upstream {
		t.Errorf("Upstream = %+v, want %+v", cfg.Upstream, want.Upstream)
	}
	if cfg.Server.ListenAddr != ":8080" || cfg.Server.ReadHeaderTimeout != 10*time.Second || cfg.Shutdown.DrainTimeout != 15*time.Second {
		t.Errorf("M0-01 defaults changed: listen=%q readHeader=%s drain=%s",
			cfg.Server.ListenAddr, cfg.Server.ReadHeaderTimeout, cfg.Shutdown.DrainTimeout)
	}
	if cfg.DefaultPool != "large" {
		t.Errorf("DefaultPool = %q, want the only pool %q", cfg.DefaultPool, "large")
	}
	if got := cfg.Pools[0].Replicas[0].Name; got != "vllm-large-0:8000" {
		t.Errorf("replica name = %q, want derived %q", got, "vllm-large-0:8000")
	}
	if cfg.Runtime != (Runtime{}) {
		t.Errorf("Runtime = %+v, want unset (Go's defaults)", cfg.Runtime)
	}
	if cfg.Server.MaxRequestBodyBytes != 32<<20 {
		t.Errorf("server.max_request_body_bytes = %s, want 32MiB", cfg.Server.MaxRequestBodyBytes)
	}
	// M0-76: the header wait is bounded by the total timeout only, the idle
	// pool is sized for thousands of streams, and DNS has its own budget.
	if cfg.Upstream.Timeouts.ResponseHeader != 0 || cfg.Upstream.MaxIdleConnsPerHost != 8192 {
		t.Errorf("upstream response_header = %s, max_idle_conns_per_host = %d; want 0 and 8192",
			cfg.Upstream.Timeouts.ResponseHeader, cfg.Upstream.MaxIdleConnsPerHost)
	}
	if got := cfg.Upstream.ConnectRetryBackoff; got != 100*time.Millisecond {
		t.Errorf("upstream.connect_retry_backoff = %s, want 100ms (ADR 0014)", got)
	}
	if got := cfg.Upstream.Timeouts.StreamResponseHeader; got != 60*time.Second {
		t.Errorf("upstream.timeouts.stream_response_header = %s, want 60s", got)
	}
	if got := cfg.Upstream.Timeouts.FirstChunk; got != 0 {
		t.Errorf("upstream.timeouts.first_chunk = %s, want 0 (bounded by total)", got)
	}
	if d := cfg.Upstream.DNS; d.TTL != 30*time.Second || d.MaxStale != 5*time.Minute || d.LookupTimeout != 2*time.Second || d.MaxHosts != 1024 {
		t.Errorf("upstream.dns = %+v, want ttl 30s, max_stale 5m, lookup_timeout 2s, max_hosts 1024", d)
	}
	if cfg.Discovery.ReloadInterval != 2*time.Second {
		t.Errorf("discovery.reload_interval = %s, want 2s", cfg.Discovery.ReloadInterval)
	}
	if !Defaults().Auth.Enabled {
		t.Error("auth must be enabled by default")
	}
}

func TestYAMLOverridesDefaults(t *testing.T) {
	cfg := mustParse(t, `
server:
  listen_addr: "127.0.0.1:18080"
  metrics_listen_addr: "127.0.0.1:19090"
  read_header_timeout: 3s
  idle_timeout: 1m
  max_request_body_bytes: 8MiB
shutdown:
  drain_timeout: 45s
discovery:
  reload_interval: 30s
upstream:
  max_idle_conns_per_host: 32
  connect_retry_backoff: 250ms
  timeouts:
    connect: 500ms
    response_header: 20s
    stream_response_header: 15s
    first_chunk: 2m
    idle_between_chunks: 30s
    total: 5m
  dns:
    ttl: 10s
    max_stale: 1m
    lookup_timeout: 500ms
    max_hosts: 16
  api_key: upstream-key
pools:
  - name: large
    replicas:
      - name: l0
        url: http://a:8000
      - name: l1
        url: https://b:8443/
  - name: small
    replicas:
      - url: http://c:8000
models:
  big: large
  tiny: small
default_pool: small
auth:
  enabled: true
  callers:
    - name: svc-a
      token: aaaaaaaaaaaaaaaaaaaa
runtime:
  gc_percent: 300
  memory_limit: 1GiB
`, nil)

	checks := []struct {
		name      string
		got, want any
	}{
		{"listen_addr", cfg.Server.ListenAddr, "127.0.0.1:18080"},
		{"metrics_listen_addr", cfg.Server.MetricsListenAddr, "127.0.0.1:19090"},
		{"read_header_timeout", cfg.Server.ReadHeaderTimeout, 3 * time.Second},
		{"idle_timeout", cfg.Server.IdleTimeout, time.Minute},
		{"max_request_body_bytes", cfg.Server.MaxRequestBodyBytes, ByteSize(8 << 20)},
		{"drain_timeout", cfg.Shutdown.DrainTimeout, 45 * time.Second},
		{"reload_interval", cfg.Discovery.ReloadInterval, 30 * time.Second},
		{"max_idle_conns_per_host", cfg.Upstream.MaxIdleConnsPerHost, 32},
		{"connect_retry_backoff", cfg.Upstream.ConnectRetryBackoff, 250 * time.Millisecond},
		{"connect", cfg.Upstream.Timeouts.Connect, 500 * time.Millisecond},
		{"response_header", cfg.Upstream.Timeouts.ResponseHeader, 20 * time.Second},
		{"idle_between_chunks", cfg.Upstream.Timeouts.IdleBetweenChunks, 30 * time.Second},
		{"total", cfg.Upstream.Timeouts.Total, 5 * time.Minute},
		{"stream_response_header", cfg.Upstream.Timeouts.StreamResponseHeader, 15 * time.Second},
		{"first_chunk", cfg.Upstream.Timeouts.FirstChunk, 2 * time.Minute},
		{"dns.ttl", cfg.Upstream.DNS.TTL, 10 * time.Second},
		{"dns.max_stale", cfg.Upstream.DNS.MaxStale, time.Minute},
		{"dns.lookup_timeout", cfg.Upstream.DNS.LookupTimeout, 500 * time.Millisecond},
		{"dns.max_hosts", cfg.Upstream.DNS.MaxHosts, 16},
		{"api_key", cfg.Upstream.APIKey.Reveal(), "upstream-key"},
		{"pools", len(cfg.Pools), 2},
		{"replica 1 name", cfg.Pools[0].Replicas[1].Name, "l1"},
		{"derived replica name", cfg.Pools[1].Replicas[0].Name, "c:8000"},
		{"default_pool", cfg.DefaultPool, "small"},
		{"caller token", cfg.Auth.Callers[0].Token.Reveal(), "aaaaaaaaaaaaaaaaaaaa"},
		{"gc_percent", cfg.Runtime.GCPercent, 300},
		{"memory_limit", cfg.Runtime.MemoryLimit, ByteSize(1 << 30)},
	}
	for _, c := range checks {
		if !reflect.DeepEqual(c.got, c.want) {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

func TestEnvOverridesYAML(t *testing.T) {
	base := minimalYAML + `
server:
  listen_addr: ":7000"
`
	tests := []struct {
		env   string
		value string
		check func(*Config) any
		want  any
	}{
		{"GATEWAY_LISTEN_ADDR", "127.0.0.1:9999", func(c *Config) any { return c.Server.ListenAddr }, "127.0.0.1:9999"},
		{"GATEWAY_METRICS_LISTEN_ADDR", ":9191", func(c *Config) any { return c.Server.MetricsListenAddr }, ":9191"},
		{"GATEWAY_READ_HEADER_TIMEOUT", "4s", func(c *Config) any { return c.Server.ReadHeaderTimeout }, 4 * time.Second},
		{"GATEWAY_IDLE_TIMEOUT", "30s", func(c *Config) any { return c.Server.IdleTimeout }, 30 * time.Second},
		{"GATEWAY_MAX_REQUEST_BODY_BYTES", "1048576", func(c *Config) any { return c.Server.MaxRequestBodyBytes }, ByteSize(1 << 20)},
		{"GATEWAY_DISCOVERY_RELOAD_INTERVAL", "2s", func(c *Config) any { return c.Discovery.ReloadInterval }, 2 * time.Second},
		{"GATEWAY_SHUTDOWN_DRAIN_TIMEOUT", "1m", func(c *Config) any { return c.Shutdown.DrainTimeout }, time.Minute},
		{"GATEWAY_SHUTDOWN_PRE_DRAIN_DELAY", "2s", func(c *Config) any { return c.Shutdown.PreDrainDelay }, 2 * time.Second},
		{"GATEWAY_UPSTREAM_CONNECT_RETRY_BACKOFF", "1s", func(c *Config) any { return c.Upstream.ConnectRetryBackoff }, time.Second},
		{"GATEWAY_UPSTREAM_MAX_IDLE_CONNS_PER_HOST", "64", func(c *Config) any { return c.Upstream.MaxIdleConnsPerHost }, 64},
		{"GATEWAY_UPSTREAM_IDLE_CONN_TIMEOUT", "45s", func(c *Config) any { return c.Upstream.IdleConnTimeout }, 45 * time.Second},
		{"GATEWAY_UPSTREAM_ENABLE_HTTP2", "true", func(c *Config) any { return c.Upstream.EnableHTTP2 }, true},
		{"GATEWAY_UPSTREAM_CONNECT_TIMEOUT", "1s", func(c *Config) any { return c.Upstream.Timeouts.Connect }, time.Second},
		{"GATEWAY_UPSTREAM_RESPONSE_HEADER_TIMEOUT", "9s", func(c *Config) any { return c.Upstream.Timeouts.ResponseHeader }, 9 * time.Second},
		{"GATEWAY_UPSTREAM_FIRST_CHUNK_TIMEOUT", "45s", func(c *Config) any { return c.Upstream.Timeouts.FirstChunk }, 45 * time.Second},
		{"GATEWAY_UPSTREAM_STREAM_RESPONSE_HEADER_TIMEOUT", "7s", func(c *Config) any { return c.Upstream.Timeouts.StreamResponseHeader }, 7 * time.Second},
		{"GATEWAY_UPSTREAM_DNS_MAX_STALE", "10m", func(c *Config) any { return c.Upstream.DNS.MaxStale }, 10 * time.Minute},
		{"GATEWAY_UPSTREAM_DNS_TTL", "45s", func(c *Config) any { return c.Upstream.DNS.TTL }, 45 * time.Second},
		{"GATEWAY_UPSTREAM_DNS_LOOKUP_TIMEOUT", "3s", func(c *Config) any { return c.Upstream.DNS.LookupTimeout }, 3 * time.Second},
		{"GATEWAY_UPSTREAM_DNS_MAX_HOSTS", "64", func(c *Config) any { return c.Upstream.DNS.MaxHosts }, 64},
		{"GATEWAY_UPSTREAM_IDLE_BETWEEN_CHUNKS_TIMEOUT", "8s", func(c *Config) any { return c.Upstream.Timeouts.IdleBetweenChunks }, 8 * time.Second},
		{"GATEWAY_UPSTREAM_TOTAL_TIMEOUT", "20m", func(c *Config) any { return c.Upstream.Timeouts.Total }, 20 * time.Minute},
		{"GATEWAY_UPSTREAM_API_KEY", "k3y", func(c *Config) any { return c.Upstream.APIKey.Reveal() }, "k3y"},
		{"GATEWAY_DEFAULT_POOL", "large", func(c *Config) any { return c.DefaultPool }, "large"},
		{"GATEWAY_AUTH_ENABLED", "false", func(c *Config) any { return c.Auth.Enabled }, false},
		{"GATEWAY_RUNTIME_GC_PERCENT", "400", func(c *Config) any { return c.Runtime.GCPercent }, 400},
		{"GATEWAY_RUNTIME_MEMORY_LIMIT", "768MiB", func(c *Config) any { return c.Runtime.MemoryLimit }, ByteSize(768 << 20)},
		{"GATEWAY_ROUTER_POLICY", "round_robin", func(c *Config) any { return c.Router.Policy }, "round_robin"},
		{"GATEWAY_ROUTER_EPSILON", "0.35", func(c *Config) any { return c.Router.Epsilon }, 0.35},
		{"GATEWAY_ROUTER_OVERFLOW_SLACK", "0", func(c *Config) any { return c.Router.OverflowSlack }, 0},
		{"GATEWAY_ROUTER_KEY_SCAN_BUDGET", "20480", func(c *Config) any { return c.Router.KeyScanBudget }, 20480},
		{"GATEWAY_ROUTER_HEALTH_POLL_INTERVAL", "3s", func(c *Config) any { return c.Router.Health.PollInterval }, 3 * time.Second},
		{"GATEWAY_ROUTER_HEALTH_POLL_TIMEOUT", "750ms", func(c *Config) any { return c.Router.Health.PollTimeout }, 750 * time.Millisecond},
		{"GATEWAY_ROUTER_HEALTH_FAILURE_THRESHOLD", "5", func(c *Config) any { return c.Router.Health.FailureThreshold }, 5},
		{"GATEWAY_ROUTER_HEALTH_SUCCESS_THRESHOLD", "3", func(c *Config) any { return c.Router.Health.SuccessThreshold }, 3},
		{"GATEWAY_ROUTER_HEALTH_PASSIVE_FAILURE_THRESHOLD", "2", func(c *Config) any { return c.Router.Health.PassiveFailureThreshold }, 2},
		{"GATEWAY_ROUTER_HEALTH_STUCK_ENGINE_TIMEOUT", "1m", func(c *Config) any { return c.Router.Health.StuckEngineTimeout }, time.Minute},
		{"GATEWAY_ROUTER_HEALTH_STUCK_ENGINE_STREAMS", "5", func(c *Config) any { return c.Router.Health.StuckEngineStreams }, 5},
		{"GATEWAY_ROUTER_HEALTH_STUCK_ENGINE_RATIO", "0.75", func(c *Config) any { return c.Router.Health.StuckEngineRatio }, 0.75},
		{"GATEWAY_ROUTER_HEALTH_STUCK_ENGINE_WINDOW", "2m", func(c *Config) any { return c.Router.Health.StuckEngineWindow }, 2 * time.Minute},
		{"GATEWAY_ROUTER_HEALTH_STUCK_ENGINE_MAX_WAITS", "1024", func(c *Config) any { return c.Router.Health.StuckEngineMaxWaits }, 1024},
		{"GATEWAY_ROUTER_HEALTH_MIN_EJECTION", "3s", func(c *Config) any { return c.Router.Health.MinEjection }, 3 * time.Second},
		{"GATEWAY_ROUTER_HEALTH_MAX_EJECTION", "2m", func(c *Config) any { return c.Router.Health.MaxEjection }, 2 * time.Minute},
		{"GATEWAY_ROUTER_HEALTH_EJECTION_BACKOFF_MULTIPLIER", "1.5", func(c *Config) any { return c.Router.Health.EjectionBackoffMultiplier }, 1.5},
		{"GATEWAY_ROUTER_HEALTH_STUCK_PROBE_ENABLED", "false", func(c *Config) any { return c.Router.Health.StuckProbeEnabled }, false},
		{"GATEWAY_ROUTER_HEALTH_STUCK_PROBE_TIMEOUT", "4s", func(c *Config) any { return c.Router.Health.StuckProbeTimeout }, 4 * time.Second},
		{"GATEWAY_ROUTER_HEALTH_STUCK_PROBE_PATH", "/v1/chat/completions", func(c *Config) any { return c.Router.Health.StuckProbePath }, "/v1/chat/completions"},
		{"GATEWAY_ROUTER_LOAD_VIEW_SCRAPE_INTERVAL", "7s", func(c *Config) any { return c.Router.LoadView.ScrapeInterval }, 7 * time.Second},
		{"GATEWAY_ROUTER_LOAD_VIEW_SCRAPE_TIMEOUT", "1s", func(c *Config) any { return c.Router.LoadView.ScrapeTimeout }, time.Second},
		{"GATEWAY_ROUTER_LOAD_VIEW_PROBE_INTERVAL", "2m", func(c *Config) any { return c.Router.LoadView.ProbeInterval }, 2 * time.Minute},
		{"GATEWAY_ROUTER_LOAD_VIEW_STALE_THRESHOLD", "20s", func(c *Config) any { return c.Router.LoadView.StaleThreshold }, 20 * time.Second},
		{"GATEWAY_PRIORITY_DEFAULT_CLASS", "background", func(c *Config) any { return c.Priority.DefaultClass }, "background"},
		{"GATEWAY_PRIORITY_ANONYMOUS_MAX_CLASS", "interactive", func(c *Config) any { return c.Priority.AnonymousMaxClass }, "interactive"},
		{"GATEWAY_PRIORITY_DEFAULT_MAX_CLASS", "interactive", func(c *Config) any { return c.Priority.DefaultMaxClass }, "interactive"},
		{"GATEWAY_PRIORITY_PROBE_INTERVAL", "2m", func(c *Config) any { return c.Priority.Probe.Interval }, 2 * time.Minute},
		{"GATEWAY_PRIORITY_PROBE_TIMEOUT", "5s", func(c *Config) any { return c.Priority.Probe.Timeout }, 5 * time.Second},
		{"GATEWAY_ADMISSION_ENABLED", "false", func(c *Config) any { return c.Admission.Enabled }, false},
		{"GATEWAY_ADMISSION_MAX_WAIT", "4s", func(c *Config) any { return c.Admission.MaxWait }, 4 * time.Second},
		{"GATEWAY_ADMISSION_MAX_QUEUE", "64", func(c *Config) any { return c.Admission.MaxQueue }, 64},
		{"GATEWAY_ADMISSION_RETRY_AFTER_MAX", "5s", func(c *Config) any { return c.Admission.RetryAfterMax }, 5 * time.Second},
		{"GATEWAY_ADMISSION_EARLY_REJECT", "true", func(c *Config) any { return c.Admission.EarlyReject }, true},
		{"GATEWAY_ADMISSION_SHED_ENABLED", "false", func(c *Config) any { return c.Admission.Shed.Enabled }, false},
		{"GATEWAY_ADMISSION_SHED_TTFT_P95_THRESHOLD", "750ms", func(c *Config) any { return c.Admission.Shed.TTFTP95Threshold }, 750 * time.Millisecond},
		{"GATEWAY_ADMISSION_SHED_WINDOW", "10s", func(c *Config) any { return c.Admission.Shed.Window }, 10 * time.Second},
		{"GATEWAY_ADMISSION_SHED_MIN_SAMPLES", "50", func(c *Config) any { return c.Admission.Shed.MinSamples }, 50},
		{"GATEWAY_ADMISSION_SHED_RECOVER_RATIO", "0.5", func(c *Config) any { return c.Admission.Shed.RecoverRatio }, 0.5},
		{"GATEWAY_ADMISSION_REDIS_ENABLED", "true", func(c *Config) any { return c.Admission.Redis.Enabled }, true},
		{"GATEWAY_ADMISSION_REDIS_ADDR", "cache:6380", func(c *Config) any { return c.Admission.Redis.Addr }, "cache:6380"},
		{"GATEWAY_ADMISSION_REDIS_USERNAME", "gw", func(c *Config) any { return c.Admission.Redis.Username }, "gw"},
		{"GATEWAY_ADMISSION_REDIS_PASSWORD", "pw", func(c *Config) any { return c.Admission.Redis.Password.Reveal() }, "pw"},
		{"GATEWAY_ADMISSION_REDIS_DB", "2", func(c *Config) any { return c.Admission.Redis.DB }, 2},
		{"GATEWAY_ADMISSION_REDIS_SYNC_INTERVAL", "200ms", func(c *Config) any { return c.Admission.Redis.SyncInterval }, 200 * time.Millisecond},
		{"GATEWAY_ADMISSION_REDIS_TIMEOUT", "20ms", func(c *Config) any { return c.Admission.Redis.Timeout }, 20 * time.Millisecond},
		{"GATEWAY_ADMISSION_REDIS_LEASE_TTL", "5s", func(c *Config) any { return c.Admission.Redis.LeaseTTL }, 5 * time.Second},
		{"GATEWAY_ADMISSION_REDIS_KEY_PREFIX", "gw2:adm", func(c *Config) any { return c.Admission.Redis.KeyPrefix }, "gw2:adm"},
		{"GATEWAY_TEMPLATES_POSTGRES_DSN", "postgres://u:p@db/x", func(c *Config) any { return c.Templates.Postgres.DSN.Reveal() }, "postgres://u:p@db/x"},
		{"GATEWAY_TEMPLATES_POSTGRES_CONNECT_TIMEOUT", "3s", func(c *Config) any { return c.Templates.Postgres.ConnectTimeout }, 3 * time.Second},
		{"GATEWAY_TEMPLATES_POSTGRES_QUERY_TIMEOUT", "1s", func(c *Config) any { return c.Templates.Postgres.QueryTimeout }, time.Second},
		{"GATEWAY_TEMPLATES_POSTGRES_MAX_CONNS", "8", func(c *Config) any { return c.Templates.Postgres.MaxConns }, 8},
		{"GATEWAY_TEMPLATES_REFRESH_INTERVAL", "10s", func(c *Config) any { return c.Templates.RefreshInterval }, 10 * time.Second},
		{"GATEWAY_TEMPLATES_MAX_VERSIONS", "100", func(c *Config) any { return c.Templates.MaxVersions }, 100},
		{"GATEWAY_TEMPLATES_MAX_TOKENS_ENABLED", "false", func(c *Config) any { return c.Templates.MaxTokens.Enabled }, false},
		{"GATEWAY_TEMPLATES_MAX_TOKENS_MARGIN", "0.5", func(c *Config) any { return c.Templates.MaxTokens.Margin }, 0.5},
		{"GATEWAY_TEMPLATES_MAX_TOKENS_MIN_SAMPLES", "50", func(c *Config) any { return c.Templates.MaxTokens.MinSamples }, 50},
		{"GATEWAY_ADMIN_LISTEN_ADDR", ":7081", func(c *Config) any { return c.Admin.ListenAddr }, ":7081"},
		{"GATEWAY_ADMIN_TOKENIZE_TIMEOUT", "9s", func(c *Config) any { return c.Admin.TokenizeTimeout }, 9 * time.Second},
		{"GATEWAY_COALESCING_MAX_FLIGHTS", "64", func(c *Config) any { return c.Coalescing.MaxFlights }, 64},
		{"GATEWAY_COALESCING_MAX_SUBSCRIBERS", "8", func(c *Config) any { return c.Coalescing.MaxSubscribers }, 8},
		{"GATEWAY_COALESCING_MAX_REPLAY_BYTES", "2097152", func(c *Config) any { return c.Coalescing.MaxReplayBytes }, 2 << 20},
		{"GATEWAY_COALESCING_MAX_KEY_BODY_BYTES", "65536", func(c *Config) any { return c.Coalescing.MaxKeyBodyBytes }, 64 << 10},
		{"GATEWAY_CACHE_ENABLED", "true", func(c *Config) any { return c.Cache.Enabled }, true},
		{"GATEWAY_CACHE_REDIS_ADDR", "cache:6380", func(c *Config) any { return c.Cache.Redis.Addr }, "cache:6380"},
		{"GATEWAY_CACHE_REDIS_USERNAME", "gw", func(c *Config) any { return c.Cache.Redis.Username }, "gw"},
		{"GATEWAY_CACHE_REDIS_PASSWORD", "pw", func(c *Config) any { return c.Cache.Redis.Password.Reveal() }, "pw"},
		{"GATEWAY_CACHE_REDIS_DB", "3", func(c *Config) any { return c.Cache.Redis.DB }, 3},
		{"GATEWAY_CACHE_CONNECTIONS", "8", func(c *Config) any { return c.Cache.Connections }, 8},
		{"GATEWAY_CACHE_TIMEOUT", "50ms", func(c *Config) any { return c.Cache.Timeout }, 50 * time.Millisecond},
		{"GATEWAY_CACHE_MAX_TTL", "10m", func(c *Config) any { return c.Cache.MaxTTL }, 10 * time.Minute},
		{"GATEWAY_CACHE_MAX_ENTRY_BYTES", "4096", func(c *Config) any { return c.Cache.MaxEntryBytes }, 4096},
		{"GATEWAY_CACHE_WRITE_QUEUE", "32", func(c *Config) any { return c.Cache.WriteQueue }, 32},
		{"GATEWAY_CACHE_WRITE_WORKERS", "3", func(c *Config) any { return c.Cache.WriteWorkers }, 3},
		{"GATEWAY_CACHE_KEY_PREFIX", "gw2:cache", func(c *Config) any { return c.Cache.KeyPrefix }, "gw2:cache"},
		{"GATEWAY_CONTEXT_ENABLED", "true", func(c *Config) any { return c.Context.Enabled }, true},
		{"GATEWAY_CONTEXT_MAX_PROBES", "6", func(c *Config) any { return c.Context.MaxProbes }, 6},
		{"GATEWAY_CONTEXT_TOKENIZE_TIMEOUT", "3s", func(c *Config) any { return c.Context.TokenizeTimeout }, 3 * time.Second},
		{"GATEWAY_CONTEXT_ASSEMBLE_TIMEOUT", "9s", func(c *Config) any { return c.Context.AssembleTimeout }, 9 * time.Second},
		{"GATEWAY_CONTEXT_MAX_BODY_BYTES", "65536", func(c *Config) any { return c.Context.MaxBodyBytes }, 64 << 10},
	}
	if len(tests) != len(envOverrides) {
		t.Fatalf("test covers %d env overrides, package defines %d", len(tests), len(envOverrides))
	}
	for _, tc := range tests {
		t.Run(tc.env, func(t *testing.T) {
			cfg := mustParse(t, base, map[string]string{tc.env: tc.value})
			if got := tc.check(cfg); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("%s=%s: got %v, want %v", tc.env, tc.value, got, tc.want)
			}
		})
	}

	t.Run("empty env value is ignored", func(t *testing.T) {
		cfg := mustParse(t, base, map[string]string{"GATEWAY_LISTEN_ADDR": ""})
		if cfg.Server.ListenAddr != ":7000" {
			t.Fatalf("listen_addr = %q, want YAML value :7000", cfg.Server.ListenAddr)
		}
	})
}

func TestEnvOverrideInvalid(t *testing.T) {
	tests := []struct{ env, value, want string }{
		{"GATEWAY_READ_HEADER_TIMEOUT", "ten", "env GATEWAY_READ_HEADER_TIMEOUT: invalid duration"},
		{"GATEWAY_UPSTREAM_MAX_IDLE_CONNS_PER_HOST", "many", "env GATEWAY_UPSTREAM_MAX_IDLE_CONNS_PER_HOST: invalid integer"},
		{"GATEWAY_AUTH_ENABLED", "maybe", "env GATEWAY_AUTH_ENABLED: invalid boolean"},
		{"GATEWAY_RUNTIME_MEMORY_LIMIT", "lots", "env GATEWAY_RUNTIME_MEMORY_LIMIT: invalid size"},
		{"GATEWAY_MAX_REQUEST_BODY_BYTES", "32MB", "env GATEWAY_MAX_REQUEST_BODY_BYTES: invalid size"},
		{"GATEWAY_DISCOVERY_RELOAD_INTERVAL", "often", "env GATEWAY_DISCOVERY_RELOAD_INTERVAL: invalid duration"},
	}
	for _, tc := range tests {
		t.Run(tc.env, func(t *testing.T) {
			_, err := Parse([]byte(minimalYAML), envMap(map[string]string{tc.env: tc.value}))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestYAMLDecodeErrors(t *testing.T) {
	tests := []struct{ name, yaml, want string }{
		{"unknown key", minimalYAML + "servr:\n  listen_addr: x\n", "field servr not found"},
		{"unknown nested key", minimalYAML + "upstream:\n  timeouts:\n    conect: 1s\n", "field conect not found"},
		{"bad duration", minimalYAML + "shutdown:\n  drain_timeout: soon\n", "parse yaml"},
		{"malformed", "pools: [", "parse yaml"},
		{"multiple documents", minimalYAML + "---\nserver: {}\n", "multiple documents"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.yaml), envMap(nil))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestLoad(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "gateway.yaml")
	if err := os.WriteFile(good, []byte(minimalYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	big := filepath.Join(dir, "big.yaml")
	if err := os.WriteFile(big, bytes.Repeat([]byte("#"), maxFileBytes+1), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Run("file from GATEWAY_CONFIG", func(t *testing.T) {
		cfg, err := Load(envMap(map[string]string{EnvConfigPath: good}))
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.DefaultPool != "large" {
			t.Fatalf("DefaultPool = %q", cfg.DefaultPool)
		}
	})
	for _, tc := range []struct{ name, path, want string }{
		{"no file means no pools", "", "set GATEWAY_CONFIG"},
		{"missing file", filepath.Join(dir, "nope.yaml"), "open "},
		{"oversized file", big, "exceeds"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(envMap(map[string]string{EnvConfigPath: tc.path, "GATEWAY_AUTH_ENABLED": "false"}))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestPoolForModel(t *testing.T) {
	cfg := mustParse(t, minimalYAML+`
models:
  m-large: large
`, nil)
	if p, ok := cfg.PoolForModel("m-large"); p != "large" || !ok {
		t.Errorf("PoolForModel(m-large) = %q, %v", p, ok)
	}
	if p, ok := cfg.PoolForModel("unknown"); p != "large" || ok {
		t.Errorf("PoolForModel(unknown) = %q, %v; want default pool, unmapped", p, ok)
	}
}

func exampleEnv() map[string]string {
	return map[string]string{
		"GATEWAY_TOKEN_CHAT_SERVICE":  "chat-service-token-0123456789",
		"GATEWAY_TOKEN_BATCH_SERVICE": "batch-service-token-0123456789",
	}
}

func TestExampleConfigIsValid(t *testing.T) {
	env := exampleEnv()
	env[EnvConfigPath] = examplePath
	cfg, err := Load(envMap(env))
	if err != nil {
		t.Fatalf("example config does not load: %v", err)
	}
	// The example's values are documented as the defaults.
	def := Defaults()
	if cfg.Server != def.Server || cfg.Shutdown != def.Shutdown || cfg.Discovery != def.Discovery || cfg.Upstream != def.Upstream || cfg.Auth.Enabled != def.Auth.Enabled || cfg.Runtime != def.Runtime {
		t.Errorf("example values drifted from built-in defaults:\n got %+v %+v %+v\nwant %+v %+v %+v",
			cfg.Server, cfg.Shutdown, cfg.Upstream, def.Server, def.Shutdown, def.Upstream)
	}
	if !reflect.DeepEqual(cfg.Router, def.Router) {
		t.Errorf("example router section drifted from built-in defaults:\n got %+v\nwant %+v", cfg.Router, def.Router)
	}
	if cfg.Priority.Probe != def.Priority.Probe {
		t.Errorf("example priority.probe = %+v, built-in default %+v", cfg.Priority.Probe, def.Priority.Probe)
	}
}

// TestExampleDocumentsEveryKey fails when a Config field or env override is
// added without documenting it in the example config.
func TestExampleDocumentsEveryKey(t *testing.T) {
	raw, err := os.ReadFile(examplePath)
	if err != nil {
		t.Fatal(err)
	}
	var doc any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	// Keys that must not appear as live values in the example (an inline
	// secret) are documented in a comment line instead.
	commentOnly := map[string]string{"auth.callers.[].token": "# token:", "admin.tokens.[].token": "# token:"}
	for _, path := range yamlPaths(reflect.TypeFor[Config](), nil) {
		key := strings.Join(path, ".")
		if hasPath(doc, path) {
			continue
		}
		if c, ok := commentOnly[key]; ok && bytes.Contains(raw, []byte(c)) {
			continue
		}
		t.Errorf("example config is missing key %s", key)
	}
	for _, name := range append([]string{EnvConfigPath}, envNames()...) {
		if !bytes.Contains(raw, []byte(name)) {
			t.Errorf("example config does not mention env var %s", name)
		}
	}
}

func envNames() []string {
	names := make([]string, 0, len(envOverrides))
	for _, o := range envOverrides {
		names = append(names, o.name)
	}
	return names
}

// yamlPaths lists every leaf key path of a struct type; "[]" marks a list.
func yamlPaths(t reflect.Type, prefix []string) [][]string {
	var out [][]string
	for f := range t.Fields() {
		tag := strings.Split(f.Tag.Get("yaml"), ",")[0]
		if tag == "" || tag == "-" {
			continue
		}
		p := append(append([]string{}, prefix...), tag)
		ft := f.Type
		switch {
		case ft.Kind() == reflect.Struct:
			out = append(out, yamlPaths(ft, p)...)
		case ft.Kind() == reflect.Slice && ft.Elem().Kind() == reflect.Struct:
			out = append(out, yamlPaths(ft.Elem(), append(p, "[]"))...)
		default:
			out = append(out, p)
		}
	}
	return out
}

// hasPath reports whether the decoded YAML contains path. Only one list
// element needs a given key, since some keys are optional per element.
func hasPath(node any, path []string) bool {
	if len(path) == 0 {
		return true
	}
	switch n := node.(type) {
	case map[string]any:
		child, ok := n[path[0]]
		return ok && hasPath(child, path[1:])
	case []any:
		if path[0] != "[]" {
			return false
		}
		for _, el := range n {
			if hasPath(el, path[1:]) {
				return true
			}
		}
	}
	return false
}

func TestSecretsNeverFormatted(t *testing.T) {
	const tok = "super-secret-caller-token"
	const key = "super-secret-upstream-key"
	cfg := mustParse(t, `
pools:
  - name: large
    replicas:
      - url: http://vllm-large-0:8000
upstream:
  api_key: `+key+`
auth:
  enabled: true
  callers:
    - name: svc
      token: `+tok+`
`, nil)
	if cfg.Auth.Callers[0].Token.Reveal() != tok || cfg.Upstream.APIKey.Reveal() != key {
		t.Fatal("Reveal must return the raw values")
	}

	var logBuf bytes.Buffer
	slog.New(slog.NewJSONHandler(&logBuf, nil)).Info("cfg", "key", cfg.Upstream.APIKey, "caller", cfg.Auth.Callers[0].Token)
	js, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ym, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	outputs := map[string]string{
		"%v":   fmt.Sprintf("%v", cfg),
		"%+v":  fmt.Sprintf("%+v", *cfg),
		"%#v":  fmt.Sprintf("%#v", *cfg),
		"%s":   fmt.Sprintf("key=%s", cfg.Upstream.APIKey),
		"slog": logBuf.String(),
		"json": string(js),
		"yaml": string(ym),
	}
	for name, out := range outputs {
		if strings.Contains(out, tok) || strings.Contains(out, key) {
			t.Errorf("%s output leaks a secret: %s", name, out)
		}
		if !strings.Contains(out, redacted) {
			t.Errorf("%s output lacks %q: %s", name, redacted, out)
		}
	}
}
