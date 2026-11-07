// Package config loads the gateway's configuration from a mounted YAML file
// and environment variables, applies defaults and validates the result.
//
// Precedence, lowest to highest: built-in defaults, the YAML file named by
// GATEWAY_CONFIG, then GATEWAY_* environment overrides. Invalid configuration
// is rejected with an error that names every offending key, so the process
// fails fast at startup instead of misbehaving on the request path.
//
// Every key is documented in deploy/compose/config/gateway.example.yaml.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// EnvConfigPath names the environment variable holding the YAML file path.
const EnvConfigPath = "GATEWAY_CONFIG"

// maxFileBytes bounds the config file read. Real configs are a few KiB.
const maxFileBytes = 1 << 20

// Built-in defaults. Capacity-related values are starting points to be
// calibrated by the harness (ADR 0005), not measured limits.
const (
	DefaultListenAddr        = ":8080"
	DefaultMetricsListenAddr = ":9090"
	DefaultReadHeaderTimeout = 10 * time.Second
	DefaultIdleTimeout       = 120 * time.Second
	DefaultDrainTimeout      = 15 * time.Second

	// DefaultMaxRequestBodyBytes mirrors ingress.DefaultMaxRequestBodyBytes
	// (config cannot import ingress; cmd/gateway tests keep them equal).
	DefaultMaxRequestBodyBytes ByteSize = 32 << 20
	// DefaultReloadInterval mirrors discovery.DefaultWatchInterval
	// (config cannot import discovery; cmd/gateway tests keep them equal).
	// 2s keeps a replica removal inside AC-M1-4's 5 s rebalance target: the
	// health tracker rebuilds rings as soon as discovery changes.
	DefaultReloadInterval = 2 * time.Second

	// DefaultMaxIdleConnsPerHost keeps connections to a replica open between
	// requests when thousands of long-lived streams finish together, so they
	// are reused instead of closed and redialled. 8192 is what the scale bench
	// used; M0-41 calibrates it.
	DefaultMaxIdleConnsPerHost     = 8192
	DefaultUpstreamIdleConnTimeout = 90 * time.Second
	DefaultConnectTimeout          = 2 * time.Second
	// DefaultConnectRetryBackoff is the pause before a failed connect is
	// retried on the same replica, when the pool has no other (ADR 0014).
	DefaultConnectRetryBackoff = 100 * time.Millisecond
	// DefaultResponseHeaderTimeout is 0: no separate limit, the wait for
	// response headers is bounded by the total timeout only. vLLM sends a
	// non-streaming response's headers after the whole generation and queues
	// requests under load, so any shorter limit fails requests a direct call
	// would serve.
	DefaultResponseHeaderTimeout = 0
	// DefaultStreamResponseHeaderTimeout bounds the header wait of a
	// streaming request. vLLM answers a streaming request with headers at
	// once, before the engine schedules it, so a long silence there means a
	// hung replica, not a long generation or a queue.
	DefaultStreamResponseHeaderTimeout = 60 * time.Second
	// DefaultFirstChunkTimeout is 0: the wait for a response's first body
	// byte is bounded by the total timeout only. vLLM sends a stream's
	// headers at once and its first chunk only after the request is
	// scheduled and prefilled, so queueing under load is spent here.
	DefaultFirstChunkTimeout        = 0
	DefaultIdleBetweenChunksTimeout = 60 * time.Second
	DefaultTotalTimeout             = 15 * time.Minute

	// DefaultDNSTTL is how long a replica host's resolved addresses are used
	// before they are looked up again.
	DefaultDNSTTL = 30 * time.Second
	// DefaultDNSMaxStale is how long past their TTL a host's addresses stay
	// in use while lookups fail temporarily (timeouts, server errors).
	DefaultDNSMaxStale = 5 * time.Minute
	// DefaultDNSLookupTimeout bounds one lookup of a replica host. It is
	// separate from the connect timeout, which covers TCP only.
	DefaultDNSLookupTimeout = 2 * time.Second
	// DefaultDNSMaxHosts caps the resolver cache. Replica hosts come from
	// config, so a handful is normal.
	DefaultDNSMaxHosts = 1024

	DefaultAuthEnabled = true

	// Default priority classes (highest to lowest priority).
	PriorityInteractive = "interactive"
	PriorityUser        = "user"
	PriorityBackground  = "background"
)

// Config is the complete gateway configuration.
type Config struct {
	Server    Server    `yaml:"server"`
	Shutdown  Shutdown  `yaml:"shutdown"`
	Discovery Discovery `yaml:"discovery"`
	Upstream  Upstream  `yaml:"upstream"`
	// Pools are groups of interchangeable replicas serving the same model
	// tier (for example "large" and "small").
	Pools []Pool `yaml:"pools"`
	// Models maps a request's `model` field to a pool name.
	Models map[string]string `yaml:"models"`
	// DefaultPool receives requests whose model is not in Models (fail open).
	// May be omitted when exactly one pool is configured.
	DefaultPool string `yaml:"default_pool"`
	Auth        Auth   `yaml:"auth"`
	// Runtime tunes the Go garbage collector; every knob is optional.
	Runtime Runtime `yaml:"runtime"`
	// Router configures request routing policy, replica health tracking and
	// the load view (router.go).
	Router RouterConfig `yaml:"router"`
	// Priority configures request priority classes and caller policy.
	Priority PriorityConfig `yaml:"priority"`
	// Admission configures per-pool slots, priority queues and shedding
	// (admission.go).
	Admission Admission `yaml:"admission"`
	// Templates configures the prompt template registry (templates.go).
	Templates Templates `yaml:"templates"`
	// Admin configures the template admin API's listener and tokens.
	Admin Admin `yaml:"admin"`
	// Coalescing bounds in-flight coalescing of identical requests (dedup.go).
	Coalescing Coalescing `yaml:"coalescing"`
	// Cache configures the Redis response cache (dedup.go).
	Cache Cache `yaml:"cache"`
	// Context configures budgeted context assembly (dedup.go).
	Context ContextBudget `yaml:"context"`
}

// PriorityConfig configures priority classes and per-caller policies.
//
// Class names are lowercase: validation holds them to namePattern, and
// ResolveClass lowercases the X-Priority header before comparing, so the
// header is matched case-insensitively against the configured names.
type PriorityConfig struct {
	// Classes lists valid priority class names, ordered from highest to lowest priority.
	Classes []string `yaml:"classes"`
	// DefaultClass is the fallback class when a request does not specify one
	// and the caller has no default policy. It is still clamped to the
	// caller's maximum (AnonymousMaxClass, DefaultMaxClass or the policy Max).
	DefaultClass string `yaml:"default_class"`
	// AnonymousMaxClass is the highest class a request without an
	// authenticated caller (auth off) may get, whatever X-Priority says.
	AnonymousMaxClass string `yaml:"anonymous_max_class"`
	// DefaultMaxClass is the highest class an authenticated caller may get
	// when it has no policy, or a policy without Max.
	DefaultMaxClass string `yaml:"default_max_class"`
	// Callers configures per-caller priority defaults and maximum allowed classes.
	Callers map[string]CallerPolicy `yaml:"callers"`
	// UpstreamValues maps each class to the vLLM `priority` sent to pools
	// with priority_scheduling (lower is served sooner; ADR 0009). Required
	// for every class while any pool sets priority_scheduling.
	UpstreamValues map[string]int `yaml:"upstream_values"`
	// Probe paces the probe that checks a flagged pool's replicas accept a
	// vLLM priority before one is sent (ADR 0018). Read at startup, and
	// validated with the whole configuration, not by Validate: a later
	// update has no effect on it.
	Probe PriorityProbe `yaml:"probe"`
}

// PriorityProbe paces the priority probe.
type PriorityProbe struct {
	// Interval is how often each flagged pool's replicas are probed.
	Interval time.Duration `yaml:"interval"`
	// Timeout bounds one probe, from sending it to its answer; at most
	// Interval.
	Timeout time.Duration `yaml:"timeout"`
}

// CallerPolicy configures priority rules for a specific caller.
type CallerPolicy struct {
	// Default is the priority class assigned when X-Priority is missing or empty.
	Default string `yaml:"default"`
	// Max is the highest priority class this caller is permitted to request.
	// If X-Priority requests a class higher than Max, it is clamped to Max.
	// Empty means PriorityConfig.DefaultMaxClass.
	Max string `yaml:"max"`
}

// IsValidClass reports whether c is one of the configured priority classes.
// The comparison is exact; class names are lowercase (see PriorityConfig).
func (p PriorityConfig) IsValidClass(c string) bool {
	_, ok := p.ClassRank(c)
	return ok
}

// ClassRank returns the priority rank of c (0 is highest priority).
// Returns (-1, false) if c is not in p.Classes.
func (p PriorityConfig) ClassRank(c string) (int, bool) {
	for i, class := range p.Classes {
		if class == c {
			return i, true
		}
	}
	return -1, false
}

// AnonymousCallerName is the caller name the auth middleware attaches when
// auth is disabled (auth.AnonymousName). A priority.callers entry under
// this name could never apply, so validation rejects it.
const AnonymousCallerName = "anonymous"

// ResolveClass resolves the priority class for a request given the
// X-Priority header value, the caller name and whether the caller
// presented a valid token (false with auth off).
//
// The requested class is the X-Priority header if it names a configured
// class (case-insensitively, surrounding space ignored), else the caller
// policy's Default, else DefaultClass. A header past MaxPriorityHeaderBytes
// names none and is not read. The result is then clamped to the caller's
// maximum:
//   - not authenticated: AnonymousMaxClass (caller policies are ignored);
//   - authenticated with a policy Max: that Max;
//   - any other authenticated caller: DefaultMaxClass.
//
// An unknown header never fails the request: it resolves as if absent.
func (p PriorityConfig) ResolveClass(headerVal, callerName string, authenticated bool) string {
	maxClass, defClass := p.limitsFor(callerName, authenticated)
	class := defClass
	if len(headerVal) <= MaxPriorityHeaderBytes {
		if h := strings.ToLower(strings.TrimSpace(headerVal)); h != "" && p.IsValidClass(h) {
			class = h
		}
	}
	return p.clamp(class, maxClass)
}

// limitsFor returns the maximum and default classes for a caller.
func (p PriorityConfig) limitsFor(callerName string, authenticated bool) (maxClass, defClass string) {
	defClass = p.DefaultClass
	if defClass == "" {
		defClass = PriorityUser
	}
	if !authenticated {
		return p.AnonymousMaxClass, defClass
	}
	maxClass = p.DefaultMaxClass
	pol, ok := p.Callers[callerName]
	if !ok {
		return maxClass, defClass
	}
	if pol.Max != "" {
		maxClass = pol.Max
	}
	if pol.Default != "" && p.IsValidClass(pol.Default) {
		defClass = pol.Default
	}
	return maxClass, defClass
}

// clamp lowers class to the ceiling for maxClass when class has the higher
// priority (rank 0 is highest).
func (p PriorityConfig) clamp(class, maxClass string) string {
	rank, ok := p.ClassRank(class)
	if !ok {
		return class
	}
	if maxRank, ok := p.ceiling(maxClass); ok && rank < maxRank {
		return p.Classes[maxRank]
	}
	return class
}

// ceiling returns the rank of maxClass. A validated config always has one;
// for a config built in code with an empty or unknown maximum it falls back
// to DefaultClass, then "user", then the lowest class, so a gap in the
// config never lifts the clamp.
func (p PriorityConfig) ceiling(maxClass string) (int, bool) {
	for _, c := range []string{maxClass, p.DefaultClass, PriorityUser} {
		if r, ok := p.ClassRank(c); ok {
			return r, true
		}
	}
	if n := len(p.Classes); n > 0 {
		return n - 1, true
	}
	return 0, false
}

// Validate checks p on its own, as Parse does for the priority section.
// Use it before swapping in a priority config built outside Parse.
func (p PriorityConfig) Validate() error {
	return errors.Join(p.validate()...)
}

// Server configures the gateway's own listeners.
type Server struct {
	// ListenAddr serves the OpenAI-wire request path and health endpoints.
	ListenAddr string `yaml:"listen_addr"`
	// MetricsListenAddr serves Prometheus metrics on an internal port.
	MetricsListenAddr string `yaml:"metrics_listen_addr"`
	// ReadHeaderTimeout bounds slow-header clients. There is deliberately no
	// write timeout: SSE streams may legitimately run for minutes.
	ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"`
	// IdleTimeout closes idle client keep-alive connections.
	IdleTimeout time.Duration `yaml:"idle_timeout"`
	// MaxRequestBodyBytes bounds a request body held in memory; larger
	// bodies get 413. Written with a unit (32MiB) or as plain bytes.
	MaxRequestBodyBytes ByteSize `yaml:"max_request_body_bytes"`
}

// Shutdown configures graceful drain on SIGTERM.
type Shutdown struct {
	// DrainTimeout bounds how long open requests and streams get to finish.
	DrainTimeout time.Duration `yaml:"drain_timeout"`
	// PreDrainDelay pauses after marking unready before closing the listener,
	// allowing load balancers to redirect traffic without 502s (default 0s).
	PreDrainDelay time.Duration `yaml:"pre_drain_delay"`
}

// Discovery configures hot reload of the replica lists.
//
// Only replica lists are hot-reloaded: the gateway re-reads the file named by
// GATEWAY_CONFIG every ReloadInterval and applies changed replicas. Changes to
// models, default_pool, the set of pools or any other key need a restart, and
// a reload that changes the set of pools is refused (the current set is kept).
type Discovery struct {
	// ReloadInterval is how often the config file is re-read for replica
	// changes. It is read once at startup.
	ReloadInterval time.Duration `yaml:"reload_interval"`
}

// Upstream configures the pooled HTTP clients towards replicas.
type Upstream struct {
	MaxIdleConnsPerHost int `yaml:"max_idle_conns_per_host"`
	// IdleConnTimeout closes pooled keep-alive connections unused for this long.
	IdleConnTimeout time.Duration `yaml:"idle_conn_timeout"`
	// EnableHTTP2 allows negotiating HTTP/2 over TLS towards HTTPS replicas or
	// front proxies, multiplexing concurrent streams and /tokenize probes.
	EnableHTTP2 bool `yaml:"enable_http2"`
	// ConnectRetryBackoff is the pause before a connect that failed (nothing
	// sent) is retried once on the same replica, used only when the pool has
	// no other replica to retry on (ADR 0014).
	ConnectRetryBackoff time.Duration    `yaml:"connect_retry_backoff"`
	Timeouts            UpstreamTimeouts `yaml:"timeouts"`
	DNS                 UpstreamDNS      `yaml:"dns"`
	// APIKey is sent upstream by the gateway; callers never hold it.
	// Prefer the GATEWAY_UPSTREAM_API_KEY environment variable.
	APIKey Secret `yaml:"api_key"`
}

// UpstreamTimeouts are the explicit per-request upstream timeouts.
type UpstreamTimeouts struct {
	// Connect bounds TCP connection establishment.
	Connect time.Duration `yaml:"connect"`
	// ResponseHeader bounds the wait for response headers after the request
	// is written. 0 (the default) means no separate limit: Total bounds it.
	// vLLM sends non-streaming headers only after the whole generation and
	// queues requests under load, so a shorter limit fails requests that a
	// direct call would serve.
	ResponseHeader time.Duration `yaml:"response_header"`
	// StreamResponseHeader bounds the wait for response headers of a
	// streaming request (vLLM sends them at once, before scheduling). 0
	// means bounded by Total only.
	StreamResponseHeader time.Duration `yaml:"stream_response_header"`
	// FirstChunk bounds the wait for the first response body byte after the
	// headers. 0 (the default) means bounded by Total only: a streaming
	// request queued inside vLLM has its headers but no chunk yet.
	FirstChunk time.Duration `yaml:"first_chunk"`
	// IdleBetweenChunks bounds the gap between body reads once the first
	// byte arrived, catching hung replicas without cutting long generations.
	IdleBetweenChunks time.Duration `yaml:"idle_between_chunks"`
	// Total bounds the whole upstream exchange.
	Total time.Duration `yaml:"total"`
}

// UpstreamDNS configures the cache of replica host addresses. Lookups run
// outside the connect timeout, one at a time per host and, once a host is
// known, in the background. A host whose lookup fails temporarily keeps
// using its last known addresses for up to MaxStale (fail open).
type UpstreamDNS struct {
	// TTL is how long resolved addresses are used before the next lookup.
	// Expired addresses keep serving while a background lookup refreshes them.
	TTL time.Duration `yaml:"ttl"`
	// MaxStale is how long past TTL addresses stay in use while lookups fail
	// temporarily. An authoritative "no such host" drops them at once.
	MaxStale time.Duration `yaml:"max_stale"`
	// LookupTimeout bounds one lookup of a replica host.
	LookupTimeout time.Duration `yaml:"lookup_timeout"`
	// MaxHosts caps the number of cached hosts (least recently used evicted).
	MaxHosts int `yaml:"max_hosts"`
}

// Pool is a named group of replicas serving the same models.
type Pool struct {
	Name     string    `yaml:"name"`
	Replicas []Replica `yaml:"replicas"`
	// MaxInflightPerReplica is the admission capacity per replica: mirror
	// vLLM's --max-num-seqs. The pool admits this times its replica count
	// at once and queues the rest by priority class. 0 means no limit
	// (never queued). Read at startup.
	MaxInflightPerReplica int `yaml:"max_inflight_per_replica"`
	// PriorityScheduling says the pool's replicas run vLLM with
	// --scheduling-policy priority, so the gateway sends each request's
	// priority.upstream_values entry as the body's `priority` (ADR 0009).
	// Unflagged pools never receive a non-zero priority. Read at startup.
	PriorityScheduling bool `yaml:"priority_scheduling"`
	// MaxTokensCeiling is the most max_tokens the gateway sets on a matched
	// template request that carried no cap (ADR 0007). 0 means the gateway
	// never sets one for this pool. Read at startup.
	MaxTokensCeiling int `yaml:"max_tokens_ceiling"`
	// MaxModelLen is the --max-model-len of the pool's replicas. vLLM refuses
	// a request whose prompt tokens plus max_tokens exceed it, so with it set
	// the gateway sets no max_tokens on a request whose prompt could leave
	// less room than the cap (the request goes on as the caller sent it).
	// 0 means unknown: the cap is set whatever the prompt's size. Read at
	// startup.
	MaxModelLen int `yaml:"max_model_len"`
	// WeightsVersion names the model weights the pool serves. It is part of
	// every coalescing and cache key (ADR 0019), so changing it when new
	// weights are deployed makes earlier cached answers unreachable. Any
	// string up to MaxWeightsVersionLen bytes; empty is a valid value. Read
	// at startup.
	WeightsVersion string `yaml:"weights_version"`
}

// MaxWeightsVersionLen bounds pools[].weights_version.
const MaxWeightsVersionLen = 128

// Replica is one upstream vLLM (or mockllm) server.
type Replica struct {
	// Name is the metrics label for this replica; defaults to URL host:port.
	Name string `yaml:"name"`
	// URL is the replica's base URL, e.g. http://vllm-large-0:8000.
	URL string `yaml:"url"`
}

// Auth configures caller authentication.
type Auth struct {
	// Enabled turns bearer-token checking on. Disable only for local dev.
	Enabled bool     `yaml:"enabled"`
	Callers []Caller `yaml:"callers"`
}

// Caller is one calling service and its bearer token.
type Caller struct {
	Name string `yaml:"name"`
	// Token is the bearer token inline. Use only in a mounted secret file.
	Token Secret `yaml:"token"`
	// TokenEnv names an environment variable holding the token. Exactly one
	// of Token and TokenEnv must be set.
	TokenEnv string `yaml:"token_env"`
}

// Defaults returns a Config with every built-in default applied and no pools.
func Defaults() Config {
	return Config{
		Server: Server{
			ListenAddr:          DefaultListenAddr,
			MetricsListenAddr:   DefaultMetricsListenAddr,
			ReadHeaderTimeout:   DefaultReadHeaderTimeout,
			IdleTimeout:         DefaultIdleTimeout,
			MaxRequestBodyBytes: DefaultMaxRequestBodyBytes,
		},
		Shutdown:  Shutdown{DrainTimeout: DefaultDrainTimeout},
		Discovery: Discovery{ReloadInterval: DefaultReloadInterval},
		Upstream: Upstream{
			MaxIdleConnsPerHost: DefaultMaxIdleConnsPerHost,
			IdleConnTimeout:     DefaultUpstreamIdleConnTimeout,
			ConnectRetryBackoff: DefaultConnectRetryBackoff,
			Timeouts: UpstreamTimeouts{
				Connect:              DefaultConnectTimeout,
				ResponseHeader:       DefaultResponseHeaderTimeout,
				StreamResponseHeader: DefaultStreamResponseHeaderTimeout,
				FirstChunk:           DefaultFirstChunkTimeout,
				IdleBetweenChunks:    DefaultIdleBetweenChunksTimeout,
				Total:                DefaultTotalTimeout,
			},
			DNS: UpstreamDNS{
				TTL:           DefaultDNSTTL,
				MaxStale:      DefaultDNSMaxStale,
				LookupTimeout: DefaultDNSLookupTimeout,
				MaxHosts:      DefaultDNSMaxHosts,
			},
		},
		Auth:   Auth{Enabled: DefaultAuthEnabled},
		Router: defaultRouter(),
		Priority: PriorityConfig{
			Classes:           []string{PriorityInteractive, PriorityUser, PriorityBackground},
			DefaultClass:      PriorityUser,
			AnonymousMaxClass: PriorityUser,
			DefaultMaxClass:   PriorityUser,
			UpstreamValues:    DefaultUpstreamPriorities(),
			Probe:             PriorityProbe{Interval: DefaultPriorityProbeInterval, Timeout: DefaultPriorityProbeTimeout},
		},
		Admission:  defaultAdmission(),
		Templates:  defaultTemplates(),
		Coalescing: defaultCoalescing(),
		Cache:      defaultCache(),
		Context:    defaultContextBudget(),
		Admin:      defaultAdmin(),
	}
}

// Load builds the configuration from the YAML file named by GATEWAY_CONFIG
// (optional) and GATEWAY_* environment variables, then validates it.
// getenv is typically os.Getenv; an empty value counts as unset.
func Load(getenv func(string) string) (*Config, error) {
	var data []byte
	if path := getenv(EnvConfigPath); path != "" {
		b, err := readFile(path)
		if err != nil {
			return nil, fmt.Errorf("config: %w", err)
		}
		data = b
	}
	return Parse(data, getenv)
}

// Parse builds the configuration from YAML bytes (may be empty) and
// environment overrides, then validates it.
func Parse(data []byte, getenv func(string) string) (*Config, error) {
	cfg := Defaults()
	if err := decodeYAML(data, &cfg); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	if err := applyEnv(&cfg, getenv); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	// Report resolve and validation problems together, so one restart shows
	// everything that needs fixing.
	if err := errors.Join(resolve(&cfg, getenv), cfg.Validate()); err != nil {
		return nil, fmt.Errorf("config: invalid configuration:\n%w", err)
	}
	return &cfg, nil
}

// PoolForModel returns the pool for a request's model name and whether the
// model was explicitly mapped. Unmapped models get the default pool.
func (c *Config) PoolForModel(model string) (pool string, mapped bool) {
	if p, ok := c.Models[model]; ok {
		return p, true
	}
	return c.DefaultPool, false
}

// PoolForModelBytes is PoolForModel over a raw UTF-8 model slice without
// allocating a string on the heap.
func (c *Config) PoolForModelBytes(model []byte) (pool string, mapped bool) {
	if p, ok := c.Models[string(model)]; ok {
		return p, true
	}
	return c.DefaultPool, false
}

func readFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s (from %s): %w", path, EnvConfigPath, err)
	}
	defer func() { _ = f.Close() }()
	data, err := io.ReadAll(io.LimitReader(f, maxFileBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	if len(data) > maxFileBytes {
		return nil, fmt.Errorf("read %s: file exceeds %d bytes", path, maxFileBytes)
	}
	return data, nil
}

// decodeYAML overlays data onto cfg. Unknown keys are rejected so typos fail
// fast instead of silently falling back to defaults.
func decodeYAML(data []byte, cfg *Config) error {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return fmt.Errorf("parse yaml: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("parse yaml: multiple documents are not supported")
	}
	return nil
}
