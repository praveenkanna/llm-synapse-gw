package config

import (
	"fmt"
	"maps"
	"math"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// Router defaults. Router health and the load view take their sections as
// they are (ADR 0022); the ring and the key scan still keep copies of
// theirs, which cmd/gateway tests keep equal.
const (
	DefaultRouterPolicy        = "affinity"
	DefaultRouterEpsilon       = 0.25
	DefaultRouterOverflowSlack = 1
	// DefaultRouterKeyScanBudget mirrors affinity.DefaultKeyScanBudget.
	DefaultRouterKeyScanBudget = 40 << 10
	// DefaultRouterVirtualNodes mirrors ring.DefaultVirtualNodes.
	DefaultRouterVirtualNodes = 512

	DefaultHealthPollInterval            = time.Second
	DefaultHealthPollTimeout             = time.Second
	DefaultHealthFailureThreshold        = 2
	DefaultHealthSuccessThreshold        = 1
	DefaultHealthPassiveFailureThreshold = 3
	DefaultHealthStuckEngineTimeout      = 30 * time.Second
	DefaultHealthStuckEngineStreams      = 3
	DefaultHealthStuckEngineRatio        = 0.5
	DefaultHealthStuckEngineWindow       = time.Minute
	DefaultHealthStuckEngineMaxWaits     = 65536
	DefaultHealthMinEjection             = 5 * time.Second
	DefaultHealthMaxEjection             = time.Minute
	DefaultHealthEjectionBackoff         = 2.0
	DefaultHealthStuckProbeEnabled       = true
	DefaultHealthStuckProbeTimeout       = 10 * time.Second
	DefaultHealthStuckProbePath          = StuckProbePathCompletions

	DefaultLoadViewScrapeInterval = 5 * time.Second
	DefaultLoadViewScrapeTimeout  = 3 * time.Second
	DefaultLoadViewProbeInterval  = 60 * time.Second
	DefaultLoadViewStaleThreshold = 15 * time.Second
)

// Generation probe paths (router.health.stuck_probe_path), which router
// health sends.
const (
	StuckProbePathCompletions     = "/v1/completions"
	StuckProbePathChatCompletions = "/v1/chat/completions"
)

// Logical load-view signals, keys of router.load_view.metric_aliases and
// the names the load view extracts.
const (
	SignalWaitingRequests    = "waiting_requests"
	SignalRunningRequests    = "running_requests"
	SignalKVCacheUsage       = "kv_cache_usage_perc"
	SignalPrefixCacheQueries = "prefix_cache_queries"
	SignalPrefixCacheHits    = "prefix_cache_hits"
)

// Router validation limits.
const (
	// MinRouterInterval bounds the health poll, scrape and probe intervals
	// so a unit typo ("5ms") cannot turn them into busy loops against replicas.
	MinRouterInterval = 100 * time.Millisecond
	// MaxHealthThreshold bounds the consecutive-result thresholds.
	MaxHealthThreshold = 100
	// MinStuckEngineTimeout bounds router.health.stuck_engine_timeout from
	// below: the stall scan runs every timeout/10 (at least 100ms), and a
	// first token later than a second is routine for a busy replica.
	MinStuckEngineTimeout = time.Second
	// MinStuckEngineWindow and MaxStuckEngineWindow bound
	// router.health.stuck_engine_window.
	MinStuckEngineWindow = time.Second
	MaxStuckEngineWindow = time.Hour
	// MaxStuckEngineMaxWaits bounds router.health.stuck_engine_max_waits.
	// A registered wait costs roughly 250 bytes (the wait, ~150 bytes, plus
	// its registry entry), so the default 65536 is ~16 MiB and the ceiling
	// 1Mi ~256 MiB.
	MaxStuckEngineMaxWaits = 1 << 20
	// MaxMetricAliases bounds the candidate names per signal.
	MaxMetricAliases = 16
	// MinEjectionFloor and MaxEjectionCeiling bound router.health.min_ejection
	// and max_ejection: below a second an ejection damps nothing, and past
	// an hour a recovered replica sits idle for no reason.
	MinEjectionFloor   = time.Second
	MaxEjectionCeiling = time.Hour
	// MaxEjectionBackoff bounds router.health.ejection_backoff_multiplier.
	MaxEjectionBackoff = 10.0
	// MaxRouterOverflowSlack bounds router.overflow_slack: a larger slack
	// would let a hot conversation pile far above the bounded-load cap.
	MaxRouterOverflowSlack = 16
	// MinRouterKeyScanBudget and MaxRouterKeyScanBudget bound
	// router.key_scan_budget: below the minimum even a chat body fails open,
	// and the maximum only stops a typo. It does not protect the 100 µs
	// route-decision target: at the maximum the costliest bodies reach 97 µs
	// p99 on an Apple M1, so a slower core misses it (M1-26 audit, row 15).
	// The default holds it and an operator calibrates anything higher (ADR
	// 0017); M1-23's reference host decides whether the maximum drops.
	MinRouterKeyScanBudget = 1 << 10
	MaxRouterKeyScanBudget = 128 << 10
	// MinRouterVirtualNodes and MaxRouterVirtualNodes bound
	// router.virtual_nodes: fewer skews a small pool's arcs, more only
	// costs memory and ring rebuild time.
	MinRouterVirtualNodes = 64
	MaxRouterVirtualNodes = 2048
	// MaxKVPenaltyWeight bounds router.load_view.kv_penalty_weight.
	MaxKVPenaltyWeight = 1024
	// MinStuckProbeTimeout and MaxStuckProbeTimeout bound
	// router.health.stuck_probe_timeout: a one-token completion on an idle
	// replica takes well under a second, and a probe held for minutes only
	// delays re-admission.
	MinStuckProbeTimeout = time.Second
	MaxStuckProbeTimeout = 5 * time.Minute
	// MaxStuckProbeModelLen bounds a router.health.stuck_probe_models value.
	MaxStuckProbeModelLen = MaxModelNameBytes
)

// metricNamePattern is the Prometheus metric name syntax.
var metricNamePattern = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)

// RouterConfig configures gateway request routing policy.
type RouterConfig struct {
	// Policy is the routing algorithm: "affinity" (default) or "round_robin".
	Policy string `yaml:"policy"`
	// Epsilon is the bounded-load headroom ε (default 0.25). A replica takes
	// an affinity request only if load + 1 ≤ cap + OverflowSlack, where
	// cap = ⌈(1+ε)·(total+1)/n⌉, load is the replica's gateway in-flight plus
	// vLLM's waiting requests (while scraped metrics are fresh) and total is
	// the pool's load before placement. Must be finite and greater than zero.
	Epsilon float64 `yaml:"epsilon"`
	// OverflowSlack is how many requests a replica may hold above the cap
	// before affinity overflows to the next replica (ADR 0015; default 1,
	// 0..16). 0 is the strict bound, which costs conversation stickiness at
	// low load because loads are integers.
	OverflowSlack int `yaml:"overflow_slack"`
	// KeyScanBudget is the work, in units, the affinity key scan may spend
	// reading one request body before it fails open (reason key_scan_budget;
	// default 40960). A unit costs about 1 ns, so the default bounds the scan
	// near 40 µs on the core it was sized on; lower it on slower cores so the
	// route decision stays within 100 µs p99 (ADR 0017).
	KeyScanBudget int `yaml:"key_scan_budget"`
	// VirtualNodes is the hash ring's virtual nodes per replica (default
	// 512, 64..2048). More nodes even out the replicas' arc shares (128
	// left a 3-replica pool 1.15x skewed) at a couple of binary-search
	// steps per route.
	VirtualNodes int `yaml:"virtual_nodes"`
	// Health configures active /health polling and replica ejection.
	Health RouterHealth `yaml:"health"`
	// LoadView configures vLLM /metrics scraping and capability probing.
	LoadView LoadView `yaml:"load_view"`
}

// RouterHealth configures the replica health tracker.
type RouterHealth struct {
	// PollInterval is how often every replica's /health is polled.
	PollInterval time.Duration `yaml:"poll_interval"`
	// PollTimeout bounds one /health request; at most PollInterval.
	PollTimeout time.Duration `yaml:"poll_timeout"`
	// FailureThreshold is the consecutive failed polls that eject a replica.
	FailureThreshold int `yaml:"failure_threshold"`
	// SuccessThreshold is the consecutive good polls that re-admit it.
	SuccessThreshold int `yaml:"success_threshold"`
	// PassiveFailureThreshold is the requests failing hard to connect
	// (refused, host or network unreachable, NXDOMAIN; caller still
	// connected) that eject a replica between polls; a passing poll starts
	// the count over. Dial timeouts, cancellations, other timeouts and
	// failures after the request was sent never count.
	PassiveFailureThreshold int `yaml:"passive_failure_threshold"`
	// StuckEngineTimeout is how long a streaming attempt may wait for its
	// first chunk before the wait counts as a stall, unless a stream that
	// started no later was answered within the last half of it (arrival
	// order: the queue is moving), and taken back if its first chunk comes
	// after all. Must be below upstream.timeouts.total
	// and, when set, upstream.timeouts.first_chunk: a stream cut off
	// earlier is never judged.
	StuckEngineTimeout time.Duration `yaml:"stuck_engine_timeout"`
	// StuckEngineStreams is the least stalls on one replica within
	// StuckEngineWindow (or stalled streams still open, if more) that eject
	// it as a stuck engine.
	StuckEngineStreams int `yaml:"stuck_engine_streams"`
	// StuckEngineRatio is the least share of stalls among stalls plus
	// first chunks on the replica within StuckEngineWindow that ejects, in
	// (0, 1]: a busy replica keeps sending first chunks, a stuck one doesn't.
	StuckEngineRatio float64 `yaml:"stuck_engine_ratio"`
	// StuckEngineWindow is the sliding window stalls and first chunks are
	// counted over.
	StuckEngineWindow time.Duration `yaml:"stuck_engine_window"`
	// StuckEngineMaxWaits caps the first-chunk waits tracked at once; a
	// stream past the cap is not watched (fail open, counted).
	StuckEngineMaxWaits int `yaml:"stuck_engine_max_waits"`
	// MinEjection is how long an ejected replica stays out at least, whatever
	// the reason: it is re-admitted by the first good poll (success_threshold
	// in a row) after that time, never before (after a stuck_engine
	// ejection, also only once its generation probe passes).
	MinEjection time.Duration `yaml:"min_ejection"`
	// MaxEjection caps the ejection time. A replica that stays in for this
	// long after re-admission starts over at MinEjection.
	MaxEjection time.Duration `yaml:"max_ejection"`
	// EjectionBackoffMultiplier multiplies the ejection time on each
	// ejection in a row (1 keeps it at MinEjection).
	EjectionBackoffMultiplier float64 `yaml:"ejection_backoff_multiplier"`
	// StuckProbeEnabled gates re-admission after a stuck_engine ejection on
	// a generation probe (a streaming one-token completion with a fixed
	// synthetic prompt) on top of the rules above; off, /health alone
	// re-admits, as for every other reason.
	StuckProbeEnabled bool `yaml:"stuck_probe_enabled"`
	// StuckProbeTimeout bounds one generation probe, until it shows
	// generation. At most StuckEngineTimeout while the probe is on: a
	// backlogged but working replica can time a probe out, which only
	// extends its ejection (costs capacity, never correctness).
	StuckProbeTimeout time.Duration `yaml:"stuck_probe_timeout"`
	// StuckProbePath is the path probed: "/v1/completions" (default; any
	// generative model) or "/v1/chat/completions" (models with a chat
	// template only).
	StuckProbePath string `yaml:"stuck_probe_path"`
	// StuckProbeModels names the model probed per pool (pool name -> model),
	// by the stuck-engine and the priority probes. A pool not named here
	// probes the first model its replica listed on /v1/models to the load
	// view, else its first model in models (by name); with none, the probe
	// fails open (ProbeModels). Read at startup.
	StuckProbeModels map[string]string `yaml:"stuck_probe_models"`
}

// LoadView configures the per-replica load view.
type LoadView struct {
	// ScrapeInterval is how often every replica's /metrics is scraped.
	ScrapeInterval time.Duration `yaml:"scrape_interval"`
	// ScrapeTimeout bounds one /metrics, /version or /v1/models request; at
	// most ScrapeInterval.
	ScrapeTimeout time.Duration `yaml:"scrape_timeout"`
	// ProbeInterval is how often /version and /v1/models are probed.
	ProbeInterval time.Duration `yaml:"probe_interval"`
	// StaleThreshold is the age past which scraped metrics are ignored and
	// routing falls back to in-flight counts; greater than ScrapeInterval.
	StaleThreshold time.Duration `yaml:"stale_threshold"`
	// MetricAliases maps each logical signal to candidate metric names, in
	// priority order (ADR 0011). YAML entries replace the default list of
	// the signals they name; unnamed signals keep their defaults.
	MetricAliases map[string][]string `yaml:"metric_aliases"`
	// KVHighWatermark is the KV-cache usage (0..1) above which a replica's
	// routing load gets a penalty while its metrics are fresh; 0 (default)
	// turns the penalty off. Must be below 1.
	KVHighWatermark float64 `yaml:"kv_high_watermark"`
	// KVPenaltyWeight is the load added at full KV-cache usage, scaled
	// linearly from the watermark; 0 (default) turns the penalty off.
	KVPenaltyWeight int `yaml:"kv_penalty_weight"`
}

// DefaultMetricAliases returns a fresh copy of the default alias table,
// covering current and previous vLLM metric names (ADR 0011).
func DefaultMetricAliases() map[string][]string {
	return map[string][]string{
		SignalWaitingRequests:    {"vllm:num_requests_waiting", "num_requests_waiting"},
		SignalRunningRequests:    {"vllm:num_requests_running", "num_requests_running"},
		SignalKVCacheUsage:       {"vllm:kv_cache_usage_perc", "vllm:gpu_cache_usage_perc", "gpu_cache_usage_perc", "kv_cache_usage_perc"},
		SignalPrefixCacheQueries: {"vllm:prefix_cache_queries_total", "vllm:gpu_prefix_cache_queries_total"},
		SignalPrefixCacheHits:    {"vllm:prefix_cache_hits_total", "vllm:gpu_prefix_cache_hits_total"},
	}
}

// defaultRouter returns the router section's defaults. The alias map is
// fresh on every call because YAML decoding merges into it.
func defaultRouter() RouterConfig {
	return RouterConfig{
		Policy:        DefaultRouterPolicy,
		Epsilon:       DefaultRouterEpsilon,
		OverflowSlack: DefaultRouterOverflowSlack,
		KeyScanBudget: DefaultRouterKeyScanBudget,
		VirtualNodes:  DefaultRouterVirtualNodes,
		Health: RouterHealth{
			PollInterval:              DefaultHealthPollInterval,
			PollTimeout:               DefaultHealthPollTimeout,
			FailureThreshold:          DefaultHealthFailureThreshold,
			SuccessThreshold:          DefaultHealthSuccessThreshold,
			PassiveFailureThreshold:   DefaultHealthPassiveFailureThreshold,
			StuckEngineTimeout:        DefaultHealthStuckEngineTimeout,
			StuckEngineStreams:        DefaultHealthStuckEngineStreams,
			StuckEngineRatio:          DefaultHealthStuckEngineRatio,
			StuckEngineWindow:         DefaultHealthStuckEngineWindow,
			StuckEngineMaxWaits:       DefaultHealthStuckEngineMaxWaits,
			MinEjection:               DefaultHealthMinEjection,
			MaxEjection:               DefaultHealthMaxEjection,
			EjectionBackoffMultiplier: DefaultHealthEjectionBackoff,
			StuckProbeEnabled:         DefaultHealthStuckProbeEnabled,
			StuckProbeTimeout:         DefaultHealthStuckProbeTimeout,
			StuckProbePath:            DefaultHealthStuckProbePath,
			StuckProbeModels:          map[string]string{},
		},
		LoadView: LoadView{
			ScrapeInterval: DefaultLoadViewScrapeInterval,
			ScrapeTimeout:  DefaultLoadViewScrapeTimeout,
			ProbeInterval:  DefaultLoadViewProbeInterval,
			StaleThreshold: DefaultLoadViewStaleThreshold,
			MetricAliases:  DefaultMetricAliases(),
		},
	}
}

func (c *Config) validateRouter() []error {
	var errs []error
	r := c.Router
	if r.Policy != "" && r.Policy != "affinity" && r.Policy != "round_robin" {
		errs = append(errs, fmt.Errorf("router.policy: %q must be \"affinity\" or \"round_robin\"", r.Policy))
	}
	// NaN and ±Inf would disable bounded load; 0 would leave no headroom at
	// all, so the hottest key could never stay on its replica.
	if e := r.Epsilon; math.IsNaN(e) || math.IsInf(e, 0) || e <= 0 {
		errs = append(errs, fmt.Errorf("router.epsilon: %v must be a finite number greater than 0 (default %v)", e, DefaultRouterEpsilon))
	}
	if r.OverflowSlack < 0 || r.OverflowSlack > MaxRouterOverflowSlack {
		errs = append(errs, fmt.Errorf("router.overflow_slack: %d out of range [0, %d] (default %d)", r.OverflowSlack, MaxRouterOverflowSlack, DefaultRouterOverflowSlack))
	}
	if r.KeyScanBudget < MinRouterKeyScanBudget || r.KeyScanBudget > MaxRouterKeyScanBudget {
		errs = append(errs, fmt.Errorf("router.key_scan_budget: %d out of range [%d, %d] (default %d)", r.KeyScanBudget, MinRouterKeyScanBudget, MaxRouterKeyScanBudget, DefaultRouterKeyScanBudget))
	}
	if r.VirtualNodes < MinRouterVirtualNodes || r.VirtualNodes > MaxRouterVirtualNodes {
		errs = append(errs, fmt.Errorf("router.virtual_nodes: %d out of range [%d, %d] (default %d)", r.VirtualNodes, MinRouterVirtualNodes, MaxRouterVirtualNodes, DefaultRouterVirtualNodes))
	}
	errs = append(errs, validateRouterHealth(r.Health)...)
	errs = append(errs, validateStuckProbe(r.Health, c.Pools)...)
	errs = append(errs, validateStuckEngineVsUpstream(r.Health, c.Upstream.Timeouts)...)
	errs = append(errs, validateLoadView(r.LoadView)...)
	return errs
}

func validateRouterHealth(h RouterHealth) []error {
	var errs []error
	errs = appendIfErr(errs, checkMinInterval("router.health.poll_interval", h.PollInterval))
	errs = appendIfErr(errs, checkPositive("router.health.poll_timeout", h.PollTimeout))
	if h.PollTimeout > h.PollInterval {
		errs = append(errs, fmt.Errorf("router.health.poll_timeout: %s exceeds router.health.poll_interval %s", h.PollTimeout, h.PollInterval))
	}
	for _, th := range []struct {
		key string
		n   int
	}{
		{"router.health.failure_threshold", h.FailureThreshold},
		{"router.health.success_threshold", h.SuccessThreshold},
		{"router.health.passive_failure_threshold", h.PassiveFailureThreshold},
		{"router.health.stuck_engine_streams", h.StuckEngineStreams},
	} {
		if th.n < 1 || th.n > MaxHealthThreshold {
			errs = append(errs, fmt.Errorf("%s: %d out of range [1, %d]", th.key, th.n, MaxHealthThreshold))
		}
	}
	if h.StuckEngineTimeout < MinStuckEngineTimeout {
		errs = append(errs, fmt.Errorf("router.health.stuck_engine_timeout: %s is below the minimum %s", h.StuckEngineTimeout, MinStuckEngineTimeout))
	}
	if r := h.StuckEngineRatio; math.IsNaN(r) || r <= 0 || r > 1 {
		errs = append(errs, fmt.Errorf("router.health.stuck_engine_ratio: %v out of range (0, 1] (default %v)", r, DefaultHealthStuckEngineRatio))
	}
	if h.StuckEngineWindow < MinStuckEngineWindow || h.StuckEngineWindow > MaxStuckEngineWindow {
		errs = append(errs, fmt.Errorf("router.health.stuck_engine_window: %s out of range [%s, %s] (default %s)", h.StuckEngineWindow, MinStuckEngineWindow, MaxStuckEngineWindow, DefaultHealthStuckEngineWindow))
	}
	if h.StuckEngineMaxWaits < 1 || h.StuckEngineMaxWaits > MaxStuckEngineMaxWaits {
		errs = append(errs, fmt.Errorf("router.health.stuck_engine_max_waits: %d out of range [1, %d] (default %d)", h.StuckEngineMaxWaits, MaxStuckEngineMaxWaits, DefaultHealthStuckEngineMaxWaits))
	}
	return append(errs, validateEjection(h)...)
}

// validateStuckEngineVsUpstream rejects upstream timeouts that end a
// stream before stuck-engine detection can judge it: such a stream is
// ended (and its wait with it) at or before stuck_engine_timeout, so no
// stall is ever counted and detection is silently off.
func validateStuckEngineVsUpstream(h RouterHealth, t UpstreamTimeouts) []error {
	var errs []error
	if t.FirstChunk > 0 && t.FirstChunk <= h.StuckEngineTimeout {
		errs = append(errs, fmt.Errorf("upstream.timeouts.first_chunk: %s must be 0 (bounded by total) or above router.health.stuck_engine_timeout %s, or stuck-engine detection never sees a stalled stream", t.FirstChunk, h.StuckEngineTimeout))
	}
	if t.Total > 0 && t.Total <= h.StuckEngineTimeout {
		errs = append(errs, fmt.Errorf("upstream.timeouts.total: %s must be above router.health.stuck_engine_timeout %s, or stuck-engine detection never sees a stalled stream", t.Total, h.StuckEngineTimeout))
	}
	return errs
}

// validateStuckProbe checks the generation probe settings; a
// stuck_probe_models key must name a configured pool.
func validateStuckProbe(h RouterHealth, pools []Pool) []error {
	var errs []error
	if h.StuckProbeTimeout < MinStuckProbeTimeout || h.StuckProbeTimeout > MaxStuckProbeTimeout {
		errs = append(errs, fmt.Errorf("router.health.stuck_probe_timeout: %s out of range [%s, %s] (default %s)", h.StuckProbeTimeout, MinStuckProbeTimeout, MaxStuckProbeTimeout, DefaultHealthStuckProbeTimeout))
	}
	if h.StuckProbeEnabled && h.StuckProbeTimeout > h.StuckEngineTimeout {
		errs = append(errs, fmt.Errorf("router.health.stuck_probe_timeout: %s exceeds router.health.stuck_engine_timeout %s: a replica slower than that is not stuck, and a longer probe only delays re-admission", h.StuckProbeTimeout, h.StuckEngineTimeout))
	}
	if h.StuckProbePath != StuckProbePathCompletions && h.StuckProbePath != StuckProbePathChatCompletions {
		errs = append(errs, fmt.Errorf("router.health.stuck_probe_path: %q must be %q or %q", h.StuckProbePath, StuckProbePathCompletions, StuckProbePathChatCompletions))
	}
	known := make(map[string]bool, len(pools))
	for _, p := range pools {
		known[p.Name] = true
	}
	for _, pool := range slices.Sorted(maps.Keys(h.StuckProbeModels)) {
		key := fmt.Sprintf("router.health.stuck_probe_models.%s", pool)
		if !known[pool] {
			errs = append(errs, fmt.Errorf("%s: no such pool", key))
		}
		if m := h.StuckProbeModels[pool]; !validModelName(m) {
			errs = append(errs, fmt.Errorf("%s: model name must be 1 to %d characters, not blank, without control characters", key, MaxStuckProbeModelLen))
		}
	}
	return errs
}

// validModelName reports whether m is a plausible served model name:
// not blank, bounded, without control characters. Spaces are allowed
// (vLLM's --served-model-name accepts them).
func validModelName(m string) bool {
	if strings.TrimSpace(m) == "" || len(m) > MaxStuckProbeModelLen || !utf8.ValidString(m) {
		return false
	}
	for _, r := range m {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func validateEjection(h RouterHealth) []error {
	var errs []error
	if h.MinEjection < MinEjectionFloor || h.MinEjection > MaxEjectionCeiling {
		errs = append(errs, fmt.Errorf("router.health.min_ejection: %s out of range [%s, %s] (default %s)", h.MinEjection, MinEjectionFloor, MaxEjectionCeiling, DefaultHealthMinEjection))
	}
	if h.MaxEjection < h.MinEjection || h.MaxEjection > MaxEjectionCeiling {
		errs = append(errs, fmt.Errorf("router.health.max_ejection: %s must be in [router.health.min_ejection %s, %s] (default %s)", h.MaxEjection, h.MinEjection, MaxEjectionCeiling, DefaultHealthMaxEjection))
	}
	if m := h.EjectionBackoffMultiplier; math.IsNaN(m) || m < 1 || m > MaxEjectionBackoff {
		errs = append(errs, fmt.Errorf("router.health.ejection_backoff_multiplier: %v out of range [1, %v] (default %v)", m, MaxEjectionBackoff, DefaultHealthEjectionBackoff))
	}
	return errs
}

func validateLoadView(lv LoadView) []error {
	var errs []error
	errs = appendIfErr(errs, checkMinInterval("router.load_view.scrape_interval", lv.ScrapeInterval))
	errs = appendIfErr(errs, checkMinInterval("router.load_view.probe_interval", lv.ProbeInterval))
	errs = appendIfErr(errs, checkPositive("router.load_view.scrape_timeout", lv.ScrapeTimeout))
	if lv.ScrapeTimeout > lv.ScrapeInterval {
		errs = append(errs, fmt.Errorf("router.load_view.scrape_timeout: %s exceeds router.load_view.scrape_interval %s", lv.ScrapeTimeout, lv.ScrapeInterval))
	}
	if lv.StaleThreshold <= lv.ScrapeInterval {
		errs = append(errs, fmt.Errorf("router.load_view.stale_threshold: %s must be greater than router.load_view.scrape_interval %s", lv.StaleThreshold, lv.ScrapeInterval))
	}
	if hw := lv.KVHighWatermark; math.IsNaN(hw) || hw < 0 || hw >= 1 {
		errs = append(errs, fmt.Errorf("router.load_view.kv_high_watermark: %v out of range [0, 1) (0 turns the KV penalty off)", hw))
	}
	if lv.KVPenaltyWeight < 0 || lv.KVPenaltyWeight > MaxKVPenaltyWeight {
		errs = append(errs, fmt.Errorf("router.load_view.kv_penalty_weight: %d out of range [0, %d] (0 turns the KV penalty off)", lv.KVPenaltyWeight, MaxKVPenaltyWeight))
	}
	return append(errs, validateMetricAliases(lv.MetricAliases)...)
}

func validateMetricAliases(aliases map[string][]string) []error {
	var errs []error
	known := DefaultMetricAliases()
	for _, sig := range slices.Sorted(maps.Keys(aliases)) {
		key := fmt.Sprintf("router.load_view.metric_aliases.%s", sig)
		if _, ok := known[sig]; !ok {
			errs = append(errs, fmt.Errorf("%s: unknown signal (known: %v)", key, slices.Sorted(maps.Keys(known))))
			continue
		}
		names := aliases[sig]
		if len(names) == 0 || len(names) > MaxMetricAliases {
			errs = append(errs, fmt.Errorf("%s: needs 1 to %d metric names, got %d", key, MaxMetricAliases, len(names)))
		}
		for i, n := range names {
			if !metricNamePattern.MatchString(n) {
				errs = append(errs, fmt.Errorf("%s[%d]: %q is not a valid Prometheus metric name", key, i, n))
			}
		}
	}
	for _, sig := range slices.Sorted(maps.Keys(known)) {
		if _, ok := aliases[sig]; !ok {
			errs = append(errs, fmt.Errorf("router.load_view.metric_aliases.%s: required", sig))
		}
	}
	return errs
}

func checkMinInterval(key string, d time.Duration) error {
	if d < MinRouterInterval {
		return fmt.Errorf("%s: %s is below the minimum %s", key, d, MinRouterInterval)
	}
	return nil
}
