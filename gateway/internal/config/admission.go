package config

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"time"
	"unicode"
)

// Admission defaults. Capacity and latency values are starting points to be
// calibrated by the harness (ADR 0005), not measured limits.
const (
	DefaultAdmissionEnabled       = true
	DefaultAdmissionMaxWait       = 10 * time.Second
	DefaultAdmissionMaxQueue      = 1024
	DefaultAdmissionRetryAfterMax = 30 * time.Second

	DefaultShedEnabled          = true
	DefaultShedTTFTP95Threshold = 2 * time.Second
	DefaultShedWindow           = 30 * time.Second
	DefaultShedMinSamples       = 20
	DefaultShedRecoverRatio     = 0.8

	DefaultAdmissionRedisEnabled      = false
	DefaultAdmissionRedisAddr         = "redis:6379"
	DefaultAdmissionRedisSyncInterval = 100 * time.Millisecond
	DefaultAdmissionRedisTimeout      = 25 * time.Millisecond
	DefaultAdmissionRedisLeaseTTL     = 2 * time.Second
	DefaultAdmissionRedisKeyPrefix    = "gw:adm"
)

// Admission validation limits.
const (
	// MaxAdmissionMaxWait bounds a class max wait: a caller held longer than
	// this is better told to retry.
	MaxAdmissionMaxWait = 10 * time.Minute
	// MaxAdmissionMaxQueue bounds one class queue. A waiter costs a few
	// hundred bytes plus the request body it holds.
	MaxAdmissionMaxQueue = 1 << 16
	// MaxPoolInflightPerReplica bounds pools[].max_inflight_per_replica
	// (vLLM's --max-num-seqs is at most a few thousand).
	MaxPoolInflightPerReplica = 1 << 16
	// MaxUpstreamPriority bounds a priority.upstream_values entry.
	MaxUpstreamPriority = 1 << 20

	// priority.probe defaults and bounds. The defaults are the load view's
	// probe interval and the stuck-engine probe's timeout, which the probe
	// used before it had settings of its own.
	DefaultPriorityProbeInterval = 60 * time.Second
	DefaultPriorityProbeTimeout  = 10 * time.Second
	MinPriorityProbeInterval     = time.Second
	MaxPriorityProbeInterval     = time.Hour
	MinPriorityProbeTimeout      = time.Second
	MaxPriorityProbeTimeout      = 5 * time.Minute
	// MinShedWindow and MaxShedWindow bound admission.shed.window.
	MinShedWindow = time.Second
	MaxShedWindow = 10 * time.Minute
	// MaxShedMinSamples bounds admission.shed.min_samples; the window keeps
	// at most shedMaxSamples samples (arbitrator), so more could never be met.
	MaxShedMinSamples = 4096
	// MinAdmissionRedisSyncInterval keeps the sync from hammering Redis.
	MinAdmissionRedisSyncInterval = 10 * time.Millisecond
	// MaxAdmissionRedisTimeout bounds one Redis call: the sync runs off the
	// request path, but a slow Redis must turn into local admission quickly.
	MaxAdmissionRedisTimeout = time.Second
	// MaxAdmissionRedisKeyPrefixLen bounds admission.redis.key_prefix.
	MaxAdmissionRedisKeyPrefixLen = 64
)

// Admission configures per-pool admission: slots, bounded priority queues,
// shedding and the cross-gateway view in Redis (M2). A pool's capacity is
// pools[].max_inflight_per_replica times its replica count; a pool without
// it is never queued.
type Admission struct {
	// Enabled turns admission on. Off, every request goes straight to routing.
	Enabled bool `yaml:"enabled"`
	// MaxWait is the longest a request of a class without an entry in
	// Classes waits in the queue before it is answered 503 + Retry-After.
	MaxWait time.Duration `yaml:"max_wait"`
	// MaxQueue is the most requests of a class without an entry in Classes
	// that wait at once per pool; past it they get 429 + Retry-After.
	MaxQueue int `yaml:"max_queue"`
	// Classes overrides MaxWait and MaxQueue per priority class.
	Classes map[string]AdmissionClass `yaml:"classes"`
	// RetryAfterMax caps the Retry-After sent with a 429 or 503.
	RetryAfterMax time.Duration `yaml:"retry_after_max"`
	// EarlyReject rejects arrivals with 503 + Retry-After immediately when the
	// EWMA slot-release interval predicts that requests queued in [0..rank]
	// will exceed the class MaxWait, avoiding pointless queue stalls under
	// sustained overload while keeping default behavior opt-in (false).
	EarlyReject bool `yaml:"early_reject"`
	// Shed stops admitting the lowest classes while the highest class's
	// time to first token is too high.
	Shed Shed `yaml:"shed"`
	// Redis shares each gateway's admission demand so the gateways together
	// stay within a pool's capacity (ADR 0004). Redis down: each gateway
	// admits locally up to the full capacity (fail open).
	Redis AdmissionRedis `yaml:"redis"`
}

// AdmissionClass is one class's queue bounds. Zero values take
// admission.max_wait and admission.max_queue.
type AdmissionClass struct {
	MaxWait  time.Duration `yaml:"max_wait"`
	MaxQueue int           `yaml:"max_queue"`
	// MaxShare is the largest fraction of a pool's slots the class may hold
	// at once, in (0, 1]; 0 means 1. Keeping long low-priority work below 1
	// leaves free slots for higher classes, so they need not wait for it.
	MaxShare float64 `yaml:"max_share"`
}

// Shed configures shedding on the highest class's TTFT p95.
type Shed struct {
	Enabled bool `yaml:"enabled"`
	// TTFTP95Threshold is the p95 time to first token of the highest
	// priority class (priority.classes[0]), over Window, above which the
	// Classes are shed: answered 429 + Retry-After instead of queued, and
	// their queued requests answered too. Shedding stops once the p95 falls
	// below TTFTP95Threshold × RecoverRatio.
	TTFTP95Threshold time.Duration `yaml:"ttft_p95_threshold"`
	Window           time.Duration `yaml:"window"`
	// MinSamples is the least samples in Window before the p95 counts;
	// fewer, and nothing is shed.
	MinSamples   int     `yaml:"min_samples"`
	RecoverRatio float64 `yaml:"recover_ratio"`
	// Classes are shed while the threshold is exceeded. Empty means the
	// lowest priority class. Must not name priority.classes[0].
	Classes []string `yaml:"classes"`
}

// AdmissionRedis configures the shared demand view.
type AdmissionRedis struct {
	Enabled bool   `yaml:"enabled"`
	Addr    string `yaml:"addr"`
	// Username and Password authenticate (Redis ACL). Prefer
	// GATEWAY_ADMISSION_REDIS_PASSWORD for the password.
	Username string `yaml:"username"`
	Password Secret `yaml:"password"`
	DB       int    `yaml:"db"`
	// SyncInterval is how often each gateway publishes its demand and reads
	// the others'.
	SyncInterval time.Duration `yaml:"sync_interval"`
	// Timeout bounds one Redis call; a slower or failed sync turns the
	// gateway to local admission until a sync succeeds.
	Timeout time.Duration `yaml:"timeout"`
	// LeaseTTL is how long a gateway's published demand counts after its
	// last sync, so a crashed gateway's slots expire. Also how long the last
	// good view is used after syncs start failing.
	LeaseTTL  time.Duration `yaml:"lease_ttl"`
	KeyPrefix string        `yaml:"key_prefix"`
}

// MaxWaitFor returns class's max wait.
func (a Admission) MaxWaitFor(class string) time.Duration {
	if c, ok := a.Classes[class]; ok && c.MaxWait > 0 {
		return c.MaxWait
	}
	return a.MaxWait
}

// MaxShareFor returns the share of a pool's slots class may hold, in (0, 1].
func (a Admission) MaxShareFor(class string) float64 {
	if c, ok := a.Classes[class]; ok && c.MaxShare > 0 {
		return c.MaxShare
	}
	return 1
}

// MaxQueueFor returns class's queue bound per pool.
func (a Admission) MaxQueueFor(class string) int {
	if c, ok := a.Classes[class]; ok && c.MaxQueue > 0 {
		return c.MaxQueue
	}
	return a.MaxQueue
}

// defaultAdmission returns the admission section's defaults. The class map
// is fresh on every call because YAML decoding merges into it.
func defaultAdmission() Admission {
	return Admission{
		Enabled:  DefaultAdmissionEnabled,
		MaxWait:  DefaultAdmissionMaxWait,
		MaxQueue: DefaultAdmissionMaxQueue,
		Classes: map[string]AdmissionClass{
			PriorityInteractive: {MaxWait: 2 * time.Second},
			PriorityBackground:  {MaxWait: 30 * time.Second, MaxQueue: 256, MaxShare: 0.5},
		},
		RetryAfterMax: DefaultAdmissionRetryAfterMax,
		Shed: Shed{
			Enabled:          DefaultShedEnabled,
			TTFTP95Threshold: DefaultShedTTFTP95Threshold,
			Window:           DefaultShedWindow,
			MinSamples:       DefaultShedMinSamples,
			RecoverRatio:     DefaultShedRecoverRatio,
		},
		Redis: AdmissionRedis{
			Enabled:      DefaultAdmissionRedisEnabled,
			Addr:         DefaultAdmissionRedisAddr,
			SyncInterval: DefaultAdmissionRedisSyncInterval,
			Timeout:      DefaultAdmissionRedisTimeout,
			LeaseTTL:     DefaultAdmissionRedisLeaseTTL,
			KeyPrefix:    DefaultAdmissionRedisKeyPrefix,
		},
	}
}

// DefaultUpstreamPriorities returns the default class -> vLLM priority map
// (lower is served sooner; ADR 0009). The default class takes vLLM's own
// default, 0, so the gateway adds nothing to its bodies.
func DefaultUpstreamPriorities() map[string]int {
	return map[string]int{PriorityInteractive: -10, PriorityUser: 0, PriorityBackground: 10}
}

// UpstreamPriority returns the vLLM priority for class (0 when unmapped).
func (p PriorityConfig) UpstreamPriority(class string) int {
	return p.UpstreamValues[class]
}

// ShedClasses returns the classes shed under admission.shed: the configured
// list, or the lowest priority class.
func (c *Config) ShedClasses() []string {
	if len(c.Admission.Shed.Classes) > 0 {
		return slices.Clone(c.Admission.Shed.Classes)
	}
	if n := len(c.Priority.Classes); n > 1 {
		return []string{c.Priority.Classes[n-1]}
	}
	return nil
}

func (c *Config) validateAdmission() []error {
	var errs []error
	a := c.Admission
	classSet := make(map[string]bool, len(c.Priority.Classes))
	for _, cl := range c.Priority.Classes {
		classSet[cl] = true
	}
	if a.MaxWait <= 0 || a.MaxWait > MaxAdmissionMaxWait {
		errs = append(errs, fmt.Errorf("admission.max_wait: %s out of range (0s, %s] (default %s)", a.MaxWait, MaxAdmissionMaxWait, DefaultAdmissionMaxWait))
	}
	if a.MaxQueue < 1 || a.MaxQueue > MaxAdmissionMaxQueue {
		errs = append(errs, fmt.Errorf("admission.max_queue: %d out of range [1, %d] (default %d)", a.MaxQueue, MaxAdmissionMaxQueue, DefaultAdmissionMaxQueue))
	}
	for _, name := range slices.Sorted(maps.Keys(a.Classes)) {
		key := "admission.classes." + name
		cl := a.Classes[name]
		if !classSet[name] {
			// A built-in default left over after priority.classes renamed
			// the classes is harmless; anything else is a typo.
			if def, ok := defaultAdmission().Classes[name]; !ok || cl != def {
				errs = append(errs, fmt.Errorf("%s: not in priority.classes", key))
			}
			continue
		}
		if cl.MaxWait < 0 || cl.MaxWait > MaxAdmissionMaxWait {
			errs = append(errs, fmt.Errorf("%s.max_wait: %s out of range [0s, %s] (0 means admission.max_wait)", key, cl.MaxWait, MaxAdmissionMaxWait))
		}
		if s := cl.MaxShare; math.IsNaN(s) || s < 0 || s > 1 {
			errs = append(errs, fmt.Errorf("%s.max_share: %v out of range [0, 1] (0 means 1)", key, s))
		}
		if cl.MaxQueue < 0 || cl.MaxQueue > MaxAdmissionMaxQueue {
			errs = append(errs, fmt.Errorf("%s.max_queue: %d out of range [0, %d] (0 means admission.max_queue)", key, cl.MaxQueue, MaxAdmissionMaxQueue))
		}
	}
	if a.RetryAfterMax < time.Second || a.RetryAfterMax > time.Hour {
		errs = append(errs, fmt.Errorf("admission.retry_after_max: %s out of range [1s, 1h] (default %s)", a.RetryAfterMax, DefaultAdmissionRetryAfterMax))
	}
	errs = append(errs, c.validateShed(classSet)...)
	errs = append(errs, validateAdmissionRedis(a.Redis)...)
	for i, p := range c.Pools {
		if p.MaxInflightPerReplica < 0 || p.MaxInflightPerReplica > MaxPoolInflightPerReplica {
			errs = append(errs, fmt.Errorf("pools[%d].max_inflight_per_replica: %d out of range [0, %d] (0 means no limit)", i, p.MaxInflightPerReplica, MaxPoolInflightPerReplica))
		}
	}
	return errs
}

func (c *Config) validateShed(classSet map[string]bool) []error {
	s := c.Admission.Shed
	if !s.Enabled {
		return nil
	}
	var errs []error
	if s.TTFTP95Threshold <= 0 {
		errs = append(errs, fmt.Errorf("admission.shed.ttft_p95_threshold: must be greater than zero, got %s", s.TTFTP95Threshold))
	}
	if s.Window < MinShedWindow || s.Window > MaxShedWindow {
		errs = append(errs, fmt.Errorf("admission.shed.window: %s out of range [%s, %s] (default %s)", s.Window, MinShedWindow, MaxShedWindow, DefaultShedWindow))
	}
	if s.MinSamples < 1 || s.MinSamples > MaxShedMinSamples {
		errs = append(errs, fmt.Errorf("admission.shed.min_samples: %d out of range [1, %d] (default %d)", s.MinSamples, MaxShedMinSamples, DefaultShedMinSamples))
	}
	if r := s.RecoverRatio; math.IsNaN(r) || r <= 0 || r > 1 {
		errs = append(errs, fmt.Errorf("admission.shed.recover_ratio: %v out of range (0, 1] (default %v)", r, DefaultShedRecoverRatio))
	}
	for i, cl := range s.Classes {
		key := fmt.Sprintf("admission.shed.classes[%d]", i)
		switch {
		case !classSet[cl]:
			errs = append(errs, fmt.Errorf("%s: %q is not in priority.classes", key, cl))
		case len(c.Priority.Classes) > 0 && cl == c.Priority.Classes[0]:
			errs = append(errs, fmt.Errorf("%s: %q is the highest priority class, whose latency shedding protects", key, cl))
		}
	}
	return errs
}

func validateAdmissionRedis(r AdmissionRedis) []error {
	if !r.Enabled {
		return nil
	}
	var errs []error
	errs = append(errs, checkListenAddr("admission.redis.addr", r.Addr)...)
	if r.DB < 0 || r.DB > 15 {
		errs = append(errs, fmt.Errorf("admission.redis.db: %d out of range [0, 15]", r.DB))
	}
	if r.SyncInterval < MinAdmissionRedisSyncInterval || r.SyncInterval > 10*time.Second {
		errs = append(errs, fmt.Errorf("admission.redis.sync_interval: %s out of range [%s, 10s] (default %s)", r.SyncInterval, MinAdmissionRedisSyncInterval, DefaultAdmissionRedisSyncInterval))
	}
	if r.Timeout <= 0 || r.Timeout > MaxAdmissionRedisTimeout || r.Timeout > r.SyncInterval {
		errs = append(errs, fmt.Errorf("admission.redis.timeout: %s must be in (0s, min(admission.redis.sync_interval, %s)] (default %s)", r.Timeout, MaxAdmissionRedisTimeout, DefaultAdmissionRedisTimeout))
	}
	if r.LeaseTTL < 2*r.SyncInterval || r.LeaseTTL > time.Minute {
		errs = append(errs, fmt.Errorf("admission.redis.lease_ttl: %s must be in [2 × admission.redis.sync_interval, 1m] (default %s)", r.LeaseTTL, DefaultAdmissionRedisLeaseTTL))
	}
	if p := r.KeyPrefix; p == "" || len(p) > MaxAdmissionRedisKeyPrefixLen || strings.ContainsFunc(p, func(c rune) bool { return unicode.IsSpace(c) || unicode.IsControl(c) || c == '{' || c == '}' }) {
		errs = append(errs, fmt.Errorf("admission.redis.key_prefix: %q must be 1 to %d characters without spaces, control characters or braces", p, MaxAdmissionRedisKeyPrefixLen))
	}
	if strings.ContainsFunc(r.Password.Reveal(), unicode.IsSpace) {
		errs = append(errs, errors.New("admission.redis.password: must not contain whitespace"))
	}
	return errs
}

// validateUpstreamPriority checks priority.upstream_values and the pools
// that send them: a flagged pool needs a value for every class.
func (c *Config) validateUpstreamPriority() []error {
	var errs []error
	classSet := make(map[string]bool, len(c.Priority.Classes))
	for _, cl := range c.Priority.Classes {
		classSet[cl] = true
	}
	for _, cl := range slices.Sorted(maps.Keys(c.Priority.UpstreamValues)) {
		v := c.Priority.UpstreamValues[cl]
		key := "priority.upstream_values." + cl
		if def, ok := DefaultUpstreamPriorities()[cl]; !classSet[cl] && (!ok || v != def) {
			errs = append(errs, fmt.Errorf("%s: not in priority.classes", key))
		}
		if v < -MaxUpstreamPriority || v > MaxUpstreamPriority {
			errs = append(errs, fmt.Errorf("%s: %d out of range [%d, %d]", key, v, -MaxUpstreamPriority, MaxUpstreamPriority))
		}
	}
	if len(c.PriorityPools()) == 0 {
		return errs
	}
	for _, cl := range c.Priority.Classes {
		if _, ok := c.Priority.UpstreamValues[cl]; !ok {
			errs = append(errs, fmt.Errorf("priority.upstream_values.%s: required while a pool sets priority_scheduling", cl))
		}
	}
	return errs
}
