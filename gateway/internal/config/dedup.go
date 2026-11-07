package config

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
)

// Coalescing, response cache and budgeted context defaults (M4, ADR 0019).
const (
	DefaultCoalesceMaxFlights      = 1024
	DefaultCoalesceMaxSubscribers  = 256
	DefaultCoalesceMaxReplayBytes  = 1 << 20
	DefaultCoalesceMaxKeyBodyBytes = 256 << 10

	DefaultCacheRedisAddr    = "redis:6379"
	DefaultCacheConnections  = 16
	DefaultCacheTimeout      = 20 * time.Millisecond
	DefaultCacheMaxTTL       = time.Hour
	DefaultCacheMaxEntry     = 512 << 10
	DefaultCacheWriteQueue   = 256
	DefaultCacheWriteWorkers = 2
	DefaultCacheKeyPrefix    = "gw:cache"

	DefaultContextMaxProbes       = 12
	DefaultContextTokenizeTimeout = 2 * time.Second
	DefaultContextAssembleTimeout = 2 * DefaultContextTokenizeTimeout
	DefaultContextMaxBodyBytes    = 4 << 20

	// Hard limits on the knobs above, so a typo cannot turn a bound into
	// no bound.
	MaxCoalesceMaxFlights     = 1 << 20
	MaxCoalesceMaxSubscribers = 1 << 16
	// MinCoalesceMaxReplayBytes keeps the one-piece slack on the replay
	// bound small next to it (coalesce.MinReplayBytes).
	MinCoalesceMaxReplayBytes  = 16 << 10
	MaxCoalesceMaxReplayBytes  = 64 << 20
	MaxCoalesceMaxKeyBodyBytes = 32 << 20
	MaxCacheConnections        = 64
	MaxCacheTimeout            = time.Second
	MaxCacheMaxTTL             = 7 * 24 * time.Hour
	MaxCacheMaxEntry           = 16 << 20
	MaxCacheWriteQueue         = 1 << 16
	MaxCacheWriteWorkers       = 64
	MaxCacheKeyPrefixLen       = 64
	MaxContextMaxProbes        = 64
	MaxContextTokenizeTimeout  = 30 * time.Second
	MaxContextAssembleTimeout  = time.Minute
	MaxContextMaxBodyBytes     = 32 << 20
)

// Coalescing bounds in-flight coalescing (ADR 0019). Which requests merge
// is decided per template (coalesce: true); these only bound the state.
type Coalescing struct {
	// MaxFlights is how many upstream calls may be shared at once; a
	// request over it runs its own call.
	MaxFlights int `yaml:"max_flights"`
	// MaxSubscribers is how many requests one flight serves besides its
	// leader; one over it runs its own call.
	MaxSubscribers int `yaml:"max_subscribers"`
	// MaxReplayBytes is the most response bytes a flight keeps for late
	// joiners and slow subscribers. A response over it seals the flight.
	MaxReplayBytes int `yaml:"max_replay_bytes"`
	// MaxKeyBodyBytes is the largest request body a key is computed for;
	// a larger one runs as an ordinary request.
	MaxKeyBodyBytes int `yaml:"max_key_body_bytes"`
}

// Cache configures the Redis response cache (ADR 0019). Which responses
// are stored is decided per template (cacheable: true and a cache_ttl).
type Cache struct {
	// Enabled turns the cache on. It is off by default: it needs Redis.
	Enabled bool `yaml:"enabled"`
	// Redis is the cache's own connection, separate from admission's.
	Redis CacheRedis `yaml:"redis"`
	// Connections is the size of the cache's connection pool.
	Connections int `yaml:"connections"`
	// Timeout bounds one lookup or store; a slower Redis is a miss.
	Timeout time.Duration `yaml:"timeout"`
	// MaxTTL caps a template's cache_ttl.
	MaxTTL time.Duration `yaml:"max_ttl"`
	// MaxEntryBytes is the largest response stored.
	MaxEntryBytes int `yaml:"max_entry_bytes"`
	// WriteQueue is how many stores may wait for a worker; a full queue
	// drops the store.
	WriteQueue int `yaml:"write_queue"`
	// WriteWorkers is how many stores run at once.
	WriteWorkers int    `yaml:"write_workers"`
	KeyPrefix    string `yaml:"key_prefix"`
}

// CacheRedis is the cache's Redis connection.
type CacheRedis struct {
	Addr string `yaml:"addr"`
	// Username and Password authenticate (Redis ACL). Prefer
	// GATEWAY_CACHE_REDIS_PASSWORD for the password.
	Username string `yaml:"username"`
	Password Secret `yaml:"password"`
	DB       int    `yaml:"db"`
}

// ContextBudget configures budgeted context assembly (Level 3, ADR 0019).
// A request opts in with X-Context-Budget.
type ContextBudget struct {
	// Enabled turns Level 3 on; off, the header is ignored.
	Enabled bool `yaml:"enabled"`
	// MaxProbes is the most /tokenize calls one request may cost.
	MaxProbes int `yaml:"max_probes"`
	// TokenizeTimeout bounds one /tokenize call.
	TokenizeTimeout time.Duration `yaml:"tokenize_timeout"`
	// AssembleTimeout bounds one request's whole assembly, every probe
	// included.
	AssembleTimeout time.Duration `yaml:"assemble_timeout"`
	// MaxBodyBytes is the largest body the assembler reads; a larger one
	// is forwarded unchanged.
	MaxBodyBytes int `yaml:"max_body_bytes"`
}

func defaultCoalescing() Coalescing {
	return Coalescing{
		MaxFlights:      DefaultCoalesceMaxFlights,
		MaxSubscribers:  DefaultCoalesceMaxSubscribers,
		MaxReplayBytes:  DefaultCoalesceMaxReplayBytes,
		MaxKeyBodyBytes: DefaultCoalesceMaxKeyBodyBytes,
	}
}

func defaultCache() Cache {
	return Cache{
		Redis:         CacheRedis{Addr: DefaultCacheRedisAddr},
		Connections:   DefaultCacheConnections,
		Timeout:       DefaultCacheTimeout,
		MaxTTL:        DefaultCacheMaxTTL,
		MaxEntryBytes: DefaultCacheMaxEntry,
		WriteQueue:    DefaultCacheWriteQueue,
		WriteWorkers:  DefaultCacheWriteWorkers,
		KeyPrefix:     DefaultCacheKeyPrefix,
	}
}

func defaultContextBudget() ContextBudget {
	return ContextBudget{
		MaxProbes:       DefaultContextMaxProbes,
		TokenizeTimeout: DefaultContextTokenizeTimeout,
		AssembleTimeout: DefaultContextAssembleTimeout,
		MaxBodyBytes:    DefaultContextMaxBodyBytes,
	}
}

func inRange(errs []error, key string, v, lo, hi int) []error {
	if v < lo || v > hi {
		return append(errs, fmt.Errorf("%s: %d out of range [%d, %d]", key, v, lo, hi))
	}
	return errs
}

func (c *Config) validateDedup() []error {
	var errs []error
	co := c.Coalescing
	errs = inRange(errs, "coalescing.max_flights", co.MaxFlights, 1, MaxCoalesceMaxFlights)
	errs = inRange(errs, "coalescing.max_subscribers", co.MaxSubscribers, 1, MaxCoalesceMaxSubscribers)
	errs = inRange(errs, "coalescing.max_replay_bytes", co.MaxReplayBytes, MinCoalesceMaxReplayBytes, MaxCoalesceMaxReplayBytes)
	errs = inRange(errs, "coalescing.max_key_body_bytes", co.MaxKeyBodyBytes, 1<<10, MaxCoalesceMaxKeyBodyBytes)

	for i, p := range c.Pools {
		if len(p.WeightsVersion) > MaxWeightsVersionLen {
			errs = append(errs, fmt.Errorf("pools[%d].weights_version: %d bytes, over the limit of %d", i, len(p.WeightsVersion), MaxWeightsVersionLen))
		}
	}

	ca := c.Cache
	if ca.Enabled {
		errs = append(errs, checkListenAddr("cache.redis.addr", ca.Redis.Addr)...)
		errs = inRange(errs, "cache.redis.db", ca.Redis.DB, 0, 15)
		errs = inRange(errs, "cache.connections", ca.Connections, 1, MaxCacheConnections)
		if ca.Timeout <= 0 || ca.Timeout > MaxCacheTimeout {
			errs = append(errs, fmt.Errorf("cache.timeout: %s out of range (0s, %s]", ca.Timeout, MaxCacheTimeout))
		}
		if ca.MaxTTL < time.Second || ca.MaxTTL > MaxCacheMaxTTL {
			errs = append(errs, fmt.Errorf("cache.max_ttl: %s out of range [1s, %s]", ca.MaxTTL, MaxCacheMaxTTL))
		}
		errs = inRange(errs, "cache.max_entry_bytes", ca.MaxEntryBytes, 1<<10, MaxCacheMaxEntry)
		errs = inRange(errs, "cache.write_queue", ca.WriteQueue, 1, MaxCacheWriteQueue)
		errs = inRange(errs, "cache.write_workers", ca.WriteWorkers, 1, MaxCacheWriteWorkers)
		if p := ca.KeyPrefix; p == "" || len(p) > MaxCacheKeyPrefixLen || strings.ContainsFunc(p, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) || r == '{' || r == '}' }) {
			errs = append(errs, fmt.Errorf("cache.key_prefix: %q must be 1 to %d characters without spaces, control characters or braces", p, MaxCacheKeyPrefixLen))
		}
		if strings.ContainsFunc(ca.Redis.Password.Reveal(), unicode.IsSpace) {
			errs = append(errs, errors.New("cache.redis.password: must not contain whitespace"))
		}
		// A stored entry must fit a flight's replay buffer, or nothing is
		// ever stored.
		if ca.MaxEntryBytes > co.MaxReplayBytes {
			errs = append(errs, fmt.Errorf("cache.max_entry_bytes: %d is above coalescing.max_replay_bytes %d, the most a flight records", ca.MaxEntryBytes, co.MaxReplayBytes))
		}
	}

	cb := c.Context
	if cb.Enabled {
		errs = inRange(errs, "context.max_probes", cb.MaxProbes, 1, MaxContextMaxProbes)
		if cb.TokenizeTimeout <= 0 || cb.TokenizeTimeout > MaxContextTokenizeTimeout {
			errs = append(errs, fmt.Errorf("context.tokenize_timeout: %s out of range (0s, %s]", cb.TokenizeTimeout, MaxContextTokenizeTimeout))
		}
		if cb.AssembleTimeout <= 0 || cb.AssembleTimeout > MaxContextAssembleTimeout {
			errs = append(errs, fmt.Errorf("context.assemble_timeout: %s out of range (0s, %s]", cb.AssembleTimeout, MaxContextAssembleTimeout))
		} else if cb.AssembleTimeout < cb.TokenizeTimeout {
			errs = append(errs, fmt.Errorf("context.assemble_timeout: %s is below context.tokenize_timeout %s, which it would silently cap", cb.AssembleTimeout, cb.TokenizeTimeout))
		}
		errs = inRange(errs, "context.max_body_bytes", cb.MaxBodyBytes, 1<<10, MaxContextMaxBodyBytes)
	}
	return errs
}
