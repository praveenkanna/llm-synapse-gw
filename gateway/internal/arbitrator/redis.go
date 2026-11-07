package arbitrator

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/redis"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// Fail-open reasons of the Redis demand sync (stage telemetry.StageAdmissionRedis).
const (
	ReasonRedisTimeout = "redis_timeout"
)

// Store publishes one gateway's demand on a pool and returns the pool's
// total demand over the gateways with a live lease, this one included.
type Store interface {
	Sync(ctx context.Context, pool, member string, demand int, lease time.Duration) (total, members int, err error)
}

// BatchStore is an optional interface a Store may implement to sync multiple
// pools in a single pipelined round-trip (P5.1).
type BatchStore interface {
	SyncBatch(ctx context.Context, member string, demands []PoolDemand, lease time.Duration) ([]PoolResult, error)
}

// PoolDemand is one pool's demand sent in a batch sync.
type PoolDemand struct {
	Pool string
	Own  int
}

// PoolResult is one pool's sync outcome.
type PoolResult struct {
	Total   int
	Members int
	Err     error
}

// FailOpener counts a background fail-open (*telemetry.Metrics).
type FailOpener interface {
	FailOpen(stage, reason string)
}

// CoordinatorConfig configures a Coordinator.
type CoordinatorConfig struct {
	// Member names this gateway in Redis; unique per process.
	Member string
	// Interval is the sync period; Timeout bounds one Redis call.
	Interval, Timeout time.Duration
	// LeaseTTL is how long a published demand counts, and how long without
	// a good sync before this gateway falls back to local admission.
	LeaseTTL time.Duration
	Logger   *slog.Logger
}

// Coordinator splits each pool's capacity among the gateways (ADR 0004).
// Every Interval it publishes this gateway's demand (ShareDemand: slots in
// use plus queued, each class capped at what it may hold) per pool with a lease, reads the pool's total, and sets this
// gateway's share on the Arbitrator. The request path never waits on it:
// admission always decides from local state. When Redis fails for longer
// than LeaseTTL, the share is cleared and the gateway admits up to the
// pool's full capacity on its own (fail open), counted once per sync round
// as gateway_failopen_total{stage="admission_redis"}.
type Coordinator struct {
	cfg      CoordinatorConfig
	arb      *Arbitrator
	store    Store
	failOpen FailOpener
	now      func() time.Time

	lastGood map[string]time.Time // by pool; only the sync goroutine
	local    map[string]bool      // pools in local fallback
	cancel   context.CancelFunc
	done     chan struct{}
}

// NewCoordinator builds a Coordinator; Start runs it.
func NewCoordinator(cfg CoordinatorConfig, arb *Arbitrator, store Store, failOpen FailOpener) *Coordinator {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	return &Coordinator{cfg: cfg, arb: arb, store: store, failOpen: failOpen, now: time.Now,
		lastGood: make(map[string]time.Time), local: make(map[string]bool)}
}

// Start syncs every Interval until Stop or ctx ends.
func (c *Coordinator) Start(ctx context.Context) {
	ctx, c.cancel = context.WithCancel(ctx)
	c.done = make(chan struct{})
	start := c.now()
	for _, p := range c.arb.Pools() {
		c.lastGood[p] = start // a lease's grace from startup
	}
	go func() {
		defer close(c.done)
		t := time.NewTicker(c.cfg.Interval)
		defer t.Stop()
		for {
			c.Sync(ctx)
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
}

// Stop ends the sync loop and waits for it. The shares stay as they were;
// the process is stopping.
func (c *Coordinator) Stop() {
	if c.cancel == nil {
		return
	}
	c.cancel()
	<-c.done
}

// Sync runs one round over every pool with a slot limit.
func (c *Coordinator) Sync(ctx context.Context) {
	type activePool struct {
		name  string
		limit int
		own   int
	}
	var active []activePool
	for _, p := range c.arb.Pools() {
		limit := c.arb.Capacity(p)
		if limit >= 0 {
			active = append(active, activePool{name: p, limit: limit, own: c.arb.ShareDemand(p)})
		}
	}
	if len(active) == 0 {
		return
	}

	failReason := ""
	if bstore, ok := c.store.(BatchStore); ok && len(active) > 1 {
		demands := make([]PoolDemand, len(active))
		for i, a := range active {
			demands[i] = PoolDemand{Pool: a.name, Own: a.own}
		}
		cctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
		results, err := bstore.SyncBatch(cctx, c.cfg.Member, demands, c.cfg.LeaseTTL)
		timedOut := cctx.Err() != nil
		cancel()
		if ctx.Err() != nil {
			return
		}
		now := c.now()
		if err != nil {
			failReason = telemetry.ReasonRedisError
			if timedOut {
				failReason = ReasonRedisTimeout
			}
			for _, a := range active {
				if last, ok := c.lastGood[a.name]; !ok {
					c.lastGood[a.name] = now
				} else if now.Sub(last) > c.cfg.LeaseTTL && !c.local[a.name] {
					c.local[a.name] = true
					c.arb.SetShare(a.name, -1)
					c.cfg.Logger.Warn("admission falling back to local limits", "pool", a.name, "err", err)
				}
			}
		} else {
			for i, res := range results {
				p := active[i].name
				limit := active[i].limit
				own := active[i].own
				if res.Err != nil {
					failReason = telemetry.ReasonRedisError
					if last, ok := c.lastGood[p]; !ok {
						c.lastGood[p] = now
					} else if now.Sub(last) > c.cfg.LeaseTTL && !c.local[p] {
						c.local[p] = true
						c.arb.SetShare(p, -1)
						c.cfg.Logger.Warn("admission falling back to local limits", "pool", p, "err", res.Err)
					}
					continue
				}
				c.lastGood[p] = now
				if c.local[p] {
					delete(c.local, p)
					c.cfg.Logger.Info("admission shared through Redis again", "pool", p)
				}
				c.arb.SetShare(p, shareOf(limit, own, res.Total, res.Members))
			}
		}
	} else {
		for _, a := range active {
			p := a.name
			limit := a.limit
			own := a.own
			cctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
			total, members, err := c.store.Sync(cctx, p, c.cfg.Member, own, c.cfg.LeaseTTL)
			timedOut := cctx.Err() != nil
			cancel()
			if ctx.Err() != nil {
				return
			}
			now := c.now()
			if err != nil {
				failReason = telemetry.ReasonRedisError
				if timedOut {
					failReason = ReasonRedisTimeout
				}
				if last, ok := c.lastGood[p]; !ok {
					c.lastGood[p] = now
				} else if now.Sub(last) > c.cfg.LeaseTTL && !c.local[p] {
					c.local[p] = true
					c.arb.SetShare(p, -1)
					c.cfg.Logger.Warn("admission falling back to local limits", "pool", p, "err", err)
				}
				continue
			}
			c.lastGood[p] = now
			if c.local[p] {
				delete(c.local, p)
				c.cfg.Logger.Info("admission shared through Redis again", "pool", p)
			}
			c.arb.SetShare(p, shareOf(limit, own, total, members))
		}
	}
	if failReason != "" && c.failOpen != nil {
		c.failOpen.FailOpen(telemetry.StageAdmissionRedis, failReason)
	}
}

// shareOf is one gateway's slots out of limit, given its own demand and the
// total over members gateways. Under the limit every gateway gets its
// demand plus an even split of the spare slots; over it, slots go in
// proportion to demand. Every gateway keeps at least one slot so a first
// request is never held for a sync round. Gateways compute their shares
// from snapshots one sync interval apart, so while demand moves the
// shares can briefly sum to more than limit (68 of 64 slots held, limits
// summing to 72, in the M2-08 proof); the replica queues the excess.
func shareOf(limit, own, total, members int) int {
	if members <= 0 || total < own {
		total, members = max(total, own), max(members, 1)
	}
	if total <= limit {
		return max(1, own+(limit-total)/members)
	}
	return max(1, limit*own/total)
}

// demandScript leases member's demand on a pool and returns the pool's
// live total and member count. Expiry uses the Redis clock, so gateways
// need no clock agreement. KEYS: members ZSET (score = lease expiry, ms),
// demand HASH. ARGV: member, demand, lease ms.
const demandScript = `
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local lease = tonumber(ARGV[3])
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, 1000)
for _, m in ipairs(expired) do
  redis.call('ZREM', KEYS[1], m)
  redis.call('HDEL', KEYS[2], m)
end
redis.call('ZADD', KEYS[1], now + lease, ARGV[1])
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('PEXPIRE', KEYS[1], lease * 2)
redis.call('PEXPIRE', KEYS[2], lease * 2)
local vals = redis.call('HVALS', KEYS[2])
local total = 0
for _, v in ipairs(vals) do total = total + tonumber(v) end
return {total, #vals}
`

var demand = redis.NewScript(demandScript)

// RedisStore is the Store on a Redis server. Keys hash-tag the pool, so a
// pool's two keys stay on one Redis Cluster slot.
type RedisStore struct {
	Client *redis.Client
	Prefix string
}

// Sync implements Store.
func (s RedisStore) Sync(ctx context.Context, pool, member string, own int, lease time.Duration) (int, int, error) {
	base := s.Prefix + ":{" + pool + "}:"
	v, err := s.Client.Eval(ctx, demand, []string{base + "members", base + "demand"},
		member, strconv.Itoa(own), strconv.FormatInt(max(lease.Milliseconds(), 1), 10))
	if err != nil {
		return 0, 0, err
	}
	if v.Kind != redis.KindArray || len(v.Array) != 2 || v.Array[0].Kind != redis.KindInt || v.Array[1].Kind != redis.KindInt {
		return 0, 0, fmt.Errorf("admission sync: unexpected reply %+v", v)
	}
	return int(v.Array[0].Int), int(v.Array[1].Int), nil
}

// SyncBatch implements BatchStore, pipelining all pools' demand scripts in 1 RTT (P5.1).
func (s RedisStore) SyncBatch(ctx context.Context, member string, demands []PoolDemand, lease time.Duration) ([]PoolResult, error) {
	if len(demands) == 0 {
		return nil, nil
	}
	cmds := make([][]string, len(demands))
	leaseMs := strconv.FormatInt(max(lease.Milliseconds(), 1), 10)
	for i, d := range demands {
		base := s.Prefix + ":{" + d.Pool + "}:"
		cmds[i] = redis.EvalArgs("EVALSHA", demand.SHA(), []string{base + "members", base + "demand"},
			[]string{member, strconv.Itoa(d.Own), leaseMs})
	}
	replies, err := s.Client.DoPipeline(ctx, cmds)
	if err != nil {
		return nil, err
	}
	results := make([]PoolResult, len(demands))
	for i, v := range replies {
		if v.Kind == redis.KindError {
			// If NOSCRIPT, fallback to individual Sync which loads the script
			tot, mems, ferr := s.Sync(ctx, demands[i].Pool, member, demands[i].Own, lease)
			results[i] = PoolResult{Total: tot, Members: mems, Err: ferr}
			continue
		}
		if v.Kind != redis.KindArray || len(v.Array) != 2 || v.Array[0].Kind != redis.KindInt || v.Array[1].Kind != redis.KindInt {
			results[i] = PoolResult{Err: fmt.Errorf("admission sync: unexpected reply %+v", v)}
			continue
		}
		results[i] = PoolResult{Total: int(v.Array[0].Int), Members: int(v.Array[1].Int)}
	}
	return results, nil
}
