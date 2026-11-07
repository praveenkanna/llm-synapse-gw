package config

import (
	"strings"
	"testing"
	"time"
)

func dedupConfig() *Config {
	return &Config{
		Coalescing: defaultCoalescing(),
		Cache:      defaultCache(),
		Context:    defaultContextBudget(),
	}
}

func TestDedupDefaultsValidate(t *testing.T) {
	c := dedupConfig()
	c.Cache.Enabled, c.Context.Enabled = true, true
	if errs := c.validateDedup(); len(errs) != 0 {
		t.Fatalf("defaults rejected: %v", errs)
	}
}

func TestDedupValidation(t *testing.T) {
	cases := map[string]struct {
		mutate func(*Config)
		want   string
	}{
		"flights zero":      {func(c *Config) { c.Coalescing.MaxFlights = 0 }, "coalescing.max_flights"},
		"flights huge":      {func(c *Config) { c.Coalescing.MaxFlights = MaxCoalesceMaxFlights + 1 }, "coalescing.max_flights"},
		"subscribers zero":  {func(c *Config) { c.Coalescing.MaxSubscribers = 0 }, "coalescing.max_subscribers"},
		"replay tiny":       {func(c *Config) { c.Coalescing.MaxReplayBytes = 10 }, "coalescing.max_replay_bytes"},
		"replay huge":       {func(c *Config) { c.Coalescing.MaxReplayBytes = MaxCoalesceMaxReplayBytes + 1 }, "coalescing.max_replay_bytes"},
		"key body zero":     {func(c *Config) { c.Coalescing.MaxKeyBodyBytes = 0 }, "coalescing.max_key_body_bytes"},
		"weights too long":  {func(c *Config) { c.Pools = []Pool{{WeightsVersion: strings.Repeat("v", MaxWeightsVersionLen+1)}} }, "pools[0].weights_version"},
		"cache addr":        {func(c *Config) { c.Cache.Enabled, c.Cache.Redis.Addr = true, "" }, "cache.redis.addr"},
		"cache timeout 0":   {func(c *Config) { c.Cache.Enabled, c.Cache.Timeout = true, 0 }, "cache.timeout"},
		"cache timeout big": {func(c *Config) { c.Cache.Enabled, c.Cache.Timeout = true, 2*time.Second }, "cache.timeout"},
		"cache ttl none":    {func(c *Config) { c.Cache.Enabled, c.Cache.MaxTTL = true, 0 }, "cache.max_ttl"},
		"cache ttl huge":    {func(c *Config) { c.Cache.Enabled, c.Cache.MaxTTL = true, 30*24*time.Hour }, "cache.max_ttl"},
		"cache queue":       {func(c *Config) { c.Cache.Enabled, c.Cache.WriteQueue = true, 0 }, "cache.write_queue"},
		"cache workers":     {func(c *Config) { c.Cache.Enabled, c.Cache.WriteWorkers = true, 0 }, "cache.write_workers"},
		"cache prefix":      {func(c *Config) { c.Cache.Enabled, c.Cache.KeyPrefix = true, "a b" }, "cache.key_prefix"},
		"cache password":    {func(c *Config) { c.Cache.Enabled, c.Cache.Redis.Password = true, "a b" }, "cache.redis.password"},
		"cache entry big":   {func(c *Config) { c.Cache.Enabled, c.Cache.MaxEntryBytes = true, c.Coalescing.MaxReplayBytes+1 }, "cache.max_entry_bytes"},
		"context probes":    {func(c *Config) { c.Context.Enabled, c.Context.MaxProbes = true, 0 }, "context.max_probes"},
		"context timeout":   {func(c *Config) { c.Context.Enabled, c.Context.TokenizeTimeout = true, 0 }, "context.tokenize_timeout"},
		"context assemble":  {func(c *Config) { c.Context.Enabled, c.Context.AssembleTimeout = true, 2*time.Minute }, "context.assemble_timeout"},
		"context assemble below tokenize": {func(c *Config) {
			c.Context.Enabled, c.Context.TokenizeTimeout, c.Context.AssembleTimeout = true, 10*time.Second, 4*time.Second
		}, "context.assemble_timeout"},
		"context body": {func(c *Config) { c.Context.Enabled, c.Context.MaxBodyBytes = true, 1 }, "context.max_body_bytes"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			c := dedupConfig()
			tc.mutate(c)
			var msgs []string
			for _, e := range c.validateDedup() {
				msgs = append(msgs, e.Error())
			}
			if !strings.Contains(strings.Join(msgs, "\n"), tc.want) {
				t.Fatalf("want an error naming %s, got %v", tc.want, msgs)
			}
		})
	}
}

func TestDedupCacheOffIgnoresItsKnobs(t *testing.T) {
	c := dedupConfig()
	c.Cache.Redis.Addr, c.Cache.Timeout = "", 0 // junk, but the cache is off
	if errs := c.validateDedup(); len(errs) != 0 {
		t.Fatalf("a disabled cache must not be validated: %v", errs)
	}
}
