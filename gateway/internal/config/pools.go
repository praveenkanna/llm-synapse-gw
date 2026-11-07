package config

import (
	"maps"
	"slices"
)

// Facts derived from the pool and model settings. Each is derived here,
// once, and the gateway's packages use these rather than deriving their own
// (ADR 0022).

// PoolNames returns the configured pool names, in configuration order.
func (c *Config) PoolNames() []string {
	names := make([]string, 0, len(c.Pools))
	for _, p := range c.Pools {
		names = append(names, p.Name)
	}
	return names
}

// PriorityPools returns the pools with priority_scheduling, as a set (empty
// when none is flagged).
func (c *Config) PriorityPools() map[string]bool {
	set := make(map[string]bool)
	for _, p := range c.Pools {
		if p.PriorityScheduling {
			set[p.Name] = true
		}
	}
	return set
}

// WeightsVersions returns each pool's weights_version (pool name -> version;
// "" when unset), part of a coalescing or cache key.
func (c *Config) WeightsVersions() map[string]string {
	v := make(map[string]string, len(c.Pools))
	for _, p := range c.Pools {
		v[p.Name] = p.WeightsVersion
	}
	return v
}

// ProbeModels returns the probe-model rule over the current settings, for
// the stuck-engine probe and the priority probe alike.
func (c *Config) ProbeModels() ProbeModels {
	mapped := make(map[string]string)
	for _, model := range slices.Sorted(maps.Keys(c.Models)) {
		if pool := c.Models[model]; mapped[pool] == "" {
			mapped[pool] = model
		}
	}
	return ProbeModels{pinned: maps.Clone(c.Router.Health.StuckProbeModels), mapped: mapped}
}

// ProbeModels picks the model a probe of a replica sends. It is a snapshot
// of the configuration it came from: later changes do not reach it.
type ProbeModels struct {
	pinned map[string]string // router.health.stuck_probe_models
	mapped map[string]string // pool -> its first model in models, by name
}

// For returns the model to probe a replica of pool with, where served is
// what the replica lists on /v1/models (nil while unknown): the pool's
// router.health.stuck_probe_models entry, else the first served model, else
// the pool's first mapped model by name, else "" (the probe fails open).
func (p ProbeModels) For(pool string, served []string) string {
	if m := p.pinned[pool]; m != "" {
		return m
	}
	if len(served) > 0 {
		return served[0]
	}
	return p.mapped[pool]
}
