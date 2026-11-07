package config

import (
	"reflect"
	"testing"
)

func poolsTestConfig() *Config {
	c := Defaults()
	c.Pools = []Pool{{Name: "small"}, {Name: "large", PriorityScheduling: true}, {Name: "batch", PriorityScheduling: true}}
	// Several models per pool, so a pick by map order rather than by name
	// fails almost every run.
	c.Models = map[string]string{"zeta": "small", "mu": "small", "kappa": "small", "delta": "small",
		"alpha": "small", "omega": "small", "theta": "small", "beta": "small", "big": "large"}
	c.Router.Health.StuckProbeModels = map[string]string{"batch": "pinned"}
	return &c
}

func TestPoolNamesInConfigOrder(t *testing.T) {
	if got := poolsTestConfig().PoolNames(); !reflect.DeepEqual(got, []string{"small", "large", "batch"}) {
		t.Fatalf("PoolNames = %v, want config order", got)
	}
}

func TestPriorityPools(t *testing.T) {
	c := poolsTestConfig()
	if got := c.PriorityPools(); !reflect.DeepEqual(got, map[string]bool{"large": true, "batch": true}) {
		t.Fatalf("PriorityPools = %v, want the pools with priority_scheduling", got)
	}
	c.Pools = []Pool{{Name: "small"}}
	if got := c.PriorityPools(); got == nil || len(got) != 0 {
		t.Fatalf("PriorityPools with none flagged = %v, want an empty set", got)
	}
}

// TestProbeModelRule: one rule for every probe (C4): the pool's pinned
// model, then the replica's first served model, then the pool's first
// mapped model by name, else none.
func TestProbeModelRule(t *testing.T) {
	c := poolsTestConfig()
	models := c.ProbeModels()
	for _, tc := range []struct {
		name, pool string
		served     []string
		want       string
	}{
		{"pinned wins over served", "batch", []string{"served"}, "pinned"},
		{"served before mapped", "small", []string{"s1", "s2"}, "s1"},
		{"first mapped by name", "small", nil, "alpha"},
		{"empty served list", "large", []string{}, "big"},
		{"nothing known", "unmapped", nil, ""},
	} {
		if got := models.For(tc.pool, tc.served); got != tc.want {
			t.Errorf("%s: For(%q, %v) = %q, want %q", tc.name, tc.pool, tc.served, got, tc.want)
		}
	}
	// A snapshot: later changes to the configuration do not reach it.
	c.Router.Health.StuckProbeModels["batch"] = "changed"
	c.Models["aaa"] = "small"
	if got := models.For("batch", nil); got != "pinned" {
		t.Errorf("pinned model after a config change = %q, want the snapshot's", got)
	}
	if got := models.For("small", nil); got != "alpha" {
		t.Errorf("mapped model after a config change = %q, want the snapshot's", got)
	}
}

func TestWeightsVersions(t *testing.T) {
	c := poolsTestConfig()
	c.Pools[1].WeightsVersion = "w2"
	if got := c.WeightsVersions(); !reflect.DeepEqual(got, map[string]string{"small": "", "large": "w2", "batch": ""}) {
		t.Fatalf("WeightsVersions = %v, want every pool with its weights_version", got)
	}
}
