package metrics

import (
	"strings"
	"testing"
)

func snap(t *testing.T, page string) Snapshot {
	t.Helper()
	s, err := Parse(strings.NewReader(page))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestDiff(t *testing.T) {
	tests := []struct {
		name          string
		before, after string
		check         func(t *testing.T, u Upstream)
	}{
		{
			name:   "vllm",
			before: "vllm:prefix_cache_queries_total 100\nvllm:prefix_cache_hits_total 40\nvllm:num_preemptions_total 1\nvllm:generation_tokens_total 10\n",
			after:  "vllm:prefix_cache_queries_total 1100\nvllm:prefix_cache_hits_total 840\nvllm:num_preemptions_total 4\nvllm:generation_tokens_total 510\n",
			check: func(t *testing.T, u Upstream) {
				if u.PrefixCacheHitRate == nil || *u.PrefixCacheHitRate != 0.8 {
					t.Errorf("hit rate = %v, want 0.8", u.PrefixCacheHitRate)
				}
				if !u.Preemptions.Exported || u.Preemptions.Delta != 3 {
					t.Errorf("preemptions = %+v", u.Preemptions)
				}
				if u.GenerationTokens.Delta != 500 || u.PromptTokens.Exported {
					t.Errorf("tokens = %+v / %+v", u.GenerationTokens, u.PromptTokens)
				}
			},
		},
		{
			name:   "mockllm has no preemption counter and never hits",
			before: "vllm:prefix_cache_queries_total{model_name=\"m\"} 0\nvllm:prefix_cache_hits_total{model_name=\"m\"} 0\n",
			after:  "vllm:prefix_cache_queries_total{model_name=\"m\"} 64\nvllm:prefix_cache_hits_total{model_name=\"m\"} 0\n",
			check: func(t *testing.T, u Upstream) {
				if u.Preemptions.Exported {
					t.Errorf("preemptions should be not exported: %+v", u.Preemptions)
				}
				if u.PrefixCacheHitRate == nil || *u.PrefixCacheHitRate != 0 {
					t.Errorf("hit rate = %v, want 0", u.PrefixCacheHitRate)
				}
			},
		},
		{
			name:   "older vllm and mockllm legacy mode: gpu_prefix_cache names",
			before: "vllm:gpu_prefix_cache_queries_total{model_name=\"m\"} 100\nvllm:gpu_prefix_cache_hits_total{model_name=\"m\"} 0\n",
			after:  "vllm:gpu_prefix_cache_queries_total{model_name=\"m\"} 500\nvllm:gpu_prefix_cache_hits_total{model_name=\"m\"} 300\n",
			check: func(t *testing.T, u Upstream) {
				if u.PrefixCacheHits.Name != "vllm:gpu_prefix_cache_hits_total" || u.PrefixCacheHitRate == nil || *u.PrefixCacheHitRate != 0.75 {
					t.Errorf("hits = %+v, hit rate = %v, want 0.75", u.PrefixCacheHits, u.PrefixCacheHitRate)
				}
			},
		},
		{
			name:   "no queries: no hit rate",
			before: "vllm:prefix_cache_queries_total 5\nvllm:prefix_cache_hits_total 5\n",
			after:  "vllm:prefix_cache_queries_total 5\nvllm:prefix_cache_hits_total 5\n",
			check: func(t *testing.T, u Upstream) {
				if u.PrefixCacheHitRate != nil {
					t.Errorf("hit rate = %v, want nil", *u.PrefixCacheHitRate)
				}
			},
		},
		{
			name:   "counter reset: after value is the delta",
			before: "vllm:num_preemptions_total 100\n",
			after:  "vllm:num_preemptions_total 7\n",
			check: func(t *testing.T, u Upstream) {
				if !u.Preemptions.Reset || u.Preemptions.Delta != 7 {
					t.Errorf("preemptions = %+v", u.Preemptions)
				}
			},
		},
		{
			name:   "older unsuffixed names",
			before: "vllm:num_preemptions 2\n",
			after:  "vllm:num_preemptions 5\n",
			check: func(t *testing.T, u Upstream) {
				if u.Preemptions.Delta != 3 || u.Preemptions.Name != "vllm:num_preemptions" {
					t.Errorf("preemptions = %+v", u.Preemptions)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.check(t, Diff("u", snap(t, tt.before), snap(t, tt.after)))
		})
	}
}

func TestTotal(t *testing.T) {
	a := Diff("a", snap(t, "vllm:prefix_cache_queries_total 0\nvllm:prefix_cache_hits_total 0\n"),
		snap(t, "vllm:prefix_cache_queries_total 300\nvllm:prefix_cache_hits_total 300\n"))
	b := Diff("b", snap(t, "vllm:prefix_cache_queries_total 0\nvllm:prefix_cache_hits_total 0\nvllm:num_preemptions_total 0\n"),
		snap(t, "vllm:prefix_cache_queries_total 100\nvllm:prefix_cache_hits_total 0\nvllm:num_preemptions_total 2\n"))
	failed := Upstream{Target: "c", Error: "status 500"}
	tot := Total([]Upstream{a, b, failed})
	if tot.PrefixCacheHitRate == nil || *tot.PrefixCacheHitRate != 0.75 {
		t.Errorf("total hit rate = %v, want 0.75 (token-weighted)", tot.PrefixCacheHitRate)
	}
	if !tot.Preemptions.Exported || tot.Preemptions.Delta != 2 {
		t.Errorf("total preemptions = %+v", tot.Preemptions)
	}
	if tot.Error != "c: status 500" {
		t.Errorf("total error = %q", tot.Error)
	}
	if empty := Total(nil); empty.PrefixCacheHitRate != nil || empty.Preemptions.Exported {
		t.Errorf("empty total = %+v", empty)
	}
}
