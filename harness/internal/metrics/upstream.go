package metrics

// vLLM counter names, current first. The V1 engine exports
// vllm:prefix_cache_{queries,hits} (in tokens) and vllm:num_preemptions; the
// Prometheus client adds the _total suffix. Older releases named the prefix
// counters vllm:gpu_prefix_cache_*, as mockllm's legacy compat mode does.
// mockllm mirrors the V1 names but has no preemption counter, which is
// reported as not exported.
var (
	namesPrefixQueries = []string{"vllm:prefix_cache_queries_total", "vllm:prefix_cache_queries", "vllm:gpu_prefix_cache_queries_total", "vllm:gpu_prefix_cache_queries"}
	namesPrefixHits    = []string{"vllm:prefix_cache_hits_total", "vllm:prefix_cache_hits", "vllm:gpu_prefix_cache_hits_total", "vllm:gpu_prefix_cache_hits"}
	namesPreemptions   = []string{"vllm:num_preemptions_total", "vllm:num_preemptions"}
	namesPromptTokens  = []string{"vllm:prompt_tokens_total", "vllm:prompt_tokens"}
	namesGenTokens     = []string{"vllm:generation_tokens_total", "vllm:generation_tokens"}
	namesSuccess       = []string{"vllm:request_success_total", "vllm:request_success"}
)

// Counter is the change of one upstream counter over a run.
type Counter struct {
	Delta    float64 `json:"delta"`
	Exported bool    `json:"exported"`        // false: the upstream does not export it
	Reset    bool    `json:"reset,omitempty"` // counter went down (upstream restarted); Delta is the after value
	Name     string  `json:"name,omitempty"`  // the metric name found
}

func counterDelta(before, after Snapshot, names []string) Counter {
	a, name, okA := after.SumAny(names...)
	if !okA {
		return Counter{}
	}
	b, _ := before.Sum(name)
	c := Counter{Exported: true, Name: name, Delta: a - b}
	if a < b {
		c.Delta, c.Reset = a, true
	}
	return c
}

func (c Counter) plus(o Counter) Counter {
	switch {
	case !o.Exported:
		return c
	case !c.Exported:
		return o
	}
	return Counter{Delta: c.Delta + o.Delta, Exported: true, Reset: c.Reset || o.Reset, Name: c.Name}
}

// Upstream is what one upstream (or the sum over all of them) did during a run.
type Upstream struct {
	Target             string   `json:"target"`
	PrefixCacheQueries Counter  `json:"prefix_cache_queries"` // tokens looked up
	PrefixCacheHits    Counter  `json:"prefix_cache_hits"`    // tokens found cached
	PrefixCacheHitRate *float64 `json:"prefix_cache_hit_rate,omitempty"`
	Preemptions        Counter  `json:"preemptions"`
	PromptTokens       Counter  `json:"prompt_tokens"`
	GenerationTokens   Counter  `json:"generation_tokens"`
	RequestSuccess     Counter  `json:"request_success"`
	Error              string   `json:"error,omitempty"` // scrape failed; counters are empty
}

// Diff computes an upstream's counter changes between two scrapes.
func Diff(target string, before, after Snapshot) Upstream {
	u := Upstream{
		Target:             target,
		PrefixCacheQueries: counterDelta(before, after, namesPrefixQueries),
		PrefixCacheHits:    counterDelta(before, after, namesPrefixHits),
		Preemptions:        counterDelta(before, after, namesPreemptions),
		PromptTokens:       counterDelta(before, after, namesPromptTokens),
		GenerationTokens:   counterDelta(before, after, namesGenTokens),
		RequestSuccess:     counterDelta(before, after, namesSuccess),
	}
	u.setHitRate()
	return u
}

func (u *Upstream) setHitRate() {
	u.PrefixCacheHitRate = nil
	if u.PrefixCacheQueries.Exported && u.PrefixCacheHits.Exported && u.PrefixCacheQueries.Delta > 0 {
		r := u.PrefixCacheHits.Delta / u.PrefixCacheQueries.Delta
		u.PrefixCacheHitRate = &r
	}
}

// Total sums several upstreams; the hit rate is recomputed from the summed
// token counts, so larger replicas weigh more. Failed scrapes are skipped and
// named in Error.
func Total(ups []Upstream) Upstream {
	t := Upstream{Target: "total"}
	for _, u := range ups {
		if u.Error != "" {
			if t.Error != "" {
				t.Error += "; "
			}
			t.Error += u.Target + ": " + u.Error
			continue
		}
		t.PrefixCacheQueries = t.PrefixCacheQueries.plus(u.PrefixCacheQueries)
		t.PrefixCacheHits = t.PrefixCacheHits.plus(u.PrefixCacheHits)
		t.Preemptions = t.Preemptions.plus(u.Preemptions)
		t.PromptTokens = t.PromptTokens.plus(u.PromptTokens)
		t.GenerationTokens = t.GenerationTokens.plus(u.GenerationTokens)
		t.RequestSuccess = t.RequestSuccess.plus(u.RequestSuccess)
	}
	t.setHitRate()
	return t
}
