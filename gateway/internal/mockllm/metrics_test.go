package mockllm

import (
	"strings"
	"testing"
)

func TestMetricsExposition(t *testing.T) {
	m := newMetrics([]string{"b-model", `we"ird`, "b-model"}, 4, CompatCurrent)
	mm := m.models["b-model"]
	mm.running.Store(2)
	mm.waiting.Store(1)
	mm.promptTokens.Add(100)
	mm.prefixHits.Add(64)
	mm.genTokens.Add(40)
	mm.success(finishReasonStop)
	mm.success(finishReasonLength)
	mm.success(finishReasonLength)
	mm.ttft.observe(0.03)
	mm.ttft.observe(20) // beyond the last bucket
	mm.e2e.observe(1.2)
	m.cancelled.Add(3)
	m.faults[faultDrop].Add(1)

	var b strings.Builder
	if err := m.writePrometheus(&b); err != nil {
		t.Fatalf("writePrometheus: %v", err)
	}
	out := b.String()
	for _, want := range []string{
		`# TYPE vllm:num_requests_running gauge`,
		`vllm:num_requests_running{model_name="b-model"} 2`,
		`vllm:num_requests_waiting{model_name="b-model"} 1`,
		`vllm:kv_cache_usage_perc{model_name="b-model"} 0.5`,
		`vllm:prefix_cache_queries_total{model_name="b-model"} 100`,
		`vllm:prefix_cache_hits_total{model_name="b-model"} 64`,
		`vllm:prompt_tokens_total{model_name="b-model"} 100`,
		`vllm:generation_tokens_total{model_name="b-model"} 40`,
		`vllm:request_success_total{finished_reason="length",model_name="b-model"} 2`,
		`vllm:request_success_total{finished_reason="stop",model_name="b-model"} 1`,
		`# TYPE vllm:time_to_first_token_seconds histogram`,
		`vllm:time_to_first_token_seconds_bucket{model_name="b-model",le="0.02"} 0`,
		`vllm:time_to_first_token_seconds_bucket{model_name="b-model",le="0.04"} 1`,
		`vllm:time_to_first_token_seconds_bucket{model_name="b-model",le="10"} 1`,
		`vllm:time_to_first_token_seconds_bucket{model_name="b-model",le="+Inf"} 2`,
		`vllm:time_to_first_token_seconds_count{model_name="b-model"} 2`,
		`vllm:time_to_first_token_seconds_sum{model_name="b-model"} 20.03`,
		`vllm:e2e_request_latency_seconds_bucket{model_name="b-model",le="1.5"} 1`,
		`mockllm_cancelled_requests_total 3`,
		`mockllm_faults_injected_total{kind="drop"} 1`,
		`mockllm_requests_total{endpoint="chat"} 0`,
		`vllm:num_requests_running{model_name="we\"ird"} 0`,
	} {
		if !strings.Contains(out, want+"\n") {
			t.Errorf("exposition missing line %q", want)
		}
	}
	if n := strings.Count(out, `vllm:num_requests_running{model_name="b-model"}`); n != 1 {
		t.Errorf("duplicate model produced %d series, want 1", n)
	}
}

func TestKVUsageUnlimited(t *testing.T) {
	m := newMetrics([]string{"x"}, 0, CompatCurrent)
	m.models["x"].running.Store(50)
	if got := m.kvUsage(m.models["x"]); got != 0 {
		t.Fatalf("kvUsage with unlimited seqs = %v, want 0", got)
	}
	m = newMetrics([]string{"x"}, 2, CompatCurrent)
	m.models["x"].running.Store(5)
	if got := m.kvUsage(m.models["x"]); got != 1 {
		t.Fatalf("kvUsage over capacity = %v, want capped at 1", got)
	}
}

func TestMetricNameSetPerCompatMode(t *testing.T) {
	current := []string{
		`# TYPE vllm:kv_cache_usage_perc gauge`,
		`vllm:kv_cache_usage_perc{model_name="m"} 0.25`,
		`# TYPE vllm:prefix_cache_queries_total counter`,
		`vllm:prefix_cache_queries_total{model_name="m"} 12`,
		`# TYPE vllm:prefix_cache_hits_total counter`,
		`vllm:prefix_cache_hits_total{model_name="m"} 0`,
	}
	legacy := []string{
		`# TYPE vllm:gpu_cache_usage_perc gauge`,
		`vllm:gpu_cache_usage_perc{model_name="m"} 0.25`,
		`# TYPE vllm:gpu_prefix_cache_queries_total counter`,
		`vllm:gpu_prefix_cache_queries_total{model_name="m"} 12`,
		`# TYPE vllm:gpu_prefix_cache_hits_total counter`,
		`vllm:gpu_prefix_cache_hits_total{model_name="m"} 0`,
	}
	// Stable names are emitted in both modes.
	stable := []string{
		`vllm:num_requests_running{model_name="m"} 1`,
		`vllm:num_requests_waiting{model_name="m"} 0`,
		`vllm:prompt_tokens_total{model_name="m"} 12`,
	}
	tests := []struct {
		mode    VLLMCompat
		want    []string
		absentN []string // metric-name prefixes no family or sample may start with
	}{
		{mode: CompatCurrent, want: current, absentN: []string{"vllm:gpu_cache_usage_perc", "vllm:gpu_prefix_cache_"}},
		{mode: CompatLegacy, want: legacy, absentN: []string{"vllm:kv_cache_usage_perc", "vllm:prefix_cache_"}},
	}
	for _, tc := range tests {
		t.Run(string(tc.mode), func(t *testing.T) {
			m := newMetrics([]string{"m"}, 4, tc.mode)
			m.models["m"].running.Store(1)
			m.models["m"].promptTokens.Add(12)
			var b strings.Builder
			if err := m.writePrometheus(&b); err != nil {
				t.Fatalf("writePrometheus: %v", err)
			}
			out := b.String()
			for _, want := range append(append([]string{}, tc.want...), stable...) {
				if !strings.Contains(out, want+"\n") {
					t.Errorf("exposition missing line %q", want)
				}
			}
			for _, name := range tc.absentN {
				// Match on the metric name at the start of each line, since
				// "vllm:prefix_cache_" is a substring of "vllm:gpu_prefix_cache_".
				for _, line := range strings.Split(out, "\n") {
					fields := strings.Fields(strings.TrimPrefix(strings.TrimPrefix(line, "# HELP "), "# TYPE "))
					if len(fields) > 0 && strings.HasPrefix(fields[0], name) {
						t.Errorf("%s mode emitted %q", tc.mode, line)
					}
				}
			}
		})
	}
}

func TestMetricNamesForUnknownModeIsCurrent(t *testing.T) {
	if got := metricNamesFor(""); got != currentMetricNames {
		t.Fatalf("metricNamesFor(\"\") = %+v, want current names", got)
	}
}
