package config

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestRouterDefaults(t *testing.T) {
	cfg := mustParse(t, minimalYAML, nil)
	r := cfg.Router
	checks := []struct {
		name      string
		got, want any
	}{
		{"policy", r.Policy, "affinity"},
		{"epsilon", r.Epsilon, 0.25},
		{"overflow_slack", r.OverflowSlack, 1},
		{"key_scan_budget", r.KeyScanBudget, 40960},
		{"virtual_nodes", r.VirtualNodes, 512},
		{"load_view.kv_high_watermark", r.LoadView.KVHighWatermark, 0.0},
		{"load_view.kv_penalty_weight", r.LoadView.KVPenaltyWeight, 0},
		{"health.poll_interval", r.Health.PollInterval, time.Second},
		{"health.poll_timeout", r.Health.PollTimeout, time.Second},
		{"health.failure_threshold", r.Health.FailureThreshold, 2},
		{"health.success_threshold", r.Health.SuccessThreshold, 1},
		{"health.passive_failure_threshold", r.Health.PassiveFailureThreshold, 3},
		{"health.stuck_engine_timeout", r.Health.StuckEngineTimeout, 30 * time.Second},
		{"health.stuck_engine_streams", r.Health.StuckEngineStreams, 3},
		{"health.stuck_engine_ratio", r.Health.StuckEngineRatio, 0.5},
		{"health.stuck_engine_window", r.Health.StuckEngineWindow, time.Minute},
		{"health.stuck_engine_max_waits", r.Health.StuckEngineMaxWaits, 65536},
		{"health.min_ejection", r.Health.MinEjection, 5 * time.Second},
		{"health.max_ejection", r.Health.MaxEjection, time.Minute},
		{"health.ejection_backoff_multiplier", r.Health.EjectionBackoffMultiplier, 2.0},
		{"health.stuck_probe_enabled", r.Health.StuckProbeEnabled, true},
		{"health.stuck_probe_timeout", r.Health.StuckProbeTimeout, 10 * time.Second},
		{"health.stuck_probe_path", r.Health.StuckProbePath, "/v1/completions"},
		{"health.stuck_probe_models", r.Health.StuckProbeModels, map[string]string{}},
		{"load_view.scrape_interval", r.LoadView.ScrapeInterval, 5 * time.Second},
		{"load_view.scrape_timeout", r.LoadView.ScrapeTimeout, 3 * time.Second},
		{"load_view.probe_interval", r.LoadView.ProbeInterval, 60 * time.Second},
		{"load_view.stale_threshold", r.LoadView.StaleThreshold, 15 * time.Second},
		{"load_view.metric_aliases", r.LoadView.MetricAliases, DefaultMetricAliases()},
	}
	for _, c := range checks {
		if !reflect.DeepEqual(c.got, c.want) {
			t.Errorf("router.%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	if got := r.LoadView.MetricAliases[SignalWaitingRequests][0]; got != "vllm:num_requests_waiting" {
		t.Errorf("first waiting alias = %q", got)
	}
}

func TestRouterYAMLOverrides(t *testing.T) {
	cfg := mustParse(t, minimalYAML+`
router:
  policy: round_robin
  epsilon: 0.5
  overflow_slack: 3
  key_scan_budget: 20480
  virtual_nodes: 256
  health:
    poll_interval: 2s
    poll_timeout: 500ms
    failure_threshold: 3
    success_threshold: 2
    passive_failure_threshold: 4
    stuck_engine_timeout: 45s
    stuck_engine_streams: 4
    stuck_engine_ratio: 0.8
    stuck_engine_window: 90s
    stuck_engine_max_waits: 4096
    min_ejection: 2s
    max_ejection: 30s
    ejection_backoff_multiplier: 1.5
    stuck_probe_enabled: false
    stuck_probe_timeout: 3s
    stuck_probe_path: /v1/chat/completions
    stuck_probe_models:
      large: org/model-7b
  load_view:
    scrape_interval: 10s
    scrape_timeout: 2s
    probe_interval: 5m
    stale_threshold: 30s
    kv_high_watermark: 0.9
    kv_penalty_weight: 8
    metric_aliases:
      waiting_requests: ["custom:waiting"]
`, nil)
	r := cfg.Router
	want := RouterConfig{
		Policy:        "round_robin",
		Epsilon:       0.5,
		OverflowSlack: 3,
		KeyScanBudget: 20480,
		VirtualNodes:  256,
		Health: RouterHealth{
			PollInterval: 2 * time.Second, PollTimeout: 500 * time.Millisecond,
			FailureThreshold: 3, SuccessThreshold: 2, PassiveFailureThreshold: 4,
			StuckEngineTimeout: 45 * time.Second, StuckEngineStreams: 4, StuckEngineRatio: 0.8, StuckEngineWindow: 90 * time.Second, StuckEngineMaxWaits: 4096,
			MinEjection: 2 * time.Second, MaxEjection: 30 * time.Second, EjectionBackoffMultiplier: 1.5,
			StuckProbeEnabled: false, StuckProbeTimeout: 3 * time.Second, StuckProbePath: "/v1/chat/completions",
			StuckProbeModels: map[string]string{"large": "org/model-7b"},
		},
		LoadView: LoadView{
			ScrapeInterval: 10 * time.Second, ScrapeTimeout: 2 * time.Second,
			ProbeInterval: 5 * time.Minute, StaleThreshold: 30 * time.Second,
			KVHighWatermark: 0.9, KVPenaltyWeight: 8,
		},
	}
	// The named signal is replaced; the others keep their defaults.
	want.LoadView.MetricAliases = DefaultMetricAliases()
	want.LoadView.MetricAliases[SignalWaitingRequests] = []string{"custom:waiting"}
	if !reflect.DeepEqual(r, want) {
		t.Errorf("router = %+v\nwant %+v", r, want)
	}
	// Decoding merges into the defaults' alias map; it must be a fresh copy.
	if got := Defaults().Router.LoadView.MetricAliases[SignalWaitingRequests][0]; got != "vllm:num_requests_waiting" {
		t.Errorf("YAML decoding mutated the shared default alias table: %q", got)
	}
}

func TestRouterValidationRejects(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		env  map[string]string
		want string
	}{
		{"unknown policy", "router:\n  policy: random\n", nil, `router.policy: "random" must be`},
		{"epsilon zero", "router:\n  epsilon: 0\n", nil, "router.epsilon: 0 must be a finite number greater than 0"},
		{"overflow slack negative", "router:\n  overflow_slack: -1\n", nil, "router.overflow_slack: -1 out of range [0, 16]"},
		{"overflow slack too large", "router:\n  overflow_slack: 17\n", nil, "router.overflow_slack: 17 out of range [0, 16]"},
		{"key scan budget zero", "router:\n  key_scan_budget: 0\n", nil, "router.key_scan_budget: 0 out of range [1024, 131072]"},
		{"key scan budget too small", "router:\n  key_scan_budget: 1023\n", nil, "router.key_scan_budget: 1023 out of range"},
		{"key scan budget too large env", "", map[string]string{"GATEWAY_ROUTER_KEY_SCAN_BUDGET": "131073"}, "router.key_scan_budget: 131073 out of range"},
		{"overflow slack env", "", map[string]string{"GATEWAY_ROUTER_OVERFLOW_SLACK": "99"}, "router.overflow_slack: 99 out of range"},
		{"epsilon negative", "router:\n  epsilon: -0.1\n", nil, "router.epsilon: -0.1 must be a finite number"},
		{"epsilon NaN yaml", "router:\n  epsilon: .nan\n", nil, "router.epsilon: NaN must be a finite number"},
		{"epsilon +Inf yaml", "router:\n  epsilon: .inf\n", nil, "router.epsilon: +Inf must be a finite number"},
		{"epsilon -Inf yaml", "router:\n  epsilon: -.inf\n", nil, "router.epsilon: -Inf must be a finite number"},
		{"epsilon NaN env", "", map[string]string{"GATEWAY_ROUTER_EPSILON": "NaN"}, "router.epsilon: NaN"},
		{"epsilon Inf env", "", map[string]string{"GATEWAY_ROUTER_EPSILON": "Inf"}, "router.epsilon: +Inf"},
		{"epsilon zero env", "", map[string]string{"GATEWAY_ROUTER_EPSILON": "0"}, "router.epsilon: 0 must be"},
		{"epsilon not a number env", "", map[string]string{"GATEWAY_ROUTER_EPSILON": "lots"}, "env GATEWAY_ROUTER_EPSILON: invalid float"},

		{"poll interval too short", "router:\n  health:\n    poll_interval: 5ms\n", nil, "router.health.poll_interval: 5ms is below the minimum 100ms"},
		{"poll timeout zero", "", map[string]string{"GATEWAY_ROUTER_HEALTH_POLL_TIMEOUT": "0s"}, "router.health.poll_timeout: must be greater than zero"},
		{"poll timeout above interval", "router:\n  health:\n    poll_timeout: 2s\n", nil, "router.health.poll_timeout: 2s exceeds router.health.poll_interval 1s"},
		{"failure threshold zero", "router:\n  health:\n    failure_threshold: 0\n", nil, "router.health.failure_threshold: 0 out of range [1, 100]"},
		{"success threshold huge", "", map[string]string{"GATEWAY_ROUTER_HEALTH_SUCCESS_THRESHOLD": "101"}, "router.health.success_threshold: 101 out of range"},
		{"passive threshold negative", "router:\n  health:\n    passive_failure_threshold: -1\n", nil, "router.health.passive_failure_threshold: -1 out of range"},
		{"stuck timeout too short", "router:\n  health:\n    stuck_engine_timeout: 500ms\n", nil, "router.health.stuck_engine_timeout: 500ms is below the minimum 1s"},
		{"stuck streams zero", "router:\n  health:\n    stuck_engine_streams: 0\n", nil, "router.health.stuck_engine_streams: 0 out of range [1, 100]"},
		{"stuck streams too many env", "", map[string]string{"GATEWAY_ROUTER_HEALTH_STUCK_ENGINE_STREAMS": "101"}, "router.health.stuck_engine_streams: 101 out of range"},
		{"stuck ratio zero", "router:\n  health:\n    stuck_engine_ratio: 0\n", nil, "router.health.stuck_engine_ratio: 0 out of range (0, 1]"},
		{"stuck ratio above one env", "", map[string]string{"GATEWAY_ROUTER_HEALTH_STUCK_ENGINE_RATIO": "1.5"}, "router.health.stuck_engine_ratio: 1.5 out of range"},
		{"stuck ratio NaN", "router:\n  health:\n    stuck_engine_ratio: .nan\n", nil, "router.health.stuck_engine_ratio: NaN out of range"},
		{"first chunk timeout at stuck timeout", "upstream:\n  timeouts:\n    first_chunk: 30s\n", nil, "upstream.timeouts.first_chunk: 30s must be 0 (bounded by total) or above router.health.stuck_engine_timeout 30s"},
		{"first chunk timeout below stuck timeout env", "", map[string]string{"GATEWAY_UPSTREAM_FIRST_CHUNK_TIMEOUT": "10s"}, "upstream.timeouts.first_chunk: 10s must be 0"},
		{"stuck timeout raised to first chunk", "upstream:\n  timeouts:\n    first_chunk: 45s\nrouter:\n  health:\n    stuck_engine_timeout: 1m\n", nil, "upstream.timeouts.first_chunk: 45s must be 0 (bounded by total) or above router.health.stuck_engine_timeout 1m0s"},
		{"total at stuck timeout", "upstream:\n  timeouts:\n    total: 30s\n    stream_response_header: 10s\n    idle_between_chunks: 10s\n", nil, "upstream.timeouts.total: 30s must be above router.health.stuck_engine_timeout 30s"},
		{"stuck window too short", "router:\n  health:\n    stuck_engine_window: 500ms\n", nil, "router.health.stuck_engine_window: 500ms out of range [1s, 1h0m0s]"},
		{"stuck window too long env", "", map[string]string{"GATEWAY_ROUTER_HEALTH_STUCK_ENGINE_WINDOW": "2h"}, "router.health.stuck_engine_window: 2h0m0s out of range"},
		{"stuck max waits zero", "router:\n  health:\n    stuck_engine_max_waits: 0\n", nil, "router.health.stuck_engine_max_waits: 0 out of range [1, 1048576]"},
		{"stuck max waits too many env", "", map[string]string{"GATEWAY_ROUTER_HEALTH_STUCK_ENGINE_MAX_WAITS": "2000000"}, "router.health.stuck_engine_max_waits: 2000000 out of range"},
		{"min ejection too short", "router:\n  health:\n    min_ejection: 500ms\n", nil, "router.health.min_ejection: 500ms out of range [1s, 1h0m0s]"},
		{"min ejection too long env", "", map[string]string{"GATEWAY_ROUTER_HEALTH_MIN_EJECTION": "2h"}, "router.health.min_ejection: 2h0m0s out of range"},
		{"max ejection below min", "router:\n  health:\n    min_ejection: 10s\n    max_ejection: 5s\n", nil, "router.health.max_ejection: 5s must be in [router.health.min_ejection 10s, 1h0m0s]"},
		{"max ejection too long", "", map[string]string{"GATEWAY_ROUTER_HEALTH_MAX_EJECTION": "61m"}, "router.health.max_ejection: 1h1m0s must be in"},
		{"backoff below one", "router:\n  health:\n    ejection_backoff_multiplier: 0.5\n", nil, "router.health.ejection_backoff_multiplier: 0.5 out of range [1, 10]"},
		{"backoff too large env", "", map[string]string{"GATEWAY_ROUTER_HEALTH_EJECTION_BACKOFF_MULTIPLIER": "11"}, "router.health.ejection_backoff_multiplier: 11 out of range"},
		{"backoff NaN", "router:\n  health:\n    ejection_backoff_multiplier: .nan\n", nil, "router.health.ejection_backoff_multiplier: NaN out of range"},
		{"backoff +Inf env", "", map[string]string{"GATEWAY_ROUTER_HEALTH_EJECTION_BACKOFF_MULTIPLIER": "Inf"}, "router.health.ejection_backoff_multiplier: +Inf out of range"},
		{"backoff not a number env", "", map[string]string{"GATEWAY_ROUTER_HEALTH_EJECTION_BACKOFF_MULTIPLIER": "double"}, "env GATEWAY_ROUTER_HEALTH_EJECTION_BACKOFF_MULTIPLIER: invalid float"},
		{"stuck probe timeout too short", "router:\n  health:\n    stuck_probe_timeout: 500ms\n", nil, "router.health.stuck_probe_timeout: 500ms out of range [1s, 5m0s]"},
		{"stuck probe timeout too long env", "", map[string]string{"GATEWAY_ROUTER_HEALTH_STUCK_PROBE_TIMEOUT": "6m"}, "router.health.stuck_probe_timeout: 6m0s out of range"},
		{"stuck probe path unknown", "router:\n  health:\n    stuck_probe_path: /v1/embeddings\n", nil, `router.health.stuck_probe_path: "/v1/embeddings" must be "/v1/completions" or "/v1/chat/completions"`},
		{"stuck probe path empty env", "", map[string]string{"GATEWAY_ROUTER_HEALTH_STUCK_PROBE_PATH": " "}, `router.health.stuck_probe_path: " " must be`},
		{"stuck probe enabled not bool env", "", map[string]string{"GATEWAY_ROUTER_HEALTH_STUCK_PROBE_ENABLED": "maybe"}, "env GATEWAY_ROUTER_HEALTH_STUCK_PROBE_ENABLED: invalid boolean"},
		{"stuck probe model unknown pool", "router:\n  health:\n    stuck_probe_models:\n      small: m\n", nil, "router.health.stuck_probe_models.small: no such pool"},
		{"stuck probe model empty", "router:\n  health:\n    stuck_probe_models:\n      large: \"\"\n", nil, "router.health.stuck_probe_models.large: model name must be 1 to 256 characters, not blank, without control characters"},
		{"stuck probe model blank", "router:\n  health:\n    stuck_probe_models:\n      large: \"   \"\n", nil, "router.health.stuck_probe_models.large: model name must be"},
		{"stuck probe model with control char", "router:\n  health:\n    stuck_probe_models:\n      large: \"a\\tb\"\n", nil, "router.health.stuck_probe_models.large: model name must be"},
		{"stuck probe timeout above stuck engine timeout", "router:\n  health:\n    stuck_probe_timeout: 31s\n", nil, "router.health.stuck_probe_timeout: 31s exceeds router.health.stuck_engine_timeout 30s"},
		{"stuck probe timeout above lowered stuck engine timeout env", "", map[string]string{"GATEWAY_ROUTER_HEALTH_STUCK_ENGINE_TIMEOUT": "5s"}, "router.health.stuck_probe_timeout: 10s exceeds router.health.stuck_engine_timeout 5s"},
		{"stuck probe model too long", "router:\n  health:\n    stuck_probe_models:\n      large: " + strings.Repeat("m", 257) + "\n", nil, "router.health.stuck_probe_models.large: model name must be"},
		{"health threshold not int env", "", map[string]string{"GATEWAY_ROUTER_HEALTH_FAILURE_THRESHOLD": "two"}, "env GATEWAY_ROUTER_HEALTH_FAILURE_THRESHOLD: invalid integer"},

		{"scrape interval too short", "", map[string]string{"GATEWAY_ROUTER_LOAD_VIEW_SCRAPE_INTERVAL": "10ms"}, "router.load_view.scrape_interval: 10ms is below the minimum"},
		{"probe interval zero", "router:\n  load_view:\n    probe_interval: 0s\n", nil, "router.load_view.probe_interval: 0s is below the minimum"},
		{"scrape timeout zero", "router:\n  load_view:\n    scrape_timeout: 0s\n", nil, "router.load_view.scrape_timeout: must be greater than zero"},
		{"scrape timeout above interval", "router:\n  load_view:\n    scrape_timeout: 6s\n", nil, "router.load_view.scrape_timeout: 6s exceeds router.load_view.scrape_interval 5s"},
		{"stale threshold not above interval", "router:\n  load_view:\n    stale_threshold: 5s\n", nil, "router.load_view.stale_threshold: 5s must be greater than router.load_view.scrape_interval 5s"},
		{"stale threshold bad env", "", map[string]string{"GATEWAY_ROUTER_LOAD_VIEW_STALE_THRESHOLD": "soon"}, "env GATEWAY_ROUTER_LOAD_VIEW_STALE_THRESHOLD: invalid duration"},
		{"unknown alias signal", "router:\n  load_view:\n    metric_aliases:\n      gpu_temp: [\"x\"]\n", nil, "router.load_view.metric_aliases.gpu_temp: unknown signal"},
		{"empty alias list", "router:\n  load_view:\n    metric_aliases:\n      waiting_requests: []\n", nil, "router.load_view.metric_aliases.waiting_requests: needs 1 to 16 metric names, got 0"},
		{"virtual nodes too few", "router:\n  virtual_nodes: 63\n", nil, "router.virtual_nodes: 63 out of range [64, 2048]"},
		{"virtual nodes too many", "router:\n  virtual_nodes: 4096\n", nil, "router.virtual_nodes: 4096 out of range"},
		{"kv watermark one", "router:\n  load_view:\n    kv_high_watermark: 1\n", nil, "router.load_view.kv_high_watermark: 1 out of range [0, 1)"},
		{"kv watermark negative", "router:\n  load_view:\n    kv_high_watermark: -0.1\n", nil, "router.load_view.kv_high_watermark: -0.1 out of range"},
		{"kv weight too large", "router:\n  load_view:\n    kv_penalty_weight: 2000\n", nil, "router.load_view.kv_penalty_weight: 2000 out of range [0, 1024]"},
		{"invalid metric name", "router:\n  load_view:\n    metric_aliases:\n      running_requests: [\"bad name\"]\n", nil, `router.load_view.metric_aliases.running_requests[0]: "bad name" is not a valid Prometheus metric name`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(minimalYAML+tc.yaml), envMap(tc.env))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestRouterValidationAccepts(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		env  map[string]string
	}{
		{"key scan budget at bounds", "router:\n  key_scan_budget: 1024\n", map[string]string{"GATEWAY_ROUTER_KEY_SCAN_BUDGET": "131072"}},
		{"small epsilon", "router:\n  epsilon: 0.01\n", nil},
		{"epsilon env", "", map[string]string{"GATEWAY_ROUTER_EPSILON": "1.5"}},
		{"empty policy means affinity", "router:\n  policy: \"\"\n", nil},
		{"intervals at floor", "router:\n  health:\n    poll_interval: 100ms\n    poll_timeout: 100ms\n  load_view:\n    scrape_interval: 100ms\n    scrape_timeout: 100ms\n    probe_interval: 100ms\n    stale_threshold: 101ms\n", nil},
		{"thresholds at bounds", "router:\n  health:\n    failure_threshold: 1\n    success_threshold: 100\n    stuck_engine_timeout: 1s\n    stuck_probe_timeout: 1s\n", nil},
		{"ejection at lower bounds", "router:\n  health:\n    min_ejection: 1s\n    max_ejection: 1s\n    ejection_backoff_multiplier: 1\n", nil},
		{"ejection at upper bounds env", "", map[string]string{"GATEWAY_ROUTER_HEALTH_MIN_EJECTION": "1h", "GATEWAY_ROUTER_HEALTH_MAX_EJECTION": "1h", "GATEWAY_ROUTER_HEALTH_EJECTION_BACKOFF_MULTIPLIER": "10"}},
		{"stuck probe at bounds", "router:\n  health:\n    stuck_probe_timeout: 1s\n    stuck_probe_models:\n      large: " + strings.Repeat("m", 256) + "\n", nil},
		{"stuck probe upper bound env", "", map[string]string{"GATEWAY_ROUTER_HEALTH_STUCK_PROBE_TIMEOUT": "5m", "GATEWAY_ROUTER_HEALTH_STUCK_ENGINE_TIMEOUT": "5m", "GATEWAY_ROUTER_HEALTH_STUCK_PROBE_PATH": "/v1/chat/completions"}},
		{"stuck probe timeout equal to stuck engine timeout", "router:\n  health:\n    stuck_probe_timeout: 30s\n", nil},
		{"stuck probe timeout above stuck engine timeout while off", "router:\n  health:\n    stuck_probe_enabled: false\n    stuck_probe_timeout: 1m\n", nil},
		{"stuck probe model with spaces", "router:\n  health:\n    stuck_probe_models:\n      large: \"My Model 7B\"\n", nil},
		{"stuck probe off", "router:\n  health:\n    stuck_probe_enabled: false\n", nil},
		{"stuck probe models null", "router:\n  health:\n    stuck_probe_models: null\n", nil},
		{"stuck probe model with slash and colon", "router:\n  health:\n    stuck_probe_models:\n      large: \"org/Model-7B:v1.5\"\n", nil},
		{"alias with colon and underscore", "router:\n  load_view:\n    metric_aliases:\n      kv_cache_usage_perc: [\"vllm:kv_cache_usage_perc\", \"_x:y_1\"]\n", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Parse([]byte(minimalYAML+tc.yaml), envMap(tc.env)); err != nil {
				t.Fatalf("Parse: %v", err)
			}
		})
	}
}

func TestMetricAliasesRequireEverySignal(t *testing.T) {
	cfg := Defaults()
	delete(cfg.Router.LoadView.MetricAliases, SignalKVCacheUsage)
	errs := validateMetricAliases(cfg.Router.LoadView.MetricAliases)
	if len(errs) != 1 || !strings.Contains(errs[0].Error(), "router.load_view.metric_aliases.kv_cache_usage_perc: required") {
		t.Fatalf("errs = %v, want one 'required' error for kv_cache_usage_perc", errs)
	}
}

func TestMetricAliasesNullOrEmptyMeansDefaults(t *testing.T) {
	for _, yaml := range []string{
		"router:\n  load_view:\n    metric_aliases:\n",
		"router:\n  load_view:\n    metric_aliases: {}\n",
		"router:\n  load_view:\n    metric_aliases: null\n",
	} {
		cfg := mustParse(t, minimalYAML+yaml, nil)
		if !reflect.DeepEqual(cfg.Router.LoadView.MetricAliases, DefaultMetricAliases()) {
			t.Errorf("%q: aliases = %v, want the defaults", yaml, cfg.Router.LoadView.MetricAliases)
		}
	}
}
