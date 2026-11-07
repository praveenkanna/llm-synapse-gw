package config

import (
	"slices"
	"strings"
	"testing"
	"time"
)

func TestAdmissionDefaults(t *testing.T) {
	cfg := mustParse(t, minimalYAML, nil)
	a := cfg.Admission
	if !a.Enabled || a.Redis.Enabled || !a.Shed.Enabled {
		t.Fatalf("enabled flags = admission %v redis %v shed %v, want true false true", a.Enabled, a.Redis.Enabled, a.Shed.Enabled)
	}
	for _, tc := range []struct {
		class string
		wait  time.Duration
		queue int
	}{
		{PriorityInteractive, 2 * time.Second, DefaultAdmissionMaxQueue},
		{PriorityUser, DefaultAdmissionMaxWait, DefaultAdmissionMaxQueue},
		{PriorityBackground, 30 * time.Second, 256},
	} {
		if got := a.MaxWaitFor(tc.class); got != tc.wait {
			t.Errorf("MaxWaitFor(%s) = %s, want %s", tc.class, got, tc.wait)
		}
		if got := a.MaxQueueFor(tc.class); got != tc.queue {
			t.Errorf("MaxQueueFor(%s) = %d, want %d", tc.class, got, tc.queue)
		}
	}
	if got := cfg.ShedClasses(); !slices.Equal(got, []string{PriorityBackground}) {
		t.Errorf("ShedClasses = %v, want [background]", got)
	}
	if cfg.Pools[0].MaxInflightPerReplica != 0 || cfg.Pools[0].PriorityScheduling {
		t.Errorf("pool defaults = %+v, want no limit and no priority scheduling", cfg.Pools[0])
	}
	for class, want := range map[string]int{PriorityInteractive: -10, PriorityUser: 0, PriorityBackground: 10} {
		if got := cfg.Priority.UpstreamPriority(class); got != want {
			t.Errorf("UpstreamPriority(%s) = %d, want %d", class, got, want)
		}
	}
}

func TestAdmissionYAML(t *testing.T) {
	cfg := mustParse(t, `
pools:
  - name: large
    max_inflight_per_replica: 8
    priority_scheduling: true
    replicas:
      - url: http://vllm-large-0:8000
auth:
  enabled: false
priority:
  classes: [high, low]
  default_class: low
  anonymous_max_class: low
  default_max_class: low
  upstream_values: {high: -5, low: 7}
admission:
  max_wait: 3s
  classes:
    low: {max_wait: 1s, max_queue: 4}
  shed:
    classes: [low]
  redis:
    enabled: true
    addr: 127.0.0.1:6379
`, nil)
	if got := cfg.Pools[0].MaxInflightPerReplica; got != 8 {
		t.Errorf("max_inflight_per_replica = %d, want 8", got)
	}
	if got := cfg.Admission.MaxWaitFor("high"); got != 3*time.Second {
		t.Errorf("MaxWaitFor(high) = %s, want 3s", got)
	}
	if got, q := cfg.Admission.MaxWaitFor("low"), cfg.Admission.MaxQueueFor("low"); got != time.Second || q != 4 {
		t.Errorf("low = %s/%d, want 1s/4", got, q)
	}
	if got := cfg.Priority.UpstreamPriority("high"); got != -5 {
		t.Errorf("UpstreamPriority(high) = %d, want -5", got)
	}
}

func TestAdmissionValidation(t *testing.T) {
	tests := []struct {
		name, yaml, want string
	}{
		{"zero max wait", "admission: {max_wait: 0s}", "admission.max_wait"},
		{"huge queue", "admission: {max_queue: 100000000}", "admission.max_queue"},
		{"unknown class", "admission: {classes: {urgent: {max_wait: 1s}}}", "admission.classes.urgent: not in priority.classes"},
		{"negative class wait", "admission: {classes: {user: {max_wait: -1s}}}", "admission.classes.user.max_wait"},
		{"short retry after", "admission: {retry_after_max: 10ms}", "admission.retry_after_max"},
		{"shed highest class", "admission: {shed: {classes: [interactive]}}", "highest priority class"},
		{"shed unknown class", "admission: {shed: {classes: [nope]}}", "admission.shed.classes[0]"},
		{"shed ratio", "admission: {shed: {recover_ratio: 1.5}}", "admission.shed.recover_ratio"},
		{"shed window", "admission: {shed: {window: 100ms}}", "admission.shed.window"},
		{"redis timeout above interval", "admission: {redis: {enabled: true, timeout: 200ms}}", "admission.redis.timeout"},
		{"redis lease too short", "admission: {redis: {enabled: true, lease_ttl: 150ms}}", "admission.redis.lease_ttl"},
		{"redis bad addr", "admission: {redis: {enabled: true, addr: nope}}", "admission.redis.addr"},
		{"redis braces in prefix", "admission: {redis: {enabled: true, key_prefix: \"a{b}\"}}", "admission.redis.key_prefix"},
		{"upstream value unknown class", "priority: {upstream_values: {urgent: 1}}", "priority.upstream_values.urgent"},
		{"upstream value range", "priority: {upstream_values: {user: 99999999}}", "priority.upstream_values.user"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(minimalYAML+tc.yaml), envMap(nil))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

func TestPoolAdmissionValidation(t *testing.T) {
	tests := []struct {
		name, yaml, want string
	}{
		{"negative per replica", `
pools:
  - name: large
    max_inflight_per_replica: -1
    replicas: [{url: "http://a:8000"}]
auth: {enabled: false}`, "pools[0].max_inflight_per_replica"},
		{"flagged pool needs every class", `
pools:
  - name: large
    priority_scheduling: true
    replicas: [{url: "http://a:8000"}]
auth: {enabled: false}
priority:
  classes: [interactive, user, background, bulk]
  upstream_values: {interactive: 0, user: 1, background: 2}`, "priority.upstream_values.bulk: required"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.yaml), envMap(nil))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}
