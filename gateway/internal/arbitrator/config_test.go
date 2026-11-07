package arbitrator

import (
	"reflect"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
)

// TestConfigFromSections: one class per priority class, highest first, with
// config's resolved per-class bounds, and the shed settings (all away from
// their defaults) and classes.
func TestConfigFromSections(t *testing.T) {
	cfg, err := config.Parse([]byte(`
pools:
  - {name: p, replicas: [{name: p-0, url: "http://p-0:8000"}]}
default_pool: p
auth: {enabled: false}
admission:
  retry_after_max: 7s
  early_reject: true
  shed: {enabled: false, ttft_p95_threshold: 3s, window: 45s, min_samples: 7, recover_ratio: 0.6}
`), func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	got := ConfigFrom(cfg)
	want := []Class{
		{Name: "interactive", MaxWait: 2 * time.Second, MaxQueue: config.DefaultAdmissionMaxQueue, MaxShare: 1},
		{Name: "user", MaxWait: config.DefaultAdmissionMaxWait, MaxQueue: config.DefaultAdmissionMaxQueue, MaxShare: 1},
		{Name: "background", MaxWait: 30 * time.Second, MaxQueue: 256, MaxShare: 0.5},
	}
	if !reflect.DeepEqual(got.Classes, want) {
		t.Fatalf("classes = %+v\nwant %+v", got.Classes, want)
	}
	if got.RetryAfterMax != 7*time.Second || !got.EarlyReject {
		t.Fatalf("retry after max = %v early_reject = %v, want 7s true", got.RetryAfterMax, got.EarlyReject)
	}
	wantShed := ShedConfig{Enabled: false, Threshold: 3 * time.Second, Window: 45 * time.Second, MinSamples: 7, RecoverRatio: 0.6, Classes: []string{"background"}}
	if !reflect.DeepEqual(got.Shed, wantShed) {
		t.Fatalf("shed = %+v, want %+v", got.Shed, wantShed)
	}
}
