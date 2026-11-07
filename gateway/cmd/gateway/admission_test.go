package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

func admissionTestConfig(t *testing.T, extra string) *config.Config {
	t.Helper()
	cfg, err := config.Parse([]byte(`
pools:
  - name: large
    max_inflight_per_replica: 4
    priority_scheduling: true
    replicas:
      - {name: large-0, url: "http://large-0:8000"}
      - {name: large-1, url: "http://large-1:8000"}
  - name: small
    replicas:
      - {name: small-0, url: "http://small-0:8000"}
models:
  large-model: large
  b-large-model: large
  small-model: small
default_pool: large
auth: {enabled: false}
`+extra), func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// TestNewAdmissionSizesPools: the arbitrator built from config gives each
// pool its per-replica capacity times its replicas (classes: arbitrator's
// TestConfigFromSections).
func TestNewAdmissionSizesPools(t *testing.T) {
	cfg := admissionTestConfig(t, "")
	disc, err := discovery.NewStatic(cfg.Pools)
	if err != nil {
		t.Fatal(err)
	}
	arb := newAdmission(cfg, disc, telemetry.NewMetrics())
	if c := arb.Capacity("large"); c != 8 {
		t.Fatalf("large capacity = %d, want 4 x 2 replicas", c)
	}
	if c := arb.Capacity("small"); c != -1 {
		t.Fatalf("small capacity = %d, want unlimited", c)
	}

	cfg.Admission.Enabled = false
	if newAdmission(cfg, disc, nil) != nil {
		t.Fatal("arbitrator built with admission off")
	}
}

// modelRecorder is a priority probe sender that records the model probed
// on each replica and accepts every probe.
type modelRecorder struct {
	mu     sync.Mutex
	models map[string]string
}

func (m *modelRecorder) Do(_ context.Context, rep discovery.Replica, req upstream.Request) (*http.Response, error) {
	var body struct {
		Model string `json:"model"`
	}
	_ = json.Unmarshal(req.Body, &body)
	m.mu.Lock()
	m.models[rep.Name] = body.Model
	m.mu.Unlock()
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}, nil
}

func TestPriorityProberModelAndPools(t *testing.T) {
	cfg := admissionTestConfig(t, "")
	disc, err := discovery.NewStatic(cfg.Pools)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.PriorityPools(); !reflect.DeepEqual(got, map[string]bool{"large": true}) {
		t.Fatalf("priority pools = %v", got)
	}
	if p := newPriorityProber(cfg, disc, nil, nil, nil, nil); p == nil {
		t.Fatal("no prober for a flagged pool")
	}
	// The probe's model is config's rule: a replica that lists models probes
	// its first; one that lists none, the pool's first mapped model by name.
	sender := &modelRecorder{models: map[string]string{}}
	served := func(replica string) []string {
		if replica == "large-1" {
			return []string{"served-x"}
		}
		return nil
	}
	newPriorityProber(cfg, disc, sender, served, nil, nil).Round(context.Background())
	if want := map[string]string{"large-0": "b-large-model", "large-1": "served-x"}; !reflect.DeepEqual(sender.models, want) {
		t.Fatalf("probed models = %v, want %v", sender.models, want)
	}
	cfg.Priority.UpstreamValues = map[string]int{"interactive": 0, "user": 0, "background": 0}
	if p := newPriorityProber(cfg, disc, nil, nil, nil, nil); p != nil {
		t.Fatal("prober built with every upstream value 0")
	}
}

// deadlineRecorder is a priority probe sender that answers every probe and
// records each one's time left before its deadline.
type deadlineRecorder struct {
	mu   sync.Mutex
	left []time.Duration
}

func (d *deadlineRecorder) Do(ctx context.Context, _ discovery.Replica, _ upstream.Request) (*http.Response, error) {
	deadline, _ := ctx.Deadline()
	d.mu.Lock()
	d.left = append(d.left, time.Until(deadline))
	d.mu.Unlock()
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}, nil
}

func (d *deadlineRecorder) calls() []time.Duration {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]time.Duration(nil), d.left...)
}

// TestPriorityProberPacedByItsOwnSettings: each probe is bounded by
// priority.probe.timeout and rounds come every priority.probe.interval, not
// by the stuck-engine probe's timeout and the load view's interval it
// borrowed before.
func TestPriorityProberPacedByItsOwnSettings(t *testing.T) {
	cfg := admissionTestConfig(t, "priority:\n  probe: {interval: 1s, timeout: 1s}\n")
	cfg.Router.Health.StuckProbeTimeout = 20 * time.Second
	cfg.Router.LoadView.ProbeInterval = time.Hour
	disc, err := discovery.NewStatic(cfg.Pools)
	if err != nil {
		t.Fatal(err)
	}
	d := &deadlineRecorder{}
	p := newPriorityProber(cfg, disc, d, nil, telemetry.NewMetrics(), nil)
	p.Round(context.Background())
	left := d.calls()
	if len(left) != 2 {
		t.Fatalf("probed %d replicas, want both of the flagged pool's", len(left))
	}
	for _, l := range left {
		if l <= 500*time.Millisecond || l > time.Second {
			t.Fatalf("probe deadline %v away, want about priority.probe.timeout (1s)", l)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)
	time.Sleep(1500 * time.Millisecond)
	p.Stop()
	// A round at once and one a second later, 2 probes each: 4 (6 if a
	// third round started at the edge).
	if n := len(d.calls()) - 2; n < 4 || n > 6 {
		t.Fatalf("%d probes in 1.5s, want 2 rounds of 2 (priority.probe.interval 1s)", n)
	}
}
