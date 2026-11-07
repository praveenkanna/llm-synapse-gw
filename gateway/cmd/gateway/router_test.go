package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/health"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/affinity"
	routerhealth "github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/health"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/loadview"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/ring"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

// TestRouterConfigDefaultsMatchPackages keeps config's router defaults (it
// cannot import the router packages) equal to the packages' own, so the
// defaults stay what M1 shipped. Router health and the load view keep none
// of their own: they take their config sections (ADR 0022).
func TestRouterConfigDefaultsMatchPackages(t *testing.T) {
	def := config.Defaults().Router
	if got, want := def.Epsilon, ring.DefaultEpsilon; got != want {
		t.Errorf("config epsilon default = %v, ring default %v", got, want)
	}
	if got, want := def.OverflowSlack, ring.DefaultOverflowSlack; got != want {
		t.Errorf("config overflow slack default = %v, ring default %v", got, want)
	}
	if got, want := def.KeyScanBudget, affinity.DefaultKeyScanBudget; got != want {
		t.Errorf("config key scan budget default = %v, affinity default %v", got, want)
	}
	if got, want := def.VirtualNodes, ring.DefaultVirtualNodes; got != want {
		t.Errorf("config virtual nodes default = %v, ring default %v", got, want)
	}
}

// TestRunServesWhileReplicaIsBlackholed: a replica that accepts connections
// and never answers must not delay startup. Liveness answers while the
// first health poll, scrape and probe are still hanging (their timeouts are
// set far beyond the test's deadline).
func TestRunServesWhileReplicaIsBlackholed(t *testing.T) {
	release := make(chan struct{})
	reached := make(chan struct{}, 64)
	var hanging atomic.Int64 // load-view requests (not /health) still held
	blackhole := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			hanging.Add(1)
			defer hanging.Add(-1)
			select {
			case reached <- struct{}{}:
			default:
			}
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer blackhole.Close()
	defer close(release)

	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	_ = probe.Close()

	dir := t.TempDir()
	path := filepath.Join(dir, "gateway.yaml")
	yaml := "pools:\n  - name: large\n    replicas:\n      - url: " + blackhole.URL + "\nauth:\n  enabled: false\n"
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{
		config.EnvConfigPath: path, "GATEWAY_LISTEN_ADDR": addr, envMetricsAddr: anyPort,
		"GATEWAY_ROUTER_HEALTH_POLL_INTERVAL":      "1h",
		"GATEWAY_ROUTER_HEALTH_POLL_TIMEOUT":       "1h",
		"GATEWAY_ROUTER_LOAD_VIEW_SCRAPE_INTERVAL": "1h",
		"GATEWAY_ROUTER_LOAD_VIEW_SCRAPE_TIMEOUT":  "1h",
		"GATEWAY_ROUTER_LOAD_VIEW_PROBE_INTERVAL":  "1h",
		"GATEWAY_ROUTER_LOAD_VIEW_STALE_THRESHOLD": "2h",
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, func(k string) string { return env[k] }, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()

	// The background scrape/probe reached the replica and is now hanging.
	select {
	case <-reached:
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("load view never contacted the replica")
	}

	client := &http.Client{Timeout: 2 * time.Second}
	var resp *http.Response
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err = client.Get("http://" + addr + health.LivenessPath)
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		cancel()
		t.Fatalf("liveness not served while the replica hangs: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("liveness status = %d", resp.StatusCode)
	}
	// Served while the first scrape/probe still hangs: startup did not wait.
	if hanging.Load() == 0 {
		t.Error("liveness answered only after the load view's first requests ended; startup waited on the replica")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return after context cancel (background probes not cancelled?)")
	}
}

// TestTrackerConfig: the tracker is told which pools serve out of order
// (priority scheduling) and gets the logger for rejected probes.
func TestTrackerConfig(t *testing.T) {
	cfg := config.Defaults()
	cfg.Pools = []config.Pool{{Name: "large", PriorityScheduling: true}, {Name: "small"}}
	logger := slog.New(slog.DiscardHandler)
	hc := trackerConfig(&cfg, nil, nil, logger)
	if !reflect.DeepEqual(hc.OutOfOrderPools, map[string]bool{"large": true}) || hc.Logger != logger {
		t.Fatalf("out-of-order pools %v, logger set %v; want the priority-scheduling pool and the logger", hc.OutOfOrderPools, hc.Logger == logger)
	}
}

// TestWithStuckProbe: the probe is wired only when enabled; its model is
// config's probe-model rule: the pool's configured one, else the replica's
// first served model, else the pool's first mapped model, else none (the
// tracker then fails open).
func TestWithStuckProbe(t *testing.T) {
	sender := upstream.New(config.Defaults().Upstream)
	cfg := config.Defaults()
	cfg.Router.Health.StuckProbeEnabled = true
	cfg.Router.Health.StuckProbeModels = map[string]string{"large": "pinned-model"}
	cfg.Models = map[string]string{"small-b": "small", "small-a": "small"}
	served := map[string][]string{"small-0": {"served-a", "served-b"}}
	lookup := func(name string) []string { return served[name] }

	off := cfg
	off.Router.Health.StuckProbeEnabled = false
	if c := withStuckProbe(routerhealth.ConfigFrom(off.Router), &off, sender, lookup); c.StuckProbeSender != nil || c.StuckProbeModel != nil {
		t.Fatal("probe wired while router.health.stuck_probe_enabled is false")
	}

	c := withStuckProbe(routerhealth.ConfigFrom(cfg.Router), &cfg, sender, lookup)
	if c.StuckProbeSender != sender {
		t.Fatal("probe sender is not the upstream client")
	}
	cfg.Router.Health.StuckProbeModels["large"] = "changed" // the config map is copied at startup
	for _, tc := range []struct {
		pool, replica, want string
	}{
		{"large", "large-0", "pinned-model"},
		{"small", "small-0", "served-a"},
		{"small", "small-1", "small-a"},
		{"other", "other-0", ""},
	} {
		if got := c.StuckProbeModel(discovery.Replica{Name: tc.replica, Pool: tc.pool}); got != tc.want {
			t.Errorf("model for %s/%s = %q, want %q", tc.pool, tc.replica, got, tc.want)
		}
	}
}

// TestServedModelsFromLoadView: the load view's /v1/models probe feeds the
// probe's model.
func TestServedModelsFromLoadView(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/models" {
			_, _ = io.WriteString(w, `{"object":"list","data":[{"id":"org/base"},{"id":"lora-1"}]}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	rep := discovery.Replica{Name: "r0", Pool: "p", BaseURL: u}
	lv := loadview.New(loadview.Config{LoadView: config.Defaults().Router.LoadView}, loadview.ReplicaListerFunc(func() []discovery.Replica { return []discovery.Replica{rep} }), nil, srv.Client())
	models := servedModels(lv)
	if got := models("r0"); len(got) != 0 {
		t.Fatalf("models before the first probe = %v, want none", got)
	}
	_ = lv.ProbeOnce(context.Background())
	if got := models("r0"); len(got) != 2 || got[0] != "org/base" {
		t.Fatalf("models = %v, want [org/base lora-1]", got)
	}
}

// TestRoutingPairsHealthWithPolicy: the affinity router comes with passive
// health, and both use the same tracker: an ejection there steers the
// router's picks, and waits and hard connect failures reported through the
// health reach it. The round-robin router comes with none, so nothing is
// reported or watched under it, since its picks ignore health.
func TestRoutingPairsHealthWithPolicy(t *testing.T) {
	cfg, err := config.Parse([]byte("pools:\n  - name: large\n    replicas:\n      - {name: r0, url: http://127.0.0.1:1}\n      - {name: r1, url: http://127.0.0.1:2}\nauth: {enabled: false}\n"), func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	disc, err := discovery.NewStatic(cfg.Pools)
	if err != nil {
		t.Fatal(err)
	}
	rep := disc.Replicas("large")[1] // r1: the health checks below run after r0 is ejected
	for _, policy := range []string{"affinity", "round_robin"} {
		t.Run(policy, func(t *testing.T) {
			cfg.Router.Policy = policy
			tracker := routerhealth.New(routerhealth.ConfigFrom(config.Defaults().Router), disc, nil, nil) // never started: passive only
			rtr, hl := newRouting(cfg, disc, tracker, loadview.New(loadview.Config{LoadView: config.Defaults().Router.LoadView}, disc, nil, nil), nil)
			if policy == "round_robin" {
				if _, ok := rtr.(*router.RoundRobin); !ok || hl != nil {
					t.Fatalf("round_robin: router %T, health %v; want *router.RoundRobin and no health", rtr, hl)
				}
				return
			}
			if _, ok := rtr.(*router.AffinityRouter); !ok || hl == nil {
				t.Fatalf("affinity: router %T, health %v; want *router.AffinityRouter with health", rtr, hl)
			}
			tracker.Eject("r0", routerhealth.ReasonStuckEngine)
			for i := range 10 {
				lease, err := rtr.Route(context.Background(), httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil), "large")
				if err != nil {
					t.Fatal(err)
				}
				lease.Release()
				if lease.Replica.Name == "r0" {
					t.Fatalf("pick %d went to r0, which the tracker ejected: the router does not read the health's tracker", i+1)
				}
			}
			w := hl.WatchFirstChunk(context.Background(), rep)
			if n := tracker.FirstChunkWaits(); n != 1 {
				t.Fatalf("waits registered with the tracker = %d, want 1", n)
			}
			w.End()
			for range routerhealth.ConfigFrom(config.Defaults().Router).PassiveFailureThreshold {
				hl.RecordTransportFailure(rep, errors.New("connection refused"))
			}
			if tracker.IsHealthy(rep.Name) {
				t.Fatal("hard connect failures reported through the health did not reach the tracker")
			}
		})
	}
}
