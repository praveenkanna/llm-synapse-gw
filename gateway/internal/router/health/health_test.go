package health_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/health"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

func makeStaticDiscovery(t *testing.T, poolName string, urls ...string) *discovery.Static {
	t.Helper()
	reps := make([]config.Replica, len(urls))
	for i, u := range urls {
		reps[i] = config.Replica{
			Name: fmt.Sprintf("rep-%d", i),
			URL:  u,
		}
	}
	pools := []config.Pool{
		{
			Name:     poolName,
			Replicas: reps,
		},
	}
	s, err := discovery.NewStatic(pools)
	if err != nil {
		t.Fatalf("failed to create static discovery: %v", err)
	}
	return s
}

func TestPassiveEjection(t *testing.T) {
	disc := makeStaticDiscovery(t, "llama", "http://127.0.0.1:8001", "http://127.0.0.1:8002")
	tm := telemetry.NewMetrics()
	cfg := health.ConfigFrom(config.Defaults().Router)
	cfg.PassiveFailureThreshold = 1
	tr := health.New(cfg, disc, tm, nil)

	if !tr.IsHealthy("rep-0") || !tr.IsHealthy("rep-1") {
		t.Fatalf("expected all replicas initially healthy")
	}

	r, err := tr.Ring("llama")
	if err != nil || len(r.Replicas()) != 2 {
		t.Fatalf("expected ring with 2 replicas, got %v (err: %v)", r, err)
	}

	// Trigger passive ejection on rep-0
	tr.RecordTransportFailure(disc.Replicas("llama")[0], errors.New("dial tcp: connection refused"))

	if tr.IsHealthy("rep-0") {
		t.Errorf("expected rep-0 to be marked unhealthy after transport failure")
	}
	if !tr.IsHealthy("rep-1") {
		t.Errorf("expected rep-1 to remain healthy")
	}

	// Ring should have rebuilt immediately without rep-0
	rAfter, err := tr.Ring("llama")
	if err != nil {
		t.Fatalf("unexpected error getting ring: %v", err)
	}
	if len(rAfter.Replicas()) != 1 || rAfter.Replicas()[0].Name != "rep-1" {
		t.Errorf("expected ring with only rep-1, got %+v", rAfter.Replicas())
	}
}

func TestActiveHealthPolling(t *testing.T) {
	ctx := context.Background()

	var rep0Healthy atomic.Bool
	rep0Healthy.Store(true)

	srv0 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			if rep0Healthy.Load() {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
			return
		}
		http.NotFound(w, r)
	}))
	defer srv0.Close()

	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv1.Close()

	disc := makeStaticDiscovery(t, "llama", srv0.URL, srv1.URL)
	cfg := health.ConfigFrom(config.Defaults().Router)
	cfg.FailureThreshold = 2
	cfg.SuccessThreshold = 1
	clock := health.NewFakeClock()
	cfg.Now = clock.Now
	tr := health.New(cfg, disc, nil, srv0.Client())

	// Initially healthy
	if err := tr.PollOnce(ctx); err != nil {
		t.Fatalf("poll error: %v", err)
	}
	if !tr.IsHealthy("rep-0") {
		t.Fatalf("rep-0 should be healthy")
	}

	// Cause rep-0 to fail
	rep0Healthy.Store(false)

	// First failure: should not eject yet (FailureThreshold is 2)
	_ = tr.PollOnce(ctx)
	if !tr.IsHealthy("rep-0") {
		t.Errorf("rep-0 should still be healthy after 1 failure")
	}

	// Second failure: should eject
	_ = tr.PollOnce(ctx)
	if tr.IsHealthy("rep-0") {
		t.Errorf("rep-0 should be ejected after 2 consecutive failures")
	}

	// Ring now only has rep-1
	r, err := tr.Ring("llama")
	if err != nil || len(r.Replicas()) != 1 {
		t.Fatalf("expected ring with 1 replica, got %+v (err: %v)", r, err)
	}

	// Now rep-0 recovers, and is re-admitted by the first good poll once
	// its ejection time has passed.
	rep0Healthy.Store(true)
	clock.Advance(cfg.MinEjection)
	_ = tr.PollOnce(ctx)

	// Should be restored
	if !tr.IsHealthy("rep-0") {
		t.Errorf("rep-0 should be restored after successful health check")
	}

	rRestored, err := tr.Ring("llama")
	if err != nil || len(rRestored.Replicas()) != 2 {
		t.Fatalf("expected ring with 2 replicas restored, got %+v (err: %v)", rRestored, err)
	}
}

// TestStuckEngineDetection: through the exported API (clock via
// Config.Now), stalled first-chunk waits eject a replica once
// stuck_engine_streams of them pile up with no first chunks on it, and a
// replica sending first chunks stays in.
func TestStuckEngineDetection(t *testing.T) {
	disc := makeStaticDiscovery(t, "llama", "http://127.0.0.1:8001", "http://127.0.0.1:8002")
	cfg := health.ConfigFrom(config.Defaults().Router)
	cfg.StuckEngineStreams = 2
	clock := health.NewFakeClock()
	cfg.Now = clock.Now
	tr := health.New(cfg, disc, nil, nil)
	rep0, rep1 := disc.Replicas("llama")[0], disc.Replicas("llama")[1]

	served := tr.StartFirstChunkWait(rep1, nil)
	stalled := []*health.FirstChunkWait{tr.StartFirstChunkWait(rep0, nil), tr.StartFirstChunkWait(rep0, nil)}
	tr.CheckStuckStreams()
	if !tr.IsHealthy("rep-0") {
		t.Fatal("rep-0 ejected before stuck_engine_timeout")
	}
	clock.Advance(cfg.StuckEngineTimeout / 2)
	served.Received()
	clock.Advance(cfg.StuckEngineTimeout / 2)
	tr.CheckStuckStreams()
	if tr.IsHealthy("rep-0") {
		t.Error("rep-0 should be ejected as a stuck engine after 2 stalled streams")
	}
	if !tr.IsHealthy("rep-1") {
		t.Error("rep-1 sent its first chunk and must stay in")
	}
	for _, w := range stalled {
		w.End()
	}
	if n := tr.FirstChunkWaits(); n != 0 {
		t.Errorf("waits = %d, want 0", n)
	}
}

func TestMidRunReplicaRemoval(t *testing.T) {
	// Proves AC-M1-4:
	// "0 failed requests except those in flight on the removed replica; routing rebalanced within 5 s"
	disc := makeStaticDiscovery(t, "chat-pool", "http://127.0.0.1:8001", "http://127.0.0.1:8002", "http://127.0.0.1:8003")
	tr := health.New(health.ConfigFrom(config.Defaults().Router), disc, nil, nil)

	// Phase 1: Traffic is routed across all 3 replicas
	for i := 0; i < 300; i++ {
		r, err := tr.Ring("chat-pool")
		if err != nil {
			t.Fatalf("phase 1 ring error: %v", err)
		}
		picked, err := r.Route(fmt.Sprintf("key-%d", i), nil, "")
		if err != nil {
			t.Fatalf("phase 1 route error: %v", err)
		}
		if picked.Name == "" {
			t.Fatalf("phase 1 empty picked replica")
		}
	}

	// Mid-run removal: rep-1 fails and is ejected
	rebalanceStart := time.Now()
	tr.Eject("rep-1", health.ReasonTransportFailure)
	rebalanceDuration := time.Since(rebalanceStart)

	// Phase 2: Concurrent requests immediately following removal
	const postRemovalRequests = 1000
	var successCount atomic.Int64
	var failCount atomic.Int64
	var routedToRemoved atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < postRemovalRequests; i++ {
		wg.Add(1)
		go func(reqID int) {
			defer wg.Done()

			r, err := tr.Ring("chat-pool")
			if err != nil {
				failCount.Add(1)
				return
			}

			key := fmt.Sprintf("conv-key-%d", reqID)
			picked, routeErr := r.Route(key, nil, "")
			if routeErr != nil {
				failCount.Add(1)
				return
			}

			if picked.Name == "rep-1" {
				routedToRemoved.Add(1)
			}

			successCount.Add(1)
		}(i)
	}

	wg.Wait()

	t.Logf("Post-removal: %d, Success: %d, Fail: %d, RoutedToRemoved: %d, Ejection Rebalance Time: %v",
		postRemovalRequests, successCount.Load(), failCount.Load(), routedToRemoved.Load(), rebalanceDuration)

	if failCount.Load() != 0 {
		t.Fatalf("expected 0 failed requests, got %d", failCount.Load())
	}
	if routedToRemoved.Load() != 0 {
		t.Fatalf("expected 0 requests routed to removed replica, got %d", routedToRemoved.Load())
	}
	if successCount.Load() != postRemovalRequests {
		t.Fatalf("expected all %d requests to succeed", postRemovalRequests)
	}
	if rebalanceDuration > 5*time.Second {
		t.Errorf("rebalance duration %v exceeded 5s target (AC-M1-4)", rebalanceDuration)
	}

	// Final ring only contains rep-0 and rep-2
	finalRing, err := tr.Ring("chat-pool")
	if err != nil {
		t.Fatalf("final ring error: %v", err)
	}
	if len(finalRing.Replicas()) != 2 {
		t.Errorf("expected final ring with 2 replicas, got %d", len(finalRing.Replicas()))
	}
	for _, rep := range finalRing.Replicas() {
		if rep.Name == "rep-1" {
			t.Errorf("removed replica rep-1 still in ring!")
		}
	}
}

// TestEjectionsCountedByPoolAndReason: every way a replica leaves the ring
// is counted in gateway_replica_ejections_total{pool,reason}, once per
// ejection, and none of them is reported as a fail-open.
func TestEjectionsCountedByPoolAndReason(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	disc := makeStaticDiscovery(t, "llama", "http://127.0.0.1:1", "http://127.0.0.1:2", srv.URL) // ports 1, 2: refused at once
	tm := telemetry.NewMetrics()
	cfg := health.ConfigFrom(config.Defaults().Router)
	cfg.FailureThreshold = 1
	cfg.PassiveFailureThreshold = 1
	tr := health.New(cfg, disc, tm, srv.Client())

	rep0 := disc.Replicas("llama")[0]
	tr.RecordTransportFailure(rep0, errors.New("dial tcp: connection refused"))
	tr.RecordTransportFailure(rep0, errors.New("dial tcp: connection refused")) // already out
	tr.Eject("rep-1", health.ReasonStuckEngine)
	tr.Eject("rep-1", health.ReasonStuckEngine) // already out for this reason
	_ = tr.PollOnce(context.Background())       // rep-2 fails /health; rep-0/1 are unreachable but already out

	rec := httptest.NewRecorder()
	tm.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		`gateway_replica_ejections_total{pool="llama",reason="transport_failure"} 1`,
		`gateway_replica_ejections_total{pool="llama",reason="stuck_engine"} 1`,
		`gateway_replica_ejections_total{pool="llama",reason="health_check_failed"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %s in:\n%s", want, body)
		}
	}
	if strings.Contains(body, `stage="health"`) {
		t.Errorf("ejections must not be counted as fail-opens:\n%s", body)
	}
}

// TestEjectionNotRecountedForNewReason: a replica already out of the ring
// (transport failure) that is then ejected as a stuck engine never re-entered
// the ring, so it is one ejection, not two.
func TestEjectionNotRecountedForNewReason(t *testing.T) {
	disc := makeStaticDiscovery(t, "llama", "http://127.0.0.1:1", "http://127.0.0.1:2")
	tm := telemetry.NewMetrics()
	cfg := health.ConfigFrom(config.Defaults().Router)
	cfg.PassiveFailureThreshold = 1
	tr := health.New(cfg, disc, tm, nil)

	tr.RecordTransportFailure(disc.Replicas("llama")[0], errors.New("dial tcp: connection refused"))
	tr.Eject("rep-0", health.ReasonStuckEngine)

	rec := httptest.NewRecorder()
	tm.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	if !strings.Contains(body, `gateway_replica_ejections_total{pool="llama",reason="transport_failure"} 1`) {
		t.Errorf("missing the transport_failure ejection:\n%s", body)
	}
	if strings.Contains(body, `reason="stuck_engine"`) {
		t.Errorf("re-ejection of an already ejected replica was counted:\n%s", body)
	}

	// After re-admission a new ejection counts again.
	tr.Restore("rep-0")
	tr.Eject("rep-0", health.ReasonStuckEngine)
	rec = httptest.NewRecorder()
	tm.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(rec.Body.String(), `gateway_replica_ejections_total{pool="llama",reason="stuck_engine"} 1`) {
		t.Errorf("ejection after re-admission not counted:\n%s", rec.Body.String())
	}
}
