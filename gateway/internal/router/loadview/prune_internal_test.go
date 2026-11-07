package loadview

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// vllmServer answers /metrics, /version and /v1/models under any base path.
func vllmServer(t *testing.T, version string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/metrics"):
			_, _ = w.Write([]byte("vllm:num_requests_waiting 3\n"))
		case strings.HasSuffix(r.URL.Path, "/version"):
			_, _ = w.Write([]byte(`{"version":"` + version + `"}`))
		case strings.HasSuffix(r.URL.Path, "/v1/models"):
			_, _ = w.Write([]byte(`{"data":[{"id":"m","max_model_len":4096}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func replicaAt(t *testing.T, name, raw string) discovery.Replica {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return discovery.Replica{Pool: "p", Name: name, BaseURL: u}
}

// switchLister returns whatever replica set was last stored.
type switchLister struct {
	reps atomic.Pointer[[]discovery.Replica]
}

func (l *switchLister) set(reps ...discovery.Replica) { l.reps.Store(&reps) }
func (l *switchLister) AllReplicas() []discovery.Replica {
	return *l.reps.Load()
}

func counters(v *View) map[string]int64 {
	out := map[string]int64{}
	v.inFlight.Range(func(k, c any) bool {
		out[k.(string)] = c.(*atomic.Int64).Load()
		return true
	})
	return out
}

func metricsBody(t *testing.T, m *telemetry.Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return rec.Body.String()
}

// TestRemovedReplicaIsPruned: a replica the lister no longer returns loses
// its state and replica-info series at the next pass; its in-flight counter
// survives while a lease holds it, a release after the removal is a safe
// no-op, and the idle counter goes at the following pass.
func TestRemovedReplicaIsPruned(t *testing.T) {
	ctx := context.Background()
	srv := vllmServer(t, "0.6.3")
	a := replicaAt(t, "a", srv.URL)
	b := replicaAt(t, "b", srv.URL+"/b")
	lister := &switchLister{}
	lister.set(a, b)
	tm := telemetry.NewMetrics()
	v := New(testConfig(), lister, tm, srv.Client())

	_ = v.ProbeOnce(ctx)
	_ = v.ScrapeOnce(ctx)
	if _, ok := v.State("b"); !ok {
		t.Fatal("no state for b before removal")
	}
	if body := metricsBody(t, tm); !strings.Contains(body, `gateway_replica_info{pool="p",replica="b",vllm_version="0.6.3"} 1`) {
		t.Fatalf("b's replica info missing before removal:\n%s", body)
	}
	v.IncInFlight("a")
	v.IncInFlight("b") // two leases held on b across the removal
	v.IncInFlight("b")

	lister.set(a) // reload removes b
	_ = v.ScrapeOnce(ctx)
	if _, ok := v.State("b"); ok {
		t.Error("state for removed b still present")
	}
	if body := metricsBody(t, tm); strings.Contains(body, `replica="b"`) {
		t.Errorf("series for removed b still exported:\n%s", body)
	}
	if got := counters(v)["b"]; got != 2 {
		t.Fatalf("b's in-flight counter = %d with two leases held, want 2", got)
	}

	// The leases release after the removal, one of them twice (a late
	// duplicate): never negative, nothing recreated.
	if got := v.DecInFlight("b"); got != 1 {
		t.Errorf("first release = %d, want 1", got)
	}
	if got := v.DecInFlight("b"); got != 0 {
		t.Errorf("second release = %d, want 0", got)
	}
	_ = v.ProbeOnce(ctx) // idle now: pruned
	if _, ok := counters(v)["b"]; ok {
		t.Errorf("idle counter for removed b kept: %v", counters(v))
	}
	if got := v.DecInFlight("b"); got != 0 {
		t.Errorf("release after pruning = %d, want 0", got)
	}
	if got := v.InFlight("b"); got != 0 {
		t.Errorf("InFlight(b) = %d, want 0", got)
	}
	if _, ok := counters(v)["b"]; ok {
		t.Error("release or read after pruning recreated b's counter")
	}
	if got := counters(v)["a"]; got != 1 {
		t.Errorf("a's in-flight = %d, want 1 (untouched)", got)
	}
	if _, ok := v.State("a"); !ok {
		t.Error("state for a, still listed, was pruned")
	}
}

// TestRepointedReplicaDropsOldState: a scrape or probe of a replica's old
// address that finishes after the reload stores nothing, and the
// replica's state from the old address is dropped.
func TestRepointedReplicaDropsOldState(t *testing.T) {
	ctx := context.Background()
	oldSrv := vllmServer(t, "0.6.3")
	newSrv := vllmServer(t, "0.7.0")
	oldA := replicaAt(t, "a", oldSrv.URL)
	newA := replicaAt(t, "a", newSrv.URL)
	lister := &switchLister{}
	lister.set(oldA)
	tm := telemetry.NewMetrics()
	v := New(testConfig(), lister, tm, &http.Client{})
	_ = v.ProbeOnce(ctx)
	if st, _ := v.State("a"); st.VLLMVersion != "0.6.3" {
		t.Fatalf("version before re-point = %q", st.VLLMVersion)
	}

	lister.set(newA)
	v.prune(lister.AllReplicas())
	if _, ok := v.State("a"); ok {
		t.Error("state from a's old address kept after re-point")
	}
	v.probeReplica(ctx, oldA) // started before the reload, finished after
	v.scrapeReplica(ctx, oldA)
	if _, ok := v.State("a"); ok {
		t.Error("a probe of the old address stored state after the re-point")
	}
	_ = v.ProbeOnce(ctx)
	if st, _ := v.State("a"); st.VLLMVersion != "0.7.0" {
		t.Errorf("version after re-point = %q, want 0.7.0", st.VLLMVersion)
	}
	if body := metricsBody(t, tm); strings.Contains(body, `vllm_version="0.6.3"`) {
		t.Errorf("old address's version still exported:\n%s", body)
	}
}

// TestRepointedReplicaKeepsInFlight: re-pointing drops scraped state, but a
// request in flight on the replica stays counted and releases to zero.
func TestRepointedReplicaKeepsInFlight(t *testing.T) {
	lister := &switchLister{}
	lister.set(replicaAt(t, "a", "http://10.0.0.1:8000"))
	v := New(testConfig(), lister, telemetry.NewMetrics(), nil)
	v.prune(lister.AllReplicas())
	v.IncInFlight("a")

	lister.set(replicaAt(t, "a", "http://10.0.0.9:9000"))
	v.prune(lister.AllReplicas())
	if got := v.InFlight("a"); got != 1 {
		t.Fatalf("InFlight(a) after re-point = %d, want 1", got)
	}
	if got := v.DecInFlight("a"); got != 0 {
		t.Fatalf("release after re-point = %d, want 0", got)
	}
	if got := v.InFlight("a"); got != 0 {
		t.Fatalf("InFlight(a) = %d, want 0", got)
	}
}

// TestPruneNeverLosesAnIncrement: leases acquired on an unlisted replica
// while prune runs over and over are all counted, and all release to zero;
// a retired counter is never revived or decremented into.
func TestPruneNeverLosesAnIncrement(t *testing.T) {
	lister := &switchLister{}
	lister.set(replicaAt(t, "a", "http://10.0.0.1:8000")) // "b" is not listed
	v := New(testConfig(), lister, nil, nil)

	const workers, rounds = 8, 2000
	stop := make(chan struct{})
	pruned := make(chan struct{})
	go func() {
		defer close(pruned)
		for {
			select {
			case <-stop:
				return
			default:
				v.prune(lister.AllReplicas())
			}
		}
	}()
	var wg sync.WaitGroup
	var lost atomic.Int64
	for range workers {
		wg.Go(func() {
			for range rounds {
				v.IncInFlight("b")
				if v.InFlight("b") < 1 { // our own lease must be counted
					lost.Add(1)
				}
				v.DecInFlight("b")
			}
			v.IncInFlight("b") // held to the end
		})
	}
	wg.Wait()
	if n := lost.Load(); n > 0 {
		t.Errorf("a held lease was not counted %d times (increment lost to a pruned counter)", n)
	}
	close(stop)
	<-pruned

	if got := v.InFlight("b"); got != workers {
		t.Fatalf("InFlight(b) with %d leases held = %d", workers, got)
	}
	for range workers {
		v.DecInFlight("b")
	}
	if got := v.InFlight("b"); got != 0 {
		t.Fatalf("InFlight(b) after every release = %d", got)
	}
	v.prune(lister.AllReplicas())
	if _, ok := counters(v)["b"]; ok {
		t.Fatal("idle counter for unlisted b kept after prune")
	}
}
