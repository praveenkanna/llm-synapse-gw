package loadview_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/loadview"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

func TestInFlightTracker(t *testing.T) {
	v := loadview.New(loadview.Config{LoadView: config.Defaults().Router.LoadView}, nil, nil, nil)

	if got := v.InFlight("rep-1"); got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}

	var wg sync.WaitGroup
	const routines = 10
	const iters = 1000

	for i := 0; i < routines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				v.IncInFlight("rep-1")
			}
		}()
	}
	wg.Wait()

	if got := v.InFlight("rep-1"); got != routines*iters {
		t.Fatalf("expected %d, got %d", routines*iters, got)
	}

	for i := 0; i < routines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				v.DecInFlight("rep-1")
			}
		}()
	}
	wg.Wait()

	if got := v.InFlight("rep-1"); got != 0 {
		t.Fatalf("expected 0 after dec, got %d", got)
	}

	// Dec below 0 clamps to 0
	v.DecInFlight("rep-1")
	if got := v.InFlight("rep-1"); got != 0 {
		t.Fatalf("expected clamped 0, got %d", got)
	}
}

func TestMetricAliasesExtraction(t *testing.T) {
	ctx := context.Background()

	// Replica 1 uses modern vLLM metric names
	modernMetrics := `# HELP vllm:num_requests_waiting Number of waiting requests
# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting 7
# HELP vllm:kv_cache_usage_perc KV-cache usage
# TYPE vllm:kv_cache_usage_perc gauge
vllm:kv_cache_usage_perc 0.42
`
	modernSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			w.Write([]byte(modernMetrics))
			return
		}
		http.NotFound(w, r)
	}))
	defer modernSrv.Close()

	// Replica 2 uses legacy vLLM metric names
	legacyMetrics := `# HELP num_requests_waiting Number of waiting requests
# TYPE num_requests_waiting gauge
num_requests_waiting 3
# HELP vllm:gpu_cache_usage_perc GPU cache usage
# TYPE vllm:gpu_cache_usage_perc gauge
vllm:gpu_cache_usage_perc 0.88
`
	legacySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			w.Write([]byte(legacyMetrics))
			return
		}
		http.NotFound(w, r)
	}))
	defer legacySrv.Close()

	u1, _ := url.Parse(modernSrv.URL)
	u2, _ := url.Parse(legacySrv.URL)

	r1 := discovery.Replica{Pool: "llama", Name: "rep-modern", BaseURL: u1}
	r2 := discovery.Replica{Pool: "llama", Name: "rep-legacy", BaseURL: u2}

	lister := loadview.ReplicaListerFunc(func() []discovery.Replica {
		return []discovery.Replica{r1, r2}
	})

	tm := telemetry.NewMetrics()
	v := loadview.New(loadview.Config{LoadView: config.Defaults().Router.LoadView}, lister, tm, modernSrv.Client())

	err := v.ScrapeOnce(ctx)
	if err != nil {
		t.Fatalf("scrape error: %v", err)
	}

	// Verify modern replica state
	s1, ok1 := v.State("rep-modern")
	if !ok1 {
		t.Fatalf("expected state for rep-modern")
	}
	if s1.WaitingRequests != 7 {
		t.Errorf("rep-modern waiting = %v, want 7", s1.WaitingRequests)
	}
	if s1.KVCacheUsage != 0.42 {
		t.Errorf("rep-modern kv = %v, want 0.42", s1.KVCacheUsage)
	}

	// Verify legacy replica state
	s2, ok2 := v.State("rep-legacy")
	if !ok2 {
		t.Fatalf("expected state for rep-legacy")
	}
	if s2.WaitingRequests != 3 {
		t.Errorf("rep-legacy waiting = %v, want 3", s2.WaitingRequests)
	}
	if s2.KVCacheUsage != 0.88 {
		t.Errorf("rep-legacy kv = %v, want 0.88", s2.KVCacheUsage)
	}

	// Verify combined load calculation
	v.IncInFlight("rep-modern")
	v.IncInFlight("rep-modern")
	// in-flight = 2, waiting = 7 => combined load = 9
	if load := v.Load(r1); load != 9 {
		t.Errorf("expected combined load 9, got %d", load)
	}
}

func TestMissingMetricsFailOpen(t *testing.T) {
	ctx := context.Background()

	// Server emitting metrics without any waiting/KV metrics
	incompleteMetrics := `some_other_metric 123`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(incompleteMetrics))
	}))
	defer srv.Close()

	u, _ := url.Parse(srv.URL)
	r := discovery.Replica{Pool: "llama", Name: "rep-sparse", BaseURL: u}
	lister := loadview.ReplicaListerFunc(func() []discovery.Replica {
		return []discovery.Replica{r}
	})

	tm := telemetry.NewMetrics()
	v := loadview.New(loadview.Config{LoadView: config.Defaults().Router.LoadView}, lister, tm, srv.Client())

	_ = v.ScrapeOnce(ctx)

	v.IncInFlight("rep-sparse")
	v.IncInFlight("rep-sparse")

	// Missing metrics falls back gracefully to in-flight
	load := v.Load(r)
	if load != 2 {
		t.Errorf("expected fallback load 2, got %d", load)
	}

	// Verify failopen metric recorded
	rec := httptest.NewRecorder()
	tm.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	want := fmt.Sprintf(`%s{stage="%s",reason="%s"}`, telemetry.MetricFailOpen, telemetry.StageLoadview, loadview.ReasonMetricMissing)
	if !strings.Contains(body, want) {
		t.Errorf("expected failopen metric %q in:\n%s", want, body)
	}
}

func TestCapabilityProbing(t *testing.T) {
	ctx := context.Background()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/version":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"version": "0.6.3.post1"}`))
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{
				"object": "list",
				"data": [
					{
						"id": "meta-llama/Llama-3.1-8B-Instruct",
						"max_model_len": 131072
					}
				]
			}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	u, _ := url.Parse(srv.URL)
	r := discovery.Replica{Pool: "llama-8b", Name: "worker-0", BaseURL: u}
	lister := loadview.ReplicaListerFunc(func() []discovery.Replica {
		return []discovery.Replica{r}
	})

	tm := telemetry.NewMetrics()
	v := loadview.New(loadview.Config{LoadView: config.Defaults().Router.LoadView}, lister, tm, srv.Client())

	err := v.ProbeOnce(ctx)
	if err != nil {
		t.Fatalf("probe error: %v", err)
	}

	s, ok := v.State("worker-0")
	if !ok {
		t.Fatalf("expected state for worker-0")
	}
	if s.VLLMVersion != "0.6.3.post1" {
		t.Errorf("vLLM version = %q, want '0.6.3.post1'", s.VLLMVersion)
	}
	if len(s.Models) != 1 || s.Models[0] != "meta-llama/Llama-3.1-8B-Instruct" {
		t.Errorf("models = %v, want ['meta-llama/Llama-3.1-8B-Instruct']", s.Models)
	}
	if s.MaxModelLen != 131072 {
		t.Errorf("max_model_len = %d, want 131072", s.MaxModelLen)
	}

	// Verify gateway_replica_info metric export
	rec := httptest.NewRecorder()
	tm.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	wantMetric := fmt.Sprintf(`%s{pool="llama-8b",replica="worker-0",vllm_version="0.6.3.post1"} 1`, telemetry.MetricReplicaInfo)
	if !strings.Contains(body, wantMetric) {
		t.Errorf("expected replica info metric %q in:\n%s", wantMetric, body)
	}
}

// TestStartDoesNotWaitForReplicas: with a replica that accepts connections
// and never answers, Start returns at once and the first probe and scrape
// still run, in the background.
func TestStartDoesNotWaitForReplicas(t *testing.T) {
	release := make(chan struct{})
	hits := make(chan string, 16)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits <- r.URL.Path
		select { // blackhole: hold the request until the test or client gives up
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)

	u, _ := url.Parse(srv.URL)
	rep := discovery.Replica{Pool: "llama", Name: "blackhole", BaseURL: u}
	cfg := loadview.Config{LoadView: config.Defaults().Router.LoadView}
	cfg.ScrapeTimeout = time.Hour // a synchronous first scrape would hang Start
	v := loadview.New(cfg, loadview.ReplicaListerFunc(func() []discovery.Replica {
		return []discovery.Replica{rep}
	}), telemetry.NewMetrics(), srv.Client())

	started := make(chan struct{})
	go func() {
		v.Start(context.Background())
		close(started)
	}()
	select {
	case <-started:
	case <-time.After(10 * time.Second): // failure guard only; passing never waits
		t.Fatal("Start blocked on a replica that never answers")
	}

	// The first scrape and probe were issued from the background goroutines.
	seen := map[string]bool{}
	for !seen["/metrics"] || !seen["/version"] {
		select {
		case p := <-hits:
			seen[p] = true
		case <-time.After(10 * time.Second):
			t.Fatalf("background scrape/probe never reached the replica; saw %v", seen)
		}
	}
	if load := v.Load(rep); load != 0 {
		t.Errorf("Load with no metrics yet = %d, want 0 (in-flight only)", load)
	}
	v.Stop() // cancels the hung requests and waits for the goroutines
}

// TestRequestsCarryUpstreamKey: against a replica that requires the
// upstream key (vLLM --api-key guards /v1/models; here every path is
// guarded, as a proxy might), the view's probes and scrapes send it and
// get the model list, version and metrics; without the key the model list
// stays empty. The key never shows in metrics or a formatted config.
func TestRequestsCarryUpstreamKey(t *testing.T) {
	const key = "lv-secret-upstream-key"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+key {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"Unauthorized"}`))
			return
		}
		switch r.URL.Path {
		case "/version":
			_, _ = w.Write([]byte(`{"version":"0.8.5"}`))
		case "/v1/models":
			_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"org/model","max_model_len":4096}]}`))
		case "/metrics":
			_, _ = w.Write([]byte("vllm:num_requests_waiting 3\n"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	rep := discovery.Replica{Pool: "p", Name: "r0", BaseURL: u}
	lister := loadview.ReplicaListerFunc(func() []discovery.Replica { return []discovery.Replica{rep} })

	without := loadview.New(loadview.Config{LoadView: config.Defaults().Router.LoadView}, lister, nil, srv.Client())
	_ = without.ProbeOnce(context.Background())
	if s, _ := without.State("r0"); len(s.Models) != 0 {
		t.Fatalf("models without the key = %v; the replica should have refused", s.Models)
	}

	cfg := loadview.Config{LoadView: config.Defaults().Router.LoadView}
	cfg.APIKey = key
	tm := telemetry.NewMetrics()
	v := loadview.New(cfg, lister, tm, srv.Client())
	_ = v.ProbeOnce(context.Background())
	_ = v.ScrapeOnce(context.Background())
	s, ok := v.State("r0")
	if !ok || len(s.Models) != 1 || s.Models[0] != "org/model" || s.VLLMVersion != "0.8.5" || !s.HasWaiting || s.WaitingRequests != 3 {
		t.Fatalf("state with the key = %+v (ok %v); want model, version and waiting requests", s, ok)
	}
	rec := httptest.NewRecorder()
	tm.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if strings.Contains(rec.Body.String(), key) {
		t.Error("metrics expose the upstream key")
	}
	if strings.Contains(fmt.Sprintf("%+v %v", cfg, cfg), key) {
		t.Error("a formatted load view config exposes the upstream key")
	}
}

// TestConfigReachesTheScrape: the load view reads router.load_view as
// given: a configured alias is what it looks for, and the scrape timeout
// bounds a replica that never answers.
func TestConfigReachesTheScrape(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Hang") != "" {
			select { // gives up, so an ignored scrape timeout fails the test rather than hanging it
			case <-release:
			case <-time.After(2 * time.Second):
			}
		}
		if r.URL.Path == "/metrics" {
			_, _ = w.Write([]byte("custom:waiting 5\ncustom:running 3\ncustom:kv 0.25\ncustom:queries 100\ncustom:hits 40\n"))
		}
	}))
	defer srv.Close()
	defer close(release)
	u, _ := url.Parse(srv.URL)
	rep := discovery.Replica{Pool: "p", Name: "r", BaseURL: u}
	lister := loadview.ReplicaListerFunc(func() []discovery.Replica { return []discovery.Replica{rep} })

	lv := config.Defaults().Router.LoadView
	lv.MetricAliases = map[string][]string{
		config.SignalWaitingRequests:    {"custom:waiting"},
		config.SignalRunningRequests:    {"custom:running"},
		config.SignalKVCacheUsage:       {"custom:kv"},
		config.SignalPrefixCacheQueries: {"custom:queries"},
		config.SignalPrefixCacheHits:    {"custom:hits"},
	}
	v := loadview.New(loadview.Config{LoadView: lv}, lister, nil, srv.Client())
	if err := v.ScrapeOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s, _ := v.State("r"); !s.HasWaiting || s.WaitingRequests != 5 || s.RunningRequests != 3 || s.KVCacheUsage != 0.25 ||
		s.PrefixCacheQueries != 100 || s.PrefixCacheHits != 40 {
		t.Fatalf("state = %+v, want every signal read through its configured alias", s)
	}

	lv.ScrapeTimeout = 50 * time.Millisecond
	hung := &http.Client{Transport: hangTransport{srv.Client().Transport}}
	v = loadview.New(loadview.Config{LoadView: lv}, lister, nil, hung)
	start := time.Now()
	_ = v.ScrapeOnce(context.Background())
	if d := time.Since(start); d > time.Second {
		t.Fatalf("scrape of a hung replica took %v, want it bounded by the 50ms scrape timeout", d)
	}
}

// hangTransport marks every request so the test server holds it.
type hangTransport struct{ base http.RoundTripper }

func (h hangTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("X-Hang", "1")
	return h.base.RoundTrip(r)
}
