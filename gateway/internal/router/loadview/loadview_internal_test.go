package loadview

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// fakeClock is a manually advanced clock, so staleness is tested without
// sleeping.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// replicaServer serves /metrics with whatever body and status are current.
type replicaServer struct {
	*httptest.Server
	body   atomic.Pointer[string]
	status atomic.Int64
}

func newReplicaServer(t *testing.T, body string) *replicaServer {
	t.Helper()
	rs := &replicaServer{}
	rs.set(http.StatusOK, body)
	rs.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(int(rs.status.Load()))
		_, _ = w.Write([]byte(*rs.body.Load()))
	}))
	t.Cleanup(rs.Close)
	return rs
}

func (rs *replicaServer) set(status int, body string) {
	rs.status.Store(int64(status))
	rs.body.Store(&body)
}

func (rs *replicaServer) replica() discovery.Replica {
	u, _ := url.Parse(rs.URL)
	return discovery.Replica{Pool: "llama", Name: "r0", BaseURL: u}
}

func newTestView(t *testing.T, rs *replicaServer) (*View, *telemetry.Metrics, *fakeClock) {
	t.Helper()
	tm := telemetry.NewMetrics()
	rep := rs.replica()
	v := New(testConfig(), ReplicaListerFunc(func() []discovery.Replica { return []discovery.Replica{rep} }), tm, rs.Client())
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	v.now = clk.now
	return v, tm, clk
}

func failOpenCount(t *testing.T, tm *telemetry.Metrics, reason string) int {
	t.Helper()
	rec := httptest.NewRecorder()
	tm.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	re := regexp.MustCompile(fmt.Sprintf(`(?m)^%s\{stage="%s",reason="%s"\} (\d+)$`, telemetry.MetricFailOpen, telemetry.StageLoadview, reason))
	m := re.FindStringSubmatch(rec.Body.String())
	if m == nil {
		return 0
	}
	n, _ := strconv.Atoi(m[1])
	return n
}

const waitingMetrics = "vllm:num_requests_waiting 7\n"

func TestLoadNeverCountsFailOpen(t *testing.T) {
	rs := newReplicaServer(t, "unrelated_metric 1\n")
	v, tm, clk := newTestView(t, rs)
	rep := rs.replica()

	for range 100 { // no state at all yet
		v.Load(rep)
	}
	if n := failOpenCount(t, tm, ReasonMetricMissing) + failOpenCount(t, tm, ReasonMetricsStale); n != 0 {
		t.Fatalf("Load counted %d fail-opens with no scrape, want 0", n)
	}

	rs.set(http.StatusOK, waitingMetrics)
	_ = v.ScrapeOnce(context.Background())
	clk.advance(time.Hour) // metrics now stale
	for range 100 {
		v.Load(rep)
	}
	if n := failOpenCount(t, tm, ReasonMetricsStale); n != 0 {
		t.Fatalf("Load counted %d stale fail-opens, want 0 (counted per scrape)", n)
	}
}

func TestFailOpenCountedOncePerScrape(t *testing.T) {
	ctx := context.Background()
	rs := newReplicaServer(t, "unrelated_metric 1\n")
	v, tm, clk := newTestView(t, rs)
	rep := rs.replica()
	v.IncInFlight(rep.Name)

	// Scrape works but has no waiting metric: one metric_missing per scrape,
	// however many routing decisions read the load in between.
	for scrape := 1; scrape <= 3; scrape++ {
		_ = v.ScrapeOnce(ctx)
		for range 50 {
			if load := v.Load(rep); load != 1 {
				t.Fatalf("Load = %d, want in-flight 1", load)
			}
		}
		if n := failOpenCount(t, tm, ReasonMetricMissing); n != scrape {
			t.Fatalf("after %d scrapes metric_missing = %d, want %d", scrape, n, scrape)
		}
	}

	// Fresh waiting metric: no fail-open, waiting adds to the load.
	rs.set(http.StatusOK, waitingMetrics)
	_ = v.ScrapeOnce(ctx)
	if load := v.Load(rep); load != 8 {
		t.Fatalf("Load = %d, want in-flight 1 + waiting 7", load)
	}

	// Scrape fails while the last good data is still fresh: nothing counted,
	// the fresh data keeps being used.
	rs.set(http.StatusInternalServerError, "")
	clk.advance(config.DefaultLoadViewStaleThreshold / 2)
	_ = v.ScrapeOnce(ctx)
	if n := failOpenCount(t, tm, ReasonMetricsStale); n != 0 {
		t.Fatalf("metrics_stale = %d while data is fresh, want 0", n)
	}
	if load := v.Load(rep); load != 8 {
		t.Fatalf("Load = %d, want 8 while data is fresh", load)
	}

	// Past the staleness window: Load falls back to in-flight, and each
	// failed scrape counts metrics_stale once.
	clk.advance(config.DefaultLoadViewStaleThreshold)
	for range 50 {
		if load := v.Load(rep); load != 1 {
			t.Fatalf("Load = %d with stale metrics, want in-flight 1", load)
		}
	}
	if n := failOpenCount(t, tm, ReasonMetricsStale); n != 0 {
		t.Fatalf("metrics_stale = %d before the next scrape, want 0", n)
	}
	_ = v.ScrapeOnce(ctx)
	_ = v.ScrapeOnce(ctx)
	if n := failOpenCount(t, tm, ReasonMetricsStale); n != 2 {
		t.Fatalf("metrics_stale = %d after 2 failed scrapes, want 2", n)
	}
	if s, _ := v.State(rep.Name); !s.Stale {
		t.Error("State().Stale = false past the staleness window")
	}
}

func TestFailedFirstScrapeCountsStale(t *testing.T) {
	rs := newReplicaServer(t, "")
	rs.set(http.StatusServiceUnavailable, "")
	v, tm, _ := newTestView(t, rs)
	_ = v.ScrapeOnce(context.Background())
	if n := failOpenCount(t, tm, ReasonMetricsStale); n != 1 {
		t.Fatalf("metrics_stale = %d after a failed first scrape, want 1", n)
	}
}

func TestScrapeAbortedByShutdownIsNotCounted(t *testing.T) {
	rs := newReplicaServer(t, waitingMetrics)
	v, tm, _ := newTestView(t, rs)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = v.ScrapeOnce(ctx)
	if n := failOpenCount(t, tm, ReasonMetricsStale); n != 0 {
		t.Fatalf("metrics_stale = %d for a scrape cancelled by shutdown, want 0", n)
	}
}

func TestWaitingMetricDisappearing(t *testing.T) {
	ctx := context.Background()
	rs := newReplicaServer(t, waitingMetrics)
	v, _, _ := newTestView(t, rs)
	rep := rs.replica()
	_ = v.ScrapeOnce(ctx)
	if load := v.Load(rep); load != 7 {
		t.Fatalf("Load = %d, want waiting 7", load)
	}
	rs.set(http.StatusOK, "unrelated_metric 1\n")
	_ = v.ScrapeOnce(ctx)
	if load := v.Load(rep); load != 0 {
		t.Fatalf("Load = %d after the waiting metric vanished, want 0 (no stale carry-over)", load)
	}
}

func TestMultiEngineSignals(t *testing.T) {
	body := `# HELP vllm:num_requests_waiting Waiting requests.
# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting{engine="0",model_name="meta-llama/Llama 3.1 8B"} 3
vllm:num_requests_waiting{engine="1",model_name="meta-llama/Llama 3.1 8B"} 4
vllm:num_requests_running{engine="0",model_name="meta-llama/Llama 3.1 8B"} 2
vllm:num_requests_running{engine="1",model_name="meta-llama/Llama 3.1 8B"} 5
vllm:kv_cache_usage_perc{engine="0"} 0.2
vllm:kv_cache_usage_perc{engine="1"} 0.6
vllm:prefix_cache_queries_total{engine="0"} 100
vllm:prefix_cache_queries_total{engine="1"} 300
vllm:prefix_cache_hits_total{engine="0"} 50
vllm:prefix_cache_hits_total{engine="1"} 150
`
	rs := newReplicaServer(t, body)
	v, _, _ := newTestView(t, rs)
	_ = v.ScrapeOnce(context.Background())
	s, ok := v.State("r0")
	if !ok {
		t.Fatal("no state after scrape")
	}
	if s.WaitingRequests != 7 || !s.HasWaiting {
		t.Errorf("waiting = %v (has %v), want 7 summed over engines", s.WaitingRequests, s.HasWaiting)
	}
	if s.RunningRequests != 7 {
		t.Errorf("running = %v, want 7", s.RunningRequests)
	}
	if s.KVCacheUsage < 0.3999 || s.KVCacheUsage > 0.4001 {
		t.Errorf("kv usage = %v, want mean 0.4", s.KVCacheUsage)
	}
	if s.PrefixCacheQueries != 400 || s.PrefixCacheHits != 200 {
		t.Errorf("prefix cache queries/hits = %v/%v, want 400/200", s.PrefixCacheQueries, s.PrefixCacheHits)
	}
	if load := v.Load(rs.replica()); load != 7 {
		t.Errorf("Load = %d, want 7", load)
	}
}

func TestLegacyPrefixCacheNames(t *testing.T) {
	rs := newReplicaServer(t, "vllm:gpu_prefix_cache_queries_total 10\nvllm:gpu_prefix_cache_hits_total 4\n")
	v, _, _ := newTestView(t, rs)
	_ = v.ScrapeOnce(context.Background())
	s, _ := v.State("r0")
	if s.PrefixCacheQueries != 10 || s.PrefixCacheHits != 4 {
		t.Errorf("legacy prefix cache = %v/%v, want 10/4", s.PrefixCacheQueries, s.PrefixCacheHits)
	}
}

func TestParsePrometheusMetrics(t *testing.T) {
	raw := strings.Join([]string{
		`# HELP x A help line with {braces} and "quotes"`,
		`# TYPE x gauge`,
		`plain 1`,
		`plain 2`,
		`spaced{model="a b c",engine="0"} 3`,
		`spaced{model="a b c",engine="1"} 4 1700000000000`,
		`escaped{v="say \"hi\" } now",w="back\\slash"} 5`,
		"tabbed\t6",
		`empty_labels{} 7`,
		`nan_value NaN`,
		`inf_value{a="1"} +Inf`,
		`neg_inf -Inf`,
		`unterminated{a="1 2`,
		`no_value{a="b"}`,
		`bad_value abc`,
		`too many fields 1 2`,
		``,
		`   `,
		`sci 1.5e3`,
		`exemplar_total{a="b"} 4 # {trace_id="x y"} 1.0 1700000000`,
		`exemplar_ts 2 1700000000 # {t="1"} 1`,
	}, "\n")
	got := ParsePrometheusMetrics([]byte(raw))
	want := map[string]Series{
		"plain":          {Sum: 3, Count: 2},
		"spaced":         {Sum: 7, Count: 2},
		"escaped":        {Sum: 5, Count: 1},
		"tabbed":         {Sum: 6, Count: 1},
		"empty_labels":   {Sum: 7, Count: 1},
		"sci":            {Sum: 1500, Count: 1},
		"exemplar_total": {Sum: 4, Count: 1},
		"exemplar_ts":    {Sum: 2, Count: 1},
	}
	if len(got) != len(want) {
		t.Errorf("parsed %d names %v, want %d", len(got), got, len(want))
	}
	for name, w := range want {
		if g := got[name]; g != w {
			t.Errorf("%s = %+v, want %+v", name, g, w)
		}
	}
}

func TestNormalizeVersion(t *testing.T) {
	tests := []struct{ in, want string }{
		{"0.6.3", "0.6.3"},
		{"0.6.3.post1", "0.6.3.post1"},
		{"0.8.5+cu124", "0.8.5+cu124"},
		{"v1.0.0-rc1", "v1.0.0-rc1"},
		{strings.Repeat("1", MaxVersionLen), strings.Repeat("1", MaxVersionLen)},
		{strings.Repeat("1", MaxVersionLen+1), telemetry.VersionUnknown},
		{"<html><body>Not Found</body></html>", telemetry.VersionUnknown},
		{"0.6.3 beta", telemetry.VersionUnknown},
		{"0.6.3\n", telemetry.VersionUnknown},
		{`0.6"3`, telemetry.VersionUnknown},
		{".hidden", telemetry.VersionUnknown},
		{"", telemetry.VersionUnknown},
	}
	for _, tc := range tests {
		if got := NormalizeVersion(tc.in); got != tc.want {
			t.Errorf("NormalizeVersion(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestReplicaInfoVersionLabel drives /version through the prober: valid
// versions export as-is, junk exports as "unknown" (never the body), and a
// changed version replaces the old series.
func TestReplicaInfoVersionLabel(t *testing.T) {
	var version atomic.Pointer[string]
	set := func(s string) { version.Store(&s) }
	set(`{"version":"0.6.3"}`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/version" {
			_, _ = w.Write([]byte(*version.Load()))
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	rep := discovery.Replica{Pool: "llama", Name: "r0", BaseURL: u}
	tm := telemetry.NewMetrics()
	v := New(testConfig(), ReplicaListerFunc(func() []discovery.Replica { return []discovery.Replica{rep} }), tm, srv.Client())

	infoLines := func() []string {
		rec := httptest.NewRecorder()
		tm.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		var out []string
		for _, l := range strings.Split(rec.Body.String(), "\n") {
			if strings.HasPrefix(l, telemetry.MetricReplicaInfo+"{") {
				out = append(out, l)
			}
		}
		return out
	}
	steps := []struct{ body, want string }{
		{`{"version":"0.6.3"}`, "0.6.3"},
		{`{"version":"0.7.0"}`, "0.7.0"}, // upgrade: old series deleted
		{"0.7.1\n", "0.7.1"},             // plain-text body
		{"<html>" + strings.Repeat("x", 60<<10) + "</html>", telemetry.VersionUnknown},
		{`{"version":"` + strings.Repeat("9", 40) + `"}`, telemetry.VersionUnknown},
	}
	for _, st := range steps {
		set(st.body)
		_ = v.ProbeOnce(context.Background())
		want := fmt.Sprintf(`%s{pool="llama",replica="r0",vllm_version="%s"} 1`, telemetry.MetricReplicaInfo, st.want)
		if got := infoLines(); len(got) != 1 || got[0] != want {
			t.Fatalf("after /version %.40q: replica info = %q, want exactly [%s]", st.body, got, want)
		}
		if s, _ := v.State("r0"); s.VLLMVersion != st.want {
			t.Errorf("state version = %q, want %q", s.VLLMVersion, st.want)
		}
	}
}

// TestHugeWaitingValueIsClamped: a bogus huge waiting value must not
// overflow int64 (which would make the busiest replica look idle).
func TestHugeWaitingValueIsClamped(t *testing.T) {
	for _, body := range []string{"vllm:num_requests_waiting 1e300\n", "vllm:num_requests_waiting 9.3e18\n"} {
		rs := newReplicaServer(t, body)
		v, _, _ := newTestView(t, rs)
		rep := rs.replica()
		v.IncInFlight(rep.Name)
		_ = v.ScrapeOnce(context.Background())
		if load := v.Load(rep); load != 1+MaxWaitingRequests {
			t.Errorf("%q: Load = %d, want in-flight 1 + cap %d", body, load, MaxWaitingRequests)
		}
	}
}

// TestOversizedMetricsDiscarded: a /metrics body over the ceiling is not
// parsed from its cut-off prefix; the scrape is discarded (counted once as
// metrics_too_large) and the previous data stays in use.
func TestOversizedMetricsDiscarded(t *testing.T) {
	ctx := context.Background()
	rs := newReplicaServer(t, waitingMetrics)
	v, tm, _ := newTestView(t, rs)
	rep := rs.replica()
	_ = v.ScrapeOnce(ctx)

	// The cut would fall inside the last sample: "...waiting 12" read as 1.
	padding := "# " + strings.Repeat("x", maxMetricsBodyBytes-len("# \n")-len("vllm:num_requests_waiting 1")) + "\n"
	rs.set(http.StatusOK, padding+"vllm:num_requests_waiting 12\n")
	_ = v.ScrapeOnce(ctx)
	if load := v.Load(rep); load != 7 {
		t.Fatalf("Load = %d after an oversized scrape, want the previous 7", load)
	}
	if n := failOpenCount(t, tm, ReasonMetricsTooLarge); n != 1 {
		t.Fatalf("metrics_too_large = %d, want 1", n)
	}
	if n := failOpenCount(t, tm, ReasonMetricMissing); n != 0 {
		t.Fatalf("metric_missing = %d, want 0 (the body was not parsed)", n)
	}

	// Exactly at the ceiling is accepted.
	exact := "vllm:num_requests_waiting 12\n"
	exact = "# " + strings.Repeat("x", maxMetricsBodyBytes-len(exact)-len("# \n")) + "\n" + exact
	if len(exact) != maxMetricsBodyBytes {
		t.Fatalf("test body is %d bytes, want %d", len(exact), maxMetricsBodyBytes)
	}
	rs.set(http.StatusOK, exact)
	_ = v.ScrapeOnce(ctx)
	if load := v.Load(rep); load != 12 {
		t.Fatalf("Load = %d for a body at the ceiling, want 12", load)
	}
}

// testConfig is the load view configuration from config's defaults.
func testConfig() Config { return Config{LoadView: config.Defaults().Router.LoadView} }

// TestKVPressurePenalty: with the opt-in watermark set, a replica above it
// carries extra load scaled to full KV usage while its metrics are fresh,
// and none once they go stale or with the penalty off.
func TestKVPressurePenalty(t *testing.T) {
	rs := newReplicaServer(t, "vllm:num_requests_waiting 2\nvllm:kv_cache_usage_perc 0.95\n")
	for _, tc := range []struct {
		name   string
		hw     float64
		weight int
		want   int64
	}{
		{"off", 0, 0, 2},
		{"weight only", 0, 8, 2},
		{"below watermark", 0.96, 8, 2},
		{"half way to full", 0.9, 8, 2 + 4},
		{"just above", 0.94, 10, 2 + 2}, // ceil(10 * 0.01/0.06)
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, _, clk := newTestView(t, rs)
			v.cfg.KVHighWatermark, v.cfg.KVPenaltyWeight = tc.hw, tc.weight
			_ = v.ScrapeOnce(context.Background())
			if got := v.Load(rs.replica()); got != tc.want {
				t.Fatalf("Load = %d, want %d", got, tc.want)
			}
			clk.advance(time.Hour)
			if got := v.Load(rs.replica()); got != 0 {
				t.Fatalf("stale Load = %d, want 0", got)
			}
		})
	}
}

// TestFilteredParseMatchesFull: parsing only the wanted names gives the
// same series for them as the full parse, and nothing else.
func TestFilteredParseMatchesFull(t *testing.T) {
	raw := []byte("# HELP x\n" +
		"vllm:num_requests_waiting{engine=\"0\",model=\"a b{}\"} 3\n" +
		"vllm:num_requests_waiting{engine=\"1\"} 4 1700000000\n" +
		"  vllm:kv_cache_usage_perc 0.5 # {trace=\"x\"} 1\n" +
		"vllm:e2e_request_latency_seconds_bucket{le=\"0.3\"} 12\n" +
		"bare_name_only\n" +
		"vllm:num_requests_running NaN\n")
	want := map[string]struct{}{"vllm:num_requests_waiting": {}, "vllm:kv_cache_usage_perc": {}, "vllm:num_requests_running": {}}
	full, got := ParsePrometheusMetrics(raw), parsePrometheus(raw, want)
	if len(got) != 2 {
		t.Fatalf("filtered = %v, want the two finite wanted series", got)
	}
	for name, s := range got {
		if full[name] != s {
			t.Errorf("%s: filtered %+v, full %+v", name, s, full[name])
		}
	}
	if got["vllm:num_requests_waiting"] != (Series{Sum: 7, Count: 2}) {
		t.Errorf("waiting = %+v", got["vllm:num_requests_waiting"])
	}
	if n := testing.AllocsPerRun(100, func() { _ = sampleName(raw[10:]) }); n != 0 {
		t.Errorf("sampleName allocates %v", n)
	}
}
