package telemetry

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func render(t *testing.T, fams ...writer) string {
	t.Helper()
	var buf bytes.Buffer
	if err := writeAll(&buf, fams); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

func TestExpositionFormat(t *testing.T) {
	c := newCounterFamily("t_requests_total", "Requests.", "pool", "status")
	c.with("small", "200").inc()
	c.with("large", "200").inc()
	c.with("large", "200").inc()
	g := newGaugeFamily("t_inflight", "In flight.", "replica")
	g.with("r0").add(3)
	g.with("r0").add(-1)
	h := newHistogramFamily("t_seconds", "Latency.", []float64{0.1, 1}, "pool")
	h.with("large").observe(0.05)
	h.with("large").observe(0.1) // le is inclusive
	h.with("large").observe(5)
	nolabel := newCounterFamily("t_disconnects_total", "Disconnects.")
	nolabel.with().inc()

	want := `# HELP t_requests_total Requests.
# TYPE t_requests_total counter
t_requests_total{pool="large",status="200"} 2
t_requests_total{pool="small",status="200"} 1
# HELP t_inflight In flight.
# TYPE t_inflight gauge
t_inflight{replica="r0"} 2
# HELP t_seconds Latency.
# TYPE t_seconds histogram
t_seconds_bucket{pool="large",le="0.1"} 2
t_seconds_bucket{pool="large",le="1"} 2
t_seconds_bucket{pool="large",le="+Inf"} 3
t_seconds_sum{pool="large"} 5.15
t_seconds_count{pool="large"} 3
# HELP t_disconnects_total Disconnects.
# TYPE t_disconnects_total counter
t_disconnects_total 1
`
	if got := render(t, c, g, h, nolabel); got != want {
		t.Fatalf("exposition mismatch\n got:\n%s\nwant:\n%s", got, want)
	}
}

func TestEmptyFamilyRendersHeaderOnly(t *testing.T) {
	c := newCounterFamily("t_total", "Help.", "a")
	if got, want := render(t, c), "# HELP t_total Help.\n# TYPE t_total counter\n"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestLabelValuesAreEscaped(t *testing.T) {
	c := newCounterFamily("t_total", "Help.", "replica")
	c.with("a\"b\\c\nd").inc()
	if got := render(t, c); !strings.Contains(got, `t_total{replica="a\"b\\c\nd"} 1`) {
		t.Fatalf("escaping wrong:\n%s", got)
	}
}

func TestSeriesCeilingIsEnforced(t *testing.T) {
	tests := []struct {
		name       string
		max        int64
		distinct   int
		concurrent bool
	}{
		{"below ceiling", 5, 3, false},
		{"at ceiling", 5, 5, false},
		{"past ceiling", 5, 50, false},
		{"past ceiling concurrently", 5, 200, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newCounterFamily("t_total", "Help.", "replica")
			c.maxSeries = tc.max
			inc := func(i int) { c.with("r" + strconv.Itoa(i)).inc() }
			if tc.concurrent {
				var wg sync.WaitGroup
				for i := range tc.distinct {
					wg.Add(1)
					go func() { defer wg.Done(); inc(i) }()
				}
				wg.Wait()
			} else {
				for i := range tc.distinct {
					inc(i)
				}
			}
			wantSeries := min(int64(tc.distinct), tc.max)
			if got := int64(c.seriesCount()); got != wantSeries {
				t.Fatalf("series = %d, want %d", got, wantSeries)
			}
			var total uint64
			for _, s := range c.snapshot() {
				total += s.val.v.Load()
			}
			if total != uint64(tc.distinct) {
				t.Fatalf("total count = %d, want %d (no increment lost)", total, tc.distinct)
			}
			overflowLine := `t_total{replica="_overflow"} ` + strconv.Itoa(tc.distinct-int(wantSeries))
			if has := strings.Contains(render(t, c), overflowLine); has != (tc.distinct > int(tc.max)) {
				t.Fatalf("overflow line %q present = %v\n%s", overflowLine, has, render(t, c))
			}
		})
	}
}

// Every gateway metric is bounded by MaxSeriesPerMetric, whatever label
// values arrive (for example replica names across many hot-reloads).
func TestGatewayMetricsStayBounded(t *testing.T) {
	m := NewMetrics()
	n := 3 * MaxSeriesPerMetric
	for i := range n {
		v := strconv.Itoa(i)
		m.requests.with("pool"+v, ClassDefault, "200").inc()
		m.inflight.with("pool", "replica"+v, ClassDefault).add(1)
		m.ttft.with("pool"+v, ClassDefault).observe(0.01)
		m.duration.with("pool"+v, ClassDefault).observe(0.01)
		m.upstreamErrors.with("replica"+v, "connect").inc()
		m.failOpen.with("stage", "reason"+v).inc()
	}
	for name, got := range map[string]int{
		MetricRequests:        m.requests.seriesCount(),
		MetricInflight:        m.inflight.seriesCount(),
		MetricTTFT:            m.ttft.seriesCount(),
		MetricRequestDuration: m.duration.seriesCount(),
		MetricUpstreamErrors:  m.upstreamErrors.seriesCount(),
		MetricFailOpen:        m.failOpen.seriesCount(),
	} {
		if got != MaxSeriesPerMetric {
			t.Errorf("%s holds %d series, want the ceiling %d", name, got, MaxSeriesPerMetric)
		}
	}
	lines := strings.Count(scrape(t, m), "\n")
	if maxLines := 13*2 + 5*(MaxSeriesPerMetric+1) + 2*(MaxSeriesPerMetric+1)*(len(ttftBuckets)+len(durationBuckets)+6); lines > maxLines {
		t.Fatalf("exposition has %d lines, want at most %d", lines, maxLines)
	}
}

func TestHistogramConcurrentObserve(t *testing.T) {
	h := newHistogram([]float64{1, 2})
	const goroutines, each = 8, 1000
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range each {
				h.observe(0.5)
			}
		}()
	}
	wg.Wait()
	if got := h.counts[0].Load(); got != goroutines*each {
		t.Fatalf("bucket count = %d, want %d", got, goroutines*each)
	}
	f := histogramFamily{newFamily("t_seconds", "h", typeHistogram, nil, func() *histogram { return h })}
	f.with()
	out := render(t, f)
	for _, want := range []string{"t_seconds_sum 4000\n", "t_seconds_count 8000\n", `t_seconds_bucket{le="+Inf"} 8000`} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in\n%s", want, out)
		}
	}
}

func TestMetricsHandler(t *testing.T) {
	m := NewMetrics()
	m.clientDisconnects.with().inc()
	rec := httptest.NewRecorder()
	m.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, MetricsPath, nil))
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != metricsContentType {
		t.Fatalf("status %d, content-type %q", rec.Code, rec.Header().Get("Content-Type"))
	}
	body := rec.Body.String()
	for _, name := range []string{MetricRequests, MetricInflight, MetricTTFT, MetricRequestDuration,
		MetricUpstreamErrors, MetricClientDisconnects, MetricFailOpen, MetricReplicaInfo,
		MetricRouteDecisions, MetricRouterDuration, MetricReplicaEjections, MetricFirstChunkStalls, MetricStuckProbes} {
		if !strings.Contains(body, fmt.Sprintf("# TYPE %s ", name)) {
			t.Errorf("metric %s not exported", name)
		}
	}
	if !strings.Contains(body, MetricClientDisconnects+" 1\n") {
		t.Errorf("disconnect count missing:\n%s", body)
	}
}

func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, MetricsPath, nil))
	return rec.Body.String()
}

// TestMetricsFailOpenConcurrent covers fail-opens counted off the handler
// goroutine (the upstream resolver's lookup goroutines).
func TestMetricsFailOpenConcurrent(t *testing.T) {
	m := NewMetrics()
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				m.FailOpen("upstream_dns", "lookup_failed")
			}
		})
	}
	wg.Wait()
	rec := httptest.NewRecorder()
	m.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, MetricsPath, nil))
	want := MetricFailOpen + `{stage="upstream_dns",reason="lookup_failed"} 800` + "\n"
	if !strings.Contains(rec.Body.String(), want) {
		t.Fatalf("missing %q in:\n%s", want, rec.Body.String())
	}
}

func TestRouterMetrics(t *testing.T) {
	m := NewMetrics()
	m.RecordRouterDecision("default", "affinity", "affinity")
	m.RecordRouterDecision("default", "affinity", "overflow")
	m.RecordRouterDecision("chat", "round_robin", "round_robin")
	m.RecordRouterDuration("default", "affinity", 0.000045) // 45 µs
	m.ReplicaEjected("default", "transport_failure")
	m.ReplicaEjected("default", "transport_failure")
	m.FirstChunkStall("default")
	m.StuckProbe("default", "timeout")
	m.StuckProbe("default", "timeout")
	m.StuckProbe("default", "pass")

	body := scrape(t, m)

	for _, want := range []string{
		`gateway_route_decisions_total{pool="default",policy="affinity",outcome="affinity"} 1`,
		`gateway_route_decisions_total{pool="default",policy="affinity",outcome="overflow"} 1`,
		`gateway_route_decisions_total{pool="chat",policy="round_robin",outcome="round_robin"} 1`,
		`gateway_router_duration_seconds_count{pool="default",policy="affinity"} 1`,
		`gateway_replica_ejections_total{pool="default",reason="transport_failure"} 2`,
		`gateway_first_chunk_stalls_total{pool="default"} 1`,
		`gateway_stuck_probe_total{pool="default",outcome="timeout"} 2`,
		`gateway_stuck_probe_total{pool="default",outcome="pass"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %s in:\n%s", want, body)
		}
	}
}

// TestMetricNamesMatchObservabilityRule pins the exported names and label
// sets to .claude/rules/observability-and-privacy.md and the Grafana
// dashboard: route decisions use {pool,policy,outcome} (no "status" label
// that is not an HTTP status), and no gauge duplicates gateway_inflight.
func TestMetricNamesMatchObservabilityRule(t *testing.T) {
	m := NewMetrics()
	want := map[string][]string{
		"gateway_requests_total":           {"pool", "class", "status"},
		"gateway_inflight":                 {"pool", "replica", "class"},
		"gateway_retries_total":            {"pool", "reason"},
		"gateway_failopen_total":           {"stage", "reason"},
		"gateway_route_decisions_total":    {"pool", "policy", "outcome"},
		"gateway_router_duration_seconds":  {"pool", "policy"},
		"gateway_replica_ejections_total":  {"pool", "reason"},
		"gateway_first_chunk_stalls_total": {"pool"},
		"gateway_stuck_probe_total":        {"pool", "outcome"},
		"gateway_replica_info":             {"pool", "replica", "vllm_version"},
		"gateway_upstream_errors_total":    {"replica", "kind"},
		"gateway_client_disconnects_total": nil,
	}
	got := map[string][]string{}
	for _, f := range []struct {
		name   string
		labels []string
	}{
		{m.requests.name, m.requests.labelNames},
		{m.inflight.name, m.inflight.labelNames},
		{m.retries.name, m.retries.labelNames},
		{m.failOpen.name, m.failOpen.labelNames},
		{m.routeDecisions.name, m.routeDecisions.labelNames},
		{m.routerDuration.name, m.routerDuration.labelNames},
		{m.replicaEjections.name, m.replicaEjections.labelNames},
		{m.firstChunkStalls.name, m.firstChunkStalls.labelNames},
		{m.stuckProbes.name, m.stuckProbes.labelNames},
		{m.replicaInfo.name, m.replicaInfo.labelNames},
		{m.upstreamErrors.name, m.upstreamErrors.labelNames},
		{m.clientDisconnects.name, m.clientDisconnects.labelNames},
	} {
		got[f.name] = f.labels
	}
	for name, labels := range want {
		if !slices.Equal(got[name], labels) {
			t.Errorf("%s labels = %v, want %v", name, got[name], labels)
		}
	}
	body := scrape(t, m)
	for _, gone := range []string{"gateway_router_decisions_total", "gateway_replica_inflight"} {
		if strings.Contains(body, gone) {
			t.Errorf("retired metric %s is still exported", gone)
		}
	}
}

func TestReplicaInfoReplacesOldVersion(t *testing.T) {
	m := NewMetrics()
	m.SetReplicaInfo("large", "r0", "0.6.3")
	m.SetReplicaInfo("large", "r1", "0.6.3")
	m.SetReplicaInfo("large", "r0", "0.6.3") // unchanged: still one series
	m.SetReplicaInfo("large", "r0", "0.7.0") // upgrade

	body := scrape(t, m)
	if strings.Contains(body, `gateway_replica_info{pool="large",replica="r0",vllm_version="0.6.3"}`) {
		t.Errorf("old version series still exported after upgrade:\n%s", body)
	}
	for _, want := range []string{
		`gateway_replica_info{pool="large",replica="r0",vllm_version="0.7.0"} 1`,
		`gateway_replica_info{pool="large",replica="r1",vllm_version="0.6.3"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %s in:\n%s", want, body)
		}
	}
	if n := m.replicaInfo.seriesCount(); n != 2 {
		t.Errorf("replica info series = %d, want 2", n)
	}
}

func TestMetricFamilyHotPathAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("testing.AllocsPerRun is inflated by -race instrumentation")
	}
	m := NewMetrics()
	// Warm series once.
	m.clientDisconnects.with().inc()
	m.firstChunkStalls.with("large").inc()
	m.failOpen.with("compile", "unknown_template")
	m.requests.with("large", "user", "200").inc()
	m.ttft.with("large", "user").observe(0.12)

	allocs := testing.AllocsPerRun(1000, func() {
		m.clientDisconnects.with().inc()
		m.firstChunkStalls.with("large").inc()
		m.failOpen.with("compile", "unknown_template").inc()
		m.requests.with("large", "user", "200").inc()
		m.ttft.with("large", "user").observe(0.12)
	})
	if allocs != 0 {
		t.Fatalf("hot-path metric updates allocated %.2f objects/op, want 0", allocs)
	}
}

func TestBufferedWriterPreservesOrder(t *testing.T) {
	var out syncBuffer
	bw := NewBufferedWriter(&out, 256, 5*time.Millisecond)
	for i := range 200 {
		line := fmt.Sprintf("line-%04d-payload\n", i)
		if _, err := bw.Write([]byte(line)); err != nil {
			t.Fatal(err)
		}
	}
	if err := bw.Close(); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	if len(lines) != 200 {
		t.Fatalf("got %d lines, want 200", len(lines))
	}
	for i, got := range lines {
		want := fmt.Sprintf("line-%04d-payload", i)
		if got != want {
			t.Fatalf("line %d = %q, want %q", i, got, want)
		}
	}
}

func BenchmarkMetricsHotPath(b *testing.B) {
	m := NewMetrics()
	m.requests.with("large", "user", "200").inc()
	m.ttft.with("large", "user").observe(0.12)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		m.requests.with("large", "user", "200").inc()
		m.ttft.with("large", "user").observe(0.12)
	}
}
