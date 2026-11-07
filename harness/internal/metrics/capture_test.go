package metrics

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// counterServer serves vLLM-style counters that grow on every scrape, plus
// process metrics, and can be switched to fail.
type counterServer struct {
	scrapes atomic.Int64
	fail    atomic.Bool
}

func (c *counterServer) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	if c.fail.Load() {
		http.Error(w, "down", http.StatusServiceUnavailable)
		return
	}
	n := c.scrapes.Add(1)
	fmt.Fprintf(w, "vllm:prefix_cache_queries_total %d\nvllm:prefix_cache_hits_total %d\n", n*100, n*50)
	fmt.Fprintf(w, "process_resident_memory_bytes %d\nprocess_cpu_seconds_total %d\n", n<<20, n)
}

func TestCapture(t *testing.T) {
	up1, up2, gw := &counterServer{}, &counterServer{}, &counterServer{}
	s1, s2, sg := httptest.NewServer(up1), httptest.NewServer(up2), httptest.NewServer(gw)
	defer s1.Close()
	defer s2.Close()
	defer sg.Close()

	cfg := CaptureConfig{Upstreams: []string{s1.URL, s2.URL}, ProcessMetrics: sg.URL, Interval: 5 * time.Millisecond}
	if !cfg.Enabled() || (CaptureConfig{}).Enabled() {
		t.Fatal("Enabled mismatch")
	}
	c, err := Start(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	for gw.scrapes.Load() < 4 {
		time.Sleep(time.Millisecond)
	}
	up2.fail.Store(true)
	res := c.Stop(context.Background())
	if again := c.Stop(context.Background()); len(again.Upstreams) != 2 {
		t.Fatalf("second Stop = %+v", again)
	}

	if len(res.Upstreams) != 2 || res.Upstreams[0].Error != "" || res.Upstreams[1].Error == "" {
		t.Fatalf("upstreams = %+v", res.Upstreams)
	}
	u := res.Upstreams[0]
	if u.PrefixCacheQueries.Delta != 100 || u.PrefixCacheHitRate == nil || *u.PrefixCacheHitRate != 0.5 {
		t.Fatalf("upstream 1 = %+v", u)
	}
	if res.Total == nil || res.Total.Error == "" || *res.Total.PrefixCacheHitRate != 0.5 {
		t.Fatalf("total = %+v", res.Total)
	}
	p := res.Process
	if p == nil || p.Samples < 3 || p.CPUSeconds <= 0 || p.RSSMaxMB < p.RSSStartMB || p.Source != sg.URL {
		t.Fatalf("process = %+v", p)
	}
}

func TestCaptureStartFails(t *testing.T) {
	down := &counterServer{}
	down.fail.Store(true)
	srv := httptest.NewServer(down)
	defer srv.Close()
	tests := []CaptureConfig{
		{Upstreams: []string{srv.URL}},
		{ProcessMetrics: srv.URL},
		{Upstreams: []string{"not a url"}},
	}
	for _, cfg := range tests {
		if _, err := Start(context.Background(), cfg); err == nil {
			t.Errorf("Start(%+v): want error", cfg)
		}
	}
}
