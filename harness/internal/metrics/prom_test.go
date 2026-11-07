package metrics

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const vllmPage = `# HELP vllm:prefix_cache_queries_total Prefix cache queries.
# TYPE vllm:prefix_cache_queries_total counter
vllm:prefix_cache_queries_total{engine="0",model_name="large-model"} 1000
vllm:prefix_cache_queries_total{engine="1",model_name="large-model"} 500
vllm:prefix_cache_hits_total{engine="0",model_name="large-model"} 600
vllm:num_preemptions_total{engine="0",model_name="large-model"} 3
vllm:time_to_first_token_seconds_bucket{le="+Inf",model_name="large-model"} 42
weird{path="/a}b",q="say \"hi\""} 7 1712345678000
nan_metric NaN

`

func TestParse(t *testing.T) {
	snap, err := Parse(strings.NewReader(vllmPage))
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Samples) != 7 {
		t.Fatalf("samples = %d, want 7: %+v", len(snap.Samples), snap.Samples)
	}
	tests := []struct {
		name string
		want float64
		ok   bool
	}{
		{"vllm:prefix_cache_queries_total", 1500, true},
		{"vllm:prefix_cache_hits_total", 600, true},
		{"vllm:num_preemptions_total", 3, true},
		{"vllm:time_to_first_token_seconds_bucket", 42, true},
		{"weird", 7, true},
		{"absent", 0, false},
	}
	for _, tt := range tests {
		got, ok := snap.Sum(tt.name)
		if got != tt.want || ok != tt.ok {
			t.Errorf("Sum(%s) = %v,%v want %v,%v", tt.name, got, ok, tt.want, tt.ok)
		}
	}
	if v, _ := snap.Sum("nan_metric"); !math.IsNaN(v) {
		t.Errorf("NaN not parsed: %v", v)
	}
	if s := snap.Samples[5]; s.Labels != `path="/a}b",q="say \"hi\""` {
		t.Errorf("labels = %q", s.Labels)
	}
	if v, name, ok := snap.SumAny("vllm:prefix_cache_hits", "vllm:prefix_cache_hits_total"); !ok || v != 600 || name != "vllm:prefix_cache_hits_total" {
		t.Errorf("SumAny = %v %q %v", v, name, ok)
	}
}

func TestParseErrors(t *testing.T) {
	for _, in := range []string{
		"no_value\n",
		"m{a=\"b\" 1\n",
		"m{a=\"b\"} one\n",
		"m 1 2 3\n",
		"{a=\"b\"} 1\n",
	} {
		if _, err := Parse(strings.NewReader(in)); err == nil {
			t.Errorf("Parse(%q): want error", in)
		}
	}
}

func TestParseSeriesCeiling(t *testing.T) {
	var b strings.Builder
	for range maxSeries + 1 {
		b.WriteString("m 1\n")
	}
	if _, err := Parse(strings.NewReader(b.String())); err == nil || !strings.Contains(err.Error(), "series") {
		t.Fatalf("err = %v, want series ceiling", err)
	}
}

func TestMetricsURL(t *testing.T) {
	tests := []struct{ in, want string }{
		{"http://127.0.0.1:8001", "http://127.0.0.1:8001/metrics"},
		{"http://127.0.0.1:8001/", "http://127.0.0.1:8001/metrics"},
		{"http://h:9090/custom", "http://h:9090/custom"},
		{"ftp://h", ""},
		{"127.0.0.1:8001", ""},
		{"http://", ""},
	}
	for _, tt := range tests {
		got, err := MetricsURL(tt.in)
		if (err != nil) != (tt.want == "") || got != tt.want {
			t.Errorf("MetricsURL(%q) = %q, %v; want %q", tt.in, got, err, tt.want)
		}
	}
}

func TestScrape(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/metrics":
			fmt.Fprint(w, vllmPage)
		case "/bad":
			fmt.Fprint(w, "garbage here now\n")
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	s := Scraper{HTTP: srv.Client()}
	snap, err := s.Scrape(context.Background(), srv.URL)
	if err != nil || snap.At.IsZero() || len(snap.Samples) != 7 {
		t.Fatalf("scrape = %+v, %v", snap, err)
	}
	for _, path := range []string{"/bad", "/missing"} {
		if _, err := s.Scrape(context.Background(), srv.URL+path); err == nil {
			t.Errorf("scrape %s: want error", path)
		}
	}
	if _, err := (Scraper{}).Scrape(context.Background(), "http://127.0.0.1:1"); err == nil {
		t.Error("scrape of closed port: want error")
	}
}
