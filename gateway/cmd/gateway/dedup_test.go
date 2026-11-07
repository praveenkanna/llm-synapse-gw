package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// TestCoalesceGroupTakesItsSection: the flight table's limits are
// coalescing's, and its size reaches the metrics.
func TestCoalesceGroupTakesItsSection(t *testing.T) {
	cfg := config.Defaults()
	cfg.Coalescing.MaxFlights, cfg.Coalescing.MaxSubscribers = 1, 2
	m := telemetry.NewMetrics()
	g := newCoalesceGroup(&cfg, m)
	if f, _, _ := g.Acquire("a", true); f == nil {
		t.Fatal("no flight for the first key")
	}
	if _, _, reason := g.Acquire("b", true); reason != telemetry.ReasonFlightsFull {
		t.Fatalf("second flight: reason %q, want %q (coalescing.max_flights 1)", reason, telemetry.ReasonFlightsFull)
	}
	for i := range 2 {
		sub := g.Join("a")
		if sub == nil {
			t.Fatalf("no room for subscriber %d of coalescing.max_subscribers 2", i+1)
		}
		defer sub.Close()
	}
	if g.Join("a") != nil {
		t.Fatal("a third subscriber joined past coalescing.max_subscribers 2")
	}
	rec := httptest.NewRecorder()
	m.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if want := telemetry.MetricCoalesceFlights + " 1\n"; !strings.Contains(rec.Body.String(), want) {
		t.Fatalf("metrics lack %q", want)
	}
}
