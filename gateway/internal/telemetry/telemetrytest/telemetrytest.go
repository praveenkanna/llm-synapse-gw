// Package telemetrytest reads what request telemetry counted, for tests:
// the metrics are the one ledger of what the request path did (AD-12).
package telemetrytest

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// Sum returns the sum of metric's series whose labels include every given
// `name="value"` pair (0 when none), from m's Prometheus exposition.
func Sum(t testing.TB, m *telemetry.Metrics, metric string, labels ...string) float64 {
	t.Helper()
	rec := httptest.NewRecorder()
	m.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, telemetry.MetricsPath, nil))
	var sum float64
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		series, value, ok := strings.Cut(line, " ")
		if !ok || strings.HasPrefix(line, "#") {
			continue
		}
		name, labelSet, _ := strings.Cut(series, "{")
		if name != metric || !hasAll(labelSet, labels) {
			continue
		}
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			t.Fatalf("metric %s: %v", series, err)
		}
		sum += v
	}
	return sum
}

func hasAll(labelSet string, labels []string) bool {
	for _, l := range labels {
		if !strings.Contains(labelSet, l) {
			return false
		}
	}
	return true
}
