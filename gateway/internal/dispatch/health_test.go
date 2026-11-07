package dispatch

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/health"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// TestTrackerHealthPutsAFullRegistryOnTheRequest: the adapter the
// Dispatcher reports through hands the tracker the attempt's request record,
// so a wait the full registry cannot hold reaches that request's log line,
// counted once.
func TestTrackerHealthPutsAFullRegistryOnTheRequest(t *testing.T) {
	var logs strings.Builder
	tel := telemetry.New(telemetry.Options{Logger: slog.New(slog.NewJSONHandler(&logs, nil))})
	disc, err := discovery.NewStatic([]config.Pool{{Name: "p", Replicas: []config.Replica{{Name: "a", URL: "http://127.0.0.1:1"}}}})
	if err != nil {
		t.Fatal(err)
	}
	cfg := health.ConfigFrom(config.Defaults().Router)
	cfg.StuckEngineMaxWaits = 1
	tr := health.New(cfg, disc, tel.Metrics(), nil)
	rep := disc.Replicas("p")[0]
	held := tr.StartFirstChunkWait(rep, nil) // fills the registry
	if held == nil {
		t.Fatal("no wait for a current replica")
	}
	defer held.End()

	adapter := TrackerHealth(tr)
	h := tel.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		adapter.WatchFirstChunk(r.Context(), rep).End()
		w.WriteHeader(http.StatusOK)
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil))
	if want := `"failopen":["stuck_engine:registry_full"]`; !strings.Contains(logs.String(), want) {
		t.Fatalf("log line %s; want it to carry %s", logs.String(), want)
	}
	rec := httptest.NewRecorder()
	tel.Metrics().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if want := `gateway_failopen_total{stage="stuck_engine",reason="registry_full"} 1` + "\n"; !strings.Contains(rec.Body.String(), want) {
		t.Fatalf("metrics lack %q (counted once)", want)
	}
}
