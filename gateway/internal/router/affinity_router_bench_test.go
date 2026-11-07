package router_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/health"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/loadview"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

func BenchmarkAffinityRouteTier1Header(b *testing.B) {
	reps := []discovery.Replica{
		{Pool: "llama-pool", Name: "rep-0", BaseURL: mustURL("http://127.0.0.1:8001")},
		{Pool: "llama-pool", Name: "rep-1", BaseURL: mustURL("http://127.0.0.1:8002")},
		{Pool: "llama-pool", Name: "rep-2", BaseURL: mustURL("http://127.0.0.1:8003")},
		{Pool: "llama-pool", Name: "rep-3", BaseURL: mustURL("http://127.0.0.1:8004")},
	}
	disc := staticDiscovery{"llama-pool": reps}

	tm := telemetry.NewMetrics()
	ht := health.New(health.ConfigFrom(config.Defaults().Router), disc, tm, nil)
	lv := loadview.New(loadview.Config{LoadView: config.Defaults().Router.LoadView}, disc, tm, nil)
	rr := router.NewRoundRobin(disc, []string{"llama-pool"})
	ar := router.NewAffinityRouter(disc, rr, ht, lv, tm)

	ctx := context.Background()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "/v1/chat/completions", nil)
	req.Header.Set("X-Session-Id", "sess-bench-header-key")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rep, err := ar.Route(ctx, req, "llama-pool")
			if err != nil || rep.Replica.Name == "" {
				b.Fatal("unexpected route failure")
			}
			rep.Release()
		}
	})
}

func BenchmarkAffinityRouteTier2PrefixHash(b *testing.B) {
	reps := []discovery.Replica{
		{Pool: "llama-pool", Name: "rep-0", BaseURL: mustURL("http://127.0.0.1:8001")},
		{Pool: "llama-pool", Name: "rep-1", BaseURL: mustURL("http://127.0.0.1:8002")},
		{Pool: "llama-pool", Name: "rep-2", BaseURL: mustURL("http://127.0.0.1:8003")},
		{Pool: "llama-pool", Name: "rep-3", BaseURL: mustURL("http://127.0.0.1:8004")},
	}
	disc := staticDiscovery{"llama-pool": reps}

	tm := telemetry.NewMetrics()
	ht := health.New(health.ConfigFrom(config.Defaults().Router), disc, tm, nil)
	lv := loadview.New(loadview.Config{LoadView: config.Defaults().Router.LoadView}, disc, tm, nil)
	rr := router.NewRoundRobin(disc, []string{"llama-pool"})
	ar := router.NewAffinityRouter(disc, rr, ht, lv, tm)

	body := []byte(`{
		"model": "meta-llama/Llama-3.1-8B-Instruct",
		"messages": [
			{"role": "system", "content": "You are a helpful assistant."},
			{"role": "user", "content": "Benchmark message content."}
		]
	}`)
	ctx := router.WithRequestBody(context.Background(), body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "/v1/chat/completions", nil)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rep, err := ar.Route(ctx, req, "llama-pool")
			if err != nil || rep.Replica.Name == "" {
				b.Fatal("unexpected route failure")
			}
			rep.Release()
		}
	})
}
