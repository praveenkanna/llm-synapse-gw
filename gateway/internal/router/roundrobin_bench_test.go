package router_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router"
)

func BenchmarkRoundRobinRoute(b *testing.B) {
	disc := staticDiscovery{
		"pool": {
			{Pool: "pool", Name: "rep1", BaseURL: mustURL("http://127.0.0.1:8001")},
			{Pool: "pool", Name: "rep2", BaseURL: mustURL("http://127.0.0.1:8002")},
			{Pool: "pool", Name: "rep3", BaseURL: mustURL("http://127.0.0.1:8003")},
			{Pool: "pool", Name: "rep4", BaseURL: mustURL("http://127.0.0.1:8004")},
		},
	}
	rr := router.NewRoundRobin(disc, []string{"pool"})
	ctx := context.Background()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://gateway/v1/chat/completions", nil)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := rr.Route(ctx, req, "pool")
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
