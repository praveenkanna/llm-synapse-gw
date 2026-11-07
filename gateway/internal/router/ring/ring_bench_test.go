package ring_test

import (
	"fmt"
	"net/url"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/ring"
)

func BenchmarkRingRoutePure(b *testing.B) {
	reps := make([]discovery.Replica, 4)
	for i := range reps {
		u, _ := url.Parse(fmt.Sprintf("http://127.0.0.1:800%d", i+1))
		reps[i] = discovery.Replica{
			Pool:    "llama-pool",
			Name:    fmt.Sprintf("rep-%d", i),
			BaseURL: u,
		}
	}
	r := ring.New(reps, ring.DefaultConfig())
	key := "session-benchmark-affinity-key-12345"

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			picked, err := r.Pick(key, nil, nil, "")
			if err != nil || picked.Replica.Name == "" {
				b.Fatal("unexpected route failure")
			}
		}
	})
}

func BenchmarkRingRouteBoundedLoad(b *testing.B) {
	reps := make([]discovery.Replica, 4)
	for i := range reps {
		u, _ := url.Parse(fmt.Sprintf("http://127.0.0.1:800%d", i+1))
		reps[i] = discovery.Replica{
			Pool:    "llama-pool",
			Name:    fmt.Sprintf("rep-%d", i),
			BaseURL: u,
		}
	}
	r := ring.New(reps, ring.DefaultConfig())
	key := "session-benchmark-affinity-key-12345"

	loadFn := func(rep discovery.Replica) int64 {
		switch rep.Name {
		case "rep-0":
			return 10
		case "rep-1":
			return 2
		default:
			return 1
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			picked, err := r.Pick(key, loadFn, nil, "")
			if err != nil || picked.Replica.Name == "" {
				b.Fatal("unexpected route failure")
			}
		}
	})
}

// BenchmarkRingRouteAcquire is the affinity router's path: bounded load with
// an acquire callback that claims the slot and re-checks the cap.
func BenchmarkRingRouteAcquire(b *testing.B) {
	reps := make([]discovery.Replica, 4)
	for i := range reps {
		u, _ := url.Parse(fmt.Sprintf("http://127.0.0.1:800%d", i+1))
		reps[i] = discovery.Replica{
			Pool:    "llama-pool",
			Name:    fmt.Sprintf("rep-%d", i),
			BaseURL: u,
		}
	}
	r := ring.New(reps, ring.DefaultConfig())
	key := "session-benchmark-affinity-key-12345"
	loadFn := func(rep discovery.Replica) int64 { return 1 }
	acquire := func(rep discovery.Replica, limit int64) bool { return true }

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			picked, err := r.Pick(key, loadFn, acquire, "")
			if err != nil || picked.Replica.Name == "" {
				b.Fatal("unexpected route failure")
			}
		}
	})
}

func BenchmarkRingCandidates(b *testing.B) {
	reps := make([]discovery.Replica, 4)
	for i := range reps {
		u, _ := url.Parse(fmt.Sprintf("http://127.0.0.1:800%d", i+1))
		reps[i] = discovery.Replica{
			Pool:    "llama-pool",
			Name:    fmt.Sprintf("rep-%d", i),
			BaseURL: u,
		}
	}
	r := ring.New(reps, ring.DefaultConfig())
	key := "session-benchmark-affinity-key-12345"

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cands := r.Candidates(key)
			if len(cands) != 4 {
				b.Fatal("unexpected candidate count")
			}
		}
	})
}
