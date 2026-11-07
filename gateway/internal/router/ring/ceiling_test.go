package ring_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/ring"
)

// ringOf is a ring of n replicas with their names.
func ringOf(n int) (*ring.Ring, []string) {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("rep-%04d", i)
	}
	return ring.New(makeReplicas(names...), ring.DefaultConfig()), names
}

// A pool past config.MaxPoolReplicas only warns, so Pick has to stay correct
// there, where it moves from a 64-bit seen-set and stack loads to slices. The
// sizes straddle the word: index 64 and up would alias in a shifted mask.
func TestPickAbovePoolCeiling(t *testing.T) {
	for _, n := range []int{config.MaxPoolReplicas, config.MaxPoolReplicas + 1, 2 * config.MaxPoolReplicas, 1024} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			r, names := ringOf(n)
			for k := range 200 {
				key := fmt.Sprintf("session-%d", k)

				// The walk yields every replica exactly once.
				order := r.Candidates(key)
				seen := map[string]bool{}
				for _, rep := range order {
					seen[rep.Name] = true
				}
				if len(order) != n || len(seen) != n {
					t.Fatalf("%s: %d candidates, %d distinct, want %d", key, len(order), len(seen), n)
				}

				// A load-free pick is the primary, and a loaded one agrees.
				load := func(discovery.Replica) int64 { return 2 }
				got, err := r.Pick(key, load, nil, "")
				if err != nil || got.Replica.Name != order[0].Name || !got.Primary {
					t.Fatalf("%s: pick %+v, %v; want primary %s", key, got, err, order[0].Name)
				}

				// Excluding a replica, the highest index included, skips it.
				for _, exclude := range []string{order[0].Name, names[n-1]} {
					got, err := r.Pick(key, load, nil, exclude)
					if err != nil || got.Replica.Name == exclude {
						t.Fatalf("%s: pick %+v excluding %s, %v", key, got, exclude, err)
					}
				}

				// A full primary overflows to the next candidate in the walk.
				full := func(rep discovery.Replica) int64 {
					if rep.Name == order[0].Name {
						return int64(10 * n)
					}
					return 0
				}
				got, err = r.Pick(key, full, nil, "")
				if err != nil || got.Replica.Name != order[1].Name || got.Primary {
					t.Fatalf("%s: pick %+v, %v; want overflow to %s", key, got, err, order[1].Name)
				}
			}
		})
	}
}

// When concurrent routes refuse every acquisition, a pool past the ceiling
// still takes the least-loaded replica, once, and past the limit.
func TestPickAllRefusedAbovePoolCeiling(t *testing.T) {
	n := config.MaxPoolReplicas + 1
	r, names := ringOf(n)
	least := names[n-1] // past the word, so a stack seen-set could not reach it
	load := func(rep discovery.Replica) int64 {
		if rep.Name == least {
			return 0
		}
		return 1
	}
	var refused, forced int
	var acquired string
	acquire := func(rep discovery.Replica, limit int64) bool {
		if limit == ring.NoLimit {
			forced++
			acquired = rep.Name
			return true
		}
		refused++
		return false
	}
	got, err := r.Pick("session", load, acquire, "")
	if err != nil || !got.OverCap || got.Replica.Name != least || acquired != least {
		t.Fatalf("pick %+v, %v, acquired %s; want %s over cap", got, err, acquired, least)
	}
	if forced != 1 || refused == 0 {
		t.Fatalf("%d forced and %d refused acquisitions, want 1 and some", forced, refused)
	}
}

// Past the ceiling a decision uses slices of its own, so concurrent picks on
// one ring share nothing (run under -race).
func TestConcurrentPicksAbovePoolCeiling(t *testing.T) {
	r, _ := ringOf(2 * config.MaxPoolReplicas)
	load := func(discovery.Replica) int64 { return 1 }
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for k := range 200 {
				if _, err := r.Pick(fmt.Sprintf("session-%d-%d", g, k), load, nil, ""); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
}

// TestPickAllocatesNothingAtThePoolCeiling ties the two limits together: a
// pool as large as config allows still decides on the stack, so raising
// config.MaxPoolReplicas past the ring's small size fails here (M1-26).
func TestPickAllocatesNothingAtThePoolCeiling(t *testing.T) {
	names := make([]string, config.MaxPoolReplicas)
	for i := range names {
		names[i] = fmt.Sprintf("rep-%d", i)
	}
	r := ring.New(makeReplicas(names...), ring.DefaultConfig())
	load := func(discovery.Replica) int64 { return 1 }
	acquire := func(discovery.Replica, int64) bool { return true }
	allocs := testing.AllocsPerRun(100, func() {
		if _, err := r.Pick("session", load, acquire, ""); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("Pick allocated %v times at %d replicas", allocs, config.MaxPoolReplicas)
	}
}
