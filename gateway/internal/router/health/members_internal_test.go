package health

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
)

func trackedNames(t *Tracker) []string {
	var out []string
	t.states.Range(func(k, _ any) bool {
		out = append(out, k.(string))
		return true
	})
	slices.Sort(out)
	return out
}

func pool(urls map[string]string) []config.Pool {
	p := config.Pool{Name: "p"}
	for _, name := range slices.Sorted(maps.Keys(urls)) {
		p.Replicas = append(p.Replicas, config.Replica{Name: name, URL: urls[name]})
	}
	return []config.Pool{p}
}

// TestRemovedReplicaStateIsPruned: health state exists for exactly the
// current replicas, a late report for a removed one recreates nothing, and
// a probe that raced with the removal is dropped.
func TestRemovedReplicaStateIsPruned(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	disc, err := discovery.NewStatic(pool(map[string]string{"a": srv.URL, "b": srv.URL + "/b"}))
	if err != nil {
		t.Fatal(err)
	}
	tr := New(testConfig(), disc, nil, srv.Client())
	if got := trackedNames(tr); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("tracked = %v", got)
	}
	removedB := disc.Replicas("p")[1]

	if err := disc.Update(pool(map[string]string{"a": srv.URL})); err != nil {
		t.Fatal(err)
	}
	tr.SyncDiscovery()
	if got := trackedNames(tr); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("tracked after removing b = %v", got)
	}

	tr.RecordTransportFailure(removedB, errors.New("dial tcp: connection refused"))
	tr.Eject("b", ReasonStuckEngine)
	tr.pollReplica(context.Background(), removedB) // a probe started before the reload
	if got := trackedNames(tr); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("tracked after late reports for b = %v", got)
	}
}

// TestStateBoundedAcrossManyReloads: replacing replicas over and over never
// grows health state or rings beyond the current set.
func TestStateBoundedAcrossManyReloads(t *testing.T) {
	disc, err := discovery.NewStatic(pool(map[string]string{"r0": "http://10.0.0.1:8000"}))
	if err != nil {
		t.Fatal(err)
	}
	tr := New(testConfig(), disc, nil, nil)
	for i := range 500 {
		prev := disc.Replicas("p")[0] // r<i>, about to be replaced
		set := map[string]string{
			fmt.Sprintf("r%d", i+1): fmt.Sprintf("http://10.0.%d.%d:8000", i/250, i%250+1),
			fmt.Sprintf("s%d", i+1): fmt.Sprintf("http://10.1.%d.%d:8000", i/250, i%250+1),
		}
		if err := disc.Update(pool(set)); err != nil {
			t.Fatal(err)
		}
		tr.SyncDiscovery()
		tr.RecordTransportFailure(prev, errors.New("refused")) // previous set's replica
	}
	if got := trackedNames(tr); len(got) != 2 {
		t.Fatalf("tracked = %d replicas after 500 reloads, want 2: %v", len(got), got)
	}
	r, err := tr.Ring("p")
	if err != nil || len(r.Replicas()) != 2 {
		t.Fatalf("ring = %v, %v; want the 2 current replicas", r, err)
	}
}
