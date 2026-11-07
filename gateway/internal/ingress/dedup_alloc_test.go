//go:build !race

package ingress

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/coalesce"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/dedup"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates"
)

// TestDedupAllocatesNothingWhenATemplateOptsOut (G4): a request whose template
// neither shares nor caches allocates nothing in the dedup stage, whatever
// its template version or Accept-Encoding lines: its key parts are joined
// only for a request that gets a key.
func TestDedupAllocatesNothingWhenATemplateOptsOut(t *testing.T) {
	const spec = `{"model":"large-model","variables":[{"name":"q","scope":"shared"}],
 "messages":[{"role":"system","segments":[{"kind":"static","text":"You are a support assistant."}]},
             {"role":"user","segments":[{"kind":"volatile","text":"{{q}}"}]}]}`
	snap, errs := templates.NewSnapshot([]templates.Record{{ID: "plain", Version: 150, Status: templates.StatusActive, Spec: json.RawMessage(spec)}}, time.Now())
	if len(errs) > 0 {
		t.Fatal(errs)
	}
	tmpl, ok := snap.Lookup("plain", 150)
	if !ok || tmpl.Coalescible() || tmpl.CacheTTL() > 0 {
		t.Fatal("want a template that neither shares nor caches")
	}
	h := &Handler{weights: map[string]string{"large": "v1"}, dedup: dedup.New(dedup.Config{
		Group:        coalesce.NewGroup(coalesce.Config{Coalescing: config.Coalescing{MaxFlights: 4, MaxSubscribers: 4, MaxReplayBytes: 1 << 20}}),
		Cache:        newMemCache(),
		KeyBodyBytes: 1 << 20,
		MaxTTL:       time.Minute,
	})}
	r := httptest.NewRequest(http.MethodPost, PathChatCompletions, nil)
	r.Header.Add("Accept-Encoding", "gzip")
	r.Header.Add("Accept-Encoding", "br")
	var ix bodyindex.Index
	_ = ix.Read([]byte(faqBody("where is my order?", false, "")))
	w := httptest.NewRecorder()
	allocs := testing.AllocsPerRun(100, func() {
		if lead, s := h.deduplicate(w, r, nil, "large", tmpl, &ix, "", false); lead != nil || s != proceed {
			t.Fatalf("lead %v step %v, want an ordinary request", lead, s)
		}
	})
	if allocs != 0 {
		t.Fatalf("the dedup stage made %.0f allocations for a template that opted out, want 0", allocs)
	}
}
