package compiler

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates"
)

const testSpec = `{"model":"large-model","variables":[{"name":"q"}],
 "messages":[{"role":"system","segments":[{"kind":"static","text":"You are a helpful assistant."}]},
             {"role":"user","segments":[{"kind":"volatile","text":"{{q}}"}]}]}`

type fixedSnap struct{ s *templates.Snapshot }

func (f fixedSnap) Snapshot() *templates.Snapshot { return f.s }

func newTestCompiler(t *testing.T, policy config.MaxTokensPolicy, ceiling int) (*Compiler, *templates.Snapshot) {
	t.Helper()
	snap, errs := templates.NewSnapshot([]templates.Record{{
		ID: "support", Version: 2, Status: templates.StatusActive, Spec: json.RawMessage(testSpec),
		Counts: json.RawMessage(`{"pool":"large","prefix_tokens":12}`),
	}}, time.Now())
	if len(errs) > 0 {
		t.Fatalf("snapshot: %v", errs)
	}
	return New(fixedSnap{snap}, Options{MaxTokens: policy, Ceilings: map[string]int{"large": ceiling}}), snap
}

func header(id, version string) http.Header {
	h := http.Header{}
	if id != "" {
		h.Set(templates.HeaderTemplateID, id)
	}
	if version != "" {
		h.Set(templates.HeaderTemplateVersion, version)
	}
	return h
}

const matchingBody = `{"model":"large-model","messages":[{"role":"system","content":"You are a helpful assistant."},{"role":"user","content":"hi"}]}`

func TestMatch(t *testing.T) {
	c, _ := newTestCompiler(t, config.MaxTokensPolicy{}, 0)
	if _, _, named := c.Match(header("", ""), []byte(matchingBody)); named {
		t.Fatal("no X-Template-Id: nothing to do")
	}
	v, reason, named := c.Match(header("support", "2"), []byte(matchingBody))
	if !named || reason != "" || v == nil || v.ID != "support" || v.Version != 2 || v.PrefixTokens() != 12 {
		t.Fatalf("Match = %+v, %q, %v", v, reason, named)
	}
	tests := []struct{ id, version, body, want string }{
		{"support", "2", `{"messages":[{"role":"system","content":"You are a helpful assistant!"}]}`, templates.ReasonStaticMismatch},
		{"support", "", matchingBody, templates.ReasonNoVersion},
		{"support", "9", matchingBody, templates.ReasonUnknownVersion},
		{"other", "1", matchingBody, templates.ReasonUnknownTemplate},
	}
	for _, tc := range tests {
		if _, got, _ := c.Match(header(tc.id, tc.version), []byte(tc.body)); got != tc.want {
			t.Errorf("%s/%s: reason %q, want %q", tc.id, tc.version, got, tc.want)
		}
	}
}

func TestMatchWithoutRegistry(t *testing.T) {
	for _, c := range []*Compiler{nil, New(nil, Options{}), New(fixedSnap{}, Options{})} {
		if _, reason, named := c.Match(header("support", "2"), []byte(matchingBody)); !named || reason != templates.ReasonRegistryUnavailable {
			t.Fatalf("reason %q named %v, want registry_unavailable", reason, named)
		}
	}
}

func TestCap(t *testing.T) {
	policy := config.MaxTokensPolicy{Enabled: true, Margin: 0.25, MinSamples: 1000}
	c, snap := newTestCompiler(t, policy, 1024)
	v, _ := snap.Lookup("support", 2)
	if got, _ := c.Cap(v, "large", 100); got != 1024 {
		t.Fatalf("before min_samples: cap %d, want the pool ceiling 1024", got)
	}
	if got, _ := c.Cap(v, "small", 100); got != 0 {
		t.Fatalf("pool without a ceiling: cap %d, want 0", got)
	}
	for i := range 1000 {
		c.Observe(v, 100+i%100) // 100..199
	}
	got, _ := c.Cap(v, "large", 100)
	if got < 249 || got > 260 {
		t.Fatalf("cap %d, want about p99.9 (199) x 1.25", got)
	}
	for range 1000 {
		c.Observe(v, 5000)
	}
	if got, _ := c.Cap(v, "large", 100); got != 1024 {
		t.Fatalf("cap %d, want clamped to the ceiling", got)
	}
	off := New(fixedSnap{snap}, Options{MaxTokens: config.MaxTokensPolicy{Enabled: false}, Ceilings: map[string]int{"large": 1024}})
	if n, _ := off.Cap(v, "large", 100); n != 0 {
		t.Fatal("policy disabled: no cap")
	}
}

func TestRetainDropsStatsOfRetiredVersions(t *testing.T) {
	c, snap := newTestCompiler(t, config.MaxTokensPolicy{Enabled: true, MinSamples: 1}, 1024)
	v, _ := snap.Lookup("support", 2)
	c.Observe(v, 10)
	c.Retain(nil)
	if n, _ := c.Cap(v, "large", 100); n != 1024 {
		t.Fatal("stats should be gone after the version left the snapshot")
	}
}

// TestCapNearModelLen: a pool that states its replicas' max_model_len gets
// no gateway cap on a request whose prompt could leave less room than the
// cap, because vLLM refuses prompt tokens plus max_tokens above that length
// and a request that works without the cap must not fail with it (live
// run, 2026-11-03: a 30,873-token prompt and a cap of 2048 on a 32,768 model).
func TestCapNearModelLen(t *testing.T) {
	policy := config.MaxTokensPolicy{Enabled: true, Margin: 0.25, MinSamples: 1000}
	_, snap := newTestCompiler(t, policy, 1024)
	v, _ := snap.Lookup("support", 2)
	c := New(fixedSnap{snap}, Options{MaxTokens: policy,
		Ceilings:  map[string]int{"large": 1024, "open": 1024},
		ModelLens: map[string]int{"large": 8192}})
	room := 8192 - 1024 - ModelLenSlack // the most body bytes that still leave room for the cap
	for _, tc := range []struct {
		name, pool string
		bodyBytes  int
		want       int
	}{
		{"small body keeps the cap", "large", 500, 1024},
		{"body exactly at the room keeps the cap", "large", room, 1024},
		{"one byte past the room drops the cap", "large", room + 1, 0},
		{"body past the model length drops the cap", "large", 100000, 0},
		{"pool without max_model_len is unchecked", "open", 100000, 1024},
	} {
		got, near := c.Cap(v, tc.pool, tc.bodyBytes)
		if got != tc.want || near != (tc.want == 0) {
			t.Errorf("%s: cap %d (near_model_len %v), want %d", tc.name, got, near, tc.want)
		}
	}
}
