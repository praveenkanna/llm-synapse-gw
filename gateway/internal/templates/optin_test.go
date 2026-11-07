package templates

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func shared(s *Spec) {
	for i := range s.Variables {
		s.Variables[i].Scope = ScopeShared
	}
}

func TestOptInNeedsEveryVariableShared(t *testing.T) {
	for _, flag := range []string{"coalesce", "cacheable"} {
		set := func(s *Spec) {
			if flag == "coalesce" {
				s.Coalesce = true
			} else {
				s.Cacheable, s.CacheTTL = true, "30s"
			}
		}
		t.Run(flag+" with undeclared scopes", func(t *testing.T) {
			s := testSpec()
			set(&s)
			errs := Validate(s)
			if len(errs) != len(s.Variables) {
				t.Fatalf("Validate = %v, want one problem per variable", errs)
			}
			for i, e := range errs {
				if want := "variables[" + string(rune('0'+i)) + "].scope"; e.Path != want {
					t.Errorf("problem %d at %q, want %q", i, e.Path, want)
				}
				if !strings.Contains(e.Msg, `"scope":"shared"`) || !strings.Contains(e.Msg, flag) {
					t.Errorf("message does not say what to do: %q", e.Msg)
				}
			}
		})
		t.Run(flag+" with a user-scoped variable", func(t *testing.T) {
			s := testSpec()
			set(&s)
			shared(&s)
			s.Variables[1].Scope = ScopeUser
			errs := Validate(s)
			if len(errs) != 1 || errs[0].Path != "variables[1].scope" || !strings.Contains(errs[0].Msg, "user-specific") {
				t.Fatalf("Validate = %v", errs)
			}
		})
		t.Run(flag+" with every variable shared", func(t *testing.T) {
			s := testSpec()
			set(&s)
			shared(&s)
			if errs := Validate(s); errs != nil {
				t.Fatalf("Validate = %v, want nil", errs)
			}
		})
		t.Run(flag+" with no variables", func(t *testing.T) {
			s := testSpec()
			set(&s)
			s.Variables, s.Examples = nil, nil
			s.Messages = []Message{{Role: "system", Segments: []Segment{{Kind: KindStatic, Text: "Summarize."}}}}
			if errs := Validate(s); errs != nil {
				t.Fatalf("Validate = %v, want nil", errs)
			}
		})
	}
}

func TestOptOutIgnoresScopes(t *testing.T) {
	s := testSpec() // neither coalesce nor cacheable: scopes are not required
	if errs := Validate(s); errs != nil {
		t.Fatalf("Validate = %v", errs)
	}
	s.Variables[0].Scope = ScopeUser
	if errs := Validate(s); errs != nil {
		t.Fatalf("a user-scoped variable is fine without opt-in: %v", errs)
	}
}

func TestUnknownScopeIsRejectedEvenWithoutOptIn(t *testing.T) {
	s := testSpec()
	s.Variables[0].Scope = "global"
	errs := Validate(s)
	if len(errs) != 1 || errs[0].Path != "variables[0].scope" || !strings.Contains(errs[0].Msg, `unknown scope "global"`) {
		t.Fatalf("Validate = %v", errs)
	}
}

func TestCacheableNeedsATTL(t *testing.T) {
	for _, ttl := range []string{"", "0s"} {
		s := testSpec()
		s.Cacheable, s.CacheTTL = true, ttl
		shared(&s)
		errs := Validate(s)
		if len(errs) != 1 || errs[0].Path != "cache_ttl" || !strings.Contains(errs[0].Msg, "above 0s") {
			t.Errorf("ttl %q: Validate = %v", ttl, errs)
		}
	}
	// A bad ttl is reported once, as before.
	s := testSpec()
	s.Cacheable, s.CacheTTL = true, "soon"
	shared(&s)
	if errs := Validate(s); len(errs) != 1 || !strings.Contains(errs[0].Msg, "is not a duration") {
		t.Fatalf("Validate = %v", errs)
	}
}

func TestVersionDedupAccessors(t *testing.T) {
	rec := func(mutate func(*Spec)) Record {
		s := testSpec()
		mutate(&s)
		raw, _ := json.Marshal(s)
		return Record{ID: "summary", Version: 1, Status: StatusActive, Spec: raw}
	}
	snap, errs := NewSnapshot([]Record{
		rec(func(*Spec) {}),
		{ID: "cached", Version: 1, Status: StatusActive, Spec: rec(func(s *Spec) { shared(s); s.Cacheable, s.CacheTTL, s.Coalesce = true, "90s", true }).Spec},
	}, time.Now())
	if len(errs) != 0 {
		t.Fatalf("NewSnapshot: %v", errs)
	}
	plain, _ := snap.Lookup("summary", 1)
	if plain.Coalescible() || plain.CacheTTL() != 0 {
		t.Errorf("a template that opted in to nothing: Coalescible=%v CacheTTL=%v", plain.Coalescible(), plain.CacheTTL())
	}
	cached, _ := snap.Lookup("cached", 1)
	if !cached.Coalescible() || cached.CacheTTL() != 90*time.Second {
		t.Errorf("opted in: Coalescible=%v CacheTTL=%v", cached.Coalescible(), cached.CacheTTL())
	}
}

func TestSnapshotSkipsOptInWithUnsharedVariables(t *testing.T) {
	s := testSpec()
	s.Coalesce = true // variables have no scope
	raw, _ := json.Marshal(s)
	snap, errs := NewSnapshot([]Record{{ID: "summary", Version: 1, Status: StatusActive, Spec: raw}}, time.Now())
	if len(errs) != 1 || !strings.Contains(errs[0].Error(), "variables[0].scope") {
		t.Fatalf("errors = %v, want one naming variables[0].scope", errs)
	}
	if _, ok := snap.Lookup("summary", 1); ok {
		t.Fatal("a version that breaks the opt-in rule must not be served")
	}
}
