package affinity_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/affinity"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// Bug #1397: when the scan budget ran out inside whitespace, the value after
// it was read from the whitespace, so a body's key depended on the budget.

// TestBudgetCutInWhitespaceFailsOpen: a budget that runs out in the spaces
// before a value fails open as key_scan_budget, never with another key or
// another reason.
func TestBudgetCutInWhitespaceFailsOpen(t *testing.T) {
	sp := strings.Repeat(" ", 40)
	for _, tc := range []struct{ name, body string }{
		{"token lists after spaces in a prompt list", `{"x":0,"prompt":[` + sp + `[1,2,3],[4]]}`},
		{"string prompt after spaces", `{"prompt":` + sp + `"hello world"}`},
		{"string after spaces in a prompt list", `{"prompt":[` + sp + `"hello world","x"]}`},
		{"messages after spaces", `{"messages":` + sp + `[{"role":"user","content":"Q"}]}`},
		{"message member after spaces", `{"messages":[{"role":"user","content":` + sp + `"Q"}]}`},
		// A short prompt string cut inside its quote search, with more than its
		// prefix of body after it: the key must not come from the bytes after
		// the string.
		{"short prompt string before 3 KB", `{"prompt":"` + strings.Repeat("abcdefghij", 10) + `","x":"` + strings.Repeat("y", 3000) + `"}`},
		{"short string in a prompt list before 3 KB", `{"prompt":["` + strings.Repeat("abcdefghij", 10) + `","` + strings.Repeat("y", 3000) + `"]}`},
		{"prompt string longer than its prefix", `{"prompt":"` + strings.Repeat("y", 3000) + `"}`},
		{"leading whitespace", sp + sp + `{"messages":[{"role":"user","content":"Q"}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			full := affinity.ExtractWithBudget([]byte(tc.body), 1<<16)
			if full.Tier != affinity.TierPrefixHash {
				t.Fatalf("with a large budget: %+v, want a key", full)
			}
			for budget := 1; budget <= 400; budget++ {
				got := affinity.ExtractWithBudget([]byte(tc.body), budget)
				switch {
				case got.Tier == affinity.TierPrefixHash && got.Key != full.Key:
					t.Fatalf("budget %d: key %s, want %s or a fail-open", budget, got.Key, full.Key)
				case got.Tier != affinity.TierPrefixHash && got.Reason != affinity.ReasonScanBudget:
					t.Fatalf("budget %d: %+v, want key_scan_budget", budget, got)
				}
			}
		})
	}
}

// TestKeyDoesNotDependOnBudget: no body gets two different keys from two
// budgets that both decide one. Bodies are built by putting whitespace at
// every place in a set of bodies, so every value can sit behind a budget cut.
func TestKeyDoesNotDependOnBudget(t *testing.T) {
	long := strings.Repeat("abcdefghij", 10)
	bases := []string{
		`{"x":0,"prompt":[[1,2,3],[4]]}`,
		`{"prompt":"hello world"}`,
		`{"prompt":["hello world","x"]}`,
		`{"prompt":[1,2,3]}`,
		`{"prompt":"` + long + `"}`,
		`{"model":"m","prompt":["` + long + `",[1]]}`,
		`{"messages":[{"role":"user","content":"Q"}]}`,
		`{"model":"m","messages":[{"role":"system","content":"S"},{"role":"user","content":"Q"}]}`,
		`{"messages":[{"role":"user","content":"` + long + `"}],"prompt":"p"}`,
	}
	bodies := 0
	for _, base := range bases {
		for at := 0; at <= len(base); at++ {
			for _, ws := range []string{" ", "\n\t ", strings.Repeat(" ", 33)} {
				body := base[:at] + ws + base[at:]
				if !json.Valid([]byte(body)) {
					continue
				}
				bodies++
				var keyed affinity.Result
				for budget := 1; budget <= 600; budget++ {
					got := affinity.ExtractWithBudget([]byte(body), budget)
					if got.Tier != affinity.TierPrefixHash {
						continue
					}
					if keyed.Key == "" {
						keyed = got
						continue
					}
					if got.Key != keyed.Key {
						t.Fatalf("%q: key %s at budget %d, but %s at a smaller one", body, got.Key, budget, keyed.Key)
					}
				}
			}
		}
	}
	if bodies < 500 {
		t.Fatalf("only %d bodies built", bodies)
	}
}

// TestUnparseableStillUnparseable: the fix must not hide a real syntax error
// as a budget cut.
func TestUnparseableStillUnparseable(t *testing.T) {
	for _, body := range []string{`{"messages":   1}`, `{"messages":   "x"}`, `{"messages":[   1]}`} {
		got := affinity.ExtractWithBudget([]byte(body), 1<<16)
		if got.Tier != affinity.TierNone || got.Reason != telemetry.ReasonUnparseableBody {
			t.Errorf("%s: %+v, want unparseable_body", body, got)
		}
	}
}
