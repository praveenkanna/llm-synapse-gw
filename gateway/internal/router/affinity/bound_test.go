package affinity_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/affinity"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/affinity/affinitytest"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// workSlack is the most one step may charge past the limit before a loop
// sees the budget spent: one inline string window and one hashed message
// prefix (8 KiB at 1 unit per 4 bytes).
const workSlack = 4 << 10

// assertBounded fails t unless the scan of body spent no more than its
// work limit plus one step. Every byte the scanner visits one at a time
// costs at least one unit, so the work bound also bounds the bytes visited
// (bytes jumped with bytes.IndexByte cost 1 unit per 32).
func assertBounded(t *testing.T, body []byte) (affinity.Result, int) {
	t.Helper()
	orig := bytes.Clone(body)
	res, work, limit := affinity.ExtractStats(body)
	if !bytes.Equal(orig, body) {
		t.Fatal("body modified")
	}
	if work > limit+workSlack {
		t.Errorf("work %d units, limit %d (+%d slack)", work, limit, workSlack)
	}
	return res, limit
}

// failOpenReason is the reason a *_failopen short-token shape fails open:
// adjacent strings are invalid JSON and stop at once as unparseable; the
// rest spend the budget.
func failOpenReason(name string) string {
	if strings.HasPrefix(name, "adjacent_") {
		return telemetry.ReasonUnparseableBody
	}
	if strings.HasPrefix(name, "duplicate_") {
		return affinity.ReasonDuplicateKey
	}
	return affinity.ReasonScanBudget
}

// TestScanBoundShortTokens: bodies made only of short keys and strings (the
// scanner's inline path) stop within the work budget, whatever their size
// (M1-22). Ahead of the key they fail open as key_scan_budget; after a
// decided key the duplicate check stops at the post-key cap and the key
// decided before it is used.
func TestScanBoundShortTokens(t *testing.T) {
	q := `[{"role":"user","content":"x"}]`
	keyX := key(t, `{"messages":`+q+`}`)
	for _, tc := range affinitytest.ShortTokenCases() {
		t.Run(tc.Name, func(t *testing.T) {
			res, limit := assertBounded(t, tc.Body)
			if strings.HasSuffix(tc.Name, "_failopen") {
				assertResult(t, res, affinity.TierNone, failOpenReason(tc.Name))
				return
			}
			if strings.Contains(tc.Name, "_prompt_") { // keyed on the prompt prefix
				if res.Tier != affinity.TierPrefixHash {
					t.Fatalf("got %+v, want a prompt key", res)
				}
				return
			}
			if res.Tier != affinity.TierPrefixHash || res.Key != keyX {
				t.Fatalf("got %+v, want key %s", res, keyX)
			}
			if limit > affinity.PostKeyBudget+1<<10 {
				t.Errorf("limit %d units: the key was not decided early, so the post-key cap did not apply", limit)
			}
		})
	}
}

// TestScanBoundEveryLoop: every loop over members, elements and strings
// stops within the budget, for shapes of short tokens in each place the
// scanner walks, valid or not. Ones whose key needs the whole shape fail
// open; prompts key on their prefix before the budget runs out.
func TestScanBoundEveryLoop(t *testing.T) {
	const n = 400_000
	cases := []struct {
		name, body string
		tier       affinity.Tier
	}{
		{"members in a leading message", `{"messages":[{"role":"system",` + strings.Repeat(`"k":"v",`, n) + `"content":"s"},{"role":"user","content":"Q"}]}`, affinity.TierNone},
		{"content parts in a leading message", `{"messages":[{"role":"user","content":[` + strings.Repeat(`{"type":"text","text":"x"},`, n/4) + `{"type":"text","text":"y"}]}]}`, affinity.TierNone},
		{"short strings in an array ahead", `{"x":[` + strings.Repeat(`"a",`, n) + `"b"],"messages":[{"role":"user","content":"Q"}]}`, affinity.TierNone},
		{"adjacent strings in an array (invalid)", `{"x":[` + strings.Repeat(`"a"`, n) + `],"messages":[{"role":"user","content":"Q"}]}`, affinity.TierNone},
		{"nested objects ahead", `{"x":` + strings.Repeat(`{"k":`, n/8) + `1` + strings.Repeat(`}`, n/8) + `,"messages":[{"role":"user","content":"Q"}]}`, affinity.TierNone},
		{"numbers ahead", `{"x":[` + strings.Repeat(`1,`, n) + `2],"messages":[{"role":"user","content":"Q"}]}`, affinity.TierNone},
		{"prompt list of short strings", `{"prompt":["first prompt",` + strings.Repeat(`"a",`, n) + `"b"]}`, affinity.TierPrefixHash},
		{"prompt token ids", `{"prompt":[` + strings.Repeat(`1,`, n) + `2]}`, affinity.TierPrefixHash},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, _ := assertBounded(t, []byte(tc.body))
			if tc.tier == affinity.TierNone {
				if res.Tier != affinity.TierNone {
					t.Fatalf("got %+v, want a fail-open", res)
				}
				return
			}
			if res.Tier != tc.tier {
				t.Fatalf("got %+v, want tier %s", res, tc.tier)
			}
		})
	}
}

// TestDuplicatesStopAtCap: a body of 50,000 duplicate "messages" members
// stops at the second one and fails open (M1-25), within the bound.
func TestDuplicatesStopAtCap(t *testing.T) {
	a := `[{"role":"user","content":"A"}]`
	b := `[{"role":"user","content":"B"}]`
	c := `[{"role":"user","content":"C"}]`
	body := `{"messages":` + a + `,` + strings.Repeat(`"messages":`+b+`,`, 50_000) + `"messages":` + c + `}`
	res, _ := assertBounded(t, []byte(body))
	assertResult(t, res, affinity.TierNone, affinity.ReasonDuplicateKey)
}
