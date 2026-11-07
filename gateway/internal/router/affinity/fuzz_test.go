package affinity_test

import (
	"bytes"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/affinity"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// FuzzExtract: Extract never panics, never writes to the body, is
// deterministic, spends no more than its work limit (plus one step), and
// returns a well-formed Result.
func FuzzExtract(f *testing.F) {
	for _, seed := range []string{
		`{"messages":[{"role":"system","content":"S"},{"role":"user","content":"Q"}]}`,
		`{"messages":[{"role" : "developer","content":[{"type":"text","text":"D"}]},{"role":"user","content":"Q"}]}`,
		`{"messages":[{"role":"system"}],"messages":null}`,
		`{"tools":[{"type":"function","function":{"name":"f","parameters":{"messages":[]}}}],"messages":[{"role":"user","content":"a\"b\\"}]}`,
		`{"prompt":"Once upon a time"}`,
		`{"prompt":[[1,2,3],[4]]}`,
		`{"prompt":[1,2,3 ]}`,
		`{"messages":[{"role":"user","content":"A`,
		`{"messages":"x","prompt":["p"]}`,
		`{"x":"\\\\\"","messages":[1]}`,
		`[]`,
		``,
	} {
		f.Add([]byte(seed))
	}
	valid := map[string]bool{
		affinity.ReasonNoKey:            true,
		telemetry.ReasonUnparseableBody: true,
		affinity.ReasonEmptyMessages:    true,
		affinity.ReasonScanBudget:       true,
		affinity.ReasonDuplicateKey:     true,
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		orig := bytes.Clone(body)
		first := affinity.Extract(nil, body)
		if !bytes.Equal(orig, body) {
			t.Fatal("body modified")
		}
		if again := affinity.Extract(nil, body); again != first {
			t.Fatalf("not deterministic: %+v then %+v", first, again)
		}
		// An empty body is decided before the scan (no_affinity_key).
		if res, work, limit := affinity.ExtractStats(body); (len(body) > 0 && res != first) || work > limit+workSlack {
			t.Fatalf("scan spent %d units, limit %d (+%d slack), result %+v vs %+v", work, limit, workSlack, res, first)
		}
		switch first.Tier {
		case affinity.TierPrefixHash:
			if len(first.Key) != 16 || first.Reason != "" {
				t.Fatalf("bad prefix-hash result %+v", first)
			}
		case affinity.TierNone:
			if first.Key != "" || !valid[first.Reason] {
				t.Fatalf("bad fail-open result %+v", first)
			}
		default:
			t.Fatalf("unexpected tier in %+v", first)
		}
	})
}
