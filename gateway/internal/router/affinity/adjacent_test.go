package affinity_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/affinity"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// TestAdjacentStrings: two strings with nothing between them stop the scan
// as unparseable (M1-22); valid bodies whose bytes come close (a space
// between the strings, a string ending in an escaped quote or an escaped
// backslash) still key. Escapes are built at run time from bs, and every
// case is checked against encoding/json.
func TestAdjacentStrings(t *testing.T) {
	q := `,"messages":[{"role":"user","content":"Q"}]}`
	cases := []struct {
		name, x string
		valid   bool
	}{
		{"adjacent", `["a""b"]`, false},
		{"adjacent empty", `["""a"]`, false},
		{"adjacent in an object value", `{"k":"a""b"}`, false},
		{"space between", `["a" ,"b"]`, true},
		{"newline between", "[\"a\"\n,\"b\"]", true},
		{"escaped quote at the end, then empty", `["say ` + bs + `"hi` + bs + `"",""]`, true},
		{"escaped quote at the end, alone", `["say ` + bs + `"hi` + bs + `""]`, true},
		{"escaped backslash at the end, then empty", `["a` + bs + bs + `",""]`, true},
		{"escaped backslash at the end, adjacent", `["a` + bs + bs + `"""]`, false},
		{"three escaped backslashes and a quote", `["a` + bs + bs + bs + `"",""]`, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := json.Valid([]byte(tc.x)); got != tc.valid {
				t.Fatalf("json.Valid(%s) = %v, the case says %v", tc.x, got, tc.valid)
			}
			res := affinity.Extract(nil, []byte(`{"x":`+tc.x+q))
			if tc.valid && res.Tier != affinity.TierPrefixHash {
				t.Errorf("valid %s: got %+v, want a key", tc.x, res)
			}
			if !tc.valid {
				assertResult(t, res, affinity.TierNone, telemetry.ReasonUnparseableBody)
			}
		})
	}
}

// FuzzSkipValueValid: the scanner accepts every valid JSON value. For any
// input encoding/json calls valid, skipValue succeeds and stops at its end
// (only whitespace after it), and a body carrying it as a member ahead of
// messages never fails open as unparseable_body (the M1-22 adjacent-strings
// stop has no false positive on valid JSON).
func FuzzSkipValueValid(f *testing.F) {
	for _, seed := range []string{
		`""`, `"a"`, `["a","b"]`, `["a" ,"b"]`, `{"k":"v","j":[1,2,{"x":null}]}`, `[true,false,null,-0.5e10]`,
		`["say ` + bs + `"hi` + bs + `"",""]`, `["a` + bs + bs + `",""]`, `"` + bs + `u0065"`, `[[],{},[{}],""]`,
		"[\"a\"\n,\"b\"]", `{"":""}`, `12345678901234567890`, ` [ "a" , { "b" : "c" } ] `,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, in []byte) {
		if !json.Valid(in) {
			return
		}
		end, ok := affinity.SkipValueAll(in)
		if !ok {
			t.Fatalf("skipValue failed on valid JSON %q", in)
		}
		if rest := bytes.TrimLeft(in[end:], " \t\r\n"); len(rest) != 0 {
			t.Fatalf("skipValue stopped at %d of valid JSON %q", end, in)
		}
		body := append(append([]byte(`{"x":`), in...), `,"messages":[{"role":"user","content":"Q"}]}`...)
		if res := affinity.Extract(nil, body); res.Reason == telemetry.ReasonUnparseableBody {
			t.Fatalf("valid member %q made the body unparseable", in)
		}
	})
}
