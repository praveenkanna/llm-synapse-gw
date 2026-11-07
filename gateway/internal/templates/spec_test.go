package templates

import (
	"reflect"
	"strings"
	"testing"
)

// testSpec is a valid spec: a static then semi_static system message and a
// volatile user turn.
func testSpec() Spec {
	return Spec{
		Model:     "large-model",
		Variables: []Variable{{Name: "tenant"}, {Name: "question"}},
		Messages: []Message{
			{Role: "system", Segments: []Segment{
				{Kind: KindStatic, Text: "You are a support assistant. "},
				{Kind: KindSemiStatic, Text: "Tenant: {{tenant}}."},
			}},
			{Role: "user", Segments: []Segment{{Kind: KindVolatile, Text: "{{question}}"}}},
		},
		Examples: []map[string]string{{"tenant": "acme", "question": "hi"}},
	}
}

const testSpecJSON = `{
  "model": "large-model",
  "variables": [{"name": "tenant"}, {"name": "question"}],
  "messages": [
    {"role": "system", "segments": [
      {"kind": "static", "text": "You are a support assistant. "},
      {"kind": "semi_static", "text": "Tenant: {{tenant}}."}]},
    {"role": "user", "segments": [{"kind": "volatile", "text": "{{question}}"}]}
  ],
  "examples": [{"tenant": "acme", "question": "hi"}]
}
`

func TestParseSpec(t *testing.T) {
	got, err := ParseSpec([]byte(testSpecJSON))
	if err != nil {
		t.Fatalf("ParseSpec: %v", err)
	}
	if want := testSpec(); !reflect.DeepEqual(got, want) {
		t.Fatalf("ParseSpec = %+v, want %+v", got, want)
	}
}

func TestParseSpecRejects(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		{"unknown top-level field", `{"model":"m","messages":[],"extra":1}`, "unknown field"},
		{"unknown nested field", `{"model":"m","messages":[{"role":"user","segments":[],"name":"x"}]}`, "unknown field"},
		{"trailing object", `{"model":"m"} {"model":"n"}`, "trailing data"},
		{"trailing garbage", `{"model":"m"}x`, "trailing data"},
		{"not an object", `[1,2]`, "decode spec"},
		{"wrong type", `{"model":1}`, "decode spec"},
		{"truncated", `{"model":"m"`, "decode spec"},
		{"empty", ``, "decode spec"},
		{"too large", `{"model":"` + strings.Repeat("a", MaxSpecBytes) + `"}`, "over the"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseSpec([]byte(tc.in))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ParseSpec err = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestParseSpecAllowsTrailingSpace(t *testing.T) {
	if _, err := ParseSpec([]byte(`{"model":"m"}` + " \n\t")); err != nil {
		t.Fatalf("ParseSpec: %v", err)
	}
}
