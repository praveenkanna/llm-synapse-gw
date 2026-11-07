package contextbudget

import (
	"encoding/json"
	"runtime"
	"strings"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex/bodyindextest"
)

// messagesWith is a chat body whose one message opens with members.
func messagesWith(members string) []byte {
	return []byte(`{"model":"m","messages":[{` + members + `"role":"user","content":"hi"}]}`)
}

// TestParseDoesNotDecodeEscapedKeys: a message's keys are read as its members
// are walked, so a decode shows up in bytes.
func TestParseDoesNotDecodeEscapedKeys(t *testing.T) {
	const size = 2 << 20
	for name, body := range map[string][]byte{
		"one huge key": messagesWith(`"` + strings.Repeat(bodyindextest.Longest("a"), size/6) + `":1,`),
		"short keys":   messagesWith(strings.Repeat(`"`+bodyindextest.Longest("a")+`":0,`, size/11)),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseBody(body, 0); err != nil {
				t.Fatal(err)
			}
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			_, _ = parseBody(body, 0)
			runtime.ReadMemStats(&after)
			if got, limit := after.TotalAlloc-before.TotalAlloc, uint64(len(body)/64); got > limit {
				t.Fatalf("Parse allocated %d bytes on a %d-byte body, want under %d", got, len(body), limit)
			}
		})
	}
}

// BenchmarkParseEscapedKeys times the keys of a message at the default
// context.max_body_bytes (docs/results).
func BenchmarkParseEscapedKeys(b *testing.B) {
	const size = 4 << 20
	for name, body := range map[string][]byte{
		"huge_key":   messagesWith(`"` + strings.Repeat(bodyindextest.Longest("a"), size/6) + `":1,`),
		"short_keys": messagesWith(strings.Repeat(`"`+bodyindextest.Longest("a")+`":0,`, size/11)),
	} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, _ = parseBody(body, 0)
			}
		})
	}
}

// An escape changes what a key costs, not what it means.
func TestParseReadsEscapedKeysInTheirLongestSpelling(t *testing.T) {
	long := bodyindextest.Longest
	body := []byte(`{"model":"m","messages":[{"` + long("role") + `":"user","` + long("content") + `":[{"` + long("type") + `":"image_url"}]}]}`)
	if _, err := parseBody(body, 0); err == nil || !strings.Contains(err.Error(), "image_url") {
		t.Fatalf("Parse = %v, want the content part read through its escaped keys", err)
	}
	body = []byte(`{"model":"m","messages":[{"` + long("role") + `a":"user"}]}`)
	if _, err := parseBody(body, 0); err == nil {
		t.Fatal("a key one byte past the longest spelling was read as role")
	}
	if !json.Valid(body) {
		t.Fatal("the test body is not JSON")
	}
}
