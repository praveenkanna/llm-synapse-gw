package affinity_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/affinity"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/affinity/affinitytest"
)

// bs is one backslash. Every escape in these tests is built from it at run
// time: an escape typed into a source file can be turned into the character
// it encodes by the tools that write the file, which silently removes the
// case under test.
var bs = string(rune(92))

// u returns the JSON escape of the 4 hex digits hex, e.g. u("0065").
func u(hex string) string { return bs + "u" + hex }

// upperHex upper-cases the hex letters a-f in s, leaving the u of each
// escape alone.
func upperHex(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'f' {
			return r - 'a' + 'A'
		}
		return r
	}, s)
}

// isKeyRaws are raw JSON strings (with their quotes) to match against keys:
// plain, escaped in lower- and upper-case hex, one escape at each position,
// fully escaped (48 bytes for "messages"), surrogates, and malformed.
func isKeyRaws() []string {
	q := func(s string) string { return `"` + s + `"` }
	raws := []string{
		q("messages"), q("Messages"), q("message"), q("messagesx"), q(""), q("m"),
		q("prompt"), q("role"), q("system"), q("developer"),
		q("m" + u("0065") + "ssages"), q("m" + u("0065") + "ss" + u("0061") + "ges"),
		q("m" + u("0045") + "ssages"), q(u("0070") + "rompt"), q("pr" + u("006F") + "mpt"),
		q(affinitytest.Escaped("messages")), q(upperHex(affinitytest.Escaped("messages"))),
		q(affinitytest.Escaped("prompt")), q(affinitytest.Escaped("messagez")), q(affinitytest.Escaped("message")),
		q("m" + bs + "/essages"), q("m" + bs + "nessages"), q("messages" + bs + bs), q("m" + bs + bs + "essages"),
		q("m" + u("00e9") + "ssages"), q("m" + u("d83d") + u("de00") + "ssages"), q("m" + u("d83d") + "ssages"),
		q("m" + u("de00") + "ssages"), q("m" + u("006") + "ssages"), q("m" + u("006g") + "ssages"),
		q("m" + u("00") + "essages"), q("m" + bs + "x65ssages"), q("m" + bs + "qssages"), q("messages" + bs),
		q("messages" + bs + bs + bs), q(bs + bs + bs), q(bs), q("m" + bs + `"` + "essages"),
	}
	// One escape at each position of each wanted key.
	for _, want := range []string{"messages", "prompt", "role", "system", "developer"} {
		for i := range want {
			raws = append(raws, q(want[:i]+affinitytest.Escaped(want[i:i+1])+want[i+1:]))
		}
	}
	return raws
}

// TestIsKeyMatchesJSON: the allocation-free key match (M1-22) agrees with
// decoding the raw JSON string with encoding/json, and matches "messages"
// and "prompt" through escapes.
func TestIsKeyMatchesJSON(t *testing.T) {
	escapedMatches := map[string]int{}
	for _, raw := range isKeyRaws() {
		for _, want := range []string{"messages", "prompt", "role", "system", "developer"} {
			var s string
			ref := json.Unmarshal([]byte(raw), &s) == nil && s == want
			got := affinity.IsKey([]byte(raw), []byte(want))
			if got != ref {
				t.Errorf("isKey(%s, %q) = %v, encoding/json says %v", raw, want, got, ref)
			}
			if got && strings.Contains(raw, bs) {
				escapedMatches[want]++
			}
		}
	}
	for _, want := range []string{"messages", "prompt"} {
		if escapedMatches[want] == 0 {
			t.Errorf("no escaped key matched %q: the escapes are missing from the test", want)
		}
	}
}

// TestIsKeyAllocs: matching a key never allocates, escaped or not.
func TestIsKeyAllocs(t *testing.T) {
	want := []byte("messages")
	for _, raw := range [][]byte{
		[]byte(`"messages"`),
		[]byte(`"m` + u("0065") + `ssages"`),
		[]byte(`"` + affinitytest.Escaped("messagez") + `"`),
	} {
		if n := testing.AllocsPerRun(100, func() { affinity.IsKey(raw, want) }); n != 0 {
			t.Errorf("isKey(%s) allocates %v times per call", raw, n)
		}
	}
}

// TestBodiesHaveEscapes: the generated bodies that are meant to hold JSON
// escapes really do (backslashes in the bytes), so a tool rewriting an
// escape into its character cannot silently turn them into plain text.
func TestBodiesHaveEscapes(t *testing.T) {
	byName := map[string][]byte{}
	for _, c := range append(affinitytest.LatencyCases(), affinitytest.WorstCases()...) {
		byName[c.Name] = c.Body
	}
	for name, min := range map[string]int{
		"escaped_keys_1.6MB_before_messages_failopen":           125_000,       // one escape per key
		"escaped_near_miss_keys_1.6MB_before_messages_failopen": 8 * 31_000,    // every byte of every key
		"system_256KB_escaped_json_failopen":                    256 << 10 / 8, // a backslash every few bytes
	} {
		body, ok := byName[name]
		if !ok {
			t.Fatalf("no shape %s", name)
		}
		if n := bytes.Count(body, []byte(bs)); n < min {
			t.Errorf("%s holds %d backslashes, want at least %d", name, n, min)
		}
	}
	if got, want := affinitytest.Escaped("k"), bs+"u006b"; got != want || len(got) != 6 {
		t.Errorf("Escaped(k) = %q, want %q", got, want)
	}
	if n := strings.Count(escapedJSON(64), bs); n < 8 {
		t.Errorf("escapedJSON(64) holds %d backslashes: %s", n, fmt.Sprintf("%q", escapedJSON(64)))
	}
}
