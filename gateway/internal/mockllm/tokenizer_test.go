package mockllm

import (
	"encoding/json"
	"hash/fnv"
	"slices"
	"strings"
	"testing"
)

// refTokenID is an independent FNV-1a reference for tokenID.
func refTokenID(chunk string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(chunk))
	return int(h.Sum32() & (1<<31 - 1))
}

func TestTokenize(t *testing.T) {
	tests := []struct {
		text   string
		chunks []string
	}{
		{text: "", chunks: nil},
		{text: "a", chunks: []string{"a"}},
		{text: "abcd", chunks: []string{"abcd"}},
		{text: "abcdefghij", chunks: []string{"abcd", "efgh", "ij"}},
		{text: "café ☃", chunks: []string{"caf\xc3", "\xa9 \xe2\x98", "\x83"}}, // bytes, not runes
	}
	for _, tc := range tests {
		t.Run(tc.text, func(t *testing.T) {
			got := Tokenize(tc.text)
			want := make([]int, 0, len(tc.chunks))
			for _, c := range tc.chunks {
				want = append(want, refTokenID(c))
			}
			if !slices.Equal(got, want) {
				t.Fatalf("Tokenize(%q) = %v, want %v", tc.text, got, want)
			}
			if again := Tokenize(tc.text); !slices.Equal(again, got) {
				t.Fatalf("Tokenize(%q) not deterministic: %v then %v", tc.text, got, again)
			}
			if n := CountText(tc.text); n != len(got) {
				t.Fatalf("CountText(%q) = %d, want %d", tc.text, n, len(got))
			}
			for _, id := range got {
				if id < 0 || id >= 1<<31 {
					t.Fatalf("token id %d outside 31 bits", id)
				}
			}
		})
	}
	for n := range 40 {
		if got, want := CountText(strings.Repeat("x", n)), (n+3)/4; got != want {
			t.Fatalf("CountText(%d bytes) = %d, want %d", n, got, want)
		}
	}
}

func TestRenderChat(t *testing.T) {
	var msgs []Message
	raw := `[
		{"role":"system","content":"You are terse."},
		{"role":"user","content":[{"type":"text","text":"Hello, "},{"type":"image_url","image_url":{"url":"data:x"}},{"type":"text","text":"world"}]},
		{"role":"assistant","content":null,"tool_calls":[{"id":"c1","type":"function","function":{"name":"f","arguments":"{}"}}]},
		{"role":"tool","content":"42","tool_call_id":"c1"}
	]`
	if err := json.Unmarshal([]byte(raw), &msgs); err != nil {
		t.Fatalf("decode messages: %v", err)
	}
	body := "<|system|>\nYou are terse.\n<|user|>\nHello, world\n<|assistant|>\n\n<|tool|>\n42\n"
	if got := RenderChat(msgs, false); got != body {
		t.Fatalf("RenderChat(false) = %q, want %q", got, body)
	}
	if got := RenderChat(msgs, true); got != body+"<|assistant|>\n" {
		t.Fatalf("RenderChat(true) = %q", got)
	}
	if got, want := CountChat(msgs, true), CountText(body+"<|assistant|>\n"); got != want {
		t.Fatalf("CountChat = %d, want %d", got, want)
	}
	if got := RenderChat(nil, true); got != "<|assistant|>\n" || CountChat(nil, true) != 4 {
		t.Fatalf("RenderChat(nil, true) = %q, CountChat = %d", got, CountChat(nil, true))
	}
	// Marshals back to the OpenAI shape, so gateway tests can build bodies from it.
	b, err := json.Marshal(Message{Role: "user", Content: "hi"})
	if err != nil || string(b) != `{"role":"user","content":"hi"}` {
		t.Fatalf("Marshal = %s, %v", b, err)
	}
}

func TestMessageRejectsBadContent(t *testing.T) {
	for _, raw := range []string{`{"role":"user","content":7}`, `{"role":"user","content":{"text":"x"}}`, `{"role":"user","content":[1]}`} {
		var m Message
		if err := json.Unmarshal([]byte(raw), &m); err == nil {
			t.Errorf("Unmarshal(%s) = %+v, want error", raw, m)
		}
	}
}

func TestPromptTokens(t *testing.T) {
	tests := []struct {
		name, raw string
		want      []int
		wantErr   bool
	}{
		{name: "absent", raw: ``, want: nil},
		{name: "null", raw: `null`, want: nil},
		{name: "string", raw: `"once upon"`, want: Tokenize("once upon")},
		{name: "strings", raw: `["once", " upon"]`, want: append(Tokenize("once"), Tokenize(" upon")...)},
		{name: "token ids", raw: `[1, 2, 3]`, want: []int{1, 2, 3}},
		{name: "token id batches", raw: `[[1, 2], [3]]`, want: []int{1, 2, 3}},
		{name: "number", raw: `7`, wantErr: true},
		{name: "object", raw: `{"a":1}`, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := promptTokens(json.RawMessage(tc.raw))
			if (err != nil) != tc.wantErr || !slices.Equal(got, tc.want) {
				t.Fatalf("promptTokens(%s) = %v, %v; want %v (err %v)", tc.raw, got, err, tc.want, tc.wantErr)
			}
		})
	}
}
