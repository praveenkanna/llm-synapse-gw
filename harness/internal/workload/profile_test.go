package workload

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const validProfile = `
name: t
model: m
arrival:
  rate: 5
  burst: {every: 10s, length: 2s, factor: 3, classes: [bg]}
classes:
  - name: chat
    weight: 1
    stream: true
    system_words: {dist: uniform, min: 5, max: 10}
    shared_prefixes: 2
    shared_prefix_ratio: 0.5
    turns: {dist: uniform, min: 1, max: 4}
    reply_words: 5
    open_conversations: 4
    prompt_words: 3
    max_tokens: 8
  - name: cls
    weight: 0
    stream: false
    triggered_by: {class: chat, count: 2}
    prompt_words: 2
    max_tokens: 1
  - name: bg
    weight: 1
    stream: false
    prompt_words: 20
    max_tokens: 4
`

func TestParseValid(t *testing.T) {
	p, err := Parse([]byte(validProfile))
	if err != nil {
		t.Fatal(err)
	}
	if p.API != APIChat {
		t.Errorf("api = %q, want chat by default", p.API)
	}
	if p.Arrival.Burst.Every != 10*time.Second || p.Arrival.Burst.Length != 2*time.Second {
		t.Errorf("burst = %+v", p.Arrival.Burst)
	}
	bg := p.Classes[2]
	if bg.Turns != Fixed(1) || bg.SystemWords != Fixed(0) {
		t.Errorf("defaults not applied: turns %+v system %+v", bg.Turns, bg.SystemWords)
	}
}

func TestParsePriority(t *testing.T) {
	tests := []struct {
		name, line, want string
	}{
		{name: "unset", line: "", want: ""},
		{name: "word", line: "    priority: background\n", want: "background"},
		{name: "digits dash underscore", line: "    priority: tier-2_batch\n", want: "tier-2_batch"},
		{name: "max length", line: "    priority: " + strings.Repeat("p", 32) + "\n", want: strings.Repeat("p", 32)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, err := Parse([]byte(edit(t, "    max_tokens: 4\n", "    max_tokens: 4\n"+tc.line)))
			if err != nil {
				t.Fatal(err)
			}
			if got := p.Classes[2].Priority; got != tc.want {
				t.Fatalf("priority = %q, want %q", got, tc.want)
			}
		})
	}
}

// edit replaces old with new in validProfile, failing if old is absent.
func edit(t *testing.T, pairs ...string) string {
	t.Helper()
	s := validProfile
	for i := 0; i < len(pairs); i += 2 {
		if !strings.Contains(s, pairs[i]) {
			t.Fatalf("test setup: %q not in profile", pairs[i])
		}
		s = strings.Replace(s, pairs[i], pairs[i+1], 1)
	}
	return s
}

func TestParseRejects(t *testing.T) {
	tests := []struct {
		name  string
		pairs []string
		want  string
	}{
		{"unknown key", []string{"model: m\n", "model: m\nprompt: hello\n"}, "field prompt not found"},
		{"no name", []string{"name: t", "name: ''"}, "name is required"},
		{"bad api", []string{"model: m\n", "model: m\napi: embeddings\n"}, "api must be"},
		{"zero rate", []string{"rate: 5", "rate: 0"}, "rate must be"},
		{"burst too long", []string{"length: 2s", "length: 10s"}, "0 < length < every"},
		{"burst classes need factor > 1", []string{"factor: 3", "factor: 1"}, "factor must be > 1"},
		{"burst unknown class", []string{"classes: [bg]", "classes: [nope]"}, `unknown class "nope"`},
		{"burst class triggers", []string{"classes: [bg]", "classes: [chat]"}, `"chat" triggers "cls"`},
		{"burst fields without every", []string{"every: 10s, ", ""}, "burst needs every"},
		{"duplicate class", []string{"name: bg", "name: chat"}, "listed twice"},
		{"bad class name", []string{"name: bg", "name: 'b g'"}, "name must be"},
		{"zero weight untriggered", []string{"    weight: 1\n    stream: false", "    weight: 0\n    stream: false"}, "weight must be > 0"},
		{"negative weight", []string{"    weight: 1\n    stream: false", "    weight: -1\n    stream: false"}, "non-negative"},
		{"trigger unknown", []string{"class: chat, count", "class: nope, count"}, `triggered_by unknown class "nope"`},
		{"trigger self", []string{"class: chat, count", "class: cls, count"}, "triggers itself"},
		{"trigger too many", []string{"count: 2}", "count: 65}"}, "at most 64"},
		{"response format value", []string{"stream: true\n", "stream: true\n    response_format: xml\n"}, "response_format must be"},
		{"response format on completions", []string{"model: m\n", "model: m\napi: completions\n", "stream: true\n", "stream: true\n    response_format: json_object\n"}, "needs the chat API"},
		{"ratio without prefixes", []string{"shared_prefixes: 2", "shared_prefixes: 0"}, "needs shared_prefixes"},
		{"ratio above one", []string{"shared_prefix_ratio: 0.5", "shared_prefix_ratio: 1.5"}, "in [0, 1]"},
		{"too many prefixes", []string{"shared_prefixes: 2", "shared_prefixes: 5000"}, "shared_prefixes must be"},
		{"multi-turn without reply", []string{"    reply_words: 5\n", ""}, "reply_words"},
		{"multi-turn without slots", []string{"    open_conversations: 4\n", ""}, "open_conversations"},
		{"too many slots", []string{"open_conversations: 4", "open_conversations: 20000"}, "open_conversations"},
		{"single-turn with reply", []string{"    prompt_words: 20\n", "    prompt_words: 20\n    reply_words: 3\n"}, "multi-turn classes only"},
		{"too many turns", []string{"min: 1, max: 4}", "min: 1, max: 101}"}, "at most 100"},
		{"context below first turn", []string{"    open_conversations: 4\n", "    open_conversations: 4\n    max_context_words: 10\n"}, "max_context_words"},
		{"zero prompt words", []string{"prompt_words: 20", "prompt_words: 0"}, "prompt_words"},
		{"zero max tokens", []string{"max_tokens: 4", "max_tokens: 0"}, "max_tokens"},
		{"priority upper case", []string{"    max_tokens: 4\n", "    max_tokens: 4\n    priority: Background\n"}, "priority must be"},
		{"priority with space", []string{"    max_tokens: 4\n", "    max_tokens: 4\n    priority: 'bulk jobs'\n"}, "priority must be"},
		{"priority with dot", []string{"    max_tokens: 4\n", "    max_tokens: 4\n    priority: a.b\n"}, "priority must be"},
		{"priority too long", []string{"    max_tokens: 4\n", "    max_tokens: 4\n    priority: " + strings.Repeat("p", 33) + "\n"}, "priority must be"},
		{"temperature too high", []string{"    max_tokens: 4\n", "    max_tokens: 4\n    temperature: 2.5\n"}, "temperature must be"},
		{"negative temperature", []string{"    max_tokens: 4\n", "    max_tokens: 4\n    temperature: -1\n"}, "temperature must be"},
		{"reserved header", []string{"    max_tokens: 4\n", "    max_tokens: 4\n    headers: {x-template-id: faq}\n"}, "X-Template-Id is set by the harness"},
		{"bad header name", []string{"    max_tokens: 4\n", "    max_tokens: 4\n    headers: {'bad name': a}\n"}, "not a header name"},
		{"bad header value", []string{"    max_tokens: 4\n", "    max_tokens: 4\n    headers: {X-A: \"a\\nb\"}\n"}, "printable ASCII"},
		{"no classes", []string{validProfile[strings.Index(validProfile, "classes:\n  - name: chat"):], "classes: []\n"}, "classes"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(edit(t, tc.pairs...)))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.want)
			}
		})
	}
	if _, err := Parse(nil); err == nil {
		t.Error("empty profile accepted")
	}
	if _, err := Parse(make([]byte, maxProfileBytes+1)); err == nil {
		t.Error("oversized profile accepted")
	}
}

func TestLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p.yaml")
	if err := os.WriteFile(path, []byte(validProfile), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(filepath.Join(dir, "missing.yaml")); err == nil {
		t.Fatal("missing file accepted")
	}
}
