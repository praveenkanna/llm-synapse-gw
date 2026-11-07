package workload

import (
	"encoding/json"
	"math/rand/v2"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

func seg(kind, text string) SpecSegment { return SpecSegment{Kind: kind, Text: text} }

func TestRenderPlaceholders(t *testing.T) {
	vars := map[string]string{"a": "X", "b_2": "yy", "inj": "{{a}}", "e": ""}
	tests := []struct {
		name string
		segs []SpecSegment
		want string
	}{
		{"static is literal", []SpecSegment{seg(KindStatic, "keep {{a}} as is")}, "keep {{a}} as is"},
		{"volatile substitutes", []SpecSegment{seg(KindVolatile, "id={{a}}.")}, "id=X."},
		{"semi_static substitutes", []SpecSegment{seg(KindSemiStatic, "t: {{b_2}}")}, "t: yy"},
		{"adjacent and repeated", []SpecSegment{seg(KindVolatile, "{{a}}{{b_2}}{{a}}")}, "XyyX"},
		{"segments joined with nothing between", []SpecSegment{seg(KindStatic, "s"), seg(KindSemiStatic, "{{b_2}}"), seg(KindVolatile, "{{a}}")}, "syyX"},
		{"spaces inside braces are not a placeholder", []SpecSegment{seg(KindVolatile, "{{ a }}")}, "{{ a }}"},
		{"upper case is not a placeholder", []SpecSegment{seg(KindVolatile, "{{A}}")}, "{{A}}"},
		{"leading digit is not a placeholder", []SpecSegment{seg(KindVolatile, "{{1a}}")}, "{{1a}}"},
		{"unclosed is literal", []SpecSegment{seg(KindVolatile, "{{a} {a}}")}, "{{a} {a}}"},
		{"triple braces keep the outer ones", []SpecSegment{seg(KindVolatile, "{{{a}}}")}, "{X}"},
		{"values are not expanded again", []SpecSegment{seg(KindVolatile, "{{inj}}")}, "{{a}}"},
		{"empty value", []SpecSegment{seg(KindVolatile, "<{{e}}>")}, "<>"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &TemplateSpec{Messages: []SpecMessage{{Role: "user", Segments: tc.segs}}}
			msgs, err := s.render(vars)
			if err != nil {
				t.Fatal(err)
			}
			if len(msgs) != 1 || msgs[0].Role != "user" || msgs[0].Content != tc.want {
				t.Fatalf("render = %+v, want one user message %q", msgs, tc.want)
			}
		})
	}
}

func TestRenderMissingVariable(t *testing.T) {
	s := &TemplateSpec{Messages: []SpecMessage{
		{Role: "system", Segments: []SpecSegment{seg(KindStatic, "s")}},
		{Role: "user", Segments: []SpecSegment{seg(KindVolatile, "{{a}}"), seg(KindVolatile, "{{gone}}")}},
	}}
	_, err := s.render(map[string]string{"a": "x"})
	if err == nil || !strings.Contains(err.Error(), `messages[1].segments[1]: no value for variable "gone"`) {
		t.Fatalf("err = %v, want a missing-variable error naming the segment", err)
	}
}

const validSpec = `{
  "model": "spec-model",
  "variables": [{"name": "tenant"}, {"name": "q"}],
  "messages": [
    {"role": "system", "segments": [
      {"kind": "static", "text": "You help."},
      {"kind": "semi_static", "text": " Tenant: {{tenant}}"}]},
    {"role": "user", "segments": [{"kind": "volatile", "text": "{{q}}"}]}
  ],
  "examples": [{"tenant": "a", "q": "b"}],
  "cacheable": false, "cache_ttl": "0s", "coalesce": false
}`

func TestParseSpec(t *testing.T) {
	s, err := ParseSpec([]byte(validSpec))
	if err != nil {
		t.Fatal(err)
	}
	msgs, err := s.render(s.Examples[0])
	if err != nil {
		t.Fatal(err)
	}
	if msgs[0].Content != "You help. Tenant: a" || msgs[1].Content != "b" {
		t.Fatalf("render = %+v", msgs)
	}
	// Order is not the harness's to check: a misordered spec renders.
	mis := strings.Replace(validSpec, `{"kind": "static", "text": "You help."}`, `{"kind": "volatile", "text": "{{q}}"}, {"kind": "static", "text": "You help."}`, 1)
	if _, err := ParseSpec([]byte(mis)); err != nil {
		t.Fatalf("misordered spec rejected: %v", err)
	}
}

func TestParseSpecRejects(t *testing.T) {
	tests := []struct {
		name, old, new, want string
	}{
		{"unknown field", `"model": "spec-model",`, `"model": "spec-model", "stop": [],`, "unknown field"},
		{"no model", `"model": "spec-model"`, `"model": ""`, "model is required"},
		{"no messages", validSpec[strings.Index(validSpec, `"messages"`):strings.Index(validSpec, `"examples"`)], `"messages": [], `, "messages"},
		{"unknown role", `"role": "user"`, `"role": "tool"`, `unknown role "tool"`},
		{"unknown kind", `"kind": "volatile"`, `"kind": "dynamic"`, `messages[1].segments[0]: unknown kind`},
		{"no segments", `"segments": [{"kind": "volatile", "text": "{{q}}"}]`, `"segments": []`, "messages[1]: no segments"},
		{"placeholder in static", `"text": "You help."`, `"text": "You help {{q}}."`, "messages[0].segments[0]: static text holds placeholder {{q}}"},
		{"undeclared variable", `"text": "{{q}}"`, `"text": "{{q}} {{who}}"`, "{{who}} names an undeclared variable"},
		{"unused variable", `{"name": "q"}`, `{"name": "q"}, {"name": "spare"}`, `"spare" is declared but never used`},
		{"duplicate variable", `{"name": "q"}`, `{"name": "q"}, {"name": "q"}`, "declared twice"},
		{"bad variable name", `{"name": "q"}`, `{"name": "Q"}`, "must match"},
		{"example misses a variable", `{"tenant": "a", "q": "b"}`, `{"tenant": "a"}`, `examples[0]: missing variable "q"`},
		{"example names an unknown one", `{"tenant": "a", "q": "b"}`, `{"tenant": "a", "q": "b", "x": "c"}`, `examples[0]: unknown variable "x"`},
		{"trailing data", "\n}", "\n} {}", "trailing data"},
		{"unknown scope", `{"name": "q"}`, `{"name": "q", "scope": "team"}`, `variables[1].scope: unknown scope "team"`},
		{"coalesce with an undeclared scope", `"coalesce": false`, `"coalesce": true`, `variables[0].scope: must be "shared"`},
		{"cacheable with an undeclared scope", `"cacheable": false, "cache_ttl": "0s"`, `"cacheable": true, "cache_ttl": "1m"`, `variables[0].scope: must be "shared"`}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if !strings.Contains(validSpec, tc.old) {
				t.Fatalf("test setup: %q not in spec", tc.old)
			}
			_, err := ParseSpec([]byte(strings.Replace(validSpec, tc.old, tc.new, 1)))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

// TestShippedM4Specs: the M4 trail-FAQ specs render the same text, opt in
// as their names say, and declare every variable shared (else the gateway
// answers 422).
func TestShippedM4Specs(t *testing.T) {
	want := map[string][2]bool{ // coalesce, cacheable
		"m4-faq-coalesce.json": {true, false},
		"m4-faq-cache.json":    {false, true},
		"m4-faq-plain.json":    {false, false},
	}
	var text string
	for name, opt := range want {
		s, _, err := LoadSpec(filepath.Join("../../profiles/templates", name))
		if err != nil {
			t.Fatal(err)
		}
		if s.Coalesce != opt[0] || s.Cacheable != opt[1] || (s.Cacheable && s.CacheTTL == "") {
			t.Errorf("%s: coalesce %t cacheable %t ttl %q", name, s.Coalesce, s.Cacheable, s.CacheTTL)
		}
		for _, v := range s.Variables {
			if v.Scope != ScopeShared {
				t.Errorf("%s: variable %s scope %q, want shared", name, v.Name, v.Scope)
			}
		}
		msgs, err := s.render(s.Examples[0])
		if err != nil {
			t.Fatal(err)
		}
		got := msgs[0].Content + "\n" + msgs[1].Content
		if text != "" && got != text {
			t.Errorf("%s renders different text from the other M4 specs", name)
		}
		text = got
	}
}

// TestShippedM4Profiles: every templated request of the M4 profiles is
// deterministic (temperature 0) and caps its own max_tokens, so the gateway
// may coalesce or cache it and never sets a cap of its own.
func TestShippedM4Profiles(t *testing.T) {
	for _, name := range []string{"m4-cache", "m4-cache-baseline", "m4-soak"} {
		p, err := Load(filepath.Join("../../profiles", name+".yaml"))
		if err != nil {
			t.Fatal(err)
		}
		g := mustGen(t, p, Options{}, 1)
		for range 500 {
			r := g.Next()
			if r.TemplateID == "" {
				continue
			}
			var b struct {
				Temperature *float64 `json:"temperature"`
				MaxTokens   int      `json:"max_tokens"`
			}
			if err := json.Unmarshal(r.Body, &b); err != nil || b.Temperature == nil || *b.Temperature != 0 || b.MaxTokens < 1 {
				t.Fatalf("%s: %s request %s is not deterministic with a cap", name, r.Class, r.Body)
			}
		}
	}
}

func TestValueGens(t *testing.T) {
	uuidRE := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	hexRE := regexp.MustCompile(`^[0-9a-f]{12}$`)
	rng := rand.New(rand.NewPCG(1, 2))
	for range 200 {
		if v := (ValueGen{Gen: GenFixed, Value: "acme"}).sample(rng); v != "acme" {
			t.Fatalf("fixed = %q", v)
		}
		if v := (ValueGen{Gen: GenPick, Values: []string{"a", "b"}}).sample(rng); v != "a" && v != "b" {
			t.Fatalf("pick = %q", v)
		}
		if v := (ValueGen{Gen: GenHex, Length: 12}).sample(rng); !hexRE.MatchString(v) {
			t.Fatalf("hex = %q", v)
		}
		if v := (ValueGen{Gen: GenUUID}).sample(rng); !uuidRE.MatchString(v) {
			t.Fatalf("uuid = %q", v)
		}
		ts := (ValueGen{Gen: GenTimestamp}).sample(rng)
		if at, err := time.Parse(time.RFC3339, ts); err != nil || at.Year() != 2026 || len(ts) != 20 {
			t.Fatalf("timestamp = %q (%v)", ts, err)
		}
		if v, err := strconv.ParseInt((ValueGen{Gen: GenInt, Min: 10, Max: 12}).sample(rng), 10, 64); err != nil || v < 10 || v > 12 {
			t.Fatalf("int = %d (%v), want 10..12", v, err)
		}
		if v := (ValueGen{Gen: GenInt, Min: 5, Max: 5}).sample(rng); v != "5" {
			t.Fatalf("int [5, 5] = %q", v)
		}
		for _, n := range []int{1, 2, 5, 170} {
			w := (ValueGen{Gen: GenWords, Length: n}).sample(rng)
			if len(w) != n || w[0] == ' ' || w[n-1] == ' ' || strings.Contains(w, "  ") || !synthetic(w) {
				t.Fatalf("words(%d) = %q", n, w)
			}
		}
	}
}

func TestValueGenRejects(t *testing.T) {
	tests := []struct {
		name string
		v    ValueGen
		want string
	}{
		{"missing gen", ValueGen{}, "missing gen"},
		{"unknown gen", ValueGen{Gen: "lorem"}, "unknown gen"},
		{"pick without values", ValueGen{Gen: GenPick}, "pick needs"},
		{"hex without length", ValueGen{Gen: GenHex}, "hex takes only length"},
		{"hex too long", ValueGen{Gen: GenHex, Length: 257}, "hex takes only length"},
		{"words without length", ValueGen{Gen: GenWords}, "words takes only length"},
		{"uuid with params", ValueGen{Gen: GenUUID, Length: 3}, "uuid takes no parameters"},
		{"fixed with values", ValueGen{Gen: GenFixed, Values: []string{"a"}}, "fixed takes only value"},
		{"int max below min", ValueGen{Gen: GenInt, Min: 5, Max: 4}, "int takes only min and max"},
		{"int negative", ValueGen{Gen: GenInt, Min: -1, Max: 4}, "int takes only min and max"},
		{"int with length", ValueGen{Gen: GenInt, Max: 4, Length: 2}, "int takes only min and max"},
		{"hex with max", ValueGen{Gen: GenHex, Length: 4, Max: 9}, "hex takes no min or max"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.v.validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

// Character targets of AC-M3-6's prompt (docs/plan/phase-3-compiled.md).
const (
	volatileAt     = 732
	minPromptChars = 4850
	maxPromptChars = 4950
	minStaticShare = 0.88
)

// promptChars is the rendered prompt's length: every message's content.
func promptChars(msgs []chatMessage) int {
	n := 0
	for _, m := range msgs {
		n += len(m.Content)
	}
	return n
}

// staticPrefix is the length of the content before the spec's first
// non-static segment, across messages: what every render shares.
func staticPrefix(s *TemplateSpec) int {
	n := 0
	for _, m := range s.Messages {
		for _, sg := range m.Segments {
			if sg.Kind != KindStatic {
				return n
			}
			n += len(sg.Text)
		}
	}
	return n
}

func loadShippedSpec(t *testing.T, name string) *TemplateSpec {
	t.Helper()
	s, sum, err := LoadSpec(filepath.Join("../../profiles/templates", name))
	if err != nil {
		t.Fatal(err)
	}
	if len(sum) != 64 {
		t.Fatalf("sha256 = %q", sum)
	}
	if s.Model != "large-model" || len(s.Examples) < 2 {
		t.Fatalf("%s: model %q, %d examples; want large-model and 2+", name, s.Model, len(s.Examples))
	}
	return s
}

func asciiOnly(t *testing.T, s *TemplateSpec) {
	t.Helper()
	for _, m := range s.Messages {
		for _, sg := range m.Segments {
			for i := 0; i < len(sg.Text); i++ {
				if sg.Text[i] >= 0x80 {
					t.Fatalf("non-ASCII byte in %q: character offsets would differ from byte offsets", sg.Text)
				}
			}
		}
	}
}

// TestShippedTemplateSpecs pins the AC-M3-6 prompt shapes: the misordered
// spec's volatile text starts at character 732 of its system message, both
// render to 4,850-4,950 characters, and the compiled one is the same text
// with at least 88% of it a static prefix.
func TestShippedTemplateSpecs(t *testing.T) {
	mis := loadShippedSpec(t, "m3-prompt-misordered.json")
	comp := loadShippedSpec(t, "m3-prompt-compiled.json")
	asciiOnly(t, mis)
	asciiOnly(t, comp)

	if got := staticPrefix(mis); got != volatileAt {
		t.Fatalf("misordered: first volatile character at %d, want %d", got, volatileAt)
	}
	if mis.Messages[0].Role != "system" || mis.Messages[0].Segments[1].Kind != KindVolatile ||
		mis.Messages[0].Segments[2].Kind != KindStatic || len(mis.Messages[0].Segments[2].Text) < 3500 {
		t.Fatal("misordered: want static, volatile, then the long static remainder in the system message")
	}
	for i, ex := range mis.Examples {
		msgs, err := mis.render(ex)
		if err != nil {
			t.Fatal(err)
		}
		sys := msgs[0].Content
		if !strings.HasPrefix(sys[volatileAt:], ex["request_id"]) {
			t.Fatalf("misordered example %d: character %d starts %q, want the request ID", i, volatileAt, sys[volatileAt:volatileAt+20])
		}
		n := promptChars(msgs)
		if n < minPromptChars || n > maxPromptChars {
			t.Fatalf("misordered example %d: %d characters, want %d-%d", i, n, minPromptChars, maxPromptChars)
		}
		t.Logf("misordered example %d: volatile at %d of %d characters (%.1f%% static prefix)", i, volatileAt, n, 100*float64(volatileAt)/float64(n))
	}

	for i, ex := range comp.Examples {
		msgs, err := comp.render(ex)
		if err != nil {
			t.Fatal(err)
		}
		misMsgs, _ := mis.render(ex)
		n, p := promptChars(msgs), staticPrefix(comp)
		if n != promptChars(misMsgs) {
			t.Fatalf("compiled example %d: %d characters, misordered %d; want the same text", i, n, promptChars(misMsgs))
		}
		if n < minPromptChars || n > maxPromptChars {
			t.Fatalf("compiled example %d: %d characters, want %d-%d", i, n, minPromptChars, maxPromptChars)
		}
		if share := float64(p) / float64(n); share < minStaticShare {
			t.Fatalf("compiled example %d: static prefix %d of %d characters (%.3f), want >= %.2f", i, p, n, share, minStaticShare)
		}
		t.Logf("compiled example %d: static prefix %d of %d characters (%.1f%%)", i, p, n, 100*float64(p)/float64(n))
	}

	// Same text, reordered: the same segments as multisets.
	texts := func(s *TemplateSpec) []string {
		var out []string
		for _, m := range s.Messages {
			for _, sg := range m.Segments {
				out = append(out, m.Role+"/"+sg.Kind+"/"+sg.Text)
			}
		}
		slices.Sort(out)
		return out
	}
	joinedStatic := func(s *TemplateSpec) string {
		var b strings.Builder
		for _, m := range s.Messages {
			for _, sg := range m.Segments {
				if sg.Kind == KindStatic {
					b.WriteString(sg.Text)
				}
			}
		}
		return b.String()
	}
	if len(joinedStatic(mis)) != len(joinedStatic(comp)) {
		t.Fatal("compiled and misordered static text differ in length")
	}
	misV, compV := slices.DeleteFunc(texts(mis), isStaticEntry), slices.DeleteFunc(texts(comp), isStaticEntry)
	if !slices.Equal(misV, compV) {
		t.Fatalf("volatile segments differ:\n%q\n%q", misV, compV)
	}
}

func isStaticEntry(s string) bool { return strings.Contains(s, "/"+KindStatic+"/") }

// TestShippedM3Profiles renders the two AC-M3-6 profiles through the
// generator: the offsets hold for generated values too, and only the
// compiled profile sends template headers.
func TestShippedM3Profiles(t *testing.T) {
	for _, tc := range []struct {
		file, id string
		version  int
	}{
		{"m3-misordered.yaml", "", 0},
		{"m3-compiled.yaml", "support-assistant", 1},
	} {
		t.Run(tc.file, func(t *testing.T) {
			p, err := Load(filepath.Join("../../profiles", tc.file))
			if err != nil {
				t.Fatal(err)
			}
			c := p.Classes[0]
			if c.Template == nil || c.Template.SpecSHA256 == "" {
				t.Fatalf("template not loaded: %+v", c.Template)
			}
			prefix := staticPrefix(c.Template.spec)
			g := mustGen(t, p, Options{}, 7)
			lo, hi := maxPromptChars, 0
			for i := range 500 {
				r := g.NextAt(time.Duration(i) * 300 * time.Millisecond)
				if r.TemplateID != tc.id || r.TemplateVersion != tc.version {
					t.Fatalf("template headers = %q/%d, want %q/%d", r.TemplateID, r.TemplateVersion, tc.id, tc.version)
				}
				b := decode(t, r)
				if b.Model != "large-model" || !b.Stream || b.StreamOptions == nil || !b.StreamOptions.IncludeUsage || len(b.Messages) != 2 {
					t.Fatalf("bad request %s", r.Body)
				}
				msgs := []chatMessage{{b.Messages[0].Role, b.Messages[0].Content}, {b.Messages[1].Role, b.Messages[1].Content}}
				n := promptChars(msgs)
				lo, hi = min(lo, n), max(hi, n)
				if n < minPromptChars || n > maxPromptChars {
					t.Fatalf("request %d: %d characters, want %d-%d", i, n, minPromptChars, maxPromptChars)
				}
				if tc.id == "" && prefix != volatileAt {
					t.Fatalf("misordered static prefix %d, want %d", prefix, volatileAt)
				}
				if share := float64(prefix) / float64(n); tc.id != "" && share < minStaticShare {
					t.Fatalf("compiled static prefix %d of %d (%.3f)", prefix, n, share)
				}
			}
			t.Logf("%s: static prefix %d, prompts %d-%d characters (%.1f%%-%.1f%% static)", tc.file, prefix, lo, hi,
				100*float64(prefix)/float64(hi), 100*float64(prefix)/float64(lo))
		})
	}
}

const templateProfile = `
name: t
model: m
arrival: {rate: 1}
classes:
  - name: tpl
    weight: 1
    stream: true
    max_tokens: 8
    priority: user
    template:
      spec: spec.json
      id: tenant-qa
      version: 2
      variables:
        tenant: acme
        q: {gen: words, length: 12}
`

// writeTemplateProfile writes validSpec and profile into a temp directory
// and returns the profile's path.
func writeTemplateProfile(t *testing.T, profile string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "spec.json"), []byte(validSpec), 0o644); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "p.yaml")
	if err := os.WriteFile(path, []byte(profile), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestParseTemplateClass(t *testing.T) {
	p, err := Load(writeTemplateProfile(t, templateProfile))
	if err != nil {
		t.Fatal(err)
	}
	tpl := p.Classes[0].Template
	if tpl == nil || tpl.ID != "tenant-qa" || tpl.Version != 2 || tpl.spec == nil || len(tpl.SpecSHA256) != 64 {
		t.Fatalf("template = %+v", tpl)
	}
	if v := tpl.Variables["tenant"]; v.Gen != GenFixed || v.Value != "acme" || v.Values != nil || v.Length != 0 {
		t.Errorf("plain string generator = %+v, want fixed acme", v)
	}
	if v := tpl.Variables["q"]; v.Gen != GenWords || v.Length != 12 {
		t.Errorf("q generator = %+v", v)
	}
}

func TestParseTemplateClassRejects(t *testing.T) {
	tests := []struct {
		name, old, new, want string
	}{
		{"id without version", "      version: 2\n", "", "id and version go together"},
		{"version without id", "      id: tenant-qa\n", "", "id and version go together"},
		{"bad id", "id: tenant-qa", "id: Tenant QA", "must match"},
		{"negative version", "version: 2", "version: -1", "positive"},
		{"missing generator", "        tenant: acme\n", "", `no generator for "tenant"`},
		{"generator for undeclared variable", "        tenant: acme\n", "        tenant: acme\n        who: x\n", `"who" is not declared`},
		{"bad generator", "{gen: words, length: 12}", "{gen: words}", "variables.q: words takes only length"},
		{"with prompt_words", "    max_tokens: 8\n", "    max_tokens: 8\n    prompt_words: 3\n", "leave them unset"},
		{"with system_words", "    max_tokens: 8\n", "    max_tokens: 8\n    system_words: 3\n", "leave them unset"},
		{"multi-turn", "    max_tokens: 8\n", "    max_tokens: 8\n    turns: 2\n    reply_words: 2\n    open_conversations: 2\n", "single-shot"},
		{"completions api", "model: m\n", "model: m\napi: completions\n", "needs the chat API"},
		{"missing spec file", "spec: spec.json", "spec: nope.json", "nope.json"},
		{"no spec", "      spec: spec.json\n", "", "spec is required"},
		{"unknown template key", "      id: tenant-qa\n", "      id: tenant-qa\n      headers: true\n", "field headers not found"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if !strings.Contains(templateProfile, tc.old) {
				t.Fatalf("test setup: %q not in profile", tc.old)
			}
			_, err := Load(writeTemplateProfile(t, strings.Replace(templateProfile, tc.old, tc.new, 1)))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

// TestGeneratorTemplateRequests: a template class renders the spec with
// generated values, carries the template headers only when id and version
// are set, and keeps priority and the body fields.
func TestGeneratorTemplateRequests(t *testing.T) {
	p, err := Load(writeTemplateProfile(t, templateProfile))
	if err != nil {
		t.Fatal(err)
	}
	r := mustGen(t, p, Options{}, 3).Next()
	if r.TemplateID != "tenant-qa" || r.TemplateVersion != 2 || r.Priority != "user" || r.Path != pathChat || !r.Stream {
		t.Fatalf("request = %+v", r)
	}
	b := decode(t, r)
	if len(b.Messages) != 2 || b.Messages[0] != (message{"system", "You help. Tenant: acme"}) ||
		b.Messages[1].Role != "user" || len(b.Messages[1].Content) != 12 || b.MaxTokens != 8 || b.Model != "m" {
		t.Fatalf("body = %+v", b)
	}
	for _, field := range []string{"template", "tenant-qa", "priority"} {
		if strings.Contains(string(r.Body), field) {
			t.Fatalf("%q leaked into the body: %s", field, r.Body)
		}
	}

	plain, err := Load(writeTemplateProfile(t, strings.Replace(templateProfile, "      id: tenant-qa\n      version: 2\n", "", 1)))
	if err != nil {
		t.Fatal(err)
	}
	if r := mustGen(t, plain, Options{}, 3).Next(); r.TemplateID != "" || r.TemplateVersion != 0 {
		t.Fatalf("plain template request carries headers %q/%d", r.TemplateID, r.TemplateVersion)
	}

	// No model in the profile: the spec's model is sent.
	noModel, err := Load(writeTemplateProfile(t, strings.Replace(templateProfile, "model: m\n", "", 1)))
	if err != nil {
		t.Fatal(err)
	}
	if b := decode(t, mustGen(t, noModel, Options{DefaultModel: "fallback"}, 3).Next()); b.Model != "spec-model" {
		t.Fatalf("model = %q, want the spec's", b.Model)
	}

	x, y := mustGen(t, p, Options{}, 5), mustGen(t, p, Options{}, 5)
	for i := range 50 {
		if string(x.Next().Body) != string(y.Next().Body) {
			t.Fatalf("request %d differs with the same seed", i)
		}
	}
}
