package workload

import (
	"encoding/json"
	"math"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/harness/internal/runner"
)

// Compile-time checks: the generator plugs into the runner.
var (
	_ runner.Source      = (*Generator)(nil)
	_ runner.TimedSource = (*Generator)(nil)
)

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type body struct {
	Model         string    `json:"model"`
	Messages      []message `json:"messages"`
	Prompt        string    `json:"prompt"`
	MaxTokens     int       `json:"max_tokens"`
	Stream        bool      `json:"stream"`
	StreamOptions *struct {
		IncludeUsage bool `json:"include_usage"`
	} `json:"stream_options"`
	ResponseFormat *struct {
		Type string `json:"type"`
	} `json:"response_format"`
	IgnoreEOS bool `json:"ignore_eos"`
}

func decode(t *testing.T, r runner.Request) body {
	t.Helper()
	var b body
	if err := json.Unmarshal(r.Body, &b); err != nil {
		t.Fatalf("bad body %s: %v", r.Body, err)
	}
	return b
}

func words(s string) int { return len(strings.Fields(s)) }

func mustGen(t *testing.T, p *Profile, opts Options, seed uint64) *Generator {
	t.Helper()
	g, err := NewGenerator(p, opts, seed)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func single(c Class) *Profile {
	if c.Name == "" {
		c.Name = "c"
	}
	if c.Weight == 0 {
		c.Weight = 1
	}
	if c.PromptWords.IsZero() {
		c.PromptWords = Fixed(3)
	}
	if c.MaxTokens.IsZero() {
		c.MaxTokens = Fixed(4)
	}
	return &Profile{Name: "p", Model: "m", Arrival: Arrival{Rate: 1}, Classes: []Class{c}}
}

// synthetic reports whether s is only lower-case words separated by single
// spaces, as the syllable table produces.
func synthetic(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == ' ' && i > 0 && i < len(s)-1 && s[i-1] != ' ' {
			continue
		}
		if c < 'a' || c > 'z' {
			return false
		}
	}
	return true
}

func TestShippedProfiles(t *testing.T) {
	paths, err := filepath.Glob("../../profiles/*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, path := range paths {
		p, err := Load(path)
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, p.Name)
		if want := strings.TrimSuffix(filepath.Base(path), ".yaml"); p.Name != want {
			t.Errorf("%s: name %q, want %q", path, p.Name, want)
		}
		g := mustGen(t, p, Options{}, 1)
		seen := map[string]int{}
		for i := range 1000 {
			r := g.NextAt(time.Duration(i) * 150 * time.Millisecond)
			b := decode(t, r)
			seen[r.Class]++
			if b.Model == "" || b.MaxTokens < 1 || b.Stream != r.Stream || len(b.Messages) == 0 {
				t.Fatalf("%s: bad request %s", p.Name, r.Body)
			}
			total := 0
			templated := p.Classes[slices.IndexFunc(p.Classes, func(c Class) bool { return c.Name == r.Class })].Template != nil
			for _, m := range b.Messages {
				// Template classes send a spec's invented text; the
				// shipped specs are checked in TestShippedTemplateSpecs.
				if !templated && !synthetic(m.Content) {
					t.Fatalf("%s: non-synthetic text %q", p.Name, m.Content)
				}
				total += words(m.Content)
			}
			// Prompts must fit an 8k-token context at up to ~2 tokens per word.
			if total > 3500 {
				t.Fatalf("%s: request of %d words", p.Name, total)
			}
		}
		for _, c := range p.Classes {
			if seen[c.Name] == 0 {
				t.Errorf("%s: class %s never generated", p.Name, c.Name)
			}
		}
	}
	slices.Sort(names)
	if want := []string{"background", "chat", "classify", "m3-compiled", "m3-misordered", "m4-cache", "m4-cache-baseline", "m4-soak", "mixed", "structured"}; !slices.Equal(names, want) {
		t.Fatalf("profiles = %v, want %v", names, want)
	}
}

func TestGeneratorDeterministic(t *testing.T) {
	p, err := Load("../../profiles/mixed.yaml")
	if err != nil {
		t.Fatal(err)
	}
	x, y, z := mustGen(t, p, Options{}, 9), mustGen(t, p, Options{}, 9), mustGen(t, p, Options{}, 10)
	differs := false
	for i := range 2000 {
		at := time.Duration(i) * 100 * time.Millisecond
		rx, ry, rz := x.NextAt(at), y.NextAt(at), z.NextAt(at)
		if rx.Class != ry.Class || string(rx.Body) != string(ry.Body) {
			t.Fatalf("request %d differs with the same seed", i)
		}
		differs = differs || string(rx.Body) != string(rz.Body)
	}
	if !differs {
		t.Fatal("different seeds gave identical requests")
	}
}

func TestGeneratorBodyFields(t *testing.T) {
	p := single(Class{Stream: true, ResponseFormat: ResponseFormatJSON, IgnoreEOS: true, MaxTokens: Fixed(9)})
	b := decode(t, mustGen(t, p, Options{}, 1).Next())
	if b.Model != "m" || b.MaxTokens != 9 || !b.Stream || b.StreamOptions == nil || !b.StreamOptions.IncludeUsage ||
		b.ResponseFormat == nil || b.ResponseFormat.Type != "json_object" || !b.IgnoreEOS {
		t.Fatalf("body = %+v", b)
	}
	if len(b.Messages) != 1 || b.Messages[0].Role != "user" || words(b.Messages[0].Content) != 3 {
		t.Fatalf("messages = %+v, want one 3-word user message", b.Messages)
	}

	plain := single(Class{})
	r := mustGen(t, plain, Options{}, 1).Next()
	if raw := string(r.Body); strings.Contains(raw, "stream_options") || strings.Contains(raw, "response_format") || strings.Contains(raw, "ignore_eos") {
		t.Fatalf("optional fields sent when unset: %s", raw)
	}

	comp := single(Class{SystemWords: Fixed(4)})
	comp.API = APICompletions
	r = mustGen(t, comp, Options{}, 1).Next()
	b = decode(t, r)
	if r.Path != pathCompletions || len(b.Messages) != 0 || words(b.Prompt) != 7 || strings.Count(b.Prompt, "\n") != 1 {
		t.Fatalf("completions request %s %s", r.Path, r.Body)
	}
}

// TestGeneratorSamplingAndHeaders: temperature and seed go in the body only
// when set (temperature 0 included), and a class's headers ride on the
// request, not in the body.
func TestGeneratorSamplingAndHeaders(t *testing.T) {
	zero, seed := 0.0, int64(7)
	r := mustGen(t, single(Class{Temperature: &zero, Seed: &seed, Headers: map[string]string{"X-Mock-Output-Tokens": "400"}}), Options{}, 1).Next()
	var b struct {
		Temperature *float64 `json:"temperature"`
		Seed        *int64   `json:"seed"`
	}
	if err := json.Unmarshal(r.Body, &b); err != nil || b.Temperature == nil || *b.Temperature != 0 || b.Seed == nil || *b.Seed != 7 {
		t.Fatalf("body %s: want temperature 0 and seed 7", r.Body)
	}
	if r.Headers["X-Mock-Output-Tokens"] != "400" || strings.Contains(string(r.Body), "X-Mock") {
		t.Fatalf("headers %v, body %s", r.Headers, r.Body)
	}
	r = mustGen(t, single(Class{}), Options{}, 1).Next()
	if raw := string(r.Body); strings.Contains(raw, "temperature") || strings.Contains(raw, "seed") || r.Headers != nil {
		t.Fatalf("unset fields sent: %s, headers %v", raw, r.Headers)
	}
}

// TestGeneratorPriority: a class's priority rides on the request (the client
// sends it as X-Priority) and never enters the body.
func TestGeneratorPriority(t *testing.T) {
	tests := []struct {
		name, priority string
	}{
		{name: "set", priority: "background"},
		{name: "unset", priority: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := mustGen(t, single(Class{Priority: tc.priority}), Options{}, 1).Next()
			if r.Priority != tc.priority {
				t.Fatalf("request priority = %q, want %q", r.Priority, tc.priority)
			}
			if strings.Contains(string(r.Body), "priority") {
				t.Fatalf("priority leaked into the body: %s", r.Body)
			}
		})
	}
}

// TestMixedProfilePriorities pins the mixed profile's mapping onto the
// gateway's priority classes, which the M2 arbitration proof relies on.
func TestMixedProfilePriorities(t *testing.T) {
	p, err := Load("../../profiles/mixed.yaml")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"chat": "interactive", "classify": "interactive", "structured": "user", "background": "background"}
	g := mustGen(t, p, Options{}, 1)
	seen := map[string]string{}
	for i := range 2000 {
		r := g.NextAt(time.Duration(i) * 100 * time.Millisecond)
		seen[r.Class] = r.Priority
	}
	if len(seen) != len(want) {
		t.Fatalf("classes seen %v, want %v", seen, want)
	}
	for class, prio := range want {
		if seen[class] != prio {
			t.Errorf("class %s sent priority %q, want %q", class, seen[class], prio)
		}
	}
}

func TestGeneratorModels(t *testing.T) {
	p := single(Class{})
	p.Classes = append(p.Classes, Class{Name: "small", Model: "s", Weight: 1, PromptWords: Fixed(1), MaxTokens: Fixed(1)})
	models := func(g *Generator) map[string]string {
		out := map[string]string{}
		for range 200 {
			r := g.Next()
			out[r.Class] = decode(t, r).Model
		}
		return out
	}
	if got := models(mustGen(t, p, Options{DefaultModel: "d"}, 1)); got["c"] != "m" || got["small"] != "s" {
		t.Errorf("models = %v, want profile and class models", got)
	}
	if got := models(mustGen(t, p, Options{Model: "o"}, 1)); got["c"] != "o" || got["small"] != "o" {
		t.Errorf("models = %v, want override everywhere", got)
	}
	p.Model = ""
	if got := models(mustGen(t, p, Options{DefaultModel: "d"}, 1)); got["c"] != "d" || got["small"] != "s" {
		t.Errorf("models = %v, want default for the class without one", got)
	}
	if _, err := NewGenerator(p, Options{}, 1); err == nil {
		t.Error("class without any model accepted")
	}
}

func TestSharedPrefixRatio(t *testing.T) {
	const n, ratio = 20000, 0.6
	p := single(Class{SystemWords: Fixed(12), SharedPrefixes: 3, SharedPrefixRatio: ratio})
	g := mustGen(t, p, Options{}, 5)
	counts := map[string]int{}
	for range n {
		b := decode(t, g.Next())
		if b.Messages[0].Role != "system" || words(b.Messages[0].Content) != 12 {
			t.Fatalf("want a 12-word system prompt first: %+v", b.Messages)
		}
		counts[b.Messages[0].Content]++
	}
	shared := 0
	for _, c := range counts {
		if c > 1 {
			shared += c
		}
	}
	if len(counts) < n*(1-ratio)*0.9 {
		t.Errorf("only %d distinct system prompts; unique prefixes are not unique", len(counts))
	}
	if got := float64(shared) / n; math.Abs(got-ratio) > 0.02 {
		t.Errorf("shared-prefix share = %.3f, want %.2f", got, ratio)
	}
	for _, c := range counts {
		if c > 1 && math.Abs(float64(c)/n-ratio/3) > 0.02 {
			t.Errorf("shared prompt used %d times, want about %.0f", c, n*ratio/3)
		}
	}
}

func TestMultiTurnHistoryGrows(t *testing.T) {
	p := single(Class{
		SystemWords: Fixed(6), SharedPrefixes: 1, SharedPrefixRatio: 1,
		Turns: Fixed(4), ReplyWords: Fixed(5), OpenConversations: 1, PromptWords: Fixed(3),
	})
	g := mustGen(t, p, Options{}, 3)
	var prev []message
	for i := range 12 {
		b := decode(t, g.Next())
		turn := i % 4
		if len(b.Messages) != 2+2*turn {
			t.Fatalf("request %d: %d messages, want %d", i, len(b.Messages), 2+2*turn)
		}
		for j, m := range b.Messages[1:] {
			want := map[bool]string{true: "user", false: "assistant"}[j%2 == 0]
			if m.Role != want {
				t.Fatalf("request %d message %d role %q, want %q", i, j+1, m.Role, want)
			}
		}
		if turn > 0 {
			// The previous request is a strict prefix: history is resent unchanged.
			if !slices.Equal(prev, b.Messages[:len(prev)]) {
				t.Fatalf("request %d does not extend the previous one", i)
			}
			if r := b.Messages[len(prev)]; r.Role != "assistant" || words(r.Content) != 5 {
				t.Fatalf("request %d: reply %+v, want 5-word assistant message", i, r)
			}
		} else if i > 0 && slices.Equal(prev[1:2], b.Messages[1:2]) {
			t.Fatalf("request %d: new conversation repeats the old first message", i)
		}
		if b.Messages[0].Role != "system" {
			t.Fatalf("request %d lost the system prompt", i)
		}
		prev = b.Messages
	}
}

func TestMaxContextWordsEndsConversation(t *testing.T) {
	const limit = 60
	p := single(Class{
		SystemWords: Fixed(10), Turns: Fixed(50), ReplyWords: Fixed(8), OpenConversations: 2,
		PromptWords: Dist{Kind: DistUniform, Min: 1, Max: 10}, MaxContextWords: limit,
	})
	g := mustGen(t, p, Options{}, 1)
	longest := 0
	for range 2000 {
		total := 0
		for _, m := range decode(t, g.Next()).Messages {
			total += words(m.Content)
		}
		if total > limit {
			t.Fatalf("request of %d words exceeds max_context_words %d", total, limit)
		}
		longest = max(longest, total)
	}
	if longest < limit-10 {
		t.Fatalf("longest request %d words; history never grew toward the limit", longest)
	}
}

func TestTriggeredClasses(t *testing.T) {
	p, err := Parse([]byte(validProfile))
	if err != nil {
		t.Fatal(err)
	}
	g := mustGen(t, p, Options{}, 2)
	const n = 30000
	counts := map[string]float64{}
	var seq []string
	for i := range n {
		r := g.Next()
		counts[r.Class]++
		if i < 200 {
			seq = append(seq, r.Class)
		}
	}
	// Every chat request is followed by exactly two cls requests.
	for i, c := range seq[:len(seq)-2] {
		if c == "chat" && (seq[i+1] != "cls" || seq[i+2] != "cls") {
			t.Fatalf("chat at %d not followed by two cls: %v", i, seq[i:i+3])
		}
	}
	if d := 2*counts["chat"] - counts["cls"]; d < 0 || d > 2 { // the last chat's follow-ups may be cut off
		t.Fatalf("cls = %.0f, chat = %.0f; want two per chat", counts["cls"], counts["chat"])
	}
	for name, want := range p.ExpectedShares() {
		if got := counts[name] / n; math.Abs(got-want) > 0.01 {
			t.Errorf("%s share = %.3f, expected %.3f", name, got, want)
		}
	}
}

func TestBurstClassesGetExtraArrivals(t *testing.T) {
	p, err := Parse([]byte(validProfile))
	if err != nil {
		t.Fatal(err)
	}
	// Remove the trigger so shares are plain weights: chat 1, bg 1.
	p.Classes = slices.DeleteFunc(slices.Clone(p.Classes), func(c Class) bool { return c.Name == "cls" })
	b := p.Arrival.Burst
	g := mustGen(t, p, Options{Burst: runner.Burst{Every: b.Every, Length: b.Length, Factor: b.Factor}}, 4)
	share := func(at time.Duration) float64 {
		bg := 0
		for range 20000 {
			if g.NextAt(at).Class == "bg" {
				bg++
			}
		}
		return float64(bg) / 20000
	}
	// Outside a window: 1:1. Inside (factor 3): chat keeps R/2, bg gets
	// R/2 + 2R, so bg is 2.5/3 of arrivals.
	for _, tc := range []struct {
		at   time.Duration
		want float64
	}{
		{0, 0.5},                    // before the first window
		{5 * time.Second, 0.5},      // between windows
		{11 * time.Second, 2.5 / 3}, // inside the window at 10s
		{21999 * time.Millisecond, 2.5 / 3},
		{22 * time.Second, 0.5}, // window over
	} {
		if got := share(tc.at); math.Abs(got-tc.want) > 0.015 {
			t.Errorf("bg share at %v = %.3f, want %.3f", tc.at, got, tc.want)
		}
	}
	// With no burst in the run options, the window mix is never used.
	g = mustGen(t, p, Options{}, 4)
	if got := share(11 * time.Second); math.Abs(got-0.5) > 0.015 {
		t.Errorf("bg share without bursts = %.3f, want 0.5", got)
	}
}

// TestBurstKeepsOtherClassRates drives mixed.yaml, which has both a trigger
// (classify after chat) and a burst class (background), with the runner's
// real arrival process: inside windows chat, structured and classify keep
// their per-second rates and background gains (factor-1)*rate.
func TestBurstKeepsOtherClassRates(t *testing.T) {
	p, err := Load("../../profiles/mixed.yaml")
	if err != nil {
		t.Fatal(err)
	}
	// Lengths do not affect which class is drawn; shrink them to keep the
	// run fast.
	for i := range p.Classes {
		c := &p.Classes[i]
		c.SystemWords, c.PromptWords, c.MaxTokens = Fixed(1), Fixed(1), Fixed(1)
		if c.ReplyWords.Upper() > 0 {
			c.ReplyWords = Fixed(1)
		}
	}
	pb := p.Arrival.Burst
	burst := runner.Burst{Every: pb.Every, Length: pb.Length, Factor: pb.Factor}
	g := mustGen(t, p, Options{Burst: burst}, 7)
	arr, err := runner.NewArrivals(p.Arrival.Rate, burst, rand.New(rand.NewPCG(7, 7)))
	if err != nil {
		t.Fatal(err)
	}
	const windows = 30
	end := windows * burst.Every
	in, out := map[string]float64{}, map[string]float64{}
	for at := arr.Next(); at < end; at = arr.Next() {
		if c := g.NextAt(at).Class; burst.Windows().Contains(at) {
			in[c]++
		} else {
			out[c]++
		}
	}
	inSecs := float64(windows-1) * burst.Length.Seconds() // windows start at k*every, k >= 1
	outSecs := end.Seconds() - inSecs
	extra := (burst.Factor - 1) * p.Arrival.Rate
	shares := p.ExpectedShares()
	for _, name := range []string{"chat", "structured", "classify", "background"} {
		inRate, outRate := in[name]/inSecs, out[name]/outSecs
		t.Logf("%s: outside %.2f/s, inside %.2f/s", name, outRate, inRate)
		want := shares[name] * p.Arrival.Rate
		if math.Abs(outRate-want) > 0.06*want {
			t.Errorf("%s outside windows: %.2f/s, expected %.2f/s", name, outRate, want)
		}
		if name == "background" {
			want += extra
		}
		if math.Abs(inRate-want) > 0.08*want {
			t.Errorf("%s inside windows: %.2f/s, want %.2f/s (outside: %.2f/s)", name, inRate, want, outRate)
		}
	}
}

func TestBurstWithoutClassesKeepsMix(t *testing.T) {
	p := single(Class{})
	p.Classes = append(p.Classes, Class{Name: "d", Weight: 3, PromptWords: Fixed(1), MaxTokens: Fixed(1)})
	g := mustGen(t, p, Options{Burst: runner.Burst{Every: time.Second, Length: 500 * time.Millisecond, Factor: 4}}, 1)
	d := 0
	for range 20000 {
		if g.NextAt(1200*time.Millisecond).Class == "d" {
			d++
		}
	}
	if got := float64(d) / 20000; math.Abs(got-0.75) > 0.015 {
		t.Fatalf("d share in burst = %.3f, want 0.75 (unchanged mix)", got)
	}
}

func TestGeneratorStateBounded(t *testing.T) {
	const slots = 8
	p := single(Class{
		SystemWords: Fixed(2), Turns: Fixed(maxTurns), ReplyWords: Fixed(2),
		OpenConversations: slots, PromptWords: Fixed(2),
	})
	p.Classes = append(p.Classes, Class{
		Name: "t", Weight: 0, PromptWords: Fixed(1), MaxTokens: Fixed(1),
		TriggeredBy: &Trigger{Class: "c", Count: Fixed(maxTriggered)},
	})
	g := mustGen(t, p, Options{}, 1)
	for range 50000 {
		g.Next()
		open := 0
		for _, conv := range g.convs[0] {
			if conv == nil {
				continue
			}
			open++
			if len(conv.history) > 2*maxTurns {
				t.Fatalf("history of %d messages exceeds %d turns", len(conv.history), maxTurns)
			}
		}
		if open > slots || len(g.convs[0]) != slots {
			t.Fatalf("%d open conversations in %d slots, ceiling %d", open, len(g.convs[0]), slots)
		}
		if len(g.pending) > maxTriggered {
			t.Fatalf("%d pending triggered requests, want <= %d", len(g.pending), maxTriggered)
		}
	}
}

func TestNewGeneratorRejects(t *testing.T) {
	if _, err := NewGenerator(nil, Options{}, 1); err == nil {
		t.Error("nil profile accepted")
	}
	if _, err := NewGenerator(&Profile{Name: "x"}, Options{}, 1); err == nil {
		t.Error("invalid profile accepted")
	}
	if _, err := NewGenerator(single(Class{}), Options{Burst: runner.Burst{Every: time.Second}}, 1); err == nil {
		t.Error("invalid burst accepted")
	}
}

func BenchmarkGeneratorMixed(b *testing.B) {
	p, err := Load("../../profiles/mixed.yaml")
	if err != nil {
		b.Fatal(err)
	}
	g, err := NewGenerator(p, Options{}, 1)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		g.NextAt(time.Duration(i) * time.Millisecond)
	}
}
