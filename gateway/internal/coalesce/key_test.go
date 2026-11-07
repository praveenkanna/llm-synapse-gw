package coalesce

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// fingerprintOf indexes body, as ingress does once per request, and
// digests it.
func fingerprintOf(body []byte, maxBytes int) (Fingerprint, string) {
	var ix bodyindex.Index
	_ = ix.Read(body)
	return FingerprintOf(&ix, maxBytes)
}

func fp(t *testing.T, body string) Fingerprint {
	t.Helper()
	f, reason := fingerprintOf([]byte(body), 1<<20)
	if reason != "" {
		t.Fatalf("FingerprintOf(%q): reason %q", body, reason)
	}
	return f
}

func keyOf(t *testing.T, body string, p Parts, extra ...string) string {
	t.Helper()
	return fp(t, body).Key(p, extra...)
}

var base = Parts{Endpoint: "/v1/chat/completions", Template: "summary", Version: "3", Pool: "large", Weights: "w1", Encoding: ""}

func TestKeyIgnoresOnlyPriorityUserWhitespaceAndOrder(t *testing.T) {
	ref := keyOf(t, `{"model":"m","temperature":0,"messages":[{"role":"user","content":"hi"}]}`, base)
	same := []string{
		`{ "messages" : [ { "role":"user", "content":"hi" } ],  "temperature":0, "model":"m" }`,
		`{"model":"m","temperature":0,"messages":[{"role":"user","content":"hi"}],"user":"alice"}`,
		`{"model":"m","priority":-5,"temperature":0,"messages":[{"role":"user","content":"hi"}]}`,
		`{"model":"m","temperature":0.9,"temperature":0,"messages":[{"role":"user","content":"hi"}]}`, // last wins, as in vLLM
		"{\n\"model\": \"m\",\n\"temperature\": 0,\n\"messages\": [\n{\"role\": \"user\", \"content\": \"hi\"}]\n}\n",
	}
	for _, b := range same {
		if got := keyOf(t, b, base); got != ref {
			t.Errorf("key differs for %s", b)
		}
	}
	differ := []string{
		`{"model":"m","temperature":0,"messages":[{"role":"user","content":"hi!"}]}`,
		`{"model":"m2","temperature":0,"messages":[{"role":"user","content":"hi"}]}`,
		`{"model":"m","temperature":0,"seed":1,"messages":[{"role":"user","content":"hi"}]}`,
		`{"model":"m","temperature":0,"top_p":0.9,"messages":[{"role":"user","content":"hi"}]}`,
		`{"model":"m","temperature":0,"max_tokens":8,"messages":[{"role":"user","content":"hi"}]}`,
		`{"model":"m","temperature":0,"stream":true,"messages":[{"role":"user","content":"hi"}]}`,
		`{"model":"m","temperature":0,"request_id":"r1","messages":[{"role":"user","content":"hi"}]}`,
		`{"model":"m","temperature":0.0,"messages":[{"role":"user","content":"hi"}]}`, // different bytes: no merge, never a wrong one
		`{"model":"m","temperature":0,"messages":[{"content":"hi","role":"user"}]}`,   // nested order is significant
		`{"model":"m","temperature":0,"messages":[{"role":"user","content":"h\u0069"}]}`,
		// priority and user are left out only as one member of a type vLLM
		// accepts; anything else is forwarded for the replica to refuse.
		`{"model":"m","priority":"high","temperature":0,"messages":[{"role":"user","content":"hi"}]}`,
		`{"model":"m","priority":null,"temperature":0,"messages":[{"role":"user","content":"hi"}]}`,
		`{"model":"m","priority":1,"priority":2,"temperature":0,"messages":[{"role":"user","content":"hi"}]}`,
		`{"model":"m","temperature":0,"messages":[{"role":"user","content":"hi"}],"user":5}`,
		`{"model":"m","temperature":0,"messages":[{"role":"user","content":"hi"}],"user":"a","user":"b"}`,
	}
	for _, b := range differ {
		if got := keyOf(t, b, base); got == ref {
			t.Errorf("key equals the reference for %s", b)
		}
	}
}

func with(change func(*Parts)) Parts {
	p := base
	change(&p)
	return p
}

func TestKeyCoversEveryPart(t *testing.T) {
	body := `{"model":"m","temperature":0,"messages":[]}`
	ref := keyOf(t, body, base)
	for name, p := range map[string]Parts{
		"endpoint": with(func(p *Parts) { p.Endpoint = "/v1/completions" }),
		"template": with(func(p *Parts) { p.Template = "other" }),
		"version":  with(func(p *Parts) { p.Version = "4" }),
		"pool":     with(func(p *Parts) { p.Pool = "small" }),
		"weights":  with(func(p *Parts) { p.Weights = "w2" }),
		"encoding": with(func(p *Parts) { p.Encoding = "gzip" }),
	} {
		if keyOf(t, body, p) == ref {
			t.Errorf("changing %s keeps the key", name)
		}
	}
	if keyOf(t, body, base, "interactive") == ref {
		t.Error("an extra value keeps the key")
	}
	if keyOf(t, body, base, "interactive") == keyOf(t, body, base, "background") {
		t.Error("different extra values share a key")
	}
	// Fields are length-prefixed: moving a byte between neighbours must not collide.
	a := keyOf(t, body, Parts{Template: "ab", Version: "c"})
	b := keyOf(t, body, Parts{Template: "a", Version: "bc"})
	if a == b {
		t.Error("parts are not delimited")
	}
	if keyOf(t, body, base, "ab", "c") == keyOf(t, body, base, "a", "bc") {
		t.Error("extras are not delimited")
	}
}

func TestDeterministic(t *testing.T) {
	cases := []struct {
		members string
		want    bool
	}{
		{`"temperature":0`, true},
		{`"temperature":0.0`, true},
		{`"temperature":-0`, true},
		{`"temperature":0e3`, true},
		{`"seed":7`, true},
		{`"seed":-3`, true},
		{`"temperature":0.7,"seed":7`, true},
		{`"temperature":0,"seed":1.5`, true},
		{`"temperature":0.1`, false},
		{`"temperature":1`, false},
		{`"temperature":"0"`, false},
		{`"temperature":null`, false},
		{`"seed":1.5`, false},
		{`"seed":1e2`, false},
		{`"seed":"5"`, false},
		{`"seed":null`, false},
		{`"seed":12345678901234567890`, false}, // over 19 digits
		{`"top_p":0.5`, false},
		{``, false}, // vLLM's default temperature is 1.0
	}
	for _, tc := range cases {
		body := `{"model":"m"`
		if tc.members != "" {
			body += "," + tc.members
		}
		body += "}"
		if got := fp(t, body).Deterministic; got != tc.want {
			t.Errorf("%s: Deterministic = %v, want %v", tc.members, got, tc.want)
		}
	}
}

func TestStreamFlag(t *testing.T) {
	if !fp(t, `{"stream":true}`).Stream || fp(t, `{"stream":false}`).Stream || fp(t, `{}`).Stream || fp(t, `{"stream":"true"}`).Stream {
		t.Fatal("Stream must be true only for the JSON literal true")
	}
}

func TestNoKeyForUnusualBodies(t *testing.T) {
	cases := map[string]struct {
		body   string
		reason string
	}{
		"not json":            {`nope`, telemetry.ReasonUnparseableBody},
		"array":               {`[1]`, telemetry.ReasonUnparseableBody},
		"empty":               {``, telemetry.ReasonUnparseableBody},
		"trailing":            {`{"a":1}{"b":2}`, telemetry.ReasonUnparseableBody},
		"invalid utf8":        {"{\"a\":\"\xff\"}", telemetry.ReasonUnparseableBody},
		"escaped key":         {`{"m\u006fdel":"x"}`, telemetry.ReasonUnsupportedKey},
		"escaped dup":         {`{"model":"x","m\u006fdel":"y"}`, telemetry.ReasonUnsupportedKey},
		"escaped slash":       {`{"a\/b":1}`, telemetry.ReasonUnsupportedKey},
		"too large":           {`{"a":"` + strings.Repeat("x", 100) + `"}`, telemetry.ReasonBodyTooLarge},
		"too large, not json": {`{"a":"` + strings.Repeat("x", 100), telemetry.ReasonBodyTooLarge}, // the size is checked first
		"lone surrogate":      {`{"a":"\ud800"}`, ""},                                              // a value: hashed as bytes, never decoded
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, reason := fingerprintOf([]byte(tc.body), 64)
			if reason != tc.reason {
				t.Fatalf("reason %q, want %q", reason, tc.reason)
			}
		})
	}
	var never bodyindex.Index // no body read: nothing to key
	if _, reason := FingerprintOf(&never, 64); reason != telemetry.ReasonUnparseableBody {
		t.Fatalf("an Index never read: reason %q", reason)
	}
}

// TestRepeatedNamesKeepTheirLastValue: however wide the body, a repeated
// name is its last member, the one vLLM reads: the key is the key of the
// body with only those members. The names are sorted to be hashed, so only
// a stable sort keeps that; a body this wide is past insertion sort, where
// an unstable one would still look stable.
func TestRepeatedNamesKeepTheirLastValue(t *testing.T) {
	const names, rounds = 9, 8
	var all, last strings.Builder
	all.WriteString(`{"temperature":0`)
	last.WriteString(`{"temperature":0`)
	for r := range rounds {
		for n := range names {
			member := fmt.Sprintf(`,"n%d":%d`, n, r)
			all.WriteString(member)
			if r == rounds-1 {
				last.WriteString(member)
			}
		}
	}
	if keyOf(t, all.String()+"}", base) != keyOf(t, last.String()+"}", base) {
		t.Fatal("a repeated name is not read as its last member")
	}
}

// TestRepeatingPriorityOrUserChangesTheKey: a single priority or user is
// left out of the key, but repeating one changes what the replica is sent,
// so the repetition is part of the key: neither a body with one of them
// nor one repeating it a different number of times shares its key.
func TestRepeatingPriorityOrUserChangesTheKey(t *testing.T) {
	for _, name := range []string{"priority", "user"} {
		member := func(v string) string { return fmt.Sprintf(`,%q:%s`, name, v) }
		body := func(values ...string) string {
			b := `{"temperature":0`
			for _, v := range values {
				b += member(v)
			}
			return b + "}"
		}
		once := keyOf(t, body("5"), base)
		twice := keyOf(t, body(`"a"`, "5"), base)
		thrice := keyOf(t, body(`"a"`, `"b"`, "5"), base)
		if once == twice || twice == thrice || once == thrice {
			t.Fatalf("%s: once %.8s, twice %.8s, thrice %.8s; each count must key apart", name, once, twice, thrice)
		}
		if got := keyOf(t, body(`"z"`, "5"), base); got != twice {
			t.Fatalf("%s: the same repetition with an earlier value changed keys twice: %.8s, %.8s", name, twice, got)
		}
	}
}

// TestNoKeyPastTheMemberCeiling (E5): a body of more top-level members
// than the index holds is valid JSON the replica serves, but it has no key
// (too_many_members), after the size and UTF-8 rules, which come first.
func TestNoKeyPastTheMemberCeiling(t *testing.T) {
	var b strings.Builder
	b.WriteString(`{"model":"m","temperature":0`)
	for i := range bodyindex.MaxMembers {
		b.WriteString(`,"x` + strconv.Itoa(i) + `":0`)
	}
	past := b.String() + "}"
	for _, tc := range []struct {
		name, body string
		max        int
		reason     string
	}{
		{"past the ceiling", past, 1 << 20, telemetry.ReasonTooManyMembers},
		{"and too large", past, 64, telemetry.ReasonBodyTooLarge},
		{"and invalid utf8", past[:len(past)-1] + ",\"y\":\"\xff\"}", 1 << 20, telemetry.ReasonUnparseableBody},
	} {
		if _, reason := fingerprintOf([]byte(tc.body), tc.max); reason != tc.reason {
			t.Errorf("%s: reason %q, want %q", tc.name, reason, tc.reason)
		}
	}
}

// Distinct escapes of what a decoder would read as one string must not
// share a key: a lone surrogate escape and the replacement character both
// decode to U+FFFD in Go, but Python's decoder keeps them apart.
func TestValuesAreNotDecoded(t *testing.T) {
	a := keyOf(t, `{"messages":"\ud800"}`, base)
	b := keyOf(t, `{"messages":"\ufffd"}`, base)
	c := keyOf(t, "{\"messages\":\"\xef\xbf\xbd\"}", base)
	if a == b || a == c || b == c {
		t.Fatal("distinct string spellings share a key")
	}
}

// requestSpec is one point of a space of requests that differ in exactly the
// ways the key must tell apart. Its String is the identity: two specs have
// the same String exactly when a replica must be allowed to give them one
// answer.
type requestSpec struct {
	model, temp, seed, topP, maxTok, stream, n, stop, msgs, tools string
	presence, frequency, logitBias, topK, minP                    string
	// prio and user, when set, are members of a kind the replica refuses,
	// so they are part of the identity; when empty, render may add
	// well-formed ones the key ignores.
	prio, user string
	parts      Parts
	class      string
}

func (s requestSpec) String() string {
	return fmt.Sprintf("%q|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%+v|%s", s.model, s.temp, s.seed, s.topP, s.maxTok, s.stream, s.n, s.stop, s.msgs, s.tools,
		s.presence, s.frequency, s.logitBias, s.topK, s.minP, s.prio, s.user, s.parts, s.class)
}

var (
	models  = []string{"m-a", "m-b", "m-a "}
	temps   = []string{"", "0", "0.0", "0.7", "1", "null", `"0"`}
	seeds   = []string{"", "1", "2", "-1", "null", "1.5"}
	topPs   = []string{"", "0.9", "1", "1.0"}
	maxToks = []string{"", "16", "17", "null"}
	streams = []string{"", "true", "false"}
	ns      = []string{"", "1", "2"}
	stops   = []string{"", `"a"`, `["a"]`, `["a","b"]`, `["b","a"]`}
	msgs    = []string{
		`[{"role":"user","content":"hi"}]`,
		`[{"role":"user","content":"hi "}]`,
		`[{"role":"system","content":"hi"}]`,
		`[{"role":"user","content":"hi"},{"role":"assistant","content":"yo"}]`,
		`[{"role":"user","content":"\u0068i"}]`,
		`[]`,
	}
	toolsV    = []string{"", `[{"type":"function","function":{"name":"f"}}]`, `[{"type":"function","function":{"name":"g"}}]`}
	penalties = []string{"", "0", "0.5", "-0.5"}
	biases    = []string{"", `{}`, `{"42":-100}`, `{"42":100}`, `{"7":-100}`}
	topKs     = []string{"", "-1", "1", "40"}
	minPs     = []string{"", "0", "0.05"}
	templates = []string{"summary", "other"}
	versions  = []string{"1", "2"}
	pools     = []string{"large", "small"}
	weights   = []string{"", "w1", "w2"}
	classes   = []string{"interactive", "background"}
	endpoints = []string{"/v1/chat/completions", "/v1/completions"}
	encodings = []string{"", "gzip"}
	badPrios  = []string{"", "", "", `"high"`, "null", "[1]", "dup"}
	badUsers  = []string{"", "", "", "5", "true", "dup"}
)

func pick[T any](r *rand.Rand, xs []T) T { return xs[r.IntN(len(xs))] }

func randomSpec(r *rand.Rand) requestSpec {
	return requestSpec{
		model: pick(r, models), temp: pick(r, temps), seed: pick(r, seeds), topP: pick(r, topPs),
		maxTok: pick(r, maxToks), stream: pick(r, streams), n: pick(r, ns), stop: pick(r, stops),
		msgs: pick(r, msgs), tools: pick(r, toolsV),
		presence: pick(r, penalties), frequency: pick(r, penalties), logitBias: pick(r, biases),
		topK: pick(r, topKs), minP: pick(r, minPs),
		prio: pick(r, badPrios), user: pick(r, badUsers),
		parts: Parts{Endpoint: pick(r, endpoints), Template: pick(r, templates), Version: pick(r, versions),
			Pool: pick(r, pools), Weights: pick(r, weights), Encoding: pick(r, encodings)},
		class: pick(r, classes),
	}
}

// render writes the spec as a request body in a random layout: member
// order, whitespace, decoys the real value overrides, and members the key
// must ignore.
func (s requestSpec) render(r *rand.Rand) string {
	type member struct{ name, val string }
	ms := []member{{"model", strconvQuote(s.model)}, {"messages", s.msgs}}
	add := func(name, val string) {
		if val != "" {
			ms = append(ms, member{name, val})
		}
	}
	add("temperature", s.temp)
	add("seed", s.seed)
	add("top_p", s.topP)
	add("max_tokens", s.maxTok)
	add("stream", s.stream)
	add("n", s.n)
	add("stop", s.stop)
	add("tools", s.tools)
	add("presence_penalty", s.presence)
	add("frequency_penalty", s.frequency)
	add("logit_bias", s.logitBias)
	add("top_k", s.topK)
	add("min_p", s.minP)
	switch {
	case s.user == "dup":
		ms = append(ms, member{"user", `"a"`}, member{"user", `"a"`}) // same value: the shuffle cannot change which wins
	case s.user != "":
		ms = append(ms, member{"user", s.user})
	case r.IntN(2) == 0:
		ms = append(ms, member{"user", strconvQuote(pick(r, []string{"alice", "bob", "c"}))})
	}
	switch {
	case s.prio == "dup":
		ms = append(ms, member{"priority", "1"}, member{"priority", "1"})
	case s.prio != "":
		ms = append(ms, member{"priority", s.prio})
	case r.IntN(2) == 0:
		ms = append(ms, member{"priority", pick(r, []string{"-1", "0", "7"})})
	}
	r.Shuffle(len(ms), func(i, j int) { ms[i], ms[j] = ms[j], ms[i] })
	var b strings.Builder
	sp := func() string { return pick(r, []string{"", " ", "\n", "\t "}) }
	b.WriteString("{" + sp())
	first := true
	write := func(m member) {
		if !first {
			b.WriteString(sp() + "," + sp())
		}
		first = false
		val := m.val
		if r.IntN(2) == 0 && json.Valid([]byte(val)) { // same tokens, other whitespace
			var buf bytes.Buffer
			if json.Indent(&buf, []byte(val), "", pick(r, []string{" ", "\t", "  "})) == nil {
				val = buf.String()
			}
		}
		b.WriteString(strconvQuote(m.name) + sp() + ":" + sp() + val)
	}
	for _, m := range ms {
		if r.IntN(4) == 0 && m.name != "user" && m.name != "priority" { // a decoy the real value overrides
			write(member{m.name, decoyFor(m.name)})
		}
		write(m)
	}
	b.WriteString(sp() + "}")
	return b.String()
}

func decoyFor(name string) string {
	switch name {
	case "model", "stop", "messages", "tools", "logit_bias":
		return `"decoy"`
	}
	return "99"
}

func strconvQuote(s string) string { b, _ := json.Marshal(s); return string(b) }

// oneFieldChanges each set one field of a spec from its own value space.
var oneFieldChanges = []struct {
	name string
	set  func(*requestSpec, *rand.Rand)
}{
	{"model", func(s *requestSpec, r *rand.Rand) { s.model = pick(r, models) }},
	{"temperature", func(s *requestSpec, r *rand.Rand) { s.temp = pick(r, temps) }},
	{"seed", func(s *requestSpec, r *rand.Rand) { s.seed = pick(r, seeds) }},
	{"top_p", func(s *requestSpec, r *rand.Rand) { s.topP = pick(r, topPs) }},
	{"max_tokens", func(s *requestSpec, r *rand.Rand) { s.maxTok = pick(r, maxToks) }},
	{"stream", func(s *requestSpec, r *rand.Rand) { s.stream = pick(r, streams) }},
	{"n", func(s *requestSpec, r *rand.Rand) { s.n = pick(r, ns) }},
	{"stop", func(s *requestSpec, r *rand.Rand) { s.stop = pick(r, stops) }},
	{"messages", func(s *requestSpec, r *rand.Rand) { s.msgs = pick(r, msgs) }},
	{"tools", func(s *requestSpec, r *rand.Rand) { s.tools = pick(r, toolsV) }},
	{"presence_penalty", func(s *requestSpec, r *rand.Rand) { s.presence = pick(r, penalties) }},
	{"frequency_penalty", func(s *requestSpec, r *rand.Rand) { s.frequency = pick(r, penalties) }},
	{"logit_bias", func(s *requestSpec, r *rand.Rand) { s.logitBias = pick(r, biases) }},
	{"top_k", func(s *requestSpec, r *rand.Rand) { s.topK = pick(r, topKs) }},
	{"min_p", func(s *requestSpec, r *rand.Rand) { s.minP = pick(r, minPs) }},
	{"malformed priority", func(s *requestSpec, r *rand.Rand) { s.prio = pick(r, badPrios) }},
	{"malformed user", func(s *requestSpec, r *rand.Rand) { s.user = pick(r, badUsers) }},
	{"endpoint", func(s *requestSpec, r *rand.Rand) { s.parts.Endpoint = pick(r, endpoints) }},
	{"template", func(s *requestSpec, r *rand.Rand) { s.parts.Template = pick(r, templates) }},
	{"version", func(s *requestSpec, r *rand.Rand) { s.parts.Version = pick(r, versions) }},
	{"pool", func(s *requestSpec, r *rand.Rand) { s.parts.Pool = pick(r, pools) }},
	{"weights", func(s *requestSpec, r *rand.Rand) { s.parts.Weights = pick(r, weights) }},
	{"encoding", func(s *requestSpec, r *rand.Rand) { s.parts.Encoding = pick(r, encodings) }},
	{"priority class", func(s *requestSpec, r *rand.Rand) { s.class = pick(r, classes) }},
}

func specKey(t *testing.T, s requestSpec, r *rand.Rand) string {
	t.Helper()
	body := s.render(r)
	f, reason := fingerprintOf([]byte(body), 1<<20)
	if reason != "" {
		t.Fatalf("spec %v has no key (%s): %s", s, reason, body)
	}
	return f.Key(s.parts, s.class)
}

// TestKeySeparatesOneFieldChanges is AC-M4-3's key half, over 10,000
// requests. Each is paired with a copy that differs in exactly one field
// (cycling through every member and key part a replica or the gateway acts
// on), which must get another key, and with itself in another random
// layout, which must get the same key. Across all of them, a key is shared
// only by requests with one identity.
func TestKeySeparatesOneFieldChanges(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	byKey := make(map[string]string) // key -> spec identity
	merges := 0
	perField := make(map[string]int)
	seen := func(key string, s requestSpec) {
		if prev, ok := byKey[key]; ok && prev != s.String() {
			t.Fatalf("two different requests share a key:\n  %s\n  %s", prev, s)
		}
		byKey[key] = s.String()
	}
	for i := range 10_000 {
		change := oneFieldChanges[i%len(oneFieldChanges)]
		s := randomSpec(r)
		d := s
		for d.String() == s.String() {
			change.set(&d, r)
		}
		key, again, other := specKey(t, s, r), specKey(t, s, r), specKey(t, d, r)
		if again != key {
			t.Fatalf("one request has two keys (layout leaked into the key): %s", s)
		}
		if other == key {
			merges++
			t.Errorf("requests differing only in %s share a key:\n  %s\n  %s", change.name, s, d)
		}
		seen(key, s)
		seen(other, d)
		perField[change.name]++
	}
	if merges != 0 {
		t.Fatalf("%d merges of differing requests, want 0", merges)
	}
	for _, c := range oneFieldChanges {
		if perField[c.name] < 10_000/len(oneFieldChanges) {
			t.Errorf("%s changed in only %d pairs", c.name, perField[c.name])
		}
	}
}

func BenchmarkFingerprint4900Chars(b *testing.B) {
	body := []byte(`{"model":"m","temperature":0,"messages":[{"role":"system","content":"` + strings.Repeat("x", 4900) + `"},{"role":"user","content":"q"}]}`)
	b.ReportAllocs()
	for b.Loop() {
		if _, reason := fingerprintOf(body, 1<<20); reason != "" {
			b.Fatal(reason)
		}
	}
}
