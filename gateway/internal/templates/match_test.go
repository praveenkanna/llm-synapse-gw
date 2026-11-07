package templates

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// matchSpec has a fully static system message, a developer message that
// opens static, and a volatile user turn.
func matchSpec() Spec {
	return Spec{
		Model:     "large-model",
		Variables: []Variable{{Name: "tenant"}, {Name: "q"}},
		Messages: []Message{
			msg("system", seg(KindStatic, "You are helpful.\n\"Quote\" é 😀 </tag> \\ / \t"), seg(KindStatic, " Be brief.")),
			msg("developer", seg(KindStatic, "Rules: <b>cite</b>. "), seg(KindSemiStatic, "Tenant {{tenant}}")),
			msg("user", seg(KindVolatile, "{{q}}")),
		},
	}
}

func record(t testing.TB, id string, version int, st Status, s Spec) Record {
	t.Helper()
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	return Record{ID: id, Version: version, Status: st, Spec: b}
}

func mustSnapshot(t testing.TB, recs ...Record) *Snapshot {
	t.Helper()
	s, errs := NewSnapshot(recs, time.Unix(100, 0))
	if len(errs) > 0 {
		t.Fatalf("NewSnapshot: %v", errs)
	}
	return s
}

// chatBody is a chat completions body with msgs as given ([role, content]
// pairs; content is a Go string or a raw JSON value).
func chatBody(msgs ...[2]any) []byte {
	var b strings.Builder
	b.WriteString(`{"model":"large-model","messages":[`)
	for i, m := range msgs {
		if i > 0 {
			b.WriteByte(',')
		}
		role, _ := json.Marshal(m[0])
		var content []byte
		switch c := m[1].(type) {
		case json.RawMessage:
			content = c
		default:
			content, _ = json.Marshal(c)
		}
		fmt.Fprintf(&b, `{"role":%s,"content":%s}`, role, content)
	}
	b.WriteString(`],"stream":true,"temperature":0.2}`)
	return []byte(b.String())
}

// asciiJSON encodes s as Python's json.dumps does by default: non-ASCII as
// \u escapes (surrogate pairs above the BMP) and "/" escaped as well.
func asciiJSON(s string) json.RawMessage {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch {
		case r == '"' || r == '\\' || r == '/':
			b.WriteByte('\\')
			b.WriteRune(r)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\t':
			b.WriteString(`\t`)
		case r < 0x20:
			fmt.Fprintf(&b, `\u%04x`, r)
		case r < 0x80:
			b.WriteRune(r)
		case r > 0xFFFF:
			r -= 0x10000
			fmt.Fprintf(&b, `\u%04X\u%04X`, 0xD800+(r>>10), 0xDC00+(r&0x3FF))
		default:
			fmt.Fprintf(&b, `\u%04x`, r)
		}
	}
	b.WriteByte('"')
	return json.RawMessage(b.String())
}

func TestMatch(t *testing.T) {
	snap := mustSnapshot(t,
		record(t, "support", 1, StatusActive, matchSpec()),
		record(t, "support", 2, StatusDeprecated, matchSpec()),
	)
	rendered, err := Render(matchSpec(), map[string]string{"tenant": "acme", "q": "Why?"})
	if err != nil {
		t.Fatal(err)
	}
	sys, dev, user := rendered[0].Content, rendered[1].Content, rendered[2].Content
	good := chatBody([2]any{"system", sys}, [2]any{"developer", dev}, [2]any{"user", user})

	cases := []struct {
		name, id, version string
		body              []byte
		want              string
	}{
		{"rendered", "support", "1", good, ""},
		{"deprecated version", "support", "2", good, ""},
		{"version with spaces", "support", " 1\t", good, ""},
		{"extra trailing messages", "support", "1", chatBody([2]any{"system", sys}, [2]any{"developer", dev}, [2]any{"user", user},
			[2]any{"assistant", "ok"}, [2]any{"user", json.RawMessage(`[{"type":"text","text":"x"}]`)}), ""},
		{"python-style escapes", "support", "1", chatBody([2]any{"system", asciiJSON(sys)}, [2]any{"developer", asciiJSON(dev)}, [2]any{"user", user}), ""},
		{"developer content is only the static text", "support", "1", chatBody([2]any{"system", sys}, [2]any{"developer", "Rules: <b>cite</b>. "}), ""},
		{"user turn content not inspected", "support", "1", chatBody([2]any{"system", sys}, [2]any{"developer", dev}, [2]any{"user", json.RawMessage(`null`)}), ""},

		{"unknown template", "billing", "1", good, ReasonUnknownTemplate},
		{"no version", "support", "", good, ReasonNoVersion},
		{"blank version", "support", "  ", good, ReasonNoVersion},
		{"zero version", "support", "0", good, ReasonBadVersion},
		{"negative version", "support", "-1", good, ReasonBadVersion},
		{"signed version", "support", "+1", good, ReasonBadVersion},
		{"decimal version", "support", "1.0", good, ReasonBadVersion},
		{"word version", "support", "v1", good, ReasonBadVersion},
		{"huge version", "support", "99999999999999999999", good, ReasonBadVersion},
		{"unserved version", "support", "3", good, ReasonUnknownVersion},

		{"not json", "support", "1", []byte(`{"messages":`), ReasonUnparseableMessages},
		{"no messages", "support", "1", []byte(`{"model":"m","prompt":"hi"}`), ReasonUnparseableMessages},
		{"null messages", "support", "1", []byte(`{"messages":null}`), ReasonUnparseableMessages},
		{"messages not an array", "support", "1", []byte(`{"messages":{"role":"system"}}`), ReasonUnparseableMessages},
		{"message not an object", "support", "1", []byte(`{"messages":["hi"]}`), ReasonUnparseableMessages},
		{"role not a string", "support", "1", []byte(`{"messages":[{"role":1,"content":"x"}]}`), ReasonUnparseableMessages},
		{"body an array", "support", "1", []byte(`[]`), ReasonUnparseableMessages},
		{"trailing garbage", "support", "1", append(append([]byte{}, good...), 'x'), ReasonUnparseableMessages},

		{"array content", "support", "1", chatBody([2]any{"system", json.RawMessage(`[{"type":"text","text":"x"}]`)}, [2]any{"developer", dev}), ReasonUnsupportedContent},
		{"null content", "support", "1", chatBody([2]any{"system", json.RawMessage(`null`)}, [2]any{"developer", dev}), ReasonUnsupportedContent},
		{"missing content", "support", "1", []byte(`{"messages":[{"role":"system"}]}`), ReasonUnsupportedContent},
		{"array content in the partial message", "support", "1", chatBody([2]any{"system", sys}, [2]any{"developer", json.RawMessage(`[]`)}), ReasonUnsupportedContent},

		{"empty messages", "support", "1", []byte(`{"messages":[]}`), ReasonStaticMismatch},
		{"only the system message", "support", "1", chatBody([2]any{"system", sys}), ReasonStaticMismatch},
		{"system role differs", "support", "1", chatBody([2]any{"developer", sys}, [2]any{"developer", dev}), ReasonStaticMismatch},
		{"system content longer", "support", "1", chatBody([2]any{"system", sys + " "}, [2]any{"developer", dev}), ReasonStaticMismatch},
		{"system content shorter", "support", "1", chatBody([2]any{"system", sys[:len(sys)-1]}, [2]any{"developer", dev}), ReasonStaticMismatch},
		{"system content one byte off", "support", "1", chatBody([2]any{"system", strings.Replace(sys, "helpful", "helpfuL", 1)}, [2]any{"developer", dev}), ReasonStaticMismatch},
		{"partial role differs", "support", "1", chatBody([2]any{"system", sys}, [2]any{"user", dev}), ReasonStaticMismatch},
		{"partial text differs", "support", "1", chatBody([2]any{"system", sys}, [2]any{"developer", "Rules: <b>cite</b>, Tenant acme"}), ReasonStaticMismatch},
		{"partial text cut short", "support", "1", chatBody([2]any{"system", sys}, [2]any{"developer", "Rules: <b>cite"}), ReasonStaticMismatch},
		{"messages swapped", "support", "1", chatBody([2]any{"developer", dev}, [2]any{"system", sys}), ReasonStaticMismatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, reason := snap.Match(tc.id, tc.version, tc.body)
			if reason != tc.want {
				t.Fatalf("Match reason = %q, want %q", reason, tc.want)
			}
			if (v != nil) != (tc.want == "") {
				t.Fatalf("Match version = %v with reason %q", v, reason)
			}
			if v != nil && (v.ID != tc.id || fmt.Sprint(v.Version) != strings.TrimSpace(tc.version)) {
				t.Fatalf("Match = %s v%d", v.ID, v.Version)
			}
		})
	}
}

func TestMatchNilSnapshot(t *testing.T) {
	var s *Snapshot
	if v, reason := s.Match("support", "1", []byte(`{}`)); v != nil || reason != ReasonRegistryUnavailable {
		t.Fatalf("Match = %v, %q", v, reason)
	}
	if _, ok := s.Lookup("support", 1); ok || s.Len() != 0 || s.Versions() != nil || !s.LoadedAt().IsZero() {
		t.Fatal("nil snapshot is not empty")
	}
}

func TestMatchEntirelyStaticAndVolatileFirst(t *testing.T) {
	static := Spec{Model: "m", Messages: []Message{msg("system", seg(KindStatic, "S")), msg("user", seg(KindStatic, "U"))}}
	volatile := Spec{Model: "m", Variables: []Variable{{Name: "q"}}, Messages: []Message{msg("user", seg(KindVolatile, "{{q}}"))}}
	snap := mustSnapshot(t, record(t, "static", 1, StatusActive, static), record(t, "volatile", 1, StatusActive, volatile))
	cases := []struct {
		id   string
		body []byte
		want string
	}{
		{"static", chatBody([2]any{"system", "S"}, [2]any{"user", "U"}), ""},
		{"static", chatBody([2]any{"system", "S"}, [2]any{"user", "U"}, [2]any{"assistant", "A"}), ""},
		{"static", chatBody([2]any{"system", "S"}, [2]any{"user", "UU"}), ReasonStaticMismatch},
		{"static", chatBody([2]any{"system", "S"}), ReasonStaticMismatch},
		{"volatile", chatBody([2]any{"user", "anything"}), ""},
		{"volatile", chatBody([2]any{"system", "anything"}), ReasonStaticMismatch},
		{"volatile", chatBody([2]any{"user", json.RawMessage(`[]`)}), ReasonUnsupportedContent},
		{"volatile", []byte(`{"messages":[]}`), ReasonStaticMismatch},
	}
	for i, tc := range cases {
		if _, reason := snap.Match(tc.id, "1", tc.body); reason != tc.want {
			t.Errorf("case %d (%s): reason = %q, want %q", i, tc.id, reason, tc.want)
		}
	}
}

func TestMatchDoesNotModifyBody(t *testing.T) {
	snap := mustSnapshot(t, record(t, "support", 1, StatusActive, matchSpec()))
	body := chatBody([2]any{"system", "x"})
	before := string(body)
	snap.Match("support", "1", body)
	if string(body) != before {
		t.Fatal("Match modified the body")
	}
}

func TestJSONStringMatch(t *testing.T) {
	cases := []struct {
		raw, want string
		whole     bool
		match     bool
	}{
		{`""`, "", true, true},
		{`""`, "", false, true},
		{`"a"`, "", false, true},
		{`"a"`, "", true, false},
		{`"abc"`, "abc", true, true},
		{`"abc"`, "ab", false, true},
		{`"abc"`, "ab", true, false},
		{`"ab"`, "abc", false, false},
		{`"a\"b"`, `a"b`, true, true},
		{`"a\/b"`, `a/b`, true, true},
		{`"\b\f\n\r\t\\"`, "\b\f\n\r\t\\", true, true},
		{`"\u00e9"`, "é", true, true},
		{`"\u00E9x"`, "é", false, true},
		{`"\ud83d\ude00"`, "😀", true, true},
		{`"\ud83d\ude00"`, "\xf0\x9f", false, false}, // never a partial rune
		{`"\ud83d"`, "\uFFFD", true, true},           // lone surrogate, as encoding/json
		{`"\ud83dx"`, "\uFFFDx", true, true},
		{`"\ude00\ud83d"`, "\uFFFD\uFFFD", true, true},
		{`"\ud83d\u0041"`, "\uFFFDA", true, true},
		{`"\u003c"`, "<", true, true},
		{`"\u0000"`, "\x00", true, true},
		{`"\x"`, "x", true, false},
		{`"\u12"`, "x", true, false},
		{`abc`, "abc", true, false},
		{`"abc`, "abc", true, false},
		{`"`, "", true, false},
		{``, "", true, false},
	}
	for _, tc := range cases {
		if got := jsonStringMatch([]byte(tc.raw), tc.want, tc.whole); got != tc.match {
			t.Errorf("jsonStringMatch(%s, %q, whole=%v) = %v, want %v", tc.raw, tc.want, tc.whole, got, tc.match)
		}
	}
}

// FuzzJSONStringMatch checks the matcher against encoding/json: for any
// valid JSON string and any valid UTF-8 want, it agrees with decoding and
// comparing, except where raw holds invalid UTF-8 that decodes to U+FFFD
// (a mismatch there only fails open).
func FuzzJSONStringMatch(f *testing.F) {
	for _, s := range []string{`"abc"`, `"\u00e9\ud83d\ude00"`, `"\ud83d"`, `"a\/b\n"`, "\"\xff\""} {
		f.Add([]byte(s), "a", false)
		f.Add([]byte(s), "", true)
	}
	f.Fuzz(func(t *testing.T, raw []byte, want string, whole bool) {
		if !utf8.ValidString(want) || !json.Valid(raw) || len(raw) == 0 || raw[0] != '"' {
			return
		}
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return
		}
		oracle := s == want || !whole && strings.HasPrefix(s, want)
		got := jsonStringMatch(raw, want, whole)
		if got == oracle {
			return
		}
		if !got && !utf8.Valid(raw) && strings.ContainsRune(want, utf8.RuneError) {
			return
		}
		t.Fatalf("jsonStringMatch(%q, %q, %v) = %v, decoded %q", raw, want, whole, got, s)
	})
}

func FuzzJSONStringMatchEncoded(f *testing.F) {
	f.Add("You are helpful.\n\"é😀</tag>", 5)
	f.Fuzz(func(t *testing.T, s string, cut int) {
		if !utf8.ValidString(s) {
			return
		}
		want := s
		if cut >= 0 && cut < len(s) && utf8.RuneStart(s[cut]) {
			want = s[:cut]
		}
		goJSON, _ := json.Marshal(s)
		for _, raw := range [][]byte{goJSON, asciiJSON(s)} {
			if !jsonStringMatch(raw, want, false) {
				t.Fatalf("prefix %q of %q not matched in %s", want, s, raw)
			}
			if !jsonStringMatch(raw, s, true) {
				t.Fatalf("%q not matched in %s", s, raw)
			}
		}
	})
}

func TestNewSnapshot(t *testing.T) {
	secret := "SECRET-PROMPT-TEXT {{tenant}}"
	bad := matchSpec()
	bad.Messages[0].Segments[0].Text = secret // a placeholder in static text
	good := record(t, "support", 1, StatusActive, matchSpec())
	good.Counts = json.RawMessage(`{"pool":"large","prefix_tokens":120,"examples":[150]}`)
	recs := []Record{
		record(t, "zeta", 3, StatusDeprecated, matchSpec()),
		good,
		record(t, "support", 2, StatusActive, matchSpec()),
		record(t, "draft", 1, StatusDraft, matchSpec()),
		record(t, "retired", 1, StatusRetired, matchSpec()),
		record(t, "Bad ID", 1, StatusActive, matchSpec()),
		record(t, "zero", 0, StatusActive, matchSpec()),
		{ID: "unparseable", Version: 1, Status: StatusActive, Spec: json.RawMessage(`{"model":"` + secret + `",`)},
		record(t, "invalid", 1, StatusActive, bad),
		{ID: "counts", Version: 1, Status: StatusActive, Spec: good.Spec, Counts: json.RawMessage(`{"prefix_tokens":"x"}`)},
		record(t, "support", 2, StatusActive, matchSpec()), // duplicate
	}
	snap, errs := NewSnapshot(recs, time.Unix(100, 0))
	if len(errs) != 8 {
		t.Fatalf("NewSnapshot errors = %d: %v", len(errs), errs)
	}
	for _, err := range errs {
		if strings.Contains(err.Error(), "SECRET") {
			t.Errorf("error holds template text: %v", err)
		}
	}
	if snap.Len() != 3 || !snap.LoadedAt().Equal(time.Unix(100, 0)) {
		t.Fatalf("Len = %d, LoadedAt = %v", snap.Len(), snap.LoadedAt())
	}
	var got []string
	for _, v := range snap.Versions() {
		got = append(got, fmt.Sprintf("%s/%d", v.ID, v.Version))
	}
	if want := "support/1 support/2 zeta/3"; strings.Join(got, " ") != want {
		t.Fatalf("Versions = %v, want %s", got, want)
	}
	v, ok := snap.Lookup("support", 1)
	if !ok || v.PrefixTokens() != 120 || v.Counts.Pool != "large" || v.Status != StatusActive {
		t.Fatalf("Lookup(support, 1) = %+v, %v", v, ok)
	}
	if p := v.Prefix(); p.Role != "developer" || len(p.Messages) != 1 || p.Text != "Rules: <b>cite</b>. " {
		t.Fatalf("Prefix = %+v", p)
	}
	if v, _ := snap.Lookup("support", 2); v.PrefixTokens() != 0 {
		t.Fatal("empty counts are not zero counts")
	}
	for _, id := range []string{"draft", "retired", "zero", "unparseable", "invalid", "counts"} {
		if _, reason := snap.Match(id, "1", chatBody()); reason != ReasonUnknownTemplate {
			t.Errorf("Match(%s) = %q, want %q", id, reason, ReasonUnknownTemplate)
		}
	}
}

func TestSnapshotVersionsIsACopy(t *testing.T) {
	snap := mustSnapshot(t, record(t, "a", 1, StatusActive, matchSpec()))
	snap.Versions()[0] = nil
	if snap.Versions()[0] == nil {
		t.Fatal("Versions exposes the snapshot's slice")
	}
}

// Keys match exactly and the last duplicate wins, as in the parser vLLM
// reads the body with: a body vLLM would read differently from encoding/json's
// case-insensitive struct decoding must not be labelled from the other copy.
func TestMatchReadsKeysAsVLLMDoes(t *testing.T) {
	snap := mustSnapshot(t, record(t, "support", 1, StatusActive, matchSpec()))
	good := string(chatBody(
		[2]any{"system", "You are helpful.\n\"Quote\" é 😀 </tag> \\ / \t Be brief."},
		[2]any{"developer", "Rules: <b>cite</b>. Tenant acme"},
		[2]any{"user", "hi"}))
	static := `[{"role":"system","content":"You are helpful.\n\"Quote\" é 😀 </tag> \\ / \t Be brief."},{"role":"developer","content":"Rules: <b>cite</b>. Tenant acme"},{"role":"user","content":"hi"}]`
	other := `[{"role":"user","content":"what vLLM sees"}]`
	cases := []struct {
		name, body string
		want       bool
	}{
		{"plain", good, true},
		{"upper-case copy after the real one", `{"messages":` + other + `,"MESSAGES":` + static + `}`, false},
		{"upper-case copy before the real one", `{"MESSAGES":` + other + `,"messages":` + static + `}`, true},
		{"only an upper-case key", `{"MESSAGES":` + static + `}`, false},
		{"duplicate messages, first is static", `{"messages":` + static + `,"messages":` + other + `}`, false},
		{"duplicate messages, last is static", `{"messages":` + other + `,"messages":` + static + `}`, true},
		{"role in capitals", strings.Replace(good, `"role"`, `"ROLE"`, 1), false},
		{"content in capitals", strings.Replace(good, `"content"`, `"Content"`, 1), false},
	}
	for _, c := range cases {
		v, reason := snap.Match("support", "1", []byte(c.body))
		if (v != nil) != c.want {
			t.Errorf("%s: matched=%v (reason %q), want %v", c.name, v != nil, reason, c.want)
		}
	}
}
