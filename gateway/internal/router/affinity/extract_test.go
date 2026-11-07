package affinity_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/affinity"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/affinity/affinitytest"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// key extracts a Tier 2 key from body, failing t if there is none.
func key(t *testing.T, body string) string {
	t.Helper()
	res := affinity.Extract(nil, []byte(body))
	assertResult(t, res, affinity.TierPrefixHash, "")
	return res.Key
}

// conv builds a chat body from raw message objects.
func conv(msgs ...string) string {
	return `{"model":"m","messages":[` + strings.Join(msgs, ",") + `]}`
}

// TestPreambleRoles: every leading system or developer message, however the
// role member is spaced or escaped, is preamble, so the first user message is
// part of the key: same conversation, same key every turn; different first
// question, different key.
func TestPreambleRoles(t *testing.T) {
	user1 := `{"role":"user","content":"first question"}`
	user2 := `{"role":"user","content":"another question"}`
	reply := `{"role":"assistant","content":"an answer"}`
	more := `{"role":"user","content":"follow-up"}`
	preambles := map[string][]string{
		"system":               {`{"role":"system","content":"S"}`},
		"developer":            {`{"role":"developer","content":"D"}`},
		"spaced colon":         {`{"role" : "system","content":"S"}`},
		"newline before value": {"{\"role\":\n \"system\",\"content\":\"S\"}"},
		"tabs":                 {"{\t\"role\"\t:\t\"developer\"\t,\"content\":\"D\"}"},
		"role after content":   {`{"content":"S","role":"system"}`},
		"escaped role key":     {`{"r\u006fle":"system","content":"S"}`},
		"escaped role value":   {`{"role":"syst\u0065m","content":"S"}`},
		"repeated role, last":  {`{"role":"user","content":"S","role":"system"}`},
		"two preambles":        {`{"role":"system","content":"S"}`, `{"role":"developer","content":"D"}`},
		"content parts":        {`{"role":"system","content":[{"type":"text","text":"S1"},{"type":"text","text":"S2"}]}`},
	}
	for name, pre := range preambles {
		t.Run(name, func(t *testing.T) {
			turn1 := key(t, conv(append(append([]string{}, pre...), user1)...))
			turn3 := key(t, conv(append(append([]string{}, pre...), user1, reply, more)...))
			other := key(t, conv(append(append([]string{}, pre...), user2)...))
			if turn1 != turn3 {
				t.Errorf("key changed between turns: %s vs %s", turn1, turn3)
			}
			if turn1 == other {
				t.Errorf("different first questions share key %s: first user message not hashed", turn1)
			}
		})
	}
}

// TestNonPreambleFirstMessage: when the first message is not preamble, only
// it is hashed, so later turns never change the key.
func TestNonPreambleFirstMessage(t *testing.T) {
	cases := map[string]string{
		"user":            `{"role":"user","content":"Q"}`,
		"role not string": `{"role":5,"content":"Q"}`,
		"no role":         `{"content":"Q"}`,
		"system as text":  `{"role":"user","content":"\"role\":\"system\""}`,
		"content parts":   `{"role":"user","content":[{"type":"text","text":"Q"},{"type":"image_url","image_url":{"url":"data:x"}}]}`,
	}
	for name, first := range cases {
		t.Run(name, func(t *testing.T) {
			a := key(t, conv(first))
			b := key(t, conv(first, `{"role":"assistant","content":"A"}`, `{"role":"user","content":"Q2"}`))
			c := key(t, conv(first, `{"role":"user","content":"Q3"}`))
			if a != b || a != c {
				t.Errorf("keys differ: %s %s %s", a, b, c)
			}
		})
	}
}

// TestPreambleOnly: a conversation of preamble messages is keyed on them.
func TestPreambleOnly(t *testing.T) {
	a := key(t, conv(`{"role":"system","content":"S1"}`))
	b := key(t, conv(`{"role":"system","content":"S2"}`))
	if a == b {
		t.Fatal("different system prompts share a key")
	}
}

// TestToolsBeforeMessages: a tools member (with a nested "messages" property
// and "role":"system" values) ahead of messages changes nothing: the key is
// the one the same messages get without tools.
func TestToolsBeforeMessages(t *testing.T) {
	msgs := `"messages":[{"role":"system","content":"S"},{"role":"user","content":"Q"}]`
	plain := key(t, `{"model":"m",`+msgs+`}`)
	for _, size := range []int{1 << 10, 32 << 10} {
		before := key(t, `{"model":"m","tools":`+denseTools(size)+`,`+msgs+`}`)
		after := key(t, `{"model":"m",`+msgs+`,"tools":`+denseTools(size)+`}`)
		if before != plain || after != plain {
			t.Errorf("tools %d B: keys %s (before) %s (after), want %s", size, before, after, plain)
		}
	}
}

// TestTopLevelKeys: escaped key names match and keys are case-sensitive.
// Repeated keys fail open (TestDuplicateKeysFailOpen).
func TestTopLevelKeys(t *testing.T) {
	a := `[{"role":"user","content":"A"}]`
	b := `[{"role":"user","content":"B"}]`
	keyA := key(t, `{"messages":`+a+`}`)
	keyB := key(t, `{"messages":`+b+`}`)
	if keyA == keyB {
		t.Fatal("different conversations share a key")
	}
	cases := []struct {
		name, body, want string
	}{
		{"escaped key", `{"m\u0065ssages":` + a + `}`, keyA},
		{"case-sensitive", `{"Messages":` + b + `,"messages":` + a + `,"MESSAGES":` + b + `}`, keyA},
		{"whitespace", "{\n  \"messages\" :\n  " + a + "\n}\n", keyA},
		{"nested messages ignored", `{"extra":{"messages":` + b + `},"messages":` + a + `}`, keyA},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := key(t, tc.body); got != tc.want {
				t.Errorf("key %s, want %s", got, tc.want)
			}
		})
	}
}

// TestFailOpenReasons: every body without a usable key is TierNone with the
// reason the router records as gateway_failopen_total{stage="affinity"}.
func TestFailOpenReasons(t *testing.T) {
	cases := []struct {
		name, body, reason string
	}{
		{"messages null", `{"messages":null}`, affinity.ReasonNoKey},
		{"messages null after a list (duplicate)", `{"messages":[{"role":"user","content":"A"}],"messages":null}`, affinity.ReasonDuplicateKey},
		{"messages empty", `{"messages":[ ]}`, affinity.ReasonEmptyMessages},
		{"messages string", `{"messages":"hi"}`, telemetry.ReasonUnparseableBody},
		{"messages of numbers", `{"messages":[1,2]}`, telemetry.ReasonUnparseableBody},
		{"no messages or prompt", `{"model":"m","stream":true}`, affinity.ReasonNoKey},
		{"empty object", `{}`, affinity.ReasonNoKey},
		{"prompt null", `{"prompt":null}`, affinity.ReasonNoKey},
		{"prompt empty", `{"prompt":""}`, affinity.ReasonNoKey},
		{"prompt empty list", `{"prompt":[]}`, affinity.ReasonNoKey},
		{"not an object", `[{"messages":[]}]`, telemetry.ReasonUnparseableBody},
		{"whitespace only", " \n ", telemetry.ReasonUnparseableBody},
		{"truncated in messages", `{"messages":[{"role":"user","content":"A`, telemetry.ReasonUnparseableBody},
		{"truncated after messages", `{"messages":[{"role":"user","content":"A"}],"model":`, telemetry.ReasonUnparseableBody},
		{"bad literal", `{"messages":nul}`, telemetry.ReasonUnparseableBody},
		{"missing colon", `{"messages" [1]}`, telemetry.ReasonUnparseableBody},
		{"unquoted key", `{messages:[]}`, telemetry.ReasonUnparseableBody},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertResult(t, affinity.Extract(nil, []byte(tc.body)), affinity.TierNone, tc.reason)
		})
	}
}

// TestMessageCap: only the first 8 KiB of a message is hashed, plus its
// length; a 256 KiB system prompt keeps its key across turns.
func TestMessageCap(t *testing.T) {
	big := text(256<<10, "sys")
	sys := func(content string) string { return `{"role":"system","content":"` + content + `"}` }
	user := `{"role":"user","content":"Q"}`
	turn1 := key(t, conv(sys(big), user))
	turn2 := key(t, conv(sys(big), user, `{"role":"assistant","content":"A"}`, `{"role":"user","content":"Q2"}`))
	if turn1 != turn2 {
		t.Errorf("256 KiB system prompt: key changed between turns")
	}
	// Same first 8 KiB, different length: different key.
	if longer := key(t, conv(sys(big+"x"), user)); longer == turn1 {
		t.Errorf("length not hashed")
	}
	// Same first 8 KiB and length, differing after the cap: one key (the
	// documented tradeoff).
	tail := []byte(big)
	tail[len(tail)-10] = 'Z'
	if same := key(t, conv(sys(string(tail)), user)); same != turn1 {
		t.Errorf("bytes after the 8 KiB cap changed the key")
	}
}

// TestCompletionsPrefix: /v1/completions keys on the first 2 KiB of the
// prompt, so a prompt that grows by appending keeps its key once it is that
// long; a list prompt keys on its first element.
func TestCompletionsPrefix(t *testing.T) {
	preamble := text(3<<10, "template")
	turn1 := preamble + `User: hello\nAssistant:`
	turn2 := turn1 + ` Hi there!\nUser: tell me more\nAssistant:`
	k1 := key(t, `{"model":"m","prompt":"`+turn1+`"}`)
	if k2 := key(t, `{"model":"m","prompt":"`+turn2+`","max_tokens":64}`); k2 != k1 {
		t.Errorf("growing prompt changed key")
	}
	if k := key(t, `{"model":"m","prompt":["`+turn2+`","second prompt"]}`); k != k1 {
		t.Errorf("list prompt: key %s, want the first element's %s", k, k1)
	}
	if other := key(t, `{"model":"m","prompt":"`+text(3<<10, "other")+`"}`); other == k1 {
		t.Errorf("different prompts share a key")
	}

	tokens := func(n int) string {
		var sb strings.Builder
		for i := range n {
			if i > 0 {
				sb.WriteByte(',')
			}
			fmt.Fprintf(&sb, "%d", 1000+i)
		}
		return sb.String()
	}
	t1 := key(t, `{"prompt":[`+tokens(600)+`]}`)
	if t2 := key(t, `{"prompt":[`+tokens(900)+` ]}`); t2 != t1 {
		t.Errorf("growing token prompt changed key")
	}
	if t3 := key(t, `{"prompt":[[`+tokens(900)+`],[1,2]]}`); t3 == "" {
		t.Errorf("token-list batch has no key")
	}
	short1 := key(t, `{"prompt":[1,2,3]}`)
	if short2 := key(t, `{"prompt":[1,2,3,4]}`); short1 == short2 {
		t.Errorf("short token prompts below the cap should key on all of it")
	}
	// The 2 KiB prefix is decided without reading the rest, so a prompt too
	// long to skip within the budget keeps its key.
	longPrompt := escapedJSON(4 << 20)
	k4 := key(t, `{"prompt":"`+longPrompt[:4096]+`"}`)
	if k := key(t, `{"prompt":"`+longPrompt+`"}`); k != k4 {
		t.Errorf("long prompt: key %s, want its prefix's %s", k, k4)
	}
	if k := key(t, `{"prompt":["`+longPrompt[:4096]+`","b"]}`); k != k4 {
		t.Errorf("list prompt: key %s, want its first element's %s", k, k4)
	}

	// messages wins over prompt when both are present.
	if k := key(t, `{"prompt":"p","messages":[{"role":"user","content":"Q"}]}`); k != key(t, conv(`{"role":"user","content":"Q"}`)) {
		t.Errorf("messages did not take precedence over prompt")
	}
}

// TestScanBudget: work is bounded. A body whose key is decided before the
// budget runs out keeps it; one where the budget runs out first fails open.
func TestScanBudget(t *testing.T) {
	short := conv(`{"role":"system","content":"S"}`, `{"role":"user","content":"Q"}`)
	want := key(t, short)

	var long strings.Builder
	long.WriteString(`{"messages":[{"role":"system","content":"S"},{"role":"user","content":"Q"}`)
	for i := range 20000 {
		fmt.Fprintf(&long, `,{"role":"assistant","content":"%d"}`, i)
	}
	long.WriteString(`]}`)
	if got := key(t, long.String()); got != want {
		t.Errorf("long conversation: key %s, want %s", got, want)
	}

	// Below the measured limits a key is still decided.
	q := `{"role":"user","content":"Q"}`
	wantQ := key(t, conv(q))
	if got := key(t, `{"tools":`+denseTools(29<<10)+`,"messages":[`+q+`]}`); got != wantQ {
		t.Errorf("29 KiB of tools before messages: key %s, want %s", got, wantQ)
	}
	// The guarantee keeps at least 10% of the budget as margin (M1-24): the
	// 29 KiB body must key from no more than 1/1.1 of the budget, so a price
	// that rises by up to 10% cannot take it away. It was 30 KiB until the
	// M1-28 prices left that body 8.9% (ADR 0017).
	for name, body := range map[string]string{
		"29 KiB of dense tools":  `{"tools":` + denseTools(29<<10) + `,"messages":[` + q + `]}`,
		"32 KiB of escaped JSON": conv(`{"role":"system","content":"`+escapedJSON(32<<10)+`"}`, q),
	} {
		res, work, limit := affinity.ExtractStats([]byte(body))
		if res.Tier == affinity.TierNone || float64(work)*1.1 > float64(limit) {
			t.Errorf("%s: %d of %d units (tier %s reason %q), want at most %d", name, work, limit, res.Tier, res.Reason, int(float64(limit)/1.1))
		}
	}
	key(t, conv(`{"role":"system","content":"`+escapedJSON(32<<10)+`"}`, q))
	key(t, imageMessage(1<<20))

	// One huge string, wherever it is, is bounded: the quote search stops
	// where the budget does.
	huge := `{"x":"` + strings.Repeat("A", 16<<20) + `","messages":[` + q + `]}`
	assertResult(t, affinity.Extract(nil, []byte(huge)), affinity.TierNone, affinity.ReasonScanBudget)
	assertResult(t, affinity.Extract(nil, []byte(imageMessage(16<<20))), affinity.TierNone, affinity.ReasonScanBudget)
	unterminated := `{"messages":[{"role":"user","content":"` + strings.Repeat("A", 32<<20)
	assertResult(t, affinity.Extract(nil, []byte(unterminated)), affinity.TierNone, affinity.ReasonScanBudget)

	// A duplicate messages member fails open at its key, before its value
	// is read (M1-25).
	dup := `{"messages":[` + q + `],"messages":[{"role":"system","content":"` + escapedJSON(2<<20) + `"},` + q + `]}`
	assertResult(t, affinity.Extract(nil, []byte(dup)), affinity.TierNone, affinity.ReasonDuplicateKey)
	// Without a key decided first, a messages value the budget cuts short
	// still fails open.
	cut := `{"messages":[{"role":"system","content":"` + escapedJSON(2<<20) + `"},` + q + `]}`
	assertResult(t, affinity.Extract(nil, []byte(cut)), affinity.TierNone, affinity.ReasonScanBudget)

	toolsFirst := `{"tools":` + denseTools(4<<20) + `,"messages":[{"role":"user","content":"Q"}]}`
	assertResult(t, affinity.Extract(nil, []byte(toolsFirst)), affinity.TierNone, affinity.ReasonScanBudget)

	// The pre-key budget is sized for AC-M1-5 on a slow server core
	// (M1-21, priced per M1-22 and M1-28): just above the measured limits
	// (about 32.6 KiB of dense tools ahead of the messages, 37.3 KiB of
	// escaped JSON in a system prompt) a body fails open. These two bodies
	// key from 45,107 and 42,821 units, so a budget raised past those, or
	// prices cut by about 9% and 4%, trip them; the guarantee cases above
	// bound it from below.
	toolsAhead36K := `{"tools":` + denseTools(36<<10) + `,"messages":[` + q + `]}`
	assertResult(t, affinity.Extract(nil, []byte(toolsAhead36K)), affinity.TierNone, affinity.ReasonScanBudget)
	escaped39K := conv(`{"role":"system","content":"`+escapedJSON(39<<10)+`"}`, q)
	assertResult(t, affinity.Extract(nil, []byte(escaped39K)), affinity.TierNone, affinity.ReasonScanBudget)

	esc := conv(`{"role":"system","content":"`+escapedJSON(1<<20)+`"}`, `{"role":"user","content":"Q"}`)
	assertResult(t, affinity.Extract(nil, []byte(esc)), affinity.TierNone, affinity.ReasonScanBudget)

	backslashes := `{"messages":[{"role":"user","content":"` + strings.Repeat(`\\`, 1<<20) + `"}]}`
	assertResult(t, affinity.Extract(nil, []byte(backslashes)), affinity.TierNone, affinity.ReasonScanBudget)

	spaces := `{"model":"m",` + strings.Repeat(" ", 1<<20) + `"messages":[]}`
	assertResult(t, affinity.Extract(nil, []byte(spaces)), affinity.TierNone, affinity.ReasonScanBudget)

	number := `{"n":` + strings.Repeat("1", 1<<20) + `,"messages":[]}`
	assertResult(t, affinity.Extract(nil, []byte(number)), affinity.TierNone, affinity.ReasonScanBudget)
}

// TestEscapes: escaped quotes and backslashes inside strings are skipped
// exactly.
func TestEscapes(t *testing.T) {
	cases := []string{
		`{"x":"a\"b","messages":[{"role":"user","content":"Q"}]}`,
		`{"x":"a\\","messages":[{"role":"user","content":"Q"}]}`,
		`{"x":"` + strings.Repeat(`\\`, 100) + `\"` + strings.Repeat("y", 100) + `","messages":[{"role":"user","content":"Q"}]}`,
		`{"x":"` + strings.Repeat("y", 100) + strings.Repeat(`\\`, 3) + `","messages":[{"role":"user","content":"Q"}]}`,
		`{"x":["]","}","[","{"],"messages":[{"role":"user","content":"Q"}]}`,
	}
	want := key(t, conv(`{"role":"user","content":"Q"}`))
	for i, body := range cases {
		if got := key(t, body); got != want {
			t.Errorf("case %d: key %s, want %s", i, got, want)
		}
	}
}

// TestBodyUnchanged: Extract never writes to the body.
func TestBodyUnchanged(t *testing.T) {
	for _, tc := range append(latencyCases(), worstCases()...) {
		orig := bytes.Clone(tc.Body)
		affinity.Extract(nil, tc.Body)
		if !bytes.Equal(orig, tc.Body) {
			t.Fatalf("%s: body modified", tc.Name)
		}
	}
}

// TestPostKeyCap: once a messages key is decided, the scan goes on for
// PostKeyBudget more work only. A duplicate "messages" member found within
// the cap fails open (M1-25); one beyond the cap is never reached, and the
// key decided before it is used. The body is never modified.
func TestPostKeyCap(t *testing.T) {
	first := `[{"role":"user","content":"A"}]`
	second := `[{"role":"user","content":"B"}]`
	keyA, keyB := key(t, `{"messages":`+first+`}`), key(t, `{"messages":`+second+`}`)
	near := denseTools(2 << 10) // well within the post-key cap
	far := denseTools(16 << 10) // past the post-key cap, well within the pre-key budget
	if 16<<10 <= affinity.PostKeyBudget || 2*(16<<10) >= affinity.PreKeyBudget {
		t.Fatalf("filler sizes do not straddle the caps (post %d, pre %d)", affinity.PostKeyBudget, affinity.PreKeyBudget)
	}
	// The far filler alone is cheap enough to read before a key.
	if got := key(t, `{"tools":`+far+`,"messages":`+second+`}`); got != keyB {
		t.Fatalf("far filler ahead of the key: key %s, want %s", got, keyB)
	}

	cases := []struct {
		name, body, want, reason string
	}{
		{"duplicate within the cap fails open", `{"messages":` + first + `,"tools":` + near + `,"messages":` + second + `}`, "", affinity.ReasonDuplicateKey},
		{"duplicate beyond the cap is ignored", `{"messages":` + first + `,"tools":` + far + `,"messages":` + second + `}`, keyA, ""},
		{"turns after the key are not read into it", conv(`{"role":"user","content":"A"}`, `{"role":"assistant","content":[`+strings.Repeat(`{"type":"text","text":"x"},`, 2000)+`{"type":"text","text":"y"}]}`), key(t, conv(`{"role":"user","content":"A"}`)), ""},
		{"duplicate with a long value fails open at its key", `{"messages":` + first + `,"messages":[{"role":"system","content":"` + escapedJSON(16<<10) + `"},{"role":"user","content":"B"}]}`, "", affinity.ReasonDuplicateKey},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(tc.body)
			orig := bytes.Clone(body)
			got := affinity.Extract(nil, body)
			if !bytes.Equal(orig, body) {
				t.Fatal("body modified")
			}
			if tc.reason != "" {
				assertResult(t, got, affinity.TierNone, tc.reason)
				return
			}
			if got.Tier != affinity.TierPrefixHash || got.Key != tc.want {
				t.Fatalf("got %+v, want key %s", got, tc.want)
			}
		})
	}
}

// TestDuplicateKeysFailOpen: a second top-level "messages" or "prompt"
// member (by any spelling of the key) fails open as duplicate_key at that
// key, so the key computation runs at most once per field and a body of
// repeated keys costs almost nothing (M1-25). Distinct fields, other
// spellings by case and nested keys are not duplicates.
func TestDuplicateKeysFailOpen(t *testing.T) {
	a := `[{"role":"user","content":"A"}]`
	b := `[{"role":"user","content":"B"}]`
	bs := "\\"
	dups := []struct{ name, body string }{
		{"messages twice", `{"messages":` + a + `,"model":"m","messages":` + b + `}`},
		{"messages twice, escaped", `{"messages":` + b + `,"messag` + bs + `u0065s":` + a + `}`},
		{"prompt twice", `{"prompt":"hello","prompt":"world"}`},
		{"prompt tokens repeated", string(affinitytest.DuplicateMembers(`"prompt":[1],`, 200_000))},
		{"prompt token lists repeated", string(affinitytest.DuplicateMembers(`"prompt":[[1]],`, 180_000))},
		{"prompt token pairs repeated", string(affinitytest.DuplicateMembers(`"prompt":[1,1],`, 150_000))},
		{"empty prompts repeated", string(affinitytest.DuplicateMembers(`"prompt":[],`, 200_000))},
		{"messages objects repeated", string(affinitytest.DuplicateMembers(`"messages":{},`, 120_000))},
		{"messages repeated", string(affinitytest.DuplicateMessages(a, 50_000))},
	}
	for _, tc := range dups {
		t.Run(tc.name, func(t *testing.T) {
			if !strings.Contains(tc.body, bs) && strings.Contains(tc.name, "escaped") {
				t.Fatal("escape lost")
			}
			res, work, _ := affinity.ExtractStats([]byte(tc.body))
			assertResult(t, res, affinity.TierNone, affinity.ReasonDuplicateKey)
			if work > 512 {
				t.Errorf("work %d units, want the scan to stop at the second key", work)
			}
		})
	}
	keyA := key(t, `{"messages":`+a+`}`)
	for _, body := range []string{
		`{"messages":` + a + `,"prompt":"hello"}`,
		`{"Messages":` + b + `,"messages":` + a + `,"MESSAGES":` + b + `}`,
		`{"extra":{"messages":` + b + `,"messages":` + b + `},"messages":` + a + `}`,
	} {
		if got := key(t, body); got != keyA {
			t.Errorf("%.60s: key %s, want %s", body, got, keyA)
		}
	}
}

// TestExtractBudget checks the budget argument: a body whose key needs more
// work than a small budget fails open with the scan-budget reason, the same
// body keys under the default, and 0 or less means the default.
func TestExtractBudget(t *testing.T) {
	q := `{"role":"user","content":"Q"}`
	body := []byte(`{"tools":` + denseTools(8<<10) + `,"messages":[` + q + `]}`)
	want := affinity.ExtractBudget(nil, body, affinity.DefaultKeyScanBudget)
	if want.Tier == affinity.TierNone {
		t.Fatalf("8 KiB of tools did not key under the default budget: %s", want.Reason)
	}
	assertResult(t, affinity.ExtractBudget(nil, body, 1<<10), affinity.TierNone, affinity.ReasonScanBudget)
	for _, b := range []int{0, -1} {
		if got := affinity.ExtractBudget(nil, body, b); got != want {
			t.Errorf("budget %d: %+v, want the default's %+v", b, got, want)
		}
	}
	if got := affinity.Extract(nil, body); got != want {
		t.Errorf("Extract: %+v, want %+v", got, want)
	}
}
