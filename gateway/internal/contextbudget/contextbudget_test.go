package contextbudget

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math/rand/v2"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

// parseIndexed reads body into an index, as ingress does once per request,
// and parses it; the index is what Assemble splices a cut through.
func parseIndexed(body []byte, keep int) (*Request, *bodyindex.Index, error) {
	ix := new(bodyindex.Index)
	_ = ix.Read(body)
	q, err := Parse(ix, keep)
	return q, ix, err
}

// parseBody is parseIndexed for a test that does not assemble.
func parseBody(body []byte, keep int) (*Request, error) {
	q, _, err := parseIndexed(body, keep)
	return q, err
}

// msgCost is the fake count of one message: its compact bytes plus a
// per-message overhead. It ignores the whitespace between messages, so it
// does not depend on how a cut was made.
func msgCost(raw []byte) int {
	var b bytes.Buffer
	_ = json.Compact(&b, raw)
	return b.Len() + 3
}

// fakeCounter counts a messages array as the sum of its messages' costs.
type fakeCounter struct {
	calls int
	err   error
}

func (c *fakeCounter) Count(_ context.Context, messages []byte) (int, error) {
	c.calls++
	if c.err != nil {
		return 0, c.err
	}
	var elems []json.RawMessage
	if err := json.Unmarshal(messages, &elems); err != nil {
		return 0, fmt.Errorf("counter got an invalid array: %w", err)
	}
	n := 0
	for _, e := range elems {
		n += msgCost(e)
	}
	return n, nil
}

var spaces = []string{"", " ", "\n  ", "\t"}

// history is a random chat body and what the test knows of it.
type history struct {
	body  []byte
	elems []string // each message as written
	roles []string
}

func randomHistory(rng *rand.Rand) history {
	var h history
	sp := func() string { return spaces[rng.IntN(len(spaces))] }
	add := func(role string) {
		content := strings.Repeat("word ", rng.IntN(12)) + `caf\u00e9 \"q\"`
		var m string
		switch rng.IntN(3) {
		case 0:
			m = fmt.Sprintf(`{"role":%q,%s"content":"%s"}`, role, sp(), content)
		case 1:
			m = fmt.Sprintf(`{ "content" : "%s", "role" : %q }`, content, role)
		default:
			m = fmt.Sprintf(`{"role":%q,"content":null,"tool_calls":[{"id":"c%d","function":{"name":"f","arguments":"{\"a\":[1,2]}"}}]}`, role, rng.IntN(99))
		}
		h.elems, h.roles = append(h.elems, m), append(h.roles, role)
	}
	for range rng.IntN(4) {
		add([]string{"system", "developer"}[rng.IntN(2)])
	}
	all := []string{"user", "user", "assistant", "tool", "system", "developer"}
	for range rng.IntN(16) {
		add(all[rng.IntN(len(all))])
	}
	var b strings.Builder
	fmt.Fprintf(&b, `{%s"model":"m",%s"messages":%s[%s`, sp(), sp(), sp(), sp())
	for i, e := range h.elems {
		if i > 0 {
			b.WriteString(sp() + "," + sp())
		}
		b.WriteString(e)
	}
	fmt.Fprintf(&b, `%s],"temperature":0,"stream":true%s}`, sp(), sp())
	h.body = []byte(b.String())
	return h
}

// oracle computes, independently of the package, the history's prefix
// (at least keep messages, as for a template's static prefix), turn starts
// and the cost of keeping the last k turns.
func (h history) oracle(keep int) (prefix int, turns []int, cost func(k int) int, kept func(k int) []string) {
	for prefix < len(h.roles) && (h.roles[prefix] == "system" || h.roles[prefix] == "developer") {
		prefix++
	}
	if keep > prefix { // a template prefix runs on to the next turn's start
		prefix = len(h.roles)
		for i := keep; i < len(h.roles); i++ {
			if h.roles[i] == "user" {
				prefix = i
				break
			}
		}
	}
	for i := prefix; i < len(h.roles); i++ {
		if i == prefix || h.roles[i] == "user" {
			turns = append(turns, i)
		}
	}
	kept = func(k int) []string {
		first := len(h.elems)
		if k > 0 {
			first = turns[len(turns)-k]
		}
		return append(append([]string(nil), h.elems[:prefix]...), h.elems[first:]...)
	}
	cost = func(k int) int {
		n := 0
		for _, e := range kept(k) {
			n += msgCost([]byte(e))
		}
		return n
	}
	return prefix, turns, cost, kept
}

// TestAssembleProperties proves AC-M4-6 on random histories and budgets:
// the assembled prompt's count is at most the budget, it keeps the most
// turns that fit, kept messages are the caller's bytes in order, the rest
// of the body is untouched and every drop is counted.
func TestAssembleProperties(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	var over, trimmed int
	defer func() {
		if !t.Failed() && (over < 100 || trimmed < 100) {
			t.Errorf("weak sample: %d over budget, %d trimmed", over, trimmed)
		}
	}()
	for iter := range 3000 {
		h := randomHistory(rng)
		keep := 0
		if rng.IntN(4) == 0 { // a template's static prefix, of any roles
			keep = rng.IntN(len(h.elems) + 1)
		}
		_, turns, cost, kept := h.oracle(keep)
		n := len(turns)
		budget := rng.IntN(cost(n) + 40)
		q, ix, err := parseIndexed(h.body, keep)
		if err != nil {
			t.Fatalf("%d: Parse(%s): %v", iter, h.body, err)
		}
		c := &fakeCounter{}
		res, err := q.Assemble(context.Background(), ix, budget, 12, c)
		minimum := cost(min(n, 1))
		if minimum > budget {
			if !errors.Is(err, ErrOverBudget) {
				t.Fatalf("%d: minimum %d over budget %d: err = %v", iter, minimum, budget, err)
			}
			over++
			continue
		}
		if err != nil {
			t.Fatalf("%d: Assemble: %v", iter, err)
		}
		if c.calls != res.Probes || res.Probes > 12 {
			t.Fatalf("%d: %d counter calls, Probes %d", iter, c.calls, res.Probes)
		}
		k := n - res.DroppedTurns
		if k < n {
			trimmed++
		}
		if cost(k) > budget || (k < n && cost(k+1) <= budget) {
			t.Fatalf("%d: kept %d of %d turns at budget %d (costs %d, next %d)", iter, k, n, budget, cost(k), cost(min(k+1, n)))
		}
		var got, orig map[string]json.RawMessage
		if err := json.Unmarshal(res.Body, &got); err != nil {
			t.Fatalf("%d: assembled body invalid: %v\n%s", iter, err, res.Body)
		}
		_ = json.Unmarshal(h.body, &orig)
		var msgs []json.RawMessage
		_ = json.Unmarshal(got["messages"], &msgs)
		want := kept(k)
		if len(msgs) != len(want) || res.DroppedMessages != len(h.elems)-len(want) {
			t.Fatalf("%d: %d messages kept, %d dropped reported; want %d kept of %d", iter, len(msgs), res.DroppedMessages, len(want), len(h.elems))
		}
		for i := range msgs {
			if string(msgs[i]) != want[i] {
				t.Fatalf("%d: message %d = %s, want the caller's %s", iter, i, msgs[i], want[i])
			}
		}
		for key, v := range orig {
			if key != "messages" && !bytes.Equal(got[key], v) {
				t.Fatalf("%d: member %q changed", iter, key)
			}
		}
		if res.DroppedTurns == 0 && !bytes.Equal(res.Body, h.body) {
			t.Fatalf("%d: nothing dropped but the body changed", iter)
		}
	}
}

// TestAssembleFewProbes: when the probes run out after a suffix was
// proven to fit, that suffix is the answer, marked approximate; only when
// nothing was proven is the request skipped. Nothing is ever over budget.
func TestAssembleFewProbes(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	var approx, skipped int
	for iter := range 2000 {
		h := randomHistory(rng)
		_, turns, cost, _ := h.oracle(0)
		n := len(turns)
		budget := rng.IntN(cost(n) + 40)
		q, ix, _ := parseIndexed(h.body, 0)
		maxProbes := 1 + rng.IntN(3)
		c := &fakeCounter{}
		res, err := q.Assemble(context.Background(), ix, budget, maxProbes, c)
		var se *SkipError
		k := n - res.DroppedTurns
		switch {
		case c.calls > maxProbes:
			t.Fatalf("%d: %d probes, limit %d", iter, c.calls, maxProbes)
		case errors.As(err, &se):
			skipped++
			if se.Reason != ReasonProbesExhausted || c.calls != maxProbes {
				t.Fatalf("%d: skipped with %q after %d probes", iter, se.Reason, c.calls)
			}
		case errors.Is(err, ErrOverBudget):
			if cost(min(n, 1)) <= budget {
				t.Fatalf("%d: 422 though the last turn fits", iter)
			}
		case err != nil:
			t.Fatalf("%d: %v", iter, err)
		case cost(k) > budget:
			t.Fatalf("%d: kept %d turns, cost %d over budget %d", iter, k, cost(k), budget)
		case res.Approximate:
			approx++
			if res.DroppedTurns == 0 || c.calls != maxProbes {
				t.Fatalf("%d: approximate with %d turns dropped after %d probes", iter, res.DroppedTurns, c.calls)
			}
		case k < n && cost(k+1) <= budget:
			t.Fatalf("%d: not approximate, but %d turns would fit", iter, k+1)
		}
	}
	if approx < 50 || skipped < 50 {
		t.Fatalf("weak sample: %d approximate, %d skipped", approx, skipped)
	}
}

// noisyCounter counts like fakeCounter plus a deterministic noise of up to
// 40 tokens per candidate, so adding a turn can lower the count.
type noisyCounter struct{ fakeCounter }

func (c *noisyCounter) Count(ctx context.Context, messages []byte) (int, error) {
	n, err := c.fakeCounter.Count(ctx, messages)
	h := fnv.New32a()
	h.Write(messages)
	return n + int(h.Sum32()%41), err
}

// TestAssembleNonMonotoneCounter: whatever the counter, what is sent fits
// the budget by that counter's own count.
func TestAssembleNonMonotoneCounter(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 6))
	for iter := range 2000 {
		h := randomHistory(rng)
		_, turns, cost, _ := h.oracle(0)
		budget := rng.IntN(cost(len(turns)) + 80)
		q, ix, _ := parseIndexed(h.body, 0)
		c := &noisyCounter{}
		res, err := q.Assemble(context.Background(), ix, budget, 1+rng.IntN(12), c)
		if err != nil {
			continue
		}
		var got map[string]json.RawMessage
		_ = json.Unmarshal(res.Body, &got)
		if n, _ := c.Count(context.Background(), got["messages"]); n > budget {
			t.Fatalf("%d: sent a prompt of %d over budget %d (approximate %t)", iter, n, budget, res.Approximate)
		}
	}
}

func TestAssembleCutsExactBytes(t *testing.T) {
	sys, u1, a1, tool, u2 := `{"role":"system","content":"s"}`, `{"role":"user","content":"one"}`,
		`{"role":"assistant","tool_calls":[{"id":"x"}]}`, `{"role":"tool","tool_call_id":"x","content":"r"}`, `{"role":"user","content":"two"}`
	for _, tc := range []struct {
		name, body, want string
		turns, msgs      int
	}{
		{"prefix", `{"messages":[` + sys + `, ` + u1 + `, ` + a1 + `,` + tool + ` , ` + u2 + `],"model":"m"}`,
			`{"messages":[` + sys + ` , ` + u2 + `],"model":"m"}`, 1, 3},
		{"no prefix", `{"model":"m","messages":[ ` + u1 + `, ` + a1 + `, ` + tool + `, ` + u2 + ` ]}`,
			`{"model":"m","messages":[ ` + u2 + ` ]}`, 1, 3},
		{"orphan before first user", `{"messages":[` + sys + `,` + a1 + `,` + u2 + `]}`,
			`{"messages":[` + sys + `,` + u2 + `]}`, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, ix, err := parseIndexed([]byte(tc.body), 0)
			if err != nil {
				t.Fatal(err)
			}
			// Room for the system message and the last turn only.
			budget := msgCost([]byte(sys)) + msgCost([]byte(u2))
			if !strings.Contains(tc.body, sys) {
				budget = msgCost([]byte(u2))
			}
			res, err := q.Assemble(context.Background(), ix, budget, 12, &fakeCounter{})
			if err != nil || string(res.Body) != tc.want || res.DroppedTurns != tc.turns || res.DroppedMessages != tc.msgs {
				t.Fatalf("got %s, %d turns, %d messages, err %v\nwant %s, %d, %d", res.Body, res.DroppedTurns, res.DroppedMessages, err, tc.want, tc.turns, tc.msgs)
			}
		})
	}
}

func TestAssembleProbeFailures(t *testing.T) {
	body := []byte(`{"messages":[{"role":"user","content":"a"},{"role":"user","content":"b"}]}`)
	for _, tc := range []struct {
		err    error
		reason string
	}{
		{errors.New("status 500"), ReasonProbeFailed},
		{fmt.Errorf("probe: %w", context.DeadlineExceeded), ReasonProbeTimeout},
		{&SkipError{Reason: ReasonNoReplica, Err: errors.New("none")}, ReasonNoReplica},
	} {
		q, ix, _ := parseIndexed(body, 0)
		_, err := q.Assemble(context.Background(), ix, 1, 12, &fakeCounter{err: tc.err})
		var se *SkipError
		if !errors.As(err, &se) || se.Reason != tc.reason {
			t.Fatalf("%v: err = %v, want reason %s", tc.err, err, tc.reason)
		}
	}
}

func TestParseRejects(t *testing.T) {
	for body, reason := range map[string]string{
		`not json`: telemetry.ReasonUnparseableBody, `[]`: telemetry.ReasonUnparseableBody, `{"model":"m"}`: telemetry.ReasonUnparseableBody,
		`{"messages":{}}`: telemetry.ReasonUnparseableBody, `{"messages":["hi"]}`: telemetry.ReasonUnparseableBody,
		`{"messages":[{"content":"no role"}]}`: telemetry.ReasonUnparseableBody, `{"messages":[{"role":1}]}`: telemetry.ReasonUnparseableBody,
		// Members that change the render but that /tokenize does not take.
		`{"documents":[{"text":"d"}],"messages":[]}`: ReasonUnmeasurable,
		`{"messages":[],"reasoning_effort":"high"}`:  ReasonUnmeasurable,
		`{"documents":[],"messages":"not an array"}`: ReasonUnmeasurable, // checked before the messages are
		// Media: every probe would make the replica process it.
		`{"messages":[{"role":"user","content":[{"type":"text","text":"a"},{"type":"image_url","image_url":{"url":"x"}}]}]}`: ReasonMultimodal,
		`{"messages":[{"role":"user","content":[{"type":"input_audio","input_audio":{}}]}]}`:                                 ReasonMultimodal,
		`{"messages":[{"content":[{"type":"video_url"}],"role":"user"}]}`:                                                    ReasonMultimodal,
	} {
		var se *SkipError
		if _, err := parseBody([]byte(body), 0); !errors.As(err, &se) || se.Reason != reason {
			t.Errorf("Parse(%s) = %v, want reason %s", body, err, reason)
		}
	}
	// Text parts only, an escaped key read by its name, and the last
	// "messages" winning are all fine.
	q, err := parseBody([]byte(`{"messages":"old","m\u0065ssages":[{"r\u006fle":"user","content":[{"type":"text","text":"a"}]}]}`), 0)
	if err != nil || len(q.elems) != 1 {
		t.Fatalf("escaped keys: %v", err)
	}
}

// A matched template's static prefix (few-shot pairs of any role, and the
// message its first variable is in) is never dropped.
func TestAssembleKeepsTemplatePrefix(t *testing.T) {
	sys, shotQ, shotA := `{"role":"system","content":"s"}`, `{"role":"user","content":"example q"}`, `{"role":"assistant","content":"example a"}`
	old, oldA, last := `{"role":"user","content":"old question"}`, `{"role":"assistant","content":"old answer"}`, `{"role":"user","content":"now"}`
	body := `{"messages":[` + strings.Join([]string{sys, shotQ, shotA, old, oldA, last}, ",") + `]}`
	q, ix, err := parseIndexed([]byte(body), 3)
	if err != nil {
		t.Fatal(err)
	}
	minimum := 0
	for _, m := range []string{sys, shotQ, shotA, last} {
		minimum += msgCost([]byte(m))
	}
	res, err := q.Assemble(context.Background(), ix, minimum, 12, &fakeCounter{})
	if want := `{"messages":[` + strings.Join([]string{sys, shotQ, shotA, last}, ",") + `]}`; err != nil || string(res.Body) != want || res.DroppedMessages != 2 || res.DroppedTurns != 1 {
		t.Fatalf("got %s (%d messages, %d turns), err %v\nwant %s", res.Body, res.DroppedMessages, res.DroppedTurns, err, want)
	}
	q, ix, _ = parseIndexed([]byte(body), 3) // the first call spliced ix
	if _, err := q.Assemble(context.Background(), ix, minimum-1, 12, &fakeCounter{}); !errors.Is(err, ErrOverBudget) {
		t.Fatalf("few-shot prefix plus last turn over budget: err = %v, want ErrOverBudget", err)
	}
}

// TestAssembleSplicesThroughTheIndex (AD-15): a cut is spliced through the
// request's index, so the index then reads the body that is sent, with
// offsets fresh for the splices after assembly; nothing dropped leaves the
// index as it was.
func TestAssembleSplicesThroughTheIndex(t *testing.T) {
	u1, a1, u2 := `{"role":"user","content":"one"}`, `{"role":"assistant","content":"a"}`, `{"role":"user","content":"two"}`
	body := `{"model":"m", "messages" : [` + u1 + `,` + a1 + `,` + u2 + `] ,"priority":3}`
	q, ix, err := parseIndexed([]byte(body), 0)
	if err != nil {
		t.Fatal(err)
	}
	res, err := q.Assemble(context.Background(), ix, msgCost([]byte(u2)), 12, &fakeCounter{})
	want := `{"model":"m", "messages" : [` + u2 + `] ,"priority":3}`
	if err != nil || string(res.Body) != want || !bytes.Equal(ix.Body(), res.Body) {
		t.Fatalf("got %s (index holds %s), err %v; want %s in both", res.Body, ix.Body(), err, want)
	}
	var fresh bodyindex.Index
	if err := fresh.Read(res.Body); err != nil || fresh.Len() != ix.Len() {
		t.Fatalf("fresh read: %v, %d members; the index has %d", err, fresh.Len(), ix.Len())
	}
	for i := range ix.Len() {
		if ix.Member(i) != fresh.Member(i) {
			t.Fatalf("member %d = %+v after the splice, a fresh read gives %+v", i, ix.Member(i), fresh.Member(i))
		}
	}
	q, ix, _ = parseIndexed([]byte(body), 0)
	before := ix.Member(2)
	if res, err := q.Assemble(context.Background(), ix, 1<<20, 12, &fakeCounter{}); err != nil || string(res.Body) != body || ix.Member(2) != before {
		t.Fatalf("nothing dropped: body %s, err %v, index moved %v", res.Body, err, ix.Member(2) != before)
	}
}

// TestParsePastTheMemberCeiling (E5): a body of more top-level members than
// the index holds is valid JSON the replica serves, but it is not
// assembled: Parse skips it as too_many_members, and the request goes on
// unchanged.
func TestParsePastTheMemberCeiling(t *testing.T) {
	body := `{"messages":[{"role":"user","content":"a"}]` + strings.Repeat(`,"x":0`, bodyindex.MaxMembers) + `}`
	var se *SkipError
	if _, err := parseBody([]byte(body), 0); !errors.As(err, &se) || se.Reason != telemetry.ReasonTooManyMembers {
		t.Fatalf("Parse = %v, want reason %s", err, telemetry.ReasonTooManyMembers)
	}
	var never bodyindex.Index
	if _, err := Parse(&never, 0); !errors.As(err, &se) || se.Reason != telemetry.ReasonUnparseableBody {
		t.Fatalf("Parse of an Index never read = %v, want reason %s", err, telemetry.ReasonUnparseableBody)
	}
}

// A template prefix that ends on a user message keeps that turn's reply:
// the turn is never split.
func TestTemplatePrefixEndingInATurnKeepsTheTurn(t *testing.T) {
	sys, shotQ, shotA := `{"role":"system","content":"s"}`, `{"role":"user","content":"example q"}`, `{"role":"assistant","content":"example a"}`
	old, oldA, last := `{"role":"user","content":"old question"}`, `{"role":"assistant","content":"old answer"}`, `{"role":"user","content":"now"}`
	body := `{"messages":[` + strings.Join([]string{sys, shotQ, shotA, old, oldA, last}, ",") + `]}`
	q, ix, err := parseIndexed([]byte(body), 2) // the prefix ends on shotQ
	if err != nil {
		t.Fatal(err)
	}
	minimum := 0
	for _, m := range []string{sys, shotQ, shotA, last} {
		minimum += msgCost([]byte(m))
	}
	res, err := q.Assemble(context.Background(), ix, minimum, 12, &fakeCounter{})
	if want := `{"messages":[` + strings.Join([]string{sys, shotQ, shotA, last}, ",") + `]}`; err != nil || string(res.Body) != want || res.DroppedTurns != 1 {
		t.Fatalf("got %s (%d turns dropped), err %v\nwant %s", res.Body, res.DroppedTurns, err, want)
	}
}

func TestTokenizeBody(t *testing.T) {
	q, _ := parseBody([]byte(`{"model":"m","temperature":1,"tools":[{"type":"function"}],"mm_processor_kwargs":{"a":1},"messages":[]}`), 0)
	if got, want := string(q.TokenizeBody([]byte(`[1]`))), `{"messages":[1],"model":"m","tools":[{"type":"function"}],"mm_processor_kwargs":{"a":1},"add_generation_prompt":true}`; got != want {
		t.Fatalf("got %s\nwant %s", got, want)
	}
	q, _ = parseBody([]byte(`{"model":"m","add_generation_prompt":false,"messages":[]}`), 0)
	if got := string(q.TokenizeBody([]byte(`[]`))); got != `{"messages":[],"model":"m","add_generation_prompt":false}` {
		t.Fatalf("got %s", got)
	}
	// Every member that changes the chat render (vLLM's tokenize request
	// takes them all) is carried byte for byte; sampling members are not.
	render := [][2]string{
		{"model", `"m"`},
		{"tools", `[{"type":"function","function":{"name":"f"}}]`},
		{"chat_template", `"{{ messages }}"`},
		{"chat_template_kwargs", `{"enable_thinking": false}`},
		{"add_generation_prompt", `false`},
		{"continue_final_message", `true`},
		{"add_special_tokens", `true`},
		{"mm_processor_kwargs", `{"a":1}`},
	}
	body, want := `{"temperature":0,"stream":true,"max_tokens":9,"messages":[]`, `{"messages":[2]`
	for i := len(render) - 1; i >= 0; i-- {
		body += fmt.Sprintf(`,%q: %s`, render[i][0], render[i][1])
	}
	for _, m := range render {
		want += fmt.Sprintf(`,%q:%s`, m[0], m[1])
	}
	q, err := parseBody([]byte(body+"}"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(q.TokenizeBody([]byte(`[2]`))); got != want+"}" {
		t.Fatalf("got  %s\nwant %s}", got, want)
	}
	// A repeated member counts as its last value, as vLLM's parser reads it.
	q, err = parseBody([]byte(`{"model":"a","add_generation_prompt":false,"messages":[],"model":"b","add_generation_prompt":true}`), 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(q.TokenizeBody([]byte(`[]`))); got != `{"messages":[],"model":"b","add_generation_prompt":true}` {
		t.Fatalf("got %s", got)
	}
}

// fakeSender answers /tokenize per replica name.
type fakeSender struct {
	calls  []string
	answer func(name string) (*http.Response, error)
}

func (s *fakeSender) Do(_ context.Context, rep discovery.Replica, _ upstream.Request) (*http.Response, error) {
	s.calls = append(s.calls, rep.Name)
	return s.answer(rep.Name)
}

func TestTokenizerReplicas(t *testing.T) {
	reps := []discovery.Replica{{Name: "a"}, {Name: "b"}}
	q, _ := parseBody([]byte(`{"messages":[]}`), 0)
	answer := func(down, slow string) func(string) (*http.Response, error) {
		return func(name string) (*http.Response, error) {
			switch name {
			case down:
				return nil, &upstream.Error{Kind: upstream.KindConnect, Replica: name, Err: errors.New("refused")}
			case slow:
				return nil, &upstream.Error{Kind: upstream.KindTimeout, Replica: name, Err: errors.New("slow")}
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"count":7,"tokens":[1,2,3,4,5,6,7]}`))}, nil
		}
	}
	// A replica that refuses connections is passed over, and the one that
	// answered takes the request's later probes.
	s := &fakeSender{answer: answer("a", "")}
	c := NewTokenizer(s, time.Second).Counter(nil, reps, q)
	for range 2 {
		if n, err := c.Count(context.Background(), []byte(`[]`)); n != 7 || err != nil {
			t.Fatalf("count = %d, %v", n, err)
		}
	}
	if got := strings.Join(s.calls, ","); got != "a,b,b" && got != "b,b" {
		t.Fatalf("calls %s", got)
	}
	// Healthy replicas are tried first; an ejected one only when no healthy
	// one could be reached.
	for range 3 {
		s := &fakeSender{answer: answer("", "")}
		if _, err := NewTokenizer(s, time.Second).Counter(reps[1:], reps, q).Count(context.Background(), []byte(`[]`)); err != nil || strings.Join(s.calls, ",") != "b" {
			t.Fatalf("healthy b: calls %v, err %v", s.calls, err)
		}
	}
	s = &fakeSender{answer: answer("b", "")}
	if _, err := NewTokenizer(s, time.Second).Counter(reps[1:], reps, q).Count(context.Background(), []byte(`[]`)); err != nil || strings.Join(s.calls, ",") != "b,a" {
		t.Fatalf("healthy b refused: calls %v, err %v", s.calls, err)
	}
	for _, tc := range []struct {
		reps   []discovery.Replica
		down   string
		reason string
	}{
		{nil, "", ReasonNoReplica},
		{reps[:1], "a", ReasonNoReplica},
		{reps[:1], "", ReasonProbeTimeout},
	} {
		slow := ""
		if tc.reason == ReasonProbeTimeout {
			slow = "a"
		}
		c := NewTokenizer(&fakeSender{answer: answer(tc.down, slow)}, time.Second).Counter(nil, tc.reps, q)
		var se *SkipError
		if _, err := c.Count(context.Background(), []byte(`[]`)); !errors.As(err, &se) || se.Reason != tc.reason {
			t.Fatalf("%+v: err = %v", tc, err)
		}
	}
}
