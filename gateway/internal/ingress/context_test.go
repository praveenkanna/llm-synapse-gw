package ingress

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/dispatch"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/mockllm"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

const contextOn = "context: {enabled: true, max_probes: 12, tokenize_timeout: 2s, max_body_bytes: 2048}\n"

// A history of a system message and three turns, the second with a tool
// call and its result. The replicas count it with mockllm's stand-in
// tokenizer, through /tokenize.
var (
	ctxSys   = `{"role":"system","content":"You are terse."}`
	ctxTurns = [][]string{
		{`{"role":"user","content":"first question, quite a long one to make it count"}`, `{"role":"assistant","content":"first answer"}`},
		{`{"role":"user","content":"look it up"}`, `{"role":"assistant","content":null,"tool_calls":[{"id":"c1","type":"function","function":{"name":"f","arguments":"{}"}}]}`,
			`{"role":"tool","tool_call_id":"c1","content":"result"}`, `{"role":"assistant","content":"found it"}`},
		{`{"role":"user","content":"last question"}`},
	}
)

// ctxBody is a chat body with the system message and the given turns.
func ctxBody(turns ...[]string) string {
	msgs := []string{ctxSys}
	for _, t := range turns {
		msgs = append(msgs, t...)
	}
	return `{"model":"large-model","messages":[` + strings.Join(msgs, ", ") + `],"max_tokens":4}`
}

// promptTokens is the stand-in count of body's messages.
func promptTokens(t *testing.T, body string) int {
	t.Helper()
	var req struct{ Messages []mockllm.Message }
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatal(err)
	}
	return mockllm.CountChat(req.Messages, true)
}

func (f *dedupFixture) generations() []mockllm.RecordedRequest {
	return append(f.large0.Mock().Requests(), f.large1.Mock().Requests()...)
}

// AC-M4-6 through the handler: the oldest turns are cut so the prompt the
// replica counts fits the budget, the replica gets the caller's bytes minus
// the dropped messages, and the drops are reported.
func TestContextBudgetAssembly(t *testing.T) {
	full := ctxBody(ctxTurns...)
	fitsLast := ctxBody(ctxTurns[2])
	fitsTwo := ctxBody(ctxTurns[1:]...)
	cases := []struct {
		name, budget, want string
		msgs, turns        int
	}{
		{"fits whole", strconv.Itoa(promptTokens(t, full)), full, 0, 0},
		{"keeps last two turns", strconv.Itoa(promptTokens(t, fitsTwo)), fitsTwo, 2, 1},
		{"keeps last turn", strconv.Itoa(promptTokens(t, fitsTwo) - 1), fitsLast, 6, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newDedupFixtureWith(t, contextOn, 0, 1<<20)
			a := f.ask(context.Background(), full, map[string]string{HeaderContextBudget: tc.budget})
			if a.err != nil || a.status != http.StatusOK {
				t.Fatalf("status %d, err %v: %s", a.status, a.err, a.body)
			}
			if a.header.Get(HeaderContextDroppedMessages) != strconv.Itoa(tc.msgs) || a.header.Get(HeaderContextDroppedTurns) != strconv.Itoa(tc.turns) || a.header.Get(HeaderContextAssembly) != "" {
				t.Fatalf("headers %v, want %d messages and %d turns dropped", a.header, tc.msgs, tc.turns)
			}
			gens := f.generations()
			if len(gens) != 1 || string(gens[0].Body) != tc.want {
				t.Fatalf("upstream got %d requests, body %s\nwant %s", len(gens), gens[0].Body, tc.want)
			}
			var resp struct {
				Usage struct {
					PromptTokens int `json:"prompt_tokens"`
				}
			}
			budget, _ := strconv.Atoi(tc.budget)
			if json.Unmarshal(a.body, &resp) != nil || resp.Usage.PromptTokens > budget {
				t.Fatalf("prompt_tokens %d over the budget %d", resp.Usage.PromptTokens, budget)
			}
			outcome := telemetry.ContextTrimmed
			if tc.turns == 0 {
				outcome = telemetry.ContextUnchanged
			}
			eventually(t, "assembly counted", func() bool {
				return f.metric(fmt.Sprintf(`%s{outcome=%q}`, telemetry.MetricContextAssembly, outcome)) == 1 &&
					f.metric(telemetry.MetricContextDroppedMessages) == float64(tc.msgs)
			})
		})
	}
}

// TestPrioritySplicedAfterTheHistoryCut (AD-15): the history cut is
// spliced through the request's index, so the priority splice after it
// reads the trimmed body's offsets (a "priority" after the messages moves
// with the cut), with no second read of the body.
func TestPrioritySplicedAfterTheHistoryCut(t *testing.T) {
	f := newDedupFixtureWith(t, contextOn, 0, 1<<20)
	f.h.priorityPools["large"] = true // as pools[].priority_scheduling: true, verified
	f.h.priorityVerified = func(string) bool { return true }
	if err := f.h.UpdatePriority(func() config.PriorityConfig {
		p := f.h.Priority()
		p.UpstreamValues = map[string]int{config.PriorityInteractive: -10, config.PriorityUser: 0, config.PriorityBackground: 10}
		return p
	}()); err != nil {
		t.Fatal(err)
	}
	full, last := ctxBody(ctxTurns...), ctxBody(ctxTurns[2])
	body := strings.TrimSuffix(full, "}") + `,"priority":3}`
	budget := strconv.Itoa(promptTokens(t, last))
	for class, want := range map[string]string{
		config.PriorityBackground: `{"priority":10,` + last[1:], // replaced: the caller's removed, the class's first
		config.PriorityUser:       last,                         // the class sends none: removed
	} {
		f.large0.Mock().ResetRequests()
		f.large1.Mock().ResetRequests()
		f.ask(context.Background(), body, map[string]string{HeaderContextBudget: budget, HeaderPriority: class})
		if gens := f.generations(); len(gens) != 1 || string(gens[0].Body) != want {
			t.Fatalf("%s: %d requests, body %s\nwant %s", class, len(gens), reqsBody(gens), want)
		}
	}
}

// bodySpy is a router that records what each request gives affinity to
// read.
type bodySpy struct {
	router.Router
	mu     sync.Mutex
	bodies []string
}

func (s *bodySpy) Route(ctx context.Context, r *http.Request, pool string) (router.Lease, error) {
	s.mu.Lock()
	s.bodies = append(s.bodies, string(router.RequestBody(ctx)))
	s.mu.Unlock()
	return s.Router.Route(ctx, r, pool)
}

// TestAffinityReadsTheBodyAfterTheCut: the affinity key scan keeps reading
// what it read before the index (ADR 0017): the caller's bytes as assembly
// left them, so a request is keyed by the history it sends, and from
// before the gateway's own priority and max_tokens splices, so they never
// move it to another replica.
func TestAffinityReadsTheBodyAfterTheCut(t *testing.T) {
	f := newDedupFixtureWith(t, contextOn, 0, 1<<20)
	spy := &bodySpy{Router: router.NewRoundRobin(f.h.disc, []string{"large", "small"})}
	up := config.Defaults().Upstream
	client := upstream.New(up)
	t.Cleanup(client.CloseIdleConnections)
	f.h.dispatch = dispatch.New(spy, client, up, dispatch.Options{})

	withPriority := func(body string) string { return strings.TrimSuffix(body, "}") + `,"priority":3}` }
	full, last := ctxBody(ctxTurns...), ctxBody(ctxTurns[2])
	a := f.ask(context.Background(), withPriority(full), map[string]string{HeaderContextBudget: strconv.Itoa(promptTokens(t, last))})
	if a.err != nil || a.status != http.StatusOK {
		t.Fatalf("status %d, err %v: %s", a.status, a.err, a.body)
	}
	if len(spy.bodies) != 1 || spy.bodies[0] != withPriority(last) {
		t.Fatalf("affinity read %q\nwant the body after the cut, before the priority splice: %s", spy.bodies, withPriority(last))
	}
	if gens := f.generations(); len(gens) != 1 || string(gens[0].Body) != last {
		t.Fatalf("%d requests, body %s\nwant the priority stripped: %s", len(gens), reqsBody(gens), last)
	}
}

func TestContextBudgetOverMinimumAnswers422(t *testing.T) {
	f := newDedupFixtureWith(t, contextOn, 0, 1<<20)
	budget := promptTokens(t, ctxBody(ctxTurns[2])) - 1
	a := f.ask(context.Background(), ctxBody(ctxTurns...), map[string]string{HeaderContextBudget: strconv.Itoa(budget)})
	var e errorBody
	if a.status != http.StatusUnprocessableEntity || json.Unmarshal(a.body, &e) != nil || e.Error.Code != CodeContextBudgetExceeded {
		t.Fatalf("status %d body %s, want 422 %s", a.status, a.body, CodeContextBudgetExceeded)
	}
	if n := f.upstreamRequests(); n != 0 {
		t.Fatalf("replicas ran %d generations for a request over its budget", n)
	}
}

// A request whose assembly cannot run is forwarded unchanged, marked
// skipped, with the reason counted. An invalid budget is one of those, not
// a 400: raw vLLM ignores the header.
func TestContextBudgetSkipped(t *testing.T) {
	big := `{"model":"large-model","messages":[` + ctxSys + `,{"role":"user","content":"` + strings.Repeat("x", 2048) + `"}]}`
	cases := []struct {
		name, yaml, path, body, budget, reason string
	}{
		{"invalid budget", contextOn, PathChatCompletions, ctxBody(ctxTurns...), "lots", ReasonInvalidBudget},
		{"zero budget", contextOn, PathChatCompletions, ctxBody(ctxTurns...), "0", ReasonInvalidBudget},
		{"body too large", contextOn, PathChatCompletions, big, "10", telemetry.ReasonBodyTooLarge},
		{"body too large wins over too many members", contextOn, PathChatCompletions, `{"model":"large-model","messages":[` + ctxSys + `]` + filler + `}`, "10", telemetry.ReasonBodyTooLarge},
		{"completions", contextOn, PathCompletions, `{"model":"large-model","prompt":"hi"}`, "10", ReasonNotChat},
		{"no messages", contextOn, PathChatCompletions, `{"model":"large-model","messages":"hi"}`, "10", "unparseable_body"},
		{"probe timeout", "context: {enabled: true, tokenize_timeout: 1ns}\n", PathChatCompletions, ctxBody(ctxTurns...), "10", "probe_timeout"},
		{"probe failed", contextOn, PathChatCompletions, strings.Replace(ctxBody(ctxTurns...), "large-model", "nope-model", 1), "10", "probe_failed"},
		{"assemble timeout", "context: {enabled: true, tokenize_timeout: 1ns, assemble_timeout: 1ns}\n", PathChatCompletions, ctxBody(ctxTurns...), "10", ReasonAssembleTimeout},
		{"unmeasurable", contextOn, PathChatCompletions, strings.Replace(ctxBody(ctxTurns...), `"max_tokens"`, `"reasoning_effort":"low","max_tokens"`, 1), "10", "unmeasurable"},
		{"multimodal", contextOn, PathChatCompletions, `{"model":"large-model","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:,"}}]}]}`, "10", "multimodal"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newDedupFixtureWith(t, tc.yaml, 0, 1<<20)
			resp, _ := f.post(t, tc.path, []byte(tc.body), map[string]string{HeaderContextBudget: tc.budget})
			if resp.Header.Get(HeaderContextAssembly) != contextSkipped || resp.Header.Get(HeaderContextDroppedMessages) != "" {
				t.Fatalf("headers %v, want %s: skipped", resp.Header, HeaderContextAssembly)
			}
			gens := f.generations()
			if len(gens) != 1 || string(gens[0].Body) != tc.body {
				t.Fatalf("upstream got %d requests, want the caller's body unchanged", len(gens))
			}
			series := fmt.Sprintf(`%s{stage=%q,reason=%q}`, telemetry.MetricFailOpen, telemetry.StageContext, tc.reason)
			eventually(t, series, func() bool { return f.metric(series) == 1 })
		})
	}
}

func TestContextBudgetOffIgnoresHeader(t *testing.T) {
	f := newDedupFixtureWith(t, "", 0, 1<<20)
	body := ctxBody(ctxTurns...)
	resp, _ := f.post(t, PathChatCompletions, []byte(body), map[string]string{HeaderContextBudget: "1"})
	if resp.StatusCode != http.StatusOK || resp.Header.Get(HeaderContextAssembly) != "" || resp.Header.Get(HeaderContextDroppedTurns) != "" {
		t.Fatalf("status %d headers %v: the header should be ignored", resp.StatusCode, resp.Header)
	}
	if gens := f.generations(); len(gens) != 1 || string(gens[0].Body) != body {
		t.Fatal("body changed with context assembly off")
	}
}

// The budget is part of a flight's key: requests that differ only in their
// budget never merge, and a follower gets its leader's X-Context-* headers.
func TestContextBudgetInCoalescingKey(t *testing.T) {
	f := newDedupFixtureWith(t, contextOn, 100, 1<<20)
	body := faqBody("where is my order?", false, "")
	ask := func(budget string) chan answer {
		hdr := faqHeaders("faq", 20)
		hdr[HeaderContextBudget] = budget
		ch := make(chan answer, 1)
		go func() { ch <- f.ask(context.Background(), body, hdr) }()
		return ch
	}
	leader := ask("1000")
	eventually(t, "the leader's flight", func() bool { return f.group.Stats().Flights == 1 })
	follower, other := ask("1000"), ask("999")
	answers := []answer{<-leader, <-follower, <-other}
	if n := f.upstreamRequests(); n != 2 {
		t.Fatalf("replicas ran %d generations, want 2: one per budget", n)
	}
	if s := f.group.Stats(); s.Led != 2 || s.Joined != 1 {
		t.Fatalf("flights led %d joined %d, want 2 and 1", s.Led, s.Joined)
	}
	for i, a := range answers {
		if a.status != http.StatusOK || a.header.Get(HeaderContextDroppedMessages) != "0" || a.header.Get(HeaderContextDroppedTurns) != "0" {
			t.Fatalf("answer %d: status %d headers %v", i, a.status, a.header)
		}
	}
}

// fewShotSpec opens with a system message and a few-shot pair, all
// static, before the caller's question.
const fewShotSpec = `{"model":"large-model","variables":[{"name":"q"}],
 "messages":[{"role":"system","segments":[{"kind":"static","text":"You are terse."}]},
             {"role":"user","segments":[{"kind":"static","text":"example question"}]},
             {"role":"assistant","segments":[{"kind":"static","text":"example answer"}]},
             {"role":"user","segments":[{"kind":"volatile","text":"{{q}}"}]}]}`

// A matched template's static prefix, few-shot pair included, and the
// message holding its variable are never dropped: the history after them
// goes first, and a prefix plus last turn over budget answers 422.
func TestContextBudgetKeepsTemplatePrefix(t *testing.T) {
	prefix := []string{ctxSys, `{"role":"user","content":"example question"}`, `{"role":"assistant","content":"example answer"}`, `{"role":"user","content":"q1"}`}
	history := []string{`{"role":"assistant","content":"a1"}`, `{"role":"user","content":"q2"}`, `{"role":"assistant","content":"a2"}`}
	last := `{"role":"user","content":"now"}`
	body := func(msgs ...string) string {
		return `{"model":"large-model","messages":[` + strings.Join(msgs, ", ") + `]}`
	}
	full := body(append(append(prefix[:4:4], history...), last)...)
	kept := body(append(prefix[:4:4], history[0], last)...) // a1 answers q1: the template's turn is never split
	hdr := func(budget int) map[string]string {
		return map[string]string{templates.HeaderTemplateID: "fewshot", templates.HeaderTemplateVersion: "1", HeaderContextBudget: strconv.Itoa(budget)}
	}

	f := newDedupFixtureWith(t, contextOn, 0, 1<<20)
	a := f.ask(context.Background(), full, hdr(promptTokens(t, kept)))
	if a.status != http.StatusOK || a.header.Get(HeaderContextDroppedMessages) != "2" || a.header.Get(HeaderContextDroppedTurns) != "1" {
		t.Fatalf("status %d headers %v: %s", a.status, a.header, a.body)
	}
	if gens := f.generations(); len(gens) != 1 || string(gens[0].Body) != kept {
		t.Fatalf("upstream body %s\nwant %s", gens[0].Body, kept)
	}
	if a := f.ask(context.Background(), full, hdr(promptTokens(t, kept)-1)); a.status != http.StatusUnprocessableEntity {
		t.Fatalf("prefix plus last turn over budget: status %d, want 422", a.status)
	}
}

// With too few probes to prove the largest fit, the suffix proven to fit
// is sent, marked approximate: never the whole over-budget prompt.
func TestContextBudgetApproximate(t *testing.T) {
	f := newDedupFixtureWith(t, "context: {enabled: true, max_probes: 2}\n", 0, 1<<20)
	// Probes: all three turns (over), one turn (fits); proving two would
	// take a third probe.
	budget := promptTokens(t, ctxBody(ctxTurns[1:]...))
	a := f.ask(context.Background(), ctxBody(ctxTurns...), map[string]string{HeaderContextBudget: strconv.Itoa(budget)})
	if a.status != http.StatusOK || a.header.Get(HeaderContextAssembly) != contextApproximate ||
		a.header.Get(HeaderContextDroppedMessages) != "6" || a.header.Get(HeaderContextDroppedTurns) != "2" {
		t.Fatalf("status %d headers %v", a.status, a.header)
	}
	if gens := f.generations(); len(gens) != 1 || string(gens[0].Body) != ctxBody(ctxTurns[2]) {
		t.Fatalf("upstream body %s", gens[0].Body)
	}
	series := fmt.Sprintf(`%s{outcome=%q}`, telemetry.MetricContextAssembly, telemetry.ContextApproximate)
	eventually(t, series, func() bool { return f.metric(series) == 1 })
}

// An invalid budget is skipped without turning the cache off; a valid one
// bypasses it.
func TestContextBudgetAndCache(t *testing.T) {
	f, cache := newCacheFixture(t, contextOn, 0)
	body := faqBody("where is my order?", false, "")
	ask := func(budget string) answer {
		hdr := faqHeaders("kb", 5)
		hdr[HeaderContextBudget] = budget
		return f.ask(context.Background(), body, hdr)
	}
	if a := ask("lots"); a.status != http.StatusOK || a.header.Get(HeaderContextAssembly) != contextSkipped {
		t.Fatalf("invalid budget: status %d headers %v", a.status, a.header)
	}
	eventually(t, "the store", func() bool { n, _ := cache.stored(); return n == 1 })
	if a := ask("lots"); a.header.Get(HeaderCache) != "hit" || a.header.Get(HeaderContextAssembly) != contextSkipped {
		t.Fatalf("invalid budget again: headers %v, want a cache hit marked skipped", a.header)
	}
	lookups := cache.lookups()
	if a := ask("1000"); a.status != http.StatusOK || a.header.Get(HeaderCache) != "" || a.header.Get(HeaderContextDroppedMessages) != "0" {
		t.Fatalf("valid budget: status %d headers %v", a.status, a.header)
	}
	if cache.lookups() != lookups {
		t.Fatal("a budgeted request looked up the cache")
	}
}
