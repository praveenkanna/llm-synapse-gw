package ingress

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex/bodyindextest"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/mockllm"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates"
)

// filler is MaxMembers top-level members, each preceded by its comma: with
// them a body is past the index's ceiling.
var filler = func() string {
	var b strings.Builder
	for i := range bodyindex.MaxMembers {
		b.WriteString(`,"x` + strconv.Itoa(i) + `":` + strconv.Itoa(i%7))
	}
	return b.String()
}()

// TestBodyPastTheIndexCeiling (E5): a body of more
// top-level members than bodyindex.MaxMembers is valid JSON the replica
// serves, so it keeps today's behaviour. It is routed by its model, and
// its priority is stripped, replaced, set or refused as any body's is
// (ADR 0018: no well-formed body is refused); every other byte goes
// upstream as the caller sent it.
func TestBodyPastTheIndexCeiling(t *testing.T) {
	f := newFixtureWith(t, upstreamValuesYAML, Options{PriorityVerified: func(string) bool { return true }},
		func(c *mockllm.Config) { c.SchedulingPolicy = mockllm.PolicyPriority })
	f.h.priorityPools["large"] = true // as pools[].priority_scheduling: true
	msgs := `"messages":[{"role":"user","content":"hi"}]`
	for _, tc := range []struct {
		name, class, body string
		pool              string // where it must arrive; "" when it is refused
		want              string // the body that arrives
	}{
		{"routed by its model", "", `{"model":"small-model",` + msgs + filler + `}`, "small", ""},
		{"routed by its last model", "", `{"model":"large-model",` + msgs + filler + `,"model":"small-model"}`, "small", ""},
		{"a non-zero priority stripped off an unflagged pool", "", `{"model":"small-model",` + msgs + filler + `,"priority":5}`,
			"small", `{"model":"small-model",` + msgs + filler + `}`},
		{"the first member stripped off an unflagged pool", "", `{"priority":5,"model":"small-model",` + msgs + filler + `}`,
			"small", `{"model":"small-model",` + msgs + filler + `}`},
		{"a priority vLLM reads as zero kept (E4)", "", `{"model":"small-model",` + msgs + filler + `,"priority":1e-400}`, "small", ""},
		{"an escaped priority stripped: it is the one vLLM reads", "", `{"model":"small-model",` + msgs + filler + `,"` + bodyindextest.Escaped("priority") + `":5}`,
			"small", `{"model":"small-model",` + msgs + filler + `}`},
		{"the class's priority set on a flagged pool", config.PriorityBackground, `{"model":"large-model",` + msgs + filler + `}`,
			"large", `{"priority":10,"model":"large-model",` + msgs + filler + `}`},
		{"the class's priority set inside the object, past leading whitespace", config.PriorityBackground, " \n" + `{"model":"large-model",` + msgs + filler + `}`,
			"large", " \n" + `{"priority":10,"model":"large-model",` + msgs + filler + `}`},
		{"a caller's priority replaced on a flagged pool", config.PriorityBackground, `{"model":"large-model",` + msgs + filler + `,"priority":3}`,
			"large", `{"priority":10,"model":"large-model",` + msgs + filler + `}`},
		{"an escaped caller's priority replaced on a flagged pool", config.PriorityBackground, `{"model":"large-model",` + msgs + filler + `,"` + bodyindextest.Escaped("priority") + `":3}`,
			"large", `{"priority":10,"model":"large-model",` + msgs + filler + `}`},
		{"a caller's priority stripped when the class sends none", config.PriorityUser, `{"model":"large-model","priority":-1000,` + msgs + filler + `}`,
			"large", `{"model":"large-model",` + msgs + filler + `}`},
		{"two priorities refused on a flagged pool", config.PriorityBackground, `{"model":"large-model","priority":1,` + msgs + filler + `,"priority":2}`, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, s := range []*mockllm.Server{f.large0, f.large1, f.small} {
				s.Mock().ResetRequests()
			}
			hdr := map[string]string{mockllm.HeaderOutputTokens: "1"}
			if tc.class != "" {
				hdr[HeaderPriority] = tc.class
			}
			resp, got := f.post(t, PathChatCompletions, []byte(tc.body), hdr)
			if tc.pool == "" {
				if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(got), CodeDuplicatePriority) {
					t.Fatalf("status %d (%.80s), want a 400 %s", resp.StatusCode, got, CodeDuplicatePriority)
				}
				return
			}
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status %d (%.120s), want the replica to serve it", resp.StatusCode, got)
			}
			reqs := f.small.Mock().Requests()
			if tc.pool == "large" {
				reqs = append(f.large0.Mock().Requests(), f.large1.Mock().Requests()...)
			}
			want := tc.want
			if want == "" {
				want = tc.body
			}
			if len(reqs) != 1 || string(reqs[0].Body) != want {
				t.Fatalf("status %d; %d requests reached %s, body %.120q..., want %.120q...", resp.StatusCode, len(reqs), tc.pool, reqsBody(reqs), want)
			}
		})
	}
	if o := f.outcomes(t); o.UnknownModel+o.UnparseableBody != 0 {
		t.Fatalf("%+v: a body past the ceiling failed open to the default pool; each is routed by its model", o)
	}
}

// TestBodyPastTheIndexCeilingGetsTheGatewayCap: a matched template's body
// past the ceiling gets the gateway's max_tokens like any other (ADR
// 0007), spliced as bytes at the object's '{'.
func TestBodyPastTheIndexCeilingGetsTheGatewayCap(t *testing.T) {
	f := newTemplateFixture(t, 64, "", Options{}, nil)
	body := `{"model":"large-model",` + tmplMessages + filler + `}`
	resp, got := f.post(t, PathChatCompletions, []byte(body), map[string]string{
		templates.HeaderTemplateID: "support", templates.HeaderTemplateVersion: "2", mockllm.HeaderOutputTokens: "1"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %.120s", resp.StatusCode, got)
	}
	if bodies := f.upstreamBodies(); len(bodies) != 1 || bodies[0] != `{"max_tokens":64,`+body[1:] {
		t.Fatalf("upstream %d bodies, first %.120q..., want max_tokens first and the rest unchanged", len(bodies), strings.Join(bodies, ""))
	}
}

// TestBodyPastTheIndexCeilingKeepsTheCallersCap: a caller's own cap past
// the ceiling is still found, so the gateway sets none (ADR 0007).
func TestBodyPastTheIndexCeilingKeepsTheCallersCap(t *testing.T) {
	for _, member := range []string{`"max_tokens":8`, `"max_completion_tokens":8`, `"` + bodyindextest.Escaped("max_tokens") + `":8`} {
		f := newTemplateFixture(t, 64, "", Options{}, nil)
		body := `{"model":"large-model",` + tmplMessages + filler + `,` + member + `}`
		resp, got := f.post(t, PathChatCompletions, []byte(body), map[string]string{
			templates.HeaderTemplateID: "support", templates.HeaderTemplateVersion: "2", mockllm.HeaderOutputTokens: "1"})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: status %d: %.120s", member, resp.StatusCode, got)
		}
		if bodies := f.upstreamBodies(); len(bodies) != 1 || bodies[0] != body {
			t.Fatalf("%s: upstream %d bodies, want the caller's unchanged", member, len(bodies))
		}
	}
}

// TestBodyPastTheIndexCeilingGetsBothSplices: past the ceiling the splices
// are made as bytes, one on the result of the other: the caller's priority
// goes, the class's and the gateway's max_tokens go in, each first in the
// object as it stands, wherever the caller's body put its '{'. Nothing is
// lost between them, and the bytes they do not touch are the caller's.
func TestBodyPastTheIndexCeilingGetsBothSplices(t *testing.T) {
	f := newTemplateFixture(t, 256, upstreamValuesYAML, Options{PriorityVerified: func(string) bool { return true }},
		func(c *mockllm.Config) { c.SchedulingPolicy = mockllm.PolicyPriority })
	f.h.priorityPools["large"] = true // as pools[].priority_scheduling: true
	hdr := map[string]string{templates.HeaderTemplateID: "support", templates.HeaderTemplateVersion: "2",
		HeaderPriority: config.PriorityBackground, mockllm.HeaderOutputTokens: "1"}
	rest := `"model":"large-model",` + tmplMessages + filler
	for _, tc := range []struct{ name, body, want string }{
		{"a caller's priority", `{` + rest + `,"priority":3}`, `{"max_tokens":256,"priority":10,` + rest + `}`},
		{"none of the caller's", `{` + rest + `}`, `{"max_tokens":256,"priority":10,` + rest + `}`},
		{"after leading whitespace", "\t {" + `"priority":3,` + rest + `}`, "\t " + `{"max_tokens":256,"priority":10,` + rest + `}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if resp, got := f.post(t, PathChatCompletions, []byte(tc.body), hdr); resp.StatusCode != http.StatusOK {
				t.Fatalf("status %d: %.120s", resp.StatusCode, got)
			}
			if bodies := f.upstreamBodies(); len(bodies) != 1 || bodies[0] != tc.want {
				t.Fatalf("upstream %d bodies, first %.120q..., want %.120q...", len(bodies), strings.Join(bodies, ""), tc.want)
			}
		})
	}
}

// TestBodyPastTheIndexCeilingSkipsDedupAndContext (E5): the index holds
// none of the members of a body past the ceiling, so coalescing and
// context assembly fail open on it with their own reason,
// too_many_members, and the request goes on unchanged.
func TestBodyPastTheIndexCeilingSkipsDedupAndContext(t *testing.T) {
	f := newDedupFixtureWith(t, "context: {enabled: true, max_probes: 12, tokenize_timeout: 2s, max_body_bytes: 1048576}\n", 0, 1<<20)
	body := faqBody("where is my order?", false, strings.TrimPrefix(filler, ",")+",")
	hdr := faqHeaders("faq", 1)
	hdr[HeaderContextBudget] = "100000"
	a := f.ask(context.Background(), body, hdr)
	if a.err != nil || a.status != http.StatusOK || a.header.Get(HeaderContextAssembly) != contextSkipped {
		t.Fatalf("status %d, err %v, %s %q: %.120s", a.status, a.err, HeaderContextAssembly, a.header.Get(HeaderContextAssembly), a.body)
	}
	if gens := f.generations(); len(gens) != 1 || string(gens[0].Body) != body {
		t.Fatalf("%d generations, want one with the caller's body", len(gens))
	}
	for _, stage := range []string{telemetry.StageCoalesce, telemetry.StageContext} {
		series := fmt.Sprintf(`%s{stage=%q,reason=%q}`, telemetry.MetricFailOpen, stage, telemetry.ReasonTooManyMembers)
		eventually(t, series, func() bool { return f.metric(series) == 1 })
	}
	if s := f.group.Stats(); s.Flights != 0 || s.Joined != 0 {
		t.Fatalf("a body past the ceiling led or joined a flight: %+v", s)
	}
}
