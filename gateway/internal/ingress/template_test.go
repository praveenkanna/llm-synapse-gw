package ingress

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/compiler"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/mockllm"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates"
)

const tmplSpec = `{"model":"large-model","variables":[{"name":"q"}],
 "messages":[{"role":"system","segments":[{"kind":"static","text":"You are a support assistant. Answer briefly."}]},
             {"role":"user","segments":[{"kind":"volatile","text":"{{q}}"}]}]}`

const tmplMessages = `"messages":[{"role":"system","content":"You are a support assistant. Answer briefly."},{"role":"user","content":"where is my order?"}]`

// templateMinSamples is the fixture compiler's min_samples; a test lowers it
// to see the learned cap.
var templateMinSamples = 1000

// templateModelLen is the fixture pool's max_model_len; 0 (unchecked) unless
// a test sets it.
var templateModelLen = 0

type staticSnap struct{ s *templates.Snapshot }

func (f staticSnap) Snapshot() *templates.Snapshot { return f.s }

// templateFixture is the gateway with telemetry and a compiler over a
// one-version snapshot (support v2) whose large pool ceiling is ceiling.
type templateFixture struct {
	*fixture // its tel is the template fixture's own
}

func newTemplateFixture(t *testing.T, ceiling int, yamlExtra string, opts Options, largeMod func(*mockllm.Config)) *templateFixture {
	t.Helper()
	snap, errs := templates.NewSnapshot([]templates.Record{{
		ID: "support", Version: 2, Status: templates.StatusActive, Spec: json.RawMessage(tmplSpec),
		Counts: json.RawMessage(`{"pool":"large","prefix_tokens":12}`),
	}}, time.Now())
	if len(errs) > 0 {
		t.Fatal(errs)
	}
	tel := telemetry.New(telemetry.Options{})
	opts.Compiler = compiler.New(staticSnap{snap}, compiler.Options{
		MaxTokens: config.MaxTokensPolicy{Enabled: true, Margin: 0.25, MinSamples: templateMinSamples},
		Ceilings:  map[string]int{"large": ceiling},
		ModelLens: map[string]int{"large": templateModelLen},
	})
	if opts.Metrics == nil {
		opts.Metrics = tel.Metrics()
	}
	f := newFixtureWith(t, yamlExtra, opts, largeMod)
	mux := http.NewServeMux()
	f.h.Register(mux)
	f.gw.Close()
	f.gw, f.tel = httptest.NewServer(tel.Middleware(mux)), tel
	t.Cleanup(f.gw.Close)
	return &templateFixture{fixture: f}
}

func (f *templateFixture) upstreamBodies() []string {
	var out []string
	for _, s := range []*mockllm.Server{f.large0, f.large1} {
		for _, r := range s.Mock().Requests() {
			out = append(out, string(r.Body))
		}
		s.Mock().ResetRequests()
	}
	return out
}

// metric returns the value of one exported series, waiting briefly: the
// middleware counts a request after its response is written.
func (f *templateFixture) metric(t *testing.T, series string, wantMin float64) float64 {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		rec := httptest.NewRecorder()
		f.tel.Metrics().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, telemetry.MetricsPath, nil))
		var got float64
		for _, line := range strings.Split(rec.Body.String(), "\n") {
			if v, ok := strings.CutPrefix(line, series+" "); ok {
				got, _ = strconv.ParseFloat(v, 64)
			}
		}
		if got >= wantMin || time.Now().After(deadline) {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
}

var tmplHeaders = map[string]string{templates.HeaderTemplateID: "support", templates.HeaderTemplateVersion: "2", mockllm.HeaderOutputTokens: "5"}

func TestMatchedTemplateGetsCapAndMetrics(t *testing.T) {
	f := newTemplateFixture(t, 512, "", Options{}, nil)
	body := []byte(`{"model":"large-model",` + tmplMessages + `}`)
	resp, got := f.post(t, PathChatCompletions, body, tmplHeaders)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %s", resp.StatusCode, got)
	}
	ups := f.upstreamBodies()
	if len(ups) != 1 || ups[0] != `{"max_tokens":512,`+string(body[1:]) {
		t.Fatalf("upstream bodies %q, want max_tokens spliced in front of the caller's bytes", ups)
	}
	labels := `{template="support",version="2"}`
	if n := f.metric(t, telemetry.MetricTemplateRequests+`{template="support",version="2",status="200"}`, 1); n != 1 {
		t.Fatalf("template requests = %v", n)
	}
	want := mockllm.CountChat([]mockllm.Message{
		{Role: "system", Content: "You are a support assistant. Answer briefly."},
		{Role: "user", Content: "where is my order?"},
	}, true)
	if n := f.metric(t, telemetry.MetricTemplatePromptTokens+labels, 1); int(n) != want {
		t.Fatalf("prompt tokens = %v, want the tokenizer count %d", n, want)
	}
	if n := f.metric(t, telemetry.MetricTemplateCompletionTokens+labels, 1); n != 5 {
		t.Fatalf("completion tokens = %v, want 5", n)
	}
	if n := f.metric(t, telemetry.MetricTemplateCacheablePromptTokens+labels, 1); n != 12 {
		t.Fatalf("cacheable prompt tokens = %v, want the registered 12", n)
	}
	if n := f.metric(t, telemetry.MetricTemplateMaxTokensSet+labels, 1); n != 1 {
		t.Fatalf("max_tokens set = %v", n)
	}
	if n := f.metric(t, telemetry.MetricTemplateRunaway+labels, 0); n != 0 {
		t.Fatalf("runaway = %v, want 0 (5 tokens under a cap of 512)", n)
	}
}

// AC-M3-7: a caller's max_tokens or max_completion_tokens is never lowered
// or replaced, even when it is above the pool ceiling.
func TestCallerCapNeverChanged(t *testing.T) {
	f := newTemplateFixture(t, 64, "", Options{}, nil)
	for _, cap := range []string{`"max_tokens":4000`, `"max_completion_tokens":4000`, `"max_tokens":null`} {
		body := []byte(`{"model":"large-model",` + tmplMessages + `,` + cap + `}`)
		resp, got := f.post(t, PathChatCompletions, body, tmplHeaders)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: status %d: %s", cap, resp.StatusCode, got)
		}
		if ups := f.upstreamBodies(); len(ups) != 1 || ups[0] != string(body) {
			t.Fatalf("%s: upstream body changed: %q", cap, ups)
		}
	}
}

// ADR 0007: the learned cap follows what uncapped callers' outputs look like.
// Callers that set their own small max_tokens cut their output short; their
// responses must not pull the cap down for the callers that set none.
func TestLearnedCapIgnoresCallerCappedRequests(t *testing.T) {
	templateMinSamples = 5
	t.Cleanup(func() { templateMinSamples = 1000 })
	f := newTemplateFixture(t, 512, "", Options{}, nil)
	send := func(extra, outputTokens string) {
		t.Helper()
		hdr := map[string]string{templates.HeaderTemplateID: "support", templates.HeaderTemplateVersion: "2", mockllm.HeaderOutputTokens: outputTokens}
		body := []byte(`{"model":"large-model",` + tmplMessages + extra + `}`)
		if resp, got := f.post(t, PathChatCompletions, body, hdr); resp.StatusCode != http.StatusOK {
			t.Fatalf("status %d: %s", resp.StatusCode, got)
		}
	}
	injected := func() int {
		t.Helper()
		send("", "5")
		ups := f.upstreamBodies()
		var b struct {
			MaxTokens int `json:"max_tokens"`
		}
		if len(ups) != 1 || json.Unmarshal([]byte(ups[0]), &b) != nil {
			t.Fatalf("upstream bodies %q", ups)
		}
		return b.MaxTokens
	}

	for range 20 {
		send(`,"max_tokens":3`, "3")
	}
	f.upstreamBodies() // drop the capped requests
	// Their responses were 3 tokens long, but not observed: an uncapped
	// caller still gets the ceiling while there are too few samples.
	if got := injected(); got != 512 {
		t.Fatalf("cap after 20 caller-capped requests = %d, want the ceiling 512", got)
	}
	for range 10 {
		send("", "5")
	}
	f.upstreamBodies()
	if got := injected(); got < 5 || got > 8 {
		t.Fatalf("cap after uncapped requests of 5 tokens = %d, want about 5 × 1.25", got)
	}
}

func TestCapHitIsRunaway(t *testing.T) {
	f := newTemplateFixture(t, 3, "", Options{}, nil)
	hdr := map[string]string{templates.HeaderTemplateID: "support", templates.HeaderTemplateVersion: "2", mockllm.HeaderOutputTokens: "10"}
	for _, stream := range []string{``, `,"stream":true,"stream_options":{"include_usage":true}`, `,"stream":true`} {
		body := []byte(`{"model":"large-model",` + tmplMessages + stream + `}`)
		if resp, got := f.post(t, PathChatCompletions, body, hdr); resp.StatusCode != http.StatusOK {
			t.Fatalf("status %d: %s", resp.StatusCode, got)
		}
	}
	if n := f.metric(t, telemetry.MetricTemplateRunaway+`{template="support",version="2"}`, 3); n != 3 {
		t.Fatalf("runaway = %v, want 3 (non-streaming, streaming with and without usage)", n)
	}
}

func TestUnmatchedTemplateFailsOpenUnchanged(t *testing.T) {
	f := newTemplateFixture(t, 512, "", Options{}, nil)
	body := []byte(`{"model":"large-model","messages":[{"role":"system","content":"You are a support assistant!"},{"role":"user","content":"hi"}]}`)
	for _, tc := range []struct{ version, reason string }{
		{"2", templates.ReasonStaticMismatch}, {"", templates.ReasonNoVersion}, {"7", templates.ReasonUnknownVersion},
	} {
		hdr := map[string]string{templates.HeaderTemplateID: "support", templates.HeaderTemplateVersion: tc.version}
		if resp, got := f.post(t, PathChatCompletions, body, hdr); resp.StatusCode != http.StatusOK {
			t.Fatalf("status %d: %s", resp.StatusCode, got)
		}
		if ups := f.upstreamBodies(); len(ups) != 1 || ups[0] != string(body) {
			t.Fatalf("%s: upstream body changed: %q", tc.reason, ups)
		}
		series := telemetry.MetricFailOpen + `{stage="compile",reason="` + tc.reason + `"}`
		if n := f.metric(t, series, 1); n != 1 {
			t.Fatalf("%s = %v, want 1", series, n)
		}
	}
	if n := f.metric(t, telemetry.MetricTemplateRequests+`{template="support",version="2",status="200"}`, 0); n != 0 {
		t.Fatalf("unmatched requests must not count as the template: %v", n)
	}
}

func TestCapAndPrioritySplicesCompose(t *testing.T) {
	f := newTemplateFixture(t, 256, upstreamValuesYAML, Options{PriorityVerified: func(string) bool { return true }},
		func(c *mockllm.Config) { c.SchedulingPolicy = mockllm.PolicyPriority })
	f.h.priorityPools["large"] = true
	hdr := map[string]string{templates.HeaderTemplateID: "support", templates.HeaderTemplateVersion: "2", HeaderPriority: "background"}
	for _, body := range []string{
		`{"model":"large-model",` + tmplMessages + `}`,
		`{"priority":3, "model":"large-model",` + tmplMessages + `}`,
		`{"model":"large-model",` + tmplMessages + `, "priority":3}`,
	} {
		if resp, got := f.post(t, PathChatCompletions, []byte(body), hdr); resp.StatusCode != http.StatusOK {
			t.Fatalf("status %d: %s", resp.StatusCode, got)
		}
		ups := f.upstreamBodies()
		if len(ups) != 1 {
			t.Fatalf("%d upstream requests", len(ups))
		}
		obj := decodeObject(t, []byte(ups[0]))
		if obj["max_tokens"] != float64(256) || obj["priority"] != float64(10) {
			t.Fatalf("body %s: max_tokens %v priority %v, want 256 and 10", body, obj["max_tokens"], obj["priority"])
		}
		if !bytes.Contains([]byte(ups[0]), []byte(tmplMessages)) {
			t.Fatalf("messages bytes changed: %s", ups[0])
		}
	}
}

// A pool that states its max_model_len gets no gateway cap on a request
// whose prompt could leave less room than the cap, and the request goes on
// as the caller sent it (live run 2026-11-03: vLLM refused a 30,873-token
// prompt plus a spliced max_tokens of 2048 on a 32,768-token model).
func TestCapSkippedWhenPromptMayNotLeaveRoom(t *testing.T) {
	old := templateModelLen
	templateModelLen = 1024 // ceiling 512 + slack 256 leaves 256 body bytes
	t.Cleanup(func() { templateModelLen = old })
	f := newTemplateFixture(t, 512, "", Options{}, nil)
	small := []byte(`{"model":"large-model","messages":[{"role":"system","content":"You are a support assistant. Answer briefly."},{"role":"user","content":"hi"}]}`)
	if len(small) > templateModelLen-512-compiler.ModelLenSlack {
		t.Fatalf("test body is %d bytes, over the room it must fit", len(small))
	}
	big := []byte(`{"model":"large-model","messages":[{"role":"system","content":"You are a support assistant. Answer briefly."},{"role":"user","content":"` + strings.Repeat("x", 600) + `"}]}`)
	for _, tc := range []struct {
		name string
		body []byte
		want string
	}{
		{"a small prompt keeps the cap", small, `{"max_tokens":512,` + string(small[1:])},
		{"a prompt that may not leave room goes on as sent", big, string(big)},
	} {
		resp, got := f.post(t, PathChatCompletions, tc.body, tmplHeaders)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: status %d: %s", tc.name, resp.StatusCode, got)
		}
		if ups := f.upstreamBodies(); len(ups) != 1 || ups[0] != tc.want {
			t.Fatalf("%s: upstream bodies %q, want %q", tc.name, ups, tc.want)
		}
	}
	if n := f.metric(t, `gateway_failopen_total{stage="max_tokens",reason="near_model_len"}`, 1); n != 1 {
		t.Fatalf("near_model_len fail-opens = %v, want 1 (the large prompt only)", n)
	}
}
