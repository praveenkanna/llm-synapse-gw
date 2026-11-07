package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/mockllm"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

// stand-in token count of the chat render of msgs, as usage.prompt_tokens
// counts it.
func standInCount(msgs []templates.RenderedMessage) int {
	return mockllm.CountChat(toMock(msgs), true)
}

func standInTokens(msgs []templates.RenderedMessage) []int {
	return mockllm.Tokenize(mockllm.RenderChat(toMock(msgs), true))
}

func commonPrefix(a, b []int) int {
	n := 0
	for n < len(a) && n < len(b) && a[n] == b[n] {
		n++
	}
	return n
}

func TestRegistrationCounts(t *testing.T) {
	f := newFixture(t)
	out := f.register("support", supportSpec)
	spec := mustParse(t, supportSpec)

	if len(out.Counts.Examples) != len(spec.Examples) {
		t.Fatalf("counts = %+v, want one per example", out.Counts)
	}
	var renders [][]templates.RenderedMessage
	for i, ex := range spec.Examples {
		msgs, err := templates.Render(spec, ex)
		if err != nil {
			t.Fatal(err)
		}
		renders = append(renders, msgs)
		if want := standInCount(msgs); out.Counts.Examples[i] != want {
			t.Errorf("examples[%d] = %d tokens, want %d", i, out.Counts.Examples[i], want)
		}
	}

	// The stand-in tokenizer splits the chat render into 4-byte chunks, so
	// every render shares the whole chunks before the first variable.
	shared := len("<|system|>\n" + supportStatic + "Tenant: ")
	wantPrefix := shared / mockllm.BytesPerToken
	if out.Counts.PrefixTokens != wantPrefix {
		t.Errorf("prefix_tokens = %d, want %d", out.Counts.PrefixTokens, wantPrefix)
	}
	if got := commonPrefix(standInTokens(renders[0]), standInTokens(renders[1])); got != wantPrefix {
		t.Errorf("the examples share %d tokens, want prefix_tokens %d", got, wantPrefix)
	}
	if stored := f.store.counts(t, "support", 1); !countsEqual(stored, out.Counts) {
		t.Errorf("stored counts %+v, want %+v", stored, out.Counts)
	}
}

func countsEqual(a, b templates.Counts) bool {
	return a.Pool == b.Pool && a.PrefixTokens == b.PrefixTokens && a.Error == b.Error && slices.Equal(a.Examples, b.Examples)
}

func TestPrefixTokens(t *testing.T) {
	for _, tc := range []struct {
		name   string
		spec   string
		shared func(spec templates.Spec) int // shared render bytes; -1: the whole render
	}{
		{
			name: "no variables: the whole render",
			spec: `{"model":"large-model","messages":[
			  {"role":"system","segments":[{"kind":"static","text":"Static only, every byte shared."}]},
			  {"role":"user","segments":[{"kind":"static","text":"Fixed question?"}]}]}`,
			shared: func(templates.Spec) int { return -1 },
		},
		{
			name: "first variable in a later message",
			spec: `{"model":"large-model","variables":[{"name":"q"}],"messages":[
			  {"role":"system","segments":[{"kind":"static","text":"You answer questions about billing."}]},
			  {"role":"user","segments":[{"kind":"static","text":"Question: "},{"kind":"volatile","text":"{{q}}"}]}]}`,
			shared: func(templates.Spec) int {
				return len("<|system|>\nYou answer questions about billing.\n<|user|>\nQuestion: ")
			},
		},
		{
			name: "variable first",
			spec: `{"model":"large-model","variables":[{"name":"q"}],"messages":[
			  {"role":"user","segments":[{"kind":"volatile","text":"{{q}} please"}]}]}`,
			shared: func(templates.Spec) int { return len("<|user|>\n") },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			out := f.register("p", tc.spec)
			spec := mustParse(t, tc.spec)
			want := tc.shared(spec) / mockllm.BytesPerToken
			if tc.shared(spec) < 0 {
				msgs, _ := templates.Render(spec, nil)
				want = standInCount(msgs)
			}
			if out.Counts.Error != "" || out.Counts.PrefixTokens != want {
				t.Errorf("counts = %+v, want prefix_tokens %d", out.Counts, want)
			}
		})
	}
}

// A failed count is stored as its error; registration still succeeds, and
// recount fills the counts in once a replica answers.
func TestCountFailureStillRegisters(t *testing.T) {
	f := newFixture(t)
	f.sender.setFail("r1", true)
	f.sender.setFail("r2", true)

	out := f.register("support", supportSpec)
	c := out.Counts
	if !strings.Contains(c.Error, "connect") || c.Pool != "" || c.PrefixTokens != 0 || c.Examples != nil {
		t.Fatalf("counts = %+v, want only an error", c)
	}
	if strings.Contains(c.Error, leakMarker) {
		t.Errorf("count error quotes text: %q", c.Error)
	}
	if stored := f.store.counts(t, "support", 1); stored.Error != c.Error {
		t.Errorf("stored counts %+v", stored)
	}
	if got := f.sender.callLog(); !slices.Equal(got, []string{"r1", "r2"}) {
		t.Errorf("tokenize calls %v, want one per replica and then stop", got)
	}

	kicks := f.kicks.Load()
	rec := f.call(http.MethodPost, "/admin/v1/templates/support/versions/1/recount", "", http.StatusBadGateway)
	if e := errorOf(t, rec); e.Code != "tokenize_failed" {
		t.Errorf("recount while down: %+v", e)
	}
	if f.kicks.Load() != kicks || f.store.counts(t, "support", 1).Error == "" {
		t.Error("a failed recount changed the stored counts")
	}

	f.sender.setFail("r1", false)
	f.sender.setFail("r2", false)
	rec = f.call(http.MethodPost, "/admin/v1/templates/support/versions/1/recount", "", http.StatusOK)
	var re recountResponse
	decode(t, rec, &re)
	want := newFixture(t).register("support", supportSpec).Counts
	if !countsEqual(re.Counts, want) {
		t.Errorf("recount = %+v, want %+v", re.Counts, want)
	}
	if stored := f.store.counts(t, "support", 1); !countsEqual(stored, want) {
		t.Errorf("stored after recount %+v", stored)
	}
	if f.kicks.Load() != kicks+1 {
		t.Error("recount did not kick the registry")
	}
}

func TestCountWithoutPoolOrReplicas(t *testing.T) {
	f := newFixture(t)
	for model, wantErr := range map[string]string{
		unmappedModel: "model is not mapped to a pool",
		"empty-model": "no replicas in pool empty",
	} {
		spec := strings.Replace(supportSpec, testModel, model, 1)
		out := f.register("support", spec)
		if !strings.Contains(out.Counts.Error, wantErr) {
			t.Errorf("%s: counts = %+v, want error %q", model, out.Counts, wantErr)
		}
		rec := f.call(http.MethodPost, "/admin/v1/templates/support/versions/"+itoa(out.Version)+"/tokenize",
			`{"variables":{"tenant":"a","question":"b"}}`, http.StatusBadGateway)
		if e := errorOf(t, rec); !strings.Contains(e.Message, wantErr) {
			t.Errorf("%s: tokenize error %+v", model, e)
		}
	}
}

func itoa(n int) string {
	b, _ := json.Marshal(n)
	return string(b)
}

func TestTokenizeFailover(t *testing.T) {
	f := newFixture(t)
	f.register("support", supportSpec)
	path := "/admin/v1/templates/support/versions/1/tokenize"
	vars := `{"variables":{"tenant":"acme","question":"hi"}}`
	msgs, _ := templates.Render(mustParse(t, supportSpec), map[string]string{"tenant": "acme", "question": "hi"})

	rec := f.call(http.MethodPost, path, vars, http.StatusOK)
	var out tokenizeResponse
	decode(t, rec, &out)
	if out.PromptTokens != standInCount(msgs) || out.Pool != "large" || out.Replica != "r1" {
		t.Errorf("tokenize = %+v, want %d tokens on r1", out, standInCount(msgs))
	}

	// A transport error before any response moves on to the next replica.
	f.sender.setFail("r1", true)
	before := len(f.sender.callLog())
	rec = f.call(http.MethodPost, path, vars, http.StatusOK)
	decode(t, rec, &out)
	if out.Replica != "r2" || out.PromptTokens != standInCount(msgs) {
		t.Errorf("tokenize with r1 down = %+v", out)
	}
	if got := f.sender.callLog()[before:]; !slices.Equal(got, []string{"r1", "r2"}) {
		t.Errorf("calls %v, want r1 then r2", got)
	}

	// A replica that answered with an error is not retried elsewhere.
	f.sender.setFail("r1", false)
	f.sender.setStatus("r1", http.StatusInternalServerError)
	before = len(f.sender.callLog())
	rec = f.call(http.MethodPost, path, vars, http.StatusBadGateway)
	e := errorOf(t, rec)
	if e.Code != "tokenize_failed" || !strings.Contains(e.Message, "status 500") || strings.Contains(e.Message, leakMarker) {
		t.Errorf("tokenize error = %+v", e)
	}
	if got := f.sender.callLog()[before:]; !slices.Equal(got, []string{"r1"}) {
		t.Errorf("calls %v, want r1 only", got)
	}

	rec = f.call(http.MethodPost, path, `{"variables":{"tenant":"acme"}}`, http.StatusBadRequest)
	if e := errorOf(t, rec); e.Code != "render_error" {
		t.Errorf("tokenize with a missing variable: %+v", e)
	}
}

type funcSender func(ctx context.Context, rep discovery.Replica, req upstream.Request) (*http.Response, error)

func (f funcSender) Do(ctx context.Context, rep discovery.Replica, req upstream.Request) (*http.Response, error) {
	return f(ctx, rep, req)
}

func reply(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body))}
}

func TestTokenizeBadReplies(t *testing.T) {
	for _, tc := range []struct {
		name   string
		sender funcSender
		want   string
	}{
		{"count without ids", func(context.Context, discovery.Replica, upstream.Request) (*http.Response, error) {
			return reply(200, `{"count":3,"tokens":[]}`), nil
		}, "does not match"},
		{"not JSON", func(context.Context, discovery.Replica, upstream.Request) (*http.Response, error) {
			return reply(200, `<html>`), nil
		}, "not a token count"},
		{"oversized", func(context.Context, discovery.Replica, upstream.Request) (*http.Response, error) {
			return reply(200, `{"count":1,"tokens":[1],"pad":"`+strings.Repeat("x", maxTokenizeResponseBytes)+`"}`), nil
		}, "over"},
		{"timeout on every replica", func(ctx context.Context, rep discovery.Replica, _ upstream.Request) (*http.Response, error) {
			<-ctx.Done()
			return nil, &upstream.Error{Kind: upstream.KindTimeout, Replica: rep.Name, Err: ctx.Err()}
		}, "timeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			o := f.srv.o
			o.Sender, o.TokenizeTimeout = tc.sender, 20*time.Millisecond
			srv, err := New(f.store, o)
			if err != nil {
				t.Fatal(err)
			}
			f.h = srv.Handler()
			out := f.register("support", supportSpec)
			if !strings.Contains(out.Counts.Error, tc.want) || out.Counts.Examples != nil {
				t.Errorf("counts = %+v, want error containing %q", out.Counts, tc.want)
			}
		})
	}
}

// The /tokenize request is vLLM's chat form with the chat-completions
// defaults, sent through the Sender (which adds the upstream API key).
func TestTokenizeRequestShape(t *testing.T) {
	f := newFixture(t)
	var got []upstream.Request
	o := f.srv.o
	o.Sender = funcSender(func(ctx context.Context, rep discovery.Replica, req upstream.Request) (*http.Response, error) {
		got = append(got, req)
		return f.sender.Do(ctx, rep, req)
	})
	srv, err := New(f.store, o)
	if err != nil {
		t.Fatal(err)
	}
	f.h = srv.Handler()
	f.call(http.MethodPost, "/admin/v1/validate", supportSpec, http.StatusOK)
	if len(got) != 0 {
		t.Fatal("validate called /tokenize")
	}
	f.register("support", supportSpec)
	if len(got) != 4 { // two synthetic renders, two examples
		t.Fatalf("%d /tokenize calls, want 4", len(got))
	}
	req := got[0]
	if req.Method != http.MethodPost || req.Path != PathTokenize || req.Header.Get("Content-Type") != "application/json" || req.Stream {
		t.Errorf("request = %+v", req)
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(req.Body, &body); err != nil {
		t.Fatal(err)
	}
	if string(body["model"]) != `"large-model"` || string(body["add_generation_prompt"]) != "true" || len(body) != 3 {
		t.Errorf("body = %s", req.Body)
	}
}

// AC-M3-1: for random variable maps, /tokenize's prompt_tokens equals
// usage.prompt_tokens of the same render sent as a chat completion. Both
// go over HTTP to a mockllm server, through the real upstream client.
func TestTokenizeMatchesUsage(t *testing.T) {
	cfg := mockllm.Defaults()
	cfg.Models = []string{testModel}
	cfg.TTFT, cfg.TokensPerSecond = 0, 0
	mock, err := mockllm.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(mock)
	defer ts.Close()

	disc, err := discovery.NewStatic([]config.Pool{{Name: "large", Replicas: []config.Replica{{Name: "m0", URL: ts.URL}}}})
	if err != nil {
		t.Fatal(err)
	}
	client := upstream.New(config.Defaults().Upstream)
	defer client.CloseIdleConnections()
	store := newFakeStore()
	srv, err := New(store, Options{
		Tokens:    []config.Caller{{Name: "ci", Token: config.Secret(tokenCI)}},
		Discovery: disc,
		Sender:    client,
		PoolFor:   func(string) (string, bool) { return "large", true },
	})
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, srv: srv, h: srv.Handler(), store: store}
	spec := `{"model":"large-model","variables":[{"name":"tenant"},{"name":"question"}],"messages":[
	  {"role":"system","segments":[{"kind":"static","text":"You are a support assistant."},{"kind":"semi_static","text":" Tenant: {{tenant}}."}]},
	  {"role":"user","segments":[{"kind":"volatile","text":"{{question}}"}]}]}`
	f.register("support", spec)
	parsed := mustParse(t, spec)

	rng := rand.New(rand.NewPCG(1, 2))
	for i := range 100 {
		vars := map[string]string{"tenant": randomText(rng, 40), "question": randomText(rng, 400)}
		body, _ := json.Marshal(map[string]any{"variables": vars})
		rec := f.call(http.MethodPost, "/admin/v1/templates/support/versions/1/tokenize", string(body), http.StatusOK)
		var tok tokenizeResponse
		decode(t, rec, &tok)

		msgs, err := templates.Render(parsed, vars)
		if err != nil {
			t.Fatal(err)
		}
		if usage := chatPromptTokens(t, ts.URL, msgs); tok.PromptTokens != usage {
			t.Fatalf("map %d: /tokenize prompt_tokens %d, usage.prompt_tokens %d", i, tok.PromptTokens, usage)
		}
	}
}

// randomText is up to n runes mixing ASCII, multi-byte runes, JSON
// metacharacters and placeholder-like text.
func randomText(rng *rand.Rand, n int) string {
	alphabet := []rune("abcxyz ABC0123456789.,!?\n\t\"\\{}<>|/é日本語🙂")
	var b strings.Builder
	for range rng.IntN(n + 1) {
		b.WriteRune(alphabet[rng.IntN(len(alphabet))])
	}
	if rng.IntN(4) == 0 {
		b.WriteString("{{tenant}}")
	}
	return b.String()
}

func chatPromptTokens(t *testing.T, baseURL string, msgs []templates.RenderedMessage) int {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"model": testModel, "messages": msgs, "max_tokens": 1})
	req, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/chat/completions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Mock-Output-Tokens", "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("chat completion: %d %s", resp.StatusCode, b)
	}
	var out struct {
		Usage struct {
			PromptTokens int `json:"prompt_tokens"`
		} `json:"usage"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out.Usage.PromptTokens
}
