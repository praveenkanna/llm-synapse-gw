package mockllm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
)

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(b)
}

// staticText returns n deterministic bytes of prose.
func staticText(n int) string {
	const s = "Follow the policy exactly. Cite the section you rely on and keep answers short. "
	return strings.Repeat(s, n/len(s)+1)[:n]
}

func tokenizeCall(t *testing.T, srv *Server, body string) tokenizeResponse {
	t.Helper()
	resp, b := mustPost(t, srv.URL()+"/tokenize", body, nil)
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("/tokenize %s: %d %s", body, resp.StatusCode, b)
	}
	var got tokenizeResponse
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("decode /tokenize: %v: %s", err, b)
	}
	if got.Count != len(got.Tokens) {
		t.Fatalf("/tokenize count %d but %d tokens", got.Count, len(got.Tokens))
	}
	return got
}

// genResult is what a generation response reports, streaming or not.
type genResult struct {
	usage    usage
	finish   string // of choice 0
	tokens   int    // content chunks (stream) for choice 0; from usage otherwise
	hasUsage bool
}

// generate posts body to path and reads usage and finish_reason from the
// JSON body or, for streams, from the chunks and the final usage chunk.
func generate(t *testing.T, srv *Server, path, body string) genResult {
	t.Helper()
	resp, err := post(t, context.Background(), srv.URL()+path, body, nil)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST %s %s: %d %s", path, body, resp.StatusCode, b)
	}
	type choice struct {
		Delta        struct{ Content string } `json:"delta"`
		Text         string                   `json:"text"`
		FinishReason *string                  `json:"finish_reason"`
	}
	if resp.Header.Get("Content-Type") != sseContentType {
		var r struct {
			Choices []choice `json:"choices"`
			Usage   usage    `json:"usage"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&r); err != nil || len(r.Choices) == 0 || r.Choices[0].FinishReason == nil {
			t.Fatalf("decode response: %v %+v", err, r)
		}
		return genResult{usage: r.Usage, finish: *r.Choices[0].FinishReason, tokens: r.Usage.CompletionTokens, hasUsage: true}
	}
	events, err := readEvents(resp.Body)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("read events: %v", err)
	}
	if len(events) == 0 || events[len(events)-1].data != "[DONE]" {
		t.Fatalf("stream does not end with [DONE]: %+v", events)
	}
	var g genResult
	for i, ev := range events[:len(events)-1] {
		var c struct {
			Choices []choice `json:"choices"`
			Usage   *usage   `json:"usage"`
		}
		if err := json.Unmarshal([]byte(ev.data), &c); err != nil {
			t.Fatalf("event %d not JSON: %s", i, ev.data)
		}
		if c.Usage != nil {
			if len(c.Choices) != 0 || i != len(events)-2 {
				t.Fatalf("usage chunk must have no choices and come right before [DONE]: event %d of %d: %s", i, len(events), ev.data)
			}
			g.usage, g.hasUsage = *c.Usage, true
			continue
		}
		for _, ch := range c.Choices {
			if ch.Delta.Content != "" || ch.Text != "" {
				g.tokens++
			}
			if ch.FinishReason != nil {
				g.finish = *ch.FinishReason
			}
		}
	}
	return g
}

const streamUsage = `,"stream":true,"stream_options":{"include_usage":true}`

// TestTokenizeMatchesUsage: /tokenize's count equals usage.prompt_tokens of
// the same messages or prompt, streaming and not, with the prefix cache on
// and off (the count is computed differently when no tokens are needed).
func TestTokenizeMatchesUsage(t *testing.T) {
	chats := map[string][]Message{
		"one user turn": {{Role: "user", Content: "hi"}},
		"long system":   {{Role: "system", Content: staticText(4500)}, {Role: "user", Content: "What applies to refunds?"}},
		"no messages":   {},
		"odd lengths":   {{Role: "developer", Content: "abc"}, {Role: "user", Content: "café ☃"}},
	}
	prompts := map[string]string{
		"string prompt":    `"once upon a time"`,
		"token ids prompt": `[11, 22, 33, 44, 55]`,
		"empty prompt":     `""`,
	}
	for _, blocks := range []int{DefaultPrefixCacheBlocks, 0} {
		t.Run(fmt.Sprintf("prefix cache %d", blocks), func(t *testing.T) {
			srv := startMock(t, func(c *Config) { c.PrefixCacheBlocks = blocks })
			for name, msgs := range chats {
				m := mustJSON(t, msgs)
				tok := tokenizeCall(t, srv, `{"model":"test-model","messages":`+m+`}`)
				if want := CountChat(msgs, true); tok.Count != want || !slices.Equal(tok.Tokens, Tokenize(RenderChat(msgs, true))) {
					t.Fatalf("%s: /tokenize count %d, want %d", name, tok.Count, want)
				}
				for _, extra := range []string{"", streamUsage} {
					g := generate(t, srv, "/v1/chat/completions", `{"model":"test-model","messages":`+m+extra+`}`)
					if !g.hasUsage || g.usage.PromptTokens != tok.Count {
						t.Fatalf("%s%s: usage.prompt_tokens %d, /tokenize count %d", name, extra, g.usage.PromptTokens, tok.Count)
					}
				}
			}
			for name, p := range prompts {
				tok := tokenizeCall(t, srv, `{"model":"test-model","prompt":`+p+`}`)
				for _, extra := range []string{"", streamUsage} {
					g := generate(t, srv, "/v1/completions", `{"model":"test-model","prompt":`+p+extra+`}`)
					if !g.hasUsage || g.usage.PromptTokens != tok.Count {
						t.Fatalf("%s%s: usage.prompt_tokens %d, /tokenize count %d", name, extra, g.usage.PromptTokens, tok.Count)
					}
				}
			}
		})
	}
}

func TestTokenizeEndpoint(t *testing.T) {
	srv := startMock(t, func(c *Config) {
		c.MaxModelLen = 1234
		c.MaxBodyBytes = 512
	})
	msgs := []Message{{Role: "system", Content: "Be brief."}, {Role: "user", Content: "Hi"}}
	m := mustJSON(t, msgs)

	tok := tokenizeCall(t, srv, `{"model":"test-model","messages":`+m+`,"add_generation_prompt":false,"add_special_tokens":false}`)
	if tok.Count != CountChat(msgs, false) || !slices.Equal(tok.Tokens, Tokenize(RenderChat(msgs, false))) || tok.MaxModelLen != 1234 {
		t.Fatalf("add_generation_prompt=false: %+v", tok)
	}
	if tok := tokenizeCall(t, srv, `{"messages":`+m+`}`); tok.Count != CountChat(msgs, true) {
		t.Fatalf("default model, default add_generation_prompt: count %d, want %d", tok.Count, CountChat(msgs, true))
	}
	if tok := tokenizeCall(t, srv, `{"model":"other-model","prompt":"abcdefgh","add_special_tokens":true}`); !slices.Equal(tok.Tokens, Tokenize("abcdefgh")) {
		t.Fatalf("prompt form: %+v", tok)
	}
	if _, b := mustPost(t, srv.URL()+"/tokenize", `{"model":"test-model","prompt":""}`, nil); !strings.Contains(string(b), `"tokens":[]`) {
		t.Fatalf("empty prompt body = %s, want an empty tokens array", b)
	}

	tests := []struct {
		name, body string
		status     int
		errMsg     string
	}{
		{name: "unknown model", body: `{"model":"nope","prompt":"x"}`, status: 404, errMsg: "The model `nope` does not exist."},
		{name: "neither form", body: `{"model":"test-model"}`, status: 400, errMsg: "messages or prompt"},
		{name: "bad json", body: `{"model":`, status: 400, errMsg: "invalid JSON"},
		{name: "bad messages", body: `{"messages":"x"}`, status: 400, errMsg: "invalid messages"},
		{name: "bad content", body: `{"messages":[{"role":"user","content":5}]}`, status: 400, errMsg: "invalid messages"},
		{name: "bad prompt", body: `{"prompt":{"a":1}}`, status: 400, errMsg: "prompt must be"},
		{name: "too large", body: `{"prompt":"` + strings.Repeat("a", 600) + `"}`, status: 413, errMsg: "too large"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, b := mustPost(t, srv.URL()+"/tokenize", tc.body, nil)
			if resp.StatusCode != tc.status {
				t.Fatalf("status %d, want %d: %s", resp.StatusCode, tc.status, b)
			}
			assertErrorShape(t, CompatCurrent, b, tc.status, tc.errMsg)
		})
	}
	// /tokenize is not a generation: not captured, not counted.
	if s := srv.Mock().Stats(); s.Requests != 0 || s.Rejected != 0 || s.PromptTokens != 0 || len(srv.Mock().Requests()) != 0 {
		t.Fatalf("stats after /tokenize calls = %+v, captured %d", s, len(srv.Mock().Requests()))
	}
}

// TestUnknownModelSameOnTokenizeAndChat: /tokenize rejects an unknown model
// exactly as the chat endpoint does, in both compat modes.
func TestUnknownModelSameOnTokenizeAndChat(t *testing.T) {
	for _, mode := range []VLLMCompat{CompatCurrent, CompatLegacy} {
		srv := startMock(t, func(c *Config) { c.VLLMCompat = mode })
		chatResp, chatBody := mustPost(t, srv.URL()+"/v1/chat/completions", `{"model":"nope","messages":[]}`, nil)
		tokResp, tokBody := mustPost(t, srv.URL()+"/tokenize", `{"model":"nope","messages":[]}`, nil)
		if chatResp.StatusCode != tokResp.StatusCode || string(chatBody) != string(tokBody) {
			t.Fatalf("%s: chat %d %s, tokenize %d %s", mode, chatResp.StatusCode, chatBody, tokResp.StatusCode, tokBody)
		}
	}
}

// TestMaxTokensFinishReason: with max_tokens (or max_completion_tokens) at or
// below the mock's output length, exactly that many tokens come back with
// finish_reason "length"; above it, the full output with "stop".
func TestMaxTokensFinishReason(t *testing.T) {
	const output = 10
	srv := startMock(t, func(c *Config) { c.OutputTokens = LengthDist{Kind: DistFixed, A: output} })
	tests := []struct {
		name, limit string
		tokens      int
		finish      string
	}{
		{name: "no limit", limit: "", tokens: output, finish: "stop"},
		{name: "max_tokens below", limit: `,"max_tokens":3`, tokens: 3, finish: "length"},
		{name: "max_completion_tokens below", limit: `,"max_completion_tokens":4`, tokens: 4, finish: "length"},
		{name: "max_tokens equal", limit: `,"max_tokens":10`, tokens: output, finish: "length"},
		{name: "max_tokens above", limit: `,"max_tokens":20`, tokens: output, finish: "stop"},
		{name: "max_tokens 1", limit: `,"max_tokens":1`, tokens: 1, finish: "length"},
	}
	for _, tc := range tests {
		for _, ep := range []struct{ path, body string }{
			{"/v1/chat/completions", `{"model":"test-model","messages":[{"role":"user","content":"hi"}]`},
			{"/v1/completions", `{"model":"test-model","prompt":"hi"`},
		} {
			for _, extra := range []string{"", streamUsage} {
				t.Run(tc.name+ep.path+extra, func(t *testing.T) {
					g := generate(t, srv, ep.path, ep.body+tc.limit+extra+"}")
					if g.tokens != tc.tokens || g.finish != tc.finish || g.usage.CompletionTokens != tc.tokens ||
						g.usage.TotalTokens != g.usage.PromptTokens+tc.tokens {
						t.Fatalf("got %d tokens, finish %q, usage %+v; want %d tokens, finish %q", g.tokens, g.finish, g.usage, tc.tokens, tc.finish)
					}
				})
			}
		}
	}
}

// commonPrefixHits is the prefix-cache hit count the stand-in predicts for a
// request after another one: whole 4-byte tokens shared from the start of the
// two rendered prompts, rounded down to whole blocks.
func commonPrefixHits(a, b string) int {
	n := 0
	for n < len(a) && n < len(b) && a[n] == b[n] {
		n++
	}
	return n / BytesPerToken / PrefixBlockTokens * PrefixBlockTokens
}

func hitsDelta(t *testing.T, srv *Server, path, body string) (hits, prompt int) {
	t.Helper()
	before := srv.Mock().Stats()
	g := generate(t, srv, path, body)
	after := srv.Mock().Stats()
	if q := int(after.PromptTokens - before.PromptTokens); q != g.usage.PromptTokens {
		t.Fatalf("prefix-cache queries grew by %d, usage.prompt_tokens %d", q, g.usage.PromptTokens)
	}
	return int(after.PrefixCacheHits - before.PrefixCacheHits), g.usage.PromptTokens
}

func TestPrefixCacheHits(t *testing.T) {
	static := staticText(4500)
	chat := func(msgs ...Message) string {
		return `{"model":"test-model","messages":` + mustJSON(t, msgs) + `}`
	}

	t.Run("shared 4500-char static prefix", func(t *testing.T) {
		srv := startMock(t, nil)
		a := []Message{{Role: "system", Content: static}, {Role: "user", Content: "Ticket 1001: refund for a damaged parcel?"}}
		b := []Message{{Role: "system", Content: static}, {Role: "user", Content: "Ticket 2002: can I change my delivery address?"}}
		if hits, _ := hitsDelta(t, srv, "/v1/chat/completions", chat(a...)); hits != 0 {
			t.Fatalf("cold request hit %d tokens", hits)
		}
		hits, prompt := hitsDelta(t, srv, "/v1/chat/completions", chat(b...))
		want := commonPrefixHits(RenderChat(a, true), RenderChat(b, true))
		staticBlocks := CountText(RenderChat(b[:1], false)) / PrefixBlockTokens
		if hits != want || hits < staticBlocks*PrefixBlockTokens {
			t.Fatalf("second request hit %d of %d tokens, want %d (static part spans %d full blocks)", hits, prompt, want, staticBlocks)
		}
		metrics := getBody(t, srv.URL()+"/metrics")
		for _, line := range []string{
			fmt.Sprintf(`vllm:prefix_cache_hits_total{model_name="test-model"} %d`, hits),
			fmt.Sprintf(`vllm:prefix_cache_queries_total{model_name="test-model"} %d`, srv.Mock().Stats().PromptTokens),
			`vllm:prefix_cache_hits_total{model_name="other-model"} 0`,
		} {
			if !strings.Contains(metrics, line+"\n") {
				t.Errorf("/metrics missing %q", line)
			}
		}
		// The cache is per model.
		if hits, _ := hitsDelta(t, srv, "/v1/chat/completions", strings.Replace(chat(b...), "test-model", "other-model", 1)); hits != 0 {
			t.Fatalf("other model hit %d tokens of test-model's cache", hits)
		}
	})

	t.Run("difference at char 732", func(t *testing.T) {
		srv := startMock(t, nil)
		long := staticText(4900)
		varied := long[:732] + "#" + long[733:]
		a := []Message{{Role: "user", Content: long}}
		b := []Message{{Role: "user", Content: varied}}
		hitsDelta(t, srv, "/v1/chat/completions", chat(a...))
		hits, prompt := hitsDelta(t, srv, "/v1/chat/completions", chat(b...))
		// "<|user|>\n" is 9 bytes, so the prompts share 741 bytes: 185
		// whole tokens, 11 whole blocks.
		if want := commonPrefixHits(RenderChat(a, true), RenderChat(b, true)); hits != want || hits != 11*PrefixBlockTokens {
			t.Fatalf("hit %d of %d tokens, want %d (11 blocks before char 732)", hits, prompt, want)
		}
	})

	t.Run("completions prompt", func(t *testing.T) {
		srv := startMock(t, nil)
		body := mustJSON(t, map[string]any{"model": "test-model", "prompt": static + " tail one"})
		hitsDelta(t, srv, "/v1/completions", body)
		hits, _ := hitsDelta(t, srv, "/v1/completions", mustJSON(t, map[string]any{"model": "test-model", "prompt": static + " tail two", "stream": true, "stream_options": map[string]bool{"include_usage": true}}))
		if want := commonPrefixHits(static+" tail one", static+" tail two"); hits != want || hits == 0 {
			t.Fatalf("hits %d, want %d", hits, want)
		}
	})

	t.Run("disabled", func(t *testing.T) {
		srv := startMock(t, func(c *Config) { c.PrefixCacheBlocks = 0 })
		body := chat(Message{Role: "system", Content: static})
		hitsDelta(t, srv, "/v1/chat/completions", body)
		if hits, prompt := hitsDelta(t, srv, "/v1/chat/completions", body); hits != 0 || prompt == 0 {
			t.Fatalf("disabled cache: hits %d, prompt %d", hits, prompt)
		}
	})

	t.Run("reset_prefix_cache", func(t *testing.T) {
		srv := startMock(t, nil)
		body := chat(Message{Role: "system", Content: static})
		hitsDelta(t, srv, "/v1/chat/completions", body)
		if hits, _ := hitsDelta(t, srv, "/v1/chat/completions", body); hits == 0 {
			t.Fatal("repeat did not hit")
		}
		if resp, b := mustPost(t, srv.URL()+"/reset_prefix_cache", "", nil); resp.StatusCode != http.StatusOK || len(b) != 0 {
			t.Fatalf("reset: %d %q", resp.StatusCode, b)
		}
		if hits, _ := hitsDelta(t, srv, "/v1/chat/completions", body); hits != 0 {
			t.Fatalf("after reset hit %d tokens", hits)
		}
	})
}

// TestPrefixCacheConcurrentRequests runs mixed requests sharing a prefix in
// parallel (run with -race) against a small cache and checks the counters
// and the bound.
func TestPrefixCacheConcurrentRequests(t *testing.T) {
	const capacity = 64
	srv := startMock(t, func(c *Config) { c.PrefixCacheBlocks = capacity })
	static := staticText(2000)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 10 {
				suffix := fmt.Sprintf(" request %d-%d %s", g, i, staticText(300+g*7))
				var path, body string
				if i%2 == 0 {
					path = "/v1/chat/completions"
					body = mustJSON(t, map[string]any{"model": "test-model", "stream": i%4 == 0, "messages": []Message{{Role: "system", Content: static}, {Role: "user", Content: suffix}}})
				} else {
					path = "/v1/completions"
					body = mustJSON(t, map[string]any{"model": "test-model", "prompt": static + suffix})
				}
				resp, err := post(t, context.Background(), srv.URL()+path, body, nil)
				if err != nil {
					t.Errorf("POST: %v", err)
					return
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					t.Errorf("status %d", resp.StatusCode)
				}
			}
		})
	}
	wg.Wait()
	s := srv.Mock().Stats()
	if s.PrefixCacheHits == 0 || s.PrefixCacheHits > s.PromptTokens || s.PrefixCacheHits%PrefixBlockTokens != 0 {
		t.Fatalf("hits %d, queries %d", s.PrefixCacheHits, s.PromptTokens)
	}
	if n := srv.Mock().caches[testModel].len(); n > capacity {
		t.Fatalf("cache holds %d blocks, capacity %d", n, capacity)
	}
}
