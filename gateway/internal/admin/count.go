package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

// PathTokenize is vLLM's tokenizer endpoint on a replica.
const PathTokenize = "/tokenize"

// maxTokenizeResponseBytes bounds a /tokenize response read; one holds a
// token id per prompt token.
const maxTokenizeResponseBytes = 8 << 20

// maxErrorDrainBytes bounds what is read of a non-2xx response before
// closing it; its body is never reported (it may quote the prompt).
const maxErrorDrainBytes = 64 << 10

// Variable values of the two synthetic renders whose common token prefix
// is prefix_tokens. They differ in their first byte, so the renders part
// at the first variable.
const (
	syntheticValueA = "aaaaaaaa"
	syntheticValueB = "zzzzzzzz"
)

// tokenizeRequest is vLLM's chat-form /tokenize body, with the defaults
// chat completions uses.
type tokenizeRequest struct {
	Model               string                      `json:"model"`
	Messages            []templates.RenderedMessage `json:"messages"`
	AddGenerationPrompt bool                        `json:"add_generation_prompt"`
}

type tokenizeReply struct {
	Count  int   `json:"count"`
	Tokens []int `json:"tokens"`
}

// tokenized is one /tokenize result.
type tokenized struct {
	count   int
	tokens  []int
	replica string
}

// poolFor returns the pool counting uses for model: its mapped pool, or the
// default pool for an unmapped model.
func (s *Server) poolFor(model string) (string, error) {
	if pool, _ := s.o.PoolFor(model); pool != "" {
		return pool, nil
	}
	return "", errors.New("model is not mapped to a pool")
}

// countSpec computes a version's registration counts. On any failure the
// result holds only Error, a short message that never quotes text.
func (s *Server) countSpec(ctx context.Context, spec templates.Spec) templates.Counts {
	pool, err := s.poolFor(spec.Model)
	if err != nil {
		return templates.Counts{Error: err.Error()}
	}
	prefix, err := s.prefixTokens(ctx, pool, spec)
	if err != nil {
		return templates.Counts{Error: "prefix: " + err.Error()}
	}
	c := templates.Counts{Pool: pool, PrefixTokens: prefix}
	for i, ex := range spec.Examples {
		msgs, err := templates.Render(spec, ex)
		if err == nil {
			var t tokenized
			if t, err = s.tokenize(ctx, pool, spec.Model, msgs); err == nil {
				c.Examples = append(c.Examples, t.count)
				continue
			}
		}
		return templates.Counts{Error: fmt.Sprintf("examples[%d]: %v", i, err)}
	}
	return c
}

// prefixTokens is the longest common token prefix of two renders whose
// variables all differ: what every render shares. A template without
// variables has one render, all of it shared.
func (s *Server) prefixTokens(ctx context.Context, pool string, spec templates.Spec) (int, error) {
	a := make(map[string]string, len(spec.Variables))
	b := make(map[string]string, len(spec.Variables))
	for _, v := range spec.Variables {
		a[v.Name], b[v.Name] = syntheticValueA, syntheticValueB
	}
	ta, err := s.renderTokenize(ctx, pool, spec, a)
	if err != nil || len(spec.Variables) == 0 {
		return ta.count, err
	}
	tb, err := s.renderTokenize(ctx, pool, spec, b)
	if err != nil {
		return 0, err
	}
	n := 0
	for n < len(ta.tokens) && n < len(tb.tokens) && ta.tokens[n] == tb.tokens[n] {
		n++
	}
	return n, nil
}

func (s *Server) renderTokenize(ctx context.Context, pool string, spec templates.Spec, vars map[string]string) (tokenized, error) {
	msgs, err := templates.Render(spec, vars)
	if err != nil {
		return tokenized{}, err
	}
	return s.tokenize(ctx, pool, spec.Model, msgs)
}

// tokenize counts msgs on a replica of pool. Replicas are tried in order,
// moving on only when a call failed before any response; a response that
// is not a valid count ends the attempt.
func (s *Server) tokenize(ctx context.Context, pool, model string, msgs []templates.RenderedMessage) (tokenized, error) {
	reps := s.o.Discovery.Replicas(pool)
	if len(reps) == 0 {
		return tokenized{}, fmt.Errorf("no replicas in pool %s", pool)
	}
	body, err := json.Marshal(tokenizeRequest{Model: model, Messages: msgs, AddGenerationPrompt: true})
	if err != nil {
		return tokenized{}, fmt.Errorf("encode /tokenize request: %w", err)
	}
	var lastErr error
	for _, rep := range reps {
		if err := ctx.Err(); err != nil {
			return tokenized{}, fmt.Errorf("tokenize: %w", err)
		}
		t, sent, err := s.tokenizeOn(ctx, rep, body)
		if err == nil || sent {
			return t, err
		}
		lastErr = err
	}
	return tokenized{}, lastErr
}

// tokenizeOn sends one /tokenize call under TokenizeTimeout. sent reports
// that the replica answered, so trying another would not help.
func (s *Server) tokenizeOn(ctx context.Context, rep discovery.Replica, body []byte) (t tokenized, sent bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, s.o.TokenizeTimeout)
	defer cancel()
	resp, err := s.o.Sender.Do(ctx, rep, upstream.Request{
		Method: http.MethodPost,
		Path:   PathTokenize,
		Header: http.Header{"Content-Type": {"application/json"}},
		Body:   body,
	})
	if err != nil {
		return tokenized{}, false, fmt.Errorf("replica %s: /tokenize failed: %s", rep.Name, upstream.KindOf(err))
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxErrorDrainBytes))
		return tokenized{}, true, fmt.Errorf("replica %s: /tokenize answered status %d", rep.Name, resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxTokenizeResponseBytes+1))
	if err != nil {
		return tokenized{}, true, fmt.Errorf("replica %s: reading the /tokenize response failed: %s", rep.Name, upstream.KindOf(err))
	}
	if len(data) > maxTokenizeResponseBytes {
		return tokenized{}, true, fmt.Errorf("replica %s: /tokenize response is over %d bytes", rep.Name, maxTokenizeResponseBytes)
	}
	var reply tokenizeReply
	if err := json.Unmarshal(data, &reply); err != nil {
		return tokenized{}, true, fmt.Errorf("replica %s: /tokenize response is not a token count", rep.Name)
	}
	if reply.Count < 0 || len(reply.Tokens) != reply.Count {
		return tokenized{}, true, fmt.Errorf("replica %s: /tokenize count %d does not match its %d token ids",
			rep.Name, reply.Count, len(reply.Tokens))
	}
	return tokenized{count: reply.Count, tokens: reply.Tokens, replica: rep.Name}, true, nil
}
