package contextbudget

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

// PathTokenize is vLLM's tokenizer endpoint on a replica.
const PathTokenize = "/tokenize"

// maxTokenizeResponseBytes bounds a /tokenize response read; it holds a
// token id per prompt token.
const maxTokenizeResponseBytes = 8 << 20

// Sender sends one request to a replica (*upstream.Client).
type Sender interface {
	Do(ctx context.Context, replica discovery.Replica, req upstream.Request) (*http.Response, error)
}

// Tokenizer counts prompts on a pool's replicas through /tokenize. It is
// safe for concurrent use.
type Tokenizer struct {
	sender  Sender
	timeout time.Duration
	next    atomic.Uint64 // spreads requests over the pool's replicas
}

// NewTokenizer returns a Tokenizer whose every call is bounded by timeout.
func NewTokenizer(s Sender, timeout time.Duration) *Tokenizer {
	return &Tokenizer{sender: s, timeout: timeout}
}

// Counter returns a Counter for q on a pool whose replicas are all and
// whose healthy ones are healthy. A request starts on a healthy replica
// (any, when none is) and moves on only when a call reached none (a
// connect failure), trying the other healthy replicas before the rest;
// once a replica answers, every later probe of the request goes to it.
func (t *Tokenizer) Counter(healthy, all []discovery.Replica, q *Request) Counter {
	replicas := t.order(healthy, all)
	start := 0
	return counterFunc(func(ctx context.Context, messages []byte) (int, error) {
		if len(replicas) == 0 {
			return 0, &SkipError{Reason: ReasonNoReplica, Err: errors.New("the pool has no replicas")}
		}
		body := q.TokenizeBody(messages)
		var err error
		for range replicas {
			var n int
			n, err = t.count(ctx, replicas[start], body)
			if !upstream.Retryable(err) {
				return n, err
			}
			start = (start + 1) % len(replicas)
		}
		return 0, &SkipError{Reason: ReasonNoReplica, Err: err}
	})
}

// order returns healthy, rotated so requests spread over them, followed by
// the replicas of all that are not healthy.
func (t *Tokenizer) order(healthy, all []discovery.Replica) []discovery.Replica {
	first := healthy
	if len(first) == 0 {
		first = all
	}
	out := make([]discovery.Replica, 0, len(all))
	if n := len(first); n > 0 {
		k := int(t.next.Add(1) % uint64(n))
		out = append(append(out, first[k:]...), first[:k]...)
	}
	for _, r := range all {
		if !slices.ContainsFunc(out, func(o discovery.Replica) bool { return o.Name == r.Name }) {
			out = append(out, r)
		}
	}
	return out
}

type counterFunc func(ctx context.Context, messages []byte) (int, error)

func (f counterFunc) Count(ctx context.Context, messages []byte) (int, error) {
	return f(ctx, messages)
}

// count sends one /tokenize call. Errors never quote the prompt.
func (t *Tokenizer) count(ctx context.Context, rep discovery.Replica, body []byte) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	resp, err := t.sender.Do(ctx, rep, upstream.Request{
		Method: http.MethodPost,
		Path:   PathTokenize,
		Header: http.Header{"Content-Type": {"application/json"}},
		Body:   body,
	})
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) || upstream.KindOf(err) == upstream.KindTimeout {
			return 0, &SkipError{Reason: ReasonProbeTimeout, Err: fmt.Errorf("replica %s: /tokenize timed out", rep.Name)}
		}
		return 0, fmt.Errorf("replica %s: /tokenize failed: %w", rep.Name, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("replica %s: /tokenize answered status %d", rep.Name, resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxTokenizeResponseBytes+1))
	if err != nil || len(data) > maxTokenizeResponseBytes {
		return 0, fmt.Errorf("replica %s: /tokenize response unreadable or over %d bytes", rep.Name, maxTokenizeResponseBytes)
	}
	open, ok := bodyindex.ValidObject(data)
	if !ok {
		return 0, fmt.Errorf("replica %s: /tokenize response is not a token count", rep.Name)
	}
	m, found := bodyindex.FindLast(data, open, "count")
	if !found || m.Kind != bodyindex.Number {
		return 0, fmt.Errorf("replica %s: /tokenize response is not a token count", rep.Name)
	}
	count, err := strconv.Atoi(string(data[m.Value[0]:m.Value[1]]))
	if err != nil || count < 0 {
		return 0, fmt.Errorf("replica %s: /tokenize response is not a token count", rep.Name)
	}
	return count, nil
}
