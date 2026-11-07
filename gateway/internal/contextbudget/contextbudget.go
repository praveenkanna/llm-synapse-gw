// Package contextbudget trims a chat request's history to a token budget
// (Level 3, ADR 0019). It reads only the history in the request's own
// "messages": every leading system and developer message and the last turn
// are always kept, and whole turns are dropped oldest first until the
// prompt fits. A turn is a user message and the messages after it up to the
// next user message, so a tool call is never separated from its result.
//
// Messages are never decoded and re-encoded: kept ones are the caller's
// bytes and dropped ones are cut out as one byte range, with the comma that
// separated them. Nothing is edited or reordered.
package contextbudget

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// Fail-open reasons, for gateway_failopen_total{stage="context"}.
const (
	ReasonNoReplica       = "no_replica"
	ReasonProbeFailed     = "probe_failed"
	ReasonProbeTimeout    = "probe_timeout"
	ReasonProbesExhausted = "probes_exhausted"
	// ReasonUnmeasurable: the body has a member that changes the render
	// but that /tokenize does not take, so a count would be too low.
	ReasonUnmeasurable = "unmeasurable"
	// ReasonMultimodal: a message has a non-text content part; every probe
	// would make the replica process the media.
	ReasonMultimodal = "multimodal"
)

// ErrOverBudget is returned when the messages that are always kept are
// over the budget on their own.
var ErrOverBudget = errors.New("contextbudget: the leading system messages and the last turn are over the budget")

// SkipError is an assembly that could not run; the request goes on
// unchanged. Reason is one of the Reason constants.
type SkipError struct {
	Reason string
	Err    error
}

func (e *SkipError) Error() string { return "contextbudget: " + e.Reason + ": " + e.Err.Error() }
func (e *SkipError) Unwrap() error { return e.Err }

// Counter measures prompts.
type Counter interface {
	// Count returns the prompt tokens of the request with messages, a JSON
	// array, as its "messages".
	Count(ctx context.Context, messages []byte) (int, error)
}

// Request is a parsed chat completions body.
type Request struct {
	body        []byte
	messages    bodyindex.Member // the top-level "messages" read
	open, close int              // offsets of the messages array's '[' and ']'
	elems       [][2]int         // byte range of each message
	prefix      int              // messages always kept at the start
	turns       []int            // index of each turn's first message
	// passthrough holds the top-level members a /tokenize call needs to
	// count as the chat call will, as "key":value pairs.
	passthrough [][]byte
	genPrompt   bool // the body sets add_generation_prompt itself
}

// Members copied from the body into a /tokenize call: they change how the
// chat template renders.
var tokenizeMembers = []string{"model", "tools", "chat_template", "chat_template_kwargs",
	"add_generation_prompt", "continue_final_message", "add_special_tokens", "mm_processor_kwargs"}

// Members that change the render but that /tokenize does not take.
var unmeasurableMembers = []string{"documents", "reasoning_effort"}

// Parse reads the top-level "messages" (the last one, as vLLM's parser
// does) of the body ix indexes (the request's one index, read by ingress,
// AD-15) and finds its turns. The kept prefix is every leading system and
// developer message, and at least the first keep messages (a matched
// template's static prefix). It fails with a *SkipError unless the body is
// a JSON object whose messages are an array of objects with a string role
// and text-only content, and that /tokenize can count as the chat call. A
// body past bodyindex.MaxMembers is not assembled (too_many_members): the
// index holds none of its members. q keeps the body, never ix.
func Parse(ix *bodyindex.Index, keep int) (*Request, error) {
	q, err := parse(ix, keep)
	var se *SkipError
	if err != nil && !errors.As(err, &se) {
		err = &SkipError{Reason: telemetry.ReasonUnparseableBody, Err: err}
	}
	return q, err
}

func parse(ix *bodyindex.Index, keep int) (*Request, error) {
	switch err := ix.Err(); {
	case errors.Is(err, bodyindex.ErrTooManyMembers):
		return nil, &SkipError{Reason: telemetry.ReasonTooManyMembers, Err: err}
	case err != nil:
		return nil, err
	}
	for _, m := range unmeasurableMembers {
		if _, n := ix.Find(m); n > 0 {
			return nil, &SkipError{Reason: ReasonUnmeasurable, Err: fmt.Errorf("/tokenize does not take %q", m)}
		}
	}
	msgs, _ := ix.Find("messages")
	if msgs.Kind != bodyindex.Array {
		return nil, errors.New(`"messages" is not an array`)
	}
	body := ix.Body()
	q := &Request{body: body, messages: msgs, open: msgs.Value[0], close: msgs.Value[1] - 1}
	for _, m := range tokenizeMembers {
		if v, n := ix.Find(m); n > 0 {
			q.passthrough = append(q.passthrough, fmt.Appendf(nil, "%q:%s", m, ix.Value(v)))
			q.genPrompt = q.genPrompt || m == "add_generation_prompt"
		}
	}
	var roles []string
	for i := bodyindex.SkipSpace(body, q.open+1); body[i] != ']'; {
		end := bodyindex.SkipValue(body, i)
		role, err := readMessage(body, i)
		if err != nil {
			var se *SkipError
			if errors.As(err, &se) {
				return nil, err
			}
			return nil, fmt.Errorf("messages[%d]: %w", len(roles), err)
		}
		q.elems = append(q.elems, [2]int{i, end})
		roles = append(roles, role)
		if i = bodyindex.SkipSpace(body, end); body[i] == ',' {
			i = bodyindex.SkipSpace(body, i+1)
		}
	}
	for q.prefix < len(roles) && (roles[q.prefix] == "system" || roles[q.prefix] == "developer") {
		q.prefix++
	}
	if keep = min(keep, len(roles)); keep > q.prefix {
		// A template prefix may end inside a turn (on a user message): the
		// rest of that turn is kept with it, so a turn is never split.
		q.prefix = keep
		for q.prefix < len(roles) && roles[q.prefix] != "user" {
			q.prefix++
		}
	}
	for i := q.prefix; i < len(roles); i++ {
		if i == q.prefix || roles[i] == "user" {
			q.turns = append(q.turns, i)
		}
	}
	return q, nil
}

// Result is an assembled request.
type Result struct {
	Body            []byte // the body to send; the caller's own when nothing was dropped
	DroppedMessages int
	DroppedTurns    int
	Probes          int // Counter calls made
	Tokens          int // exact prompt token count of Body from /tokenize
	// Approximate marks a body that fits the budget but that may not keep
	// the most turns that would: measuring stopped (probes ran out, or one
	// failed) after a smaller suffix was proven to fit.
	Approximate bool
}

// Assemble returns the body with the most recent turns that fit budget,
// measured by c in at most maxProbes calls. It returns ErrOverBudget when
// even the last turn alone does not fit, and a *SkipError when it cannot
// measure before any suffix is proven to fit. A prompt's count must not
// shrink as turns are added for the result to be the largest that fits;
// whatever the counter, the result's count is within budget.
//
// ix is the index q was parsed from. A body with turns dropped is spliced
// through it (the kept messages replace the messages value), so ix then
// indexes Result.Body: the splices after assembly read fresh offsets.
func (q *Request) Assemble(ctx context.Context, ix *bodyindex.Index, budget, maxProbes int, c Counter) (Result, error) {
	var res Result
	var buf []byte
	measure := func(k int) (int, bool, error) {
		if res.Probes == maxProbes {
			return 0, false, &SkipError{Reason: ReasonProbesExhausted, Err: fmt.Errorf("%d probes did not settle the count", maxProbes)}
		}
		res.Probes++
		buf = q.cut(buf[:0], q.open, q.close+1, k)
		n, err := c.Count(ctx, buf)
		if err != nil {
			var se *SkipError
			switch {
			case errors.As(err, &se):
				return 0, false, err
			case errors.Is(err, context.DeadlineExceeded):
				return 0, false, &SkipError{Reason: ReasonProbeTimeout, Err: err}
			}
			return 0, false, &SkipError{Reason: ReasonProbeFailed, Err: err}
		}
		return n, n <= budget, nil
	}
	n := len(q.turns)
	tok, ok, err := measure(n)
	if err != nil || ok {
		res.Body = q.body
		if err == nil {
			res.Tokens = tok
		}
		return res, err
	}
	// lo is the most turns known to fit (0: none yet), hi the fewest known
	// not to.
	lo, hi, loTokens, hiTokens := 0, n, 0, tok
	for hi-lo > 1 {
		mid := lo + (hi-lo)/2
		if hiTokens > loTokens && budget >= loTokens && budget < hiTokens {
			secant := lo + int(float64(budget-loTokens)/float64(hiTokens-loTokens)*float64(hi-lo))
			if secant > lo && secant < hi {
				mid = secant
			}
		}
		if tok, ok, err = measure(mid); err != nil {
			if lo == 0 {
				return res, err
			}
			res.Approximate = true
			break
		}
		if ok {
			lo, loTokens = mid, tok
		} else {
			hi, hiTokens = mid, tok
		}
	}
	if lo == 0 {
		return res, ErrOverBudget
	}
	// The kept messages, in the probes' buffer (the first probe sized it
	// for them all); the splice copies them into the new body.
	buf = q.cut(buf[:0], q.open, q.close+1, lo)
	res.Body = ix.ReplaceValue(q.messages, buf, ix)
	res.DroppedMessages = q.firstKept(lo) - q.prefix
	res.DroppedTurns = n - lo
	res.Tokens = loTokens
	return res, nil
}

// firstKept is the index of the first message after the prefix that is
// kept when the last k turns are.
func (q *Request) firstKept(k int) int {
	if k == 0 {
		return len(q.elems)
	}
	return q.turns[len(q.turns)-k]
}

// cut appends body[lo:hi] to dst without the messages between the prefix
// and the last k turns.
func (q *Request) cut(dst []byte, lo, hi, k int) []byte {
	first := q.firstKept(k)
	if first == q.prefix {
		return append(dst, q.body[lo:hi]...)
	}
	var from, to int
	if q.prefix > 0 { // from the end of the prefix to the end of the last dropped message
		from, to = q.elems[q.prefix-1][1], q.elems[first-1][1]
	} else { // from the first message to the first kept one
		from, to = q.elems[0][0], q.elems[first][0]
	}
	return append(append(dst, q.body[lo:from]...), q.body[to:hi]...)
}

// TokenizeBody returns a vLLM /tokenize request for the chat request with
// messages as its "messages", carrying the members that shape its render.
// add_generation_prompt is true unless the body sets it, as for chat
// completions.
func (q *Request) TokenizeBody(messages []byte) []byte {
	b := append([]byte(`{"messages":`), messages...)
	for _, m := range q.passthrough {
		b = append(append(b, ','), m...)
	}
	if !q.genPrompt {
		b = append(b, `,"add_generation_prompt":true`...)
	}
	return append(b, '}')
}

// readMessage returns the "role" of the message object starting at b[i].
// A content part that is not text is a *SkipError (ReasonMultimodal).
func readMessage(b []byte, i int) (string, error) {
	if b[i] != '{' {
		return "", errors.New("not an object")
	}
	role, found := "", false
	err := eachMember(b, i, func(key []byte, v [2]int) error {
		switch {
		case bodyindex.KeyIs(key, "role"):
			found = true
			return json.Unmarshal(b[v[0]:v[1]], &role)
		case bodyindex.KeyIs(key, "content"):
			return textOnly(b, v[0])
		}
		return nil
	})
	if err == nil && !found {
		err = errors.New("no role")
	}
	return role, err
}

// textOnly fails with ReasonMultimodal when the content value at b[i] is
// an array with a part whose "type" is not "text".
func textOnly(b []byte, i int) error {
	if b[i] != '[' {
		return nil
	}
	for i = bodyindex.SkipSpace(b, i+1); b[i] != ']'; {
		typ := ""
		if b[i] == '{' {
			_ = eachMember(b, i, func(key []byte, v [2]int) error {
				if bodyindex.KeyIs(key, "type") {
					_ = json.Unmarshal(b[v[0]:v[1]], &typ)
				}
				return nil
			})
		}
		if typ != "text" {
			return &SkipError{Reason: ReasonMultimodal, Err: fmt.Errorf("a %q content part", typ)}
		}
		if i = bodyindex.SkipSpace(b, bodyindex.SkipValue(b, i)); b[i] == ',' {
			i = bodyindex.SkipSpace(b, i+1)
		}
	}
	return nil
}

// eachMember calls fn with each member of the valid JSON object starting
// at b[i], a message or a content part (the body's top level is the
// index's): its key, quotes included, for bodyindex.KeyIs, which never decodes
// one a caller made long, and its value's byte range.
func eachMember(b []byte, i int, fn func(key []byte, v [2]int) error) error {
	if b[i] != '{' {
		return errors.New("not an object")
	}
	for i = bodyindex.SkipSpace(b, i+1); b[i] != '}'; {
		ks := i
		i = bodyindex.SkipString(b, i)
		key := b[ks:i]
		vs := bodyindex.SkipSpace(b, bodyindex.SkipSpace(b, i)+1) // past ':'
		i = bodyindex.SkipValue(b, vs)
		if err := fn(key, [2]int{vs, i}); err != nil {
			return err
		}
		if i = bodyindex.SkipSpace(b, i); b[i] == ',' {
			i = bodyindex.SkipSpace(b, i+1)
		}
	}
	return nil
}
