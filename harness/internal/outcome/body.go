package outcome

import (
	"bytes"
	"encoding/json"
	"time"
)

// Response body ceilings; the harness never holds unbounded data.
const (
	maxSSELine      = 1 << 20  // one SSE line
	maxResponseBody = 16 << 20 // a whole non-streaming body
)

// Response is what a response body showed, from send to its end.
type Response struct {
	// Err is the error kind; "" when the response was complete.
	Err      string
	Complete bool
	// ErrorEvent: a stream carried an SSE error event.
	ErrorEvent bool
	// TTFT is send to the first token-bearing event of a stream, or to the
	// end of a body or of a stream with none (the caller sees nothing before).
	TTFT time.Duration
	// FirstByte is send to the first body byte; zero when none arrived.
	FirstByte    time.Duration
	Duration     time.Duration
	ITLMean      time.Duration
	ITLMax       time.Duration
	Chunks       int // token-bearing SSE events
	OutputTokens int
	PromptTokens int
}

// Body measures one 2xx response body as it arrives. Feed it every read with
// Write, stop reading once Done, then End it. It is not safe for concurrent
// use.
type Body struct {
	stream bool
	start  time.Time
	onGap  func(time.Duration)

	line     []byte // a stream's partial line, carried between writes
	body     []byte // a non-streaming body
	failed   string // error kind that ends measuring: an error event, an oversized line or body
	bad      bool   // a stream event was not valid JSON (measuring goes on, see streamLine)
	done     bool   // a stream reached data: [DONE]
	finish   bool   // a choice ended with a finish_reason
	seenByte bool   // a body byte arrived
	last     time.Time
	gapSum   time.Duration
	r        Response
}

// NewBody starts measuring the body of a request sent at start. stream says
// whether it is an SSE stream; onGap (may be nil) is called with every
// inter-token gap as it happens.
func NewBody(stream bool, start time.Time, onGap func(time.Duration)) *Body {
	return &Body{stream: stream, start: start, onGap: onGap}
}

// Done reports whether the body has said all that matters: a stream reached
// data: [DONE], or the body showed a failure that ends measuring. The
// caller stops reading then.
func (b *Body) Done() bool { return b.done || b.failed != "" }

// Write records p, the bytes of one read, received at at.
func (b *Body) Write(p []byte, at time.Time) {
	if len(p) == 0 || b.Done() {
		return
	}
	if !b.seenByte {
		b.seenByte, b.r.FirstByte = true, at.Sub(b.start)
	}
	if !b.stream {
		if len(b.body)+len(p) > maxResponseBody {
			b.failed = ErrBadResponse
			return
		}
		b.body = append(b.body, p...)
		return
	}
	for len(p) > 0 && !b.Done() {
		i := bytes.IndexByte(p, '\n')
		if n := len(p); i >= 0 {
			n = i
		} else if len(b.line)+n > maxSSELine {
			b.failed = ErrBadResponse
			return
		} else {
			b.line = append(b.line, p...)
			return
		}
		if len(b.line)+i > maxSSELine {
			b.failed = ErrBadResponse
			return
		}
		line := p[:i]
		if len(b.line) > 0 {
			line = append(b.line, line...)
			b.line = b.line[:0]
		}
		b.streamLine(bytes.TrimSuffix(line, []byte("\r")), at)
		p = p[i+1:]
	}
}

// streamChunk holds the fields of an SSE chunk the harness inspects.
type streamChunk struct {
	Choices []struct {
		Text  string `json:"text"`
		Delta struct {
			Content          string          `json:"content"`
			ReasoningContent string          `json:"reasoning_content"`
			ToolCalls        json.RawMessage `json:"tool_calls"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *usage          `json:"usage"`
	Error json.RawMessage `json:"error"`
}

type usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
}

func (c *streamChunk) hasToken() bool {
	for _, ch := range c.Choices {
		d := ch.Delta
		if ch.Text != "" || d.Content != "" || d.ReasoningContent != "" || (len(d.ToolCalls) > 0 && string(d.ToolCalls) != "null") {
			return true
		}
	}
	return false
}

// streamLine handles one SSE line received at at.
func (b *Body) streamLine(line []byte, at time.Time) {
	data, ok := bytes.CutPrefix(line, []byte("data:"))
	if !ok {
		return // blank separators, comments, event/id fields
	}
	data = bytes.TrimSpace(data)
	if bytes.Equal(data, []byte("[DONE]")) {
		b.done = true
		return
	}
	var chunk streamChunk
	if err := json.Unmarshal(data, &chunk); err != nil {
		// Keep reading: when a replica dies mid-event, the gateway closes the
		// half-sent event and then sends an error event, which decides the
		// kind (stream_error).
		b.bad = true
		return
	}
	if len(chunk.Error) > 0 && string(chunk.Error) != "null" {
		b.failed, b.r.ErrorEvent = ErrStreamError, true
		return
	}
	if chunk.Usage != nil {
		b.r.PromptTokens, b.r.OutputTokens = chunk.Usage.PromptTokens, chunk.Usage.CompletionTokens
	}
	for _, ch := range chunk.Choices {
		if ch.FinishReason != nil && *ch.FinishReason != "" {
			b.finish = true
		}
	}
	if !chunk.hasToken() {
		return
	}
	b.r.Chunks++
	if b.last.IsZero() {
		b.r.TTFT = at.Sub(b.start)
	} else {
		gap := at.Sub(b.last)
		b.gapSum += gap
		b.r.ITLMax = max(b.r.ITLMax, gap)
		if b.onGap != nil {
			b.onGap(gap)
		}
	}
	b.last = at
}

// End finishes the body at at. readErr is the kind of a transport failure
// that ended reading early ("" when the body ended on its own, or reading
// stopped because Done). A failure the body showed takes precedence (an
// error event before an event that was not JSON); then a stream that
// reached data: [DONE] after a finish_reason is complete whatever happened
// to the connection after it; then readErr; anything else is truncated,
// including a last line with no line ending (an SSE event ends with a
// newline).
func (b *Body) End(at time.Time, readErr string) Response {
	r := b.r
	r.Duration = at.Sub(b.start)
	if b.stream {
		switch {
		case b.failed != "":
			r.Err = b.failed
		case b.bad:
			r.Err = ErrBadResponse
		case b.done && b.finish:
		case readErr != "":
			r.Err = readErr
		default:
			r.Err = ErrTruncated // no [DONE], or [DONE] without a finish_reason
		}
		if r.Chunks == 0 {
			r.TTFT = r.Duration // no token-bearing event: the caller saw nothing before the end, as for a body
		}
		if r.Chunks > 1 {
			r.ITLMean = b.gapSum / time.Duration(r.Chunks-1)
		}
		if r.OutputTokens == 0 {
			r.OutputTokens = r.Chunks // no usage count: one token per event, as vLLM streams
		}
	} else {
		r.TTFT = r.Duration
		switch {
		case b.failed != "":
			r.Err = b.failed
		case readErr != "":
			r.Err = readErr
		default:
			r.Err = b.parseBody(&r)
		}
	}
	r.Complete = r.Err == ""
	return r
}

// parseBody reads a non-streaming body's usage and returns its error kind:
// bad_response unless it is JSON with at least one choice (a body with no
// choices is not the response a generation gives), truncated unless every
// choice has a finish_reason.
func (b *Body) parseBody(r *Response) string {
	var v struct {
		Choices []struct {
			FinishReason *string `json:"finish_reason"`
		} `json:"choices"`
		Usage *usage `json:"usage"`
	}
	if err := json.Unmarshal(b.body, &v); err != nil || len(v.Choices) == 0 {
		return ErrBadResponse
	}
	if v.Usage != nil {
		r.PromptTokens, r.OutputTokens = v.Usage.PromptTokens, v.Usage.CompletionTokens
	}
	for _, c := range v.Choices {
		if c.FinishReason == nil || *c.FinishReason == "" {
			return ErrTruncated
		}
	}
	return ""
}
