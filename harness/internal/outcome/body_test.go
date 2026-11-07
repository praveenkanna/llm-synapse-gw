package outcome

import (
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)

// at returns t0 plus ms milliseconds.
func at(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }

const (
	roleChunk  = "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}\n\n"
	tokChunk   = "data: {\"choices\":[{\"delta\":{\"content\":\"tok\"},\"finish_reason\":null}]}\n\n"
	lastChunk  = "data: {\"choices\":[{\"delta\":{\"content\":\"end\"},\"finish_reason\":\"stop\"}]}\n\n"
	usageChunk = "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3}}\n\n"
	doneChunk  = "data: [DONE]\n\n"
	errChunk   = "data: {\"error\":{\"message\":\"upstream died\",\"code\":502}}\n\n"
)

// stream feeds writes to a stream body, the i-th at i*10 ms after send.
func stream(t *testing.T, onGap func(time.Duration), writes ...string) *Body {
	t.Helper()
	b := NewBody(true, t0, onGap)
	for i, w := range writes {
		b.Write([]byte(w), at((i+1)*10))
	}
	return b
}

func TestStreamComplete(t *testing.T) {
	var gaps []time.Duration
	b := stream(t, func(d time.Duration) { gaps = append(gaps, d) }, roleChunk, tokChunk, tokChunk, lastChunk, usageChunk, doneChunk)
	if !b.Done() {
		t.Fatal("not Done after [DONE]")
	}
	r := b.End(at(70), "")
	if r.Err != "" || !r.Complete {
		t.Fatalf("Err %q Complete %v, want a complete stream", r.Err, r.Complete)
	}
	// TTFT is the first token-bearing event (the 2nd write), not the role-only first byte.
	if r.FirstByte != 10*time.Millisecond || r.TTFT != 20*time.Millisecond {
		t.Fatalf("FirstByte %v TTFT %v, want 10ms and 20ms", r.FirstByte, r.TTFT)
	}
	if r.Chunks != 3 || r.OutputTokens != 3 || r.PromptTokens != 7 || r.Duration != 70*time.Millisecond {
		t.Fatalf("response = %+v", r)
	}
	if len(gaps) != 2 || r.ITLMean != 10*time.Millisecond || r.ITLMax != 10*time.Millisecond {
		t.Fatalf("gaps %v ITL mean %v max %v, want two 10ms gaps", gaps, r.ITLMean, r.ITLMax)
	}
}

// TestStreamLinesSplitAcrossReads: an event split over reads, with CRLF line
// endings, is measured when its last byte lands.
func TestStreamLinesSplitAcrossReads(t *testing.T) {
	crlf := strings.ReplaceAll(tokChunk, "\n", "\r\n")
	b := NewBody(true, t0, nil)
	b.Write([]byte(crlf[:15]), at(5))
	b.Write([]byte(crlf[15:]), at(25))
	b.Write([]byte(strings.ReplaceAll(lastChunk+doneChunk, "\n", "\r\n")), at(30))
	r := b.End(at(30), "")
	if !r.Complete || r.Chunks != 2 || r.FirstByte != 5*time.Millisecond || r.TTFT != 25*time.Millisecond {
		t.Fatalf("response = %+v, want complete, 2 chunks, first byte 5ms, TTFT 25ms", r)
	}
	if r.OutputTokens != 2 {
		t.Fatalf("OutputTokens = %d, want one per token event without usage", r.OutputTokens)
	}
}

func TestStreamFailures(t *testing.T) {
	tests := []struct {
		name    string
		writes  []string
		readErr string
		want    string
		event   bool
	}{
		{name: "no [DONE]", writes: []string{tokChunk, lastChunk}, want: ErrTruncated},
		{name: "[DONE] without a finish_reason", writes: []string{tokChunk, usageChunk, doneChunk}, want: ErrTruncated},
		{name: "error event", writes: []string{tokChunk, errChunk, lastChunk, doneChunk}, want: ErrStreamError, event: true},
		{name: "bad JSON event", writes: []string{tokChunk, "data: {not json\n\n", lastChunk, doneChunk}, want: ErrBadResponse},
		{name: "read failure before [DONE]", writes: []string{tokChunk}, readErr: ErrTimeout, want: ErrTimeout},
		{name: "error event wins over a read failure", writes: []string{errChunk}, readErr: ErrRead, want: ErrStreamError, event: true},
		{name: "line over the limit", writes: []string{"data: " + strings.Repeat("x", maxSSELine+1)}, want: ErrBadResponse},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := stream(t, nil, tc.writes...).End(at(100), tc.readErr)
			if r.Err != tc.want || r.Complete || r.ErrorEvent != tc.event {
				t.Fatalf("Err %q Complete %v ErrorEvent %v, want %q, false, %v", r.Err, r.Complete, r.ErrorEvent, tc.want, tc.event)
			}
		})
	}
}

// TestStreamCompleteDespiteLateReadFailure: once [DONE] came after a
// finish_reason, a failure reading what follows does not fail the request.
func TestStreamCompleteDespiteLateReadFailure(t *testing.T) {
	if r := stream(t, nil, lastChunk, doneChunk).End(at(30), ErrRead); !r.Complete {
		t.Fatalf("Err %q, want complete", r.Err)
	}
}

// TestStreamWithNoTokenEvent: a stream that ends before any token-bearing
// event (EOS first) has shown the caller nothing until its end, so its TTFT
// is the whole response, as for a body; zero would pull the percentiles down.
func TestStreamWithNoTokenEvent(t *testing.T) {
	eos := "data: {\"choices\":[{\"delta\":{\"content\":\"\"},\"finish_reason\":\"stop\"}]}\n\n"
	r := stream(t, nil, roleChunk, eos, doneChunk).End(at(40), "")
	if !r.Complete || r.Chunks != 0 || r.FirstByte != 10*time.Millisecond || r.TTFT != 40*time.Millisecond {
		t.Fatalf("response = %+v, want complete, no chunks, first byte 10ms, TTFT = whole response (40ms)", r)
	}
}

// TestDoneStopsMeasuring: bytes after [DONE] or a failure change nothing.
func TestDoneStopsMeasuring(t *testing.T) {
	b := stream(t, nil, lastChunk, doneChunk)
	b.Write([]byte(errChunk+tokChunk), at(99))
	if r := b.End(at(100), ""); !r.Complete || r.Chunks != 1 {
		t.Fatalf("response = %+v, want the stream as it was at [DONE]", r)
	}
}

func TestBody(t *testing.T) {
	const okBody = `{"choices":[{"message":{"content":"x"},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":9}}`
	tests := []struct {
		name    string
		writes  []string
		readErr string
		want    string
	}{
		{name: "complete", writes: []string{okBody[:20], okBody[20:]}},
		{name: "a choice without a finish_reason", writes: []string{`{"choices":[{"finish_reason":"stop"},{"message":{}}]}`}, want: ErrTruncated},
		{name: "no choices", writes: []string{`{"choices":[]}`}, want: ErrBadResponse},
		{name: "not JSON", writes: []string{"not json"}, want: ErrBadResponse},
		{name: "read failure", writes: []string{okBody[:20]}, readErr: ErrRead, want: ErrRead},
		{name: "over the limit", writes: []string{strings.Repeat("x", maxResponseBody), "x"}, want: ErrBadResponse},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := NewBody(false, t0, nil)
			for i, w := range tc.writes {
				b.Write([]byte(w), at((i+1)*10))
			}
			r := b.End(at(50), tc.readErr)
			if r.Err != tc.want || r.Complete != (tc.want == "") {
				t.Fatalf("Err %q Complete %v, want %q", r.Err, r.Complete, tc.want)
			}
			if tc.want == "" && (r.TTFT != 50*time.Millisecond || r.FirstByte != 10*time.Millisecond || r.OutputTokens != 9 || r.PromptTokens != 7) {
				t.Fatalf("response = %+v, want TTFT = whole response (50ms), first byte 10ms, usage read", r)
			}
		})
	}
}

// TestStreamMeasuringDoesNotGrow: measuring a long stream, with events split
// across reads, allocates per event only what parsing it needs (nothing is
// buffered beyond the current line).
func TestStreamMeasuringDoesNotGrow(t *testing.T) {
	b := NewBody(true, t0, nil)
	half := len(tokChunk) / 2
	first, rest := []byte(tokChunk[:half]), []byte(tokChunk[half:])
	for range 1000 {
		b.Write(first, at(1))
		b.Write(rest, at(2))
	}
	// One partial line is carried between writes, and its buffer is reused.
	if cap(b.line) > 2*len(tokChunk) || len(b.body) != 0 {
		t.Fatalf("carried line cap %d, body %d bytes after 1000 events; want one line's worth, no body", cap(b.line), len(b.body))
	}
	if r := b.End(at(3), ""); r.Chunks != 1000 {
		t.Fatalf("chunks = %d, want every event counted", r.Chunks)
	}
}

// TestErrorEventAfterHalfSentEvent: when a replica dies mid-event the gateway
// closes the half-sent event and then sends an error event (ingress
// writeStreamError); the error event decides the kind.
func TestErrorEventAfterHalfSentEvent(t *testing.T) {
	half := "data: {\"choices\":[{\"delta\":{\"content\":\"par\n\n"
	gatewayErr := "data: {\"error\":{\"message\":\"upstream replica failed mid-stream\",\"type\":\"upstream_error\",\"param\":null,\"code\":\"upstream_other\"}}\n\n"
	r := stream(t, nil, tokChunk, half, gatewayErr).End(at(40), "")
	if r.Err != ErrStreamError || !r.ErrorEvent || r.Complete {
		t.Fatalf("Err %q ErrorEvent %v Complete %v, want stream_error with the error event", r.Err, r.ErrorEvent, r.Complete)
	}
	// Without an error event after it, a broken event is a bad response.
	if r := stream(t, nil, tokChunk, half, lastChunk, doneChunk).End(at(40), ""); r.Err != ErrBadResponse {
		t.Fatalf("broken event then [DONE]: Err %q, want bad_response", r.Err)
	}
}

// TestUnterminatedLastLine: an SSE event ends with a newline, so a stream
// whose last bytes are "data: [DONE]" with none is truncated.
func TestUnterminatedLastLine(t *testing.T) {
	if r := stream(t, nil, lastChunk, "data: [DONE]").End(at(30), ""); r.Err != ErrTruncated {
		t.Fatalf("Err %q, want truncated", r.Err)
	}
}

// TestLineLimitAcrossReads: a line over the limit fails however its bytes are
// split across reads.
func TestLineLimitAcrossReads(t *testing.T) {
	long := "data: " + strings.Repeat("x", maxSSELine)
	b := NewBody(true, t0, nil)
	b.Write([]byte(long[:maxSSELine/2]), at(1))
	b.Write([]byte(long[maxSSELine/2:]+"\n\n"), at(2))
	if r := b.End(at(3), ""); r.Err != ErrBadResponse || !b.Done() {
		t.Fatalf("Err %q Done %v, want bad_response for an oversized line ending in a later read", r.Err, b.Done())
	}
}
