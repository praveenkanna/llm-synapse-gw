package ingress

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

// Failure handling (.claude/rules/streaming-and-failure.md):
//
//   - connect fails before any byte is sent: retry once, on another replica
//     of the pool, or on the same one after a short backoff when the pool
//     has no other (ADR 0014; package dispatch);
//   - fails after the request is sent, before the response: 502 or 504 with
//     an OpenAI-style error body, no retry (see sendFailed);
//   - fails mid-stream: one SSE error event, then the stream ends. Never
//     retried: the caller already holds part of the generation;
//   - fails mid-body on a non-streaming response: the client connection is
//     aborted, so a truncated body never looks complete;
//   - the caller goes away: the upstream request shares the caller's
//     context, so it is cancelled at once and the replica frees its KV cache.

// Mid-stream error event fields.
const (
	midStreamErrorMessage = "upstream replica failed mid-stream"
	upstreamErrorType     = "upstream_error"
	upstreamCodePrefix    = "upstream_"
)

// clientWriteError marks a relay failure on the caller's side of the
// exchange: the gateway could not write to the client.
type clientWriteError struct{ err error }

func (e *clientWriteError) Error() string { return "write to client: " + e.err.Error() }
func (e *clientWriteError) Unwrap() error { return e.err }

// isClientGone reports whether a relay failed because the caller left.
func isClientGone(r *http.Request, err error) bool {
	var cw *clientWriteError
	return errors.As(err, &cw) || r.Context().Err() != nil
}

// abortResponse ends the exchange without completing the response: net/http
// closes the client connection (HTTP/1.1) or resets the stream (HTTP/2), so
// the caller sees a failed read instead of a body that looks complete.
// http.ErrAbortHandler is the documented way to do this; recoverPanic passes
// it through without logging.
func abortResponse() {
	panic(http.ErrAbortHandler)
}

// writeStreamError ends a stream whose replica failed mid-way with one SSE
// error event, first closing any event the replica left half-sent so the
// error parses as an event of its own. It returns the write error, if any.
func writeStreamError(w http.ResponseWriter, tail *sseTail, kind upstream.Kind) error {
	b, _ := json.Marshal(errorBody{Error: errorDetail{
		Message: midStreamErrorMessage,
		Type:    upstreamErrorType,
		Code:    upstreamCodePrefix + string(kind),
	}})
	ev := make([]byte, 0, len(b)+16)
	ev = append(ev, tail.terminator()...)
	ev = append(ev, "data: "...)
	ev = append(ev, b...)
	ev = append(ev, "\n\n"...)
	if _, err := w.Write(ev); err != nil {
		return err
	}
	return http.NewResponseController(w).Flush()
}

// sseTail remembers the last bytes relayed on a stream, to tell whether the
// stream stopped on an event boundary (a blank line). It never holds more
// than sseTailBytes: the stream itself is not buffered.
type sseTail struct {
	buf [sseTailBytes]byte // ends with the latest byte relayed
	n   int                // valid bytes at the end of buf
}

// sseTailBytes is enough to recognise the longest event terminator, CRLF CRLF.
const sseTailBytes = 4

// eventTerminators are the endings that close an SSE event: a line ending
// (LF, CR or CRLF) followed by an empty line. CRLF CRLF ends in LF CRLF.
var eventTerminators = []string{"\n\n", "\r\r", "\n\r\n", "\r\r\n"}

// observe records bytes just written to the client.
func (t *sseTail) observe(p []byte) {
	if len(p) >= sseTailBytes {
		copy(t.buf[:], p[len(p)-sseTailBytes:])
		t.n = sseTailBytes
		return
	}
	copy(t.buf[:], t.buf[len(p):]) // slide left to make room at the end
	copy(t.buf[sseTailBytes-len(p):], p)
	t.n = min(t.n+len(p), sseTailBytes)
}

// terminator returns what must be written so that the next bytes start a
// new event: "" at a boundary (or before anything was relayed), "\n" when
// only the blank line is missing, "\n\n" in the middle of a line.
func (t *sseTail) terminator() string {
	if t.n == 0 {
		return ""
	}
	s := string(t.buf[sseTailBytes-t.n:])
	for _, end := range eventTerminators {
		if strings.HasSuffix(s, end) {
			return ""
		}
	}
	if s[len(s)-1] == '\n' {
		return "\n"
	}
	return "\n\n"
}
