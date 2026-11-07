package ingress

import (
	"net/http"
	"strings"
)

// SSE handling. Streamed responses are relayed chunk by chunk (see relay);
// the helpers here keep anything between the replica and the caller from
// buffering them:
//   - the upstream request of a streaming call asks for identity encoding
//     (upstream.Request.Stream), so a replica (or a proxy in front of it)
//     never compresses the stream;
//   - the streamed response carries X-Accel-Buffering: no, so a buffering
//     reverse proxy in front of the gateway (nginx and compatibles) passes
//     events through as they arrive.
//
// The gateway itself never compresses a response: bytes relay as received.
const (
	eventStreamMediaType = "text/event-stream"

	headerXAccelBuffering = "X-Accel-Buffering"
)

// isEventStream reports whether a response is a Server-Sent Events stream
// without allocating a MIME parameter map.
func isEventStream(h http.Header) bool {
	ct := strings.TrimSpace(h.Get("Content-Type"))
	if len(ct) < len(eventStreamMediaType) {
		return false
	}
	if !strings.EqualFold(ct[:len(eventStreamMediaType)], eventStreamMediaType) {
		return false
	}
	rest := strings.TrimSpace(ct[len(eventStreamMediaType):])
	return rest == "" || rest[0] == ';'
}

// resolveFlusher unwraps w once to find an http.Flusher so chunk loops do not
// walk wrapper chains on every token.
func resolveFlusher(w http.ResponseWriter) http.Flusher {
	for cur := w; cur != nil; {
		if fl, ok := cur.(http.Flusher); ok {
			return fl
		}
		u, ok := cur.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			return nil
		}
		cur = u.Unwrap()
	}
	return nil
}

func flushWriter(w http.ResponseWriter) {
	if fl := resolveFlusher(w); fl != nil {
		fl.Flush()
		return
	}
	_ = http.NewResponseController(w).Flush()
}

// markUnbuffered sets the response headers that keep proxies downstream of
// the gateway from buffering a stream.
func markUnbuffered(h http.Header) {
	h.Set(headerXAccelBuffering, "no")
}
