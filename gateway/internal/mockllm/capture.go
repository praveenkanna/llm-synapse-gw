package mockllm

import (
	"net/http"
	"strings"
	"sync"
	"time"
)

// RedactedValue replaces the value of every credential header in captured
// requests. The header key and the number of values stay, so tests can still
// assert that a credential was (or was not) forwarded.
const RedactedValue = "[REDACTED]"

// redactedHeaders carry credentials; their values are never retained.
var redactedHeaders = []string{"Authorization", "Proxy-Authorization"}

// RecordedRequest is one request as mockllm received it. Tests compare Body
// with what the caller sent to prove byte-exact forwarding.
//
// Capture exists for tests against synthetic traffic only; it is bounded in
// entries and bytes, never logged, and credential header values are replaced
// with RedactedValue.
type RecordedRequest struct {
	Seq    uint64      `json:"seq"`
	Time   time.Time   `json:"time"`
	Method string      `json:"method"`
	Path   string      `json:"path"`
	Query  string      `json:"query,omitempty"`
	Header http.Header `json:"header"`
	// Body holds at most CaptureMaxBytes of the received body (base64 in JSON).
	Body []byte `json:"body"`
	// BodyBytes is the full received body length.
	BodyBytes int `json:"body_bytes"`
	// Truncated reports that Body is shorter than BodyBytes.
	Truncated bool `json:"truncated"`
}

// recorder is a fixed-capacity ring of recent requests.
type recorder struct {
	maxBytes int

	mu   sync.Mutex
	ring []RecordedRequest
	next int    // index of the slot to write next
	full bool   // ring has wrapped at least once
	seq  uint64 // total recorded since the last reset
}

func newRecorder(entries, maxBytes int) *recorder {
	return &recorder{maxBytes: maxBytes, ring: make([]RecordedRequest, entries)}
}

// record stores a copy of r's metadata and up to maxBytes of body.
func (c *recorder) record(r *http.Request, body []byte) {
	if len(c.ring) == 0 {
		return
	}
	kept := body
	if len(kept) > c.maxBytes {
		kept = kept[:c.maxBytes]
	}
	rr := RecordedRequest{
		Time:      time.Now(),
		Method:    r.Method,
		Path:      r.URL.Path,
		Query:     r.URL.RawQuery,
		Header:    redactHeader(r.Header),
		Body:      append([]byte(nil), kept...),
		BodyBytes: len(body),
		Truncated: len(kept) < len(body),
	}
	c.mu.Lock()
	c.seq++
	rr.Seq = c.seq
	c.ring[c.next] = rr
	c.next = (c.next + 1) % len(c.ring)
	if c.next == 0 {
		c.full = true
	}
	c.mu.Unlock()
}

// redactHeader returns a copy of h with credential values replaced. Keys are
// matched case-insensitively so a non-canonical key cannot slip through.
func redactHeader(h http.Header) http.Header {
	out := h.Clone()
	for key, vals := range out {
		if !isRedactedHeader(key) {
			continue
		}
		redacted := make([]string, len(vals))
		for i := range redacted {
			redacted[i] = RedactedValue
		}
		out[key] = redacted
	}
	return out
}

func isRedactedHeader(key string) bool {
	for _, k := range redactedHeaders {
		if strings.EqualFold(key, k) {
			return true
		}
	}
	return false
}

// snapshot returns the retained requests, oldest first. Entries share no
// memory with the ring.
func (c *recorder) snapshot() []RecordedRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := c.next
	start := 0
	if c.full {
		n = len(c.ring)
		start = c.next
	}
	out := make([]RecordedRequest, 0, n)
	for i := 0; i < n; i++ {
		rr := c.ring[(start+i)%len(c.ring)]
		rr.Header = rr.Header.Clone()
		rr.Body = append([]byte(nil), rr.Body...)
		out = append(out, rr)
	}
	return out
}

func (c *recorder) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	clear(c.ring)
	c.next, c.full, c.seq = 0, false, 0
}
