package telemetry

import (
	"encoding/binary"
	"encoding/hex"
	"math/rand/v2"
	"net/http"
	"strings"
)

// W3C Trace Context (https://www.w3.org/TR/trace-context/). The gateway
// accepts a caller's traceparent, or starts a trace when there is none (or it
// is invalid), and sends upstream a traceparent with the same trace ID and the
// gateway's own span ID as parent. tracestate passes through unchanged when
// the caller's trace is continued and is dropped when a new trace starts.
const (
	HeaderTraceparent = "Traceparent"
	HeaderTracestate  = "Tracestate"

	traceVersion     = "00"
	traceparentLen   = 55 // version 00: 2+1+32+1+16+1+2
	traceIDHexLen    = 32
	spanIDHexLen     = 16
	flagsNotSampled  = "00"
	invalidVersionFF = "ff"
)

// traceContext is the gateway's position in a trace.
type traceContext struct {
	traceID  string // 32 lowercase hex
	spanID   string // 16 lowercase hex: the gateway's span, sent upstream as parent
	parentID string // caller's span; empty when the gateway started the trace
	flags    string // 2 lowercase hex
	tp       string // preformatted 55-byte traceparent header
}

// traceparent renders the header value sent upstream.
func (tc traceContext) traceparent() string {
	if tc.tp != "" {
		return tc.tp
	}
	return traceVersion + "-" + tc.traceID + "-" + tc.spanID + "-" + tc.flags
}

// newTraceContext continues the caller's trace from h, or starts a new one.
// continued reports whether the caller's trace was valid and kept.
// Both traceID, spanID and the outgoing traceparent header share one 55-byte
// string allocation instead of three separate strings.
func newTraceContext(h http.Header) (tc traceContext, continued bool) {
	var buf [traceparentLen]byte
	buf[0], buf[1], buf[2] = '0', '0', '-'
	buf[35] = '-'
	buf[52] = '-'

	encodeRandomHex(buf[36:52])

	if vals := h[HeaderTraceparent]; len(vals) == 1 {
		if traceID, parentID, flags, ok := parseTraceparent(vals[0]); ok {
			copy(buf[3:35], traceID)
			copy(buf[53:55], flags)
			s := string(buf[:])
			return traceContext{
				traceID:  traceID,
				spanID:   s[36:52],
				parentID: parentID,
				flags:    flags,
				tp:       s,
			}, true
		}
	}

	encodeRandomHex(buf[3:35])
	buf[53], buf[54] = '0', '0'
	s := string(buf[:])
	return traceContext{
		traceID: s[3:35],
		spanID:  s[36:52],
		flags:   flagsNotSampled,
		tp:      s,
	}, false
}

// encodeRandomHex fills dst (even length, 16 or 32) with non-zero random lowercase hex.
func encodeRandomHex(dst []byte) {
	switch len(dst) {
	case spanIDHexLen:
		var raw [8]byte
		for {
			if u := rand.Uint64(); u != 0 {
				binary.LittleEndian.PutUint64(raw[:], u)
				break
			}
		}
		hex.Encode(dst, raw[:])
	case traceIDHexLen:
		var raw [16]byte
		for {
			u0, u1 := rand.Uint64(), rand.Uint64()
			if u0 != 0 || u1 != 0 {
				binary.LittleEndian.PutUint64(raw[0:8], u0)
				binary.LittleEndian.PutUint64(raw[8:16], u1)
				break
			}
		}
		hex.Encode(dst, raw[:])
	default:
		raw := make([]byte, len(dst)/2)
		for {
			nonZero := false
			for i := 0; i < len(raw); i += 8 {
				var tmp [8]byte
				u := rand.Uint64()
				if u != 0 {
					nonZero = true
				}
				binary.LittleEndian.PutUint64(tmp[:], u)
				copy(raw[i:], tmp[:])
			}
			if nonZero {
				break
			}
		}
		hex.Encode(dst, raw)
	}
}

func randomHex(hexLen int) string {
	var stack [traceIDHexLen]byte
	if hexLen <= len(stack) {
		encodeRandomHex(stack[:hexLen])
		return string(stack[:hexLen])
	}
	b := make([]byte, hexLen)
	encodeRandomHex(b)
	return string(b)
}

// parseTraceparent validates a traceparent value. Versions above 00 are
// accepted when their first four fields are well formed; the gateway then
// propagates them as version 00, as the specification requires.
func parseTraceparent(v string) (traceID, parentID, flags string, ok bool) {
	v = strings.TrimSpace(v)
	if len(v) < traceparentLen || v[2] != '-' || v[35] != '-' || v[52] != '-' {
		return "", "", "", false
	}
	version := v[0:2]
	if !isLowerHex(version) || version == invalidVersionFF {
		return "", "", "", false
	}
	if version == traceVersion && len(v) != traceparentLen {
		return "", "", "", false
	}
	if len(v) > traceparentLen && v[traceparentLen] != '-' {
		return "", "", "", false
	}
	traceID, parentID, flags = v[3:35], v[36:52], v[53:55]
	if !isLowerHex(traceID) || !isLowerHex(parentID) || !isLowerHex(flags) ||
		allZero(traceID) || allZero(parentID) {
		return "", "", "", false
	}
	return traceID, parentID, flags, true
}

func isLowerHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func allZero(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] != '0' {
			return false
		}
	}
	return true
}
