package telemetry

import (
	"net/http"
	"strings"
	"testing"
)

const (
	validTraceID  = "4bf92f3577b34da6a3ce929d0e0e4736"
	validParentID = "00f067aa0ba902b7"
	validTP       = "00-" + validTraceID + "-" + validParentID + "-01"
)

func TestParseTraceparent(t *testing.T) {
	tests := []struct {
		name  string
		in    string
		ok    bool
		flags string
	}{
		{"valid sampled", validTP, true, "01"},
		{"valid not sampled", "00-" + validTraceID + "-" + validParentID + "-00", true, "00"},
		{"surrounding spaces", "  " + validTP + " ", true, "01"},
		{"future version with extra field", "cc-" + validTraceID + "-" + validParentID + "-01-what-the-future", true, "01"},
		{"future version exact length", "01-" + validTraceID + "-" + validParentID + "-01", true, "01"},
		{"empty", "", false, ""},
		{"version ff", "ff-" + validTraceID + "-" + validParentID + "-01", false, ""},
		{"version 00 too long", validTP + "-extra", false, ""},
		{"future version bad separator", "cc-" + validTraceID + "-" + validParentID + "-01x", false, ""},
		{"uppercase hex", "00-" + strings.ToUpper(validTraceID) + "-" + validParentID + "-01", false, ""},
		{"zero trace id", "00-" + strings.Repeat("0", 32) + "-" + validParentID + "-01", false, ""},
		{"zero parent id", "00-" + validTraceID + "-" + strings.Repeat("0", 16) + "-01", false, ""},
		{"short trace id", "00-" + validTraceID[:31] + "-" + validParentID + "-01", false, ""},
		{"non-hex flags", "00-" + validTraceID + "-" + validParentID + "-0g", false, ""},
		{"wrong separators", "00_" + validTraceID + "_" + validParentID + "_01", false, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			traceID, parentID, flags, ok := parseTraceparent(tc.in)
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
			if ok && (traceID != validTraceID || parentID != validParentID || flags != tc.flags) {
				t.Fatalf("got %s %s %s", traceID, parentID, flags)
			}
		})
	}
}

func TestNewTraceContext(t *testing.T) {
	tests := []struct {
		name      string
		values    []string
		continued bool
	}{
		{"continues a valid trace", []string{validTP}, true},
		{"starts a trace when missing", nil, false},
		{"starts a trace when invalid", []string{"garbage"}, false},
		{"starts a trace on duplicate headers", []string{validTP, validTP}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := http.Header{}
			for _, v := range tc.values {
				h.Add(HeaderTraceparent, v)
			}
			tc2, continued := newTraceContext(h)
			if continued != tc.continued {
				t.Fatalf("continued = %v, want %v", continued, tc.continued)
			}
			out := tc2.traceparent()
			gotTrace, gotParent, _, ok := parseTraceparent(out)
			if !ok {
				t.Fatalf("outgoing traceparent %q is invalid", out)
			}
			if gotParent != tc2.spanID || gotParent == validParentID {
				t.Fatalf("outgoing parent %q must be the gateway's own span", gotParent)
			}
			if continued && (gotTrace != validTraceID || tc2.parentID != validParentID || !strings.HasSuffix(out, "-01")) {
				t.Fatalf("continued trace lost identity: %q", out)
			}
			if !continued && (gotTrace == validTraceID || tc2.parentID != "" || !strings.HasSuffix(out, "-00")) {
				t.Fatalf("new trace wrong: %q parent=%q", out, tc2.parentID)
			}
		})
	}
}

func TestRandomHex(t *testing.T) {
	seen := make(map[string]bool)
	for range 1000 {
		for _, n := range []int{spanIDHexLen, traceIDHexLen} {
			s := randomHex(n)
			if len(s) != n || !isLowerHex(s) || allZero(s) {
				t.Fatalf("randomHex(%d) = %q", n, s)
			}
			if seen[s] {
				t.Fatalf("duplicate id %q", s)
			}
			seen[s] = true
		}
	}
}
