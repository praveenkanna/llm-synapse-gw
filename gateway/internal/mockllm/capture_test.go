package mockllm

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRecorderBoundedRing(t *testing.T) {
	const capacity = 3
	rec := newRecorder(capacity, 4)
	for i := range 10 {
		r := httptest.NewRequest("POST", fmt.Sprintf("/v1/completions?i=%d", i), nil)
		r.Header.Set("X-Test", fmt.Sprint(i))
		rec.record(r, []byte(fmt.Sprintf("body-%d", i)))
	}
	got := rec.snapshot()
	if len(got) != capacity {
		t.Fatalf("len = %d, want %d (ring must stay bounded)", len(got), capacity)
	}
	for j, rr := range got {
		i := 7 + j // oldest retained first
		if rr.Seq != uint64(i+1) || rr.Query != fmt.Sprintf("i=%d", i) || rr.Header.Get("X-Test") != fmt.Sprint(i) {
			t.Fatalf("entry %d = seq %d query %q header %q, want request %d", j, rr.Seq, rr.Query, rr.Header.Get("X-Test"), i)
		}
		if string(rr.Body) != "body" || !rr.Truncated || rr.BodyBytes != len(fmt.Sprintf("body-%d", i)) {
			t.Fatalf("entry %d body = %q truncated=%v bytes=%d", j, rr.Body, rr.Truncated, rr.BodyBytes)
		}
	}
}

func TestRecorderPartialAndReset(t *testing.T) {
	rec := newRecorder(4, 1024)
	rec.record(httptest.NewRequest("POST", "/a", nil), []byte("one"))
	rec.record(httptest.NewRequest("POST", "/b", nil), []byte("two"))
	got := rec.snapshot()
	if len(got) != 2 || got[0].Path != "/a" || got[1].Path != "/b" || got[0].Truncated {
		t.Fatalf("snapshot = %+v", got)
	}
	got[0].Body[0] = 'X' // snapshots must not alias the ring
	got[0].Header.Set("Mutated", "1")
	again := rec.snapshot()
	if string(again[0].Body) != "one" || again[0].Header.Get("Mutated") != "" {
		t.Fatalf("snapshot aliases ring: %q %v", again[0].Body, again[0].Header)
	}
	rec.reset()
	if n := len(rec.snapshot()); n != 0 {
		t.Fatalf("after reset len = %d", n)
	}
	rec.record(httptest.NewRequest("POST", "/c", nil), nil)
	if s := rec.snapshot(); len(s) != 1 || s[0].Seq != 1 {
		t.Fatalf("after reset + record = %+v", s)
	}
}

func TestRecorderDisabled(t *testing.T) {
	rec := newRecorder(0, 1024)
	rec.record(httptest.NewRequest("POST", "/a", strings.NewReader("x")), []byte("x"))
	if n := len(rec.snapshot()); n != 0 {
		t.Fatalf("disabled recorder kept %d entries", n)
	}
}

func TestRecorderRedactsCredentials(t *testing.T) {
	rec := newRecorder(2, 1024)
	r := httptest.NewRequest("POST", "/v1/chat/completions", nil)
	r.Header.Set("Authorization", "Bearer sk-secret-token")
	r.Header.Add("Proxy-Authorization", "Basic c2VjcmV0")
	r.Header.Add("Proxy-Authorization", "Basic b3RoZXI=")
	r.Header["authorization"] = []string{"Bearer lower-case-key"} // non-canonical key
	r.Header.Set("X-Other", "kept")
	rec.record(r, []byte("{}"))

	got := rec.snapshot()
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	h := got[0].Header
	tests := []struct {
		key  string
		want []string
	}{
		{"Authorization", []string{RedactedValue}},
		{"Proxy-Authorization", []string{RedactedValue, RedactedValue}},
		{"authorization", []string{RedactedValue}},
		{"X-Other", []string{"kept"}},
	}
	for _, tt := range tests {
		if v := h[tt.key]; fmt.Sprint(v) != fmt.Sprint(tt.want) {
			t.Errorf("header %s = %q, want %q", tt.key, v, tt.want)
		}
	}
	if r.Header.Get("Authorization") != "Bearer sk-secret-token" {
		t.Fatalf("redaction mutated the live request header: %q", r.Header.Get("Authorization"))
	}

	rec.record(httptest.NewRequest("POST", "/v1/completions", nil), nil)
	if h := rec.snapshot()[1].Header; h.Get("Authorization") != "" || len(h.Values("Proxy-Authorization")) != 0 {
		t.Fatalf("absent credential headers must stay absent: %v", h)
	}
}
