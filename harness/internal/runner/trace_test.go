package runner

import (
	"context"
	"regexp"
	"strings"
	"testing"
)

// traceparentRE matches a valid version-00 W3C traceparent (lower-case hex,
// non-zero IDs checked separately).
var traceparentRE = regexp.MustCompile(`^00-([0-9a-f]{32})-([0-9a-f]{16})-01$`)

func checkTraceparent(t *testing.T, v string) (traceID string) {
	t.Helper()
	m := traceparentRE.FindStringSubmatch(v)
	if m == nil {
		t.Fatalf("traceparent %q is not a valid version-00 header", v)
	}
	if m[1] == strings.Repeat("0", 32) || m[2] == strings.Repeat("0", 16) {
		t.Fatalf("traceparent %q has an all-zero ID", v)
	}
	return m[1]
}

func TestNewTraceparentValidAndUnique(t *testing.T) {
	const n = 10000
	seen := make(map[string]bool, n)
	for range n {
		h, id := newTraceparent()
		if got := checkTraceparent(t, h); got != id {
			t.Fatalf("trace ID %q does not match header %q", id, h)
		}
		if seen[id] {
			t.Fatalf("trace ID %q repeated", id)
		}
		seen[id] = true
	}
}

func TestClientSendsFreshTraceparent(t *testing.T) {
	f := &fakeUpstream{tokens: 2}
	srv := f.start(t)
	c := newClient(srv.URL)
	var ids []string
	for _, stream := range []bool{true, false, true} {
		req := streamReq()
		req.Stream = stream
		if !stream {
			req.Body = []byte(`{}`)
		}
		res := c.Do(context.Background(), req, nil)
		if !res.OK() {
			t.Fatalf("result = %+v", res)
		}
		ids = append(ids, res.TraceID)
	}
	sent := f.traceparents()
	if len(sent) != len(ids) {
		t.Fatalf("upstream saw %d traceparents, want %d", len(sent), len(ids))
	}
	seen := map[string]bool{}
	for i, h := range sent {
		id := checkTraceparent(t, h)
		if id != ids[i] {
			t.Fatalf("request %d: sent trace ID %s, result has %s", i, id, ids[i])
		}
		if seen[id] {
			t.Fatalf("trace ID %s reused across requests", id)
		}
		seen[id] = true
	}
}

func TestRunRecordsTraceIDs(t *testing.T) {
	f := &fakeUpstream{tokens: 2}
	_, recs := runOnce(t, Config{Concurrency: 4, MaxRequests: 30, Seed: 3}, f, 3)
	sent := map[string]bool{}
	for _, h := range f.traceparents() {
		sent[checkTraceparent(t, h)] = true
	}
	if len(sent) != 30 {
		t.Fatalf("upstream saw %d distinct trace IDs, want 30", len(sent))
	}
	for _, r := range recs {
		if !sent[r.TraceID] {
			t.Fatalf("record seq %d has trace ID %q the upstream never saw", r.Seq, r.TraceID)
		}
	}
}
