package ingress

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRelayBufferSizeByResponseKind(t *testing.T) {
	tests := []struct {
		name   string
		stream bool
		want   int
	}{
		{name: "SSE stream holds a small buffer for its whole life", stream: true, want: streamRelayBufferBytes},
		{name: "other bodies use a large buffer briefly", stream: false, want: bodyRelayBufferBytes},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pool := relayBuffersFor(tc.stream)
			bp := pool.Get().(*[]byte)
			defer pool.Put(bp)
			if got := len(*bp); got != tc.want {
				t.Fatalf("buffer size = %d, want %d", got, tc.want)
			}
		})
	}
	if streamRelayBufferBytes >= bodyRelayBufferBytes {
		t.Fatalf("stream buffer (%d) must be smaller than the body buffer (%d)", streamRelayBufferBytes, bodyRelayBufferBytes)
	}
}

// chunkReader returns one chunk per Read, like a replica sending one SSE
// event per HTTP chunk. It never allocates.
type chunkReader struct {
	chunk []byte
	left  int
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if r.left == 0 {
		return 0, io.EOF
	}
	r.left--
	return copy(p, r.chunk), nil
}

// flushDiscard is a ResponseWriter that drops the body and counts flushes.
type flushDiscard struct {
	hdr     http.Header
	flushes int
}

func (w *flushDiscard) Header() http.Header         { return w.hdr }
func (w *flushDiscard) WriteHeader(int)             {}
func (w *flushDiscard) Write(p []byte) (int, error) { return len(p), nil }
func (w *flushDiscard) Flush()                      { w.flushes++ }

func TestRelayAllocatesNothingPerChunk(t *testing.T) {
	const chunks = 1000
	w := &flushDiscard{hdr: http.Header{}}
	src := &chunkReader{chunk: []byte(benchEvent)}
	var tail sseTail
	allocs := testing.AllocsPerRun(20, func() {
		src.left = chunks
		if err := relay(w, src, streamRelayBuffers, nil, &tail, nil); err != nil {
			t.Fatal(err)
		}
	})
	// A few allocations per stream at most (response controller, a pool
	// refill); anything per chunk would show up as >= chunks.
	if allocs > 8 {
		t.Fatalf("relay of %d chunks made %.0f allocations; want a small constant, none per chunk", chunks, allocs)
	}
	if w.flushes < chunks {
		t.Fatalf("flushed %d times for %d chunks; want a flush after every write", w.flushes, chunks)
	}
}

func TestStreamEventLargerThanRelayBufferArrivesIntact(t *testing.T) {
	// One event three times the stream buffer, e.g. a large tool-call delta.
	big := "data: {\"delta\":\"" + strings.Repeat("x", 3*streamRelayBufferBytes) + "\"}\n\n"
	want := big + "data: [DONE]\n\n"
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", sseContentType)
		_, _ = io.WriteString(w, big)
		http.NewResponseController(w).Flush()
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(up.Close)
	gw := newGatewayTo(t, up.URL)
	resp, err := gw.Client().Post(gw.URL+PathChatCompletions, "application/json",
		strings.NewReader(`{"model":"large-model","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte(want)) {
		t.Fatalf("stream changed in relay: got %d bytes, want %d", len(got), len(want))
	}
}
