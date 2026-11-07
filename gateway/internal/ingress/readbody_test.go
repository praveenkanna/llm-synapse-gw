package ingress

import (
	"bytes"
	"encoding/json"
	"io"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex"
)

// indexedFields is the raw last "model" and "stream" of body as the
// request's index reads them (AD-15); ok is false unless body is one JSON
// object.
func indexedFields(body []byte) (model, stream json.RawMessage, ok bool) {
	var ix bodyindex.Index
	if ix.Read(body) != nil {
		return nil, nil, false
	}
	m, _ := ix.Find("model")
	s, _ := ix.Find("stream")
	return ix.Value(m), ix.Value(s), true
}

// decoderTopLevelFields is a slow, obviously-correct reference for the
// request index's reading: it walks the object with a json.Decoder.
func decoderTopLevelFields(body []byte) (model, stream json.RawMessage, ok bool) {
	dec := json.NewDecoder(bytes.NewReader(body))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return nil, nil, false
	}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, nil, false
		}
		key, _ := tok.(string)
		var val json.RawMessage
		if err := dec.Decode(&val); err != nil {
			return nil, nil, false
		}
		switch key {
		case "model":
			model = val
		case "stream":
			stream = val
		}
	}
	if tok, err := dec.Token(); err != nil || tok != json.Delim('}') {
		return nil, nil, false
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, nil, false
	}
	return model, stream, true
}

func FuzzTopLevelFields(f *testing.F) {
	for _, seed := range []string{
		`{"model":"a"}`, `{"MODEL":"b","model":"a","stream":true}`, `{"model":"first","model":"second"}`,
		`{"x":"}\"{","model":"a","stream":true,"y":[1,2,{"model":"z"}]}`, `{"model":"esc"}`,
		` { "model" : "a" , "stream" : false } `, `{"a":[[],{}],"b":{"c":[{"d":"]"}]},"model":null}`,
		`{"model":"a"}{}`, `{"model":"a",}`, `null`, `[]`, `{}`, `"x"`, `{"a":1.5e-3,"model":"m"}`, ``,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		gotModel, gotStream, gotOK := indexedFields(body)
		wantModel, wantStream, wantOK := decoderTopLevelFields(body)
		if gotOK != wantOK {
			t.Fatalf("ok = %v, reference %v for %q", gotOK, wantOK, body)
		}
		if !gotOK {
			return
		}
		// Compare compacted: the two may differ in whitespace inside a value.
		if !sameJSON(gotModel, wantModel) || !sameJSON(gotStream, wantStream) {
			t.Fatalf("fields (%q, %q), reference (%q, %q) for %q", gotModel, gotStream, wantModel, wantStream, body)
		}
	})
}

func sameJSON(a, b json.RawMessage) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	var ca, cb bytes.Buffer
	return json.Compact(&ca, a) == nil && json.Compact(&cb, b) == nil && bytes.Equal(ca.Bytes(), cb.Bytes())
}

// TestReadBodyDoesNotCopyLargeBodies guards the hot path: reading "model"
// from a large prompt must not allocate in proportion to the body.
func TestReadBodyDoesNotCopyLargeBodies(t *testing.T) {
	body := []byte(`{"messages":[{"role":"user","content":"` + strings.Repeat("x", 4<<20) + `"}],"model":"large","stream":true}`)
	var ix bodyindex.Index
	info, _ := readBody(&ix, body) // warm up
	if info.model != "large" || !info.stream {
		t.Fatalf("readBody = %+v", info)
	}
	// Measure bytes, not allocation counts: the race detector adds the odd
	// small allocation, but a copy of the body shows up as megabytes.
	const runs = 5
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range runs {
		readBody(&ix, body)
	}
	runtime.ReadMemStats(&after)
	perCall := (after.TotalAlloc - before.TotalAlloc) / runs
	if limit := uint64(len(body) / 64); perCall > limit {
		t.Fatalf("readBody allocated %d bytes per call on a %d-byte body; want under %d (no copies of the body)", perCall, len(body), limit)
	}
}

func BenchmarkReadBody(b *testing.B) {
	for _, size := range []int{1 << 10, 1 << 20, 32 << 20} {
		body := []byte(`{"messages":[{"role":"user","content":"` + strings.Repeat("x", size) + `"}],"model":"large","stream":true}`)
		b.Run(byteSize(size), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(body)))
			for b.Loop() {
				var ix bodyindex.Index // one per request, as handleGenerate declares it
				readBody(&ix, body)
			}
		})
	}
}

func byteSize(n int) string {
	if n >= 1<<20 {
		return strconv.Itoa(n>>20) + "MiB"
	}
	return strconv.Itoa(n>>10) + "KiB"
}
