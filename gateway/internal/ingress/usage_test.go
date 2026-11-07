package ingress

import (
	"strings"
	"testing"
)

func scanAll(chunks ...string) *usageScan {
	var u usageScan
	for _, c := range chunks {
		u.observe([]byte(c))
	}
	return &u
}

func TestUsageScanJSONBody(t *testing.T) {
	body := `{"id":"x","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"hi \"finish_reason\":\"length\" \"prompt_tokens\": 9"},"finish_reason":"stop"}],"usage":{"prompt_tokens":123,"total_tokens":130,"completion_tokens":7,"prompt_tokens_details":null}}`
	u := scanAll(body)
	p, c, ok := u.usage()
	if !ok || p != 123 || c != 7 {
		t.Fatalf("usage = %d, %d, %v; want 123, 7, true", p, c, ok)
	}
	if u.finishedLength {
		t.Fatal("finish_reason inside generated text must not count")
	}
}

func TestUsageScanEverySplit(t *testing.T) {
	body := `{"choices":[{"finish_reason" : "length"}],"usage": {"prompt_tokens": 4096, "completion_tokens": 512}}`
	for i := 0; i <= len(body); i++ {
		for j := i; j <= len(body); j++ {
			u := scanAll(body[:i], body[i:j], body[j:])
			p, c, ok := u.usage()
			if !ok || p != 4096 || c != 512 || !u.finishedLength {
				t.Fatalf("split %d/%d: usage = %d, %d, %v, length=%v", i, j, p, c, ok, u.finishedLength)
			}
		}
	}
}

func TestUsageScanSSE(t *testing.T) {
	stream := strings.Join([]string{
		`data: {"choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}]}`,
		`data: {"choices":[{"index":0,"delta":{"content":"completion_tokens"},"finish_reason":null}],"usage":{"prompt_tokens":50,"completion_tokens":1}}`,
		`data: {"choices":[{"index":0,"delta":{},"finish_reason":"length"}]}`,
		`data: {"choices":[],"usage":{"prompt_tokens":50,"completion_tokens":16,"total_tokens":66}}`,
		`data: [DONE]`, ``,
	}, "\n\n")
	u := scanAll(stream)
	p, c, ok := u.usage()
	if !ok || p != 50 || c != 16 || !u.finishedLength {
		t.Fatalf("usage = %d, %d, %v, length=%v", p, c, ok, u.finishedLength)
	}
}

func TestUsageScanNoUsage(t *testing.T) {
	u := scanAll(`data: {"choices":[{"delta":{"content":"a"},"finish_reason":"stop"}]}` + "\n\ndata: [DONE]\n\n")
	if _, _, ok := u.usage(); ok {
		t.Fatal("no usage in the stream, want ok false")
	}
	if u.finishedLength {
		t.Fatal("stop is not length")
	}
}

func BenchmarkUsageScan(b *testing.B) {
	body := []byte(`data: {"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"some generated words here"},"finish_reason":null}]}` + "\n\n")
	b.SetBytes(int64(len(body)))
	var u usageScan
	for b.Loop() {
		u.observe(body)
	}
}
