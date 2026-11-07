package affinity_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/affinity"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

func TestHeaderExtraction(t *testing.T) {
	ctx := context.Background()

	// Tier 1: X-Session-Id
	req1, _ := http.NewRequestWithContext(ctx, http.MethodPost, "/", nil)
	req1.Header.Set("X-Session-Id", "sess-1234")
	res1 := affinity.Extract(req1, nil)
	if res1.Tier != affinity.TierHeader || res1.Key != "sess-1234" {
		t.Errorf("expected TierHeader sess-1234, got %+v", res1)
	}

	// Tier 1: X-Conversation-Id alias
	req2, _ := http.NewRequestWithContext(ctx, http.MethodPost, "/", nil)
	req2.Header.Set("X-Conversation-Id", "conv-5678")
	res2 := affinity.Extract(req2, nil)
	if res2.Tier != affinity.TierHeader || res2.Key != "conv-5678" {
		t.Errorf("expected TierHeader conv-5678, got %+v", res2)
	}

	// X-Session-Id takes precedence over X-Conversation-Id
	req3, _ := http.NewRequestWithContext(ctx, http.MethodPost, "/", nil)
	req3.Header.Set("X-Session-Id", "sess-priority")
	req3.Header.Set("X-Conversation-Id", "conv-secondary")
	res3 := affinity.Extract(req3, []byte(`{"messages":[{"role":"user","content":"ignored"}]}`))
	if res3.Tier != affinity.TierHeader || res3.Key != "sess-priority" {
		t.Errorf("expected TierHeader sess-priority, got %+v", res3)
	}
}

func TestPrefixHashMultiTurnStability(t *testing.T) {
	// Turn 1
	bodyTurn1 := []byte(`{
		"model": "meta-llama/Llama-3.1-8B-Instruct",
		"messages": [
			{"role": "system", "content": "You are a helpful assistant."},
			{"role": "user", "content": "Hello world!"}
		]
	}`)

	// Turn 2: adds assistant reply and user followup
	bodyTurn2 := []byte(`{
		"model": "meta-llama/Llama-3.1-8B-Instruct",
		"messages": [
			{"role": "system", "content": "You are a helpful assistant."},
			{"role": "user", "content": "Hello world!"},
			{"role": "assistant", "content": "Hello! How can I help you today?"},
			{"role": "user", "content": "What is the capital of France?"}
		]
	}`)

	// Turn 5: much longer conversation
	bodyTurn5 := []byte(`{
		"model": "meta-llama/Llama-3.1-8B-Instruct",
		"messages": [
			{"role": "system", "content": "You are a helpful assistant."},
			{"role": "user", "content": "Hello world!"},
			{"role": "assistant", "content": "Hello! How can I help you today?"},
			{"role": "user", "content": "What is the capital of France?"},
			{"role": "assistant", "content": "The capital of France is Paris."},
			{"role": "user", "content": "Tell me about its history."},
			{"role": "assistant", "content": "Paris has a rich history dating back to..."},
			{"role": "user", "content": "Continue."}
		]
	}`)

	res1 := affinity.Extract(nil, bodyTurn1)
	res2 := affinity.Extract(nil, bodyTurn2)
	res5 := affinity.Extract(nil, bodyTurn5)

	if res1.Tier != affinity.TierPrefixHash || res1.Key == "" {
		t.Fatalf("expected valid TierPrefixHash on turn 1, got %+v", res1)
	}
	if res2.Tier != affinity.TierPrefixHash || res2.Key != res1.Key {
		t.Errorf("turn 2 key %s did not match turn 1 key %s", res2.Key, res1.Key)
	}
	if res5.Tier != affinity.TierPrefixHash || res5.Key != res1.Key {
		t.Errorf("turn 5 key %s did not match turn 1 key %s", res5.Key, res1.Key)
	}
}

func TestPrefixHashDifferentConversations(t *testing.T) {
	convA := []byte(`{"messages":[{"role":"user","content":"Question A"}]}`)
	convB := []byte(`{"messages":[{"role":"user","content":"Question B"}]}`)

	resA := affinity.Extract(nil, convA)
	resB := affinity.Extract(nil, convB)

	if resA.Key == resB.Key {
		t.Errorf("expected different keys for different conversations, got %s", resA.Key)
	}
}

func TestPromptExtraction(t *testing.T) {
	body := []byte(`{"model": "qwen2.5", "prompt": "Once upon a time"}`)
	res := affinity.Extract(nil, body)
	if res.Tier != affinity.TierPrefixHash || res.Key == "" {
		t.Errorf("expected TierPrefixHash for prompt, got %+v", res)
	}
}

func TestFailOpen(t *testing.T) {
	// Empty body
	res1 := affinity.Extract(nil, nil)
	if res1.Tier != affinity.TierNone || res1.Reason != affinity.ReasonNoKey {
		t.Errorf("expected TierNone/ReasonNoKey for nil body, got %+v", res1)
	}

	// Malformed JSON
	res2 := affinity.Extract(nil, []byte(`{not valid json`))
	if res2.Tier != affinity.TierNone || res2.Reason != telemetry.ReasonUnparseableBody {
		t.Errorf("expected TierNone/telemetry.ReasonUnparseableBody for malformed json, got %+v", res2)
	}

	// Empty messages array
	res3 := affinity.Extract(nil, []byte(`{"messages": []}`))
	if res3.Tier != affinity.TierNone || res3.Reason != affinity.ReasonEmptyMessages {
		t.Errorf("expected TierNone/ReasonEmptyMessages for empty messages, got %+v", res3)
	}

	// No messages or prompt
	res4 := affinity.Extract(nil, []byte(`{"model": "llama"}`))
	if res4.Tier != affinity.TierNone || res4.Reason != affinity.ReasonNoKey {
		t.Errorf("expected TierNone/ReasonNoKey for missing messages, got %+v", res4)
	}
}
