package mockllm

import (
	"encoding/json"
	"net/http"
)

// tokenizeRequest is vLLM's POST /tokenize body: the chat form (messages,
// add_generation_prompt defaulting to true) or the completion form (prompt).
// add_special_tokens is accepted; the stand-in tokenizer has none to add.
type tokenizeRequest struct {
	Model               string          `json:"model"`
	Messages            json.RawMessage `json:"messages"`
	Prompt              json.RawMessage `json:"prompt"`
	AddGenerationPrompt *bool           `json:"add_generation_prompt"`
	AddSpecialTokens    *bool           `json:"add_special_tokens"`
}

type tokenizeResponse struct {
	Count       int   `json:"count"`
	MaxModelLen int   `json:"max_model_len"`
	Tokens      []int `json:"tokens"`
}

// handleTokenize serves vLLM's /tokenize with the stand-in tokenizer, so a
// count from here equals usage.prompt_tokens of the same chat messages or
// prompt sent to /v1/chat/completions or /v1/completions. It is not a
// generation: it is neither captured nor counted in the request metrics.
func (m *Mock) handleTokenize(w http.ResponseWriter, r *http.Request) {
	tokens, rerr := m.tokenize(w, r)
	if rerr != nil {
		m.writeError(w, rerr.status, rerr.msg, rerr.errType)
		return
	}
	if tokens == nil {
		tokens = []int{}
	}
	writeJSON(w, http.StatusOK, tokenizeResponse{Count: len(tokens), MaxModelLen: m.cfg.MaxModelLen, Tokens: tokens})
}

func (m *Mock) tokenize(w http.ResponseWriter, r *http.Request) ([]int, *requestError) {
	body, rerr := m.readBody(w, r)
	if rerr != nil {
		return nil, rerr
	}
	var req tokenizeRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, badRequest("invalid JSON body: %v", err)
	}
	if _, rerr := m.resolveModel(req.Model); rerr != nil {
		return nil, rerr
	}
	switch {
	case present(req.Messages):
		msgs, rerr := decodeMessages(req.Messages)
		if rerr != nil {
			return nil, rerr
		}
		addGenerationPrompt := req.AddGenerationPrompt == nil || *req.AddGenerationPrompt
		return Tokenize(RenderChat(msgs, addGenerationPrompt)), nil
	case present(req.Prompt):
		tokens, err := promptTokens(req.Prompt)
		if err != nil {
			return nil, badRequest("%v", err)
		}
		return tokens, nil
	}
	return nil, badRequest("either messages or prompt is required")
}

func present(raw json.RawMessage) bool { return len(raw) > 0 && string(raw) != "null" }
