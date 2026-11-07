package mockllm

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
)

// The stand-in tokenizer. mockllm has no model, so it counts tokens with a
// fixed, deterministic scheme that the gateway's tests can reproduce exactly:
// a chat request renders as "<|role|>\n" + content + "\n" per message, plus
// "<|assistant|>\n" when a generation prompt is added, and the rendered text
// splits into consecutive BytesPerToken-byte chunks (the last may be shorter).
// Each chunk's token id is a 31-bit FNV-1a hash of its bytes. Completions
// tokenize the prompt as it is.

// BytesPerToken is the stand-in tokenizer's chunk size.
const BytesPerToken = 4

const (
	fnv32Offset = 2166136261
	fnv32Prime  = 16777619
	tokenIDMask = 1<<31 - 1
)

// Message is a chat message as the stand-in tokenizer sees it. Content
// decodes from a string, null, or an OpenAI array of content parts, whose
// "text" parts are concatenated (other part types contribute nothing).
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// UnmarshalJSON implements json.Unmarshaler.
func (m *Message) UnmarshalJSON(b []byte) error {
	var raw struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	content, err := contentText(raw.Content)
	if err != nil {
		return err
	}
	*m = Message{Role: raw.Role, Content: content}
	return nil
}

var errContentType = errors.New("message content must be a string, null or an array of content parts")

func contentText(raw json.RawMessage) (string, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || string(raw) == "null" {
		return "", nil
	}
	switch raw[0] {
	case '"':
		var s string
		err := json.Unmarshal(raw, &s)
		return s, err
	case '[':
		var parts []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if err := json.Unmarshal(raw, &parts); err != nil {
			return "", err
		}
		var b strings.Builder
		for _, p := range parts {
			if p.Type == "text" {
				b.WriteString(p.Text)
			}
		}
		return b.String(), nil
	}
	return "", errContentType
}

// RenderChat renders messages the way the stand-in chat template does.
func RenderChat(messages []Message, addGenerationPrompt bool) string {
	var b strings.Builder
	for _, m := range messages {
		b.WriteString("<|")
		b.WriteString(m.Role)
		b.WriteString("|>\n")
		b.WriteString(m.Content)
		b.WriteString("\n")
	}
	if addGenerationPrompt {
		b.WriteString("<|assistant|>\n")
	}
	return b.String()
}

// Tokenize returns the stand-in token ids of text.
func Tokenize(text string) []int {
	tokens := make([]int, 0, CountText(text))
	for i := 0; i < len(text); i += BytesPerToken {
		tokens = append(tokens, tokenID(text[i:min(i+BytesPerToken, len(text))]))
	}
	return tokens
}

// CountText returns len(Tokenize(text)) without tokenizing.
func CountText(text string) int {
	return (len(text) + BytesPerToken - 1) / BytesPerToken
}

// CountChat returns the stand-in token count of a chat request's messages.
// Chat completions always add the generation prompt.
func CountChat(messages []Message, addGenerationPrompt bool) int {
	return CountText(RenderChat(messages, addGenerationPrompt))
}

func tokenID(chunk string) int {
	h := uint32(fnv32Offset)
	for i := 0; i < len(chunk); i++ {
		h ^= uint32(chunk[i])
		h *= fnv32Prime
	}
	return int(h & tokenIDMask)
}

// promptTokens tokenizes a /v1/completions "prompt": a string, an array of
// strings (tokenized one by one and concatenated), or token ids (an array, or
// an array of arrays, concatenated). Absent or null is no tokens.
func promptTokens(raw json.RawMessage) ([]int, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return Tokenize(s), nil
	}
	var strs []string
	if json.Unmarshal(raw, &strs) == nil {
		var out []int
		for _, s := range strs {
			out = append(out, Tokenize(s)...)
		}
		return out, nil
	}
	var ids []int
	if json.Unmarshal(raw, &ids) == nil {
		return ids, nil
	}
	var batches [][]int
	if json.Unmarshal(raw, &batches) == nil {
		var out []int
		for _, b := range batches {
			out = append(out, b...)
		}
		return out, nil
	}
	return nil, errors.New("prompt must be a string, an array of strings, or token ids")
}
