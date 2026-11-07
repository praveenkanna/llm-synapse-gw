package runner

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"
)

// API paths the load generator can drive.
const (
	APIChat        = "chat"
	APICompletions = "completions"

	pathChat        = "/v1/chat/completions"
	pathCompletions = "/v1/completions"
)

// Request is one generated request. Body is sent as is.
type Request struct {
	Class  string
	Path   string
	Stream bool
	Body   []byte
	// Priority, when set, is sent as the X-Priority header: the gateway's
	// priority class for the request.
	Priority string
	// TemplateID and TemplateVersion, when both set, are sent as the
	// X-Template-Id and X-Template-Version headers: the registered
	// template version the messages were rendered from (ADR 0006).
	TemplateID      string
	TemplateVersion int
	// Headers are extra request headers, e.g. mockllm's X-Mock-Output-Tokens.
	// They never replace a header the client sets itself.
	Headers map[string]string
}

// Source produces the request sequence. The runner calls Next from one
// goroutine at a time, in sequence order, so implementations need not be
// safe for concurrent use; a seeded Source therefore yields the same
// sequence on every run. Workload profiles (M0-14) plug in here.
type Source interface {
	Next() Request
}

// TimedSource is a Source whose request mix depends on when a request is
// scheduled, e.g. a profile whose bursts add only background work. In rate
// mode the runner calls NextAt with each arrival's offset from the run start
// instead of Next; in concurrency mode it calls Next.
type TimedSource interface {
	Source
	NextAt(at time.Duration) Request
}

// ClassSpec describes one traffic class of the built-in synthetic source.
type ClassSpec struct {
	Name        string  `json:"name"`
	Weight      float64 `json:"weight"`
	Stream      bool    `json:"stream"`
	MaxTokens   int     `json:"max_tokens"`
	PromptWords int     `json:"prompt_words"`
}

// ParseClassSpec parses "name:weight:stream|nostream:max_tokens:prompt_words".
// Trailing fields may be omitted and take defaults (1, stream, 128, 64).
func ParseClassSpec(s string) (ClassSpec, error) {
	c := ClassSpec{Weight: 1, Stream: true, MaxTokens: 128, PromptWords: 64}
	parts := strings.Split(s, ":")
	if len(parts) > 5 {
		return c, fmt.Errorf("class %q: want name:weight:stream|nostream:max_tokens:prompt_words", s)
	}
	c.Name = parts[0]
	var err error
	if len(parts) > 1 {
		if c.Weight, err = strconv.ParseFloat(parts[1], 64); err != nil {
			return c, fmt.Errorf("class %q: weight: %w", s, err)
		}
	}
	if len(parts) > 2 {
		switch parts[2] {
		case "stream":
			c.Stream = true
		case "nostream":
			c.Stream = false
		default:
			return c, fmt.Errorf("class %q: mode must be stream or nostream", s)
		}
	}
	if len(parts) > 3 {
		if c.MaxTokens, err = strconv.Atoi(parts[3]); err != nil {
			return c, fmt.Errorf("class %q: max_tokens: %w", s, err)
		}
	}
	if len(parts) > 4 {
		if c.PromptWords, err = strconv.Atoi(parts[4]); err != nil {
			return c, fmt.Errorf("class %q: prompt_words: %w", s, err)
		}
	}
	return c, c.validate()
}

func (c ClassSpec) validate() error {
	switch {
	case c.Name == "" || strings.ContainsAny(c.Name, " \t\n,:"):
		return fmt.Errorf("class name %q must be non-empty without spaces, commas or colons", c.Name)
	case !(c.Weight > 0):
		return fmt.Errorf("class %q: weight must be > 0", c.Name)
	case c.MaxTokens < 1:
		return fmt.Errorf("class %q: max_tokens must be >= 1", c.Name)
	case c.PromptWords < 1:
		return fmt.Errorf("class %q: prompt_words must be >= 1", c.Name)
	}
	return nil
}

// SyntheticConfig configures the built-in synthetic source.
type SyntheticConfig struct {
	Model       string      `json:"model"`
	API         string      `json:"api"`
	SystemWords int         `json:"system_words"` // shared system prompt per class; 0 disables
	Classes     []ClassSpec `json:"classes"`
}

// Synthetic generates requests from a weighted class mix with synthetic,
// meaningless text (never real prompts). Each class has one shared system
// prompt, fixed by the seed, and a fresh user message per request.
type Synthetic struct {
	cfg     SyntheticConfig
	path    string
	rng     *rand.Rand
	cum     []float64 // cumulative class weights
	systems []string
}

// NewSynthetic builds a synthetic source. The same seed yields the same
// request sequence.
func NewSynthetic(cfg SyntheticConfig, seed uint64) (*Synthetic, error) {
	if cfg.Model == "" {
		return nil, errors.New("model is required")
	}
	if cfg.SystemWords < 0 {
		return nil, errors.New("system_words must be >= 0")
	}
	if len(cfg.Classes) == 0 {
		return nil, errors.New("at least one class is required")
	}
	s := &Synthetic{cfg: cfg, rng: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))}
	switch cfg.API {
	case APIChat, "":
		s.cfg.API, s.path = APIChat, pathChat
	case APICompletions:
		s.path = pathCompletions
	default:
		return nil, fmt.Errorf("api must be %q or %q", APIChat, APICompletions)
	}
	seen := map[string]bool{}
	total := 0.0
	for _, c := range cfg.Classes {
		if err := c.validate(); err != nil {
			return nil, err
		}
		if seen[c.Name] {
			return nil, fmt.Errorf("class %q listed twice", c.Name)
		}
		seen[c.Name] = true
		total += c.Weight
		s.cum = append(s.cum, total)
		s.systems = append(s.systems, s.words(cfg.SystemWords))
	}
	return s, nil
}

// syllables build synthetic words; the text carries no meaning.
var syllables = [...]string{
	"ka", "lo", "mi", "ne", "ru", "ta", "vo", "xe", "zi", "po", "qua", "sen",
	"dor", "fal", "gim", "hep", "jus", "ket", "lum", "mov", "nix", "ost", "pra", "ril",
}

func (s *Synthetic) words(n int) string {
	var b strings.Builder
	for i := range n {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(syllables[s.rng.IntN(len(syllables))])
		b.WriteString(syllables[s.rng.IntN(len(syllables))])
	}
	return b.String()
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type streamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

type genBody struct {
	Model         string         `json:"model"`
	Messages      []chatMessage  `json:"messages,omitempty"`
	Prompt        string         `json:"prompt,omitempty"`
	MaxTokens     int            `json:"max_tokens"`
	Stream        bool           `json:"stream"`
	StreamOptions *streamOptions `json:"stream_options,omitempty"`
}

// Next returns the next request in the sequence.
func (s *Synthetic) Next() Request {
	x := s.rng.Float64() * s.cum[len(s.cum)-1]
	i := 0
	for i < len(s.cum)-1 && x >= s.cum[i] {
		i++
	}
	c := s.cfg.Classes[i]
	user := s.words(c.PromptWords)
	body := genBody{Model: s.cfg.Model, MaxTokens: c.MaxTokens, Stream: c.Stream}
	if c.Stream {
		body.StreamOptions = &streamOptions{IncludeUsage: true}
	}
	if s.cfg.API == APIChat {
		if sys := s.systems[i]; sys != "" {
			body.Messages = append(body.Messages, chatMessage{Role: "system", Content: sys})
		}
		body.Messages = append(body.Messages, chatMessage{Role: "user", Content: user})
	} else {
		body.Prompt = strings.TrimSpace(s.systems[i] + "\n" + user)
	}
	b, err := json.Marshal(body)
	if err != nil { // only plain strings and ints; cannot fail
		panic(err)
	}
	return Request{Class: c.Name, Path: s.path, Stream: c.Stream, Body: b}
}
