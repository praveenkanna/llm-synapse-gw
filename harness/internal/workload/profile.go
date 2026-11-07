// Package workload loads synthetic workload profiles (harness/profiles/*.yaml)
// and generates the request sequence they describe, as a runner.Source.
//
// A profile is a mix of traffic classes. Each class sets its API mode
// (streaming or not, JSON response format), prompt and output length
// distributions, shared system prompts and the share of conversations that
// use them (the shared-prefix ratio), turns per conversation with growing
// history, optionally the gateway priority class it sends as X-Priority,
// and optionally a trigger that fires it around another class's
// requests (classification calls around each chat turn). The profile also
// sets the default arrival rate and burst shape, and which classes a burst
// adds load to.
//
// A class can instead render its messages from a template version spec
// (the gateway registry's JSON format) with generated variable values, and
// send the template ID and version as headers (ADR 0006).
//
// All text is synthetic: meaningless words built from a fixed syllable
// table, fixed by the seed, or the invented text of a template spec. No real
// prompt is ever read, stored or sent.
package workload

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// API modes a profile can drive.
const (
	APIChat        = "chat"
	APICompletions = "completions"
)

// ResponseFormatJSON asks for a JSON object response (vLLM guided decoding).
const ResponseFormatJSON = "json_object"

// Ceilings that keep generator state bounded.
const (
	maxClasses           = 32
	maxSharedPrefixes    = 1024
	maxOpenConversations = 10_000
	maxTurns             = 100
	maxProfileBytes      = 1 << 20
	maxPriorityLen       = 32
	maxHeaders           = 16
)

// Profile is one workload profile file.
type Profile struct {
	Name        string  `yaml:"name" json:"name"`
	Description string  `yaml:"description,omitempty" json:"description,omitempty"`
	API         string  `yaml:"api,omitempty" json:"api"`               // chat (default) or completions
	Model       string  `yaml:"model,omitempty" json:"model,omitempty"` // default model for classes that set none
	Arrival     Arrival `yaml:"arrival" json:"arrival"`                 // defaults for loadgen -rate and -burst-*
	Classes     []Class `yaml:"classes" json:"classes"`                 // traffic classes, at least one
}

// Arrival is the profile's default open-loop arrival process: Poisson at
// Rate per second, with optional periodic bursts.
type Arrival struct {
	Rate  float64 `yaml:"rate" json:"rate"`
	Burst Burst   `yaml:"burst,omitempty" json:"burst"`
}

// Burst raises the arrival rate to Factor times the base rate for Length
// every Every (windows start at Every, 2*Every, ...). When Classes is set, the
// extra arrivals of a burst go only to those classes (weighted by their
// weights) and the others keep their base rate; otherwise every class scales.
// A burst class must not trigger other classes.
type Burst struct {
	Every   time.Duration `yaml:"every,omitempty" json:"every_ns,omitempty"`
	Length  time.Duration `yaml:"length,omitempty" json:"length_ns,omitempty"`
	Factor  float64       `yaml:"factor,omitempty" json:"factor,omitempty"`
	Classes []string      `yaml:"classes,omitempty" json:"classes,omitempty"`
}

// Class is one traffic class.
type Class struct {
	Name   string  `yaml:"name" json:"name"`
	Model  string  `yaml:"model,omitempty" json:"model,omitempty"` // overrides the profile model
	Weight float64 `yaml:"weight" json:"weight"`                   // share of arrivals; 0 only for triggered classes
	Stream bool    `yaml:"stream" json:"stream"`
	// ResponseFormat is "" (free text) or json_object.
	ResponseFormat string `yaml:"response_format,omitempty" json:"response_format,omitempty"`
	// IgnoreEOS asks vLLM to generate exactly max_tokens, so output lengths
	// follow MaxTokens on a real model too. vLLM-only; mockllm ignores it.
	IgnoreEOS bool `yaml:"ignore_eos,omitempty" json:"ignore_eos,omitempty"`
	// Priority, when set, is sent as the X-Priority header on every request
	// of the class (a gateway priority class such as interactive, user or
	// background).
	Priority string `yaml:"priority,omitempty" json:"priority,omitempty"`
	// Temperature and Seed, when set, are sent in every request body. A
	// request is deterministic for the gateway's coalescing and response
	// cache only with temperature 0 or an integer seed (ADR 0019).
	Temperature *float64 `yaml:"temperature,omitempty" json:"temperature,omitempty"`
	Seed        *int64   `yaml:"seed,omitempty" json:"seed,omitempty"`
	// Headers are extra request headers on every request of the class,
	// e.g. mockllm's X-Mock-Output-Tokens to force long outputs.
	Headers map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`

	// SystemWords is the length of each system prompt (0 = none).
	SystemWords Dist `yaml:"system_words" json:"system_words"`
	// SharedPrefixes is how many distinct shared system prompts the class
	// has; they are fixed by the seed and reused across conversations.
	SharedPrefixes int `yaml:"shared_prefixes" json:"shared_prefixes"`
	// SharedPrefixRatio is the share of conversations that start with a
	// shared system prompt; the rest get a unique one of the same length
	// distribution.
	SharedPrefixRatio float64 `yaml:"shared_prefix_ratio" json:"shared_prefix_ratio"`

	PromptWords Dist `yaml:"prompt_words" json:"prompt_words"` // user message per turn
	MaxTokens   Dist `yaml:"max_tokens" json:"max_tokens"`     // max_tokens per request

	// Turns per conversation (default 1: single shot). Each later turn
	// resends the whole history: system, then user/assistant pairs, then
	// the new user message.
	Turns Dist `yaml:"turns,omitempty" json:"turns"`
	// ReplyWords is the length of the synthetic assistant reply added to
	// the history after each turn. Required when Turns can exceed 1.
	ReplyWords Dist `yaml:"reply_words,omitempty" json:"reply_words"`
	// OpenConversations caps conversations in progress; each multi-turn
	// request continues a random open one or starts a new one in a free
	// slot, so conversations interleave.
	OpenConversations int `yaml:"open_conversations,omitempty" json:"open_conversations,omitempty"`
	// MaxContextWords ends a conversation early when its next request
	// would exceed this many words, keeping requests inside the model's
	// context (0 = no limit).
	MaxContextWords int `yaml:"max_context_words,omitempty" json:"max_context_words,omitempty"`

	// TriggeredBy fires Count requests of this class right after each
	// request of another class, taking the next arrivals.
	TriggeredBy *Trigger `yaml:"triggered_by,omitempty" json:"triggered_by,omitempty"`

	// Template, when set, renders every request's messages from a template
	// spec instead of synthetic words: single-shot, chat API, and no
	// system_words, shared prefixes or prompt_words.
	Template *Template `yaml:"template,omitempty" json:"template,omitempty"`
}

// Trigger ties a class to another class's requests.
type Trigger struct {
	Class string `yaml:"class" json:"class"`
	Count Dist   `yaml:"count" json:"count"`
}

// Load reads and validates a profile file.
func Load(path string) (*Profile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("profile: %w", err)
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxProfileBytes+1))
	if err != nil {
		return nil, fmt.Errorf("profile %s: %w", path, err)
	}
	p, err := parse(b, filepath.Dir(path))
	if err != nil {
		return nil, fmt.Errorf("profile %s: %w", path, err)
	}
	return p, nil
}

// Parse decodes and validates a profile. Unknown keys are rejected.
// Template spec paths are resolved against the working directory.
func Parse(b []byte) (*Profile, error) { return parse(b, "") }

// parse is Parse with template spec paths resolved against dir.
func parse(b []byte, dir string) (*Profile, error) {
	if len(b) > maxProfileBytes {
		return nil, fmt.Errorf("larger than %d bytes", maxProfileBytes)
	}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	var p Profile
	if err := dec.Decode(&p); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("empty profile")
		}
		return nil, err
	}
	out := p.withDefaults()
	for _, c := range out.Classes {
		if c.Template != nil {
			if err := c.Template.load(dir); err != nil {
				return nil, fmt.Errorf("class %q: %w", c.Name, err)
			}
		}
	}
	if err := out.Validate(); err != nil {
		return nil, err
	}
	return out, nil
}

// withDefaults returns a copy of p with omitted optional fields filled in:
// the chat API, one turn per conversation and no system prompt.
func (p *Profile) withDefaults() *Profile {
	out := *p
	out.Classes = slices.Clone(p.Classes)
	if out.API == "" {
		out.API = APIChat
	}
	for i := range out.Classes {
		c := &out.Classes[i]
		if c.Turns.IsZero() {
			c.Turns = Fixed(1)
		}
		if c.SystemWords.IsZero() {
			c.SystemWords = Fixed(0)
		}
	}
	return &out
}

// Validate checks the whole profile.
func (p *Profile) Validate() error {
	if p.Name == "" {
		return errors.New("name is required")
	}
	if p.API != APIChat && p.API != APICompletions {
		return fmt.Errorf("api must be %q or %q", APIChat, APICompletions)
	}
	if err := p.Arrival.validate(); err != nil {
		return fmt.Errorf("arrival: %w", err)
	}
	if len(p.Classes) == 0 || len(p.Classes) > maxClasses {
		return fmt.Errorf("want 1 to %d classes, got %d", maxClasses, len(p.Classes))
	}
	byName := map[string]*Class{}
	total := 0.0
	for i := range p.Classes {
		c := &p.Classes[i]
		if err := c.validate(p.API); err != nil {
			return fmt.Errorf("class %q: %w", c.Name, err)
		}
		if byName[c.Name] != nil {
			return fmt.Errorf("class %q listed twice", c.Name)
		}
		byName[c.Name] = c
		total += c.Weight
	}
	if !(total > 0) {
		return errors.New("at least one class needs a weight > 0")
	}
	for _, c := range p.Classes {
		if t := c.TriggeredBy; t != nil {
			src := byName[t.Class]
			switch {
			case src == nil:
				return fmt.Errorf("class %q: triggered_by unknown class %q", c.Name, t.Class)
			case src.Name == c.Name:
				return fmt.Errorf("class %q: triggers itself", c.Name)
			case src.TriggeredBy != nil:
				return fmt.Errorf("class %q: triggered by %q, which is itself triggered (one level only)", c.Name, t.Class)
			}
		}
	}
	burstWeight := 0.0
	for _, name := range p.Arrival.Burst.Classes {
		c := byName[name]
		if c == nil {
			return fmt.Errorf("arrival.burst.classes: unknown class %q", name)
		}
		burstWeight += c.Weight
		for _, t := range p.Classes {
			if t.TriggeredBy != nil && t.TriggeredBy.Class == name {
				// Its triggered calls would add load to a non-burst class
				// inside every window.
				return fmt.Errorf("arrival.burst.classes: %q triggers %q; burst classes must not trigger others", name, t.Name)
			}
		}
	}
	if len(p.Arrival.Burst.Classes) > 0 && !(burstWeight > 0) {
		return errors.New("arrival.burst.classes need a total weight > 0")
	}
	return nil
}

func (a Arrival) validate() error {
	if !(a.Rate > 0) || math.IsInf(a.Rate, 0) {
		return errors.New("rate must be a positive number")
	}
	return a.Burst.validate()
}

func (b Burst) validate() error {
	if b.Every == 0 {
		if b.Length != 0 || b.Factor != 0 || len(b.Classes) > 0 {
			return errors.New("burst needs every > 0")
		}
		return nil
	}
	if b.Every < 0 || b.Length <= 0 || b.Length >= b.Every {
		return errors.New("burst needs every > 0 and 0 < length < every")
	}
	if !(b.Factor > 0) || math.IsInf(b.Factor, 0) {
		return errors.New("burst factor must be a positive number")
	}
	if len(b.Classes) > 0 && !(b.Factor > 1) {
		return errors.New("burst factor must be > 1 when burst classes are set")
	}
	return nil
}

func (c *Class) validate(api string) error {
	checks := []struct {
		name string
		d    Dist
		lo   int
	}{
		{"system_words", c.SystemWords, 0},
		{"max_tokens", c.MaxTokens, 1},
		{"turns", c.Turns, 1},
	}
	if c.Template != nil {
		if err := c.validateTemplate(api); err != nil {
			return fmt.Errorf("template: %w", err)
		}
	} else {
		checks = append(checks, struct {
			name string
			d    Dist
			lo   int
		}{"prompt_words", c.PromptWords, 1})
	}
	switch {
	case c.Name == "" || strings.ContainsAny(c.Name, " \t\n,:"):
		return errors.New("name must be non-empty without spaces, commas or colons")
	case c.Weight < 0 || math.IsNaN(c.Weight) || math.IsInf(c.Weight, 0):
		return errors.New("weight must be a non-negative number")
	case c.Weight == 0 && c.TriggeredBy == nil:
		return errors.New("weight must be > 0 unless the class is triggered_by another")
	case c.ResponseFormat != "" && c.ResponseFormat != ResponseFormatJSON:
		return fmt.Errorf("response_format must be empty or %q", ResponseFormatJSON)
	case c.ResponseFormat != "" && api != APIChat:
		return errors.New("response_format needs the chat API")
	case c.SharedPrefixes < 0 || c.SharedPrefixes > maxSharedPrefixes:
		return fmt.Errorf("shared_prefixes must be in [0, %d]", maxSharedPrefixes)
	case !(c.SharedPrefixRatio >= 0 && c.SharedPrefixRatio <= 1):
		return errors.New("shared_prefix_ratio must be in [0, 1]")
	case c.SharedPrefixRatio > 0 && c.SharedPrefixes == 0:
		return errors.New("shared_prefix_ratio > 0 needs shared_prefixes > 0")
	case c.SharedPrefixRatio > 0 && c.SystemWords.Upper() == 0:
		return errors.New("shared_prefix_ratio > 0 needs system_words > 0")
	case c.MaxContextWords < 0:
		return errors.New("max_context_words must be >= 0")
	case c.Priority != "" && !validPriority(c.Priority):
		return fmt.Errorf("priority must be 1 to %d of a-z, 0-9, '-' or '_'", maxPriorityLen)
	case c.Temperature != nil && !(*c.Temperature >= 0 && *c.Temperature <= 2):
		return errors.New("temperature must be in [0, 2]")
	}
	if err := validateHeaders(c.Headers); err != nil {
		return fmt.Errorf("headers: %w", err)
	}
	if c.Turns.Upper() > 1 {
		checks = append(checks, struct {
			name string
			d    Dist
			lo   int
		}{"reply_words", c.ReplyWords, 1})
		if c.OpenConversations < 1 || c.OpenConversations > maxOpenConversations {
			return fmt.Errorf("multi-turn classes need open_conversations in [1, %d]", maxOpenConversations)
		}
	} else if !c.ReplyWords.IsZero() || c.OpenConversations != 0 {
		return errors.New("reply_words and open_conversations apply to multi-turn classes only")
	}
	for _, ch := range checks {
		if err := ch.d.Validate(ch.lo); err != nil {
			return fmt.Errorf("%s: %w", ch.name, err)
		}
	}
	if c.Turns.Upper() > maxTurns {
		return fmt.Errorf("turns must be at most %d", maxTurns)
	}
	if t := c.TriggeredBy; t != nil {
		if t.Class == "" {
			return errors.New("triggered_by.class is required")
		}
		if err := t.Count.Validate(0); err != nil {
			return fmt.Errorf("triggered_by.count: %w", err)
		}
		if t.Count.Upper() > maxTriggered {
			return fmt.Errorf("triggered_by.count must be at most %d", maxTriggered)
		}
	}
	if c.MaxContextWords > 0 && c.SystemWords.Upper()+c.PromptWords.Upper() > c.MaxContextWords {
		return errors.New("max_context_words is smaller than the longest first turn")
	}
	return nil
}

// validateTemplate checks that a template class sets nothing the template
// replaces, then the template block itself.
func (c *Class) validateTemplate(api string) error {
	switch {
	case api != APIChat:
		return errors.New("needs the chat API")
	case !c.PromptWords.IsZero() || c.SystemWords.Upper() != 0 || c.SharedPrefixes != 0 || c.SharedPrefixRatio != 0:
		return errors.New("replaces system_words, shared_prefixes, shared_prefix_ratio and prompt_words; leave them unset")
	case c.Turns.Upper() != 1 || c.MaxContextWords != 0:
		return errors.New("is single-shot: leave turns and max_context_words unset")
	}
	return c.Template.validate()
}

// validPriority reports whether s is a usable X-Priority value.
func validPriority(s string) bool {
	if len(s) == 0 || len(s) > maxPriorityLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

// reservedHeaders are set by the client or another class key and may not be
// listed under headers.
var reservedHeaders = map[string]bool{
	"Accept": true, "Authorization": true, "Content-Length": true, "Content-Type": true,
	"Host": true, "Traceparent": true, "X-Priority": true, "X-Template-Id": true, "X-Template-Version": true,
}

// validateHeaders checks a class's extra headers: RFC 9110 token names,
// printable values, at most maxHeaders, none the harness sets itself.
func validateHeaders(h map[string]string) error {
	if len(h) > maxHeaders {
		return fmt.Errorf("at most %d headers", maxHeaders)
	}
	for k, v := range h {
		if k == "" || strings.IndexFunc(k, func(r rune) bool { return !isTokenChar(r) }) >= 0 {
			return fmt.Errorf("%q is not a header name", k)
		}
		if reservedHeaders[http.CanonicalHeaderKey(k)] {
			return fmt.Errorf("%s is set by the harness", http.CanonicalHeaderKey(k))
		}
		if strings.IndexFunc(v, func(r rune) bool { return r < ' ' || r > '~' }) >= 0 {
			return fmt.Errorf("%s: value must be printable ASCII", k)
		}
	}
	return nil
}

func isTokenChar(r rune) bool {
	return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("!#$%&'*+-.^_`|~", r)
}

// ModelFor returns the model a class sends: its own, else the profile's,
// else its template spec's.
func (p *Profile) ModelFor(c Class) string {
	switch {
	case c.Model != "":
		return c.Model
	case p.Model != "":
		return p.Model
	case c.Template != nil && c.Template.spec != nil:
		return c.Template.spec.Model
	}
	return ""
}
