// Package compiler is the request path's template step (M3, ADR 0006). A
// request that names a template with X-Template-Id and X-Template-Version
// is checked against the registry snapshot: when its messages open with
// the version's static segments it is labelled with the template, and
// without a caller cap it may get the version's max_tokens (ADR 0007).
// Anything else forwards unchanged with a fail-open reason. The step never
// rewrites messages and never waits on the store.
package compiler

import (
	"math"
	"net/http"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates"
)

// maxTokensQuantile is the observed completion length the cap covers.
const maxTokensQuantile = 0.999

// Snapshotter returns the current registry snapshot; nil until loaded.
type Snapshotter interface {
	Snapshot() *templates.Snapshot
}

// Options configures a Compiler.
type Options struct {
	MaxTokens config.MaxTokensPolicy
	// Ceilings maps pool names to pools[].max_tokens_ceiling (0: never set).
	Ceilings map[string]int
	// ModelLens maps pool names to pools[].max_model_len (0: unchecked).
	ModelLens map[string]int
	// MaxVersions bounds the completion-length table (templates.max_versions).
	MaxVersions int
}

// Compiler is safe for concurrent use.
type Compiler struct {
	reg       Snapshotter
	policy    config.MaxTokensPolicy
	ceilings  map[string]int
	modelLens map[string]int
	lengths   *lengthStats
}

// New returns a Compiler over reg. reg may be nil (registry off): then
// every request naming a template fails open as registry_unavailable.
func New(reg Snapshotter, o Options) *Compiler {
	if o.MaxVersions <= 0 {
		o.MaxVersions = templates.DefaultMaxVersions
	}
	return &Compiler{reg: reg, policy: o.MaxTokens, ceilings: o.Ceilings, modelLens: o.ModelLens, lengths: newLengthStats(o.MaxVersions)}
}

// FromConfig returns a Compiler configured from cfg over reg.
func FromConfig(cfg *config.Config, reg Snapshotter) *Compiler {
	ceilings := make(map[string]int, len(cfg.Pools))
	modelLens := make(map[string]int, len(cfg.Pools))
	for _, p := range cfg.Pools {
		ceilings[p.Name] = p.MaxTokensCeiling
		modelLens[p.Name] = p.MaxModelLen
	}
	return New(reg, Options{MaxTokens: cfg.Templates.MaxTokens, Ceilings: ceilings, ModelLens: modelLens, MaxVersions: cfg.Templates.MaxVersions})
}

// Match checks the request against the snapshot. named is false when the
// request names no template (nothing to do). Otherwise it returns the
// matched version, or nil and the fail-open reason.
func (c *Compiler) Match(h http.Header, body []byte) (v *templates.Version, reason string, named bool) {
	id := h.Get(templates.HeaderTemplateID)
	if id == "" {
		return nil, "", false
	}
	var snap *templates.Snapshot
	if c != nil && c.reg != nil {
		snap = c.reg.Snapshot()
	}
	v, reason = snap.Match(id, h.Get(templates.HeaderTemplateVersion), body)
	return v, reason, true
}

// MatchIndex is Match over the request's already-read body index (AD-15).
func (c *Compiler) MatchIndex(h http.Header, ix *bodyindex.Index) (v *templates.Version, reason string, named bool) {
	id := h.Get(templates.HeaderTemplateID)
	if id == "" {
		return nil, "", false
	}
	var snap *templates.Snapshot
	if c != nil && c.reg != nil {
		snap = c.reg.Snapshot()
	}
	v, reason = snap.MatchIndex(id, h.Get(templates.HeaderTemplateVersion), ix)
	return v, reason, true
}

// Cap returns the max_tokens to set on a matched request to pool that
// carries no cap of its own, or 0 to set none: min(observed p99.9 × (1 +
// margin), the pool ceiling), the ceiling alone until the version has
// min_samples completions, and nothing on a pool without a ceiling.
//
// A pool that states its max_model_len gets no cap when the request's
// prompt could leave less room than the cap: vLLM refuses prompt tokens
// plus max_tokens above that length, and a request that works without the
// gateway's cap must not fail with it. A prompt has at most as many tokens
// as its body has bytes, plus the chat template's special tokens
// (ModelLenSlack), so bodyBytes bounds it without a tokenizer call.
// nearModelLen says that is why n is 0.
func (c *Compiler) Cap(v *templates.Version, pool string, bodyBytes int) (n int, nearModelLen bool) {
	return c.capWithPromptBound(v, pool, bodyBytes+ModelLenSlack)
}

// CapForTokens is Cap when budgeted context assembly has already measured the
// exact prompt token count via /tokenize (including chat template markers),
// avoiding the conservative 1-byte-per-token upper bound.
func (c *Compiler) CapForTokens(v *templates.Version, pool string, promptTokens int) (n int, nearModelLen bool) {
	return c.capWithPromptBound(v, pool, promptTokens)
}

func (c *Compiler) capWithPromptBound(v *templates.Version, pool string, promptBound int) (n int, nearModelLen bool) {
	if c == nil || v == nil || !c.policy.Enabled {
		return 0, false
	}
	ceiling := c.ceilings[pool]
	if ceiling <= 0 {
		return 0, false
	}
	n = ceiling
	if q, ok := c.lengths.quantile(versionKey{v.ID, v.Version}, maxTokensQuantile, c.policy.MinSamples); ok {
		capped := int(math.Ceil(float64(q) * (1 + c.policy.Margin)))
		n = min(max(capped, 1), ceiling)
	}
	if modelLen := c.modelLens[pool]; modelLen > 0 && promptBound+n > modelLen {
		return 0, true
	}
	return n, false
}

// ModelLenSlack is the tokens the chat template adds that are not bytes of
// the body (role markers, the generation prompt), allowed for when checking
// a prompt against the pool's max_model_len.
const ModelLenSlack = 256

// Observe records a matched request's completion length.
func (c *Compiler) Observe(v *templates.Version, completionTokens int) {
	if c == nil || v == nil {
		return
	}
	c.lengths.observe(versionKey{v.ID, v.Version}, completionTokens)
}

// Retain drops the length statistics of versions not in snap. Call it
// when the snapshot changes.
func (c *Compiler) Retain(snap *templates.Snapshot) {
	if c == nil {
		return
	}
	c.lengths.retain(func(k versionKey) bool {
		_, ok := snap.Lookup(k.id, k.version)
		return ok
	})
}
