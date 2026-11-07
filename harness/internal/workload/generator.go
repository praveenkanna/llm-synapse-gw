package workload

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/harness/internal/runner"
)

// API paths, as the runner's built-in source uses them.
const (
	pathChat        = "/v1/chat/completions"
	pathCompletions = "/v1/completions"
)

// Triggered-request ceilings. One fresh request enqueues at most
// maxClasses*maxTriggered follow-ups, and triggered requests never trigger
// more, so the queue drains before it can reach maxPending; the cap is a
// guard.
const (
	maxTriggered = 64
	maxPending   = maxClasses * maxTriggered
)

// Options adjusts a profile at run time.
type Options struct {
	// Model, when set, replaces every class's model.
	Model string
	// DefaultModel is used by classes when neither the class nor the
	// profile names a model.
	DefaultModel string
	// Burst is the effective burst shape of the run's arrivals (the
	// profile's, or loadgen's overrides). The generator uses it to send a
	// burst's extra arrivals to the profile's burst classes. Zero means no
	// bursts.
	Burst runner.Burst
}

// Generator produces a profile's request sequence. It implements
// runner.Source and runner.TimedSource. Like every Source, it is not safe
// for concurrent use: the runner calls it from one goroutine at a time.
type Generator struct {
	p       *Profile
	path    string
	models  []string
	rng     *rand.Rand
	burst   runner.Burst
	base    []float64 // cumulative class weights outside bursts
	inBurst []float64 // cumulative class weights inside burst windows
	shared  [][]string
	convs   [][]*conversation // per class; nil for single-turn classes
	pending []int             // triggered class indices, FIFO
	fires   [][]int           // class index -> classes it triggers
}

type conversation struct {
	system    string
	history   []chatMessage
	words     int // words of system + history
	turnsLeft int
}

// NewGenerator returns a generator for p. The same profile, options and seed
// yield the same request sequence.
func NewGenerator(p *Profile, opts Options, seed uint64) (*Generator, error) {
	if p == nil {
		return nil, errors.New("profile is required")
	}
	p = p.withDefaults()
	if err := p.Validate(); err != nil {
		return nil, err
	}
	if err := opts.Burst.Validate(); err != nil {
		return nil, err
	}
	g := &Generator{
		p:     p,
		path:  pathChat,
		rng:   rand.New(rand.NewPCG(seed, seed^0x6a09e667f3bcc908)),
		burst: opts.Burst,
		fires: make([][]int, len(p.Classes)),
	}
	if p.API == APICompletions {
		g.path = pathCompletions
	}
	index := map[string]int{}
	for i, c := range p.Classes {
		index[c.Name] = i
	}
	inBurstClass := map[int]bool{}
	for _, name := range p.Arrival.Burst.Classes {
		inBurstClass[index[name]] = true
	}
	g.base, g.inBurst = classWeights(p, inBurstClass, opts.Burst.Factor)
	for i, c := range p.Classes {
		model := opts.Model
		if model == "" {
			model = p.ModelFor(c)
		}
		if model == "" {
			model = opts.DefaultModel
		}
		if model == "" {
			return nil, fmt.Errorf("class %q: no model (set model in the profile or pass one)", c.Name)
		}
		g.models = append(g.models, model)
		prefixes := make([]string, c.SharedPrefixes)
		for j := range prefixes {
			prefixes[j] = g.words(c.SystemWords.Sample(g.rng))
		}
		g.shared = append(g.shared, prefixes)
		var slots []*conversation
		if c.Turns.Upper() > 1 {
			slots = make([]*conversation, c.OpenConversations)
		}
		g.convs = append(g.convs, slots)
		if t := c.TriggeredBy; t != nil {
			g.fires[index[t.Class]] = append(g.fires[index[t.Class]], i)
		}
	}
	return g, nil
}

// classWeights returns cumulative fresh-draw class weights outside and inside
// burst windows. Triggered requests take arrivals from the same process as
// fresh draws, so with P requests per fresh draw (see triggerLoad) and
// arrival rate R, class c is drawn fresh at R*p_c/P, where p_c = w_c/W.
//
// Inside a window the arrival rate is factor*R. With burst classes, every
// class keeps its base rate and the extra (factor-1)*R goes to the burst
// classes by weight: non-burst class c keeps weight p_c/P and burst class c
// gets p_c/P + (factor-1)*w_c/B, where B is the burst classes' total weight.
// Burst classes trigger nothing (Validate), so the triggered rate stays
// R*(P-1)/P and the weights sum to factor - (P-1)/P, the in-window fresh
// rate over R. With no burst classes every class scales, so the mix is
// unchanged.
func classWeights(p *Profile, burstClass map[int]bool, factor float64) (base, inBurst []float64) {
	var total, burstTotal float64
	for i, c := range p.Classes {
		total += c.Weight
		if burstClass[i] {
			burstTotal += c.Weight
		}
	}
	_, perFresh := p.triggerLoad()
	extra := len(burstClass) > 0 && factor > 1
	var cb, ci float64
	for i, c := range p.Classes {
		share := c.Weight / total
		cb += share
		switch {
		case !extra:
			ci += share
		case burstClass[i]:
			ci += share/perFresh + (factor-1)*c.Weight/burstTotal
		default:
			ci += share / perFresh
		}
		base = append(base, cb)
		inBurst = append(inBurst, ci)
	}
	return base, inBurst
}

// Next returns the next request using the base (non-burst) class mix. The
// closed-loop runner calls it.
func (g *Generator) Next() runner.Request { return g.next(g.base) }

// NextAt returns the next request for an arrival at offset at from the run
// start; arrivals inside a burst window use the burst class mix.
func (g *Generator) NextAt(at time.Duration) runner.Request {
	if g.burst.Windows().Contains(at) {
		return g.next(g.inBurst)
	}
	return g.next(g.base)
}

func (g *Generator) next(cum []float64) runner.Request {
	var i int
	if len(g.pending) > 0 {
		i = g.pending[0]
		g.pending = g.pending[1:]
	} else {
		i = pick(cum, g.rng.Float64()*cum[len(cum)-1])
		for _, t := range g.fires[i] {
			for range g.p.Classes[t].TriggeredBy.Count.Sample(g.rng) {
				if len(g.pending) < maxPending {
					g.pending = append(g.pending, t)
				}
			}
		}
	}
	return g.build(i)
}

func pick(cum []float64, x float64) int {
	i := 0
	for i < len(cum)-1 && x >= cum[i] {
		i++
	}
	return i
}

// build makes one request of class i: a single shot, or the next turn of an
// open conversation.
func (g *Generator) build(i int) runner.Request {
	c := &g.p.Classes[i]
	if c.Template != nil {
		return g.buildTemplate(i)
	}
	userWords := c.PromptWords.Sample(g.rng)
	var conv *conversation
	if slots := g.convs[i]; slots != nil {
		s := g.rng.IntN(len(slots))
		conv = slots[s]
		if conv != nil && c.MaxContextWords > 0 && conv.words+userWords > c.MaxContextWords {
			conv = nil // would overflow the context: the conversation ends
		}
		if conv == nil {
			conv = g.newConversation(i)
			slots[s] = conv
		}
		defer func() {
			reply := g.words(c.ReplyWords.Sample(g.rng))
			conv.words += strings.Count(reply, " ") + 1
			conv.history = append(conv.history, chatMessage{Role: "assistant", Content: reply})
			if conv.turnsLeft--; conv.turnsLeft <= 0 {
				slots[s] = nil
			}
		}()
	} else {
		conv = g.newConversation(i)
	}
	user := chatMessage{Role: "user", Content: g.words(userWords)}
	body := g.body(i, c)
	if g.p.API == APIChat {
		msgs := make([]chatMessage, 0, len(conv.history)+2)
		if conv.system != "" {
			msgs = append(msgs, chatMessage{Role: "system", Content: conv.system})
		}
		body.Messages = append(append(msgs, conv.history...), user)
	} else {
		parts := make([]string, 0, len(conv.history)+2)
		if conv.system != "" {
			parts = append(parts, conv.system)
		}
		for _, m := range conv.history {
			parts = append(parts, m.Content)
		}
		body.Prompt = strings.Join(append(parts, user.Content), "\n")
	}
	if conv.turnsLeft > 0 { // multi-turn: the user message joins the history
		conv.history = append(conv.history, user)
		conv.words += userWords
	}
	return g.request(c, body)
}

// buildTemplate makes one request of template class i: the spec rendered
// with fresh variable values, plus the template headers when set.
func (g *Generator) buildTemplate(i int) runner.Request {
	c := &g.p.Classes[i]
	t := c.Template
	msgs, err := t.spec.render(t.sampleVars(g.rng))
	if err != nil { // every declared variable has a generator (validate)
		panic(err)
	}
	body := g.body(i, c)
	body.Messages = msgs
	r := g.request(c, body)
	r.TemplateID, r.TemplateVersion = t.ID, t.Version
	return r
}

// body returns class i's request fields other than the prompt.
func (g *Generator) body(i int, c *Class) genBody {
	body := genBody{
		Model: g.models[i], MaxTokens: c.MaxTokens.Sample(g.rng), Stream: c.Stream, IgnoreEOS: c.IgnoreEOS,
		Temperature: c.Temperature, Seed: c.Seed,
	}
	if c.Stream {
		body.StreamOptions = &streamOptions{IncludeUsage: true}
	}
	if c.ResponseFormat != "" {
		body.ResponseFormat = &responseFormat{Type: c.ResponseFormat}
	}
	return body
}

func (g *Generator) request(c *Class, body genBody) runner.Request {
	b, err := json.Marshal(body)
	if err != nil { // only strings, ints and bools; cannot fail
		panic(err)
	}
	return runner.Request{Class: c.Name, Path: g.path, Stream: c.Stream, Body: b, Priority: c.Priority, Headers: c.Headers}
}

func (g *Generator) newConversation(i int) *conversation {
	c := &g.p.Classes[i]
	conv := &conversation{}
	if c.SharedPrefixRatio > 0 && g.rng.Float64() < c.SharedPrefixRatio {
		conv.system = g.shared[i][g.rng.IntN(len(g.shared[i]))]
	} else if n := c.SystemWords.Sample(g.rng); n > 0 {
		conv.system = g.words(n)
	}
	if conv.system != "" {
		conv.words = strings.Count(conv.system, " ") + 1
	}
	if g.convs[i] != nil {
		conv.turnsLeft = c.Turns.Sample(g.rng)
	}
	return conv
}

// syllables build synthetic words; the text carries no meaning.
var syllables = [...]string{
	"ka", "lo", "mi", "ne", "ru", "ta", "vo", "xe", "zi", "po", "qua", "sen",
	"dor", "fal", "gim", "hep", "jus", "ket", "lum", "mov", "nix", "ost", "pra", "ril",
}

// words returns n synthetic words separated by single spaces.
func (g *Generator) words(n int) string {
	if n <= 0 {
		return ""
	}
	var b strings.Builder
	b.Grow(n * 7)
	for i := range n {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(syllables[g.rng.IntN(len(syllables))])
		b.WriteString(syllables[g.rng.IntN(len(syllables))])
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

type responseFormat struct {
	Type string `json:"type"`
}

type genBody struct {
	Model          string          `json:"model"`
	Messages       []chatMessage   `json:"messages,omitempty"`
	Prompt         string          `json:"prompt,omitempty"`
	MaxTokens      int             `json:"max_tokens"`
	Temperature    *float64        `json:"temperature,omitempty"`
	Seed           *int64          `json:"seed,omitempty"`
	Stream         bool            `json:"stream"`
	StreamOptions  *streamOptions  `json:"stream_options,omitempty"`
	ResponseFormat *responseFormat `json:"response_format,omitempty"`
	IgnoreEOS      bool            `json:"ignore_eos,omitempty"`
}

// ExpectedShares returns each class's expected share of requests outside
// bursts. Triggered requests take arrivals from the same process, so a
// trigger lowers the other classes' shares. Useful for sizing a rate.
func (p *Profile) ExpectedShares() map[string]float64 {
	total := 0.0
	for _, c := range p.Classes {
		total += c.Weight
	}
	triggered, perFresh := p.triggerLoad()
	out := map[string]float64{}
	for i, c := range p.Classes {
		out[c.Name] = (c.Weight/total + triggered[i]) / perFresh
	}
	return out
}

// triggerLoad returns, per class, the mean number of its requests that one
// fresh draw triggers, and perFresh, the mean number of requests per fresh
// draw (1 plus every triggered one). Weights are normalized.
func (p *Profile) triggerLoad() (triggered []float64, perFresh float64) {
	total := 0.0
	for _, c := range p.Classes {
		total += c.Weight
	}
	triggered = make([]float64, len(p.Classes))
	perFresh = 1
	for i, c := range p.Classes {
		t := c.TriggeredBy
		if t == nil {
			continue
		}
		src := slices.IndexFunc(p.Classes, func(x Class) bool { return x.Name == t.Class })
		triggered[i] = p.Classes[src].Weight / total * t.Count.mean()
		perFresh += triggered[i]
	}
	return triggered, perFresh
}

// mean returns the expected value of the distribution, including clamping.
func (d Dist) mean() float64 {
	switch d.Kind {
	case DistUniform:
		return float64(d.Min+d.Max) / 2
	case DistLognormal:
		// Numerically integrate the clamped lognormal; precision is ample
		// for sizing.
		const steps = 4000
		sum := 0.0
		for k := range steps {
			z := -6 + 12*(float64(k)+0.5)/steps
			pdf := math.Exp(-z*z/2) / math.Sqrt(2*math.Pi)
			v := max(float64(d.Min), min(float64(d.Max), d.Median*math.Exp(d.Sigma*z)))
			sum += v * pdf * 12 / steps
		}
		return sum
	default:
		return float64(d.Value)
	}
}
