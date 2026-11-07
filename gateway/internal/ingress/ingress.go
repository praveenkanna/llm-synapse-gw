// Package ingress is the OpenAI-wire request path: it accepts
// /v1/chat/completions, /v1/completions and /v1/models, resolves the request's
// model to a pool, picks a replica and relays the exchange.
//
// The request body is forwarded byte for byte. Its top level is indexed once
// (package bodyindex) and only the members the stages need are read from
// it; nothing is re-serialized, and the few splices the gateway makes (a
// priority, a max_tokens, a trimmed history) go through that index.
// Streamed (SSE) responses relay chunk by chunk with a flush after every
// write and are never compressed or buffered (see stream.go).
// If the model cannot be read, is too long or is not mapped, the request still
// goes to the default pool unchanged (fail open) and the reason is counted.
package ingress

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/arbitrator"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/auth"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/coalesce"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/compiler"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/contextbudget"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/dedup"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/dispatch"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

// Routes served by the ingress.
const (
	PathChatCompletions = "/v1/chat/completions"
	PathCompletions     = "/v1/completions"
	PathModels          = "/v1/models"
)

// HeaderPriority is the header inspected for priority class requests.
const HeaderPriority = "X-Priority"

type priorityKey struct{}

// WithPriority returns a context carrying the request's priority class.
func WithPriority(ctx context.Context, priority string) context.Context {
	return context.WithValue(ctx, priorityKey{}, priority)
}

// PriorityFrom returns the priority class attached to the request context, if any.
func PriorityFrom(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if p, ok := telemetry.FromContext(ctx).PriorityClass(); ok {
		return p
	}
	if p, ok := ctx.Value(priorityKey{}).(string); ok {
		return p
	}
	return ""
}

const (
	// DefaultMaxRequestBodyBytes bounds a request body held in memory. It
	// covers long-context prompts (a 128k-token prompt is ~0.5 MiB of text)
	// with ample room for tools and multi-part content.
	DefaultMaxRequestBodyBytes = 32 << 20
	// streamRelayBufferBytes sizes the buffer that relays an SSE stream. A
	// stream holds it for its whole life (minutes, at thousands of streams per
	// gateway), so it stays small: one read returns at most one chunk of the
	// replica's response, typically one event of a few hundred bytes, and the
	// upstream transport reads its connection 4 KiB at a time anyway. A larger
	// event simply takes more than one read; every read is relayed at once.
	streamRelayBufferBytes = 4 << 10
	// bodyRelayBufferBytes sizes the buffer that relays any other response.
	// Those arrive whole once the replica has finished, so the buffer is held
	// only while the body is copied, in a few large reads.
	bodyRelayBufferBytes = 32 << 10
)

// Fail-open reasons, for gateway_failopen_total{stage,reason} (stages in telemetry).
const (
	ReasonUnknownModel = "unknown_model"
	ReasonModelTooLong = "model_too_long" // over config.MaxModelNameBytes: not decoded in full
)

// maxModelValueBytes is the longest raw "model" value decoded: the quotes
// and config.MaxModelNameBytes bytes, each written as a \uXXXX escape, the
// longest spelling JSON has of a byte. Past it the name cannot fit, so a
// mapped name reaches its pool however the caller spelled it.
const maxModelValueBytes = 6*config.MaxModelNameBytes + 2

// Options tunes a Handler. Zero values take defaults.
type Options struct {
	// MaxRequestBodyBytes bounds request bodies; larger ones get 413.
	MaxRequestBodyBytes int64
	// Logger receives upstream failure lines (never prompt or response text).
	Logger *slog.Logger
	// Admission, if set, admits generation requests per pool before they are
	// routed (M2): slots, priority queues, shedding.
	Admission *arbitrator.Arbitrator
	// PriorityVerified reports whether a priority_scheduling pool currently
	// accepts a vLLM priority (*priorityprobe.Prober). If nil, none is sent.
	PriorityVerified func(pool string) bool
	// Metrics counts upstream priority changes; may be nil.
	Metrics *telemetry.Metrics
	// Compiler matches requests naming a template against the registry
	// snapshot (M3). If nil, such requests fail open as
	// registry_unavailable.
	Compiler *compiler.Compiler
	// Coalesce shares one upstream generation between identical,
	// deterministic requests of templates that opted in (M4, ADR 0019). If
	// nil, nothing is shared.
	Coalesce *coalesce.Group
	// Cache serves and stores responses of cacheable templates (M4, ADR
	// 0019). If nil, nothing is cached.
	Cache dedup.Cache
	// HealthyReplicas returns a pool's replicas that passive health has not
	// ejected (*health.Tracker.Healthy); budgeted context measures on them
	// first. If nil, every replica is tried in discovery order.
	HealthyReplicas func(pool string) []discovery.Replica
}

// Handler serves the OpenAI-wire routes. It is safe for concurrent use.
type Handler struct {
	models      map[string]string
	poolFor     func(model string) (pool string, mapped bool) // optional test hook
	defaultPool string
	disc        discovery.Discovery
	dispatch    *dispatch.Dispatcher
	maxBody     int64
	logger      *slog.Logger

	admission        *arbitrator.Arbitrator
	priorityPools    map[string]bool // pools with priority_scheduling
	priorityVerified func(pool string) bool
	metrics          *telemetry.Metrics
	compiler         *compiler.Compiler
	dedup            *dedup.Policy     // coalescing and the response cache
	weights          map[string]string // pool -> weights_version, part of a key
	// tokenizer measures budgeted context; nil when context.enabled is off.
	tokenizer       *contextbudget.Tokenizer
	healthy         func(pool string) []discovery.Replica
	contextProbes   int           // context.max_probes
	contextMaxBody  int           // context.max_body_bytes
	contextAssemble time.Duration // context.assemble_timeout

	priority atomic.Pointer[config.PriorityConfig]
}

// New builds a Handler from a validated configuration. disp sends requests
// to the pools' replicas: routing, the one connect retry, passive health
// reports and stuck-engine watching. client serves the budgeted-context
// /tokenize calls; production builds disp with the same client.
func New(cfg *config.Config, disc discovery.Discovery, client *upstream.Client, disp *dispatch.Dispatcher, opts Options) *Handler {
	if disp == nil {
		panic("ingress: New needs a dispatcher")
	}
	if opts.MaxRequestBodyBytes <= 0 {
		opts.MaxRequestBodyBytes = DefaultMaxRequestBodyBytes
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	h := &Handler{
		models:      cfg.Models,
		defaultPool: cfg.DefaultPool,
		disc:        disc,
		dispatch:    disp,
		maxBody:     opts.MaxRequestBodyBytes,
		logger:      opts.Logger,

		admission:        opts.Admission,
		priorityPools:    cfg.PriorityPools(),
		priorityVerified: opts.PriorityVerified,
		metrics:          opts.Metrics,
		compiler:         opts.Compiler,
		weights:          cfg.WeightsVersions(),
		contextProbes:    cfg.Context.MaxProbes,
		contextMaxBody:   cfg.Context.MaxBodyBytes,
		contextAssemble:  cfg.Context.AssembleTimeout,
		healthy:          opts.HealthyReplicas,
	}
	h.dedup = dedup.New(dedup.Config{
		Group:        opts.Coalesce,
		Cache:        opts.Cache,
		KeyBodyBytes: cfg.Coalescing.MaxKeyBodyBytes,
		MaxTTL:       cfg.Cache.MaxTTL,
		OnStore:      opts.Metrics.CacheStore, // nil-safe
	})
	if cfg.Context.Enabled {
		h.tokenizer = contextbudget.NewTokenizer(client, cfg.Context.TokenizeTimeout)
	}
	h.priority.Store(&cfg.Priority)
	return h
}

// UpdatePriority validates p and makes it the active priority
// configuration. An invalid p is rejected and the previous configuration
// stays active. p is copied, so later changes to its slices or map by the
// caller do not reach requests in flight.
func (h *Handler) UpdatePriority(p config.PriorityConfig) error {
	if err := p.Validate(); err != nil {
		return fmt.Errorf("ingress: priority config rejected, keeping the previous one: %w", err)
	}
	p.Classes = slices.Clone(p.Classes)
	p.Callers = maps.Clone(p.Callers)
	p.UpstreamValues = maps.Clone(p.UpstreamValues)
	p.Probe = h.Priority().Probe // read at startup: what runs stays what is reported
	h.priority.Store(&p)
	return nil
}

// withPriority resolves the request's priority class from X-Priority and
// the caller attached by the auth middleware, records it on the request's
// telemetry and returns r carrying it.
func (h *Handler) withPriority(r *http.Request) *http.Request {
	ctx := r.Context()
	caller, _ := auth.CallerFrom(ctx)
	class := config.PriorityUser
	if p := h.priority.Load(); p != nil {
		class = p.ResolveClass(r.Header.Get(HeaderPriority), caller.Name, caller.Authenticated)
	}
	if rec := telemetry.FromContext(ctx); rec != nil {
		rec.SetPriorityClass(class)
		return r
	}
	return r.WithContext(WithPriority(ctx, class))
}

// Priority returns the active priority configuration.
func (h *Handler) Priority() config.PriorityConfig {
	if p := h.priority.Load(); p != nil {
		return *p
	}
	return config.PriorityConfig{}
}

// Register adds the ingress routes to mux.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST "+PathChatCompletions, h.handleGenerate)
	mux.HandleFunc("POST "+PathCompletions, h.handleGenerate)
	mux.HandleFunc("GET "+PathModels, h.handleModels)
}

// handleGenerate runs a generation request's stages in order; what each
// stage's answer records is the progression's (progress.go).
func (h *Handler) handleGenerate(w http.ResponseWriter, r *http.Request) {
	defer h.recoverPanic()
	rec := telemetry.FromContext(r.Context())
	p := &progress{rec: rec}
	body, s := h.readRequest(w, r)
	if p.ended(s) {
		return
	}
	// The body's one index (AD-15): read here once, passed down by pointer
	// and never kept, so it stays on this stack. A splice of a body it
	// holds leaves it indexing the new body; a body past its ceiling, which
	// it holds no members of, is spliced as bytes (admission.go).
	var ix bodyindex.Index
	info, ok := readBodyWith(&ix, body, false)
	r = h.withPriority(r)
	pool := h.resolvePool(rec, info, ok)
	p.resolved(pool)
	if p.ended(h.refuseDuplicatePriority(w, pool, info, ok)) {
		return
	}
	tmpl := h.compileIndex(rec, r, &ix)
	budget, limit := h.contextBudget(r)
	if budget != "" && limit == 0 {
		h.skipContext(w, rec, ReasonInvalidBudget)
	}
	lead, s := h.deduplicate(w, r, rec, pool, tmpl, &ix, budget, limit > 0)
	if p.ended(s) {
		return
	}
	if lead != nil {
		p.lead = lead
		w, r = lead.begin(w, r)
		defer lead.end()
	}
	promptTokens := 0
	if limit > 0 {
		var cs step
		cs, promptTokens = h.assembleContextTokens(w, r, rec, pool, tmpl, limit, &ix)
		if p.ended(cs) {
			return
		}
	}
	admitStart := rec.Now()
	slot, s := h.admit(w, r, pool)
	if p.ended(s) {
		return
	}
	defer slot.Release()
	body = ix.Body() // the caller's bytes, with any history cut
	if rec != nil {
		rec.SetRequestBody(body) // affinity reads the caller's bytes without r.WithContext copy
	} else {
		r = r.WithContext(router.WithRequestBody(r.Context(), body))
	}
	out, capped := h.buildUpstreamBody(rec, pool, PriorityFrom(r.Context()), tmpl, &ix, info, ok, promptTokens)
	p.sent(h.forward(w, r, pool, info.stream, out, templateMatch{
		version:    tmpl,
		callerCap:  !ok || info.hasCap,
		gatewayCap: capped,
		shedSlot:   slot,
		admitStart: admitStart,
	}))
}

const maxPreallocatedBodyBytes = 128 << 10

// readRequest reads the request body, up to server.max_request_body_bytes.
// A larger body is answered 413; a known oversized Content-Length is rejected
// in O(1) before reading or allocating.
func (h *Handler) readRequest(w http.ResponseWriter, r *http.Request) ([]byte, step) {
	if r.ContentLength > h.maxBody {
		return h.rejectBodyTooLarge(w)
	}
	if r.Body == nil || r.Body == http.NoBody {
		return nil, proceed
	}
	body, tooBig, err := readBoundedBody(r.Body, r.ContentLength, h.maxBody)
	if err != nil {
		return nil, gone // the caller stopped sending
	}
	if tooBig {
		return h.rejectBodyTooLarge(w)
	}
	return body, proceed
}

func (h *Handler) rejectBodyTooLarge(w http.ResponseWriter) ([]byte, step) {
	// net/http would close the connection itself after an oversized body,
	// but a wrapping ResponseWriter (telemetry) hides that hint from
	// MaxBytesReader; ask for the close explicitly so the unread rest of
	// the body is never parsed as a next request.
	w.Header().Set("Connection", "close")
	writeError(w, http.StatusRequestEntityTooLarge, "request body exceeds the gateway limit", "invalid_request_error", "request_too_large")
	return nil, answered
}

func readBoundedBody(rc io.Reader, contentLength, maxBytes int64) ([]byte, bool, error) {
	var buf []byte
	switch {
	case contentLength == 0:
		var one [1]byte
		n, err := rc.Read(one[:])
		if n == 0 {
			if err == io.EOF {
				return nil, false, nil
			}
			return nil, false, err
		}
		if maxBytes < 1 {
			return nil, true, nil
		}
		buf = make([]byte, 1, int(min(int64(512), maxBytes+1)))
		buf[0] = one[0]
	case contentLength > 0:
		want := min(contentLength, maxPreallocatedBodyBytes)
		initCap := int(min(want+1, maxBytes+1))
		buf = make([]byte, 0, initCap)
	default:
		initCap := int(min(int64(2048), maxBytes+1))
		buf = make([]byte, 0, initCap)
	}
	for {
		if len(buf) == cap(buf) {
			if int64(len(buf)) > maxBytes {
				return nil, true, nil
			}
			nextCap := min(int64(cap(buf))*2, maxBytes+1)
			if nextCap <= int64(cap(buf)) {
				nextCap = int64(cap(buf)) + 1
			}
			grown := make([]byte, len(buf), int(nextCap))
			copy(grown, buf)
			buf = grown
		}
		maxRead := cap(buf)
		if limit := int(min(int64(cap(buf)), maxBytes+1)); maxRead > limit {
			maxRead = limit
		}
		n, err := rc.Read(buf[len(buf):maxRead])
		buf = buf[:len(buf)+n]
		if int64(len(buf)) > maxBytes {
			return nil, true, nil
		}
		if err != nil {
			if err == io.EOF {
				return buf, false, nil
			}
			return nil, false, err
		}
	}
}

// compileIndex matches a request that names a template against the registry
// snapshot (ADR 0006) using its top-level index, and labels its telemetry.
func (h *Handler) compileIndex(rec *telemetry.Request, r *http.Request, ix *bodyindex.Index) *templates.Version {
	v, reason, named := h.compiler.MatchIndex(r.Header, ix)
	if !named {
		return nil
	}
	if v == nil {
		rec.FailOpen(telemetry.StageCompile, reason)
		return nil
	}
	rec.SetTemplate(v.ID, v.Version, v.PrefixTokens())
	return v
}

// buildUpstreamBody fuses priority injection/stripping/replacement and
// template max_tokens injection into at most a single output slice allocation.
func (h *Handler) buildUpstreamBody(rec *telemetry.Request, pool, class string, tmpl *templates.Version, ix *bodyindex.Index, info bodyInfo, parsed bool, promptTokens int) ([]byte, bool) {
	body := ix.Body()
	var prioBuf, maxTokBuf [32]byte
	prioEntry, prioRemove := h.planPriority(rec, pool, class, ix, info, parsed, prioBuf[:0])
	var maxTokEntry []byte
	capped := false
	if tmpl != nil && parsed && !info.hasCap {
		var n int
		var nearModelLen bool
		if promptTokens > 0 {
			n, nearModelLen = h.compiler.CapForTokens(tmpl, pool, promptTokens)
		} else {
			upstreamLen := bodyindex.SpliceLen(body, ix.Open(), len(prioEntry), prioRemove)
			n, nearModelLen = h.compiler.Cap(tmpl, pool, upstreamLen)
		}
		if n > 0 {
			rec.TemplateMaxTokens(n)
			maxTokEntry = strconv.AppendInt(append(maxTokBuf[:0], `"max_tokens":`...), int64(n), 10)
			capped = true
		} else if nearModelLen {
			rec.FailOpen(telemetry.StageMaxTokens, telemetry.ReasonNearModelLen)
		}
	}
	if len(maxTokEntry) == 0 && len(prioEntry) == 0 && prioRemove.Kind == bodyindex.Invalid {
		return body, false
	}
	return bodyindex.Splice(body, ix.Open(), maxTokEntry, prioEntry, prioRemove), capped
}

func (h *Handler) handleModels(w http.ResponseWriter, r *http.Request) {
	defer h.recoverPanic()
	p := &progress{rec: telemetry.FromContext(r.Context())}
	r = h.withPriority(r)
	p.sent(h.forward(w, r, h.defaultPool, false, nil, templateMatch{}))
}

// resolvePool maps the body's model to a pool, failing open to the default.
// parsed is false when the body's model could not be read.
func (h *Handler) resolvePool(rec *telemetry.Request, info bodyInfo, parsed bool) string {
	if !parsed {
		rec.FailOpen(telemetry.StagePoolResolve, telemetry.ReasonUnparseableBody)
		return h.defaultPool
	}
	if info.modelTooLong {
		rec.FailOpen(telemetry.StagePoolResolve, ReasonModelTooLong)
		return h.defaultPool
	}
	var pool string
	var mapped bool
	if h.poolFor != nil {
		m := info.model
		if info.rawModelOK && m == "" && len(info.modelBytes) > 0 {
			m = string(info.modelBytes)
		}
		pool, mapped = h.poolFor(m)
	} else {
		if info.rawModelOK {
			pool, mapped = h.models[string(info.modelBytes)]
		} else {
			pool, mapped = h.models[info.model]
		}
		if !mapped {
			pool = h.defaultPool
		}
	}
	if !mapped {
		rec.FailOpen(telemetry.StagePoolResolve, ReasonUnknownModel)
	}
	return pool
}

// bodyInfo is what the gateway reads from a request body. It holds no
// offsets: where a member is belongs to the body's index, which every
// splice keeps fresh.
type bodyInfo struct {
	model      string // top-level "model"; empty when absent, too long, or unallocated
	modelBytes []byte // unescaped ASCII "model" slice into body when rawModelOK is true
	rawModelOK bool   // modelBytes holds the exact decoded model name
	stream     bool   // top-level "stream" is JSON true
	// modelTooLong: "model" is a string past config.MaxModelNameBytes.
	modelTooLong bool
	// priorities is how many top-level "priority" members (vLLM
	// scheduling) the body has, and priority the last one's value, which
	// is the one vLLM reads.
	priorities int
	priority   []byte
	// hasCap: the body has a top-level "max_tokens" or
	// "max_completion_tokens", whatever its value: the caller chose its
	// cap, and the gateway never sets or changes one (ADR 0007).
	hasCap bool
}

// readBody indexes body into ix and returns its top-level "model",
// "stream", "priority" and caps. The body itself is never re-serialized.
// ok is false when the body is not a JSON object or "model" is not a
// string. A "stream" that is not a boolean counts as false: the replica
// rejects it. A "model" past config.MaxModelNameBytes is not read: the rest
// of the body is read as usual and its model is unmapped.
//
// Keys match case-sensitively (an escaped spelling by its decoded name)
// and a repeated key keeps its last value, as in vLLM's parser;
// encoding/json's struct decoding would also accept "MODEL". A body past
// bodyindex.MaxMembers is valid JSON the replica serves (ADR 0018: no
// well-formed body is refused), so it is read by walking it, in constant
// memory: it is routed, and its priority handled, like any other.
func readBody(ix *bodyindex.Index, body []byte) (info bodyInfo, ok bool) {
	return readBodyWith(ix, body, true)
}

func readBodyWith(ix *bodyindex.Index, body []byte, needModelString bool) (info bodyInfo, ok bool) {
	var top topLevel
	switch err := ix.Read(body); {
	case err == nil:
		for i := range ix.Len() {
			top.note(ix, ix.Member(i))
		}
	case errors.Is(err, bodyindex.ErrTooManyMembers):
		ix.Walk(func(m bodyindex.Member) { top.note(ix, m) })
	default:
		return bodyInfo{}, false
	}
	info.priorities, info.priority, info.hasCap = top.priorities, top.priority, top.caps > 0
	info.stream = string(top.stream) == "true" // absent or non-boolean: false
	info.modelBytes, info.rawModelOK, info.model, info.modelTooLong, ok = readModelWith(top.model, needModelString)
	if !ok {
		return bodyInfo{stream: info.stream}, false
	}
	return info, true
}

// readModelWith reads the raw value of "model", a valid JSON value or nil when
// absent. It decodes at most maxModelValueBytes, so what a caller writes
// there costs no more than the body's one validation: a string past the
// limit is left undecoded and any other value is refused by its first byte.
// ok is false when the value is not a string; null, like no value, is a
// valid empty model, which is simply unmapped.
func readModelWith(raw []byte, needString bool) (rawSlice []byte, rawOK bool, model string, tooLong, ok bool) {
	switch {
	case len(raw) == 0, raw[0] == 'n':
		return nil, true, "", false, true
	case raw[0] != '"':
		return nil, false, "", false, false
	case len(raw) > maxModelValueBytes:
		return nil, false, "", true, true
	}
	if len(raw) >= 2 && raw[len(raw)-1] == '"' {
		inner := raw[1 : len(raw)-1]
		if isUnescapedASCII(inner) {
			if len(inner) > config.MaxModelNameBytes {
				return nil, false, "", true, true
			}
			if needString {
				return inner, true, string(inner), false, true
			}
			return inner, true, "", false, true
		}
	}
	if err := json.Unmarshal(raw, &model); err != nil {
		return nil, false, "", false, false
	}
	if len(model) > config.MaxModelNameBytes {
		return nil, false, "", true, true
	}
	return nil, false, model, false, true
}

func isUnescapedASCII(b []byte) bool {
	for _, c := range b {
		if c == '\\' || c >= 0x80 || c < 0x20 {
			return false
		}
	}
	return true
}

// topLevel is what readBody collects from the body's top-level members:
// the raw values of the last "model", "stream" and "priority", how many
// "priority" members there are, and how many "max_tokens" or
// "max_completion_tokens".
type topLevel struct {
	model, stream, priority []byte
	priorities, caps        int
}

func (t *topLevel) note(ix *bodyindex.Index, m bodyindex.Member) {
	switch {
	case ix.Is(m, "model"):
		t.model = ix.Value(m)
	case ix.Is(m, "stream"):
		t.stream = ix.Value(m)
	case ix.Is(m, "priority"):
		t.priority, t.priorities = ix.Value(m), t.priorities+1
	case ix.Is(m, "max_tokens"), ix.Is(m, "max_completion_tokens"):
		t.caps++
	}
}

// forward sends the request to a replica of pool with the given body
// (package dispatch: routing, the one connect retry, passive health,
// stuck-engine watching) and relays the response. stream marks a streaming
// call (the body asked for one). A request that gets no response is
// answered here: 503 when the pool has no replica, 504 or 502 when the
// replica failed, nothing when the caller already left (dispatch counts
// that one). It returns gone when the caller left during the relay, and
// what was delivered, for a flight the request leads.
func (h *Handler) forward(w http.ResponseWriter, r *http.Request, pool string, stream bool, body []byte, tm templateMatch) (step, dedup.Delivery) {
	ex, err := h.dispatch.Send(r, pool, upstream.Request{
		Method:   r.Method,
		Path:     r.URL.Path,
		RawQuery: r.URL.RawQuery,
		Header:   r.Header,
		Body:     body,
		Stream:   stream,
	})
	if err != nil {
		h.sendFailed(w, pool, err)
		return answered, dedup.Delivery{End: dedup.Answered}
	}
	defer ex.Close() // on every exit, panics included: ends the attempt, releases the replica
	return h.relayResponse(w, r, ex, tm)
}

// sendFailed answers a request that got no response from its pool. The
// dispatcher has already counted, logged and traced every failure it
// documents; anything else is logged here and answered 502.
func (h *Handler) sendFailed(w http.ResponseWriter, pool string, err error) {
	switch {
	case errors.Is(err, dispatch.ErrNoReplicas):
		writeError(w, http.StatusServiceUnavailable, "no replicas available for pool "+pool, "service_unavailable", "no_replicas")
	case errors.Is(err, dispatch.ErrCallerGone):
		// nobody is listening
	default:
		kind := upstream.KindOther // an error Send does not document: still never a silent 200
		var failed *dispatch.Error
		if errors.As(err, &failed) {
			kind = upstream.KindOf(failed.Err)
		} else {
			h.logger.Error("dispatch returned an undocumented error", "pool", pool, "err", err)
		}
		if kind == upstream.KindTimeout {
			writeError(w, http.StatusGatewayTimeout, "upstream replica timed out", upstreamErrorType, upstreamCodePrefix+string(kind))
			return
		}
		writeError(w, http.StatusBadGateway, "upstream replica unavailable", upstreamErrorType, upstreamCodePrefix+string(kind))
	}
}

// relayResponse sends the replica's response to the caller. If the replica
// fails mid-body, a stream ends with one SSE error event and any other
// response is aborted, so the caller never takes a truncated body for a
// complete one. tm is the matched template version, if any: its response's
// usage is read as it is relayed. It returns gone when the caller left,
// and what was delivered.
func (h *Handler) relayResponse(w http.ResponseWriter, r *http.Request, ex *dispatch.Exchange, tm templateMatch) (step, dedup.Delivery) {
	rec := telemetry.FromContext(r.Context())
	resp := ex.Response
	upstream.CopyResponseHeaders(w.Header(), resp.Header)
	stream := isEventStream(resp.Header)
	if stream {
		markUnbuffered(w.Header())
	}
	w.WriteHeader(resp.StatusCode)
	if stream {
		// Send headers now, as the replica did: the caller sees the stream
		// open before the first event, exactly as it would talking direct.
		flushWriter(w)
	}
	var tail sseTail
	var usageVal usageScan
	var usage *usageScan
	ok2xx := resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices
	if tm.version != nil && ok2xx {
		usage = &usageVal
	}
	var shedSlot *arbitrator.Slot
	if stream && ok2xx {
		shedSlot = tm.shedSlot
	}
	err := relayWithShed(w, resp.Body, relayBuffersFor(stream), rec, shedSlot, tm.admitStart, &tail, usage)
	if err == nil {
		h.recordUsage(rec, tm, usage)
		return answered, dedup.Delivery{End: dedup.Whole, Runaway: tm.gatewayCap && usage != nil && usage.finishedLength}
	}
	if isClientGone(r, err) {
		return gone, dedup.Delivery{} // counted as client_closed, not the upstream status; the deferred Close cancels the upstream request
	}
	h.recordUpstreamError(r, ex.Replica, err, "upstream relay failed")
	rec.ResponseFailed() // counted as aborted or stream_error, not the upstream status
	if !stream {
		abortResponse()
	}
	if werr := writeStreamError(w, &tail, upstream.KindOf(err)); werr != nil {
		return gone, dedup.Delivery{End: dedup.Failed}
	}
	return answered, dedup.Delivery{End: dedup.Failed}
}

// templateMatch is a request's matched template version, if any; whether
// the caller set its own max_tokens or max_completion_tokens (or the body
// could not be read, so that is unknown); and whether the gateway set one.
type templateMatch struct {
	version    *templates.Version
	callerCap  bool
	gatewayCap bool
	shedSlot   *arbitrator.Slot
	admitStart time.Time
}

// recordUsage records a matched template response's usage on its
// telemetry and, when the caller set no cap of its own, in the compiler's
// completion lengths: a caller's small cap cuts its output short and would
// teach the policy a length no uncapped caller shares (ADR 0007).
func (h *Handler) recordUsage(rec *telemetry.Request, tm templateMatch, usage *usageScan) {
	if usage == nil {
		return
	}
	prompt, completion, ok := usage.usage()
	if !ok {
		if usage.finishedLength {
			rec.TemplateLengthEnding()
		}
		return
	}
	rec.TemplateUsage(prompt, completion, usage.finishedLength)
	if !tm.callerCap {
		h.compiler.Observe(tm.version, completion)
	}
}

// recordUpstreamError counts, traces and logs an upstream failure.
func (h *Handler) recordUpstreamError(r *http.Request, replica discovery.Replica, err error, msg string) {
	kind := upstream.KindOf(err)
	telemetry.FromContext(r.Context()).UpstreamError(replica.Name, string(kind))
	h.logger.WarnContext(r.Context(), msg, "pool", replica.Pool, "replica", replica.Name,
		"kind", string(kind), "err", err)
}

// recoverPanic keeps a bug on the request path from crashing the process.
// The connection is aborted; nothing from the request is logged.
func (h *Handler) recoverPanic() {
	v := recover()
	if v == nil {
		return
	}
	if v != http.ErrAbortHandler { // sentinel compared by identity, as net/http does
		h.logger.Error("panic in ingress handler", "panic", v)
	}
	panic(http.ErrAbortHandler)
}

// Relay buffer pools, one per buffer size (see streamRelayBufferBytes).
var (
	streamRelayBuffers = newBufferPool(streamRelayBufferBytes)
	bodyRelayBuffers   = newBufferPool(bodyRelayBufferBytes)
)

// relayBuffersFor returns the buffer pool for relaying a response body: small
// buffers for an SSE stream, which holds one for its whole life, large ones
// for any other body.
func relayBuffersFor(stream bool) *sync.Pool {
	if stream {
		return streamRelayBuffers
	}
	return bodyRelayBuffers
}

// newBufferPool returns a pool of *[]byte of the given size.
func newBufferPool(size int) *sync.Pool {
	return &sync.Pool{New: func() any {
		b := make([]byte, size)
		return &b
	}}
}

// relay copies src to w as bytes arrive, flushing after every write so
// nothing waits in a gateway buffer. The read buffer comes from buffers and
// goes back when the body ends. rec (may be nil) is told when the first body
// byte has been relayed: that moment is the gateway's TTFT; tail (may be nil)
// sees every byte written. A failed write to w is returned as a
// *clientWriteError; any other error comes from reading src.
func relay(w http.ResponseWriter, src io.Reader, buffers *sync.Pool, rec *telemetry.Request, tail *sseTail, usage *usageScan) error {
	return relayWithShed(w, src, buffers, rec, nil, time.Time{}, tail, usage)
}

func relayWithShed(w http.ResponseWriter, src io.Reader, buffers *sync.Pool, rec *telemetry.Request, shedSlot *arbitrator.Slot, admitStart time.Time, tail *sseTail, usage *usageScan) error {
	bp := buffers.Get().(*[]byte)
	defer buffers.Put(bp)
	buf := *bp
	fl := resolveFlusher(w)
	var rc *http.ResponseController
	if fl == nil {
		rc = http.NewResponseController(w)
	}
	first := true
	for {
		n, rerr := src.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return &clientWriteError{err: werr}
			}
			if tail != nil {
				tail.observe(buf[:n])
			}
			if usage != nil {
				usage.observe(buf[:n])
			}
			if fl != nil {
				fl.Flush()
			} else {
				_ = rc.Flush() // not all writers can flush; the write already succeeded
			}
			if first {
				first = false
				rec.FirstByte()
				if shedSlot != nil {
					shedSlot.ObserveTTFT(rec.Now().Sub(admitStart))
				}
			}
		}
		if rerr == io.EOF {
			return nil
		}
		if rerr != nil {
			return rerr
		}
	}
}

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Message string  `json:"message"`
	Type    string  `json:"type"`
	Param   *string `json:"param"`
	Code    string  `json:"code"`
}

// writeError answers with an OpenAI-style error body.
func writeError(w http.ResponseWriter, status int, msg, typ, code string) {
	b, _ := json.Marshal(errorBody{Error: errorDetail{Message: msg, Type: typ, Code: code}})
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(append(b, '\n'))
}
