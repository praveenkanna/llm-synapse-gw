package mockllm

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Request headers that steer a single request. Real vLLM ignores them.
const (
	HeaderFault        = "X-Mock-Fault"
	HeaderOutputTokens = "X-Mock-Output-Tokens"
	HeaderTTFTMs       = "X-Mock-TTFT-Ms"
)

const (
	// maxRefuseDuration bounds POST /mock/refuse so a typo can't take the mock away for good.
	maxRefuseDuration = 5 * time.Minute
	// idRandomBytes is the length of the random part of response IDs.
	idRandomBytes = 16
)

// Stats is a point-in-time view of the mock's counters, for tests.
type Stats struct {
	Requests  uint64            `json:"requests"`  // generation requests received
	Completed uint64            `json:"completed"` // finished normally
	Cancelled uint64            `json:"cancelled"` // client went away first
	Rejected  uint64            `json:"rejected"`  // answered with a 4xx
	Faults    map[string]uint64 `json:"faults"`    // injected faults by kind
	Running   int64             `json:"running"`
	Waiting   int64             `json:"waiting"`
	// PriorityRequests counts requests whose body carried a valid non-zero
	// "priority", accepted or not.
	PriorityRequests uint64 `json:"priority_requests"`
	// PromptTokens are the stand-in prompt tokens of accepted generation
	// requests, which are also the prefix-cache queries.
	PromptTokens uint64 `json:"prompt_tokens"`
	// PrefixCacheHits are the prompt tokens served from the prefix cache.
	PrefixCacheHits uint64 `json:"prefix_cache_hits"`
}

// Mock is the mockllm HTTP handler. Create it with New.
type Mock struct {
	cfg      Config
	created  int64 // unix seconds, reported as "created"
	models   map[string]bool
	metrics  *metrics
	recorder *recorder
	slots    *slots                  // nil when MaxNumSeqs == 0
	caches   map[string]*prefixCache // by model; nil entries when PrefixCacheBlocks == 0
	mux      *http.ServeMux

	rngMu sync.Mutex
	rng   *rand.Rand

	refuseMu  sync.Mutex
	refuseFor func(time.Duration) error // set by Server; nil without one
}

// New validates cfg and builds a Mock.
func New(cfg Config) (*Mock, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid mockllm config: %w", err)
	}
	seed := cfg.Seed
	if seed == 0 {
		seed = rand.Uint64()
	}
	m := &Mock{
		cfg:      cfg,
		created:  time.Now().Unix(),
		models:   make(map[string]bool, len(cfg.Models)),
		metrics:  newMetrics(cfg.Models, cfg.MaxNumSeqs, cfg.VLLMCompat),
		recorder: newRecorder(cfg.CaptureEntries, cfg.CaptureMaxBytes),
		caches:   make(map[string]*prefixCache, len(cfg.Models)),
		rng:      rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
	}
	for _, name := range cfg.Models {
		m.models[name] = true
		if cfg.PrefixCacheBlocks > 0 && m.caches[name] == nil {
			m.caches[name] = newPrefixCache(cfg.PrefixCacheBlocks)
		}
	}
	if cfg.MaxNumSeqs > 0 {
		m.slots = newSlots(cfg.MaxNumSeqs)
	}
	m.mux = m.routes()
	return m, nil
}

func (m *Mock) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", m.handleGenerate(endpointChat))
	mux.HandleFunc("POST /v1/completions", m.handleGenerate(endpointCompletion))
	mux.HandleFunc("GET /v1/models", m.handleModels)
	mux.HandleFunc("POST /tokenize", m.handleTokenize)
	mux.HandleFunc("GET /health", handleHealth)
	mux.HandleFunc("GET /metrics", m.handleMetrics)
	if m.cfg.ControlEnabled {
		mux.HandleFunc("GET /mock/stats", m.handleStats)
		mux.HandleFunc("GET /mock/requests", m.handleRequests)
		mux.HandleFunc("DELETE /mock/requests", m.handleResetRequests)
		mux.HandleFunc("POST /mock/refuse", m.handleRefuse)
		mux.HandleFunc("POST /reset_prefix_cache", m.handleResetPrefixCache)
	}
	return mux
}

// ServeHTTP implements http.Handler.
func (m *Mock) ServeHTTP(w http.ResponseWriter, r *http.Request) { m.mux.ServeHTTP(w, r) }

// Stats returns the current counters.
func (m *Mock) Stats() Stats {
	s := Stats{
		Completed:        m.metrics.completed.Load(),
		Cancelled:        m.metrics.cancelled.Load(),
		Rejected:         m.metrics.rejected.Load(),
		Faults:           make(map[string]uint64, len(faultKinds)),
		PriorityRequests: m.metrics.priority.Load(),
	}
	for _, c := range m.metrics.requests {
		s.Requests += c.Load()
	}
	for k, c := range m.metrics.faults {
		s.Faults[k] = c.Load()
	}
	for _, mm := range m.metrics.models {
		s.Running += mm.running.Load()
		s.Waiting += mm.waiting.Load()
		s.PromptTokens += mm.promptTokens.Load()
		s.PrefixCacheHits += mm.prefixHits.Load()
	}
	return s
}

// ResetPrefixCache drops every cached prefix block of every model.
func (m *Mock) ResetPrefixCache() {
	for _, c := range m.caches {
		c.reset()
	}
}

// Requests returns the captured requests, oldest first.
func (m *Mock) Requests() []RecordedRequest { return m.recorder.snapshot() }

// ResetRequests clears the capture.
func (m *Mock) ResetRequests() { m.recorder.reset() }

func (m *Mock) setRefuser(f func(time.Duration) error) {
	m.refuseMu.Lock()
	m.refuseFor = f
	m.refuseMu.Unlock()
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK) // vLLM answers /health with 200 and an empty body
}

func (m *Mock) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", prometheusContentType)
	_ = m.metrics.writePrometheus(w) // a failed write means the scraper left
}

type modelCard struct {
	ID          string  `json:"id"`
	Object      string  `json:"object"`
	Created     int64   `json:"created"`
	OwnedBy     string  `json:"owned_by"`
	Root        string  `json:"root"`
	Parent      *string `json:"parent"`
	MaxModelLen int     `json:"max_model_len"`
}

func (m *Mock) handleModels(w http.ResponseWriter, _ *http.Request) {
	cards := make([]modelCard, 0, len(m.cfg.Models))
	for _, name := range m.cfg.Models {
		cards = append(cards, modelCard{
			ID: name, Object: "model", Created: m.created, OwnedBy: "vllm", Root: name, MaxModelLen: m.cfg.MaxModelLen,
		})
	}
	writeJSON(w, http.StatusOK, struct {
		Object string      `json:"object"`
		Data   []modelCard `json:"data"`
	}{Object: "list", Data: cards})
}

func (m *Mock) handleStats(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, m.Stats())
}

func (m *Mock) handleRequests(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, m.Requests())
}

func (m *Mock) handleResetRequests(w http.ResponseWriter, _ *http.Request) {
	m.ResetRequests()
	w.WriteHeader(http.StatusNoContent)
}

func (m *Mock) handleResetPrefixCache(w http.ResponseWriter, _ *http.Request) {
	m.ResetPrefixCache()
	w.WriteHeader(http.StatusOK) // vLLM answers with 200 and an empty body
}

func (m *Mock) handleRefuse(w http.ResponseWriter, r *http.Request) {
	m.refuseMu.Lock()
	refuse := m.refuseFor
	m.refuseMu.Unlock()
	if refuse == nil {
		m.writeError(w, http.StatusNotImplemented, "refusal needs a mockllm Server listener", "NotImplementedError")
		return
	}
	d, err := time.ParseDuration(r.URL.Query().Get("for"))
	if err != nil || d <= 0 || d > maxRefuseDuration {
		m.writeError(w, http.StatusBadRequest, fmt.Sprintf("query parameter for must be a duration in (0, %s]", maxRefuseDuration), "BadRequestError")
		return
	}
	if err := refuse(d); err != nil {
		m.writeError(w, http.StatusInternalServerError, err.Error(), "InternalServerError")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"refusing_for": d.String()})
}

// genRequest holds the only request fields the mock looks at.
type genRequest struct {
	Model               string `json:"model"`
	Stream              bool   `json:"stream"`
	MaxTokens           *int   `json:"max_tokens"`
	MaxCompletionTokens *int   `json:"max_completion_tokens"`
	N                   *int   `json:"n"`
	StreamOptions       *struct {
		IncludeUsage bool `json:"include_usage"`
	} `json:"stream_options"`
	Priority json.RawMessage `json:"priority"` // checked as an integer in plan
	Messages json.RawMessage `json:"messages"` // chat; decoded in promptOf
	Prompt   json.RawMessage `json:"prompt"`   // completions; decoded in promptOf
}

// plan is everything decided about a request before generation starts.
type plan struct {
	endpoint     string
	model        string
	stream       bool
	includeUsage bool
	choices      int
	priority     int // lower is admitted first under PolicyPriority
	promptTokens int
	prompt       []int  // prompt token ids for the prefix cache; nil when it is off
	tokens       int    // output tokens per choice
	finish       string // finish_reason for a normal completion
	fault        string
	ttft         time.Duration
}

// requestError is a 4xx answer decided while planning.
type requestError struct {
	status  int
	errType string
	msg     string
}

func (e *requestError) Error() string { return e.msg }

func badRequest(format string, a ...any) *requestError {
	return &requestError{status: http.StatusBadRequest, errType: "BadRequestError", msg: fmt.Sprintf(format, a...)}
}

func (m *Mock) handleGenerate(endpoint string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		m.metrics.requests[endpoint].Add(1)
		body, rerr := m.readBody(w, r)
		if rerr != nil {
			m.reject(w, rerr)
			return
		}
		m.recorder.record(r, body)
		p, rerr := m.plan(endpoint, r.Header, body)
		if rerr != nil {
			m.reject(w, rerr)
			return
		}
		mm := m.metrics.models[p.model]
		mm.promptTokens.Add(uint64(p.promptTokens))
		mm.prefixHits.Add(uint64(m.caches[p.model].query(p.prompt)))
		p.prompt = nil // not needed while generating
		m.serve(r.Context(), w, p, start)
	}
}

// readBody reads the request body up to MaxBodyBytes.
func (m *Mock) readBody(w http.ResponseWriter, r *http.Request) ([]byte, *requestError) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, m.cfg.MaxBodyBytes))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			return nil, &requestError{status: http.StatusRequestEntityTooLarge, errType: "BadRequestError", msg: "request body too large"}
		}
		return nil, badRequest("read body: %v", err)
	}
	return body, nil
}

// resolveModel applies vLLM's fallback to the served model for an empty name
// and rejects names it does not serve.
func (m *Mock) resolveModel(name string) (string, *requestError) {
	if name == "" {
		name = m.cfg.Models[0]
	}
	if !m.models[name] {
		return "", &requestError{status: http.StatusNotFound, errType: "NotFoundError", msg: fmt.Sprintf("The model `%s` does not exist.", name)}
	}
	return name, nil
}

// promptOf counts a generation request's prompt with the stand-in tokenizer
// (chat requests always get the generation prompt) and returns the token ids
// too when the prefix cache needs them.
func (m *Mock) promptOf(endpoint string, req genRequest) (int, []int, *requestError) {
	withTokens := m.cfg.PrefixCacheBlocks > 0
	if endpoint == endpointChat {
		msgs, rerr := decodeMessages(req.Messages)
		if rerr != nil {
			return 0, nil, rerr
		}
		text := RenderChat(msgs, true)
		if !withTokens {
			return CountText(text), nil, nil
		}
		tokens := Tokenize(text)
		return len(tokens), tokens, nil
	}
	tokens, err := promptTokens(req.Prompt)
	if err != nil {
		return 0, nil, badRequest("%v", err)
	}
	if !withTokens {
		return len(tokens), nil, nil
	}
	return len(tokens), tokens, nil
}

// decodeMessages decodes a "messages" array; absent or null is no messages.
func decodeMessages(raw json.RawMessage) ([]Message, *requestError) {
	if !present(raw) {
		return nil, nil
	}
	var msgs []Message
	if err := json.Unmarshal(raw, &msgs); err != nil {
		return nil, badRequest("invalid messages: %v", err)
	}
	return msgs, nil
}

func (m *Mock) reject(w http.ResponseWriter, e *requestError) {
	m.metrics.rejected.Add(1)
	m.writeError(w, e.status, e.msg, e.errType)
}

// plan parses the body and headers and draws the random choices.
func (m *Mock) plan(endpoint string, h http.Header, body []byte) (plan, *requestError) {
	var req genRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return plan{}, badRequest("invalid JSON body: %v", err)
	}
	model, rerr := m.resolveModel(req.Model)
	if rerr != nil {
		return plan{}, rerr
	}
	p := plan{endpoint: endpoint, model: model, stream: req.Stream, choices: 1}
	if rerr := m.checkPriority(&p, req.Priority); rerr != nil {
		return plan{}, rerr
	}
	if req.N != nil {
		if *req.N < 1 || *req.N > MaxChoices {
			return plan{}, badRequest("n must be in [1, %d]", MaxChoices)
		}
		p.choices = *req.N
	}
	p.includeUsage = req.Stream && req.StreamOptions != nil && req.StreamOptions.IncludeUsage
	if p.promptTokens, p.prompt, rerr = m.promptOf(endpoint, req); rerr != nil {
		return plan{}, rerr
	}
	if p.promptTokens >= m.cfg.MaxModelLen {
		return plan{}, badRequest("This model's maximum context length is %d tokens. However, your request has %d input tokens.", m.cfg.MaxModelLen, p.promptTokens)
	}
	limit, rerr := tokenLimit(req, m.cfg.MaxModelLen-p.promptTokens)
	if rerr != nil {
		return plan{}, rerr
	}
	if rerr := m.drawAndOverride(&p, h, limit); rerr != nil {
		return plan{}, rerr
	}
	return p, nil
}

// laxInt reads a JSON value as vLLM's request model reads an int field
// (pydantic's lax mode): an integer, a number with no fraction (0.0, 10.0,
// and 1e-400, which underflows to zero), a bool, or a string holding an
// integer. null and everything else are refused. vLLM also reads strings
// such as "1.0" and "1_000", which no test sends.
func laxInt(raw []byte) (int, bool) {
	switch raw[0] {
	case 't':
		return 1, true
	case 'f':
		return 0, true
	case '"':
		var s string
		if json.Unmarshal(raw, &s) != nil {
			return 0, false
		}
		n, err := strconv.Atoi(strings.TrimSpace(s))
		return n, err == nil
	case '{', '[', 'n':
		return 0, false
	}
	if n, err := strconv.Atoi(string(raw)); err == nil {
		return n, true
	}
	f, err := strconv.ParseFloat(string(raw), 64)
	if err != nil || f != math.Trunc(f) || math.Abs(f) >= 1<<63 {
		return 0, false
	}
	return int(f), true
}

// checkPriority parses the "priority" field (absent means 0) into p. Like
// vLLM, a non-zero priority is an error unless priority scheduling is on.
func (m *Mock) checkPriority(p *plan, raw json.RawMessage) *requestError {
	if len(raw) == 0 {
		return nil
	}
	v, ok := laxInt(raw)
	if !ok {
		return badRequest("priority must be an integer")
	}
	p.priority = v
	if p.priority == 0 {
		return nil
	}
	m.metrics.priority.Add(1)
	if m.cfg.SchedulingPolicy != PolicyPriority {
		return badRequest("Got priority %d but Priority scheduling is not enabled.", p.priority)
	}
	return nil
}

// tokenLimit returns the output cap from max_tokens / max_completion_tokens and the context room.
func tokenLimit(req genRequest, room int) (int, *requestError) {
	limit := room
	for _, f := range []struct {
		name string
		v    *int
	}{{"max_tokens", req.MaxTokens}, {"max_completion_tokens", req.MaxCompletionTokens}} {
		if f.v == nil {
			continue
		}
		if *f.v < 1 {
			return 0, badRequest("%s must be at least 1, got %d", f.name, *f.v)
		}
		limit = min(limit, *f.v)
	}
	return limit, nil
}

// drawAndOverride fills tokens, finish, fault and ttft from config and headers.
func (m *Mock) drawAndOverride(p *plan, h http.Header, limit int) *requestError {
	m.rngMu.Lock()
	tokens := m.cfg.OutputTokens.Sample(m.rng)
	fault := m.drawFaultLocked()
	m.rngMu.Unlock()
	p.ttft = m.cfg.TTFT

	if v := h.Get(HeaderOutputTokens); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxOutputTokens {
			return badRequest("%s must be an integer in [1, %d]", HeaderOutputTokens, maxOutputTokens)
		}
		tokens = n
	}
	if v := h.Get(HeaderTTFTMs); v != "" {
		ms, err := strconv.Atoi(v)
		if err != nil || ms < 0 {
			return badRequest("%s must be a non-negative integer", HeaderTTFTMs)
		}
		p.ttft = time.Duration(ms) * time.Millisecond
	}
	if v := h.Get(HeaderFault); v != "" {
		f := strings.ToLower(v)
		if f != faultNone && !isFaultKind(f) {
			return badRequest("%s must be one of none, %s", HeaderFault, strings.Join(faultKinds, ", "))
		}
		fault = f
	}
	p.fault = fault
	p.tokens, p.finish = tokens, finishReasonStop
	if tokens >= limit {
		p.tokens, p.finish = limit, finishReasonLength
	}
	return nil
}

// drawFaultLocked picks a fault by the configured rates. Caller holds rngMu.
func (m *Mock) drawFaultLocked() string {
	f := m.cfg.Faults
	if f.sum() == 0 {
		return faultNone
	}
	u := m.rng.Float64()
	for _, c := range []struct {
		kind string
		rate float64
	}{{fault5xx, f.Error5xx}, {faultDrop, f.Drop}, {faultHang, f.Hang}, {faultStall, f.Stall}} {
		if u < c.rate {
			return c.kind
		}
		u -= c.rate
	}
	return faultNone
}

func isFaultKind(s string) bool {
	for _, k := range faultKinds {
		if s == k {
			return true
		}
	}
	return false
}

// newID returns prefix plus random hex, like vLLM's request IDs.
func (m *Mock) newID(prefix string) string {
	var b [idRandomBytes]byte
	m.rngMu.Lock()
	for i := range b {
		b[i] = byte(m.rng.Uint32())
	}
	m.rngMu.Unlock()
	return prefix + hex.EncodeToString(b[:])
}

// acquire takes a running slot, waiting (and counted as waiting) while all
// MaxNumSeqs slots are taken; waiters are admitted in priority order (see
// slots). It returns false if ctx ends first.
func (m *Mock) acquire(ctx context.Context, mm *modelMetrics, priority int) bool {
	if m.slots == nil {
		mm.running.Add(1)
		return true
	}
	w := m.slots.take(priority)
	if w == nil {
		mm.running.Add(1)
		return true
	}
	mm.waiting.Add(1)
	defer mm.waiting.Add(-1)
	if !m.slots.wait(ctx, w) {
		return false
	}
	mm.running.Add(1)
	return true
}

func (m *Mock) release(mm *modelMetrics) {
	mm.running.Add(-1)
	if m.slots != nil {
		m.slots.release()
	}
}

// errorBody is the current vLLM (and OpenAI) error shape: {"error":{...}}.
type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Message string  `json:"message"`
	Type    string  `json:"type"`
	Param   *string `json:"param"`
	Code    int     `json:"code"`
}

// legacyErrorBody is the older vLLM error shape: a flat object tagged
// "object":"error".
type legacyErrorBody struct {
	Object  string  `json:"object"`
	Message string  `json:"message"`
	Type    string  `json:"type"`
	Param   *string `json:"param"`
	Code    int     `json:"code"`
}

// errorPayload builds the error body for a compat mode.
func errorPayload(mode VLLMCompat, status int, msg, errType string) any {
	if mode == CompatLegacy {
		return legacyErrorBody{Object: "error", Message: msg, Type: errType, Code: status}
	}
	return errorBody{Error: errorDetail{Message: msg, Type: errType, Code: status}}
}

// writeError writes an error body in the configured vLLM shape.
func (m *Mock) writeError(w http.ResponseWriter, status int, msg, errType string) {
	writeJSON(w, status, errorPayload(m.cfg.VLLMCompat, status, msg, errType))
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	_ = sendJSON(w, status, v) // a failed write means the client left
}

// sendJSON writes v as a complete JSON response and flushes it, returning an
// error if the body did not reach the connection. Content-Length is set so
// the flush does not switch the response to chunked encoding (vLLM sends a
// Content-Length on non-streaming answers).
func sendJSON(w http.ResponseWriter, status int, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		http.Error(w, "encode response: "+err.Error(), http.StatusInternalServerError)
		return fmt.Errorf("encode response: %w", err)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(b)))
	w.WriteHeader(status)
	if _, err := w.Write(b); err != nil {
		return fmt.Errorf("write response: %w", err)
	}
	if err := http.NewResponseController(w).Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return fmt.Errorf("flush response: %w", err)
	}
	return nil
}
