package ingress

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/contextbudget"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates"
)

// Budgeted context assembly (Level 3, ADR 0019). A chat request that sends
// X-Context-Budget has its oldest history turns dropped until its prompt
// fits, measured through a replica's /tokenize. It runs after the request
// leads a flight or misses one, so a flight's request is assembled once, by
// its leader, and its followers get the leader's X-Context-* headers.
const (
	HeaderContextBudget          = "X-Context-Budget"
	HeaderContextAssembly        = "X-Context-Assembly"
	HeaderContextDroppedMessages = "X-Context-Dropped-Messages"
	HeaderContextDroppedTurns    = "X-Context-Dropped-Turns"
	// contextSkipped is X-Context-Assembly on a request forwarded unchanged
	// because its assembly could not run.
	contextSkipped = "skipped"
	// contextApproximate is X-Context-Assembly on a request trimmed to a
	// suffix proven to fit before measuring stopped: within budget, but
	// perhaps not the most turns that would fit.
	contextApproximate = "approximate"
	// CodeContextBudgetExceeded is the 422 of a request whose system
	// messages and last turn alone are over its budget.
	CodeContextBudgetExceeded = "context_budget_exceeded"
)

// Fail-open reasons of stage context found by the handler; the rest come
// from contextbudget. A budget that is not a positive integer is an
// optimization that cannot run, not a bad request: raw vLLM ignores the
// header.
const (
	ReasonInvalidBudget   = "invalid_budget"
	ReasonNotChat         = "not_chat"
	ReasonAssembleTimeout = "assemble_timeout" // context.assemble_timeout ran out before any fit was proven
)

// maxBudgetHeaderBytes is the longest X-Context-Budget read: the digits of
// any int with room for zeros. A longer one is an invalid budget.
const maxBudgetHeaderBytes = 32

// contextBudget returns the request's X-Context-Budget when assembly is on
// ("" when the request did not opt in) and the budget it states: 0 when it
// is not a positive integer.
func (h *Handler) contextBudget(r *http.Request) (header string, limit int) {
	if h.tokenizer == nil {
		return "", 0
	}
	header = r.Header.Get(HeaderContextBudget)
	if len(header) > maxBudgetHeaderBytes {
		return header, 0
	}
	if n, err := strconv.Atoi(header); err == nil && n > 0 {
		limit = n
	}
	return header, limit
}

// skipContext records that the request's assembly could not run; it goes
// on unchanged.
func (h *Handler) skipContext(w http.ResponseWriter, rec *telemetry.Request, reason string) {
	rec.FailOpen(telemetry.StageContext, reason)
	h.metrics.ContextAssembly(telemetry.ContextSkipped, 0)
	w.Header().Set(HeaderContextAssembly, contextSkipped)
}

// assembleContext fits the body ix indexes to limit. tmpl is the matched
// template, if any: its static prefix is never dropped. A body with turns
// dropped is spliced through ix, which then indexes the body to send on.
// It returns that the request goes on, or was answered (422), or that its
// caller left.
func (h *Handler) assembleContext(w http.ResponseWriter, r *http.Request, rec *telemetry.Request, pool string, tmpl *templates.Version, limit int, ix *bodyindex.Index) step {
	s, _ := h.assembleContextTokens(w, r, rec, pool, tmpl, limit, ix)
	return s
}

func (h *Handler) assembleContextTokens(w http.ResponseWriter, r *http.Request, rec *telemetry.Request, pool string, tmpl *templates.Version, limit int, ix *bodyindex.Index) (step, int) {
	skip := func(reason string) (step, int) {
		h.skipContext(w, rec, reason)
		return proceed, 0
	}
	switch {
	case r.URL.Path != PathChatCompletions:
		return skip(ReasonNotChat)
	case len(ix.Body()) > h.contextMaxBody:
		return skip(telemetry.ReasonBodyTooLarge)
	}
	keep := 0
	if tmpl != nil {
		p := tmpl.Prefix()
		keep = len(p.Messages)
		if p.Role != "" { // the message the first variable is in
			keep++
		}
	}
	var se *contextbudget.SkipError
	q, err := contextbudget.Parse(ix, keep)
	if errors.As(err, &se) {
		return skip(se.Reason)
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.contextAssemble)
	defer cancel()
	var healthy []discovery.Replica
	if h.healthy != nil {
		healthy = h.healthy(pool)
	}
	res, err := q.Assemble(ctx, ix, limit, h.contextProbes, h.tokenizer.Counter(healthy, h.disc.Replicas(pool), q))
	switch {
	case errors.Is(err, contextbudget.ErrOverBudget):
		h.metrics.ContextAssembly(telemetry.ContextOverBudget, 0)
		writeError(w, http.StatusUnprocessableEntity, "the system messages and the last turn alone are over the X-Context-Budget of "+strconv.Itoa(limit)+" tokens",
			"invalid_request_error", CodeContextBudgetExceeded)
		return answered, 0
	case err != nil && r.Context().Err() != nil:
		return gone, 0
	case err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded):
		return skip(ReasonAssembleTimeout)
	case errors.As(err, &se):
		return skip(se.Reason)
	case err != nil:
		return skip(contextbudget.ReasonProbeFailed)
	}
	outcome := telemetry.ContextUnchanged
	switch {
	case res.Approximate:
		outcome = telemetry.ContextApproximate
		w.Header().Set(HeaderContextAssembly, contextApproximate)
	case res.DroppedTurns > 0:
		outcome = telemetry.ContextTrimmed
	}
	h.metrics.ContextAssembly(outcome, res.DroppedMessages)
	w.Header().Set(HeaderContextDroppedMessages, strconv.Itoa(res.DroppedMessages))
	w.Header().Set(HeaderContextDroppedTurns, strconv.Itoa(res.DroppedTurns))
	return proceed, res.Tokens // ix indexes res.Body
}
