// Package admin is the template registry's admin API (M3-02): it
// registers, validates, inspects, diffs, renders and counts template
// versions and moves them through their lifecycle (ADR 0006). It listens on
// its own port, never behind the public route, and every route needs an
// admin bearer token.
//
// Nothing here is on the request path: the request path reads the
// registry's in-memory snapshot, and a successful write here only asks for
// an early refresh (Options.Kick).
//
// Token counts (M3-05, ADRs 0008, 0011) come from POST /tokenize on a
// replica of the model's pool, with the defaults chat completions uses.
// Registration stores each example's count and prefix_tokens, the longest
// common token prefix of two renders whose variables all differ: the part
// every caller shares. A count that fails is stored as its error, and
// registration still succeeds.
//
// Template text, variables, rendered messages and token ids are never
// logged. A request's log line carries its method, route pattern, status,
// the admin token's name and the template ID and version.
package admin

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates/pgstore"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

// MaxBodyBytes caps a request body: a spec, plus room for the JSON around
// a render's or tokenize's variables.
const MaxBodyBytes = templates.MaxSpecBytes + 16<<10

// Store is the template store the admin API writes (*pgstore.Store). It
// returns pgstore's sentinel errors.
type Store interface {
	CreateVersion(ctx context.Context, id, description string, spec, counts json.RawMessage) (templates.Record, error)
	Get(ctx context.Context, id string, version int) (templates.Record, error)
	List(ctx context.Context) ([]pgstore.TemplateSummary, error)
	SetStatus(ctx context.Context, id string, version int, to templates.Status) (templates.Record, error)
	SetCounts(ctx context.Context, id string, version int, counts json.RawMessage) error
	DeleteDraft(ctx context.Context, id string, version int) error
}

var _ Store = (*pgstore.Store)(nil)

// Sender sends one upstream request (*upstream.Client, which adds the
// upstream API key).
type Sender interface {
	Do(ctx context.Context, replica discovery.Replica, req upstream.Request) (*http.Response, error)
}

// Options configures a Server.
type Options struct {
	// Tokens are the accepted bearer tokens, resolved (config.Admin.Tokens).
	// At least one is required.
	Tokens []config.Caller
	// TokenizeTimeout bounds one /tokenize call to one replica; 0 takes
	// config.DefaultAdminTokenizeTimeout.
	TokenizeTimeout time.Duration
	// Discovery lists a pool's replicas for /tokenize. Required.
	Discovery discovery.Discovery
	// Sender sends /tokenize to a replica. Required.
	Sender Sender
	// PoolFor maps a spec's model to its pool (config.Config.PoolForModel).
	// Required.
	PoolFor func(model string) (pool string, mapped bool)
	// Kick, if set, is called after every successful write, so the
	// registry snapshot refreshes promptly (templates.Registry.Kick). It
	// must not block.
	Kick func()
	// Logger gets one line per admin request; nil discards.
	Logger *slog.Logger
}

// Server serves /admin/v1. It is safe for concurrent use.
type Server struct {
	store  Store
	o      Options
	tokens []tokenEntry
	mux    *http.ServeMux
}

type tokenEntry struct {
	name string
	hash [sha256.Size]byte
}

// New builds a Server. It fails without a store, a token, or the
// dependencies counting needs.
func New(store Store, o Options) (*Server, error) {
	switch {
	case store == nil:
		return nil, errors.New("admin: no template store")
	case o.Discovery == nil || o.Sender == nil || o.PoolFor == nil:
		return nil, errors.New("admin: Discovery, Sender and PoolFor are required")
	case len(o.Tokens) == 0:
		return nil, errors.New("admin: at least one admin token is required")
	}
	s := &Server{store: store, o: o, tokens: make([]tokenEntry, 0, len(o.Tokens))}
	for i, t := range o.Tokens {
		if t.Name == "" || !t.Token.IsSet() {
			return nil, fmt.Errorf("admin: tokens[%d] has no name or an unresolved token", i)
		}
		s.tokens = append(s.tokens, tokenEntry{name: t.Name, hash: sha256.Sum256([]byte(t.Token.Reveal()))})
	}
	if s.o.TokenizeTimeout <= 0 {
		s.o.TokenizeTimeout = config.DefaultAdminTokenizeTimeout
	}
	if s.o.Logger == nil {
		s.o.Logger = slog.New(slog.DiscardHandler)
	}
	if s.o.Kick == nil {
		s.o.Kick = func() {}
	}
	s.mux = s.routes()
	return s, nil
}

// Handler returns the admin API's handler.
func (s *Server) Handler() http.Handler { return http.HandlerFunc(s.serve) }

// reqInfo is what a request's log line reports beyond method and status;
// handlers fill it as they learn the template and version.
type reqInfo struct {
	admin   string
	id      string
	version int
}

type infoKey struct{}

func infoFrom(ctx context.Context) *reqInfo {
	if ri, ok := ctx.Value(infoKey{}).(*reqInfo); ok {
		return ri
	}
	return &reqInfo{}
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	sw := &statusWriter{ResponseWriter: w}
	ri := &reqInfo{}
	r = r.WithContext(context.WithValue(r.Context(), infoKey{}, ri))
	defer func() {
		if v := recover(); v != nil {
			if v == http.ErrAbortHandler {
				panic(v)
			}
			s.o.Logger.Error("admin handler panic", "route", r.Pattern, "panic", fmt.Sprint(v))
			if !sw.wrote {
				writeError(sw, http.StatusInternalServerError, "internal error", typeServer, "internal_error")
			}
		}
		s.logRequest(r, sw.code(), ri, time.Since(start))
	}()

	name, ok := s.authenticate(r.Header.Get("Authorization"))
	if !ok {
		writeUnauthorized(sw, name)
		return
	}
	ri.admin = name
	r.Body = http.MaxBytesReader(sw, r.Body, MaxBodyBytes)
	s.mux.ServeHTTP(sw, r) // sets r.Pattern on r itself
}

func (s *Server) logRequest(r *http.Request, status int, ri *reqInfo, d time.Duration) {
	attrs := []any{"method", r.Method, "route", r.Pattern, "status", status, "duration_ms", d.Milliseconds()}
	if ri.admin != "" {
		attrs = append(attrs, "admin", ri.admin)
	}
	if ri.id != "" {
		attrs = append(attrs, "template", ri.id)
	}
	if ri.version > 0 {
		attrs = append(attrs, "version", ri.version)
	}
	s.o.Logger.Info("admin request", attrs...)
}

// Error codes of a 401.
const (
	codeMissingToken = "missing_api_key"
	codeInvalidToken = "invalid_api_key"
)

// authenticate returns the matched token's name, or on failure the 401
// error code. Every configured token is compared, in constant time and
// without an early exit, so timing reveals neither the token length nor
// which token matched.
func (s *Server) authenticate(header string) (string, bool) {
	scheme, token, ok := strings.Cut(strings.TrimSpace(header), " ")
	token = strings.TrimSpace(token)
	if !ok || !strings.EqualFold(scheme, "bearer") || token == "" {
		return codeMissingToken, false
	}
	sum := sha256.Sum256([]byte(token))
	found := -1
	for i := range s.tokens {
		eq := subtle.ConstantTimeCompare(sum[:], s.tokens[i].hash[:])
		found = subtle.ConstantTimeSelect(eq, i, found)
	}
	if found < 0 {
		return codeInvalidToken, false
	}
	return s.tokens[found].name, true
}

// statusWriter records the status a handler sent.
type statusWriter struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wrote {
		w.status, w.wrote = code, true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if !w.wrote {
		w.status, w.wrote = http.StatusOK, true
	}
	return w.ResponseWriter.Write(b)
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *statusWriter) code() int {
	if !w.wrote {
		return http.StatusOK
	}
	return w.status
}

// Error types of the error body.
const (
	typeInvalid  = "invalid_request_error"
	typeNotFound = "not_found_error"
	typeConflict = "conflict_error"
	typeUpstream = "upstream_error"
	typeServer   = "server_error"
	typeUnavail  = "service_unavailable"
)

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Message string                     `json:"message"`
	Type    string                     `json:"type"`
	Code    string                     `json:"code"`
	Errors  templates.ValidationErrors `json:"errors,omitempty"`
}

func writeError(w http.ResponseWriter, status int, msg, typ, code string) {
	writeJSON(w, status, errorBody{Error: errorDetail{Message: msg, Type: typ, Code: code}})
}

// writeInvalidSpec answers 422 with every validation problem. Their
// messages name paths and short names, never segment text.
func writeInvalidSpec(w http.ResponseWriter, errs templates.ValidationErrors) {
	writeJSON(w, http.StatusUnprocessableEntity, errorBody{Error: errorDetail{
		Message: "invalid template spec: " + errs.Error(),
		Type:    typeInvalid,
		Code:    "invalid_spec",
		Errors:  errs,
	}})
}

func writeUnauthorized(w http.ResponseWriter, code string) {
	msg := "invalid admin bearer token"
	if code == codeMissingToken {
		msg = "missing bearer token in Authorization header"
	}
	w.Header().Set("WWW-Authenticate", `Bearer realm="llm-synapse-gw-admin"`)
	writeError(w, http.StatusUnauthorized, msg, typeInvalid, code)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		status = http.StatusInternalServerError
		b, _ = json.Marshal(errorBody{Error: errorDetail{Message: "could not encode the response", Type: typeServer, Code: "internal_error"}})
	}
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(append(b, '\n'))
}
