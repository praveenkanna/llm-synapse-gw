package admin

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates/pgstore"
)

// PathPrefix is the admin API's path prefix.
const PathPrefix = "/admin/v1"

func (s *Server) routes() *http.ServeMux {
	mux := http.NewServeMux()
	const version = "/admin/v1/templates/{id}/versions/{v}"
	mux.HandleFunc("GET /admin/v1/templates", s.handleList)
	mux.HandleFunc("POST /admin/v1/validate", s.handleValidate)
	mux.HandleFunc("POST /admin/v1/templates/{id}/versions", s.handleCreate)
	mux.HandleFunc("GET "+version, s.handleGet)
	mux.HandleFunc("DELETE "+version, s.handleDelete)
	mux.HandleFunc("POST "+version+"/status", s.handleStatus)
	mux.HandleFunc("GET "+version+"/diff", s.handleDiff)
	mux.HandleFunc("POST "+version+"/render", s.handleRender)
	mux.HandleFunc("POST "+version+"/tokenize", s.handleTokenize)
	mux.HandleFunc("POST "+version+"/recount", s.handleRecount)
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusNotFound, "no admin route matches this method and path", typeNotFound, "unknown_route")
	})
	return mux
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	list, err := s.store.List(r.Context())
	if err != nil {
		s.storeError(w, r, "list", err)
		return
	}
	if list == nil {
		list = []pgstore.TemplateSummary{}
	}
	writeJSON(w, http.StatusOK, struct {
		Templates []pgstore.TemplateSummary `json:"templates"`
	}{list})
}

type validateResponse struct {
	Valid             bool `json:"valid"`
	StaticPrefixBytes int  `json:"static_prefix_bytes"`
}

func (s *Server) handleValidate(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody(w, r)
	if !ok {
		return
	}
	spec, ok := parseValidSpec(w, body)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, validateResponse{Valid: true, StaticPrefixBytes: templates.StaticPrefix(spec).Bytes()})
}

type createResponse struct {
	ID                string           `json:"id"`
	Version           int              `json:"version"`
	Status            templates.Status `json:"status"`
	Counts            templates.Counts `json:"counts"`
	StaticPrefixBytes int              `json:"static_prefix_bytes"`
}

func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	body, ok := readBody(w, r)
	if !ok {
		return
	}
	spec, ok := parseValidSpec(w, body)
	if !ok {
		return
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, body); err != nil { // ParseSpec accepted it, so it is JSON
		writeError(w, http.StatusBadRequest, "spec is not valid JSON", typeInvalid, "invalid_json")
		return
	}
	counts := s.countSpec(r.Context(), spec)
	countsJSON, err := json.Marshal(counts)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not encode token counts", typeServer, "internal_error")
		return
	}
	rec, err := s.store.CreateVersion(r.Context(), id, spec.Description, compact.Bytes(), countsJSON)
	if err != nil {
		s.storeError(w, r, "create version", err)
		return
	}
	infoFrom(r.Context()).version = rec.Version
	s.o.Kick()
	w.Header().Set("Location", fmt.Sprintf("%s/templates/%s/versions/%d", PathPrefix, id, rec.Version))
	writeJSON(w, http.StatusCreated, createResponse{
		ID:                rec.ID,
		Version:           rec.Version,
		Status:            rec.Status,
		Counts:            counts,
		StaticPrefixBytes: templates.StaticPrefix(spec).Bytes(),
	})
}

type versionResponse struct {
	ID        string           `json:"id"`
	Version   int              `json:"version"`
	Status    templates.Status `json:"status"`
	Spec      json.RawMessage  `json:"spec"`
	Counts    json.RawMessage  `json:"counts"`
	CreatedAt time.Time        `json:"created_at"`
	UpdatedAt time.Time        `json:"updated_at"`
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	id, v, ok := pathVersion(w, r)
	if !ok {
		return
	}
	rec, err := s.store.Get(r.Context(), id, v)
	if err != nil {
		s.storeError(w, r, "get", err)
		return
	}
	writeJSON(w, http.StatusOK, versionResponse{
		ID:        rec.ID,
		Version:   rec.Version,
		Status:    rec.Status,
		Spec:      orNull(rec.Spec),
		Counts:    orNull(rec.Counts),
		CreatedAt: rec.CreatedAt,
		UpdatedAt: rec.UpdatedAt,
	})
}

type statusRequest struct {
	Status templates.Status `json:"status"`
}

type statusResponse struct {
	ID        string           `json:"id"`
	Version   int              `json:"version"`
	Status    templates.Status `json:"status"`
	UpdatedAt time.Time        `json:"updated_at"`
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	id, v, ok := pathVersion(w, r)
	if !ok {
		return
	}
	var req statusRequest
	if !decodeBody(w, r, &req) {
		return
	}
	if !req.Status.Valid() {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("unknown status %.32q; want draft, active, deprecated or retired", req.Status),
			typeInvalid, "unknown_status")
		return
	}
	rec, err := s.store.SetStatus(r.Context(), id, v, req.Status)
	if err != nil {
		s.storeError(w, r, "set status", err)
		return
	}
	s.o.Kick()
	writeJSON(w, http.StatusOK, statusResponse{ID: rec.ID, Version: rec.Version, Status: rec.Status, UpdatedAt: rec.UpdatedAt})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	id, v, ok := pathVersion(w, r)
	if !ok {
		return
	}
	if err := s.store.DeleteDraft(r.Context(), id, v); err != nil {
		s.storeError(w, r, "delete draft", err)
		return
	}
	s.o.Kick()
	w.WriteHeader(http.StatusNoContent)
}

type diffResponse struct {
	ID      string `json:"id"`
	Version int    `json:"version"`
	Against int    `json:"against"`
	templates.DiffResult
}

// handleDiff compares version `against` (from) with version v (to).
func (s *Server) handleDiff(w http.ResponseWriter, r *http.Request) {
	id, v, ok := pathVersion(w, r)
	if !ok {
		return
	}
	against, ok := parseVersion(r.URL.Query().Get("against"))
	if !ok {
		writeError(w, http.StatusBadRequest, "query parameter against must be a positive version number", typeInvalid, "invalid_against")
		return
	}
	to, ok := s.loadSpec(w, r, id, v)
	if !ok {
		return
	}
	from, ok := s.loadSpec(w, r, id, against)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, diffResponse{ID: id, Version: v, Against: against, DiffResult: templates.Diff(from, to)})
}

type variablesRequest struct {
	Variables map[string]string `json:"variables"`
}

func (s *Server) handleRender(w http.ResponseWriter, r *http.Request) {
	_, msgs, ok := s.render(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Messages []templates.RenderedMessage `json:"messages"`
	}{msgs})
}

type tokenizeResponse struct {
	PromptTokens int    `json:"prompt_tokens"`
	Pool         string `json:"pool"`
	Replica      string `json:"replica"`
}

func (s *Server) handleTokenize(w http.ResponseWriter, r *http.Request) {
	spec, msgs, ok := s.render(w, r)
	if !ok {
		return
	}
	pool, err := s.poolFor(spec.Model)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error(), typeUpstream, "tokenize_failed")
		return
	}
	t, err := s.tokenize(r.Context(), pool, spec.Model, msgs)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error(), typeUpstream, "tokenize_failed")
		return
	}
	writeJSON(w, http.StatusOK, tokenizeResponse{PromptTokens: t.count, Pool: pool, Replica: t.replica})
}

type recountResponse struct {
	ID      string           `json:"id"`
	Version int              `json:"version"`
	Counts  templates.Counts `json:"counts"`
}

// handleRecount recomputes a version's counts, e.g. after a replica was
// down at registration. A failed count answers 502 and keeps the stored
// counts.
func (s *Server) handleRecount(w http.ResponseWriter, r *http.Request) {
	id, v, ok := pathVersion(w, r)
	if !ok {
		return
	}
	spec, ok := s.loadSpec(w, r, id, v)
	if !ok {
		return
	}
	counts := s.countSpec(r.Context(), spec)
	if counts.Error != "" {
		writeError(w, http.StatusBadGateway, counts.Error, typeUpstream, "tokenize_failed")
		return
	}
	countsJSON, err := json.Marshal(counts)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not encode token counts", typeServer, "internal_error")
		return
	}
	if err := s.store.SetCounts(r.Context(), id, v, countsJSON); err != nil {
		s.storeError(w, r, "set counts", err)
		return
	}
	s.o.Kick()
	writeJSON(w, http.StatusOK, recountResponse{ID: id, Version: v, Counts: counts})
}

// render decodes a variables body, loads the version and renders it.
func (s *Server) render(w http.ResponseWriter, r *http.Request) (templates.Spec, []templates.RenderedMessage, bool) {
	id, v, ok := pathVersion(w, r)
	if !ok {
		return templates.Spec{}, nil, false
	}
	var req variablesRequest
	if !decodeBody(w, r, &req) {
		return templates.Spec{}, nil, false
	}
	spec, ok := s.loadSpec(w, r, id, v)
	if !ok {
		return templates.Spec{}, nil, false
	}
	msgs, err := templates.Render(spec, req.Variables)
	if err != nil { // names variables, never text
		writeError(w, http.StatusBadRequest, err.Error(), typeInvalid, "render_error")
		return templates.Spec{}, nil, false
	}
	return spec, msgs, true
}

// loadSpec reads a stored version's spec.
func (s *Server) loadSpec(w http.ResponseWriter, r *http.Request, id string, v int) (templates.Spec, bool) {
	rec, err := s.store.Get(r.Context(), id, v)
	if err != nil {
		s.storeError(w, r, "get", err)
		return templates.Spec{}, false
	}
	spec, err := templates.ParseSpec(rec.Spec)
	if err != nil {
		s.o.Logger.Error("stored template spec is unreadable", "template", id, "version", v, "error", err)
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("stored spec of version %d is unreadable", v), typeServer, "corrupt_spec")
		return templates.Spec{}, false
	}
	return spec, true
}

// storeError maps a store error to a response. Unknown errors are a store
// outage: logged (store errors carry no DSN or spec text) and answered 503.
func (s *Server) storeError(w http.ResponseWriter, r *http.Request, op string, err error) {
	msg := strings.TrimPrefix(err.Error(), "pgstore: ")
	switch {
	case errors.Is(err, pgstore.ErrNotFound):
		writeError(w, http.StatusNotFound, msg, typeNotFound, "not_found")
	case errors.Is(err, pgstore.ErrInvalidTransition):
		writeError(w, http.StatusConflict, msg, typeConflict, "invalid_transition")
	case errors.Is(err, pgstore.ErrNotDraft):
		writeError(w, http.StatusConflict, msg, typeConflict, "not_draft")
	case errors.Is(err, pgstore.ErrInvalidID):
		writeError(w, http.StatusUnprocessableEntity, msg, typeInvalid, "invalid_id")
	default:
		ri := infoFrom(r.Context())
		s.o.Logger.Warn("admin template store error", "op", op, "template", ri.id, "error", err)
		writeError(w, http.StatusServiceUnavailable, "template store unavailable", typeUnavail, "store_unavailable")
	}
}

// pathID reads and checks the {id} path value. Only a valid ID is logged.
func pathID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("id")
	if !templates.ValidID(id) {
		writeError(w, http.StatusUnprocessableEntity, "template id must match [a-z0-9][a-z0-9._-]{0,127}", typeInvalid, "invalid_id")
		return "", false
	}
	infoFrom(r.Context()).id = id
	return id, true
}

// pathVersion reads and checks the {id} and {v} path values.
func pathVersion(w http.ResponseWriter, r *http.Request) (string, int, bool) {
	id, ok := pathID(w, r)
	if !ok {
		return "", 0, false
	}
	v, ok := parseVersion(r.PathValue("v"))
	if !ok {
		writeError(w, http.StatusBadRequest, "version must be a positive integer", typeInvalid, "invalid_version")
		return "", 0, false
	}
	infoFrom(r.Context()).version = v
	return id, v, true
}

// maxVersion is Postgres int's maximum, the store's version column.
const maxVersion = 1<<31 - 1

func parseVersion(s string) (int, bool) {
	if s == "" || s[0] == '+' || len(s) > 10 {
		return 0, false
	}
	v, err := strconv.Atoi(s)
	if err != nil || v < 1 || v > maxVersion {
		return 0, false
	}
	return v, true
}

// readBody reads the request body, at most MaxBodyBytes (413 beyond).
func readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		if mbe := (*http.MaxBytesError)(nil); errors.As(err, &mbe) {
			writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("request body is over the %d-byte limit", MaxBodyBytes),
				typeInvalid, "request_too_large")
			return nil, false
		}
		writeError(w, http.StatusBadRequest, "could not read the request body", typeInvalid, "bad_body")
		return nil, false
	}
	return body, true
}

// decodeBody decodes a small JSON body strictly into v. An empty body
// leaves v zero.
func decodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	body, ok := readBody(w, r)
	if !ok {
		return false
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return true
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error(), typeInvalid, "invalid_json")
		return false
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid JSON body: trailing data after the object", typeInvalid, "invalid_json")
		return false
	}
	return true
}

// parseValidSpec parses and validates a spec body: 413 over the spec
// limit, 400 when it does not decode, 422 with every problem when invalid.
func parseValidSpec(w http.ResponseWriter, body []byte) (templates.Spec, bool) {
	if len(body) > templates.MaxSpecBytes {
		writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("spec is over the %d-byte limit", templates.MaxSpecBytes),
			typeInvalid, "request_too_large")
		return templates.Spec{}, false
	}
	spec, err := templates.ParseSpec(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), typeInvalid, "invalid_json")
		return templates.Spec{}, false
	}
	if errs := templates.Validate(spec); len(errs) > 0 {
		writeInvalidSpec(w, errs)
		return templates.Spec{}, false
	}
	return spec, true
}

// orNull is raw, or JSON null when empty.
func orNull(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage("null")
	}
	return raw
}
