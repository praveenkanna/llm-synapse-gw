package admin

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates/pgstore"
)

const supportStatic = "You are the support assistant. " + leakMarker + " Answer briefly.\n"

// supportSpec has a static prefix, a semi_static tenant line and a
// volatile question.
var supportSpec = `{
  "model": "large-model",
  "description": "support bot",
  "variables": [{"name": "tenant"}, {"name": "question"}],
  "messages": [
    {"role": "system", "segments": [
      {"kind": "static", "text": ` + jsonString(supportStatic) + `},
      {"kind": "semi_static", "text": "Tenant: {{tenant}}"}]},
    {"role": "user", "segments": [{"kind": "volatile", "text": "{{question}}"}]}
  ],
  "examples": [
    {"tenant": "acme", "question": "` + leakMarker + ` how do I reset my password?"},
    {"tenant": "globex", "question": "why is the invoice late?"}
  ]
}`

// supportSpecV2 changes the static text: a cache-breaking change.
var supportSpecV2 = strings.Replace(supportSpec, "Answer briefly.", "Answer in full.", 1)

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func TestNewRequiresTokensAndDependencies(t *testing.T) {
	f := newFixture(t)
	base := f.srv.o
	for name, mutate := range map[string]func(*Options){
		"no tokens":        func(o *Options) { o.Tokens = nil },
		"unresolved token": func(o *Options) { o.Tokens = []config.Caller{{Name: "ci", TokenEnv: "X"}} },
		"no name":          func(o *Options) { o.Tokens = []config.Caller{{Token: config.Secret(tokenCI)}} },
		"no sender":        func(o *Options) { o.Sender = nil },
		"no discovery":     func(o *Options) { o.Discovery = nil },
		"no pool mapping":  func(o *Options) { o.PoolFor = nil },
	} {
		o := base
		mutate(&o)
		if _, err := New(f.store, o); err == nil {
			t.Errorf("%s: New succeeded", name)
		}
	}
	if _, err := New(nil, base); err == nil {
		t.Error("nil store: New succeeded")
	}
}

func TestAuth(t *testing.T) {
	f := newFixture(t)
	for _, tc := range []struct {
		name   string
		header string
		status int
		code   string
	}{
		{"missing", "", http.StatusUnauthorized, codeMissingToken},
		{"not bearer", "Basic " + tokenCI, http.StatusUnauthorized, codeMissingToken},
		{"empty bearer", "Bearer ", http.StatusUnauthorized, codeMissingToken},
		{"wrong", "Bearer " + tokenCI + "x", http.StatusUnauthorized, codeInvalidToken},
		{"prefix of a token", "Bearer admin-token", http.StatusUnauthorized, codeInvalidToken},
		{"first token", "Bearer " + tokenCI, http.StatusOK, ""},
		{"second token, lowercase scheme", "bearer " + tokenOps, http.StatusOK, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodGet, "/admin/v1/templates", nil)
			if tc.header != "" {
				req.Header.Set("Authorization", tc.header)
			}
			rec := httptest.NewRecorder()
			f.h.ServeHTTP(rec, req)
			if rec.Code != tc.status {
				t.Fatalf("status %d, want %d; body %s", rec.Code, tc.status, rec.Body.String())
			}
			if tc.code == "" {
				return
			}
			e := errorOf(t, rec)
			if e.Code != tc.code || e.Type != typeInvalid {
				t.Errorf("error %+v, want code %s", e, tc.code)
			}
			if rec.Header().Get("WWW-Authenticate") == "" {
				t.Error("401 without WWW-Authenticate")
			}
			if strings.Contains(rec.Body.String(), tokenCI) {
				t.Error("401 body echoes the token")
			}
		})
	}
}

func TestAuthBeforeRouting(t *testing.T) {
	f := newFixture(t)
	if rec := f.do(http.MethodGet, "/admin/v1/nope", "", ""); rec.Code != http.StatusUnauthorized {
		t.Errorf("unknown route without a token: %d, want 401", rec.Code)
	}
	if rec := f.do(http.MethodPost, "/admin/v1/templates/x/versions", supportSpec, ""); rec.Code != http.StatusUnauthorized {
		t.Errorf("create without a token: %d, want 401", rec.Code)
	}
	if len(f.store.versions) != 0 || f.kicks.Load() != 0 {
		t.Error("an unauthenticated create reached the store")
	}
	rec := f.call(http.MethodGet, "/admin/v1/nope", "", http.StatusNotFound)
	if e := errorOf(t, rec); e.Code != "unknown_route" {
		t.Errorf("unknown route error %+v", e)
	}
}

func TestValidate(t *testing.T) {
	f := newFixture(t)
	rec := f.call(http.MethodPost, "/admin/v1/validate", supportSpec, http.StatusOK)
	var out validateResponse
	decode(t, rec, &out)
	if !out.Valid || out.StaticPrefixBytes != len(supportStatic) {
		t.Errorf("validate = %+v, want valid with %d static bytes", out, len(supportStatic))
	}

	for name, body := range map[string]string{
		"not JSON":        `{"model":`,
		"unknown field":   `{"model":"m","messages":[],"bogus":1}`,
		"trailing data":   `{"model":"m","messages":[]} {}`,
		"wrong JSON type": `{"model":1}`,
	} {
		rec := f.call(http.MethodPost, "/admin/v1/validate", body, http.StatusBadRequest)
		if e := errorOf(t, rec); e.Code != "invalid_json" {
			t.Errorf("%s: error %+v", name, e)
		}
	}
	if f.kicks.Load() != 0 {
		t.Errorf("validate kicked the registry %d times", f.kicks.Load())
	}
}

// AC-M3-2: a static segment after a volatile one is rejected, and the
// error names both segments.
func TestMisorderedSpecRejected(t *testing.T) {
	f := newFixture(t)
	misordered := `{
	  "model": "large-model",
	  "variables": [{"name": "question"}],
	  "messages": [
	    {"role": "system", "segments": [{"kind": "static", "text": "You are helpful."}]},
	    {"role": "user", "segments": [{"kind": "volatile", "text": "{{question}}"}]},
	    {"role": "system", "segments": [{"kind": "static", "text": "Policy: ` + leakMarker + `"}]}
	  ]
	}`
	for _, path := range []string{"/admin/v1/validate", "/admin/v1/templates/misordered/versions"} {
		rec := f.call(http.MethodPost, path, misordered, http.StatusUnprocessableEntity)
		e := errorOf(t, rec)
		if e.Code != "invalid_spec" || len(e.Errors) != 1 {
			t.Fatalf("%s: error %+v", path, e)
		}
		got := e.Errors[0]
		if got.Path != "messages[2].segments[0]" || !strings.Contains(got.Msg, "messages[1].segments[0]") {
			t.Errorf("%s: problem %+v; want it at messages[2].segments[0] naming messages[1].segments[0]", path, got)
		}
		if !strings.Contains(e.Message, "messages[2].segments[0]") || !strings.Contains(e.Message, "messages[1].segments[0]") {
			t.Errorf("%s: message %q does not name both segments", path, e.Message)
		}
		if strings.Contains(rec.Body.String(), leakMarker) {
			t.Errorf("%s: validation error quotes segment text", path)
		}
	}
	if len(f.store.versions) != 0 || f.kicks.Load() != 0 {
		t.Error("a rejected spec was stored")
	}
}

// A template that shares generations between callers must declare every
// variable shared (ADR 0019, AC-M4-3): registration refuses it, naming the
// variable, and stores nothing.
func TestRegistrationRefusesOptInWithUnsharedVariables(t *testing.T) {
	f := newFixture(t)
	optIn := strings.Replace(supportSpec, `"description": "support bot",`, `"description": "support bot", "coalesce": true,`, 1)
	for _, path := range []string{"/admin/v1/validate", "/admin/v1/templates/support/versions"} {
		rec := f.call(http.MethodPost, path, optIn, http.StatusUnprocessableEntity)
		e := errorOf(t, rec)
		if e.Code != "invalid_spec" || len(e.Errors) != 2 {
			t.Fatalf("%s: error %+v, want one problem per variable", path, e)
		}
		for i, got := range e.Errors {
			if want := "variables[" + string(rune('0'+i)) + "].scope"; got.Path != want {
				t.Errorf("%s: problem %d at %q, want %q", path, i, got.Path, want)
			}
		}
		if strings.Contains(rec.Body.String(), leakMarker) {
			t.Errorf("%s: error quotes segment text", path)
		}
	}
	if len(f.store.versions) != 0 {
		t.Error("a rejected spec was stored")
	}
	// Declaring the variables shared makes the same template acceptable.
	shared := strings.Replace(optIn, `[{"name": "tenant"}, {"name": "question"}]`,
		`[{"name": "tenant", "scope": "shared"}, {"name": "question", "scope": "shared"}]`, 1)
	f.call(http.MethodPost, "/admin/v1/validate", shared, http.StatusOK)
}

func TestValidateReportsEveryProblem(t *testing.T) {
	f := newFixture(t)
	spec := `{"model": "", "variables": [{"name": "a"}, {"name": "a"}],
	  "messages": [{"role": "robot", "segments": [{"kind": "static", "text": "{{b}}"}]}]}`
	rec := f.call(http.MethodPost, "/admin/v1/validate", spec, http.StatusUnprocessableEntity)
	e := errorOf(t, rec)
	want := templates.Validate(mustParse(t, spec))
	if len(e.Errors) != len(want) || len(want) < 4 {
		t.Fatalf("got %d problems %+v, want all %d: %+v", len(e.Errors), e.Errors, len(want), want)
	}
}

func mustParse(t *testing.T, spec string) templates.Spec {
	t.Helper()
	s, err := templates.ParseSpec([]byte(spec))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestCreateAndGet(t *testing.T) {
	f := newFixture(t)
	out := f.register("support", supportSpec)
	if out.ID != "support" || out.Version != 1 || out.Status != templates.StatusDraft {
		t.Errorf("create = %+v", out)
	}
	if out.StaticPrefixBytes != len(supportStatic) {
		t.Errorf("static_prefix_bytes = %d, want %d", out.StaticPrefixBytes, len(supportStatic))
	}
	if out.Counts.Error != "" || out.Counts.Pool != "large" || len(out.Counts.Examples) != 2 {
		t.Errorf("counts = %+v", out.Counts)
	}
	if f.kicks.Load() != 1 {
		t.Errorf("kicks after create = %d, want 1", f.kicks.Load())
	}
	if f.store.descs["support"] != "support bot" {
		t.Errorf("template description = %q, want the spec's", f.store.descs["support"])
	}
	if got := f.register("support", supportSpecV2); got.Version != 2 {
		t.Errorf("second version = %d, want 2", got.Version)
	}

	rec := f.call(http.MethodGet, "/admin/v1/templates/support/versions/1", "", http.StatusOK)
	var v struct {
		ID      string           `json:"id"`
		Version int              `json:"version"`
		Status  templates.Status `json:"status"`
		Spec    templates.Spec   `json:"spec"`
		Counts  templates.Counts `json:"counts"`
	}
	decode(t, rec, &v)
	if v.ID != "support" || v.Version != 1 || v.Status != templates.StatusDraft {
		t.Errorf("get = %+v", v)
	}
	if v.Spec.Messages[0].Segments[0].Text != supportStatic || v.Spec.Model != testModel {
		t.Errorf("get spec = %+v", v.Spec)
	}
	if v.Counts.PrefixTokens != out.Counts.PrefixTokens {
		t.Errorf("get counts = %+v, want %+v", v.Counts, out.Counts)
	}

	rec = f.call(http.MethodGet, "/admin/v1/templates", "", http.StatusOK)
	var list struct {
		Templates []pgstore.TemplateSummary `json:"templates"`
	}
	decode(t, rec, &list)
	if len(list.Templates) != 1 || len(list.Templates[0].Versions) != 2 || list.Templates[0].Description != "support bot" {
		t.Errorf("list = %+v", list)
	}
}

func TestEmptyListIsArray(t *testing.T) {
	f := newFixture(t)
	rec := f.call(http.MethodGet, "/admin/v1/templates", "", http.StatusOK)
	if got := strings.TrimSpace(rec.Body.String()); got != `{"templates":[]}` {
		t.Errorf("empty list = %s", got)
	}
}

func TestPathErrors(t *testing.T) {
	f := newFixture(t)
	f.register("support", supportSpec)
	for _, tc := range []struct {
		method, path, body string
		status             int
		code               string
	}{
		{http.MethodPost, "/admin/v1/templates/Bad_ID/versions", supportSpec, http.StatusUnprocessableEntity, "invalid_id"},
		{http.MethodGet, "/admin/v1/templates/Bad_ID/versions/1", "", http.StatusUnprocessableEntity, "invalid_id"},
		{http.MethodGet, "/admin/v1/templates/support/versions/0", "", http.StatusBadRequest, "invalid_version"},
		{http.MethodGet, "/admin/v1/templates/support/versions/-1", "", http.StatusBadRequest, "invalid_version"},
		{http.MethodGet, "/admin/v1/templates/support/versions/+1", "", http.StatusBadRequest, "invalid_version"},
		{http.MethodGet, "/admin/v1/templates/support/versions/abc", "", http.StatusBadRequest, "invalid_version"},
		{http.MethodGet, "/admin/v1/templates/support/versions/99999999999", "", http.StatusBadRequest, "invalid_version"},
		{http.MethodGet, "/admin/v1/templates/support/versions/9", "", http.StatusNotFound, "not_found"},
		{http.MethodGet, "/admin/v1/templates/other/versions/1", "", http.StatusNotFound, "not_found"},
		{http.MethodPost, "/admin/v1/templates/support/versions/9/render", `{"variables":{}}`, http.StatusNotFound, "not_found"},
		{http.MethodPut, "/admin/v1/templates", "", http.StatusNotFound, "unknown_route"},
	} {
		rec := f.call(tc.method, tc.path, tc.body, tc.status)
		if e := errorOf(t, rec); e.Code != tc.code {
			t.Errorf("%s %s: code %q, want %q", tc.method, tc.path, e.Code, tc.code)
		}
	}
}

func TestStatusTransitions(t *testing.T) {
	f := newFixture(t)
	f.register("support", supportSpec)
	kicks := f.kicks.Load()
	path := "/admin/v1/templates/support/versions/1/status"

	rec := f.call(http.MethodPost, path, `{"status":"deprecated"}`, http.StatusConflict)
	if e := errorOf(t, rec); e.Code != "invalid_transition" || e.Type != typeConflict {
		t.Errorf("draft to deprecated: %+v", e)
	}
	rec = f.call(http.MethodPost, path, `{"status":"live"}`, http.StatusBadRequest)
	if e := errorOf(t, rec); e.Code != "unknown_status" {
		t.Errorf("unknown status: %+v", e)
	}
	f.call(http.MethodPost, path, `{}`, http.StatusBadRequest)
	f.call(http.MethodPost, path, `{"status":"active","x":1}`, http.StatusBadRequest)
	if f.kicks.Load() != kicks {
		t.Error("a failed status change kicked the registry")
	}

	rec = f.call(http.MethodPost, path, `{"status":"active"}`, http.StatusOK)
	var out statusResponse
	decode(t, rec, &out)
	if out.Status != templates.StatusActive || out.Version != 1 {
		t.Errorf("status = %+v", out)
	}
	if f.kicks.Load() != kicks+1 {
		t.Errorf("kicks = %d, want %d", f.kicks.Load(), kicks+1)
	}
	f.call(http.MethodPost, path, `{"status":"deprecated"}`, http.StatusOK)
	f.call(http.MethodPost, path, `{"status":"retired"}`, http.StatusOK)
	f.call(http.MethodPost, path, `{"status":"active"}`, http.StatusConflict)
	f.call(http.MethodPost, "/admin/v1/templates/support/versions/7/status", `{"status":"active"}`, http.StatusNotFound)
}

func TestDeleteDraftOnly(t *testing.T) {
	f := newFixture(t)
	f.register("support", supportSpec)
	f.register("support", supportSpecV2)
	f.call(http.MethodPost, "/admin/v1/templates/support/versions/1/status", `{"status":"active"}`, http.StatusOK)
	kicks := f.kicks.Load()

	rec := f.call(http.MethodDelete, "/admin/v1/templates/support/versions/1", "", http.StatusConflict)
	if e := errorOf(t, rec); e.Code != "not_draft" {
		t.Errorf("delete active: %+v", e)
	}
	if f.kicks.Load() != kicks {
		t.Error("a refused delete kicked the registry")
	}
	rec = f.call(http.MethodDelete, "/admin/v1/templates/support/versions/2", "", http.StatusNoContent)
	if rec.Body.Len() != 0 {
		t.Errorf("204 with a body: %q", rec.Body.String())
	}
	if f.kicks.Load() != kicks+1 {
		t.Error("delete did not kick the registry")
	}
	f.call(http.MethodGet, "/admin/v1/templates/support/versions/2", "", http.StatusNotFound)
	f.call(http.MethodDelete, "/admin/v1/templates/support/versions/2", "", http.StatusNotFound)
}

func TestDiff(t *testing.T) {
	f := newFixture(t)
	f.register("support", supportSpec)
	f.register("support", supportSpecV2)

	rec := f.call(http.MethodGet, "/admin/v1/templates/support/versions/2/diff?against=1", "", http.StatusOK)
	var out diffResponse
	decode(t, rec, &out)
	want := templates.Diff(mustParse(t, supportSpec), mustParse(t, supportSpecV2))
	if out.Version != 2 || out.Against != 1 || !out.PrefixChanged || len(out.Changes) != len(want.Changes) {
		t.Fatalf("diff = %+v, want %+v", out, want)
	}
	c := out.Changes[0]
	if c.Path != "messages[0].segments[0]" || c.Field != templates.FieldText || !strings.Contains(c.To, "Answer in full.") {
		t.Errorf("change = %+v", c)
	}

	rec = f.call(http.MethodGet, "/admin/v1/templates/support/versions/1/diff?against=1", "", http.StatusOK)
	decode(t, rec, &out)
	if out.PrefixChanged || len(out.Changes) != 0 {
		t.Errorf("self diff = %+v", out)
	}
	for _, q := range []string{"", "?against=", "?against=0", "?against=x"} {
		rec := f.call(http.MethodGet, "/admin/v1/templates/support/versions/2/diff"+q, "", http.StatusBadRequest)
		if e := errorOf(t, rec); e.Code != "invalid_against" {
			t.Errorf("against %q: %+v", q, e)
		}
	}
	f.call(http.MethodGet, "/admin/v1/templates/support/versions/2/diff?against=5", "", http.StatusNotFound)
}

func TestRender(t *testing.T) {
	f := newFixture(t)
	f.register("support", supportSpec)
	path := "/admin/v1/templates/support/versions/1/render"
	vars := map[string]string{"tenant": "acme", "question": "hello?"}
	body, _ := json.Marshal(map[string]any{"variables": vars})

	rec := f.call(http.MethodPost, path, string(body), http.StatusOK)
	var out struct {
		Messages []templates.RenderedMessage `json:"messages"`
	}
	decode(t, rec, &out)
	want, _ := templates.Render(mustParse(t, supportSpec), vars)
	if fmt.Sprint(out.Messages) != fmt.Sprint(want) {
		t.Errorf("render = %+v, want %+v", out.Messages, want)
	}

	rec = f.call(http.MethodPost, path, `{"variables":{"tenant":"acme","extra":"x"}}`, http.StatusBadRequest)
	e := errorOf(t, rec)
	if e.Code != "render_error" || !strings.Contains(e.Message, `"question"`) || !strings.Contains(e.Message, `"extra"`) {
		t.Errorf("render error = %+v", e)
	}
	f.call(http.MethodPost, path, `{"variables":{"tenant":1}}`, http.StatusBadRequest)
	f.call(http.MethodPost, path, `{"vars":{}}`, http.StatusBadRequest)
	if f.kicks.Load() != 1 {
		t.Errorf("render kicked the registry (kicks %d)", f.kicks.Load())
	}
}

func TestStoreUnavailable(t *testing.T) {
	f := newFixture(t)
	f.register("support", supportSpec)
	f.store.setErr(errors.New("pgstore: get: connection refused"))
	kicks := f.kicks.Load()
	for _, tc := range []struct{ method, path, body string }{
		{http.MethodGet, "/admin/v1/templates", ""},
		{http.MethodPost, "/admin/v1/templates/support/versions", supportSpec},
		{http.MethodGet, "/admin/v1/templates/support/versions/1", ""},
		{http.MethodPost, "/admin/v1/templates/support/versions/1/status", `{"status":"active"}`},
		{http.MethodDelete, "/admin/v1/templates/support/versions/1", ""},
		{http.MethodPost, "/admin/v1/templates/support/versions/1/recount", ""},
	} {
		rec := f.call(tc.method, tc.path, tc.body, http.StatusServiceUnavailable)
		if e := errorOf(t, rec); e.Code != "store_unavailable" || strings.Contains(e.Message, "connection refused") {
			t.Errorf("%s %s: %+v", tc.method, tc.path, e)
		}
	}
	if f.kicks.Load() != kicks {
		t.Error("a failed write kicked the registry")
	}
}

func TestBodyCap(t *testing.T) {
	f := newFixture(t)
	huge := `{"model":"` + strings.Repeat("m", MaxBodyBytes) + `"}`
	rec := f.call(http.MethodPost, "/admin/v1/validate", huge, http.StatusRequestEntityTooLarge)
	if e := errorOf(t, rec); e.Code != "request_too_large" {
		t.Errorf("over the body cap: %+v", e)
	}
	overSpec := `{"model":"` + strings.Repeat("m", templates.MaxSpecBytes) + `"}`
	if len(overSpec) > MaxBodyBytes {
		t.Fatal("test spec is over the body cap")
	}
	f.call(http.MethodPost, "/admin/v1/templates/x/versions", overSpec, http.StatusRequestEntityTooLarge)
	f.call(http.MethodPost, "/admin/v1/templates/support/versions/1/render", `{"variables":{"a":"`+strings.Repeat("v", MaxBodyBytes)+`"}}`,
		http.StatusRequestEntityTooLarge)
	if len(f.store.versions) != 0 {
		t.Error("an oversized spec was stored")
	}
}

// Logs carry method, route, status, token name, template and version, and
// never spec text, variables, rendered text, upstream bodies or tokens.
func TestLogsNeverCarryText(t *testing.T) {
	f := newFixture(t)
	f.register("support", supportSpec)
	vars := `{"variables":{"tenant":"` + leakMarker + `","question":"` + leakMarker + `"}}`
	f.call(http.MethodPost, "/admin/v1/templates/support/versions/1/render", vars, http.StatusOK)
	f.call(http.MethodPost, "/admin/v1/templates/support/versions/1/tokenize", vars, http.StatusOK)
	f.call(http.MethodGet, "/admin/v1/templates/support/versions/1", "", http.StatusOK)
	f.call(http.MethodGet, "/admin/v1/templates/support/versions/1/diff?against=1", "", http.StatusOK)
	f.call(http.MethodPost, "/admin/v1/validate", strings.Replace(supportSpec, `"static"`, `"bogus"`, 1), http.StatusUnprocessableEntity)
	f.sender.setStatus("r1", http.StatusInternalServerError) // its body echoes the marker
	f.call(http.MethodPost, "/admin/v1/templates/support/versions/1/tokenize", vars, http.StatusBadGateway)
	f.do(http.MethodGet, "/admin/v1/templates", "", "wrong-"+tokenCI)

	logs := f.logs.String()
	for _, secret := range []string{leakMarker, tokenCI, tokenOps, "Answer briefly", "Tenant:"} {
		if strings.Contains(logs, secret) {
			t.Errorf("logs contain %q:\n%s", secret, logs)
		}
	}
	lines := strings.Split(strings.TrimSpace(logs), "\n")
	if len(lines) != 8 {
		t.Fatalf("%d log lines, want one per request (8):\n%s", len(lines), logs)
	}
	var first map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"msg": "admin request", "method": "POST", "route": "POST /admin/v1/templates/{id}/versions",
		"status": float64(201), "admin": "ci", "template": "support", "version": float64(1)}
	for k, v := range want {
		if first[k] != v {
			t.Errorf("create log %s = %v, want %v (line %s)", k, first[k], v, lines[0])
		}
	}
	var last map[string]any
	_ = json.Unmarshal([]byte(lines[len(lines)-1]), &last)
	if last["status"] != float64(401) || last["admin"] != nil {
		t.Errorf("401 log line = %v", last)
	}
}

func TestKickOnlyAfterWrites(t *testing.T) {
	f := newFixture(t)
	f.register("support", supportSpec) // 1
	reads := []struct{ method, path, body string }{
		{http.MethodGet, "/admin/v1/templates", ""},
		{http.MethodPost, "/admin/v1/validate", supportSpec},
		{http.MethodGet, "/admin/v1/templates/support/versions/1", ""},
		{http.MethodGet, "/admin/v1/templates/support/versions/1/diff?against=1", ""},
		{http.MethodPost, "/admin/v1/templates/support/versions/1/render", `{"variables":{"tenant":"a","question":"b"}}`},
		{http.MethodPost, "/admin/v1/templates/support/versions/1/tokenize", `{"variables":{"tenant":"a","question":"b"}}`},
	}
	for _, r := range reads {
		f.call(r.method, r.path, r.body, http.StatusOK)
	}
	if got := f.kicks.Load(); got != 1 {
		t.Fatalf("kicks after reads = %d, want 1 (the create)", got)
	}
	f.call(http.MethodPost, "/admin/v1/templates/support/versions/1/recount", "", http.StatusOK)          // 2
	f.call(http.MethodPost, "/admin/v1/templates/support/versions/1/status", `{"status":"retired"}`, 200) // 3
	f.register("support", supportSpec)                                                                    // 4
	f.call(http.MethodDelete, "/admin/v1/templates/support/versions/2", "", http.StatusNoContent)         // 5
	if got := f.kicks.Load(); got != 5 {
		t.Errorf("kicks = %d, want 5", got)
	}
}
