package auth

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
)

const (
	chatToken  = "chat-token-0123456789"
	batchToken = "batch-token-0123456789"
)

func enabledConfig() config.Auth {
	return config.Auth{
		Enabled: true,
		Callers: []config.Caller{
			{Name: "chat-service", Token: chatToken},
			{Name: "batch-service", Token: batchToken},
		},
	}
}

func mustNew(t *testing.T, cfg config.Auth) *Authenticator {
	t.Helper()
	a, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return a
}

// echoCaller is the downstream stage: it reports the caller it sees.
func echoCaller(t *testing.T) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, ok := CallerFrom(r.Context())
		if !ok {
			t.Error("caller not attached to context")
		}
		w.Header().Set("X-Caller", c.Name)
		if c.Authenticated {
			w.Header().Set("X-Authenticated", "true")
		}
		w.WriteHeader(http.StatusOK)
	})
}

func TestMiddleware(t *testing.T) {
	tests := []struct {
		name       string
		cfg        config.Auth
		header     []string // Authorization values; nil means absent
		wantStatus int
		wantCaller string
		wantAuth   bool
		wantCode   string
	}{
		{name: "valid token first caller", cfg: enabledConfig(), header: []string{"Bearer " + chatToken}, wantStatus: 200, wantCaller: "chat-service", wantAuth: true},
		{name: "valid token second caller", cfg: enabledConfig(), header: []string{"Bearer " + batchToken}, wantStatus: 200, wantCaller: "batch-service", wantAuth: true},
		{name: "scheme is case-insensitive", cfg: enabledConfig(), header: []string{"bearer " + chatToken}, wantStatus: 200, wantCaller: "chat-service", wantAuth: true},
		{name: "extra spaces tolerated", cfg: enabledConfig(), header: []string{"  Bearer   " + chatToken + "  "}, wantStatus: 200, wantCaller: "chat-service", wantAuth: true},
		{name: "missing header", cfg: enabledConfig(), wantStatus: 401, wantCode: CodeMissingToken},
		{name: "empty header", cfg: enabledConfig(), header: []string{""}, wantStatus: 401, wantCode: CodeMissingToken},
		{name: "bearer without token", cfg: enabledConfig(), header: []string{"Bearer "}, wantStatus: 401, wantCode: CodeMissingToken},
		{name: "wrong scheme", cfg: enabledConfig(), header: []string{"Basic " + chatToken}, wantStatus: 401, wantCode: CodeMissingToken},
		{name: "raw token without scheme", cfg: enabledConfig(), header: []string{chatToken}, wantStatus: 401, wantCode: CodeMissingToken},
		{name: "invalid token", cfg: enabledConfig(), header: []string{"Bearer not-a-real-token-xyz"}, wantStatus: 401, wantCode: CodeInvalidToken},
		{name: "prefix of valid token", cfg: enabledConfig(), header: []string{"Bearer " + chatToken[:len(chatToken)-1]}, wantStatus: 401, wantCode: CodeInvalidToken},
		{name: "valid token plus suffix", cfg: enabledConfig(), header: []string{"Bearer " + chatToken + "x"}, wantStatus: 401, wantCode: CodeInvalidToken},
		{name: "token case matters", cfg: enabledConfig(), header: []string{"Bearer " + strings.ToUpper(chatToken)}, wantStatus: 401, wantCode: CodeInvalidToken},
		{name: "disabled: no header passes as anonymous", cfg: config.Auth{Enabled: false}, wantStatus: 200, wantCaller: AnonymousName},
		{name: "disabled: any token passes as anonymous", cfg: config.Auth{Enabled: false}, header: []string{"Bearer whatever"}, wantStatus: 200, wantCaller: AnonymousName},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := mustNew(t, tc.cfg)
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{}`))
			for _, v := range tc.header {
				req.Header.Add("Authorization", v)
			}
			rec := httptest.NewRecorder()
			a.Middleware(echoCaller(t)).ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tc.wantStatus, rec.Body)
			}
			if tc.wantStatus == http.StatusOK {
				if got := rec.Header().Get("X-Caller"); got != tc.wantCaller {
					t.Fatalf("caller = %q, want %q", got, tc.wantCaller)
				}
				if got := rec.Header().Get("X-Authenticated") == "true"; got != tc.wantAuth {
					t.Fatalf("authenticated = %v, want %v", got, tc.wantAuth)
				}
				return
			}
			assertUnauthorized(t, rec, tc.wantCode)
		})
	}
}

func assertUnauthorized(t *testing.T, rec *httptest.ResponseRecorder, wantCode string) {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q", ct)
	}
	if !strings.HasPrefix(rec.Header().Get("WWW-Authenticate"), "Bearer") {
		t.Fatalf("WWW-Authenticate = %q", rec.Header().Get("WWW-Authenticate"))
	}
	if strings.Contains(rec.Body.String(), chatToken) || strings.Contains(rec.Body.String(), "not-a-real-token") {
		t.Fatalf("401 body echoes a token: %s", rec.Body)
	}
	var body struct {
		Error struct {
			Message string  `json:"message"`
			Type    string  `json:"type"`
			Param   *string `json:"param"`
			Code    string  `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("401 body not JSON: %v (%s)", err, rec.Body)
	}
	if body.Error.Code != wantCode || body.Error.Type != "invalid_request_error" || body.Error.Message == "" {
		t.Fatalf("error = %+v, want code %q", body.Error, wantCode)
	}
}

func TestMiddlewareStats(t *testing.T) {
	a := mustNew(t, enabledConfig())
	h := a.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	send := func(authz string) {
		req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
		if authz != "" {
			req.Header.Set("Authorization", authz)
		}
		h.ServeHTTP(httptest.NewRecorder(), req)
	}
	send("Bearer " + chatToken)
	send("Bearer " + batchToken)
	send("")
	send("Bearer nope-nope-nope-nope")
	send("Bearer nope-nope-nope-nope")

	want := Stats{Authenticated: 2, MissingToken: 1, InvalidToken: 2}
	if got := a.Stats(); got != want {
		t.Fatalf("stats = %+v, want %+v", got, want)
	}

	off := mustNew(t, config.Auth{})
	off.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})).
		ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	if got := off.Stats(); got != (Stats{Anonymous: 1}) {
		t.Fatalf("disabled stats = %+v", got)
	}
}

func TestMiddlewareDoesNotCallNextOnReject(t *testing.T) {
	a := mustNew(t, enabledConfig())
	called := false
	h := a.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/completions", nil))
	if called {
		t.Fatal("next handler ran for an unauthenticated request")
	}
}

func TestNew(t *testing.T) {
	tests := []struct {
		name    string
		cfg     config.Auth
		wantErr string
	}{
		{name: "disabled with no callers", cfg: config.Auth{Enabled: false}},
		{name: "disabled ignores unresolved callers", cfg: config.Auth{Callers: []config.Caller{{Name: "svc", TokenEnv: "UNSET"}}}},
		{name: "enabled", cfg: enabledConfig()},
		{name: "enabled with no callers", cfg: config.Auth{Enabled: true}, wantErr: "no callers"},
		{name: "enabled with unresolved token", cfg: config.Auth{Enabled: true, Callers: []config.Caller{{Name: "svc", TokenEnv: "T"}}}, wantErr: "callers[0]"},
		{name: "enabled with nameless caller", cfg: config.Auth{Enabled: true, Callers: []config.Caller{{Token: chatToken}}}, wantErr: "callers[0]"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a, err := New(tc.cfg)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("New: %v", err)
				}
				if a.Enabled() != tc.cfg.Enabled {
					t.Fatalf("Enabled = %v, want %v", a.Enabled(), tc.cfg.Enabled)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("New error = %v, want containing %q", err, tc.wantErr)
			}
			if strings.Contains(err.Error(), chatToken) {
				t.Fatal("error leaks a token")
			}
		})
	}
}

func TestNewFromParsedConfig(t *testing.T) {
	// Tokens resolved from token_env by internal/config authenticate.
	env := map[string]string{"GATEWAY_TOKEN_CHAT": chatToken}
	cfg, err := config.Parse([]byte(`
pools:
  - name: large
    replicas:
      - url: http://127.0.0.1:1
auth:
  callers:
    - name: chat-service
      token_env: GATEWAY_TOKEN_CHAT
`), func(k string) string { return env[k] })
	if err != nil {
		t.Fatalf("config.Parse: %v", err)
	}
	a := mustNew(t, cfg.Auth)
	if c, code := a.Authenticate("Bearer " + chatToken); code != "" || c.Name != "chat-service" || !c.Authenticated {
		t.Fatalf("Authenticate = %+v, %q", c, code)
	}
}

func TestCallerFromEmptyContext(t *testing.T) {
	if _, ok := CallerFrom(httptest.NewRequest(http.MethodGet, "/", nil).Context()); ok {
		t.Fatal("CallerFrom reported a caller on a bare context")
	}
}

func TestAuthenticateConcurrent(t *testing.T) {
	a := mustNew(t, enabledConfig())
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tok, want := chatToken, "chat-service"
			if i%2 == 1 {
				tok, want = batchToken, "batch-service"
			}
			for range 100 {
				if c, code := a.Authenticate("Bearer " + tok); code != "" || c.Name != want {
					t.Errorf("Authenticate = %+v, %q", c, code)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func BenchmarkAuthenticate(b *testing.B) {
	a, err := New(enabledConfig())
	if err != nil {
		b.Fatal(err)
	}
	h := "Bearer " + batchToken
	b.ReportAllocs()
	for b.Loop() {
		if _, code := a.Authenticate(h); code != "" {
			b.Fatal(code)
		}
	}
}
