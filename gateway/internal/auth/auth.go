// Package auth authenticates calling services with per-service bearer tokens
// and makes the caller's identity available to later request stages.
//
// Tokens come from config (auth.callers, already resolved from token_env and
// validated by internal/config). A presented token is compared against every
// configured token in constant time: both sides are SHA-256 hashed to a fixed
// length and every caller is checked without an early exit, so neither the
// token length nor which caller matched leaks through timing. Only a header
// past MaxAuthorizationBytes, which no configured token fits, skips that: it
// is never hashed, so its cost does not grow with it, and all its sender can
// learn is that it is too long.
//
// The caller's token is never forwarded upstream: internal/upstream drops the
// caller's Authorization header and injects the gateway's own upstream API key.
//
// Auth can be disabled per environment (auth.enabled: false) for local dev.
// Requests then pass through as the Anonymous caller.
package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// AnonymousName is the caller name attached when auth is disabled.
const AnonymousName = "anonymous"

// bearerScheme is the RFC 6750 authorization scheme (matched case-insensitively).
const bearerScheme = "bearer"

// MaxAuthorizationBytes is the longest Authorization value read: the scheme,
// one space and the longest token config accepts. Nothing longer can be a
// valid token, so it is refused without being hashed.
const MaxAuthorizationBytes = len(bearerScheme) + 1 + config.MaxTokenLength

// Error codes in the OpenAI-style 401 body.
const (
	CodeMissingToken = "missing_api_key"
	CodeInvalidToken = "invalid_api_key"
)

// Caller is the authenticated identity of a request.
type Caller struct {
	// Name is the configured caller name (a low-cardinality label), or
	// AnonymousName when auth is disabled.
	Name string
	// Authenticated is true when the caller presented a valid token.
	Authenticated bool
}

// Anonymous is the identity attached to every request when auth is disabled.
var Anonymous = Caller{Name: AnonymousName}

type ctxKey struct{}

// WithCaller returns a copy of ctx carrying c.
func WithCaller(ctx context.Context, c Caller) context.Context {
	return context.WithValue(ctx, ctxKey{}, c)
}

// CallerFrom returns the caller attached by the middleware, if any.
func CallerFrom(ctx context.Context) (Caller, bool) {
	if ctx == nil {
		return Caller{}, false
	}
	if name, authd, ok := telemetry.FromContext(ctx).CallerIdentity(); ok {
		return Caller{Name: name, Authenticated: authd}, true
	}
	c, ok := ctx.Value(ctxKey{}).(Caller)
	return c, ok
}

// Stats is a point-in-time view of the authenticator's counters.
type Stats struct {
	Authenticated uint64 // requests with a valid token
	Anonymous     uint64 // requests passed through with auth disabled
	MissingToken  uint64 // rejected: no bearer token
	InvalidToken  uint64 // rejected: token matched no caller
}

type entry struct {
	name string
	hash [sha256.Size]byte
}

// Authenticator checks bearer tokens. It is immutable after New and safe for
// concurrent use.
type Authenticator struct {
	enabled bool
	callers []entry

	authenticated, anonymous, missing, invalid atomic.Uint64
}

// New builds an Authenticator from validated configuration. When auth is
// enabled every caller must have a non-empty resolved token.
func New(cfg config.Auth) (*Authenticator, error) {
	a := &Authenticator{enabled: cfg.Enabled}
	if !cfg.Enabled {
		return a, nil
	}
	if len(cfg.Callers) == 0 {
		return nil, errors.New("auth: enabled with no callers")
	}
	callers := make([]entry, 0, len(cfg.Callers))
	for i, c := range cfg.Callers {
		if c.Name == "" || !c.Token.IsSet() {
			return nil, fmt.Errorf("auth: callers[%d] has no name or unresolved token", i)
		}
		callers = append(callers, entry{name: c.Name, hash: sha256.Sum256([]byte(c.Token.Reveal()))})
	}
	a.callers = callers
	return a, nil
}

// Enabled reports whether tokens are checked.
func (a *Authenticator) Enabled() bool { return a.enabled }

// Stats returns the current counters.
func (a *Authenticator) Stats() Stats {
	return Stats{
		Authenticated: a.authenticated.Load(),
		Anonymous:     a.anonymous.Load(),
		MissingToken:  a.missing.Load(),
		InvalidToken:  a.invalid.Load(),
	}
}

// Authenticate resolves the caller for an Authorization header value.
// It returns the matched caller, or an error code (CodeMissingToken or
// CodeInvalidToken). With auth disabled it always returns Anonymous.
func (a *Authenticator) Authenticate(authorization string) (Caller, string) {
	if !a.enabled {
		return Anonymous, ""
	}
	if len(authorization) > MaxAuthorizationBytes {
		// Too long to be a token, so it is never matched. How it opens picks
		// the code, the one the whole value gets but for space past the bound.
		if _, ok := bearerToken(authorization[:MaxAuthorizationBytes]); ok {
			return Caller{}, CodeInvalidToken
		}
		return Caller{}, CodeMissingToken
	}
	token, ok := bearerToken(authorization)
	if !ok {
		return Caller{}, CodeMissingToken
	}
	if name, ok := a.match(token); ok {
		return Caller{Name: name, Authenticated: true}, ""
	}
	return Caller{}, CodeInvalidToken
}

// match compares token against every caller in constant time. It never exits
// early, so timing does not reveal which (if any) caller matched.
func (a *Authenticator) match(token string) (string, bool) {
	sum := sha256.Sum256([]byte(token))
	found := -1
	for i := range a.callers {
		eq := subtle.ConstantTimeCompare(sum[:], a.callers[i].hash[:])
		// Tokens are unique (config validation), so at most one matches.
		found = subtle.ConstantTimeSelect(eq, i, found)
	}
	if found < 0 {
		return "", false
	}
	return a.callers[found].name, true
}

// bearerToken extracts the token from "Bearer <token>". The scheme is
// case-insensitive (RFC 6750 / RFC 9110); an empty token counts as missing.
func bearerToken(h string) (string, bool) {
	scheme, token, ok := strings.Cut(strings.TrimSpace(h), " ")
	if !ok || !strings.EqualFold(scheme, bearerScheme) {
		return "", false
	}
	token = strings.TrimSpace(token)
	return token, token != ""
}

// Middleware rejects requests without a valid bearer token with 401 and an
// OpenAI-style error body, and attaches the Caller to the request context for
// later stages. With auth disabled it attaches Anonymous and passes through.
//
// It also annotates the request's telemetry record (installed by the
// telemetry middleware, which wraps this one): the rejection reason on a 401
// (gateway_auth_rejections_total{reason}), otherwise the caller name for the
// log line. Only the configured caller name is recorded, never the token.
func (a *Authenticator) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		caller, code := a.Authenticate(r.Header.Get("Authorization"))
		tel := telemetry.FromContext(r.Context())
		switch code {
		case "":
		case CodeMissingToken:
			a.missing.Add(1)
			tel.AuthRejected(code)
			writeUnauthorized(w, "missing bearer token in Authorization header", code)
			return
		default:
			a.invalid.Add(1)
			tel.AuthRejected(code)
			writeUnauthorized(w, "invalid bearer token", code)
			return
		}
		if caller.Authenticated {
			a.authenticated.Add(1)
		} else {
			a.anonymous.Add(1)
		}
		if tel != nil {
			tel.SetCallerIdentity(caller.Name, caller.Authenticated)
			next.ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(w, r.WithContext(WithCaller(r.Context(), caller)))
	})
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

// writeUnauthorized answers 401 with an OpenAI-style error body. The message
// never echoes the presented token.
func writeUnauthorized(w http.ResponseWriter, msg, code string) {
	b, _ := json.Marshal(errorBody{Error: errorDetail{Message: msg, Type: "invalid_request_error", Code: code}})
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Cache-Control", "no-store")
	h.Set("WWW-Authenticate", `Bearer realm="llm-synapse-gw"`)
	w.WriteHeader(http.StatusUnauthorized)
	_, _ = w.Write(append(b, '\n'))
}
