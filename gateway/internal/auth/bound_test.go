package auth

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
)

func TestAuthorizationBoundConstant(t *testing.T) {
	if config.MaxTokenLength != 4096 {
		t.Errorf("MaxTokenLength = %d: a change moves which tokens start, and the results file", config.MaxTokenLength)
	}
	if MaxAuthorizationBytes != len("Bearer ")+config.MaxTokenLength {
		t.Errorf("MaxAuthorizationBytes = %d does not fit the longest token after its scheme", MaxAuthorizationBytes)
	}
}

// TestLongestTokenAuthenticates: the bound must not lock out a token config
// accepts.
func TestLongestTokenAuthenticates(t *testing.T) {
	token := strings.Repeat("t", config.MaxTokenLength)
	a := mustNew(t, config.Auth{Enabled: true, Callers: []config.Caller{{Name: "long", Token: config.Secret(token)}}})
	header := "Bearer " + token
	if len(header) != MaxAuthorizationBytes {
		t.Fatalf("header is %d bytes, want exactly the limit", len(header))
	}
	if c, code := a.Authenticate(header); code != "" || !c.Authenticated || c.Name != "long" {
		t.Fatalf("Authenticate = %+v, %q", c, code)
	}
}

func TestOverLongAuthorization(t *testing.T) {
	over := MaxAuthorizationBytes + 1
	tests := []struct {
		name   string
		header string
		want   string
	}{
		{name: "a token one past the limit", header: "Bearer " + strings.Repeat("t", over), want: CodeInvalidToken},
		{name: "a megabyte token", header: "Bearer " + strings.Repeat("t", 1<<20), want: CodeInvalidToken},
		{name: "any case of the scheme", header: "bEARER " + strings.Repeat("t", over), want: CodeInvalidToken},
		{name: "a valid token, then more", header: "Bearer " + chatToken + strings.Repeat("t", over), want: CodeInvalidToken},
		{name: "a valid token, then a second word", header: "Bearer " + chatToken + " " + strings.Repeat("t", over), want: CodeInvalidToken},
		{name: "another scheme", header: "Basic " + strings.Repeat("t", over), want: CodeMissingToken},
		{name: "no scheme", header: strings.Repeat("t", over), want: CodeMissingToken},
		{name: "a valid token past padding", header: "Bearer" + strings.Repeat(" ", over) + chatToken, want: CodeMissingToken},
	}
	a := mustNew(t, enabledConfig())
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, code := a.Authenticate(tc.header)
			if code != tc.want || c.Authenticated || c.Name != "" {
				t.Fatalf("Authenticate = %+v, %q; want no caller, %q", c, code, tc.want)
			}
		})
	}
}

// TestLongestTokenThenMore: a configured token of the longest length, then
// anything, is refused. The first MaxAuthorizationBytes hold that token
// exactly, so a check that matched what they hold would let it in.
func TestLongestTokenThenMore(t *testing.T) {
	token := strings.Repeat("t", config.MaxTokenLength)
	a := mustNew(t, config.Auth{Enabled: true, Callers: []config.Caller{{Name: "long", Token: config.Secret(token)}}})
	for name, header := range map[string]string{
		"one more byte":   "Bearer " + token + "t",
		"a second word":   "Bearer " + token + " x",
		"a megabyte more": "Bearer " + token + strings.Repeat("t", 1<<20),
	} {
		t.Run(name, func(t *testing.T) {
			if len(header) <= MaxAuthorizationBytes {
				t.Fatalf("the header is %d bytes, not past the limit", len(header))
			}
			c, code := a.Authenticate(header)
			if code != CodeInvalidToken || c.Authenticated || c.Name != "" {
				t.Fatalf("Authenticate = %+v, %q; want no caller, %q", c, code, CodeInvalidToken)
			}
		})
	}
}

func TestOverLongAuthorizationWithAuthOff(t *testing.T) {
	a := mustNew(t, config.Auth{})
	c, code := a.Authenticate("Bearer " + strings.Repeat("t", MaxAuthorizationBytes+1))
	if code != "" || c != Anonymous {
		t.Fatalf("Authenticate = %+v, %q; want Anonymous", c, code)
	}
}

// TestOverLongAuthorizationThroughTheMiddleware: the request ends as any
// failed authentication does: a 401, counted, and never reaching the next
// stage.
func TestOverLongAuthorizationThroughTheMiddleware(t *testing.T) {
	a := mustNew(t, enabledConfig())
	called := false
	h := a.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	r.Header.Set("Authorization", "Bearer "+strings.Repeat("t", 1<<20))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if called {
		t.Fatal("the next stage ran")
	}
	if w.Code != http.StatusUnauthorized || !strings.Contains(w.Body.String(), CodeInvalidToken) {
		t.Fatalf("answered %d: %s", w.Code, w.Body)
	}
	if strings.Contains(w.Body.String(), "ttttt") {
		t.Fatal("the answer echoes the token")
	}
	if s := a.Stats(); s.InvalidToken != 1 || s.Authenticated != 0 {
		t.Fatalf("stats = %+v", s)
	}
}

func authCost(a *Authenticator, header string, runs int) time.Duration {
	best := time.Hour
	for range runs {
		start := time.Now()
		a.Authenticate(header)
		if d := time.Since(start); d < best {
			best = d
		}
	}
	return best
}

// TestAuthorizationCostStopsAtTheBound: a check costs the same with a
// header just past the bound as with one 200 times as long. Hashing costs 200
// times as much, so the ratio is far from the limit whichever way noise
// leans. Non-ASCII space is what strings.TrimSpace reads rune by rune.
func TestAuthorizationCostStopsAtTheBound(t *testing.T) {
	const maxRatio = 4
	a := mustNew(t, enabledConfig())
	over := MaxAuthorizationBytes + 1
	for name, value := range map[string]func(n int) string{
		"token":        func(n int) string { return "Bearer " + strings.Repeat("t", n) },
		"non-ascii":    func(n int) string { return strings.Repeat("\u00a0", n/2) + "Bearer t" },
		"ending space": func(n int) string { return "Bearer t" + strings.Repeat("\u00a0", n/2) },
	} {
		t.Run(name, func(t *testing.T) {
			var base, huge time.Duration
			for range 5 {
				base, huge = authCost(a, value(over), 100), authCost(a, value(200*over), 100)
				if huge <= maxRatio*base {
					return
				}
			}
			t.Fatalf("a check took %v at %d bytes, %v at %d", huge, 200*over, base, over)
		})
	}
}

// BenchmarkAuthenticateHeader times a check at and past the limit
// (docs/results). Sizes are literal, so a run before the bound compares
// like for like.
func BenchmarkAuthenticateHeader(b *testing.B) {
	a, err := New(enabledConfig())
	if err != nil {
		b.Fatal(err)
	}
	for _, size := range []int{4103, 8 << 10, 1 << 20} {
		token := "Bearer " + strings.Repeat("t", size-len("Bearer "))
		space := strings.Repeat("\u00a0", size/2) + "Bearer t"
		b.Run("token_"+strconv.Itoa(size), func(b *testing.B) { benchAuth(b, a, token) })
		b.Run("space_"+strconv.Itoa(size), func(b *testing.B) { benchAuth(b, a, space) })
	}
}

func benchAuth(b *testing.B, a *Authenticator, header string) {
	b.ReportAllocs()
	for b.Loop() {
		a.Authenticate(header)
	}
}
