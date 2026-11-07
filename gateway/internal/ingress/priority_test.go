package ingress

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/auth"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

// priorityFixture serves the gateway with auth and telemetry for priority
// tests; each fixture has its own metrics so series counts start at zero.
type priorityFixture struct {
	h   *Handler
	tel *telemetry.Telemetry
	srv *httptest.Server
}

func newPriorityFixture(t *testing.T, cfgYAML string) *priorityFixture {
	t.Helper()
	cfg := parseConfig(t, cfgYAML)
	disc, err := discovery.NewStatic(cfg.Pools)
	if err != nil {
		t.Fatal(err)
	}
	client := upstream.New(cfg.Upstream)
	t.Cleanup(client.CloseIdleConnections)
	authn, err := auth.New(cfg.Auth)
	if err != nil {
		t.Fatal(err)
	}
	f := &priorityFixture{h: New(cfg, disc, client, roundRobin(cfg, disc, client, nil), Options{}), tel: telemetry.New(telemetry.Options{})}
	api := http.NewServeMux()
	f.h.Register(api)
	f.srv = httptest.NewServer(f.tel.Middleware(authn.Middleware(api)))
	t.Cleanup(f.srv.Close)
	return f
}

// post sends a chat completion with an optional bearer token and X-Priority.
func (f *priorityFixture) post(t *testing.T, token, priorityHeader string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, f.srv.URL+PathChatCompletions, bytes.NewReader([]byte(`{"model":"large-model","messages":[{"role":"user","content":"hi"}]}`)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if priorityHeader != "" {
		req.Header.Set(HeaderPriority, priorityHeader)
	}
	resp, err := f.srv.Client().Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp
}

func (f *priorityFixture) metric(t *testing.T, series string) float64 {
	t.Helper()
	rec := httptest.NewRecorder()
	f.tel.Metrics().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, telemetry.MetricsPath, nil))
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if v, ok := strings.CutPrefix(line, series+" "); ok {
			n, err := strconv.ParseFloat(v, 64)
			if err != nil {
				t.Fatal(err)
			}
			return n
		}
	}
	return 0
}

// waitForMetric waits until series reaches wantMin: the request is counted
// when the telemetry middleware finishes, which may trail the response.
func (f *priorityFixture) waitForMetric(t *testing.T, series string, wantMin float64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := f.metric(t, series)
		if got >= wantMin {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for metric %s >= %v; got %v", series, wantMin, got)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func classSeries(class string) string {
	return `gateway_requests_total{pool="large",class="` + class + `",status="200"}`
}

// prioritySettings are the global priority knobs a test case varies; empty
// fields mean "user".
type prioritySettings struct {
	defaultClass, anonymousMax, defaultMax string
}

func orUserClass(c string) string {
	if c == "" {
		return config.PriorityUser
	}
	return c
}

func priorityYAML(mockURL string, authEnabled bool, ps prioritySettings) string {
	return `
pools:
  - name: large
    replicas:
      - {name: large-0, url: ` + mockURL + `}
default_pool: large
auth:
  enabled: ` + strconv.FormatBool(authEnabled) + `
  callers:
    - name: chat-service
      token: "chat-service-token-12345"
    - name: batch-service
      token: "batch-service-token-12345"
    - name: plain-service
      token: "plain-service-token-12345"
    - name: capped-service
      token: "capped-service-token-12345"
priority:
  classes:
    - interactive
    - user
    - background
  default_class: ` + orUserClass(ps.defaultClass) + `
  anonymous_max_class: ` + orUserClass(ps.anonymousMax) + `
  default_max_class: ` + orUserClass(ps.defaultMax) + `
  callers:
    chat-service:
      default: interactive
      max: interactive
    batch-service:
      default: background
      max: user
    capped-service:
      max: background
`
}

// TestIngressPriorityClamp checks the class each kind of caller ends up with,
// as counted in gateway_requests_total, through the real auth middleware. A
// bad X-Priority never fails the request.
func TestIngressPriorityClamp(t *testing.T) {
	mockSrv := startMock(t, nil)
	anonTop := prioritySettings{anonymousMax: config.PriorityInteractive}
	anonLow := prioritySettings{defaultClass: config.PriorityBackground, anonymousMax: config.PriorityBackground}
	defTop := prioritySettings{defaultMax: config.PriorityInteractive}
	tests := []struct {
		name   string
		ps     prioritySettings
		auth   bool
		token  string
		header string
		want   string
	}{
		// Auth off: the middleware attaches the unauthenticated "anonymous"
		// caller, which must get anonymous_max_class, not default_max_class.
		{name: "anonymous without header", want: config.PriorityUser},
		{name: "anonymous interactive clamped", header: "interactive", want: config.PriorityUser},
		{name: "anonymous mixed-case interactive clamped", header: "Interactive", want: config.PriorityUser},
		{name: "anonymous uppercase background honored", header: "BACKGROUND", want: config.PriorityBackground},
		{name: "anonymous unknown header", header: "!!urgent!!", want: config.PriorityUser},
		{name: "anonymous max interactive above default max", ps: anonTop, header: "interactive", want: config.PriorityInteractive},
		{name: "anonymous max background below default max", ps: anonLow, header: "interactive", want: config.PriorityBackground},
		{name: "anonymous max background without header", ps: anonLow, want: config.PriorityBackground},
		{name: "anonymous ignores a raised default max", ps: defTop, header: "interactive", want: config.PriorityUser},
		// Auth on.
		{name: "no-policy caller interactive clamped", auth: true, token: "plain-service-token-12345", header: "interactive", want: config.PriorityUser},
		{name: "no-policy caller ignores a raised anonymous max", ps: anonTop, auth: true, token: "plain-service-token-12345", header: "interactive", want: config.PriorityUser},
		{name: "no-policy caller with raised default max", ps: defTop, auth: true, token: "plain-service-token-12345", header: "interactive", want: config.PriorityInteractive},
		{name: "no-policy caller background honored", auth: true, token: "plain-service-token-12345", header: "background", want: config.PriorityBackground},
		{name: "max-only caller default clamped to max", auth: true, token: "capped-service-token-12345", want: config.PriorityBackground},
		{name: "max-only caller header clamped to max", auth: true, token: "capped-service-token-12345", header: "INTERACTIVE", want: config.PriorityBackground},
		{name: "caller with both honors header within max", auth: true, token: "chat-service-token-12345", header: "InterActive", want: config.PriorityInteractive},
		{name: "caller with both clamps header", auth: true, token: "batch-service-token-12345", header: "interactive", want: config.PriorityUser},
		{name: "caller with both unknown header gets its default", auth: true, token: "batch-service-token-12345", header: "ultra-urgent", want: config.PriorityBackground},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newPriorityFixture(t, priorityYAML(mockSrv.URL(), tc.auth, tc.ps))
			resp := f.post(t, tc.token, tc.header)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}
			f.waitForMetric(t, classSeries(tc.want), 1)
			for _, other := range []string{config.PriorityInteractive, config.PriorityUser, config.PriorityBackground} {
				if other == tc.want {
					continue
				}
				if n := f.metric(t, classSeries(other)); n != 0 {
					t.Errorf("class %q counted %v requests, want 0", other, n)
				}
			}
		})
	}
}

// config cannot import auth, so it repeats the auth-off caller name.
func TestAnonymousCallerNameMatchesAuth(t *testing.T) {
	if auth.AnonymousName != config.AnonymousCallerName {
		t.Fatalf("auth.AnonymousName = %q, config.AnonymousCallerName = %q", auth.AnonymousName, config.AnonymousCallerName)
	}
}

func TestIngressPriorityHotReload(t *testing.T) {
	mockSrv := startMock(t, nil)
	f := newPriorityFixture(t, priorityYAML(mockSrv.URL(), true, prioritySettings{}))

	// Without caller policies, chat-service falls back to default_class.
	err := f.h.UpdatePriority(config.PriorityConfig{
		Classes:           []string{"interactive", "user", "background"},
		DefaultClass:      "background",
		AnonymousMaxClass: "user",
		DefaultMaxClass:   "user",
	})
	if err != nil {
		t.Fatalf("UpdatePriority: %v", err)
	}
	if p := f.h.Priority(); p.DefaultClass != "background" {
		t.Fatalf("updated priority default_class = %q, want background", p.DefaultClass)
	}
	if resp := f.post(t, "chat-service-token-12345", ""); resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	f.waitForMetric(t, classSeries(config.PriorityBackground), 1)

	// The policy is gone too, so chat-service's interactive is now clamped.
	if resp := f.post(t, "chat-service-token-12345", "interactive"); resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	f.waitForMetric(t, classSeries(config.PriorityUser), 1)
	if n := f.metric(t, classSeries(config.PriorityInteractive)); n != 0 {
		t.Errorf("interactive counted %v requests after reload, want 0", n)
	}
}

func TestUpdatePriorityRejectsInvalidAndKeepsPrevious(t *testing.T) {
	mockSrv := startMock(t, nil)
	f := newPriorityFixture(t, priorityYAML(mockSrv.URL(), false, prioritySettings{}))
	before := f.h.Priority()

	invalid := []config.PriorityConfig{
		{},
		{Classes: []string{"high", "low"}, DefaultClass: "low"}, // no maxima
		{Classes: []string{"interactive", "user"}, DefaultClass: "interactive", AnonymousMaxClass: "user", DefaultMaxClass: "user"},
	}
	for i, p := range invalid {
		if err := f.h.UpdatePriority(p); err == nil {
			t.Errorf("invalid[%d]: UpdatePriority accepted %+v", i, p)
		}
	}
	if after := f.h.Priority(); !reflect.DeepEqual(after, before) {
		t.Fatalf("priority config changed after rejected updates: %+v, want %+v", after, before)
	}
	// The kept config still clamps an anonymous interactive request to user.
	if resp := f.post(t, "", "interactive"); resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	f.waitForMetric(t, classSeries(config.PriorityUser), 1)
}

func TestUpdatePriorityCopiesInput(t *testing.T) {
	mockSrv := startMock(t, nil)
	f := newPriorityFixture(t, priorityYAML(mockSrv.URL(), true, prioritySettings{}))
	p := config.Defaults().Priority
	p.Callers = map[string]config.CallerPolicy{"plain-service": {Max: config.PriorityBackground}}
	if err := f.h.UpdatePriority(p); err != nil {
		t.Fatalf("UpdatePriority: %v", err)
	}
	p.Callers["plain-service"] = config.CallerPolicy{Max: config.PriorityInteractive}
	p.Classes[0] = "mutated"
	got := f.h.Priority()
	if got.Callers["plain-service"].Max != config.PriorityBackground || got.Classes[0] != config.PriorityInteractive {
		t.Fatalf("stored config changed with the caller's copy: %+v", got)
	}
}
