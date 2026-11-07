package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPriorityConfig_DefaultsAndValidation(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		cfg := Defaults()
		if len(cfg.Priority.Classes) != 3 {
			t.Fatalf("expected 3 default classes, got %d", len(cfg.Priority.Classes))
		}
		expected := []string{PriorityInteractive, PriorityUser, PriorityBackground}
		for i, want := range expected {
			if cfg.Priority.Classes[i] != want {
				t.Errorf("class[%d] = %q, want %q", i, cfg.Priority.Classes[i], want)
			}
		}
		if cfg.Priority.DefaultClass != PriorityUser {
			t.Errorf("default_class = %q, want %q", cfg.Priority.DefaultClass, PriorityUser)
		}
	})

	validBase := minimalYAML + `
priority:
  classes:
    - interactive
    - user
    - background
  default_class: user
`

	tests := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name: "empty classes",
			yaml: minimalYAML + `
priority:
  classes: []
  default_class: user
`,
			wantErr: "priority.classes: at least one class is required",
		},
		{
			name: "invalid class name",
			yaml: minimalYAML + `
priority:
  classes:
    - interactive
    - "invalid name!"
  default_class: interactive
`,
			wantErr: "must match",
		},
		{
			name: "duplicate class name",
			yaml: minimalYAML + `
priority:
  classes:
    - interactive
    - interactive
  default_class: interactive
`,
			wantErr: "duplicate class",
		},
		{
			name: "default class missing from classes",
			yaml: minimalYAML + `
priority:
  classes:
    - interactive
    - background
  default_class: user
`,
			wantErr: "priority.default_class: \"user\" is not in priority.classes",
		},
		{
			name: "caller default class invalid",
			yaml: validBase + `
  callers:
    chat:
      default: unknown_class
`,
			wantErr: "priority.callers[chat].default: \"unknown_class\" is not in priority.classes",
		},
		{
			name: "caller max class invalid",
			yaml: validBase + `
  callers:
    chat:
      max: unknown_class
`,
			wantErr: "priority.callers[chat].max: \"unknown_class\" is not in priority.classes",
		},
		{
			name: "caller default exceeds max priority",
			yaml: validBase + `
  callers:
    batch:
      default: interactive
      max: user
`,
			wantErr: "default class \"interactive\" has higher priority than max \"user\"",
		},
		{
			name: "uppercase class name rejected with lowercase hint",
			yaml: minimalYAML + `
priority:
  classes: [Interactive, user, background]
`,
			wantErr: "priority.classes[0]: \"Interactive\" must match",
		},
		{
			name: "uppercase default_class gets lowercase hint",
			yaml: minimalYAML + `
priority:
  classes: [interactive, user, background]
  default_class: User
`,
			wantErr: "priority.default_class: \"User\" is not in priority.classes (class names are lowercase)",
		},
		{
			name:    "anonymous_max_class empty",
			yaml:    validBase + "  anonymous_max_class: \"\"\n",
			wantErr: "priority.anonymous_max_class: required",
		},
		{
			name:    "anonymous_max_class unknown",
			yaml:    validBase + "  anonymous_max_class: urgent\n",
			wantErr: "priority.anonymous_max_class: \"urgent\" is not in priority.classes",
		},
		{
			name:    "anonymous_max_class uppercase",
			yaml:    validBase + "  anonymous_max_class: INTERACTIVE\n",
			wantErr: "priority.anonymous_max_class: \"INTERACTIVE\" is not in priority.classes (class names are lowercase)",
		},
		{
			name:    "default_max_class empty",
			yaml:    validBase + "  default_max_class: \"\"\n",
			wantErr: "priority.default_max_class: required",
		},
		{
			name:    "default_max_class unknown",
			yaml:    validBase + "  default_max_class: urgent\n",
			wantErr: "priority.default_max_class: \"urgent\" is not in priority.classes",
		},
		{
			name: "custom classes must set the maxima too",
			yaml: minimalYAML + `
priority:
  classes: [high, low]
  default_class: low
`,
			wantErr: "priority.anonymous_max_class: \"user\" is not in priority.classes",
		},
		{
			name: "custom classes with maxima",
			yaml: minimalYAML + `
priority:
  classes: [high, low]
  default_class: low
  anonymous_max_class: low
  default_max_class: high
`,
		},
		{
			name: "caller max uppercase",
			yaml: validBase + `
  callers:
    chat:
      max: Interactive
`,
			wantErr: "priority.callers[chat].max: \"Interactive\" is not in priority.classes (class names are lowercase)",
		},
		{
			name: "caller default without max above default_max_class",
			yaml: validBase + `
  callers:
    chat:
      default: interactive
`,
			wantErr: "priority.callers[chat]: default class \"interactive\" has higher priority than priority.default_max_class \"user\"",
		},
		{
			name: "caller default without max within raised default_max_class",
			yaml: validBase + `  default_max_class: interactive
  callers:
    chat:
      default: interactive
`,
		},
		{
			name: "caller max only",
			yaml: validBase + `
  callers:
    batch:
      max: background
`,
		},
		{
			name: "caller policy for the auth-off caller name",
			yaml: validBase + `
  callers:
    anonymous:
      max: interactive
`,
			wantErr: "priority.callers[anonymous]: \"anonymous\" is the caller name used with auth off",
		},
		{
			name: "default_class above anonymous_max_class",
			yaml: validBase + `  anonymous_max_class: background
`,
			wantErr: "priority.default_class: \"user\" has higher priority than priority.anonymous_max_class \"background\"",
		},
		{
			name: "default_class above default_max_class",
			yaml: minimalYAML + `
priority:
  classes: [interactive, user, background]
  default_class: interactive
  anonymous_max_class: interactive
`,
			wantErr: "priority.default_class: \"interactive\" has higher priority than priority.default_max_class \"user\"",
		},
		{
			name: "default_class at both maxima",
			yaml: minimalYAML + `
priority:
  classes: [interactive, user, background]
  default_class: background
  anonymous_max_class: background
  default_max_class: background
`,
		},
		{
			name: "valid caller policy",
			yaml: validBase + `
  callers:
    chat:
      default: interactive
      max: interactive
    batch:
      default: background
      max: user
`,
			wantErr: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.yaml), envMap(nil))
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			} else {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
				}
			}
		})
	}
}

// testPriority is the default priority config plus a caller for every
// policy shape ResolveClass distinguishes.
func testPriority() PriorityConfig {
	p := Defaults().Priority
	p.Callers = map[string]CallerPolicy{
		"chat-service":  {Default: PriorityInteractive, Max: PriorityInteractive},
		"batch-service": {Default: PriorityBackground, Max: PriorityUser},
		"max-bg-only":   {Max: PriorityBackground},  // default_class user is above max
		"max-top-only":  {Max: PriorityInteractive}, // may ask for anything
		"default-only":  {Default: PriorityBackground},
	}
	return p
}

func TestPriorityConfig_ExtractionPrecedence(t *testing.T) {
	tests := []struct {
		name          string
		mutate        func(*PriorityConfig)
		headerVal     string
		callerName    string
		authenticated bool
		want          string
	}{
		// Anonymous (auth off): clamped to anonymous_max_class. The auth
		// middleware names the caller "anonymous"; only Authenticated counts.
		{name: "auth-off caller name is ignored", headerVal: "interactive", callerName: AnonymousCallerName, want: PriorityUser},
		{
			name:       "auth-off uses anonymous max, not default max",
			mutate:     func(p *PriorityConfig) { p.AnonymousMaxClass = PriorityInteractive },
			headerVal:  "interactive",
			callerName: AnonymousCallerName,
			want:       PriorityInteractive,
		},
		{
			name: "auth-off lower anonymous max clamps the header",
			mutate: func(p *PriorityConfig) {
				p.AnonymousMaxClass, p.DefaultClass = PriorityBackground, PriorityBackground
			},
			headerVal:  "interactive",
			callerName: AnonymousCallerName,
			want:       PriorityBackground,
		},
		{
			name: "unauthenticated caller policy is ignored",
			mutate: func(p *PriorityConfig) {
				p.Callers[AnonymousCallerName] = CallerPolicy{Default: PriorityInteractive, Max: PriorityInteractive}
			},
			headerVal:  "interactive",
			callerName: AnonymousCallerName,
			want:       PriorityUser,
		},
		{name: "anonymous without header gets default_class", want: PriorityUser},
		{name: "anonymous interactive header clamped to anonymous max", headerVal: "interactive", want: PriorityUser},
		{name: "anonymous mixed-case interactive header clamped", headerVal: "  INTERACTIVE  ", want: PriorityUser},
		{name: "anonymous background header honored", headerVal: "background", want: PriorityBackground},
		{name: "anonymous mixed-case background header honored", headerVal: "BackGround", want: PriorityBackground},
		{name: "anonymous unknown header falls back to default_class", headerVal: "super-fast", want: PriorityUser},
		{
			name:      "anonymous max at top class trusts the header",
			mutate:    func(p *PriorityConfig) { p.AnonymousMaxClass = PriorityInteractive },
			headerVal: "Interactive",
			want:      PriorityInteractive,
		},
		{
			name:   "anonymous default_class above anonymous max is clamped",
			mutate: func(p *PriorityConfig) { p.DefaultClass = PriorityInteractive },
			want:   PriorityUser,
		},
		{
			name:   "anonymous max below default_class clamps the default",
			mutate: func(p *PriorityConfig) { p.AnonymousMaxClass = PriorityBackground },
			want:   PriorityBackground,
		},

		// Authenticated caller without a policy: clamped to default_max_class.
		{
			name:          "authenticated caller does not use anonymous max",
			mutate:        func(p *PriorityConfig) { p.AnonymousMaxClass = PriorityInteractive },
			headerVal:     "interactive",
			callerName:    "other-service",
			authenticated: true,
			want:          PriorityUser,
		},
		{name: "no-policy caller without header gets default_class", callerName: "other-service", authenticated: true, want: PriorityUser},
		{name: "no-policy caller interactive header clamped", headerVal: "interactive", callerName: "other-service", authenticated: true, want: PriorityUser},
		{name: "no-policy caller background header honored", headerVal: "background", callerName: "other-service", authenticated: true, want: PriorityBackground},
		{name: "no-policy caller unknown header", headerVal: "urgent!!", callerName: "other-service", authenticated: true, want: PriorityUser},
		{
			name:       "no-policy caller with default_max interactive trusts the header",
			mutate:     func(p *PriorityConfig) { p.DefaultMaxClass = PriorityInteractive },
			headerVal:  "interactive",
			callerName: "other-service", authenticated: true,
			want: PriorityInteractive,
		},
		{
			name:       "anonymous max does not apply to authenticated callers",
			mutate:     func(p *PriorityConfig) { p.AnonymousMaxClass = PriorityBackground },
			callerName: "other-service", authenticated: true,
			want: PriorityUser,
		},

		// Caller with max only.
		{name: "max-only caller default_class clamped to max", callerName: "max-bg-only", authenticated: true, want: PriorityBackground},
		{name: "max-only caller header clamped to max", headerVal: "user", callerName: "max-bg-only", authenticated: true, want: PriorityBackground},
		{name: "max-only caller unknown header gets clamped default", headerVal: "nope", callerName: "max-bg-only", authenticated: true, want: PriorityBackground},
		{name: "max-only caller with top max honors header", headerVal: "interactive", callerName: "max-top-only", authenticated: true, want: PriorityInteractive},
		{name: "max-only caller with top max default is default_class", callerName: "max-top-only", authenticated: true, want: PriorityUser},

		// Caller with default only: max is default_max_class.
		{name: "default-only caller without header", callerName: "default-only", authenticated: true, want: PriorityBackground},
		{name: "default-only caller interactive header clamped to default max", headerVal: "interactive", callerName: "default-only", authenticated: true, want: PriorityUser},

		// Caller with both.
		{name: "batch caller interactive clamped to user max", headerVal: "interactive", callerName: "batch-service", authenticated: true, want: PriorityUser},
		{name: "batch caller user within max", headerVal: "user", callerName: "batch-service", authenticated: true, want: PriorityUser},
		{name: "batch caller background within max", headerVal: "background", callerName: "batch-service", authenticated: true, want: PriorityBackground},
		{name: "batch caller mixed-case header", headerVal: " BACKGROUND", callerName: "batch-service", authenticated: true, want: PriorityBackground},
		{name: "batch caller unknown header falls back to caller default", headerVal: "super-fast", callerName: "batch-service", authenticated: true, want: PriorityBackground},
		{name: "batch caller empty header gets caller default", callerName: "batch-service", authenticated: true, want: PriorityBackground},
		{name: "chat caller interactive allowed by max", headerVal: "interactive", callerName: "chat-service", authenticated: true, want: PriorityInteractive},
		{name: "chat caller mixed-case header", headerVal: "InterActive", callerName: "chat-service", authenticated: true, want: PriorityInteractive},
		{name: "chat caller empty header gets caller default", callerName: "chat-service", authenticated: true, want: PriorityInteractive},
		{
			// Validation rejects this; ResolveClass still never exceeds max.
			name: "caller default above max is clamped at runtime",
			mutate: func(p *PriorityConfig) {
				p.Callers["bad"] = CallerPolicy{Default: PriorityInteractive, Max: PriorityUser}
			},
			callerName: "bad", authenticated: true,
			want: PriorityUser,
		},
		{
			name:       "header with non-ASCII case fold is unknown",
			headerVal:  "İnteractive",
			callerName: "chat-service", authenticated: true,
			want: PriorityInteractive, // falls back to the caller default
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := testPriority()
			if tc.mutate != nil {
				tc.mutate(&p)
			}
			got := p.ResolveClass(tc.headerVal, tc.callerName, tc.authenticated)
			if got != tc.want {
				t.Errorf("ResolveClass(%q, %q, %v) = %q, want %q", tc.headerVal, tc.callerName, tc.authenticated, got, tc.want)
			}
		})
	}
}

// A PriorityConfig built in code with empty or unknown maxima (Validate
// would reject it) still clamps: the ceiling falls back to DefaultClass,
// then "user", then the lowest class.
func TestPriorityConfig_ResolveClassCeilingFallback(t *testing.T) {
	std := []string{PriorityInteractive, PriorityUser, PriorityBackground}
	tests := []struct {
		name string
		p    PriorityConfig
		want string
	}{
		{name: "empty maxima and default fall back to user", p: PriorityConfig{Classes: std}, want: PriorityUser},
		{name: "empty maxima fall back to default_class", p: PriorityConfig{Classes: std, DefaultClass: PriorityBackground}, want: PriorityBackground},
		{name: "unknown maxima fall back to default_class", p: PriorityConfig{Classes: std, DefaultClass: PriorityBackground, AnonymousMaxClass: "nope", DefaultMaxClass: "nope"}, want: PriorityBackground},
		{name: "custom classes without user use default_class", p: PriorityConfig{Classes: []string{"high", "low"}, DefaultClass: "low"}, want: "low"},
		{name: "custom classes without any default use the lowest class", p: PriorityConfig{Classes: []string{"high", "mid", "low"}}, want: "low"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			header := strings.ToUpper(tc.p.Classes[0])
			for _, authenticated := range []bool{false, true} {
				if got := tc.p.ResolveClass(header, "svc", authenticated); got != tc.want {
					t.Errorf("authenticated=%v: ResolveClass(%q) = %q, want %q", authenticated, header, got, tc.want)
				}
			}
		})
	}
}

func TestPriorityConfig_Validate(t *testing.T) {
	if err := Defaults().Priority.Validate(); err != nil {
		t.Fatalf("defaults: %v", err)
	}
	bad := Defaults().Priority
	bad.AnonymousMaxClass = ""
	if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "priority.anonymous_max_class: required") {
		t.Fatalf("Validate() = %v, want anonymous_max_class required", err)
	}
}

func TestPriorityConfig_Reload(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "gateway.yaml")

	yaml1 := minimalYAML + `
priority:
  classes:
    - interactive
    - user
    - background
  default_class: user
  callers:
    test-svc:
      default: user
`
	if err := os.WriteFile(configPath, []byte(yaml1), 0o600); err != nil {
		t.Fatal(err)
	}

	getenv := envMap(map[string]string{
		EnvConfigPath:          configPath,
		"GATEWAY_AUTH_ENABLED": "false",
	})

	cfg1, err := Load(getenv)
	if err != nil {
		t.Fatalf("first Load failed: %v", err)
	}
	if cfg1.Priority.DefaultClass != "user" {
		t.Fatalf("cfg1 default_class = %q, want %q", cfg1.Priority.DefaultClass, "user")
	}
	if pol := cfg1.Priority.Callers["test-svc"]; pol.Default != "user" {
		t.Fatalf("cfg1 test-svc default = %q, want %q", pol.Default, "user")
	}

	// Hot reload / reload with changed priority settings
	yaml2 := minimalYAML + `
priority:
  classes:
    - interactive
    - user
    - background
  default_class: background
  callers:
    test-svc:
      default: interactive
      max: interactive
`
	if err := os.WriteFile(configPath, []byte(yaml2), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg2, err := Load(getenv)
	if err != nil {
		t.Fatalf("second Load failed: %v", err)
	}
	if cfg2.Priority.DefaultClass != "background" {
		t.Fatalf("cfg2 default_class = %q, want %q", cfg2.Priority.DefaultClass, "background")
	}
	if pol := cfg2.Priority.Callers["test-svc"]; pol.Default != "interactive" || pol.Max != "interactive" {
		t.Fatalf("cfg2 test-svc policy = %+v, want default=interactive, max=interactive", pol)
	}
}

// TestPriorityProbeSettings: priority.probe defaults to what the probe
// borrowed before (the load view's probe interval, the stuck-engine probe's
// timeout), is bounded, keeps the timeout within the interval, and is not
// part of what UpdatePriority validates.
func TestPriorityProbeSettings(t *testing.T) {
	cfg := mustParse(t, minimalYAML, nil)
	if p := cfg.Priority.Probe; p.Interval != cfg.Router.LoadView.ProbeInterval || p.Timeout != cfg.Router.Health.StuckProbeTimeout {
		t.Fatalf("defaults = %+v, want the load view's probe interval and the stuck probe's timeout", p)
	}
	for _, tc := range []struct {
		yaml, want string // want "" accepts
	}{
		{"priority: {probe: {interval: 500ms, timeout: 500ms}}", "priority.probe.interval: 500ms out of range"},
		{"priority: {probe: {interval: 2h}}", "priority.probe.interval: 2h0m0s out of range"},
		{"priority: {probe: {interval: 1h0m1s}}", "priority.probe.interval: 1h0m1s out of range"},
		{"priority: {probe: {timeout: 0s}}", "priority.probe.timeout: 0s out of range"},
		{"priority: {probe: {timeout: 500ms}}", "priority.probe.timeout: 500ms out of range"},
		{"priority: {probe: {interval: 30m, timeout: 10m}}", "priority.probe.timeout: 10m0s out of range"},
		{"priority: {probe: {interval: 5s, timeout: 6s}}", "priority.probe.timeout: 6s exceeds priority.probe.interval"},
		{"priority: {probe: {interval: 1s, timeout: 1s}}", ""},
		{"priority: {probe: {interval: 1h, timeout: 5m}}", ""},
	} {
		_, err := Parse([]byte(minimalYAML+tc.yaml+"\n"), envMap(nil))
		switch {
		case tc.want == "" && err != nil:
			t.Errorf("%s: %v, want it accepted", tc.yaml, err)
		case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
			t.Errorf("%s: err %v, want %q", tc.yaml, err, tc.want)
		}
	}
	p := cfg.Priority
	p.Probe = PriorityProbe{}
	if err := p.Validate(); err != nil {
		t.Errorf("PriorityConfig.Validate with no probe settings: %v, want them ignored", err)
	}
}
