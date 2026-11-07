package config

import (
	"fmt"
	"strings"
	"time"
	"unicode"
)

// Template registry and admin API defaults (M3).
const (
	DefaultTemplatesRefreshInterval = 5 * time.Second
	DefaultTemplatesMaxVersions     = 4096
	DefaultPostgresConnectTimeout   = 2 * time.Second
	DefaultPostgresQueryTimeout     = 2 * time.Second
	DefaultPostgresMaxConns         = 4
	// DefaultMaxTokensMargin is the headroom over a template's observed
	// p99.9 output length (ADR 0007).
	DefaultMaxTokensMargin     = 0.25
	DefaultMaxTokensMinSamples = 1000

	DefaultAdminListenAddr      = ":8081"
	DefaultAdminTokenizeTimeout = 5 * time.Second

	// MaxTemplatesMaxVersions caps templates.max_versions: every served
	// version is held in memory and may label metric series.
	MaxTemplatesMaxVersions = 65536
	// MaxTokensCeilingLimit caps pools[].max_tokens_ceiling.
	MaxTokensCeilingLimit = 1 << 20
	// MaxModelLenLimit caps pools[].max_model_len.
	MaxModelLenLimit = 1 << 24
)

// Templates configures the prompt template registry (M3): the Postgres
// store, the request path's in-memory snapshot and the max_tokens policy.
// With no DSN the registry is off: X-Template-Id is ignored (fail open,
// counted) and the admin API does not start.
type Templates struct {
	Postgres Postgres `yaml:"postgres"`
	// RefreshInterval is how often the snapshot is checked for changes.
	RefreshInterval time.Duration `yaml:"refresh_interval"`
	// MaxVersions caps the served (active and deprecated) versions held in
	// the snapshot; a store holding more keeps the last snapshot.
	MaxVersions int             `yaml:"max_versions"`
	MaxTokens   MaxTokensPolicy `yaml:"max_tokens"`
}

// Enabled reports whether the registry has a store.
func (t Templates) Enabled() bool { return t.Postgres.DSN.IsSet() }

// Postgres configures the template store connection.
type Postgres struct {
	// DSN is a libpq connection string or URL. Prefer the
	// GATEWAY_TEMPLATES_POSTGRES_DSN environment variable: it holds a password.
	DSN            Secret        `yaml:"dsn"`
	ConnectTimeout time.Duration `yaml:"connect_timeout"`
	// QueryTimeout bounds every store query, including the refresher's.
	QueryTimeout time.Duration `yaml:"query_timeout"`
	MaxConns     int           `yaml:"max_conns"`
}

// MaxTokensPolicy sets max_tokens on matched template requests that carry
// no cap of their own (ADR 0007). A caller's cap is never changed.
type MaxTokensPolicy struct {
	Enabled bool `yaml:"enabled"`
	// Margin is the headroom over the template version's observed p99.9
	// completion length: cap = p99.9 × (1 + Margin), at most the pool's
	// max_tokens_ceiling.
	Margin float64 `yaml:"margin"`
	// MinSamples is how many completions a version needs before its p99.9
	// is used; before that only the pool ceiling applies.
	MinSamples int `yaml:"min_samples"`
}

// Admin configures the template admin API. It listens on its own port,
// which must not be routed through the public proxy, and starts only when
// ListenAddr is set, at least one token is configured and the registry is
// enabled.
type Admin struct {
	// ListenAddr serves /admin/v1. Empty turns the admin API off.
	ListenAddr string `yaml:"listen_addr"`
	// Tokens are the bearer tokens accepted by the admin API, configured
	// like auth.callers (name plus token or token_env).
	Tokens []Caller `yaml:"tokens"`
	// TokenizeTimeout bounds one /tokenize call to a replica at
	// registration.
	TokenizeTimeout time.Duration `yaml:"tokenize_timeout"`
}

// AdminEnabled reports whether the admin API should start.
func (c *Config) AdminEnabled() bool {
	return c.Admin.ListenAddr != "" && len(c.Admin.Tokens) > 0 && c.Templates.Enabled()
}

func defaultTemplates() Templates {
	return Templates{
		Postgres: Postgres{
			ConnectTimeout: DefaultPostgresConnectTimeout,
			QueryTimeout:   DefaultPostgresQueryTimeout,
			MaxConns:       DefaultPostgresMaxConns,
		},
		RefreshInterval: DefaultTemplatesRefreshInterval,
		MaxVersions:     DefaultTemplatesMaxVersions,
		MaxTokens: MaxTokensPolicy{
			Enabled:    true,
			Margin:     DefaultMaxTokensMargin,
			MinSamples: DefaultMaxTokensMinSamples,
		},
	}
}

func defaultAdmin() Admin {
	return Admin{ListenAddr: DefaultAdminListenAddr, TokenizeTimeout: DefaultAdminTokenizeTimeout}
}

// resolveAdmin fills admin tokens from their environment variables. With
// the admin API off (no listen address or no registry), tokens are not
// resolved, as caller tokens are not with auth off.
func resolveAdmin(cfg *Config, getenv func(string) string) []error {
	if cfg.Admin.ListenAddr == "" || !cfg.Templates.Enabled() {
		return nil
	}
	var errs []error
	for i := range cfg.Admin.Tokens {
		t := &cfg.Admin.Tokens[i]
		key := fmt.Sprintf("admin.tokens[%d]", i)
		switch {
		case t.Token.IsSet() && t.TokenEnv != "":
			errs = append(errs, fmt.Errorf("%s: set only one of token and token_env", key))
		case t.TokenEnv != "":
			v := getenv(t.TokenEnv)
			if v == "" {
				errs = append(errs, fmt.Errorf("%s.token_env: environment variable %s is empty or unset", key, t.TokenEnv))
				continue
			}
			t.Token = Secret(v)
		}
	}
	return errs
}

func (c *Config) validateTemplates() []error {
	var errs []error
	t := c.Templates
	if t.RefreshInterval < MinReloadInterval {
		errs = append(errs, fmt.Errorf("templates.refresh_interval: %s is below the minimum %s", t.RefreshInterval, MinReloadInterval))
	}
	if t.MaxVersions < 1 || t.MaxVersions > MaxTemplatesMaxVersions {
		errs = append(errs, fmt.Errorf("templates.max_versions: %d out of range [1, %d]", t.MaxVersions, MaxTemplatesMaxVersions))
	}
	errs = appendIfErr(errs, checkPositive("templates.postgres.connect_timeout", t.Postgres.ConnectTimeout))
	errs = appendIfErr(errs, checkPositive("templates.postgres.query_timeout", t.Postgres.QueryTimeout))
	if t.Postgres.MaxConns < 1 || t.Postgres.MaxConns > 64 {
		errs = append(errs, fmt.Errorf("templates.postgres.max_conns: %d out of range [1, 64]", t.Postgres.MaxConns))
	}
	if t.Postgres.QueryTimeout >= t.RefreshInterval && t.RefreshInterval > 0 {
		errs = append(errs, fmt.Errorf("templates.postgres.query_timeout: %s must be below templates.refresh_interval %s",
			t.Postgres.QueryTimeout, t.RefreshInterval))
	}
	if m := t.MaxTokens.Margin; m < 0 || m > 10 {
		errs = append(errs, fmt.Errorf("templates.max_tokens.margin: %g out of range [0, 10]", m))
	}
	if n := t.MaxTokens.MinSamples; n < 1 || n > 10_000_000 {
		errs = append(errs, fmt.Errorf("templates.max_tokens.min_samples: %d out of range [1, 10000000]", n))
	}
	for i, p := range c.Pools {
		if p.MaxModelLen < 0 || p.MaxModelLen > MaxModelLenLimit {
			errs = append(errs, fmt.Errorf("pools[%d].max_model_len: %d out of range [0, %d]", i, p.MaxModelLen, MaxModelLenLimit))
		}
		if p.MaxTokensCeiling < 0 || p.MaxTokensCeiling > MaxTokensCeilingLimit {
			errs = append(errs, fmt.Errorf("pools[%d].max_tokens_ceiling: %d out of range [0, %d]", i, p.MaxTokensCeiling, MaxTokensCeilingLimit))
		}
	}
	return errs
}

func (c *Config) validateAdmin() []error {
	a := c.Admin
	if !c.AdminEnabled() {
		return nil
	}
	var errs []error
	errs = append(errs, checkListenAddr("admin.listen_addr", a.ListenAddr)...)
	for _, other := range []struct{ key, addr string }{
		{"server.listen_addr", c.Server.ListenAddr},
		{"server.metrics_listen_addr", c.Server.MetricsListenAddr},
	} {
		if addrsConflict(a.ListenAddr, other.addr) {
			errs = append(errs, fmt.Errorf("admin.listen_addr: %q conflicts with %s %q", a.ListenAddr, other.key, other.addr))
		}
	}
	errs = appendIfErr(errs, checkPositive("admin.tokenize_timeout", a.TokenizeTimeout))
	names := make(map[string]bool, len(a.Tokens))
	tokens := make(map[Secret]string, len(a.Tokens))
	for i, t := range a.Tokens {
		key := fmt.Sprintf("admin.tokens[%d]", i)
		switch {
		case !namePattern.MatchString(t.Name):
			errs = append(errs, fmt.Errorf("%s.name: %q must match %s", key, t.Name, namePattern))
		case names[t.Name]:
			errs = append(errs, fmt.Errorf("%s.name: duplicate admin token name %q", key, t.Name))
		}
		names[t.Name] = true
		if !t.Token.IsSet() {
			if t.TokenEnv == "" {
				errs = append(errs, fmt.Errorf("%s: one of token or token_env is required", key))
			}
			continue
		}
		tok := t.Token.Reveal()
		if len(tok) < MinTokenLength {
			errs = append(errs, fmt.Errorf("%s: token %q is shorter than %d characters", key, t.Name, MinTokenLength))
		}
		if strings.ContainsFunc(tok, unicode.IsSpace) {
			errs = append(errs, fmt.Errorf("%s: token %q must not contain whitespace", key, t.Name))
		}
		if prev, dup := tokens[t.Token]; dup {
			errs = append(errs, fmt.Errorf("%s: token %q duplicates admin token %q", key, t.Name, prev))
		} else {
			tokens[t.Token] = t.Name
		}
		for _, cl := range c.Auth.Callers {
			if cl.Token.IsSet() && cl.Token == t.Token {
				errs = append(errs, fmt.Errorf("%s: token %q is also caller %q's token; admin tokens must be separate", key, t.Name, cl.Name))
			}
		}
	}
	return errs
}
