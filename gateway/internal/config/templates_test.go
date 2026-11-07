package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const adminYAML = minimalYAML + `
templates:
  postgres:
    dsn: postgres://gateway:pw@postgres/gateway
admin:
  listen_addr: ":8081"
  tokens:
    - name: owners
      token_env: ADMIN_TOKEN
`

func TestAdminEnabledNeedsListenTokensAndRegistry(t *testing.T) {
	cfg := mustParse(t, adminYAML, map[string]string{"ADMIN_TOKEN": "admin-token-0123456789"})
	if !cfg.AdminEnabled() {
		t.Fatal("admin API should be enabled with a DSN, listen address and token")
	}
	if got := cfg.Admin.Tokens[0].Token.Reveal(); got != "admin-token-0123456789" {
		t.Fatalf("admin token not resolved from env: %q", got)
	}
	// Without a DSN the admin API is off and its token env is not required.
	off := mustParse(t, minimalYAML+`
admin:
  tokens:
    - name: owners
      token_env: ADMIN_TOKEN
`, nil)
	if off.AdminEnabled() || off.Templates.Enabled() {
		t.Fatal("admin and registry must be off without templates.postgres.dsn")
	}
}

func TestAdminValidation(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		env  map[string]string
		want string
	}{
		{"missing token env", adminYAML, nil, "admin.tokens[0].token_env: environment variable ADMIN_TOKEN is empty or unset"},
		{"short token", adminYAML, map[string]string{"ADMIN_TOKEN": "short"}, "shorter than 16"},
		{"conflicts with metrics port", strings.Replace(adminYAML, `":8081"`, `":9090"`, 1),
			map[string]string{"ADMIN_TOKEN": "admin-token-0123456789"}, "admin.listen_addr"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.yaml), envMap(tc.env))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestTemplatesValidation(t *testing.T) {
	tests := []struct {
		name, yaml, want string
	}{
		{"refresh too fast", "templates:\n  refresh_interval: 10ms\n", "templates.refresh_interval"},
		{"max_versions zero", "templates:\n  max_versions: 0\n", "templates.max_versions"},
		{"negative margin", "templates:\n  max_tokens:\n    margin: -1\n", "templates.max_tokens.margin"},
		{"query timeout above refresh", "templates:\n  refresh_interval: 1s\n  postgres:\n    query_timeout: 2s\n", "templates.postgres.query_timeout"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(minimalYAML+tc.yaml), envMap(nil))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestPoolMaxTokensCeilingBounds(t *testing.T) {
	y := strings.Replace(minimalYAML, "pools:\n  - name: large\n", "pools:\n  - name: large\n    max_tokens_ceiling: -1\n", 1)
	if y == minimalYAML {
		t.Skip("minimalYAML layout changed")
	}
	_, err := Parse([]byte(y), envMap(nil))
	if err == nil || !strings.Contains(err.Error(), "max_tokens_ceiling") {
		t.Fatalf("want max_tokens_ceiling error, got %v", err)
	}
}

func TestComposeConfigsValid(t *testing.T) {
	env := map[string]string{
		"GATEWAY_TOKEN_CHAT_SERVICE":        "chat-token-0123456789",
		"GATEWAY_TOKEN_BATCH_SERVICE":       "batch-token-0123456789",
		"GATEWAY_TEMPLATES_POSTGRES_DSN":    "postgres://gateway:pw@postgres:5432/gateway",
		"GATEWAY_ADMIN_TOKEN_PROMPT_OWNERS": "admin-token-0123456789",
	}
	for _, name := range []string{"gateway.dev.yaml", "gateway.gpu.yaml"} {
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(filepath.Dir(examplePath), name))
			if err != nil {
				t.Fatal(err)
			}
			cfg := mustParse(t, string(raw), env)
			if !cfg.AdminEnabled() {
				t.Fatal("admin API should be on with the DSN and token set")
			}
			if cfg.Pools[0].MaxTokensCeiling == 0 {
				t.Fatal("large pool should set max_tokens_ceiling")
			}
		})
	}
}
