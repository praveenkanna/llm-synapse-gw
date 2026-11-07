package config

import (
	"fmt"
	"strings"
	"testing"
)

// withPools prepends a valid two-pool section to extra YAML.
func withPools(extra string) string {
	return `
pools:
  - name: large
    replicas:
      - url: http://l0:8000
  - name: small
    replicas:
      - url: http://s0:8000
default_pool: large
` + extra
}

const authOff = "auth:\n  enabled: false\n"

// poolOf is a one-pool section with n replicas.
func poolOf(n int) string {
	var sb strings.Builder
	sb.WriteString("pools:\n  - name: large\n    replicas:\n")
	for i := range n {
		fmt.Fprintf(&sb, "      - url: http://r%d:8000\n", i)
	}
	return sb.String()
}

// MaxPoolReplicas is the size AC-M1-5 is proven for (M1-26): a larger pool
// is valid and runs, because the cost of one is a warning's worth of
// latency, never a refused config.
func TestPoolReplicaCeiling(t *testing.T) {
	if MaxPoolReplicas != 64 {
		t.Fatalf("MaxPoolReplicas = %d, want 64: moving it moves what AC-M1-5 was measured at", MaxPoolReplicas)
	}
	for _, n := range []int{MaxPoolReplicas, MaxPoolReplicas + 1, 4 * MaxPoolReplicas} {
		cfg, err := Parse([]byte(poolOf(n)+authOff), envMap(nil))
		if err != nil {
			t.Fatalf("a pool of %d: %v", n, err)
		}
		if got := len(cfg.Pools[0].Replicas); got != n {
			t.Fatalf("a pool of %d loaded %d replicas", n, got)
		}
	}
}

// A replica's name is hashed several times on every route, so its limit is
// the longest name AC-M1-5 was measured with (audit row 14).
func TestReplicaNameLimit(t *testing.T) {
	pool := func(n int) []byte {
		return []byte("pools:\n  - name: large\n    replicas:\n      - name: " + strings.Repeat("r", n) + "\n        url: http://r0:8000\n" + authOff)
	}
	if _, err := Parse(pool(128), envMap(nil)); err != nil {
		t.Fatalf("a 128-byte name: %v", err)
	}
	if _, err := Parse(pool(129), envMap(nil)); err == nil || !strings.Contains(err.Error(), "longer than 128 characters") {
		t.Fatalf("a 129-byte name: %v", err)
	}
}

func TestValidationRejects(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		env  map[string]string
		want string
	}{
		// server / shutdown
		{"listen addr not host:port", withPools(authOff + "server:\n  listen_addr: \"8080\"\n"), nil, "server.listen_addr: \"8080\" is not host:port"},
		{"listen addr bad port", withPools(authOff + "server:\n  listen_addr: \":99999\"\n"), nil, "server.listen_addr: \":99999\" has invalid port"},
		{"metrics addr clashes", withPools(authOff + "server:\n  metrics_listen_addr: \":8080\"\n"), nil, "server.metrics_listen_addr: \":8080\" conflicts"},
		{"metrics addr clashes via wildcard", withPools(authOff + "server:\n  listen_addr: \"127.0.0.1:9000\"\n  metrics_listen_addr: \":9000\"\n"), nil, "conflicts with server.listen_addr"},
		{"zero read header timeout", withPools(authOff + "server:\n  read_header_timeout: 0s\n"), nil, "server.read_header_timeout: must be greater than zero"},
		{"negative idle timeout", withPools(authOff + "server:\n  idle_timeout: -1s\n"), nil, "server.idle_timeout: must be greater than zero"},
		{"body limit without unit", withPools(authOff + "server:\n  max_request_body_bytes: 32\n"), nil, "server.max_request_body_bytes: 32 bytes out of range [1KiB, 1GiB]"},
		{"body limit zero", withPools(authOff), map[string]string{"GATEWAY_MAX_REQUEST_BODY_BYTES": "0"}, "server.max_request_body_bytes: 0 bytes out of range"},
		{"body limit huge", withPools(authOff + "server:\n  max_request_body_bytes: 2GiB\n"), nil, "server.max_request_body_bytes: 2147483648 bytes out of range"},
		{"body limit bad unit", withPools(authOff + "server:\n  max_request_body_bytes: 32MB\n"), nil, "invalid size \"32MB\""},
		{"zero drain timeout", withPools(authOff), map[string]string{"GATEWAY_SHUTDOWN_DRAIN_TIMEOUT": "0s"}, "shutdown.drain_timeout: must be greater than zero"},

		// discovery
		{"zero reload interval", withPools(authOff + "discovery:\n  reload_interval: 0s\n"), nil, "discovery.reload_interval: 0s is below the minimum 100ms"},
		{"negative reload interval", withPools(authOff), map[string]string{"GATEWAY_DISCOVERY_RELOAD_INTERVAL": "-1s"}, "discovery.reload_interval: -1s is below the minimum"},
		{"reload interval too short", withPools(authOff + "discovery:\n  reload_interval: 5ms\n"), nil, "discovery.reload_interval: 5ms is below the minimum"},

		// upstream
		{"idle conns zero", withPools(authOff + "upstream:\n  max_idle_conns_per_host: 0\n"), nil, "upstream.max_idle_conns_per_host: 0 out of range"},
		{"idle conns huge", withPools(authOff + "upstream:\n  max_idle_conns_per_host: 1000000\n"), nil, "out of range"},
		{"negative connect retry backoff", withPools(authOff), map[string]string{"GATEWAY_UPSTREAM_CONNECT_RETRY_BACKOFF": "-1ms"}, "upstream.connect_retry_backoff: -1ms out of range [0s, 5s]"},
		{"connect retry backoff too long", withPools(authOff + "upstream:\n  connect_retry_backoff: 6s\n"), nil, "upstream.connect_retry_backoff: 6s out of range"},
		{"connect retry backoff exceeds total", withPools(authOff + "upstream:\n  connect_retry_backoff: 3s\n  timeouts:\n    total: 2s\n    idle_between_chunks: 1s\n    stream_response_header: 1s\n    connect: 1s\n  dns:\n    lookup_timeout: 1s\n"), nil, "upstream.connect_retry_backoff: 3s exceeds upstream.timeouts.total 2s"},
		{"zero connect timeout", withPools(authOff + "upstream:\n  timeouts:\n    connect: 0s\n"), nil, "upstream.timeouts.connect: must be greater than zero"},
		{"zero total timeout", withPools(authOff + "upstream:\n  timeouts:\n    total: 0s\n"), nil, "upstream.timeouts.total: must be greater than zero"},
		{"chunk gap exceeds total", withPools(authOff + "upstream:\n  timeouts:\n    idle_between_chunks: 20m\n"), nil, "upstream.timeouts.idle_between_chunks: 20m0s exceeds upstream.timeouts.total"},
		{"header exceeds total", withPools(authOff + "upstream:\n  timeouts:\n    total: 10s\n    idle_between_chunks: 5s\n    stream_response_header: 5s\n    response_header: 1m\n"), nil, "upstream.timeouts.response_header: 1m0s exceeds"},
		{"negative header timeout", withPools(authOff), map[string]string{"GATEWAY_UPSTREAM_RESPONSE_HEADER_TIMEOUT": "-1s"}, "upstream.timeouts.response_header: must not be negative"},
		{"negative stream header timeout", withPools(authOff + "upstream:\n  timeouts:\n    stream_response_header: -1s\n"), nil, "upstream.timeouts.stream_response_header: must not be negative"},
		{"stream header exceeds total", withPools(authOff + "upstream:\n  timeouts:\n    total: 30s\n    idle_between_chunks: 5s\n"), nil, "upstream.timeouts.stream_response_header: 1m0s exceeds"},
		{"negative first chunk timeout", withPools(authOff), map[string]string{"GATEWAY_UPSTREAM_FIRST_CHUNK_TIMEOUT": "-1s"}, "upstream.timeouts.first_chunk: must not be negative"},
		{"first chunk exceeds total", withPools(authOff + "upstream:\n  timeouts:\n    first_chunk: 16m\n"), nil, "upstream.timeouts.first_chunk: 16m0s exceeds"},
		{"negative dns max stale", withPools(authOff), map[string]string{"GATEWAY_UPSTREAM_DNS_MAX_STALE": "-1s"}, "upstream.dns.max_stale: -1s out of range [0s, 24h0m0s]"},
		{"dns max stale too long", withPools(authOff + "upstream:\n  dns:\n    max_stale: 25h\n"), nil, "upstream.dns.max_stale: 25h0m0s out of range"},
		{"dns ttl too short", withPools(authOff + "upstream:\n  dns:\n    ttl: 10ms\n"), nil, "upstream.dns.ttl: 10ms out of range [1s, 1h0m0s]"},
		{"dns ttl too long", withPools(authOff), map[string]string{"GATEWAY_UPSTREAM_DNS_TTL": "2h"}, "upstream.dns.ttl: 2h0m0s out of range"},
		{"zero dns lookup timeout", withPools(authOff + "upstream:\n  dns:\n    lookup_timeout: 0s\n"), nil, "upstream.dns.lookup_timeout: must be greater than zero"},
		{"dns lookup exceeds total", withPools(authOff + "upstream:\n  timeouts:\n    total: 1s\n    idle_between_chunks: 1s\n  dns:\n    lookup_timeout: 2s\n"), nil, "upstream.dns.lookup_timeout: 2s exceeds upstream.timeouts.total"},
		{"dns max hosts zero", withPools(authOff + "upstream:\n  dns:\n    max_hosts: 0\n"), nil, "upstream.dns.max_hosts: 0 out of range [1, 65536]"},
		{"dns max hosts huge", withPools(authOff), map[string]string{"GATEWAY_UPSTREAM_DNS_MAX_HOSTS": "100000"}, "upstream.dns.max_hosts: 100000 out of range"},
		{"api key whitespace", withPools(authOff), map[string]string{"GATEWAY_UPSTREAM_API_KEY": "a b"}, "upstream.api_key: must not contain whitespace"},

		// runtime
		{"negative gc percent", withPools(authOff + "runtime:\n  gc_percent: -1\n"), nil, "runtime.gc_percent: -1 out of range [0, 10000]"},
		{"huge gc percent", withPools(authOff), map[string]string{"GATEWAY_RUNTIME_GC_PERCENT": "20000"}, "runtime.gc_percent: 20000 out of range"},
		{"memory limit without unit", withPools(authOff + "runtime:\n  memory_limit: 768\n"), nil, "runtime.memory_limit: 768 bytes is below the minimum 64MiB"},
		{"memory limit bad unit", withPools(authOff + "runtime:\n  memory_limit: 768MB\n"), nil, "invalid size \"768MB\""},

		// pools and replicas
		{"no pools", authOff, nil, "pools: at least one pool is required"},
		{"bad pool name", "pools:\n  - name: Large Pool\n    replicas:\n      - url: http://a:1\n" + authOff, nil, "pools[0].name: \"Large Pool\" must match"},
		{"empty pool name", "pools:\n  - replicas:\n      - url: http://a:1\n" + authOff, nil, "pools[0].name: \"\" must match"},
		{"duplicate pool", "pools:\n  - name: large\n    replicas:\n      - url: http://a:1\n  - name: large\n    replicas:\n      - url: http://b:1\ndefault_pool: large\n" + authOff, nil, "pools[1].name: duplicate pool \"large\""},
		{"pool without replicas", "pools:\n  - name: large\n" + authOff, nil, "pools[0].replicas: pool \"large\" needs at least one replica"},
		{"replica url missing", "pools:\n  - name: large\n    replicas:\n      - name: r0\n" + authOff, nil, "pools[0].replicas[0].url: required"},
		{"replica url scheme", "pools:\n  - name: large\n    replicas:\n      - url: grpc://a:1\n" + authOff, nil, "must use http or https"},
		{"replica url no host", "pools:\n  - name: large\n    replicas:\n      - name: r\n        url: http://\n" + authOff, nil, "has no host"},
		{"replica url with path", "pools:\n  - name: large\n    replicas:\n      - url: http://a:1/v1\n" + authOff, nil, "must be a base URL without a path"},
		{"replica url with query", "pools:\n  - name: large\n    replicas:\n      - url: http://a:1?x=1\n" + authOff, nil, "must not have a query or fragment"},
		{"replica url with credentials", "pools:\n  - name: large\n    replicas:\n      - url: http://u:p@a:1\n" + authOff, nil, "credentials in the URL are not allowed"},
		{"replica url bad port", "pools:\n  - name: large\n    replicas:\n      - url: http://a:0\n" + authOff, nil, "has invalid port"},
		{"duplicate replica url in pool", "pools:\n  - name: large\n    replicas:\n      - name: a\n        url: http://a:1\n      - name: b\n        url: http://a:1\n" + authOff, nil, "pools[0].replicas[1].url: duplicate replica"},
		{"duplicate replica name across pools", "pools:\n  - name: a\n    replicas:\n      - name: r0\n        url: http://a:1\n  - name: b\n    replicas:\n      - name: r0\n        url: http://b:1\ndefault_pool: a\n" + authOff, nil, "pools[1].replicas[0].name: \"r0\" already used by pools[0].replicas[0]"},
		{"replica name with space", "pools:\n  - name: large\n    replicas:\n      - name: \"r 0\"\n        url: http://a:1\n" + authOff, nil, "must not contain whitespace"},

		// routing
		{"default pool required with several pools", "pools:\n  - name: a\n    replicas:\n      - url: http://a:1\n  - name: b\n    replicas:\n      - url: http://b:1\n" + authOff, nil, "default_pool: required when more than one pool"},
		{"default pool unknown", withPools(authOff), map[string]string{"GATEWAY_DEFAULT_POOL": "medium"}, "default_pool: unknown pool \"medium\""},
		{"model maps to unknown pool", withPools(authOff + "models:\n  m1: medium\n"), nil, "models[\"m1\"]: unknown pool \"medium\""},
		{"empty model name", withPools(authOff + "models:\n  \"\": large\n"), nil, "models: empty model name"},

		// auth
		{"auth on without callers", withPools(""), nil, "auth.callers: at least one caller is required"},
		{"caller token too short", withPools("auth:\n  callers:\n    - name: svc\n      token: short\n"), nil, "shorter than 16 characters"},
		{"caller token whitespace", withPools("auth:\n  callers:\n    - name: svc\n      token: \"aaaaaaaa bbbbbbbbbb\"\n"), nil, "must not contain whitespace"},
		{"caller without token", withPools("auth:\n  callers:\n    - name: svc\n"), nil, "auth.callers[0]: one of token or token_env is required"},
		{"caller with both token and env", withPools("auth:\n  callers:\n    - name: svc\n      token: aaaaaaaaaaaaaaaaaaaa\n      token_env: T\n"), map[string]string{"T": "bbbbbbbbbbbbbbbbbbbb"}, "auth.callers[0]: set only one of token and token_env"},
		{"caller token env unset", withPools("auth:\n  callers:\n    - name: svc\n      token_env: GATEWAY_TOKEN_SVC\n"), nil, "auth.callers[0].token_env: environment variable GATEWAY_TOKEN_SVC is empty or unset"},
		{"caller bad name", withPools("auth:\n  callers:\n    - name: Svc!\n      token: aaaaaaaaaaaaaaaaaaaa\n"), nil, "auth.callers[0].name: \"Svc!\" must match"},
		{"duplicate caller name", withPools("auth:\n  callers:\n    - name: svc\n      token: aaaaaaaaaaaaaaaaaaaa\n    - name: svc\n      token: bbbbbbbbbbbbbbbbbbbb\n"), nil, "auth.callers[1].name: duplicate caller \"svc\""},
		{"duplicate caller token", withPools("auth:\n  callers:\n    - name: a\n      token: aaaaaaaaaaaaaaaaaaaa\n    - name: b\n      token_env: T\n"), map[string]string{"T": "aaaaaaaaaaaaaaaaaaaa"}, "token for caller \"b\" duplicates the token of caller \"a\""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.yaml), envMap(tc.env))
			if err == nil {
				t.Fatalf("Parse succeeded, want error containing %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v\nwant containing %q", err, tc.want)
			}
			if !strings.HasPrefix(err.Error(), "config: ") {
				t.Fatalf("err %q lacks the config: prefix", err)
			}
		})
	}
}

func TestValidationAccepts(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		env  map[string]string
	}{
		{"single pool, default derived", minimalYAML, nil},
		{"https replica with trailing slash", "pools:\n  - name: large\n    replicas:\n      - url: https://a:8443/\n" + authOff, nil},
		{"ephemeral ports never clash", withPools(authOff + "server:\n  listen_addr: \":0\"\n  metrics_listen_addr: \":0\"\n"), nil},
		{"distinct hosts same port", withPools(authOff + "server:\n  listen_addr: \"127.0.0.1:9000\"\n  metrics_listen_addr: \"127.0.0.2:9000\"\n"), nil},
		{"auth off ignores unset token env", withPools("auth:\n  enabled: false\n  callers:\n    - name: svc\n      token_env: UNSET\n"), nil},
		{"auth on with env token", withPools("auth:\n  callers:\n    - name: svc\n      token_env: T\n"), map[string]string{"T": "0123456789abcdef"}},
		{"body limit at bounds", withPools(authOff + "server:\n  max_request_body_bytes: 1KiB\n"), map[string]string{"GATEWAY_MAX_REQUEST_BODY_BYTES": "1GiB"}},
		{"body limit at floor", withPools(authOff + "server:\n  max_request_body_bytes: 1024\n"), nil},
		{"reload interval at floor", withPools(authOff + "discovery:\n  reload_interval: 100ms\n"), nil},
		{"header timeout zero means total", withPools(authOff + "upstream:\n  timeouts:\n    response_header: 0s\n"), nil},
		{"header timeout equal to total", withPools(authOff + "upstream:\n  timeouts:\n    response_header: 15m\n"), nil},
		{"connect retry backoff bounds", withPools(authOff + "upstream:\n  connect_retry_backoff: 0s\n"), map[string]string{"GATEWAY_UPSTREAM_CONNECT_RETRY_BACKOFF": "5s"}},
		{"stream header zero means total", withPools(authOff + "upstream:\n  timeouts:\n    stream_response_header: 0s\n"), nil},
		{"dns bounds", withPools(authOff + "upstream:\n  dns:\n    ttl: 1s\n    max_stale: 0s\n    max_hosts: 1\n"), map[string]string{"GATEWAY_UPSTREAM_DNS_MAX_HOSTS": "65536", "GATEWAY_UPSTREAM_DNS_TTL": "1h"}},
		{"auth enabled by env", withPools("auth:\n  enabled: false\n  callers:\n    - name: svc\n      token: 0123456789abcdef\n"), map[string]string{"GATEWAY_AUTH_ENABLED": "true"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Parse([]byte(tc.yaml), envMap(tc.env)); err != nil {
				t.Fatalf("Parse: %v", err)
			}
		})
	}
}

func TestValidationReportsEveryProblem(t *testing.T) {
	yaml := `
server:
  read_header_timeout: 0s
upstream:
  max_idle_conns_per_host: -1
pools:
  - name: large
    replicas:
      - url: ftp://x
models:
  m: nowhere
auth:
  callers:
    - name: svc
      token_env: MISSING
`
	_, err := Parse([]byte(yaml), envMap(nil))
	if err == nil {
		t.Fatal("Parse succeeded, want errors")
	}
	for _, want := range []string{
		"server.read_header_timeout",
		"upstream.max_idle_conns_per_host",
		"pools[0].replicas[0].url",
		`models["m"]`,
		"auth.callers[0].token_env",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %s:\n%v", want, err)
		}
	}
}

func TestDuplicateReplicaByNormalisedURL(t *testing.T) {
	tests := []struct {
		name    string
		first   string
		second  string
		wantDup bool
	}{
		{"identical", "http://a:1", "http://a:1", true},
		{"trailing slash", "http://a:1", "http://a:1/", true},
		{"several trailing slashes", "http://a:1/", "http://a:1//", true},
		{"upper-case host", "http://vllm-0:8000", "http://VLLM-0:8000", true},
		{"upper-case scheme", "http://a:1", "HTTP://a:1", true},
		{"mixed case and slash", "https://Vllm.Local:8443/", "HTTPS://vllm.local:8443", true},
		{"different port", "http://a:1", "http://a:2", false},
		{"different scheme", "http://a:1", "https://a:1", false},
		{"different host", "http://a:1", "http://b:1", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			yaml := "pools:\n  - name: large\n    replicas:\n      - name: r0\n        url: " + tc.first +
				"\n      - name: r1\n        url: " + tc.second + "\n" + authOff
			_, err := Parse([]byte(yaml), envMap(nil))
			const want = "pools[0].replicas[1].url: duplicate replica"
			switch {
			case tc.wantDup && (err == nil || !strings.Contains(err.Error(), want)):
				t.Fatalf("err = %v, want containing %q", err, want)
			case !tc.wantDup && err != nil:
				t.Fatalf("Parse: %v", err)
			}
		})
	}
}

func TestNormalizeReplicaURL(t *testing.T) {
	tests := []struct{ in, want string }{
		{"http://a:1", "http://a:1"},
		{"http://a:1/", "http://a:1"},
		{"HTTP://A:1/", "http://a:1"},
		{"https://Host.Example:8443//", "https://host.example:8443"},
		{"://bad", "://bad"},
	}
	for _, tc := range tests {
		if got := normalizeReplicaURL(tc.in); got != tc.want {
			t.Errorf("normalizeReplicaURL(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
