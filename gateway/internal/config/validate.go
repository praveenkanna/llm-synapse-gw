package config

import (
	"errors"
	"fmt"
	"maps"
	"net"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// Validation limits.
const (
	// MinTokenLength is the shortest caller token accepted, so guessable
	// tokens fail at startup.
	MinTokenLength = 16
	// MaxTokenLength is the longest caller token accepted. It bounds what
	// a presented token can make the gateway hash (auth.MaxAuthorizationBytes),
	// so a longer one could never authenticate and fails at startup.
	MaxTokenLength = 4096
	// MaxModelNameBytes is the longest model name the gateway maps to a pool:
	// the longest plausible served name, an organisation and a repository
	// or a mount path. A longer name in models fails at startup, since no
	// request could reach it; a longer one in a request is not decoded and
	// goes to the default pool (ingress).
	MaxModelNameBytes = 256
	// MaxPriorityHeaderBytes is the longest X-Priority value read. It is the
	// 63 bytes of a class name (namePattern) with ample room for surrounding
	// space; a longer value resolves as if the header were absent.
	MaxPriorityHeaderBytes = 256
	// MaxIdleConnsPerHostLimit caps upstream idle connections per replica.
	MaxIdleConnsPerHostLimit = 65536
	// MaxReplicaNameLen bounds a replica's name, which every route decision
	// hashes a few times per replica in the pool.
	MaxReplicaNameLen = 128
	// MaxPoolReplicas is the pool size AC-M1-5 is proven for, not a limit: a
	// larger pool runs, and discovery logs a warning naming it. A route
	// decision reads every replica's load, so its cost grows with the pool:
	// 64 replicas add about 5 µs to a longest walk and the ring allocates
	// nothing, past 64 it allocates on every route, and at 256 a route that
	// concurrent routes refuse everywhere passes 100 µs on its own, on an
	// Apple M1 (M1-26). Raising it means measuring AC-M1-5 again.
	MaxPoolReplicas = 64

	// MinRequestBodyBytes and MaxRequestBodyBytesLimit bound
	// server.max_request_body_bytes. The floor catches a missing unit
	// ("32" bytes would reject every real request); the ceiling keeps one
	// request from holding an unreasonable share of memory.
	MinRequestBodyBytes      ByteSize = 1 << 10
	MaxRequestBodyBytesLimit ByteSize = 1 << 30
	// MinReloadInterval bounds discovery.reload_interval so a typo ("5ms")
	// cannot turn the config poll into a busy loop.
	MinReloadInterval = 100 * time.Millisecond

	// MinDNSTTL and MaxDNSTTL bound upstream.dns.ttl: too short turns every
	// dial back into a lookup, too long keeps a moved replica's old address.
	MinDNSTTL = time.Second
	MaxDNSTTL = time.Hour
	// MaxConnectRetryBackoff caps upstream.connect_retry_backoff: the pause
	// adds to a request's latency, so keep it short.
	MaxConnectRetryBackoff = 5 * time.Second
	// MaxDNSMaxStale caps upstream.dns.max_stale.
	MaxDNSMaxStale = 24 * time.Hour
	// MaxDNSMaxHostsLimit caps upstream.dns.max_hosts.
	MaxDNSMaxHostsLimit = 65536
)

// namePattern constrains pool and caller names: they become metric label
// values and log fields, so keep them short and plain.
var namePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)

// resolve fills derived values (replica names, the single-pool default,
// default metric aliases, caller tokens from env) and reports problems it finds doing so.
func resolve(cfg *Config, getenv func(string) string) error {
	var errs []error
	for pi := range cfg.Pools {
		for ri := range cfg.Pools[pi].Replicas {
			r := &cfg.Pools[pi].Replicas[ri]
			if r.Name == "" {
				if u, err := url.Parse(r.URL); err == nil {
					r.Name = u.Host
				}
			}
		}
	}
	if cfg.Router.LoadView.MetricAliases == nil {
		// A bare `metric_aliases:` (YAML null) means the defaults, like {}.
		cfg.Router.LoadView.MetricAliases = DefaultMetricAliases()
	}
	if cfg.DefaultPool == "" && len(cfg.Pools) == 1 {
		cfg.DefaultPool = cfg.Pools[0].Name
	}
	errs = append(errs, resolveAdmin(cfg, getenv)...)
	if !cfg.Auth.Enabled {
		return errors.Join(errs...)
	}
	for i := range cfg.Auth.Callers {
		c := &cfg.Auth.Callers[i]
		key := fmt.Sprintf("auth.callers[%d]", i)
		switch {
		case c.Token.IsSet() && c.TokenEnv != "":
			errs = append(errs, fmt.Errorf("%s: set only one of token and token_env", key))
		case c.TokenEnv != "":
			v := getenv(c.TokenEnv)
			if v == "" {
				errs = append(errs, fmt.Errorf("%s.token_env: environment variable %s is empty or unset", key, c.TokenEnv))
				continue
			}
			c.Token = Secret(v)
		}
	}
	return errors.Join(errs...)
}

// Validate checks the resolved configuration and returns every problem found,
// each prefixed with the YAML key it concerns.
func (c *Config) Validate() error {
	var errs []error
	errs = append(errs, c.validateServer()...)
	errs = append(errs, c.validateDiscovery()...)
	errs = append(errs, c.validateUpstream()...)
	errs = append(errs, c.validatePools()...)
	errs = append(errs, c.validateRouting()...)
	errs = append(errs, c.validateRouter()...)
	errs = append(errs, c.validateAuth()...)
	errs = append(errs, c.validateRuntime()...)
	errs = append(errs, c.validatePriority()...)
	errs = append(errs, c.validateUpstreamPriority()...)
	errs = append(errs, c.validateAdmission()...)
	errs = append(errs, c.validateTemplates()...)
	errs = append(errs, c.validateDedup()...)
	errs = append(errs, c.validateAdmin()...)
	return errors.Join(errs...)
}

func (c *Config) validateServer() []error {
	var errs []error
	errs = append(errs, checkListenAddr("server.listen_addr", c.Server.ListenAddr)...)
	errs = append(errs, checkListenAddr("server.metrics_listen_addr", c.Server.MetricsListenAddr)...)
	if addrsConflict(c.Server.ListenAddr, c.Server.MetricsListenAddr) {
		errs = append(errs, fmt.Errorf("server.metrics_listen_addr: %q conflicts with server.listen_addr %q",
			c.Server.MetricsListenAddr, c.Server.ListenAddr))
	}
	errs = appendIfErr(errs, checkPositive("server.read_header_timeout", c.Server.ReadHeaderTimeout))
	errs = appendIfErr(errs, checkPositive("server.idle_timeout", c.Server.IdleTimeout))
	if b := c.Server.MaxRequestBodyBytes; b < MinRequestBodyBytes || b > MaxRequestBodyBytesLimit {
		errs = append(errs, fmt.Errorf("server.max_request_body_bytes: %d bytes out of range [%s, %s]",
			b, MinRequestBodyBytes, MaxRequestBodyBytesLimit))
	}
	errs = appendIfErr(errs, checkPositive("shutdown.drain_timeout", c.Shutdown.DrainTimeout))
	if c.Shutdown.PreDrainDelay < 0 {
		errs = append(errs, errors.New("shutdown.pre_drain_delay: must not be negative"))
	} else if c.Shutdown.DrainTimeout > 0 && c.Shutdown.PreDrainDelay >= c.Shutdown.DrainTimeout {
		errs = append(errs, fmt.Errorf("shutdown.pre_drain_delay: %s must be less than shutdown.drain_timeout %s", c.Shutdown.PreDrainDelay, c.Shutdown.DrainTimeout))
	}
	return errs
}

func (c *Config) validateDiscovery() []error {
	if d := c.Discovery.ReloadInterval; d < MinReloadInterval {
		return []error{fmt.Errorf("discovery.reload_interval: %s is below the minimum %s", d, MinReloadInterval)}
	}
	return nil
}

func (c *Config) validateUpstream() []error {
	var errs []error
	u := c.Upstream
	if u.MaxIdleConnsPerHost < 1 || u.MaxIdleConnsPerHost > MaxIdleConnsPerHostLimit {
		errs = append(errs, fmt.Errorf("upstream.max_idle_conns_per_host: %d out of range [1, %d]",
			u.MaxIdleConnsPerHost, MaxIdleConnsPerHostLimit))
	}
	errs = appendIfErr(errs, checkPositive("upstream.idle_conn_timeout", u.IdleConnTimeout))
	t := u.Timeouts
	if b := u.ConnectRetryBackoff; b < 0 || b > MaxConnectRetryBackoff {
		errs = append(errs, fmt.Errorf("upstream.connect_retry_backoff: %s out of range [0s, %s]", b, MaxConnectRetryBackoff))
	} else if t.Total > 0 && b > t.Total {
		errs = append(errs, fmt.Errorf("upstream.connect_retry_backoff: %s exceeds upstream.timeouts.total %s", b, t.Total))
	}
	errs = appendIfErr(errs, checkPositive("upstream.timeouts.connect", t.Connect))
	for _, h := range []struct {
		key string
		d   time.Duration
	}{
		{"upstream.timeouts.response_header", t.ResponseHeader},
		{"upstream.timeouts.stream_response_header", t.StreamResponseHeader},
		{"upstream.timeouts.first_chunk", t.FirstChunk},
	} {
		if h.d < 0 {
			errs = append(errs, fmt.Errorf("%s: must not be negative (0 means bounded by upstream.timeouts.total)", h.key))
		}
	}
	errs = appendIfErr(errs, checkPositive("upstream.timeouts.idle_between_chunks", t.IdleBetweenChunks))
	errs = appendIfErr(errs, checkPositive("upstream.timeouts.total", t.Total))
	for _, sub := range []struct {
		key string
		d   time.Duration
	}{
		{"upstream.timeouts.connect", t.Connect},
		{"upstream.timeouts.response_header", t.ResponseHeader},
		{"upstream.timeouts.stream_response_header", t.StreamResponseHeader},
		{"upstream.timeouts.first_chunk", t.FirstChunk},
		{"upstream.timeouts.idle_between_chunks", t.IdleBetweenChunks},
	} {
		if t.Total > 0 && sub.d > t.Total {
			errs = append(errs, fmt.Errorf("%s: %s exceeds upstream.timeouts.total %s", sub.key, sub.d, t.Total))
		}
	}
	errs = append(errs, validateDNS(u.DNS, t.Total)...)
	if strings.ContainsFunc(u.APIKey.Reveal(), unicode.IsSpace) {
		errs = append(errs, errors.New("upstream.api_key: must not contain whitespace"))
	}
	return errs
}

func validateDNS(d UpstreamDNS, total time.Duration) []error {
	var errs []error
	if d.TTL < MinDNSTTL || d.TTL > MaxDNSTTL {
		errs = append(errs, fmt.Errorf("upstream.dns.ttl: %s out of range [%s, %s]", d.TTL, MinDNSTTL, MaxDNSTTL))
	}
	if d.MaxStale < 0 || d.MaxStale > MaxDNSMaxStale {
		errs = append(errs, fmt.Errorf("upstream.dns.max_stale: %s out of range [0s, %s]", d.MaxStale, MaxDNSMaxStale))
	}
	errs = appendIfErr(errs, checkPositive("upstream.dns.lookup_timeout", d.LookupTimeout))
	if total > 0 && d.LookupTimeout > total {
		errs = append(errs, fmt.Errorf("upstream.dns.lookup_timeout: %s exceeds upstream.timeouts.total %s", d.LookupTimeout, total))
	}
	if d.MaxHosts < 1 || d.MaxHosts > MaxDNSMaxHostsLimit {
		errs = append(errs, fmt.Errorf("upstream.dns.max_hosts: %d out of range [1, %d]", d.MaxHosts, MaxDNSMaxHostsLimit))
	}
	return errs
}

func (c *Config) validatePools() []error {
	if len(c.Pools) == 0 {
		return []error{fmt.Errorf("pools: at least one pool is required (set %s to a YAML file; see deploy/compose/config/gateway.example.yaml)", EnvConfigPath)}
	}
	var errs []error
	poolNames := make(map[string]bool, len(c.Pools))
	replicaNames := make(map[string]string)
	for pi, p := range c.Pools {
		key := fmt.Sprintf("pools[%d]", pi)
		switch {
		case !namePattern.MatchString(p.Name):
			errs = append(errs, fmt.Errorf("%s.name: %q must match %s", key, p.Name, namePattern))
		case poolNames[p.Name]:
			errs = append(errs, fmt.Errorf("%s.name: duplicate pool %q", key, p.Name))
		}
		poolNames[p.Name] = true
		if len(p.Replicas) == 0 {
			errs = append(errs, fmt.Errorf("%s.replicas: pool %q needs at least one replica", key, p.Name))
		}
		urls := make(map[string]string, len(p.Replicas)) // normalised URL -> first spelling
		for ri, r := range p.Replicas {
			rkey := fmt.Sprintf("%s.replicas[%d]", key, ri)
			errs = appendIfErr(errs, checkReplicaURL(rkey+".url", r.URL))
			norm := normalizeReplicaURL(r.URL)
			if prev, dup := urls[norm]; dup {
				errs = append(errs, fmt.Errorf("%s.url: duplicate replica %q in pool %q (same as %q)", rkey, r.URL, p.Name, prev))
			} else {
				urls[norm] = r.URL
			}
			errs = appendIfErr(errs, checkReplicaName(rkey+".name", r.Name))
			if prev, dup := replicaNames[r.Name]; dup && r.Name != "" {
				errs = append(errs, fmt.Errorf("%s.name: %q already used by %s", rkey, r.Name, prev))
			} else {
				replicaNames[r.Name] = rkey
			}
		}
	}
	return errs
}

func (c *Config) validateRouting() []error {
	var errs []error
	pools := make(map[string]bool, len(c.Pools))
	for _, p := range c.Pools {
		pools[p.Name] = true
	}
	switch {
	case c.DefaultPool == "" && len(c.Pools) > 1:
		errs = append(errs, errors.New("default_pool: required when more than one pool is configured"))
	case c.DefaultPool != "" && !pools[c.DefaultPool]:
		errs = append(errs, fmt.Errorf("default_pool: unknown pool %q", c.DefaultPool))
	}
	for _, model := range slices.Sorted(maps.Keys(c.Models)) {
		pool := c.Models[model]
		if strings.TrimSpace(model) == "" {
			errs = append(errs, errors.New("models: empty model name"))
			continue
		}
		if len(model) > MaxModelNameBytes {
			errs = append(errs, fmt.Errorf("models[%.32q...]: longer than %d bytes, so no request could reach it", model, MaxModelNameBytes))
			continue
		}
		if !pools[pool] {
			errs = append(errs, fmt.Errorf("models[%q]: unknown pool %q", model, pool))
		}
	}
	return errs
}

func (c *Config) validateAuth() []error {
	var errs []error
	if c.Auth.Enabled && len(c.Auth.Callers) == 0 {
		errs = append(errs, errors.New("auth.callers: at least one caller is required when auth.enabled is true (set GATEWAY_AUTH_ENABLED=false for local dev)"))
	}
	names := make(map[string]bool, len(c.Auth.Callers))
	tokens := make(map[Secret]string, len(c.Auth.Callers))
	for i, cl := range c.Auth.Callers {
		key := fmt.Sprintf("auth.callers[%d]", i)
		switch {
		case !namePattern.MatchString(cl.Name):
			errs = append(errs, fmt.Errorf("%s.name: %q must match %s", key, cl.Name, namePattern))
		case names[cl.Name]:
			errs = append(errs, fmt.Errorf("%s.name: duplicate caller %q", key, cl.Name))
		}
		names[cl.Name] = true
		if !c.Auth.Enabled {
			continue
		}
		if !cl.Token.IsSet() {
			if cl.TokenEnv == "" {
				errs = append(errs, fmt.Errorf("%s: one of token or token_env is required", key))
			}
			continue // an empty token_env was reported by resolve
		}
		tok := cl.Token.Reveal()
		if len(tok) < MinTokenLength {
			errs = append(errs, fmt.Errorf("%s: token for caller %q is shorter than %d characters", key, cl.Name, MinTokenLength))
		}
		if len(tok) > MaxTokenLength {
			errs = append(errs, fmt.Errorf("%s: token for caller %q is longer than %d characters", key, cl.Name, MaxTokenLength))
		}
		if strings.ContainsFunc(tok, unicode.IsSpace) {
			errs = append(errs, fmt.Errorf("%s: token for caller %q must not contain whitespace", key, cl.Name))
		}
		if prev, dup := tokens[cl.Token]; dup {
			errs = append(errs, fmt.Errorf("%s: token for caller %q duplicates the token of caller %q", key, cl.Name, prev))
		} else {
			tokens[cl.Token] = cl.Name
		}
	}
	return errs
}

func checkListenAddr(key, addr string) []error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return []error{fmt.Errorf("%s: %q is not host:port: %w", key, addr, err)}
	}
	var errs []error
	if n, err := strconv.Atoi(port); err != nil || n < 0 || n > 65535 {
		errs = append(errs, fmt.Errorf("%s: %q has invalid port %q", key, addr, port))
	}
	if host != "" && strings.ContainsFunc(host, unicode.IsSpace) {
		errs = append(errs, fmt.Errorf("%s: %q has invalid host", key, addr))
	}
	return errs
}

// addrsConflict reports whether two listen addresses would bind the same
// port on overlapping interfaces. Port 0 (ephemeral) never conflicts.
func addrsConflict(a, b string) bool {
	ah, ap, errA := net.SplitHostPort(a)
	bh, bp, errB := net.SplitHostPort(b)
	if errA != nil || errB != nil || ap != bp || ap == "0" {
		return false
	}
	return ah == bh || isWildcard(ah) || isWildcard(bh)
}

func isWildcard(host string) bool {
	return host == "" || host == "0.0.0.0" || host == "::"
}

func checkReplicaURL(key, raw string) error {
	if raw == "" {
		return fmt.Errorf("%s: required", key)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("%s: %q is not a valid URL: %w", key, raw, err)
	}
	switch {
	case u.Scheme != "http" && u.Scheme != "https":
		return fmt.Errorf("%s: %q must use http or https", key, raw)
	case u.Host == "" || u.Hostname() == "":
		return fmt.Errorf("%s: %q has no host", key, raw)
	case u.User != nil:
		return fmt.Errorf("%s: credentials in the URL are not allowed; use upstream.api_key", key)
	case u.RawQuery != "" || u.Fragment != "":
		return fmt.Errorf("%s: %q must not have a query or fragment", key, raw)
	case u.Path != "" && u.Path != "/":
		return fmt.Errorf("%s: %q must be a base URL without a path (the gateway appends /v1/...)", key, raw)
	}
	if p := u.Port(); p != "" {
		if n, err := strconv.Atoi(p); err != nil || n < 1 || n > 65535 {
			return fmt.Errorf("%s: %q has invalid port %q", key, raw, p)
		}
	}
	return nil
}

// normalizeReplicaURL returns the form used to detect duplicate replicas:
// scheme and host lower-cased (both are case-insensitive) and trailing "/"
// trimmed. A URL that does not parse is returned trimmed as is; its error is
// reported by checkReplicaURL.
func normalizeReplicaURL(raw string) string {
	trimmed := strings.TrimRight(raw, "/")
	u, err := url.Parse(trimmed)
	if err != nil {
		return trimmed
	}
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)
	return u.String()
}

func checkReplicaName(key, name string) error {
	switch {
	case name == "":
		return fmt.Errorf("%s: required (could not derive one from the URL)", key)
	case len(name) > MaxReplicaNameLen:
		return fmt.Errorf("%s: longer than %d characters", key, MaxReplicaNameLen)
	case strings.ContainsFunc(name, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }):
		return fmt.Errorf("%s: %q must not contain whitespace or control characters", key, name)
	}
	return nil
}

func checkPositive(key string, d time.Duration) error {
	if d <= 0 {
		return fmt.Errorf("%s: must be greater than zero, got %s", key, d)
	}
	return nil
}

func appendIfErr(errs []error, err error) []error {
	if err != nil {
		return append(errs, err)
	}
	return errs
}

func (c *Config) validatePriority() []error {
	return append(c.Priority.validate(), validatePriorityProbe(c.Priority.Probe)...)
}

// validatePriorityProbe checks priority.probe. It is not part of
// PriorityConfig.validate: UpdatePriority ignores the probe settings.
func validatePriorityProbe(p PriorityProbe) []error {
	var errs []error
	if p.Interval < MinPriorityProbeInterval || p.Interval > MaxPriorityProbeInterval {
		errs = append(errs, fmt.Errorf("priority.probe.interval: %s out of range [%s, %s] (default %s)", p.Interval, MinPriorityProbeInterval, MaxPriorityProbeInterval, DefaultPriorityProbeInterval))
	}
	if p.Timeout < MinPriorityProbeTimeout || p.Timeout > MaxPriorityProbeTimeout {
		errs = append(errs, fmt.Errorf("priority.probe.timeout: %s out of range [%s, %s] (default %s)", p.Timeout, MinPriorityProbeTimeout, MaxPriorityProbeTimeout, DefaultPriorityProbeTimeout))
	} else if p.Timeout > p.Interval {
		errs = append(errs, fmt.Errorf("priority.probe.timeout: %s exceeds priority.probe.interval %s", p.Timeout, p.Interval))
	}
	return errs
}

func (p PriorityConfig) validate() []error {
	var errs []error
	if len(p.Classes) == 0 {
		return []error{errors.New("priority.classes: at least one class is required")}
	}
	classSet := make(map[string]bool, len(p.Classes))
	for i, cls := range p.Classes {
		key := fmt.Sprintf("priority.classes[%d]", i)
		switch {
		case !namePattern.MatchString(cls):
			errs = append(errs, fmt.Errorf("%s: %q must match %s (class names are lowercase)", key, cls, namePattern))
		case classSet[cls]:
			errs = append(errs, fmt.Errorf("%s: duplicate class %q", key, cls))
		}
		classSet[cls] = true
	}
	defErr := checkClassRef(classSet, "priority.default_class", p.DefaultClass, true)
	errs = appendIfErr(errs, defErr)
	for _, m := range []struct{ key, class string }{
		{"priority.anonymous_max_class", p.AnonymousMaxClass},
		{"priority.default_max_class", p.DefaultMaxClass},
	} {
		if err := checkClassRef(classSet, m.key, m.class, true); err != nil {
			errs = append(errs, err)
			continue
		}
		if defErr == nil && p.ranksAbove(p.DefaultClass, m.class) {
			errs = append(errs, fmt.Errorf("priority.default_class: %q has higher priority than %s %q; lower default_class or raise the maximum", p.DefaultClass, m.key, m.class))
		}
	}
	for name, pol := range p.Callers {
		errs = append(errs, p.validateCallerPolicy(classSet, name, pol)...)
	}
	return errs
}

// ranksAbove reports whether class a has a higher priority than class b.
// Both must be configured classes.
func (p PriorityConfig) ranksAbove(a, b string) bool {
	ra, okA := p.ClassRank(a)
	rb, okB := p.ClassRank(b)
	return okA && okB && ra < rb
}

// validateCallerPolicy checks one priority.callers entry: known classes, and
// a default that its effective maximum (max, else default_max_class) would
// not silently clamp.
func (p PriorityConfig) validateCallerPolicy(classSet map[string]bool, name string, pol CallerPolicy) []error {
	var errs []error
	key := fmt.Sprintf("priority.callers[%s]", name)
	if !namePattern.MatchString(name) {
		errs = append(errs, fmt.Errorf("%s: caller name %q must match %s", key, name, namePattern))
	}
	if name == AnonymousCallerName {
		errs = append(errs, fmt.Errorf("%s: %q is the caller name used with auth off, where caller policies do not apply; use priority.anonymous_max_class", key, name))
	}
	defErr := checkClassRef(classSet, key+".default", pol.Default, false)
	maxErr := checkClassRef(classSet, key+".max", pol.Max, false)
	errs = appendIfErr(appendIfErr(errs, defErr), maxErr)
	if pol.Default == "" || defErr != nil || maxErr != nil {
		return errs
	}
	maxClass, maxKey := pol.Max, "max"
	if maxClass == "" {
		maxClass, maxKey = p.DefaultMaxClass, "priority.default_max_class"
	}
	if p.ranksAbove(pol.Default, maxClass) {
		errs = append(errs, fmt.Errorf("%s: default class %q has higher priority than %s %q", key, pol.Default, maxKey, maxClass))
	}
	return errs
}

// checkClassRef reports a class reference (key: value) that is not one of
// the configured classes. Empty is an error only when required.
func checkClassRef(classSet map[string]bool, key, value string, required bool) error {
	switch {
	case value == "" && required:
		return fmt.Errorf("%s: required", key)
	case value == "" || classSet[value]:
		return nil
	case classSet[strings.ToLower(value)]:
		return fmt.Errorf("%s: %q is not in priority.classes (class names are lowercase)", key, value)
	default:
		return fmt.Errorf("%s: %q is not in priority.classes", key, value)
	}
}
