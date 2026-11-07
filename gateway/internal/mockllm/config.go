// Package mockllm is an OpenAI-wire-compatible mock of a vLLM replica for
// GPU-less development and tests (ADR 0010).
//
// It serves /v1/chat/completions (streaming and not), /v1/completions,
// /v1/models, /tokenize, /health and a vLLM-named /metrics. Prompt tokens are
// counted with a deterministic stand-in tokenizer (see Tokenize and
// RenderChat), so /tokenize and usage.prompt_tokens agree, and vLLM's
// automatic prefix caching is emulated over those tokens (MOCKLLM_PREFIX_CACHE_BLOCKS)
// so vllm:prefix_cache_hits_total reflects shared prefixes. Timing (TTFT, tokens/s),
// output length and failure injection (connect refuse, 5xx, mid-stream drop,
// hang, mid-stream stall) are configurable. It counts requests cancelled by the
// client and keeps a bounded capture of received request bytes so tests can
// assert byte-exact forwarding.
//
// Test and harness control lives under /mock/ and in request headers that
// real vLLM ignores:
//
//	X-Mock-Fault: none|5xx|drop|hang|stall   force a fault for this request
//	X-Mock-Output-Tokens: N                  force the output length
//	X-Mock-TTFT-Ms: N                        force the time to first token
//
//	GET    /mock/stats          counters as JSON
//	GET    /mock/requests       captured requests (body base64 in JSON)
//	DELETE /mock/requests       clear the capture
//	POST   /mock/refuse?for=2s  close the listener (connection refused) for a while
//	POST   /reset_prefix_cache  drop every cached prefix block (vLLM's dev-mode endpoint)
//
// MOCKLLM_VLLM_COMPAT=current|legacy picks which vLLM release family the mock
// imitates where vLLM changed its wire or metric names (ADR 0011): the error
// body shape and the KV-cache / prefix-cache metric names. See VLLMCompat.
//
// MOCKLLM_SCHEDULING_POLICY=fcfs|priority mirrors vLLM's --scheduling-policy.
// Under fcfs a request whose top-level "priority" is non-zero gets 400, as
// vLLM answers; under priority, requests waiting for a MOCKLLM_MAX_NUM_SEQS
// slot are admitted lowest "priority" value first, ties in arrival order.
// /mock/stats counts requests that carried a non-zero priority.
//
// Configuration comes from MOCKLLM_* environment variables; see FromEnv.
package mockllm

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"
)

// Environment variable names read by FromEnv.
const (
	EnvListenAddr        = "MOCKLLM_LISTEN_ADDR"
	EnvModels            = "MOCKLLM_MODELS"
	EnvMaxModelLen       = "MOCKLLM_MAX_MODEL_LEN"
	EnvTTFT              = "MOCKLLM_TTFT"
	EnvTokensPerSecond   = "MOCKLLM_TOKENS_PER_SECOND"
	EnvOutputTokens      = "MOCKLLM_OUTPUT_TOKENS"
	EnvMaxNumSeqs        = "MOCKLLM_MAX_NUM_SEQS"
	EnvFault5xxRate      = "MOCKLLM_FAULT_5XX_RATE"
	EnvFaultDropRate     = "MOCKLLM_FAULT_DROP_RATE"
	EnvFaultHangRate     = "MOCKLLM_FAULT_HANG_RATE"
	EnvFaultStallRate    = "MOCKLLM_FAULT_STALL_RATE"
	EnvErrorStatus       = "MOCKLLM_ERROR_STATUS"
	EnvCaptureEntries    = "MOCKLLM_CAPTURE_ENTRIES"
	EnvCaptureMaxBytes   = "MOCKLLM_CAPTURE_MAX_BYTES"
	EnvMaxBodyBytes      = "MOCKLLM_MAX_BODY_BYTES"
	EnvControlEnabled    = "MOCKLLM_CONTROL_ENABLED"
	EnvSeed              = "MOCKLLM_SEED"
	EnvVLLMCompat        = "MOCKLLM_VLLM_COMPAT"
	EnvSchedulingPolicy  = "MOCKLLM_SCHEDULING_POLICY"
	EnvPrefixCacheBlocks = "MOCKLLM_PREFIX_CACHE_BLOCKS"
)

// SchedulingPolicy mirrors vLLM's --scheduling-policy: the order in which
// requests waiting for a MaxNumSeqs slot are admitted, and whether the
// request's "priority" field is accepted.
type SchedulingPolicy string

const (
	// PolicyFCFS admits waiters in arrival order and, like vLLM, rejects a
	// non-zero "priority" with 400.
	PolicyFCFS SchedulingPolicy = "fcfs"
	// PolicyPriority admits the waiter with the lowest "priority" value
	// first, ties in arrival order.
	PolicyPriority SchedulingPolicy = "priority"
)

func (p SchedulingPolicy) valid() bool { return p == PolicyFCFS || p == PolicyPriority }

// VLLMCompat selects which vLLM release family mockllm imitates where vLLM
// changed its output between releases (ADR 0011). Tests run the gateway and
// harness against both so neither depends on one pinned vLLM version.
type VLLMCompat string

const (
	// CompatCurrent imitates current vLLM: error bodies are
	// {"error":{"message","type","param","code"}} and the metrics are
	// vllm:kv_cache_usage_perc and vllm:prefix_cache_{queries,hits}_total.
	CompatCurrent VLLMCompat = "current"
	// CompatLegacy imitates older vLLM: error bodies are
	// {"object":"error","message","type","param","code"} and the metrics are
	// vllm:gpu_cache_usage_perc and vllm:gpu_prefix_cache_{queries,hits}_total.
	CompatLegacy VLLMCompat = "legacy"
)

func (v VLLMCompat) valid() bool { return v == CompatCurrent || v == CompatLegacy }

// Defaults. They model a small, fast replica so that tests run quickly; the
// harness overrides them per profile.
const (
	DefaultListenAddr        = ":8000"
	DefaultModel             = "mock-model"
	DefaultMaxModelLen       = 8192
	DefaultTTFT              = 50 * time.Millisecond
	DefaultTokensPerSecond   = 50.0
	DefaultOutputTokens      = "uniform:16:128"
	DefaultErrorStatus       = 500
	DefaultCaptureEntries    = 64
	DefaultCaptureMaxBytes   = 256 << 10
	DefaultMaxBodyBytes      = 8 << 20
	DefaultVLLMCompat        = CompatCurrent
	DefaultSchedulingPolicy  = PolicyFCFS
	DefaultPrefixCacheBlocks = 32768

	// MaxPrefixCacheBlocks bounds the prefix cache (about 100 bytes a block)
	// whatever the config says.
	MaxPrefixCacheBlocks = 1 << 22
	// MaxChoices bounds the "n" request field so one request cannot make the
	// mock allocate without limit.
	MaxChoices = 16
	// maxOutputTokens bounds any sampled or forced output length.
	maxOutputTokens = 1 << 16
	// maxCaptureEntries bounds the capture ring whatever the config says.
	maxCaptureEntries = 4096
)

// Config holds every mockllm tunable.
type Config struct {
	// ListenAddr is the TCP address to listen on (":0" picks a free port).
	ListenAddr string
	// Models are the model names served; the first one is the default in /v1/models order.
	Models []string
	// MaxModelLen is reported by /v1/models.
	MaxModelLen int
	// TTFT is the delay before the response (non-stream) or the first token (stream).
	TTFT time.Duration
	// TokensPerSecond is the decode rate after the first token; <= 0 means no delay.
	TokensPerSecond float64
	// OutputTokens is the output length distribution per choice.
	OutputTokens LengthDist
	// MaxNumSeqs limits concurrently running requests like vLLM's
	// --max-num-seqs; extra requests wait. 0 means unlimited.
	MaxNumSeqs int
	// SchedulingPolicy orders the requests waiting for a MaxNumSeqs slot
	// and decides whether a non-zero "priority" is accepted.
	SchedulingPolicy SchedulingPolicy
	// Faults are the per-request fault probabilities.
	Faults FaultRates
	// ErrorStatus is the HTTP status returned by the 5xx fault.
	ErrorStatus int
	// CaptureEntries is how many received requests are kept (ring buffer); 0 disables capture.
	CaptureEntries int
	// CaptureMaxBytes caps the stored body bytes per captured request.
	CaptureMaxBytes int
	// MaxBodyBytes caps request bodies; larger ones get 413.
	MaxBodyBytes int64
	// ControlEnabled exposes the /mock/ control endpoints.
	ControlEnabled bool
	// Seed makes sampling deterministic; 0 picks a random seed.
	Seed uint64
	// VLLMCompat picks the error body shape and metric-name set.
	VLLMCompat VLLMCompat
	// PrefixCacheBlocks is the per-model prefix-cache capacity in
	// PrefixBlockTokens-token blocks; 0 disables prefix caching (no hits).
	PrefixCacheBlocks int
}

// FaultRates are probabilities in [0,1]; their sum must not exceed 1.
type FaultRates struct {
	Error5xx float64 // respond with ErrorStatus before any token
	Drop     float64 // cut the connection mid-response
	Hang     float64 // never respond; wait for the client to give up
	Stall    float64 // stream some tokens, then stop sending without closing
}

func (f FaultRates) sum() float64 { return f.Error5xx + f.Drop + f.Hang + f.Stall }

// Defaults returns the default configuration.
func Defaults() Config {
	dist, err := ParseLengthDist(DefaultOutputTokens)
	if err != nil {
		panic(fmt.Sprintf("mockllm: invalid default output distribution: %v", err))
	}
	return Config{
		ListenAddr:        DefaultListenAddr,
		Models:            []string{DefaultModel},
		MaxModelLen:       DefaultMaxModelLen,
		TTFT:              DefaultTTFT,
		TokensPerSecond:   DefaultTokensPerSecond,
		OutputTokens:      dist,
		ErrorStatus:       DefaultErrorStatus,
		CaptureEntries:    DefaultCaptureEntries,
		CaptureMaxBytes:   DefaultCaptureMaxBytes,
		MaxBodyBytes:      DefaultMaxBodyBytes,
		ControlEnabled:    true,
		VLLMCompat:        DefaultVLLMCompat,
		SchedulingPolicy:  DefaultSchedulingPolicy,
		PrefixCacheBlocks: DefaultPrefixCacheBlocks,
	}
}

// Validate reports the first invalid field.
func (c Config) Validate() error {
	var errs []error
	if c.ListenAddr == "" {
		errs = append(errs, errors.New("listen address is empty"))
	}
	if len(c.Models) == 0 {
		errs = append(errs, errors.New("at least one model is required"))
	}
	for _, m := range c.Models {
		if strings.TrimSpace(m) == "" {
			errs = append(errs, errors.New("model names must be non-empty"))
			break
		}
	}
	if c.MaxModelLen < 1 {
		errs = append(errs, fmt.Errorf("max model len %d must be >= 1", c.MaxModelLen))
	}
	if c.TTFT < 0 {
		errs = append(errs, fmt.Errorf("ttft %s must be >= 0", c.TTFT))
	}
	if math.IsNaN(c.TokensPerSecond) || math.IsInf(c.TokensPerSecond, 0) {
		errs = append(errs, errors.New("tokens per second must be finite"))
	}
	if err := c.OutputTokens.validate(); err != nil {
		errs = append(errs, fmt.Errorf("output tokens: %w", err))
	}
	if c.MaxNumSeqs < 0 {
		errs = append(errs, fmt.Errorf("max num seqs %d must be >= 0", c.MaxNumSeqs))
	}
	for name, r := range map[string]float64{
		"5xx": c.Faults.Error5xx, "drop": c.Faults.Drop, "hang": c.Faults.Hang, "stall": c.Faults.Stall,
	} {
		if !(r >= 0 && r <= 1) {
			errs = append(errs, fmt.Errorf("fault rate %s=%v must be in [0,1]", name, r))
		}
	}
	if c.Faults.sum() > 1+1e-9 {
		errs = append(errs, fmt.Errorf("fault rates sum to %v, must be <= 1", c.Faults.sum()))
	}
	if c.ErrorStatus < 500 || c.ErrorStatus > 599 {
		errs = append(errs, fmt.Errorf("error status %d must be 5xx", c.ErrorStatus))
	}
	if c.CaptureEntries < 0 || c.CaptureEntries > maxCaptureEntries {
		errs = append(errs, fmt.Errorf("capture entries %d must be in [0,%d]", c.CaptureEntries, maxCaptureEntries))
	}
	if c.CaptureMaxBytes < 0 {
		errs = append(errs, fmt.Errorf("capture max bytes %d must be >= 0", c.CaptureMaxBytes))
	}
	if c.MaxBodyBytes < 1 {
		errs = append(errs, fmt.Errorf("max body bytes %d must be >= 1", c.MaxBodyBytes))
	}
	if !c.VLLMCompat.valid() {
		errs = append(errs, fmt.Errorf("vllm compat %q must be %q or %q", c.VLLMCompat, CompatCurrent, CompatLegacy))
	}
	if !c.SchedulingPolicy.valid() {
		errs = append(errs, fmt.Errorf("scheduling policy %q must be %q or %q", c.SchedulingPolicy, PolicyFCFS, PolicyPriority))
	}
	if c.PrefixCacheBlocks < 0 || c.PrefixCacheBlocks > MaxPrefixCacheBlocks {
		errs = append(errs, fmt.Errorf("prefix cache blocks %d must be in [0,%d]", c.PrefixCacheBlocks, MaxPrefixCacheBlocks))
	}
	return errors.Join(errs...)
}

// FromEnv builds a Config from Defaults overridden by MOCKLLM_* variables.
func FromEnv(getenv func(string) string) (Config, error) {
	c := Defaults()
	p := envParser{getenv: getenv}

	p.str(EnvListenAddr, &c.ListenAddr)
	if v := getenv(EnvModels); v != "" {
		c.Models = splitList(v)
	}
	p.integer(EnvMaxModelLen, &c.MaxModelLen)
	p.duration(EnvTTFT, &c.TTFT)
	p.float(EnvTokensPerSecond, &c.TokensPerSecond)
	if v := getenv(EnvOutputTokens); v != "" {
		d, err := ParseLengthDist(v)
		if err != nil {
			p.errs = append(p.errs, fmt.Errorf("%s: %w", EnvOutputTokens, err))
		} else {
			c.OutputTokens = d
		}
	}
	p.integer(EnvMaxNumSeqs, &c.MaxNumSeqs)
	p.float(EnvFault5xxRate, &c.Faults.Error5xx)
	p.float(EnvFaultDropRate, &c.Faults.Drop)
	p.float(EnvFaultHangRate, &c.Faults.Hang)
	p.float(EnvFaultStallRate, &c.Faults.Stall)
	p.integer(EnvErrorStatus, &c.ErrorStatus)
	p.integer(EnvCaptureEntries, &c.CaptureEntries)
	p.integer(EnvCaptureMaxBytes, &c.CaptureMaxBytes)
	p.int64(EnvMaxBodyBytes, &c.MaxBodyBytes)
	p.boolean(EnvControlEnabled, &c.ControlEnabled)
	p.uint64(EnvSeed, &c.Seed)
	p.integer(EnvPrefixCacheBlocks, &c.PrefixCacheBlocks)
	if v, ok := p.lookup(EnvVLLMCompat); ok {
		if mode := VLLMCompat(strings.ToLower(v)); mode.valid() {
			c.VLLMCompat = mode
		} else {
			p.fail(EnvVLLMCompat, v, fmt.Errorf("want %q or %q", CompatCurrent, CompatLegacy))
		}
	}
	if v, ok := p.lookup(EnvSchedulingPolicy); ok {
		if policy := SchedulingPolicy(strings.ToLower(v)); policy.valid() {
			c.SchedulingPolicy = policy
		} else {
			p.fail(EnvSchedulingPolicy, v, fmt.Errorf("want %q or %q", PolicyFCFS, PolicyPriority))
		}
	}

	if err := errors.Join(p.errs...); err != nil {
		return Config{}, err
	}
	if err := c.Validate(); err != nil {
		return Config{}, fmt.Errorf("invalid mockllm config: %w", err)
	}
	return c, nil
}

func splitList(v string) []string {
	var out []string
	for _, s := range strings.Split(v, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// envParser collects parse errors so FromEnv reports all of them at once.
type envParser struct {
	getenv func(string) string
	errs   []error
}

func (p *envParser) lookup(key string) (string, bool) {
	v := strings.TrimSpace(p.getenv(key))
	return v, v != ""
}

func (p *envParser) fail(key, v string, err error) {
	p.errs = append(p.errs, fmt.Errorf("%s=%q: %w", key, v, err))
}

func (p *envParser) str(key string, dst *string) {
	if v, ok := p.lookup(key); ok {
		*dst = v
	}
}

func (p *envParser) integer(key string, dst *int) {
	if v, ok := p.lookup(key); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			p.fail(key, v, err)
			return
		}
		*dst = n
	}
}

func (p *envParser) int64(key string, dst *int64) {
	if v, ok := p.lookup(key); ok {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			p.fail(key, v, err)
			return
		}
		*dst = n
	}
}

func (p *envParser) uint64(key string, dst *uint64) {
	if v, ok := p.lookup(key); ok {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			p.fail(key, v, err)
			return
		}
		*dst = n
	}
}

func (p *envParser) float(key string, dst *float64) {
	if v, ok := p.lookup(key); ok {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			p.fail(key, v, err)
			return
		}
		*dst = f
	}
}

func (p *envParser) duration(key string, dst *time.Duration) {
	if v, ok := p.lookup(key); ok {
		d, err := time.ParseDuration(v)
		if err != nil {
			p.fail(key, v, err)
			return
		}
		*dst = d
	}
}

func (p *envParser) boolean(key string, dst *bool) {
	if v, ok := p.lookup(key); ok {
		b, err := strconv.ParseBool(v)
		if err != nil {
			p.fail(key, v, err)
			return
		}
		*dst = b
	}
}

// DistKind names an output length distribution.
type DistKind string

// Supported distributions.
const (
	DistFixed   DistKind = "fixed"   // always A tokens
	DistUniform DistKind = "uniform" // uniform integer in [A, B]
	DistNormal  DistKind = "normal"  // normal(mean A, stddev B), rounded, clamped to >= 1
)

// LengthDist is an output length distribution in tokens. It parses from
// "fixed:N", "uniform:MIN:MAX" or "normal:MEAN:STDDEV".
type LengthDist struct {
	Kind DistKind
	A, B float64
}

// ParseLengthDist parses a distribution spec.
func ParseLengthDist(spec string) (LengthDist, error) {
	parts := strings.Split(strings.TrimSpace(spec), ":")
	kind := DistKind(strings.ToLower(parts[0]))
	want := map[DistKind]int{DistFixed: 1, DistUniform: 2, DistNormal: 2}
	n, ok := want[kind]
	if !ok {
		return LengthDist{}, fmt.Errorf("unknown distribution %q (want fixed, uniform or normal)", parts[0])
	}
	if len(parts)-1 != n {
		return LengthDist{}, fmt.Errorf("%s takes %d parameter(s), got %d", kind, n, len(parts)-1)
	}
	vals := make([]float64, 2)
	for i := 0; i < n; i++ {
		f, err := strconv.ParseFloat(parts[i+1], 64)
		if err != nil {
			return LengthDist{}, fmt.Errorf("%s parameter %q: %w", kind, parts[i+1], err)
		}
		vals[i] = f
	}
	d := LengthDist{Kind: kind, A: vals[0], B: vals[1]}
	if err := d.validate(); err != nil {
		return LengthDist{}, err
	}
	return d, nil
}

// String renders the distribution in the form ParseLengthDist accepts.
func (d LengthDist) String() string {
	if d.Kind == DistFixed {
		return fmt.Sprintf("fixed:%g", d.A)
	}
	return fmt.Sprintf("%s:%g:%g", d.Kind, d.A, d.B)
}

func (d LengthDist) validate() error {
	if math.IsNaN(d.A) || math.IsNaN(d.B) || math.IsInf(d.A, 0) || math.IsInf(d.B, 0) {
		return errors.New("parameters must be finite")
	}
	switch d.Kind {
	case DistFixed:
		if d.A < 1 || d.A > maxOutputTokens {
			return fmt.Errorf("fixed length %g must be in [1,%d]", d.A, maxOutputTokens)
		}
	case DistUniform:
		if d.A < 1 || d.B < d.A || d.B > maxOutputTokens {
			return fmt.Errorf("uniform range [%g,%g] must satisfy 1 <= min <= max <= %d", d.A, d.B, maxOutputTokens)
		}
	case DistNormal:
		if d.A < 1 || d.A > maxOutputTokens || d.B < 0 {
			return fmt.Errorf("normal mean %g must be in [1,%d] and stddev %g >= 0", d.A, maxOutputTokens, d.B)
		}
	default:
		return fmt.Errorf("unknown distribution %q", d.Kind)
	}
	return nil
}

// Sample draws one length in [1, maxOutputTokens].
func (d LengthDist) Sample(r *rand.Rand) int {
	var v float64
	switch d.Kind {
	case DistUniform:
		lo, hi := int(d.A), int(d.B)
		return clampTokens(lo + r.IntN(hi-lo+1))
	case DistNormal:
		v = d.A + d.B*r.NormFloat64()
	default:
		v = d.A
	}
	return clampTokens(int(math.Round(v)))
}

func clampTokens(n int) int {
	return max(1, min(n, maxOutputTokens))
}
