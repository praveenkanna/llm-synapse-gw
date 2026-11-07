// Package loadview tracks per-replica load, scrapes upstream vLLM metrics,
// and probes replica capabilities.
//
// It maintains:
//   - An authoritative, zero-lock in-flight request counter per replica.
//   - Periodic scraping of vLLM /metrics with an alias table for version-agnostic
//     signal extraction (ADR 0011). Missing or stale metrics fail open to in-flight count only.
//   - Capability probing of /version and /v1/models, exported to gateway_replica_info.
//
// Every request to a replica carries the gateway's upstream API key
// (Config.APIKey), as proxied requests do: vLLM started with --api-key
// rejects /v1/models without it (its /version and /metrics are open, but
// a proxy in front of a replica may guard every path, and the key goes
// only to the replica it already authenticates to). The key is never
// logged or exported: the view logs nothing and labels carry no headers.
//
// Scraping and probing run in background goroutines only; nothing on the
// request path or at startup waits for a replica.
//
// Every scrape and probe pass first prunes replicas the lister no longer
// returns (a discovery hot reload removed them): their state, their idle
// in-flight counters and their per-replica telemetry series.
package loadview

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

const (
	// ReasonMetricMissing: a scrape succeeded but none of the waiting-requests
	// candidate metrics was in it.
	ReasonMetricMissing = "metric_missing"
	// ReasonMetricsStale: a scrape failed and the last good one is older than
	// the staleness window (or there never was one).
	ReasonMetricsStale = "metrics_stale"
	// ReasonMetricsTooLarge: /metrics exceeded maxMetricsBodyBytes, so the
	// scrape was discarded rather than parsed from a cut-off body.
	ReasonMetricsTooLarge = "metrics_too_large"
)

// Response size ceilings for replica endpoints.
const (
	maxMetricsBodyBytes = 1 << 20
	maxVersionBodyBytes = 4 << 10
	maxModelsBodyBytes  = 256 << 10
)

// MaxVersionLen caps the vllm_version label of gateway_replica_info.
const MaxVersionLen = 32

// MaxWaitingRequests caps the waiting-requests value added to a replica's
// load. A replica reporting more (or a bogus huge value, which would
// overflow int64 on conversion) is treated as this busy.
const MaxWaitingRequests = 1 << 20

// errBodyTooLarge reports a replica response over its size ceiling.
var errBodyTooLarge = errors.New("response body exceeds size limit")

// versionPattern accepts version-like strings (0.6.3, 0.6.3.post1,
// 0.8.5+cu124, v1.0.0-rc1); anything else is exported as "unknown".
var versionPattern = regexp.MustCompile(`^[0-9A-Za-z][0-9A-Za-z.+_-]*$`)

// Config configures the load view scraper and capability prober. Its
// settings are router.load_view, validated by config, which owns their
// defaults, bounds and the metric alias table (ADR 0022).
type Config struct {
	config.LoadView
	// APIKey is the upstream API key (upstream.api_key) sent as a bearer
	// token on every scrape and probe; unset sends none.
	APIKey config.Secret
}

// ReplicaState stores the latest known metrics and metadata for a replica.
type ReplicaState struct {
	// WaitingRequests is valid only when HasWaiting is true.
	WaitingRequests float64
	HasWaiting      bool
	RunningRequests float64
	// KVCacheUsage is the mean across the replica's engines (0..1).
	KVCacheUsage float64
	// PrefixCacheQueries and PrefixCacheHits are cumulative counters (tokens).
	PrefixCacheQueries float64
	PrefixCacheHits    float64
	LastScraped        time.Time
	VLLMVersion        string
	Models             []string
	MaxModelLen        int
	Stale              bool
}

// ReplicaLister supplies the active replicas across all pools.
type ReplicaLister interface {
	AllReplicas() []discovery.Replica
}

// ReplicaListerFunc adapts a function to ReplicaLister.
type ReplicaListerFunc func() []discovery.Replica

func (f ReplicaListerFunc) AllReplicas() []discovery.Replica {
	return f()
}

// View maintains replica load state, in-flight tracking, and metric scraping.
type View struct {
	cfg        Config
	lister     ReplicaLister
	metrics    *telemetry.Metrics
	httpClient *http.Client
	now        func() time.Time

	inFlight sync.Map // replicaName (string) -> *atomic.Int64
	states   sync.Map // replicaName (string) -> *stateEntry (immutable once stored)
	// wanted is every configured alias name: the scrape parses only those
	// samples, skipping vLLM's many unrelated histograms without
	// allocating (built once in New).
	wanted map[string]struct{}
	// stateMu serialises the read-modify-write of states by the scrape and
	// probe goroutines so neither loses the other's fields, and orders them
	// with pruning. Readers (Load, State) never take it.
	stateMu sync.Mutex
	// current maps each listed replica's name to its base URL, as of the
	// last prune (nil before the first). Guarded by stateMu.
	current map[string]string

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New constructs a new View. cfg.LoadView must be a validated
// router.load_view: New applies no defaults (a zero interval would stop
// Start). A nil client gets a dedicated one; per-request timeouts come from
// ScrapeTimeout.
func New(cfg Config, lister ReplicaLister, tm *telemetry.Metrics, client *http.Client) *View {
	if client == nil {
		client = &http.Client{}
	}

	wanted := make(map[string]struct{})
	for _, names := range cfg.MetricAliases {
		for _, n := range names {
			wanted[n] = struct{}{}
		}
	}
	return &View{
		cfg:        cfg,
		lister:     lister,
		metrics:    tm,
		httpClient: client,
		now:        time.Now,
		wanted:     wanted,
	}
}

// stateEntry is a stored ReplicaState with the routing load it adds while
// fresh, precomputed when it is stored so Load does no float math and
// reads the clock only for a replica that adds load.
type stateEntry struct {
	ReplicaState
	// extra is the load the scraped metrics add (waiting requests, clamped,
	// plus any KV-pressure penalty); 0 adds none.
	extra int64
}

// storeState publishes s as replica's state. Callers that read-modify-write
// hold stateMu.
func (v *View) storeState(replica string, s ReplicaState) {
	v.states.Store(replica, &stateEntry{ReplicaState: s, extra: v.extraLoad(s)})
}

// extraLoad is the load s adds to in-flight while fresh.
func (v *View) extraLoad(s ReplicaState) int64 {
	if !s.HasWaiting || s.LastScraped.IsZero() {
		return 0
	}
	var extra int64
	if w := s.WaitingRequests; w > 0 {
		if w > MaxWaitingRequests {
			w = MaxWaitingRequests
		}
		extra = int64(w)
	}
	if hw, wt := v.cfg.KVHighWatermark, v.cfg.KVPenaltyWeight; wt > 0 && hw > 0 && hw < 1 && s.KVCacheUsage > hw {
		frac := (min(s.KVCacheUsage, 1) - hw) / (1 - hw)
		extra += int64(math.Ceil(frac * float64(wt)))
	}
	return extra
}

// Start begins background scraping and probing and returns at once. The
// first probe and scrape run in the background goroutines, so startup (and
// liveness) never waits on a replica.
func (v *View) Start(ctx context.Context) {
	ctx, v.cancel = context.WithCancel(ctx)
	v.wg.Add(2)
	go v.loop(ctx, v.cfg.ScrapeInterval, v.ScrapeOnce)
	go v.loop(ctx, v.cfg.ProbeInterval, v.ProbeOnce)
}

// Stop terminates background scraping and waits for goroutines to exit.
func (v *View) Stop() {
	if v.cancel != nil {
		v.cancel()
	}
	v.wg.Wait()
}

// loop runs once immediately, then every interval until ctx is done.
func (v *View) loop(ctx context.Context, interval time.Duration, once func(context.Context) error) {
	defer v.wg.Done()
	_ = once(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = once(ctx)
		}
	}
}

// IncInFlight increments the in-flight counter for replica and returns the new value.
func (v *View) IncInFlight(replica string) int64 {
	for {
		c := v.getOrInitCounter(replica)
		if n := c.Add(1); n > 0 {
			return n
		}
		// prune retired this counter (see retiredCounter) after we loaded
		// it: drop it and count on a fresh one, so no increment is lost.
		v.inFlight.CompareAndDelete(replica, c)
	}
}

// DecInFlight decrements the in-flight counter for replica and returns the
// new value. A replica with no counter (pruned after a reload removed it)
// is a no-op returning 0, so releasing a lease held across the removal is
// safe and creates nothing.
func (v *View) DecInFlight(replica string) int64 {
	val, ok := v.inFlight.Load(replica)
	if !ok {
		return 0
	}
	c := val.(*atomic.Int64)
	n := c.Add(-1)
	if n < 0 {
		// An unmatched release: clamp to 0, but never revive a counter
		// prune retired.
		if n > retiredCounter/2 {
			c.CompareAndSwap(n, 0)
		}
		return 0
	}
	return n
}

// InFlight returns the current in-flight counter for replica (0, and
// nothing created, for a replica without one).
func (v *View) InFlight(replica string) int64 {
	val, ok := v.inFlight.Load(replica)
	if !ok {
		return 0
	}
	n := val.(*atomic.Int64).Load()
	if n < 0 {
		return 0
	}
	return n
}

func (v *View) getOrInitCounter(replica string) *atomic.Int64 {
	if val, ok := v.inFlight.Load(replica); ok {
		return val.(*atomic.Int64)
	}
	newC := &atomic.Int64{}
	actual, _ := v.inFlight.LoadOrStore(replica, newC)
	return actual.(*atomic.Int64)
}

// State returns the cached metrics and capabilities for replica.
func (v *View) State(replica string) (ReplicaState, bool) {
	if val, ok := v.states.Load(replica); ok {
		res := val.(*stateEntry).ReplicaState
		res.Stale = v.isStale(res.LastScraped)
		return res, true
	}
	return ReplicaState{}, false
}

// SeedState stores s as replica's scraped state, as a scrape would. It is a
// test hook: packages that time or test routing use it to run Load's whole
// path without a replica to scrape.
func (v *View) SeedState(replica string, s ReplicaState) {
	v.storeState(replica, s)
}

func (v *View) isStale(lastScraped time.Time) bool {
	return lastScraped.IsZero() || v.now().Sub(lastScraped) > v.cfg.StaleThreshold
}

// Load returns the combined routing load for replica: in-flight plus the
// replica's waiting requests while scraped metrics are fresh. With missing
// or stale metrics it is the in-flight count only. Load is on the request
// path and records nothing: the fail-open is counted once per scrape (see
// scrapeReplica), not once per routing candidate.
func (v *View) Load(replica discovery.Replica) int64 {
	inflight := v.InFlight(replica.Name)
	val, ok := v.states.Load(replica.Name)
	if !ok {
		return inflight
	}
	e := val.(*stateEntry)
	// The clock is read only for a replica whose metrics add load.
	if e.extra <= 0 || v.isStale(e.LastScraped) {
		return inflight
	}
	return inflight + e.extra
}

// ScrapeOnce scrapes /metrics from all replicas concurrently.
func (v *View) ScrapeOnce(ctx context.Context) error {
	v.forEachReplica(ctx, v.scrapeReplica)
	return nil
}

// ProbeOnce probes /version and /v1/models across all replicas.
func (v *View) ProbeOnce(ctx context.Context) error {
	v.forEachReplica(ctx, v.probeReplica)
	return nil
}

func (v *View) forEachReplica(ctx context.Context, fn func(context.Context, discovery.Replica)) {
	if v.lister == nil {
		return
	}
	reps := v.lister.AllReplicas()
	v.prune(reps)
	var wg sync.WaitGroup
	for _, rep := range reps {
		wg.Add(1)
		go func(r discovery.Replica) {
			defer wg.Done()
			fn(ctx, r)
		}(rep)
	}
	wg.Wait()
}

// scrapeReplica refreshes one replica's metrics and counts at most one
// fail-open for it per scrape: metric_missing when the scrape worked but
// had no waiting-requests metric, metrics_stale when it failed and the last
// good data is too old to use.
func (v *View) scrapeReplica(ctx context.Context, rep discovery.Replica) {
	raw, err := v.get(ctx, rep, "metrics", maxMetricsBodyBytes)
	if ctx.Err() != nil {
		return // shutting down: not a replica problem
	}
	if errors.Is(err, errBodyTooLarge) {
		// A cut-off body could parse its last sample wrongly and miss later
		// ones; discard the scrape and keep the previous data.
		v.failOpen(ReasonMetricsTooLarge)
		return
	}
	var parsed map[string]Series
	if err == nil {
		parsed = parsePrometheus(raw, v.wanted) // outside stateMu: replicas parse in parallel
	}

	v.stateMu.Lock()
	defer v.stateMu.Unlock()
	if !v.isCurrent(rep) {
		return // removed or re-pointed while the scrape ran
	}
	state := v.loadState(rep.Name)
	if err != nil {
		if v.isStale(state.LastScraped) {
			v.failOpen(ReasonMetricsStale)
		}
		return
	}

	state.LastScraped = v.now()
	waiting, ok := matchSignal(parsed, v.cfg.MetricAliases[config.SignalWaitingRequests])
	state.WaitingRequests, state.HasWaiting = waiting.Sum, ok
	if !ok {
		v.failOpen(ReasonMetricMissing)
	}
	if s, ok := matchSignal(parsed, v.cfg.MetricAliases[config.SignalRunningRequests]); ok {
		state.RunningRequests = s.Sum
	}
	if s, ok := matchSignal(parsed, v.cfg.MetricAliases[config.SignalKVCacheUsage]); ok {
		state.KVCacheUsage = s.Mean() // a fraction per engine: average, don't add
	}
	if s, ok := matchSignal(parsed, v.cfg.MetricAliases[config.SignalPrefixCacheQueries]); ok {
		state.PrefixCacheQueries = s.Sum
	}
	if s, ok := matchSignal(parsed, v.cfg.MetricAliases[config.SignalPrefixCacheHits]); ok {
		state.PrefixCacheHits = s.Sum
	}
	v.storeState(rep.Name, state)
}

// loadState returns a copy of replica's state (zero if none). Callers hold stateMu.
func (v *View) loadState(replica string) ReplicaState {
	if val, ok := v.states.Load(replica); ok {
		return val.(*stateEntry).ReplicaState
	}
	return ReplicaState{}
}

func (v *View) failOpen(reason string) {
	if v.metrics != nil {
		v.metrics.FailOpen(telemetry.StageLoadview, reason)
	}
}

// matchSignal returns the series of the first candidate name present.
func matchSignal(parsed map[string]Series, candidates []string) (Series, bool) {
	for _, cand := range candidates {
		if s, ok := parsed[cand]; ok {
			return s, true
		}
	}
	return Series{}, false
}

func (v *View) probeReplica(ctx context.Context, rep discovery.Replica) {
	version, verr := v.probeVersion(ctx, rep)
	models, maxLen, _ := v.probeModels(ctx, rep)

	v.stateMu.Lock()
	defer v.stateMu.Unlock()
	if !v.isCurrent(rep) {
		return // removed or re-pointed while the probe ran: export nothing for it
	}
	if verr == nil && v.metrics != nil {
		v.metrics.SetReplicaInfo(rep.Pool, rep.Name, version)
	}
	state := v.loadState(rep.Name)
	if verr == nil {
		state.VLLMVersion = version
	}
	if len(models) > 0 {
		state.Models = models
	}
	if maxLen > 0 {
		state.MaxModelLen = maxLen
	}
	v.storeState(rep.Name, state)
}

// probeVersion reads /version ({"version":"0.6.3"} or plain text) and
// returns it validated: a version-like string of at most MaxVersionLen
// characters, or telemetry.VersionUnknown.
func (v *View) probeVersion(ctx context.Context, rep discovery.Replica) (string, error) {
	body, err := v.get(ctx, rep, "version", maxVersionBodyBytes)
	if errors.Is(err, errBodyTooLarge) {
		return telemetry.VersionUnknown, nil // it answered, but not with a version
	}
	if err != nil {
		return "", err
	}
	var doc struct {
		Version string `json:"version"`
	}
	raw := ""
	if err := json.Unmarshal(body, &doc); err == nil && doc.Version != "" {
		raw = doc.Version
	} else {
		raw = strings.Trim(strings.TrimSpace(string(body)), `"`)
	}
	if raw == "" {
		return "", errors.New("empty version response")
	}
	return NormalizeVersion(raw), nil
}

// NormalizeVersion returns v if it looks like a version string of at most
// MaxVersionLen characters, else telemetry.VersionUnknown. It keeps an
// unexpected /version body (an HTML error page, say) out of metric labels.
func NormalizeVersion(v string) string {
	if len(v) > MaxVersionLen || !versionPattern.MatchString(v) {
		return telemetry.VersionUnknown
	}
	return v
}

func (v *View) probeModels(ctx context.Context, rep discovery.Replica) ([]string, int, error) {
	body, err := v.get(ctx, rep, "v1/models", maxModelsBodyBytes)
	if err != nil {
		return nil, 0, err
	}
	var doc struct {
		Data []struct {
			ID          string `json:"id"`
			MaxModelLen int    `json:"max_model_len"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, 0, err
	}
	var models []string
	var maxLen int
	for _, m := range doc.Data {
		if m.ID != "" {
			models = append(models, m.ID)
		}
		if m.MaxModelLen > maxLen {
			maxLen = m.MaxModelLen
		}
	}
	return models, maxLen, nil
}

// get fetches path from rep within ScrapeTimeout and returns its 200
// response body, or errBodyTooLarge if the body is longer than limit.
func (v *View) get(ctx context.Context, rep discovery.Replica, path string, limit int64) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, v.cfg.ScrapeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rep.BaseURL.JoinPath(path).String(), nil)
	if err != nil {
		return nil, err
	}
	if v.cfg.APIKey.IsSet() {
		req.Header.Set("Authorization", "Bearer "+v.cfg.APIKey.Reveal())
	}
	resp, err := v.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, limit))
		return nil, fmt.Errorf("GET /%s: unexpected status %d", path, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("GET /%s: %w (%d bytes)", path, errBodyTooLarge, limit)
	}
	return body, nil
}
