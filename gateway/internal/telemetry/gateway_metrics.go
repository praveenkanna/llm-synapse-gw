package telemetry

import (
	"bytes"
	"net/http"
	"strings"
	"sync"
)

// Metric names (prefix gateway_). Labels are low-cardinality only: pool,
// replica, class, stage, reason, status, kind, policy, outcome and the
// validated vllm_version. status is always an HTTP status or a response
// outcome (below). Never caller names, tokens or trace IDs.
const (
	MetricRequests          = "gateway_requests_total"
	MetricInflight          = "gateway_inflight"
	MetricTTFT              = "gateway_ttft_seconds"
	MetricRequestDuration   = "gateway_request_duration_seconds"
	MetricUpstreamErrors    = "gateway_upstream_errors_total"
	MetricClientDisconnects = "gateway_client_disconnects_total"
	MetricFailOpen          = "gateway_failopen_total"
	MetricAuthRejections    = "gateway_auth_rejections_total"
	// MetricReplicaInfo exports metadata about active replicas (vLLM version).
	MetricReplicaInfo = "gateway_replica_info"
	// MetricRetries counts connect retries now, in M0, rather than waiting
	// for an outcome on M1's gateway_route_decisions_total: the retry exists
	// before any routing policy does, and a separate counter keeps the M1
	// metric about policy decisions only.
	MetricRetries = "gateway_retries_total"
	// MetricRouteDecisions counts routing decisions by pool, policy and
	// outcome (affinity, overflow, failopen, retry, round_robin, ...). The
	// label is "outcome", not "status": it is not an HTTP status. Per-replica
	// in-flight load is gateway_inflight; there is no separate router gauge.
	MetricRouteDecisions = "gateway_route_decisions_total"
	// MetricRouterDuration measures routing decision latency in seconds.
	MetricRouterDuration = "gateway_router_duration_seconds"
	// MetricReplicaEjections counts replicas taken out of their pool's
	// routing ring, by pool and reason (transport_failure,
	// health_check_failed, stuck_engine). An ejection is not a fail-open: the
	// request is still routed, to the remaining replicas.
	MetricReplicaEjections = "gateway_replica_ejections_total"
	// MetricFirstChunkStalls counts streams that waited for their first
	// chunk past router.health.stuck_engine_timeout while their replica sent
	// no first chunk on any stream, by pool: the stuck-engine signal that
	// ejects a replica (reason stuck_engine) once enough of them pile up.
	MetricFirstChunkStalls = "gateway_first_chunk_stalls_total"
	// MetricStuckProbes counts generation probes sent to replicas ejected as
	// stuck engines before re-admission, by pool and outcome (pass, fail,
	// timeout, rejected). A rejected probe fails open (re-admitted on
	// /health alone, also counted in gateway_failopen_total).
	MetricStuckProbes = "gateway_stuck_probe_total"
	// Admission (M2). Queue depth and wait are per pool and class; shed
	// counts the requests answered 429 because their class is shed;
	// admission decisions count every request admission saw by outcome
	// (admitted, queue_full, queue_timeout, shed, client_gone).
	MetricQueueDepth         = "gateway_queue_depth"
	MetricQueueWait          = "gateway_queue_wait_seconds"
	MetricShed               = "gateway_shed_total"
	MetricAdmissionDecisions = "gateway_admission_decisions_total"
	// MetricAdmissionSlots is the slots in use per pool; it returns to 0
	// when load stops (AC-M2-4).
	MetricAdmissionSlots = "gateway_admission_slots_in_use"
	// MetricAdmissionLimit is the pool's effective slot limit on this
	// gateway: its capacity, or its share of it while the Redis view is
	// fresh. -1 means no limit.
	MetricAdmissionLimit = "gateway_admission_limit"
	// MetricShedding is 1 while the pool sheds, else 0.
	MetricShedding = "gateway_shedding"
	// MetricUpstreamPriority counts requests sent with a gateway-set vLLM
	// priority, and caller priorities replaced or removed, by pool and
	// outcome (injected, replaced, stripped).
	MetricUpstreamPriority = "gateway_upstream_priority_total"
)

// Admission decision outcomes for gateway_admission_decisions_total.
const (
	AdmissionAdmitted     = "admitted"
	AdmissionQueueFull    = "queue_full"
	AdmissionQueueTimeout = "queue_timeout"
	AdmissionShed         = "shed"
	AdmissionClientGone   = "client_gone"
)

// VersionUnknown is gateway_replica_info's vllm_version when a replica
// answered /version with something that is not a plausible version string.
const VersionUnknown = "unknown"

// Outcomes that replace the HTTP status in gateway_requests_total{status}
// (and are logged as "outcome") when the status line said success but the
// response did not complete. They are a fixed, low-cardinality set.
const (
	// OutcomeAborted: the connection was aborted after the status line was
	// sent (a non-streaming body cut off mid-way, or a panic mid-response), so
	// the caller saw a failed read instead of a complete body.
	OutcomeAborted = "aborted"
	// OutcomeStreamError: the replica failed mid-stream and the stream ended
	// with one SSE error event.
	OutcomeStreamError = "stream_error"
	// OutcomeClientClosed: the caller went away after the status line was
	// sent (mid-stream, or mid-body), so the response it asked for was never
	// fully delivered. Not a gateway or replica failure; a caller that goes
	// away before any status is sent is counted as 499 instead.
	OutcomeClientClosed = "client_closed"
)

// Label values used when a request never reached the stage that sets them.
const (
	// ClassDefault is the only request class until admission (M2) adds more.
	ClassDefault = "default"
	// PoolNone labels requests rejected before a pool was resolved (413,
	// unmatched routes, auth rejections).
	PoolNone = "none"
)

// Prometheus text exposition content type.
const metricsContentType = "text/plain; version=0.0.4; charset=utf-8"

// Histogram buckets in seconds. TTFT follows vLLM's exporter and extends to
// 60s so long-context prefill and overload queue waits do not clip at 10s;
// duration covers short classification calls up to the 15-minute upstream total timeout.
var (
	ttftBuckets           = []float64{0.001, 0.005, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10, 15, 20, 30, 60}
	durationBuckets       = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 60, 120, 300, 600, 900}
	routerDurationBuckets = []float64{0.000001, 0.000005, 0.00001, 0.000025, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.005, 0.01}
	queueWaitBuckets      = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600}
)

// Metrics holds the gateway's Prometheus metrics. It is safe for concurrent
// use; every update is an atomic operation.
type Metrics struct {
	requests          counterFamily
	inflight          gaugeFamily
	ttft              histogramFamily
	duration          histogramFamily
	upstreamErrors    counterFamily
	clientDisconnects counterFamily
	failOpen          counterFamily
	authRejections    counterFamily
	retries           counterFamily
	replicaInfo       gaugeFamily
	routeDecisions    counterFamily
	routerDuration    histogramFamily
	replicaEjections  counterFamily
	firstChunkStalls  counterFamily
	stuckProbes       counterFamily

	queueDepth         gaugeFamily
	queueWait          histogramFamily
	shed               counterFamily
	admissionDecisions counterFamily
	admissionSlots     gaugeFamily
	admissionLimit     gaugeFamily
	shedding           gaugeFamily
	upstreamPriority   counterFamily

	templates templateMetrics
	dedup     dedupMetrics

	// replicaVersions remembers the vllm_version exported per pool and
	// replica, so a changed version replaces its old series instead of
	// leaving it at 1. Bounded by MaxSeriesPerMetric entries.
	replicaInfoMu   sync.Mutex
	replicaVersions map[string]string

	bufPool sync.Pool
}

// NewMetrics returns an empty metric set.
func NewMetrics() *Metrics {
	return &Metrics{
		requests: newCounterFamily(MetricRequests,
			"Requests on the OpenAI-wire routes, by final HTTP status, or by outcome (aborted, stream_error, client_closed) when the response did not complete after its status was sent.", "pool", "class", "status"),
		inflight: newGaugeFamily(MetricInflight,
			"Requests currently sent to a replica.", "pool", "replica", "class"),
		ttft: newHistogramFamily(MetricTTFT,
			"Time from request arrival to the first response body byte relayed to the caller.", ttftBuckets, "pool", "class"),
		duration: newHistogramFamily(MetricRequestDuration,
			"Time from request arrival until the response is complete.", durationBuckets, "pool", "class"),
		upstreamErrors: newCounterFamily(MetricUpstreamErrors,
			"Upstream failures before or during the response, by replica and kind.", "replica", "kind"),
		clientDisconnects: newCounterFamily(MetricClientDisconnects,
			"Requests whose caller went away before the response was complete."),
		failOpen: newCounterFamily(MetricFailOpen,
			"Optimization stages skipped (request forwarded unchanged), by stage and reason.", "stage", "reason"),
		authRejections: newCounterFamily(MetricAuthRejections,
			"Requests rejected by caller auth with 401, by reason (missing_api_key, invalid_api_key).", "reason"),
		retries: newCounterFamily(MetricRetries,
			"Requests retried once after a failed connect (nothing reached the replica), on another replica of the pool, or the same one after a backoff when the pool has no other; by reason (connect, dns).", "pool", "reason"),
		replicaInfo: newGaugeFamily(MetricReplicaInfo,
			"Information about upstream replicas (vLLM version). Value is always 1.", "pool", "replica", "vllm_version"),
		routeDecisions: newCounterFamily(MetricRouteDecisions,
			"Routing policy decisions made by the gateway router, by pool, policy and outcome.", "pool", "policy", "outcome"),
		routerDuration: newHistogramFamily(MetricRouterDuration,
			"Time spent making a routing decision.", routerDurationBuckets, "pool", "policy"),
		replicaEjections: newCounterFamily(MetricReplicaEjections,
			"Replicas ejected from their pool's routing ring, by pool and reason.", "pool", "reason"),
		firstChunkStalls: newCounterFamily(MetricFirstChunkStalls,
			"Streams that waited past the stuck-engine timeout for their first chunk while their replica sent no first chunk on any stream, by pool.", "pool"),
		stuckProbes: newCounterFamily(MetricStuckProbes,
			"Generation probes sent to replicas ejected as stuck engines before re-admission, by pool and outcome (pass, fail, timeout, rejected).", "pool", "outcome"),
		queueDepth: newGaugeFamily(MetricQueueDepth,
			"Requests waiting in the admission queue, by pool and class.", "pool", "class"),
		queueWait: newHistogramFamily(MetricQueueWait,
			"Time a request waited in the admission queue before it was admitted or answered, by pool and class.", queueWaitBuckets, "pool", "class"),
		shed: newCounterFamily(MetricShed,
			"Requests answered 429 because their class was shed while the highest class's TTFT p95 was over the threshold, by pool and class.", "pool", "class"),
		admissionDecisions: newCounterFamily(MetricAdmissionDecisions,
			"Admission decisions by pool, class and outcome (admitted, queue_full, queue_timeout, shed, client_gone).", "pool", "class", "outcome"),
		admissionSlots: newGaugeFamily(MetricAdmissionSlots,
			"Admission slots in use, by pool.", "pool"),
		admissionLimit: newGaugeFamily(MetricAdmissionLimit,
			"Effective admission slot limit on this gateway, by pool (-1: no limit).", "pool"),
		shedding: newGaugeFamily(MetricShedding,
			"1 while the pool sheds its lowest classes, else 0.", "pool"),
		upstreamPriority: newCounterFamily(MetricUpstreamPriority,
			"Requests whose vLLM priority the gateway set (injected), put in place of the caller's own (replaced) or removed (stripped), by pool and outcome.", "pool", "outcome"),
		templates:       newTemplateMetrics(),
		dedup:           newDedupMetrics(),
		replicaVersions: make(map[string]string),
		bufPool:         sync.Pool{New: func() any { return new(bytes.Buffer) }},
	}
}

// FailOpen counts a stage that failed open outside any one request's
// handler, for example a replica address lookup on an upstream dial
// goroutine. Safe for concurrent use. Request-path stages use
// Request.FailOpen instead, which also puts the reason in the request log.
func (m *Metrics) FailOpen(stage, reason string) {
	m.failOpen.with(stage, reason).inc()
}

// SetReplicaInfo sets the gateway_replica_info gauge to 1 for the given
// replica and vLLM version. When the replica's version changes, the series
// with the previous version is deleted. version must already be validated
// (the load view caps it; see loadview); it is exported as given.
func (m *Metrics) SetReplicaInfo(pool, replica, version string) {
	if m == nil {
		return
	}
	key := pool + labelKeySep + replica
	m.replicaInfoMu.Lock()
	defer m.replicaInfoMu.Unlock()
	prev, known := m.replicaVersions[key]
	if known && prev != version {
		m.replicaInfo.remove(pool, replica, prev)
	}
	if known || len(m.replicaVersions) < MaxSeriesPerMetric {
		m.replicaVersions[key] = version
	}
	m.replicaInfo.with(pool, replica, version).set(1)
}

// RecordRouterDecision counts a routing policy decision by its outcome.
func (m *Metrics) RecordRouterDecision(pool, policy, outcome string) {
	if m != nil {
		m.routeDecisions.with(pool, policy, outcome).inc()
	}
}

// RecordRouterDuration records the time spent making a routing decision.
func (m *Metrics) RecordRouterDuration(pool, policy string, durationSeconds float64) {
	if m != nil {
		m.routerDuration.with(pool, policy).observe(durationSeconds)
	}
}

// ReplicaEjected counts a replica taken out of its pool's routing ring.
func (m *Metrics) ReplicaEjected(pool, reason string) {
	if m != nil {
		m.replicaEjections.with(pool, reason).inc()
	}
}

// FirstChunkStall counts a stream whose first chunk stalled on a replica
// of pool.
func (m *Metrics) FirstChunkStall(pool string) {
	if m != nil {
		m.firstChunkStalls.with(pool).inc()
	}
}

// StuckProbe counts a generation probe of a stuck-engine replica of pool
// by its outcome.
func (m *Metrics) StuckProbe(pool, outcome string) {
	if m != nil {
		m.stuckProbes.with(pool, outcome).inc()
	}
}

// QueueDepthAdd moves the pool's queue depth gauge for class by d.
func (m *Metrics) QueueDepthAdd(pool, class string, d int64) {
	if m != nil {
		m.queueDepth.with(pool, class).add(d)
	}
}

// ObserveQueueWait records the time a request of class waited for pool.
func (m *Metrics) ObserveQueueWait(pool, class string, seconds float64) {
	if m != nil {
		m.queueWait.with(pool, class).observe(seconds)
	}
}

// AdmissionDecision counts an admission decision; a shed one is also
// counted in gateway_shed_total.
func (m *Metrics) AdmissionDecision(pool, class, outcome string) {
	if m == nil {
		return
	}
	m.admissionDecisions.with(pool, class, outcome).inc()
	if outcome == AdmissionShed {
		m.shed.with(pool, class).inc()
	}
}

// AdmissionSlotsAdd moves the pool's slots-in-use gauge by d.
func (m *Metrics) AdmissionSlotsAdd(pool string, d int64) {
	if m != nil {
		m.admissionSlots.with(pool).add(d)
	}
}

// SetAdmissionLimit sets the pool's effective slot limit (-1: none).
func (m *Metrics) SetAdmissionLimit(pool string, limit int64) {
	if m != nil {
		m.admissionLimit.with(pool).set(limit)
	}
}

// SetShedding sets the pool's shedding gauge.
func (m *Metrics) SetShedding(pool string, on bool) {
	if m == nil {
		return
	}
	var v int64
	if on {
		v = 1
	}
	m.shedding.with(pool).set(v)
}

// UpstreamPriority counts a gateway-set (injected) or removed (stripped)
// vLLM priority on a request to pool.
func (m *Metrics) UpstreamPriority(pool, outcome string) {
	if m != nil {
		m.upstreamPriority.with(pool, outcome).inc()
	}
}

func (m *Metrics) families() []writer {
	fams := []writer{m.requests, m.inflight, m.ttft, m.duration, m.upstreamErrors, m.clientDisconnects, m.failOpen, m.authRejections, m.retries, m.replicaInfo, m.routeDecisions, m.routerDuration, m.replicaEjections, m.firstChunkStalls, m.stuckProbes,
		m.queueDepth, m.queueWait, m.shed, m.admissionDecisions, m.admissionSlots, m.admissionLimit, m.shedding, m.upstreamPriority}
	fams = append(fams, m.templates.families()...)
	return append(fams, m.dedup.families()...)
}

// ServeHTTP renders the metrics in Prometheus text format.
func (m *Metrics) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	buf := m.bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer m.bufPool.Put(buf)
	m.templates.refreshSnapshotGauges()
	m.dedup.refreshGauges()
	if err := writeAll(buf, m.families()); err != nil {
		http.Error(w, "render metrics", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", metricsContentType)
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(buf.Bytes())
}

// RetainReplicas deletes the per-replica series of every replica not in
// current (replica name -> pool), for example after a discovery reload
// removed or moved it: gateway_replica_info, gateway_upstream_errors_total
// and gateway_inflight. A gateway_inflight series is deleted only at zero;
// one above zero belongs to requests still in flight on the removed
// replica, and a later call deletes it once they finish. Series with an
// empty replica label and the overflow series are kept.
func (m *Metrics) RetainReplicas(current map[string]string) {
	if m == nil {
		return
	}
	stale := func(pool, replica string) bool {
		if replica == "" {
			return false
		}
		p, ok := current[replica]
		return !ok || (pool != "" && p != pool)
	}

	m.replicaInfoMu.Lock()
	for key := range m.replicaVersions {
		pool, replica, _ := strings.Cut(key, labelKeySep)
		if stale(pool, replica) {
			delete(m.replicaVersions, key)
		}
	}
	m.replicaInfo.removeIf(func(l []string, _ *gauge) bool { return stale(l[0], l[1]) }) // pool, replica, vllm_version
	m.replicaInfoMu.Unlock()

	m.upstreamErrors.removeIf(func(l []string, _ *counter) bool { return stale("", l[0]) }) // replica, kind
	// gateway_inflight labels: pool, replica, class.
	m.inflight.removeIf(func(l []string, g *gauge) bool {
		return stale(l[0], l[1]) && g.v.Load() == 0
	})
}
