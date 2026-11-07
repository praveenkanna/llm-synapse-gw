package telemetry

import "sync/atomic"

// Coalescing, response cache and budgeted context metrics (M4, ADR 0019).
// The template and version labels come only from registry entries a request
// matched, as for the M3 per-template metrics.
const (
	// MetricCoalesced counts requests answered from another request's
	// upstream call: each is one generation the replicas did not run.
	MetricCoalesced = "gateway_coalesced_total"
	// MetricCacheHits and MetricCacheMisses count lookups of requests that
	// were eligible for the response cache.
	MetricCacheHits   = "gateway_cache_hits_total"
	MetricCacheMisses = "gateway_cache_misses_total"
	// MetricCacheStores counts attempts to store a response, by outcome.
	MetricCacheStores = "gateway_cache_stores_total"
	// MetricCoalesceEvents counts flight events: sealed (a response
	// outgrew the replay bound), evicted (a subscriber fell behind it) and
	// cancelled (the upstream call was cancelled because no client was left).
	MetricCoalesceEvents = "gateway_coalesce_events_total"
	// MetricCoalesceFlights and MetricCoalesceRetainedBytes are the flights
	// holding memory now and the response bytes they hold.
	MetricCoalesceFlights       = "gateway_coalesce_flights"
	MetricCoalesceRetainedBytes = "gateway_coalesce_retained_bytes"
	// MetricContextAssembly counts budgeted-context requests by outcome.
	MetricContextAssembly = "gateway_context_assembly_total"
	// MetricContextDroppedMessages counts history messages dropped to fit a
	// budget.
	MetricContextDroppedMessages = "gateway_context_dropped_messages_total"
)

// Coalesce flight events for gateway_coalesce_events_total.
const (
	CoalesceSealed    = "sealed"
	CoalesceEvicted   = "evicted"
	CoalesceCancelled = "cancelled"
)

// Context assembly outcomes for gateway_context_assembly_total.
const (
	ContextUnchanged = "unchanged" // the history already fit
	ContextTrimmed   = "trimmed"   // oldest turns were dropped
	// ContextApproximate: trimmed to a suffix proven to fit before
	// measuring stopped; within budget, perhaps not the most that fits.
	ContextApproximate = "approximate"
	ContextOverBudget  = "over_budget" // the kept minimum cannot fit: answered 422
	ContextSkipped     = "skipped"     // could not measure: forwarded unchanged
)

type dedupMetrics struct {
	coalesced      counterFamily
	cacheHits      counterFamily
	cacheMisses    counterFamily
	cacheStores    counterFamily
	events         counterFamily
	flights        gaugeFamily
	retained       gaugeFamily
	contextResults counterFamily
	contextDropped counterFamily

	// source reports the flight table's size at scrape time.
	source atomic.Pointer[func() (flights int, retainedBytes int64)]
}

func newDedupMetrics() dedupMetrics {
	return dedupMetrics{
		coalesced: newCounterFamily(MetricCoalesced,
			"Requests answered from another request's upstream call, by template and version.", "template", "version"),
		cacheHits: newCounterFamily(MetricCacheHits,
			"Response cache hits, by template and version.", "template", "version"),
		cacheMisses: newCounterFamily(MetricCacheMisses,
			"Response cache lookups that found nothing (or failed open), by template and version.", "template", "version"),
		cacheStores: newCounterFamily(MetricCacheStores,
			"Attempts to store a response in the cache, by outcome.", "outcome"),
		events: newCounterFamily(MetricCoalesceEvents,
			"Coalescing flight events: sealed, evicted, cancelled.", "event"),
		flights: newGaugeFamily(MetricCoalesceFlights,
			"Coalescing flights holding memory now."),
		retained: newGaugeFamily(MetricCoalesceRetainedBytes,
			"Response bytes held by coalescing flights for replay."),
		contextResults: newCounterFamily(MetricContextAssembly,
			"Budgeted-context requests by outcome.", "outcome"),
		contextDropped: newCounterFamily(MetricContextDroppedMessages,
			"History messages dropped to fit a context budget."),
	}
}

func (d *dedupMetrics) families() []writer {
	return []writer{d.coalesced, d.cacheHits, d.cacheMisses, d.cacheStores, d.events, d.flights, d.retained, d.contextResults, d.contextDropped}
}

// SetCoalesceSource sets the function read at every scrape for the flight
// gauges.
func (m *Metrics) SetCoalesceSource(f func() (flights int, retainedBytes int64)) {
	if m != nil {
		m.dedup.source.Store(&f)
	}
}

func (d *dedupMetrics) refreshGauges() {
	f := d.source.Load()
	if f == nil {
		return
	}
	flights, retained := (*f)()
	d.flights.with().set(int64(flights))
	d.retained.with().set(retained)
}

// CoalesceEvent counts a flight event.
func (m *Metrics) CoalesceEvent(event string) {
	if m != nil {
		m.dedup.events.with(event).inc()
	}
}

// CacheStore counts an attempt to store a response, by outcome.
func (m *Metrics) CacheStore(outcome string) {
	if m != nil {
		m.dedup.cacheStores.with(outcome).inc()
	}
}

// ContextAssembly counts a budgeted-context request by outcome; dropped is
// the history messages it cut.
func (m *Metrics) ContextAssembly(outcome string, dropped int) {
	if m == nil {
		return
	}
	m.dedup.contextResults.with(outcome).inc()
	if dropped > 0 {
		m.dedup.contextDropped.with().add(uint64(dropped))
	}
}

// retain drops the series of versions for which keep returns false.
func (d *dedupMetrics) retain(keep func(template, version string) bool) {
	for _, f := range []counterFamily{d.coalesced, d.cacheHits, d.cacheMisses} {
		f.removeIf(func(l []string, _ *counter) bool { return !keep(l[0], l[1]) })
	}
}

// Coalesced records that the request was answered from another request's
// upstream call. It needs SetTemplate first.
func (r *Request) Coalesced() {
	if r == nil || r.tmpl == nil {
		return
	}
	r.cacheOutcome = "coalesced"
	r.m.dedup.coalesced.with(r.tmpl.id, r.tmpl.version).inc()
}

// Disconnected reports whether the request was recorded as a client
// disconnect.
func (r *Request) Disconnected() bool { return r != nil && r.disconnected }

// CacheLookup records the result of a response cache lookup for a request
// that matched a template.
func (r *Request) CacheLookup(hit bool) {
	if r == nil || r.tmpl == nil {
		return
	}
	if hit {
		r.cacheOutcome = "hit"
		r.m.dedup.cacheHits.with(r.tmpl.id, r.tmpl.version).inc()
		return
	}
	r.cacheOutcome = "miss"
	r.m.dedup.cacheMisses.with(r.tmpl.id, r.tmpl.version).inc()
}
