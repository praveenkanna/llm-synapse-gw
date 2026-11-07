package telemetry

import (
	"strconv"
	"sync/atomic"
	"time"
)

// Per-template metrics (M3). Their template and version labels come only
// from registry entries a request matched, never from caller-supplied
// strings, so the series are bounded by templates.max_versions (and by
// MaxSeriesPerMetric). RetainTemplates drops the series of versions that
// left the registry snapshot.
const (
	MetricTemplateRequests         = "gateway_template_requests_total"
	MetricTemplateTTFT             = "gateway_template_ttft_seconds"
	MetricTemplatePromptTokens     = "gateway_template_prompt_tokens_total"
	MetricTemplateCompletionTokens = "gateway_template_completion_tokens_total"
	// MetricTemplateCompletionLength is the completion length distribution,
	// in tokens: what the max_tokens policy's p99.9 is drawn from.
	MetricTemplateCompletionLength = "gateway_template_completion_tokens"
	// MetricTemplateCacheablePromptTokens adds the version's registered
	// prefix_tokens (the prompt tokens every render shares) per request.
	MetricTemplateCacheablePromptTokens = "gateway_template_cacheable_prompt_tokens_total"
	MetricTemplateMaxTokensSet          = "gateway_template_max_tokens_set_total"
	MetricTemplateRunaway               = "gateway_template_runaway_total"
	MetricTemplateSnapshotVersions      = "gateway_template_snapshot_versions"
	MetricTemplateSnapshotAge           = "gateway_template_snapshot_age_seconds"
)

var completionTokenBuckets = []float64{16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536}

type templateMetrics struct {
	requests         counterFamily
	ttft             histogramFamily
	promptTokens     counterFamily
	completionTokens counterFamily
	completionLength histogramFamily
	cacheable        counterFamily
	maxTokensSet     counterFamily
	runaway          counterFamily
	snapshotVersions gaugeFamily
	snapshotAge      gaugeFamily

	// snapshot reports the registry snapshot's size and age at scrape time.
	snapshot atomic.Pointer[func() (versions int, age time.Duration, ok bool)]
}

func newTemplateMetrics() templateMetrics {
	return templateMetrics{
		requests: newCounterFamily(MetricTemplateRequests,
			"Requests that matched a registered template version, by template, version and final HTTP status or outcome.", "template", "version", "status"),
		ttft: newHistogramFamily(MetricTemplateTTFT,
			"TTFT of successful requests that matched a template version.", ttftBuckets, "template", "version"),
		promptTokens: newCounterFamily(MetricTemplatePromptTokens,
			"Prompt tokens reported in response usage of matched template requests.", "template", "version"),
		completionTokens: newCounterFamily(MetricTemplateCompletionTokens,
			"Completion tokens reported in response usage of matched template requests.", "template", "version"),
		completionLength: newHistogramFamily(MetricTemplateCompletionLength,
			"Completion length in tokens of matched template requests with usage.", completionTokenBuckets, "template", "version"),
		cacheable: newCounterFamily(MetricTemplateCacheablePromptTokens,
			"The version's registered shared-prefix tokens, added per successful matched request: the prompt tokens the prefix cache can serve.", "template", "version"),
		maxTokensSet: newCounterFamily(MetricTemplateMaxTokensSet,
			"Matched template requests without a caller cap on which the gateway set max_tokens (ADR 0007).", "template", "version"),
		runaway: newCounterFamily(MetricTemplateRunaway,
			"Matched template requests that ended with finish_reason length on a max_tokens the gateway set.", "template", "version"),
		snapshotVersions: newGaugeFamily(MetricTemplateSnapshotVersions,
			"Template versions in the request path's registry snapshot."),
		snapshotAge: newGaugeFamily(MetricTemplateSnapshotAge,
			"Seconds since the registry snapshot was last confirmed current against the store; grows while the store is unreachable."),
	}
}

func (t *templateMetrics) families() []writer {
	return []writer{t.requests, t.ttft, t.promptTokens, t.completionTokens, t.completionLength,
		t.cacheable, t.maxTokensSet, t.runaway, t.snapshotVersions, t.snapshotAge}
}

// SetTemplateSnapshotSource sets the function read at every scrape for the
// snapshot size and age gauges. ok false (no registry) exports neither.
func (m *Metrics) SetTemplateSnapshotSource(f func() (versions int, age time.Duration, ok bool)) {
	if m != nil {
		m.templates.snapshot.Store(&f)
	}
}

func (t *templateMetrics) refreshSnapshotGauges() {
	f := t.snapshot.Load()
	if f == nil {
		return
	}
	versions, age, ok := (*f)()
	if !ok {
		return
	}
	t.snapshotVersions.with().set(int64(versions))
	t.snapshotAge.with().set(int64(age / time.Second))
}

// RetainTemplates deletes the series of every template version for which
// keep returns false, for example once it left the registry snapshot.
func (m *Metrics) RetainTemplates(keep func(template, version string) bool) {
	if m == nil {
		return
	}
	t := &m.templates
	drop := func(l []string) bool { return !keep(l[0], l[1]) }
	t.requests.removeIf(func(l []string, _ *counter) bool { return drop(l) })
	t.ttft.removeIf(func(l []string, _ *histogram) bool { return drop(l) })
	t.completionLength.removeIf(func(l []string, _ *histogram) bool { return drop(l) })
	for _, f := range []counterFamily{t.promptTokens, t.completionTokens, t.cacheable, t.maxTokensSet, t.runaway} {
		f.removeIf(func(l []string, _ *counter) bool { return drop(l) })
	}
	m.dedup.retain(keep)
}

// templateRecord is the template part of a request's telemetry.
type templateRecord struct {
	id, version      string
	cacheable        int
	maxTokensSet     int // the max_tokens the gateway set; 0 if none
	hasUsage         bool
	promptTokens     int
	completionTokens int
	runaway          bool
}

// SetTemplate records that the request matched template id, version.
// cacheable is the version's registered shared-prefix tokens (0 unknown).
func (r *Request) SetTemplate(id string, version, cacheable int) {
	if r == nil {
		return
	}
	r.tmpl = &templateRecord{id: id, version: strconv.Itoa(version), cacheable: cacheable}
}

// TemplateMaxTokens records the max_tokens the gateway set on a matched
// template request that carried no cap of its own.
func (r *Request) TemplateMaxTokens(n int) {
	if r == nil || r.tmpl == nil {
		return
	}
	r.tmpl.maxTokensSet = n
}

// TemplateUsage records the response usage of a matched template request
// and whether it ended with finish_reason length. A length ending on a cap
// the gateway set is a runaway.
func (r *Request) TemplateUsage(promptTokens, completionTokens int, finishedLength bool) {
	if r == nil || r.tmpl == nil {
		return
	}
	t := r.tmpl
	t.hasUsage, t.promptTokens, t.completionTokens = true, promptTokens, completionTokens
	t.runaway = finishedLength && t.maxTokensSet > 0
}

// TemplateLengthEnding marks a runaway when usage was not reported (a
// stream without include_usage) but the response ended on length.
func (r *Request) TemplateLengthEnding() {
	if r == nil || r.tmpl == nil {
		return
	}
	r.tmpl.runaway = r.tmpl.maxTokensSet > 0
}

func (t *templateMetrics) finish(rec *Request, statusLabel string, status int) {
	tr := rec.tmpl
	if tr == nil {
		return
	}
	id, v := tr.id, tr.version
	t.requests.with(id, v, statusLabel).inc()
	if tr.maxTokensSet > 0 {
		t.maxTokensSet.with(id, v).inc()
	}
	if tr.runaway {
		t.runaway.with(id, v).inc()
	}
	if status >= 400 {
		return
	}
	if rec.hasFirstByte {
		t.ttft.with(id, v).observe(rec.firstByte.Seconds())
	}
	if tr.cacheable > 0 {
		t.cacheable.with(id, v).add(uint64(tr.cacheable))
	}
	if tr.hasUsage {
		t.promptTokens.with(id, v).add(uint64(max(tr.promptTokens, 0)))
		t.completionTokens.with(id, v).add(uint64(max(tr.completionTokens, 0)))
		t.completionLength.with(id, v).observe(float64(tr.completionTokens))
	}
}
