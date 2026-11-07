package mockllm

import (
	"bufio"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Metric names follow vLLM's Prometheus exporter so the load view and
// dashboards read mockllm and real replicas the same way. Names vLLM renamed
// between releases come from a per-mode set (metricNamesFor); the rest are
// stable across the releases we imitate (ADR 0011).
const (
	metricRunning         = "vllm:num_requests_running"
	metricWaiting         = "vllm:num_requests_waiting"
	metricPromptTokens    = "vllm:prompt_tokens_total"
	metricGenTokens       = "vllm:generation_tokens_total"
	metricRequestSuccess  = "vllm:request_success_total"
	metricTTFT            = "vllm:time_to_first_token_seconds"
	metricE2E             = "vllm:e2e_request_latency_seconds"
	metricMockRequests    = "mockllm_requests_total"
	metricMockCancelled   = "mockllm_cancelled_requests_total"
	metricMockFaults      = "mockllm_faults_injected_total"
	metricMockRejected    = "mockllm_rejected_requests_total"
	metricMockPriority    = "mockllm_priority_requests_total"
	finishReasonStop      = "stop"
	finishReasonLength    = "length"
	labelModelName        = "model_name"
	labelFinishedReason   = "finished_reason"
	labelFaultKind        = "kind"
	labelEndpoint         = "endpoint"
	prometheusContentType = "text/plain; version=0.0.4; charset=utf-8"
)

// metricNames are the vLLM metric names that differ between release families.
type metricNames struct {
	kvUsage       string
	prefixQueries string
	prefixHits    string
}

var (
	currentMetricNames = metricNames{
		kvUsage:       "vllm:kv_cache_usage_perc",
		prefixQueries: "vllm:prefix_cache_queries_total",
		prefixHits:    "vllm:prefix_cache_hits_total",
	}
	legacyMetricNames = metricNames{
		kvUsage:       "vllm:gpu_cache_usage_perc",
		prefixQueries: "vllm:gpu_prefix_cache_queries_total",
		prefixHits:    "vllm:gpu_prefix_cache_hits_total",
	}
)

// metricNamesFor returns the name set for a compat mode; anything but legacy
// gets the current names.
func metricNamesFor(mode VLLMCompat) metricNames {
	if mode == CompatLegacy {
		return legacyMetricNames
	}
	return currentMetricNames
}

// Histogram buckets copied from vLLM's exporter.
var (
	ttftBuckets = []float64{0.001, 0.005, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0}
	e2eBuckets  = []float64{0.3, 0.5, 0.8, 1.0, 1.5, 2.0, 2.5, 5.0, 10.0, 15.0, 20.0, 30.0, 40.0, 50.0, 60.0}
)

// histogram is a fixed-bucket Prometheus histogram.
type histogram struct {
	bounds []float64
	mu     sync.Mutex
	counts []uint64 // per bucket, non-cumulative; last is +Inf
	sum    float64
	count  uint64
}

func newHistogram(bounds []float64) *histogram {
	return &histogram{bounds: bounds, counts: make([]uint64, len(bounds)+1)}
}

func (h *histogram) observe(v float64) {
	i := sort.SearchFloat64s(h.bounds, v) // first bound >= v
	h.mu.Lock()
	h.counts[i]++
	h.sum += v
	h.count++
	h.mu.Unlock()
}

type histSnapshot struct {
	bounds     []float64
	cumulative []uint64
	sum        float64
	count      uint64
}

func (h *histogram) snapshot() histSnapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	cum := make([]uint64, len(h.counts))
	var acc uint64
	for i, c := range h.counts {
		acc += c
		cum[i] = acc
	}
	return histSnapshot{bounds: h.bounds, cumulative: cum, sum: h.sum, count: h.count}
}

// modelMetrics holds the vLLM-named series for one served model. The model
// set is fixed at startup, so the series set is bounded.
type modelMetrics struct {
	running       atomic.Int64
	waiting       atomic.Int64
	promptTokens  atomic.Uint64 // also the prefix-cache queries, in tokens
	prefixHits    atomic.Uint64 // prompt tokens served from the prefix cache
	genTokens     atomic.Uint64
	successStop   atomic.Uint64
	successLength atomic.Uint64
	ttft          *histogram
	e2e           *histogram
}

func newModelMetrics() *modelMetrics {
	return &modelMetrics{ttft: newHistogram(ttftBuckets), e2e: newHistogram(e2eBuckets)}
}

func (m *modelMetrics) success(reason string) {
	if reason == finishReasonLength {
		m.successLength.Add(1)
		return
	}
	m.successStop.Add(1)
}

// Fault kinds.
const (
	faultNone  = "none"
	fault5xx   = "5xx"
	faultDrop  = "drop"
	faultHang  = "hang"
	faultStall = "stall"
)

var faultKinds = []string{fault5xx, faultDrop, faultHang, faultStall}

// Endpoint labels for mockllm_requests_total.
const (
	endpointChat       = "chat"
	endpointCompletion = "completions"
)

// metrics is the whole metric set of one mock instance.
type metrics struct {
	models     map[string]*modelMetrics
	modelNames []string // sorted, for stable output
	maxNumSeqs int
	names      metricNames

	requests  map[string]*atomic.Uint64 // by endpoint
	faults    map[string]*atomic.Uint64 // by fault kind
	cancelled atomic.Uint64
	completed atomic.Uint64
	rejected  atomic.Uint64 // 4xx: bad JSON, unknown model, body too large
	priority  atomic.Uint64 // requests carrying a non-zero "priority"
}

func newMetrics(models []string, maxNumSeqs int, mode VLLMCompat) *metrics {
	m := &metrics{
		models:     make(map[string]*modelMetrics, len(models)),
		maxNumSeqs: maxNumSeqs,
		names:      metricNamesFor(mode),
		requests: map[string]*atomic.Uint64{
			endpointChat: new(atomic.Uint64), endpointCompletion: new(atomic.Uint64),
		},
		faults: make(map[string]*atomic.Uint64, len(faultKinds)),
	}
	for _, name := range models {
		if _, ok := m.models[name]; !ok {
			m.models[name] = newModelMetrics()
			m.modelNames = append(m.modelNames, name)
		}
	}
	sort.Strings(m.modelNames)
	for _, k := range faultKinds {
		m.faults[k] = new(atomic.Uint64)
	}
	return m
}

// kvUsage is a stand-in for KV-cache pressure: running / max-num-seqs.
func (m *metrics) kvUsage(mm *modelMetrics) float64 {
	if m.maxNumSeqs <= 0 {
		return 0
	}
	return math.Min(1, float64(mm.running.Load())/float64(m.maxNumSeqs))
}

// writePrometheus renders the text exposition format.
func (m *metrics) writePrometheus(w io.Writer) error {
	bw := bufio.NewWriter(w)
	pw := promWriter{w: bw}

	pw.family(metricRunning, "gauge", "Number of requests in model execution batches.")
	for _, name := range m.modelNames {
		pw.sample(metricRunning, modelLabel(name), float64(m.models[name].running.Load()))
	}
	pw.family(metricWaiting, "gauge", "Number of requests waiting to be processed.")
	for _, name := range m.modelNames {
		pw.sample(metricWaiting, modelLabel(name), float64(m.models[name].waiting.Load()))
	}
	pw.family(m.names.kvUsage, "gauge", "KV-cache usage. 1 means 100 percent usage (mock: running / max_num_seqs).")
	for _, name := range m.modelNames {
		pw.sample(m.names.kvUsage, modelLabel(name), m.kvUsage(m.models[name]))
	}
	pw.family(m.names.prefixQueries, "counter", "Prefix cache queries, in terms of number of queried tokens.")
	for _, name := range m.modelNames {
		pw.sample(m.names.prefixQueries, modelLabel(name), float64(m.models[name].promptTokens.Load()))
	}
	pw.family(m.names.prefixHits, "counter", "Prefix cache hits, in terms of number of cached tokens.")
	for _, name := range m.modelNames {
		pw.sample(m.names.prefixHits, modelLabel(name), float64(m.models[name].prefixHits.Load()))
	}
	pw.family(metricPromptTokens, "counter", "Number of prefill tokens processed.")
	for _, name := range m.modelNames {
		pw.sample(metricPromptTokens, modelLabel(name), float64(m.models[name].promptTokens.Load()))
	}
	pw.family(metricGenTokens, "counter", "Number of generation tokens processed.")
	for _, name := range m.modelNames {
		pw.sample(metricGenTokens, modelLabel(name), float64(m.models[name].genTokens.Load()))
	}
	pw.family(metricRequestSuccess, "counter", "Count of successfully processed requests.")
	for _, name := range m.modelNames {
		mm := m.models[name]
		pw.sample(metricRequestSuccess, labels(labelFinishedReason, finishReasonLength, labelModelName, name), float64(mm.successLength.Load()))
		pw.sample(metricRequestSuccess, labels(labelFinishedReason, finishReasonStop, labelModelName, name), float64(mm.successStop.Load()))
	}
	pw.family(metricTTFT, "histogram", "Histogram of time to first token in seconds.")
	for _, name := range m.modelNames {
		pw.histogram(metricTTFT, name, m.models[name].ttft.snapshot())
	}
	pw.family(metricE2E, "histogram", "Histogram of end to end request latency in seconds.")
	for _, name := range m.modelNames {
		pw.histogram(metricE2E, name, m.models[name].e2e.snapshot())
	}

	pw.family(metricMockRequests, "counter", "Generation requests received by mockllm.")
	for _, ep := range []string{endpointChat, endpointCompletion} {
		pw.sample(metricMockRequests, labels(labelEndpoint, ep), float64(m.requests[ep].Load()))
	}
	pw.family(metricMockCancelled, "counter", "Generation requests cancelled by the client before completion.")
	pw.sample(metricMockCancelled, "", float64(m.cancelled.Load()))
	pw.family(metricMockFaults, "counter", "Faults injected by mockllm.")
	for _, k := range faultKinds {
		pw.sample(metricMockFaults, labels(labelFaultKind, k), float64(m.faults[k].Load()))
	}
	pw.family(metricMockRejected, "counter", "Generation requests rejected with a 4xx.")
	pw.sample(metricMockRejected, "", float64(m.rejected.Load()))
	pw.family(metricMockPriority, "counter", "Generation requests that carried a non-zero priority.")
	pw.sample(metricMockPriority, "", float64(m.priority.Load()))

	if pw.err != nil {
		return pw.err
	}
	return bw.Flush()
}

// promWriter writes the exposition format and keeps the first error.
type promWriter struct {
	w   *bufio.Writer
	err error
}

func (p *promWriter) write(s ...string) {
	for _, x := range s {
		if p.err != nil {
			return
		}
		_, p.err = p.w.WriteString(x)
	}
}

func (p *promWriter) family(name, typ, help string) {
	p.write("# HELP ", name, " ", help, "\n# TYPE ", name, " ", typ, "\n")
}

func (p *promWriter) sample(name, lbls string, v float64) {
	if lbls != "" {
		lbls = "{" + lbls + "}"
	}
	p.write(name, lbls, " ", formatFloat(v), "\n")
}

func (p *promWriter) histogram(name, model string, s histSnapshot) {
	ml := modelLabel(model)
	for i, b := range s.bounds {
		p.sample(name+"_bucket", ml+`,le="`+formatFloat(b)+`"`, float64(s.cumulative[i]))
	}
	p.sample(name+"_bucket", ml+`,le="+Inf"`, float64(s.cumulative[len(s.cumulative)-1]))
	p.sample(name+"_sum", ml, s.sum)
	p.sample(name+"_count", ml, float64(s.count))
}

func modelLabel(model string) string { return labels(labelModelName, model) }

var labelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

// labels renders k="v" pairs with Prometheus label-value escaping.
func labels(kv ...string) string {
	var out []byte
	for i := 0; i+1 < len(kv); i += 2 {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, kv[i]...)
		out = append(out, '=')
		out = append(out, '"')
		out = append(out, labelEscaper.Replace(kv[i+1])...)
		out = append(out, '"')
	}
	return string(out)
}

func formatFloat(v float64) string {
	if math.IsInf(v, 1) {
		return "+Inf"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}
