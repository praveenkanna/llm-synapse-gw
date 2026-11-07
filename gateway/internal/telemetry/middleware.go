// Package telemetry measures the request path: Prometheus metrics (served on
// an internal port), one structured JSON log line per request, and W3C trace
// context (traceparent accepted, generated when missing, propagated upstream).
//
// It never records prompt text, response text, tool arguments or credential
// headers: only routing decisions, timings, sizes, statuses and IDs.
package telemetry

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// DefaultPathPrefix selects the requests the Middleware instruments: the
// OpenAI-wire routes. Health probes and the like pass through untouched.
const DefaultPathPrefix = "/v1/"

// Log line fields. The message is always LogMessage.
const (
	LogMessage = "request"

	statusClientClosed = 499 // nginx convention: caller went away before any response
	statusPanic        = http.StatusInternalServerError
	maxLoggedPathBytes = 256
)

// Options tunes a Telemetry. Zero values take defaults.
type Options struct {
	// Logger receives one line per request. Default: discard.
	Logger *slog.Logger
	// PathPrefix selects instrumented requests. Default: DefaultPathPrefix.
	PathPrefix string
	// Now is the clock (tests). Default: time.Now.
	Now func() time.Time
}

// Telemetry instruments the request path. It is safe for concurrent use.
type Telemetry struct {
	metrics *Metrics
	logger  *slog.Logger
	prefix  string
	now     func() time.Time
}

// New returns a Telemetry with an empty metric set.
func New(opts Options) *Telemetry {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	if opts.PathPrefix == "" {
		opts.PathPrefix = DefaultPathPrefix
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Telemetry{metrics: NewMetrics(), logger: opts.Logger, prefix: opts.PathPrefix, now: opts.Now}
}

// Metrics returns the metric set; it is also the /metrics handler.
func (t *Telemetry) Metrics() *Metrics { return t.metrics }

// Middleware instruments requests under the path prefix: it sets up trace
// context (the forwarded request carries the gateway's traceparent), puts a
// *Request in the context for later stages, and on completion records metrics
// and writes one log line. Other requests pass straight to next.
func (t *Telemetry) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, t.prefix) {
			next.ServeHTTP(w, r)
			return
		}
		tc, continued := newTraceContext(r.Header)
		rec := &Request{m: t.metrics, start: t.now(), now: t.now, trace: tc, class: ClassDefault}
		rec.tpHeader[0] = tc.traceparent()

		r2 := r.WithContext(withRequest(r.Context(), rec))
		hdr := make(http.Header, len(r.Header)+1)
		for k, vv := range r.Header {
			hdr[k] = vv[:len(vv):len(vv)] // share read-only backing, clip capacity so Append/Add cannot alias
		}
		hdr[HeaderTraceparent] = rec.tpHeader[:1:1]
		if !continued {
			delete(hdr, HeaderTracestate) // belongs to a trace we did not continue
		}
		r2.Header = hdr

		sw := &statusWriter{ResponseWriter: w}
		defer func() {
			if v := recover(); v != nil {
				t.finish(rec, sw, r2, true)
				panic(v) // net/http aborts the connection
			}
			t.finish(rec, sw, r2, false)
		}()
		next.ServeHTTP(sw, r2)
	})
}

// finish records the request's metrics and writes its log line.
func (t *Telemetry) finish(rec *Request, sw *statusWriter, r *http.Request, panicked bool) {
	duration := t.now().Sub(rec.start)
	status := finalStatus(rec, sw, panicked)
	outcome := failedOutcome(rec, sw, panicked)
	statusLabel := outcome
	if statusLabel == "" {
		statusLabel = strconv.Itoa(status)
	}
	pool := rec.pool
	if pool == "" {
		pool = PoolNone
	}

	m := t.metrics
	if rec.inflight != nil {
		rec.inflight.add(-1)
	}
	m.requests.with(pool, rec.class, statusLabel).inc()
	m.duration.with(pool, rec.class).observe(duration.Seconds())
	// TTFT is a success-path measure: error bodies arrive fast and would
	// flatter the distribution. A response that failed later still had a
	// genuine first byte, so it counts.
	if rec.hasFirstByte && status < http.StatusBadRequest {
		m.ttft.with(pool, rec.class).observe(rec.firstByte.Seconds())
	}
	m.templates.finish(rec, statusLabel, status)

	t.log(rec, sw, r, logSummary{pool: pool, status: status, outcome: outcome, duration: duration})
}

// finalStatus is the HTTP status the caller got on the status line.
func finalStatus(rec *Request, sw *statusWriter, panicked bool) int {
	switch {
	case sw.status != 0:
		return sw.status
	case panicked:
		return statusPanic
	case rec.disconnected:
		return statusClientClosed
	default:
		return http.StatusOK // net/http sends an empty 200 when a handler writes nothing
	}
}

// failedOutcome returns the outcome of a response that did not complete
// after its status line was sent ("" otherwise): aborted when the handler
// panicked mid-response (net/http then aborts the connection, which is how a
// non-streaming body cut off by the replica ends), stream_error when the
// replica failed mid-stream and the stream ended with an error event, and
// client_closed when the caller went away mid-response. A replica failure
// outranks a later disconnect (the caller leaving while its error event was
// written): the failure is what an operator has to act on.
func failedOutcome(rec *Request, sw *statusWriter, panicked bool) string {
	switch {
	case sw.status == 0:
		return ""
	case panicked:
		return OutcomeAborted
	case rec.responseFailed:
		return OutcomeStreamError
	case rec.disconnected:
		return OutcomeClientClosed
	default:
		return ""
	}
}

// logSummary is what finish derived for the log line.
type logSummary struct {
	pool     string
	status   int
	outcome  string
	duration time.Duration
}

// log writes the per-request line: IDs, routing, timings, sizes and status.
// Never the body, prompt, response, query string or headers.
func (t *Telemetry) log(rec *Request, sw *statusWriter, r *http.Request, s logSummary) {
	var attrBuf [32]slog.Attr
	attrs := attrBuf[:0]
	attrs = append(attrs,
		slog.String("trace_id", rec.trace.traceID),
		slog.String("span_id", rec.trace.spanID),
	)
	if rec.trace.parentID != "" {
		attrs = append(attrs, slog.String("parent_span_id", rec.trace.parentID))
	}
	attrs = append(attrs,
		slog.String("caller", rec.caller),
		slog.String("method", r.Method),
		slog.String("path", truncate(r.URL.Path, maxLoggedPathBytes)),
		slog.String("pool", s.pool),
		slog.String("replica", rec.replica),
		slog.String("class", rec.class),
		slog.Int("status", s.status),
		slog.Float64("duration_ms", millis(s.duration)),
		slog.Int64("request_bytes", max(r.ContentLength, 0)),
		slog.Int64("response_bytes", sw.bytes),
	)
	if s.outcome != "" {
		attrs = append(attrs, slog.String("outcome", s.outcome))
	}
	if rec.hasFirstByte {
		attrs = append(attrs, slog.Float64("ttft_ms", millis(rec.firstByte)))
	}
	if rec.retries > 0 {
		attrs = append(attrs, slog.Int("retries", rec.retries))
	}
	if rec.queued {
		attrs = append(attrs, slog.Float64("queue_wait_ms", millis(rec.queueWait)))
	}
	if len(rec.failOpen) > 0 {
		attrs = append(attrs, slog.Any("failopen", rec.failOpen))
	}
	if rec.authRejection != "" {
		attrs = append(attrs, slog.String("auth_rejection", rec.authRejection))
	}
	if rec.upstreamError != "" {
		attrs = append(attrs, slog.String("upstream_error", rec.upstreamError))
	}
	if rec.disconnected {
		attrs = append(attrs, slog.Bool("client_disconnect", true))
	}
	if rec.cacheOutcome != "" {
		attrs = append(attrs, slog.String("cache", rec.cacheOutcome))
	}
	if t := rec.tmpl; t != nil {
		attrs = append(attrs, slog.String("template", t.id), slog.String("template_version", t.version))
		if t.maxTokensSet > 0 {
			attrs = append(attrs, slog.Int("gateway_max_tokens", t.maxTokensSet))
		}
		if t.hasUsage {
			attrs = append(attrs, slog.Int("prompt_tokens", t.promptTokens), slog.Int("completion_tokens", t.completionTokens))
		}
		if t.runaway {
			attrs = append(attrs, slog.Bool("runaway", true))
		}
	}
	// Background context: the trace ID is already on the line, so the
	// trace-aware log handler must not add it twice.
	t.logger.LogAttrs(context.Background(), slog.LevelInfo, LogMessage, attrs...)
}

func millis(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// statusWriter records the status and body size written. Unwrap lets
// http.ResponseController reach the underlying writer's Flush, so streams
// still flush after every event.
type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 && code >= http.StatusOK { // 1xx are informational
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(p)
	w.bytes += int64(n)
	return n, err
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
