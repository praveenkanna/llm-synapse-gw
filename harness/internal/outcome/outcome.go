// Package outcome is the one definition of what a harness request ended as
// (ADR 0021): the error kinds, the success rule, completeness, TTFT, the
// percentile method for verdicts, the burst-window rule and the results
// record that the load generator writes and every reader shares.
//
// A response body is measured as it arrives (Body), so a stream's TTFT and
// inter-token gaps are taken when their bytes land, and the facts readers
// need (ok, complete, both TTFTs, the burst window) are written into each
// record rather than re-derived by each reader. Only kinds and numbers are
// recorded: never prompt or response text.
package outcome

import (
	"math"
	"slices"
	"strconv"
	"time"
)

// Error kinds recorded per request; "" means the request succeeded.
const (
	ErrDropped     = "dropped"      // not sent: in-flight ceiling reached (open loop)
	ErrConnect     = "connect"      // dial or TLS failure
	ErrTimeout     = "timeout"      // per-request timeout
	ErrCanceled    = "canceled"     // run interrupted
	ErrHTTPStatus  = "http_status"  // non-2xx response
	ErrStreamError = "stream_error" // SSE error event from upstream
	ErrTruncated   = "truncated"    // the response ended before the generation finished
	ErrRead        = "read"         // response body read failed
	ErrBadResponse = "bad_response" // body is not the expected JSON
	ErrRequest     = "request"      // any other transport failure
)

// OK is the success rule: a 2xx status and no error kind, so no transport
// failure, no in-stream error event, and a complete response.
func OK(status int, errKind string) bool { return errKind == "" && status >= 200 && status < 300 }

// StatusKey names a request's outcome for status counts: the HTTP status
// ("200", "503"), or "error:<kind>" when the request failed.
func StatusKey(status int, errKind string) string {
	if errKind != "" {
		return "error:" + errKind
	}
	return strconv.Itoa(status)
}

// Stats is a latency distribution in milliseconds.
type Stats struct {
	Count  uint64  `json:"count"`
	MeanMS float64 `json:"mean_ms"`
	P50MS  float64 `json:"p50_ms"`
	P95MS  float64 `json:"p95_ms"`
	P99MS  float64 `json:"p99_ms"`
	MaxMS  float64 `json:"max_ms"`
}

// Percentiles returns the nearest-rank percentiles of values (ms), the
// method every verdict uses. It sorts values in place.
func Percentiles(values []float64) Stats {
	n := len(values)
	if n == 0 {
		return Stats{}
	}
	slices.Sort(values)
	var sum float64
	for _, v := range values {
		sum += v
	}
	return Stats{
		Count: uint64(n), MeanMS: math.Round(sum/float64(n)*1000) / 1000,
		P50MS: Rank(values, 0.50), P95MS: Rank(values, 0.95), P99MS: Rank(values, 0.99), MaxMS: values[n-1],
	}
}

// Rank returns the nearest-rank q-quantile of sorted (0 < q <= 1): the
// smallest value with at least q of the samples at or below it.
func Rank(sorted []float64, q float64) float64 {
	n := len(sorted)
	if n == 0 {
		return math.NaN()
	}
	i := int(math.Ceil(q*float64(n))) - 1
	return sorted[min(max(i, 0), n-1)]
}

// BurstWindows are periodic windows of a raised arrival rate: they start at
// k*Every for k >= 1 and last Length. A zero Every means none.
type BurstWindows struct {
	Every  time.Duration
	Length time.Duration
}

// Contains reports whether the scheduled arrival offset at falls in a window.
func (w BurstWindows) Contains(at time.Duration) bool {
	if w.Every <= 0 || at < w.Every {
		return false
	}
	return at%w.Every < w.Length
}

// MS converts a duration to fractional milliseconds rounded to microseconds,
// the unit of every record and summary.
func MS(d time.Duration) float64 {
	return math.Round(float64(d)/float64(time.Microsecond)) / 1000
}
