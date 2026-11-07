package loadview

import (
	"bufio"
	"bytes"
	"math"
	"strconv"
)

// Series aggregates every sample of one metric name in a scrape. vLLM
// running several engines (data parallel) emits one series per engine,
// e.g. vllm:num_requests_waiting{engine="0"} and {engine="1"}.
type Series struct {
	Sum   float64
	Count int
}

// Mean is Sum / Count, for per-engine fractions such as KV-cache usage.
func (s Series) Mean() float64 {
	if s.Count == 0 {
		return 0
	}
	return s.Sum / float64(s.Count)
}

// ParsePrometheusMetrics parses Prometheus text exposition into metric name
// -> Series, summing samples that share a name. Label sets are parsed with
// their quoting rules, so label values containing spaces, braces or escaped
// quotes do not break a line. OpenMetrics exemplars after the value are
// ignored. Comments, malformed lines and non-finite values (NaN, ±Inf) are
// skipped.
func ParsePrometheusMetrics(raw []byte) map[string]Series {
	return parsePrometheus(raw, nil)
}

// parsePrometheus is ParsePrometheusMetrics keeping only the metric names
// in want (every name when want is nil). An unwanted sample is skipped
// before its labels and value are parsed, without allocating.
func parsePrometheus(raw []byte, want map[string]struct{}) map[string]Series {
	out := make(map[string]Series, len(want))
	sc := bufio.NewScanner(bytes.NewReader(raw))
	sc.Buffer(make([]byte, 0, 64<<10), maxMetricsBodyBytes)
	for sc.Scan() {
		line := sc.Bytes()
		if want != nil {
			name := sampleName(line)
			if name == nil {
				continue
			}
			if _, ok := want[string(name)]; !ok { // no allocation: map index
				continue
			}
		}
		name, val, ok := parseSample(line)
		if !ok || math.IsNaN(val) || math.IsInf(val, 0) {
			continue
		}
		s := out[name]
		s.Sum += val
		s.Count++
		out[name] = s
	}
	return out
}

// sampleName returns the metric name of a sample line, or nil for a
// comment, a blank or a malformed line.
func sampleName(line []byte) []byte {
	i := 0
	for i < len(line) && (line[i] == ' ' || line[i] == '\t') {
		i++
	}
	if i == len(line) || line[i] == '#' {
		return nil
	}
	j := i
	for j < len(line) && line[j] != '{' && line[j] != ' ' && line[j] != '\t' {
		j++
	}
	if j == i || j == len(line) {
		return nil
	}
	return line[i:j]
}

// parseSample parses `name[{labels}] value [timestamp]`.
func parseSample(line []byte) (string, float64, bool) {
	line = bytes.TrimSpace(line)
	if len(line) == 0 || line[0] == '#' {
		return "", 0, false
	}
	i := bytes.IndexAny(line, "{ \t")
	if i <= 0 {
		return "", 0, false
	}
	name, rest := line[:i], line[i:]
	if rest[0] == '{' {
		end, ok := labelsEnd(rest)
		if !ok {
			return "", 0, false
		}
		rest = rest[end:]
	}
	// OpenMetrics exemplar: `value [timestamp] # {labels} value [timestamp]`.
	if j := bytes.Index(rest, []byte(" #")); j >= 0 {
		rest = rest[:j]
	}
	fields := bytes.Fields(rest)
	if len(fields) == 0 || len(fields) > 2 {
		return "", 0, false
	}
	val, err := strconv.ParseFloat(string(fields[0]), 64)
	if err != nil {
		return "", 0, false
	}
	return string(name), val, true
}

// labelsEnd returns the index just past the '}' closing the label set that
// starts at b[0] == '{'. Quoted values may contain any byte; a backslash
// escapes the next one.
func labelsEnd(b []byte) (int, bool) {
	inQuote := false
	for i := 1; i < len(b); i++ {
		switch c := b[i]; {
		case inQuote && c == '\\':
			i++
		case c == '"':
			inQuote = !inQuote
		case !inQuote && c == '}':
			return i + 1, true
		}
	}
	return 0, false
}
