package telemetry

import (
	"bufio"
	"io"
	"maps"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Minimal Prometheus primitives (text exposition format 0.0.4), standard
// library only. Updates are lock-free on the hot path: counters, gauges and
// histogram buckets are atomics, histogram sums are striped across cache
// lines, and series lookup uses copy-on-write atomic maps keyed by fixed-arity
// string arrays (0 B/op, 0 allocs/op) once a label combination has been seen.

// MaxSeriesPerMetric bounds the label combinations one metric may hold. Label
// values come from configuration (pools, replicas) and fixed enums, so real
// use stays far below it; it exists so no map grows without bound (for
// example across many replica hot-reloads). Further combinations are counted
// under a single series whose labels are all OverflowLabelValue.
const MaxSeriesPerMetric = 1000

// OverflowLabelValue labels the catch-all series of a metric at its ceiling.
const OverflowLabelValue = "_overflow"

// labelKeySep joins label values into a map key; it cannot appear in UTF-8.
const labelKeySep = "\xff"

const (
	typeCounter   = "counter"
	typeGauge     = "gauge"
	typeHistogram = "histogram"
)

// counter is a monotonically increasing count.
type counter struct{ v atomic.Uint64 }

func (c *counter) inc()         { c.v.Add(1) }
func (c *counter) add(n uint64) { c.v.Add(n) }

// gauge is a value that goes up and down.
type gauge struct{ v atomic.Int64 }

func (g *gauge) add(d int64) { g.v.Add(d) }
func (g *gauge) set(v int64) { g.v.Store(v) }

const histSumShards = 8

type paddedSumShard struct {
	bits atomic.Uint64
	_    [56]byte // isolate each shard on its own 64-byte cache line
}

// histogram is a fixed-bucket histogram. counts are per bucket
// (non-cumulative); the last one is +Inf. sumShards stripes the float64 sum
// across cache lines so concurrent observations do not spin on one CAS word.
type histogram struct {
	bounds    []float64
	counts    []atomic.Uint64
	sumShards [histSumShards]paddedSumShard
}

func newHistogram(bounds []float64) *histogram {
	return &histogram{bounds: bounds, counts: make([]atomic.Uint64, len(bounds)+1)}
}

func (h *histogram) observe(v float64) {
	b := h.bounds
	lo, hi := 0, len(b)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if b[mid] < v {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	c := h.counts[lo].Add(1)
	s0 := &h.sumShards[0].bits
	if old := s0.Load(); s0.CompareAndSwap(old, math.Float64bits(math.Float64frombits(old)+v)) {
		return
	}
	shard := &h.sumShards[c&(histSumShards-1)].bits
	for {
		old := shard.Load()
		if shard.CompareAndSwap(old, math.Float64bits(math.Float64frombits(old)+v)) {
			return
		}
	}
}

func (h *histogram) sum() float64 {
	var total, comp float64
	for i := range h.sumShards {
		x := math.Float64frombits(h.sumShards[i].bits.Load())
		t := total + x
		if math.Abs(total) >= math.Abs(x) {
			comp += (total - t) + x
		} else {
			comp += (x - t) + total
		}
		total = t
	}
	return total + comp
}

// series is one label combination of a metric.
type series[T any] struct {
	labels []string
	val    *T
}

// family is a metric with a fixed set of label names.
// Arities 0..3 (every metric in the gateway) use lock-free copy-on-write
// atomic maps keyed by fixed string arrays so hot-path lookups allocate
// nothing; higher arities fall back to sync.Map.
type family[T any] struct {
	name, help, typ string
	labelNames      []string
	newVal          func() *T
	maxSeries       int64

	mu   sync.Mutex
	s0   atomic.Pointer[series[T]]
	m1   atomic.Pointer[map[string]*series[T]]
	m2   atomic.Pointer[map[[2]string]*series[T]]
	m3   atomic.Pointer[map[[3]string]*series[T]]
	more sync.Map // fallback for >3 labels or mismatched test arity
	n    atomic.Int64
	over atomic.Pointer[series[T]]
}

func newFamily[T any](name, help, typ string, labelNames []string, newVal func() *T) *family[T] {
	return &family[T]{name: name, help: help, typ: typ, labelNames: labelNames, newVal: newVal, maxSeries: MaxSeriesPerMetric}
}

// with returns the value for the given label values (one per label name),
// creating it if needed, or the overflow series once the ceiling is reached.
func (f *family[T]) with(values ...string) *T {
	if len(values) == len(f.labelNames) {
		switch len(values) {
		case 0:
			if s := f.s0.Load(); s != nil {
				return s.val
			}
			return f.with0()
		case 1:
			if m := f.m1.Load(); m != nil {
				if s, ok := (*m)[values[0]]; ok {
					return s.val
				}
			}
			return f.with1(values[0])
		case 2:
			k := [2]string{values[0], values[1]}
			if m := f.m2.Load(); m != nil {
				if s, ok := (*m)[k]; ok {
					return s.val
				}
			}
			return f.with2(k)
		case 3:
			k := [3]string{values[0], values[1], values[2]}
			if m := f.m3.Load(); m != nil {
				if s, ok := (*m)[k]; ok {
					return s.val
				}
			}
			return f.with3(k)
		}
	}
	return f.withFallback(values)
}

func (f *family[T]) with0() *T {
	f.mu.Lock()
	defer f.mu.Unlock()
	if s := f.s0.Load(); s != nil {
		return s.val
	}
	if f.n.Load() >= f.maxSeries {
		return f.overflow()
	}
	s := &series[T]{val: f.newVal()}
	f.n.Add(1)
	f.s0.Store(s)
	return s.val
}

func (f *family[T]) with1(k string) *T {
	f.mu.Lock()
	defer f.mu.Unlock()
	cur := f.m1.Load()
	if cur != nil {
		if s, ok := (*cur)[k]; ok {
			return s.val
		}
	}
	if f.n.Load() >= f.maxSeries {
		return f.overflow()
	}
	next := make(map[string]*series[T], mapCap(cur))
	if cur != nil {
		maps.Copy(next, *cur)
	}
	s := &series[T]{labels: []string{k}, val: f.newVal()}
	next[k] = s
	f.n.Add(1)
	f.m1.Store(&next)
	return s.val
}

func (f *family[T]) with2(k [2]string) *T {
	f.mu.Lock()
	defer f.mu.Unlock()
	cur := f.m2.Load()
	if cur != nil {
		if s, ok := (*cur)[k]; ok {
			return s.val
		}
	}
	if f.n.Load() >= f.maxSeries {
		return f.overflow()
	}
	next := make(map[[2]string]*series[T], mapCap(cur))
	if cur != nil {
		maps.Copy(next, *cur)
	}
	s := &series[T]{labels: []string{k[0], k[1]}, val: f.newVal()}
	next[k] = s
	f.n.Add(1)
	f.m2.Store(&next)
	return s.val
}

func (f *family[T]) with3(k [3]string) *T {
	f.mu.Lock()
	defer f.mu.Unlock()
	cur := f.m3.Load()
	if cur != nil {
		if s, ok := (*cur)[k]; ok {
			return s.val
		}
	}
	if f.n.Load() >= f.maxSeries {
		return f.overflow()
	}
	next := make(map[[3]string]*series[T], mapCap(cur))
	if cur != nil {
		maps.Copy(next, *cur)
	}
	s := &series[T]{labels: []string{k[0], k[1], k[2]}, val: f.newVal()}
	next[k] = s
	f.n.Add(1)
	f.m3.Store(&next)
	return s.val
}

func mapCap[K comparable, V any](m *map[K]V) int {
	if m == nil {
		return 4
	}
	return len(*m) + 1
}

func (f *family[T]) withFallback(values []string) *T {
	key := strings.Join(values, labelKeySep)
	if s, ok := f.more.Load(key); ok {
		return s.(*series[T]).val
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if s, ok := f.more.Load(key); ok {
		return s.(*series[T]).val
	}
	if f.n.Load() >= f.maxSeries {
		return f.overflow()
	}
	labels := make([]string, len(values))
	copy(labels, values)
	s := &series[T]{labels: labels, val: f.newVal()}
	f.more.Store(key, s)
	f.n.Add(1)
	return s.val
}

// remove deletes the series with the given label values, if present, so
// it is no longer exported (for example an info metric whose value changed).
func (f *family[T]) remove(values ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(values) == len(f.labelNames) {
		switch len(values) {
		case 0:
			if f.s0.Swap(nil) != nil {
				f.n.Add(-1)
			}
			return
		case 1:
			cur := f.m1.Load()
			if cur == nil {
				return
			}
			if _, ok := (*cur)[values[0]]; !ok {
				return
			}
			next := maps.Clone(*cur)
			delete(next, values[0])
			f.n.Add(-1)
			f.m1.Store(&next)
			return
		case 2:
			cur := f.m2.Load()
			k := [2]string{values[0], values[1]}
			if cur == nil {
				return
			}
			if _, ok := (*cur)[k]; !ok {
				return
			}
			next := maps.Clone(*cur)
			delete(next, k)
			f.n.Add(-1)
			f.m2.Store(&next)
			return
		case 3:
			cur := f.m3.Load()
			k := [3]string{values[0], values[1], values[2]}
			if cur == nil {
				return
			}
			if _, ok := (*cur)[k]; !ok {
				return
			}
			next := maps.Clone(*cur)
			delete(next, k)
			f.n.Add(-1)
			f.m3.Store(&next)
			return
		}
	}
	if _, ok := f.more.LoadAndDelete(strings.Join(values, labelKeySep)); ok {
		f.n.Add(-1)
	}
}

// removeIf deletes every series (overflow excluded) for which drop returns
// true. drop sees the series' label values in labelNames order.
func (f *family[T]) removeIf(drop func(labels []string, val *T) bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch len(f.labelNames) {
	case 0:
		if s := f.s0.Load(); s != nil && drop(s.labels, s.val) {
			f.s0.Store(nil)
			f.n.Add(-1)
		}
	case 1:
		if cur := f.m1.Load(); cur != nil {
			var next map[string]*series[T]
			var removed int64
			for k, s := range *cur {
				if drop(s.labels, s.val) {
					if next == nil {
						next = maps.Clone(*cur)
					}
					delete(next, k)
					removed++
				}
			}
			if removed > 0 {
				f.n.Add(-removed)
				f.m1.Store(&next)
			}
		}
	case 2:
		if cur := f.m2.Load(); cur != nil {
			var next map[[2]string]*series[T]
			var removed int64
			for k, s := range *cur {
				if drop(s.labels, s.val) {
					if next == nil {
						next = maps.Clone(*cur)
					}
					delete(next, k)
					removed++
				}
			}
			if removed > 0 {
				f.n.Add(-removed)
				f.m2.Store(&next)
			}
		}
	case 3:
		if cur := f.m3.Load(); cur != nil {
			var next map[[3]string]*series[T]
			var removed int64
			for k, s := range *cur {
				if drop(s.labels, s.val) {
					if next == nil {
						next = maps.Clone(*cur)
					}
					delete(next, k)
					removed++
				}
			}
			if removed > 0 {
				f.n.Add(-removed)
				f.m3.Store(&next)
			}
		}
	}
	f.more.Range(func(key, v any) bool {
		s := v.(*series[T])
		if drop(s.labels, s.val) && f.more.CompareAndDelete(key, v) {
			f.n.Add(-1)
		}
		return true
	})
}

func (f *family[T]) overflow() *T {
	if s := f.over.Load(); s != nil {
		return s.val
	}
	labels := make([]string, len(f.labelNames))
	for i := range labels {
		labels[i] = OverflowLabelValue
	}
	f.over.CompareAndSwap(nil, &series[T]{labels: labels, val: f.newVal()})
	return f.over.Load().val
}

// seriesCount returns the number of stored series, excluding overflow.
func (f *family[T]) seriesCount() int { return int(f.n.Load()) }

// snapshot returns every series (overflow last), ordered by label values.
func (f *family[T]) snapshot() []*series[T] {
	var out []*series[T]
	if s := f.s0.Load(); s != nil {
		out = append(out, s)
	}
	if m := f.m1.Load(); m != nil {
		out = slices.Grow(out, len(*m))
		for _, s := range *m {
			out = append(out, s)
		}
	}
	if m := f.m2.Load(); m != nil {
		out = slices.Grow(out, len(*m))
		for _, s := range *m {
			out = append(out, s)
		}
	}
	if m := f.m3.Load(); m != nil {
		out = slices.Grow(out, len(*m))
		for _, s := range *m {
			out = append(out, s)
		}
	}
	f.more.Range(func(_, v any) bool {
		out = append(out, v.(*series[T]))
		return true
	})
	sort.Slice(out, func(i, j int) bool {
		return compareLabels(out[i].labels, out[j].labels) < 0
	})
	if over := f.over.Load(); over != nil {
		out = append(out, over)
	}
	return out
}

func compareLabels(a, b []string) int {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return len(a) - len(b)
}

func (f *family[T]) writeHeader(w *bufio.Writer) {
	w.WriteString("# HELP ")
	w.WriteString(f.name)
	w.WriteByte(' ')
	w.WriteString(f.help)
	w.WriteString("\n# TYPE ")
	w.WriteString(f.name)
	w.WriteByte(' ')
	w.WriteString(f.typ)
	w.WriteByte('\n')
}

// counterFamily and friends write their own sample lines.
type (
	counterFamily   struct{ *family[counter] }
	gaugeFamily     struct{ *family[gauge] }
	histogramFamily struct{ *family[histogram] }
)

func newCounterFamily(name, help string, labelNames ...string) counterFamily {
	return counterFamily{newFamily(name, help, typeCounter, labelNames, func() *counter { return new(counter) })}
}

func newGaugeFamily(name, help string, labelNames ...string) gaugeFamily {
	return gaugeFamily{newFamily(name, help, typeGauge, labelNames, func() *gauge { return new(gauge) })}
}

func newHistogramFamily(name, help string, bounds []float64, labelNames ...string) histogramFamily {
	return histogramFamily{newFamily(name, help, typeHistogram, labelNames, func() *histogram { return newHistogram(bounds) })}
}

func (f counterFamily) write(w *bufio.Writer) {
	f.writeHeader(w)
	for _, s := range f.snapshot() {
		writeSample(w, f.name, f.labelNames, s.labels, "", "", strconv.FormatUint(s.val.v.Load(), 10))
	}
}

func (f gaugeFamily) write(w *bufio.Writer) {
	f.writeHeader(w)
	for _, s := range f.snapshot() {
		writeSample(w, f.name, f.labelNames, s.labels, "", "", strconv.FormatInt(s.val.v.Load(), 10))
	}
}

func (f histogramFamily) write(w *bufio.Writer) {
	f.writeHeader(w)
	for _, s := range f.snapshot() {
		h := s.val
		var cum uint64
		for i := range h.counts {
			cum += h.counts[i].Load()
			le := "+Inf"
			if i < len(h.bounds) {
				le = formatFloat(h.bounds[i])
			}
			writeSample(w, f.name+"_bucket", f.labelNames, s.labels, "le", le, strconv.FormatUint(cum, 10))
		}
		// count is the cumulative total so it always equals the +Inf bucket.
		writeSample(w, f.name+"_sum", f.labelNames, s.labels, "", "", formatFloat(h.sum()))
		writeSample(w, f.name+"_count", f.labelNames, s.labels, "", "", strconv.FormatUint(cum, 10))
	}
}

// writeSample writes `name{l1="v1",...,extra="x"} value`.
func writeSample(w *bufio.Writer, name string, names, values []string, extraName, extraValue, value string) {
	w.WriteString(name)
	if len(names) > 0 || extraName != "" {
		w.WriteByte('{')
		for i, n := range names {
			if i > 0 {
				w.WriteByte(',')
			}
			writeLabel(w, n, values[i])
		}
		if extraName != "" {
			if len(names) > 0 {
				w.WriteByte(',')
			}
			writeLabel(w, extraName, extraValue)
		}
		w.WriteByte('}')
	}
	w.WriteByte(' ')
	w.WriteString(value)
	w.WriteByte('\n')
}

func writeLabel(w *bufio.Writer, name, value string) {
	w.WriteString(name)
	w.WriteString(`="`)
	if !strings.ContainsAny(value, "\\\"\n") {
		w.WriteString(value)
	} else {
		w.WriteString(labelEscaper.Replace(value))
	}
	w.WriteByte('"')
}

var labelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

func formatFloat(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	case math.IsNaN(v):
		return "NaN"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// writer is a metric family that can render itself.
type writer interface{ write(*bufio.Writer) }

// writeAll renders families in order to out.
func writeAll(out io.Writer, families []writer) error {
	w := bufio.NewWriter(out)
	for _, f := range families {
		f.write(w)
	}
	return w.Flush()
}
