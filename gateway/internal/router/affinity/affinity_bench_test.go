package affinity_test

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/affinity"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/affinity/affinitytest"
)

func BenchmarkHeaderExtraction(b *testing.B) {
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "/", nil)
	req.Header.Set("X-Session-Id", "sess-bench-1234")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			res := affinity.Extract(req, nil)
			if res.Tier != affinity.TierHeader {
				b.Fatal("unexpected tier")
			}
		}
	})
}

func BenchmarkPrefixHashTurn1(b *testing.B) {
	body := []byte(`{
		"model": "meta-llama/Llama-3.1-8B-Instruct",
		"messages": [
			{"role": "system", "content": "You are a helpful assistant."},
			{"role": "user", "content": "Hello world!"}
		]
	}`)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			res := affinity.Extract(nil, body)
			if res.Tier != affinity.TierPrefixHash {
				b.Fatal("unexpected tier")
			}
		}
	})
}

func BenchmarkPrefixHashTurn50(b *testing.B) {
	// Build a 50-turn conversation payload
	turns := `{"role": "system", "content": "You are a helpful assistant."}`
	for i := range 50 {
		turns += fmt.Sprintf(`,{"role": "user", "content": "turn %d question"}, {"role": "assistant", "content": "turn %d reply with some longer padding content to simulate real conversations"}`, i, i)
	}
	body := []byte(fmt.Sprintf(`{"model": "meta-llama/Llama-3.1-8B-Instruct", "messages": [%s]}`, turns))

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			res := affinity.Extract(nil, body)
			if res.Tier != affinity.TierPrefixHash {
				b.Fatal("unexpected tier")
			}
		}
	})
}

// BenchmarkExtractLatency times every Extract call into a preallocated slice
// and reports p50/p99/max per body size (AC-M1-5: route decision ≤ 100 µs p99).
func BenchmarkExtractLatency(b *testing.B) {
	for _, tc := range append(latencyCases(), worstCases()...) {
		b.Run(tc.Name, func(b *testing.B) {
			lat := make([]time.Duration, b.N)
			b.SetBytes(int64(len(tc.Body)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := range lat {
				start := time.Now()
				res := affinity.Extract(nil, tc.Body)
				lat[i] = time.Since(start)
				if res.Tier == "" {
					b.Fatal("empty tier")
				}
			}
			b.StopTimer()
			slices.Sort(lat)
			b.ReportMetric(micros(quantile(lat, 0.50)), "p50-µs")
			b.ReportMetric(micros(quantile(lat, 0.99)), "p99-µs")
			b.ReportMetric(micros(lat[len(lat)-1]), "max-µs")
		})
	}
}

// sweepBudgets are the pre-key budgets BenchmarkExtractBudgetSweep times.
var sweepBudgets = []int{16 << 10, 20 << 10, 24 << 10, 28 << 10, 32 << 10, 36 << 10, 38 << 10, 40 << 10, 44 << 10, 48 << 10, 52 << 10, 64 << 10}

// BenchmarkExtractBudgetSweep times the shapes that spend the whole pre-key
// budget (they fail open) at each candidate budget and reports p50/p99/max,
// as BenchmarkExtractLatency does. It is how the budget was chosen (M1-21):
// run it with -count 5 on the slowest core the gateway must serve and take
// the largest budget whose every run stays well under AC-M1-5's 100 µs.
func BenchmarkExtractBudgetSweep(b *testing.B) {
	var shapes []affinitytest.Case
	for _, tc := range append(latencyCases(), worstCases()...) {
		switch tc.Name {
		case "tools_1MB_before_messages_failopen", "tools_4MB_before_messages_failopen", "system_256KB_escaped_json_failopen",
			// The shapes with the highest p99 on the Cascade Lake pods (M1-24, ADR 0017).
			"empty_strings_1.6MB_before_messages_failopen", "short_objects_1.6MB_before_messages_failopen",
			"bool_members_1.6MB_before_messages_failopen":
			shapes = append(shapes, tc)
		}
	}
	for _, budget := range sweepBudgets {
		for _, tc := range shapes {
			b.Run(fmt.Sprintf("budget_%dK/%s", budget>>10, tc.Name), func(b *testing.B) {
				lat := make([]time.Duration, b.N)
				b.ResetTimer()
				for i := range lat {
					start := time.Now()
					res := affinity.ExtractWithBudget(tc.Body, budget)
					lat[i] = time.Since(start)
					if res.Reason != affinity.ReasonScanBudget {
						b.Fatalf("%s at %d: %+v, want a scan-budget fail-open", tc.Name, budget, res)
					}
				}
				b.StopTimer()
				slices.Sort(lat)
				b.ReportMetric(micros(quantile(lat, 0.50)), "p50-µs")
				b.ReportMetric(micros(quantile(lat, 0.99)), "p99-µs")
				b.ReportMetric(micros(lat[len(lat)-1]), "max-µs")
			})
		}
	}
}

// costCases are every gated shape plus the other places the scanner walks
// (members inside a message, content parts, nested objects, numbers, prompt
// lists), for BenchmarkExtractCostPerUnit.
func costCases() []affinitytest.Case {
	const n = 400_000
	q := `{"role":"user","content":"Q"}`
	extra := []affinitytest.Case{
		{Name: "members_in_leading_message", Body: []byte(`{"messages":[{"role":"system",` + strings.Repeat(`"k":"v",`, n) + `"content":"s"},` + q + `]}`)},
		{Name: "content_parts_in_leading_message", Body: []byte(`{"messages":[{"role":"user","content":[` + strings.Repeat(`{"type":"text","text":"x"},`, n/4) + `{"type":"text","text":"y"}]}]}`)},
		{Name: "short_strings_ahead", Body: affinitytest.ArrayAhead(`"a",`, n)},
		{Name: "nested_objects_ahead", Body: []byte(`{"x":` + strings.Repeat(`{"k":`, n/8) + `1` + strings.Repeat(`}`, n/8) + `,"messages":[` + q + `]}`)},
		{Name: "numbers_ahead", Body: []byte(`{"x":[` + strings.Repeat(`1,`, n) + `2],"messages":[` + q + `]}`)},
		{Name: "prompt_short_strings", Body: []byte(`{"prompt":["first prompt",` + strings.Repeat(`"a",`, n) + `"b"]}`)},
		{Name: "prompt_token_ids", Body: []byte(`{"prompt":[` + strings.Repeat(`1,`, n) + `2]}`)},
		{Name: "adjacent_2byte_strings_ahead", Body: affinitytest.ArrayAhead(`"ab"`, n)},
		{Name: "adjacent_4byte_strings_ahead", Body: affinitytest.ArrayAhead(`"abcd"`, n)},
		{Name: "spaced_3byte_strings_ahead", Body: affinitytest.ArrayAhead(`"abc" `, n)},
		{Name: "spaced_empty_strings_ahead", Body: affinitytest.ArrayAhead(`"" `, n)},
		{Name: "comma_3byte_strings_ahead", Body: affinitytest.ArrayAhead(`"abc",`, n)},
		{Name: "true_members_ahead", Body: affinitytest.ShortMembers(`"k":true,`, n/2)},
		{Name: "null_members_ahead", Body: affinitytest.ShortMembers(`"k":null,`, n/2)},
		{Name: "number_members_ahead", Body: affinitytest.ShortMembers(`"k":1,`, n/2)},
	}
	return append(append(latencyCases(), worstCases()...), extra...)
}

// BenchmarkExtractCostPerUnit reports, per shape, the work units one Extract
// spends and the time each unit takes (p50 of the call time over units).
// A unit should cost about the same time on every path, so that the work
// budget bounds the time: no shape should cost much more per unit than
// tools_1MB_before_messages_failopen, the shape the budget was calibrated
// on (M1-21, M1-22).
func BenchmarkExtractCostPerUnit(b *testing.B) {
	for _, tc := range costCases() {
		b.Run(tc.Name, func(b *testing.B) {
			_, work, _ := affinity.ExtractStats(tc.Body)
			lat := make([]time.Duration, b.N)
			b.ResetTimer()
			for i := range lat {
				start := time.Now()
				affinity.Extract(nil, tc.Body)
				lat[i] = time.Since(start)
			}
			b.StopTimer()
			slices.Sort(lat)
			b.ReportMetric(float64(work), "units")
			b.ReportMetric(float64(quantile(lat, 0.50))/float64(work), "ns/unit")
		})
	}
}

// sweepString returns n bytes of plain text (no quote or backslash).
func sweepString(n int) string {
	return strings.Repeat("abcdefghijklmnopqrstuvwxyz0123456789", n/36+1)[:n]
}

// sweepBody repeats item until the body holds about 1.6 MB of it, between
// prefix and suffix, so every point spends the whole budget.
func sweepBody(prefix, item, suffix string) []byte {
	return []byte(prefix + strings.Repeat(item, max(1_600_000/len(item), 1_000)) + suffix)
}

// costSweepPoints are the points of BenchmarkExtractCostSweep: a string of
// every length from 0 to 40 bytes in each place a string can sit (array
// element, top-level member key, member value, the content of short system
// turns, a prompt list), and numbers of 1 to 20 digits and the three
// literals as member values and as array elements. ref is the calibrated
// shape, measured in the same run.
func costSweepPoints() []affinitytest.Case {
	user := `"messages":[{"role":"user","content":"Q"}]}`
	pts := []affinitytest.Case{{Name: "ref/tools_1MB_before_messages_failopen", Body: affinitytest.Chat(`"tools":`+affinitytest.DenseTools(1<<20)+`,`, 1<<10, 3)}}
	for n := 0; n <= 40; n++ {
		s := `"` + sweepString(n) + `"`
		pts = append(pts,
			affinitytest.Case{Name: fmt.Sprintf("str_array/n%02d", n), Body: sweepBody(`{"x":[`, s+",", `""],`+user)},
			affinitytest.Case{Name: fmt.Sprintf("str_key/n%02d", n), Body: sweepBody(`{"model":"m",`, s+":1,", user)},
			affinitytest.Case{Name: fmt.Sprintf("str_value/n%02d", n), Body: sweepBody(`{"model":"m",`, `"k":`+s+",", user)},
			affinitytest.Case{Name: fmt.Sprintf("str_content/n%02d", n), Body: sweepBody(`{"messages":[`, `{"role":"system","content":`+s+`},`, `{"role":"user","content":"Q"}]}`)},
			affinitytest.Case{Name: fmt.Sprintf("str_prompt/n%02d", n), Body: sweepBody(`{"prompt":["first prompt text",`, s+",", `""]}`)},
		)
	}
	scalar := func(name, v string) {
		pts = append(pts,
			affinitytest.Case{Name: "scalar_value/" + name, Body: sweepBody(`{"model":"m",`, `"k":`+v+",", user)},
			affinitytest.Case{Name: "scalar_array/" + name, Body: sweepBody(`{"x":[`, v+",", `0],`+user)},
		)
	}
	for d := 1; d <= 20; d++ {
		scalar(fmt.Sprintf("d%02d", d), strings.Repeat("7", d))
	}
	for _, lit := range []string{"true", "false", "null"} {
		scalar(lit, lit)
	}
	// Stretches of structural bytes between strings, by length: skipDepth
	// reads them one way to shortRun and another past it (M1-28). The shapes
	// above stop at 3-byte stretches. digits are plain bytes, brackets the
	// patterns the byte loop branches on, and the k_ points are members of a
	// skipped object, as in a JSON schema's "strict":true.
	for _, n := range []int{4, 5, 6, 7, 8, 9, 10, 12, 16, 24} {
		item := `"",` + strings.Repeat("1", n-2) + `,`
		pts = append(pts, affinitytest.Case{Name: fmt.Sprintf("stretch_digits/n%02d", n), Body: sweepBody(`{"x":[`, item, `""],`+user)})
	}
	for _, reps := range []int{1, 2, 3, 4, 8} { // stretches of 1+3n bytes
		item := `"",` + strings.Repeat(`[],`, reps)
		pts = append(pts, affinitytest.Case{Name: fmt.Sprintf("stretch_brackets/n%02d", 1+3*reps), Body: sweepBody(`{"x":[`, item, `""],`+user)})
	}
	for _, v := range []string{"true", "1234", "false", "12345678", "[1,2,3]", "{}", "null"} {
		name := strings.NewReplacer("[1,2,3]", "list", "{}", "object").Replace(v)
		pts = append(pts, affinitytest.Case{Name: "stretch_members/" + name, Body: sweepBody(`{"x":{`, `"k":`+v+`,`, `"z":0},`+user)})
	}
	return pts
}

// BenchmarkExtractCostSweep reports the time per work unit (p50 of the call
// time over the units spent) at every point of costSweepPoints, so the
// prices can be set from the whole curve rather than shape by shape (M1-22).
// scripts/bench_p99.py --sweep summarises it against the ref point.
func BenchmarkExtractCostSweep(b *testing.B) {
	for _, tc := range costSweepPoints() {
		_, work, _ := affinity.ExtractStats(tc.Body)
		b.Run(tc.Name, func(b *testing.B) {
			lat := make([]time.Duration, b.N)
			b.ResetTimer()
			for i := range lat {
				start := time.Now()
				affinity.Extract(nil, tc.Body)
				lat[i] = time.Since(start)
			}
			b.StopTimer()
			slices.Sort(lat)
			b.ReportMetric(float64(work), "units")
			b.ReportMetric(float64(quantile(lat, 0.50))/float64(work), "ns/unit")
		})
	}
}
