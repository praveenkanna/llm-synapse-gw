package ingress

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/arbitrator"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/mockllm"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry/telemetrytest"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

func decodeObject(t testing.TB, b []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("not a JSON object: %v: %q", err, b)
	}
	return m
}

var spliceBodies = []string{
	`{"model":"m","priority":5,"messages":[{"role":"user","content":"x"}]}`,
	`{"priority":5,"model":"m"}`,
	`{"model":"m","priority":5}`,
	`{ "priority" : -3 , "model":"m" }`,
	`{"priority":5}`,
	`{"model":"m",  "priority":  7.5  }`,
	`{"model":"m","messages":[{"content":"\"priority\":1,"}],"priority":1}`,
}

// indexed reads body into a fresh index, as handleGenerate does.
func indexed(t testing.TB, body []byte) (*bodyindex.Index, bodyInfo) {
	t.Helper()
	ix := new(bodyindex.Index)
	info, ok := readBody(ix, body)
	if !ok {
		t.Fatalf("readBody(%q) failed", body)
	}
	return ix, info
}

// freshIndex fails t unless ix indexes body as a fresh read would: a
// splice leaves the index right for the next one.
func freshIndex(t testing.TB, ix *bodyindex.Index, body []byte) {
	t.Helper()
	var fresh bodyindex.Index
	if err := fresh.Read(body); err != nil || !bytes.Equal(ix.Body(), body) || ix.Len() != fresh.Len() {
		t.Fatalf("index holds %q (%d members), the body is %q (%d, %v)", ix.Body(), ix.Len(), body, fresh.Len(), err)
	}
	for i := range ix.Len() {
		if ix.Member(i) != fresh.Member(i) {
			t.Fatalf("member %d = %+v, a fresh read gives %+v, body %q", i, ix.Member(i), fresh.Member(i), body)
		}
	}
}

func TestStripPriorityKeepsEverythingElse(t *testing.T) {
	for _, body := range spliceBodies {
		ix, info := indexed(t, []byte(body))
		if info.priorities != 1 {
			t.Fatalf("scan %q: %d priorities", body, info.priorities)
		}
		got := stripPriority(ix)
		want := decodeObject(t, []byte(body))
		delete(want, "priority")
		if g := decodeObject(t, got); !reflect.DeepEqual(g, want) {
			t.Errorf("strip %q = %q", body, got)
		}
		freshIndex(t, ix, got)
	}
}

func TestInjectPriorityKeepsEverythingElse(t *testing.T) {
	for _, body := range []string{`{"model":"m","messages":[{"role":"user","content":"x"}]}`, `{}`, ` { } `, `{ "model" : "m" }`} {
		ix, info := indexed(t, []byte(body))
		if info.priorities != 0 {
			t.Fatalf("scan %q: %d priorities", body, info.priorities)
		}
		open := ix.Open()
		got := insertFirst(ix, ix.Body(), priorityMember(20))
		want := decodeObject(t, []byte(body))
		want["priority"] = float64(20)
		if g := decodeObject(t, got); !reflect.DeepEqual(g, want) {
			t.Errorf("inject %q = %q", body, got)
		}
		if !bytes.HasSuffix(got, []byte(body)[open+1:]) {
			t.Errorf("inject %q changed the bytes after the new member: %q", body, got)
		}
		freshIndex(t, ix, got)
	}
}

func FuzzPrioritySplice(f *testing.F) {
	for _, s := range spliceBodies {
		f.Add([]byte(s))
	}
	f.Add([]byte(`{"model":"m"}`))
	f.Fuzz(func(t *testing.T, body []byte) {
		var ix bodyindex.Index
		info, ok := readBody(&ix, body)
		if !ok {
			return
		}
		var want map[string]any
		if json.Unmarshal(body, &want) != nil {
			return
		}
		switch info.priorities {
		case 0:
			got := insertFirst(&ix, ix.Body(), priorityMember(-7))
			want["priority"] = float64(-7)
			if g := decodeObject(t, got); !reflect.DeepEqual(g, want) {
				t.Fatalf("inject %q = %q", body, got)
			}
			freshIndex(t, &ix, got)
		case 1:
			got := stripPriority(&ix)
			delete(want, "priority")
			if g := decodeObject(t, got); !reflect.DeepEqual(g, want) {
				t.Fatalf("strip %q = %q", body, got)
			}
			freshIndex(t, &ix, got)
			replaced := insertFirst(&ix, got, priorityMember(-7))
			want["priority"] = float64(-7)
			if g := decodeObject(t, replaced); !reflect.DeepEqual(g, want) {
				t.Fatalf("replace %q = %q", body, replaced)
			}
			freshIndex(t, &ix, replaced)
		}
	})
}

// TestPriorityVLLMReadsAsZeroIsKept (E4): the priority rule uses the
// gateway's one zero rule (bodyindex.IsZeroNumber, ParseFloat's), so a
// literal that underflows to zero ("1e-400"), which vLLM reads as 0 and
// accepts under any scheduling policy, is forwarded as it is, like "0",
// on a pool without priority scheduling and on one whose class sends none.
func TestPriorityVLLMReadsAsZeroIsKept(t *testing.T) {
	m := telemetry.NewMetrics()
	h := newFixture(t, upstreamValuesYAML, Options{Metrics: m, PriorityVerified: func(string) bool { return true }}).h
	h.priorityPools["large"] = true
	for _, tc := range []struct{ pool, class, body string }{
		{"small", config.PriorityUser, `{"model":"small-model","priority":1e-400}`},
		{"small", config.PriorityUser, `{"model":"small-model","priority":-1e-400,"max_tokens":1}`},
		{"large", config.PriorityUser, `{"priority":1e-400,"model":"large-model"}`},
		{"large", config.PriorityUser, `{"model":"large-model","priority":0e7}`},
	} {
		ix, info := indexed(t, []byte(tc.body))
		if got := h.upstreamBody(nil, tc.pool, tc.class, ix, info, true); string(got) != tc.body {
			t.Errorf("%s pool, %s: sent %q, want it unchanged", tc.pool, tc.body, got)
		}
	}
	if n := telemetrytest.Sum(t, m, telemetry.MetricUpstreamPriority); n != 0 {
		t.Fatalf("%v priorities counted as changed, want none", n)
	}
	// A priority vLLM reads as non-zero is still stripped.
	ix, info := indexed(t, []byte(`{"model":"small-model","priority":5e-324}`))
	if got := h.upstreamBody(nil, "small", config.PriorityUser, ix, info, true); string(got) != `{"model":"small-model"}` {
		t.Fatalf("the smallest non-zero priority: sent %q, want it stripped", got)
	}
}

const upstreamValuesYAML = `
priority:
  upstream_values: {interactive: -10, user: 0, background: 10}
`

// TestNewTakesPriorityPoolsFromConfig: pools[].priority_scheduling reaches
// the handler through config's derived set (the other priority tests set
// the handler's set directly).
func TestNewTakesPriorityPoolsFromConfig(t *testing.T) {
	cfg := parseConfig(t, `
pools:
  - {name: large, priority_scheduling: true, replicas: [{name: large-0, url: "http://127.0.0.1:1"}]}
  - {name: small, replicas: [{name: small-0, url: "http://127.0.0.1:2"}]}
default_pool: large
auth: {enabled: false}
`+upstreamValuesYAML)
	disc, err := discovery.NewStatic(cfg.Pools)
	if err != nil {
		t.Fatal(err)
	}
	if h := New(cfg, disc, upstream.New(cfg.Upstream), roundRobin(cfg, disc, upstream.New(cfg.Upstream), nil), Options{}); !reflect.DeepEqual(h.priorityPools, map[string]bool{"large": true}) {
		t.Fatalf("priority pools = %v, want the pool with priority_scheduling", h.priorityPools)
	}
}

func TestPriorityInjectedOnlyOnVerifiedFlaggedPool(t *testing.T) {
	m := telemetry.NewMetrics()
	f := newFixtureWith(t, upstreamValuesYAML, Options{PriorityVerified: func(string) bool { return true }, Metrics: m},
		func(c *mockllm.Config) { c.SchedulingPolicy = mockllm.PolicyPriority })
	f.h.priorityPools["large"] = true // as pools[].priority_scheduling: true
	msgs := `"messages":[{"role":"user","content":"keep \"me\" exactly"}]`
	body := []byte(`{"model":"large-model",` + msgs + `,"max_tokens":1}`)

	for class, want := range map[string]string{"background": `{"priority":10,`, "user": ""} {
		f.large0.Mock().ResetRequests()
		f.large1.Mock().ResetRequests()
		resp, got := f.post(t, PathChatCompletions, body, map[string]string{HeaderPriority: class, mockllm.HeaderOutputTokens: "1"})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: status %d: %s", class, resp.StatusCode, got)
		}
		reqs := append(f.large0.Mock().Requests(), f.large1.Mock().Requests()...)
		if len(reqs) != 1 {
			t.Fatalf("%s: %d upstream requests", class, len(reqs))
		}
		up := reqs[0].Body
		if want == "" {
			if !bytes.Equal(up, body) {
				t.Errorf("%s: body changed for a zero priority: %q", class, up)
			}
			continue
		}
		if !bytes.Equal(up, append([]byte(want), body[1:]...)) {
			t.Errorf("%s: upstream body %q, want %q + original", class, up, want)
		}
		if !bytes.Contains(up, []byte(msgs)) {
			t.Errorf("%s: messages bytes changed", class)
		}
	}

}

func TestCallerPriorityNeverOverridesClassOnFlaggedPool(t *testing.T) {
	m := telemetry.NewMetrics()
	f := newFixtureWith(t, upstreamValuesYAML, Options{PriorityVerified: func(string) bool { return true }, Metrics: m},
		func(c *mockllm.Config) { c.SchedulingPolicy = mockllm.PolicyPriority })
	f.h.priorityPools["large"] = true
	cases := []struct{ class, in, want string }{
		{"background", `{"model":"large-model","priority":3,"max_tokens":1}`, `{"priority":10,"model":"large-model","max_tokens":1}`},
		{"background", `{"priority":-1000,"model":"large-model"}`, `{"priority":10,"model":"large-model"}`},
		{"background", `{"model":"large-model","priority":"high"}`, `{"priority":10,"model":"large-model"}`},
		{"background", `{"model":"large-model","priority":10}`, `{"model":"large-model","priority":10}`},
		{"user", `{"model":"large-model","priority":-1000}`, `{"model":"large-model"}`},
		{"user", `{"model":"large-model","priority":0}`, `{"model":"large-model","priority":0}`},
	}
	for _, c := range cases {
		f.large0.Mock().ResetRequests()
		f.large1.Mock().ResetRequests()
		resp, got := f.post(t, PathChatCompletions, []byte(c.in), map[string]string{HeaderPriority: c.class, mockllm.HeaderOutputTokens: "1"})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s %s: status %d: %s", c.class, c.in, resp.StatusCode, got)
		}
		reqs := append(f.large0.Mock().Requests(), f.large1.Mock().Requests()...)
		if len(reqs) != 1 || string(reqs[0].Body) != c.want {
			t.Errorf("%s %s: upstream %q, want %q", c.class, c.in, reqsBody(reqs), c.want)
		}
	}
	rec := httptest.NewRecorder()
	m.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for outcome, want := range map[string]int{PriorityReplaced: 3, PriorityStripped: 1} {
		line := fmt.Sprintf("%s{pool=\"large\",outcome=%q} %d\n", telemetry.MetricUpstreamPriority, outcome, want)
		if !strings.Contains(rec.Body.String(), line) {
			t.Errorf("metrics lack %q", line)
		}
	}
}

func TestDuplicatePriorityRefusedOnFlaggedPool(t *testing.T) {
	f := newFixtureWith(t, upstreamValuesYAML, Options{PriorityVerified: func(string) bool { return true }},
		func(c *mockllm.Config) { c.SchedulingPolicy = mockllm.PolicyPriority })
	f.h.priorityPools["large"] = true
	resp, got := f.post(t, PathChatCompletions, []byte(`{"model":"large-model","priority":10,"priority":-1000}`),
		map[string]string{HeaderPriority: "background", mockllm.HeaderOutputTokens: "1"})
	if resp.StatusCode != http.StatusBadRequest || !bytes.Contains(got, []byte(CodeDuplicatePriority)) {
		t.Fatalf("status %d: %s", resp.StatusCode, got)
	}
	if n := len(f.large0.Mock().Requests()) + len(f.large1.Mock().Requests()); n != 0 {
		t.Fatalf("%d upstream requests for a refused body", n)
	}
}

func TestPriorityNotInjectedUntilVerified(t *testing.T) {
	f := newFixture(t, upstreamValuesYAML, Options{PriorityVerified: func(string) bool { return false }})
	f.h.priorityPools["large"] = true
	cases := map[string]string{
		`{"model":"large-model","max_tokens":1}`:                  `{"model":"large-model","max_tokens":1}`,
		`{"model":"large-model","priority":-1000,"max_tokens":1}`: `{"model":"large-model","max_tokens":1}`,
	}
	for in, want := range cases {
		f.large0.Mock().ResetRequests()
		f.large1.Mock().ResetRequests()
		f.post(t, PathChatCompletions, []byte(in), map[string]string{HeaderPriority: "background", mockllm.HeaderOutputTokens: "1"})
		reqs := append(f.large0.Mock().Requests(), f.large1.Mock().Requests()...)
		if len(reqs) != 1 || string(reqs[0].Body) != want {
			t.Errorf("unverified pool, %s: upstream %q, want %q", in, reqsBody(reqs), want)
		}
	}
}

func TestCallerPriorityStrippedOnUnflaggedPool(t *testing.T) {
	f := newFixture(t, upstreamValuesYAML, Options{})
	cases := map[string]string{
		`{"model":"small-model","priority":5,"max_tokens":1}`: `{"model":"small-model","max_tokens":1}`,
		`{"priority":-2,"model":"small-model"}`:               `{"model":"small-model"}`,
		`{"model":"small-model","priority":0}`:                `{"model":"small-model","priority":0}`,
		`{"model":"small-model","priority":"high"}`:           `{"model":"small-model","priority":"high"}`,
		`{"model":"small-model","priority":1,"priority":2}`:   `{"model":"small-model","priority":1,"priority":2}`,
	}
	for in, want := range cases {
		f.small.Mock().ResetRequests()
		f.post(t, PathChatCompletions, []byte(in), map[string]string{mockllm.HeaderOutputTokens: "1"})
		reqs := f.small.Mock().Requests()
		if len(reqs) != 1 || string(reqs[0].Body) != want {
			t.Errorf("%s: upstream %q, want %q", in, reqsBody(reqs), want)
		}
	}
}

// TestLastZeroPriorityIsNotADuplicate (G2): on an unflagged pool the zero
// check comes before the duplicate one. vLLM reads the last value, so a
// body ending on a zero is left alone and is not counted as a duplicate;
// one ending on a non-zero value is.
func TestLastZeroPriorityIsNotADuplicate(t *testing.T) {
	f := newFixture(t, upstreamValuesYAML, Options{})
	duplicates := func() float64 {
		return telemetrytest.Sum(t, f.tel.Metrics(), telemetry.MetricFailOpen, `reason="`+ReasonDuplicatePriority+`"`)
	}
	requests := func() float64 { return telemetrytest.Sum(t, f.tel.Metrics(), telemetry.MetricRequests) }
	hdr := map[string]string{mockllm.HeaderOutputTokens: "1"}
	f.post(t, PathChatCompletions, []byte(`{"model":"small-model","priority":0,"priority":5}`), hdr)
	eventually(t, "the duplicate counted", func() bool { return duplicates() == 1 })
	const zeroLast = `{"model":"small-model","priority":5,"priority":0}`
	f.small.Mock().ResetRequests()
	f.post(t, PathChatCompletions, []byte(zeroLast), hdr)
	eventually(t, "both requests counted", func() bool { return requests() == 2 })
	if n := duplicates(); n != 1 {
		t.Fatalf("%v duplicates counted, want 1: a last zero is not one", n)
	}
	if reqs := f.small.Mock().Requests(); len(reqs) != 1 || string(reqs[0].Body) != zeroLast {
		t.Fatalf("upstream %q, want the body as it came", reqsBody(reqs))
	}
}

func reqsBody(reqs []mockllm.RecordedRequest) string {
	if len(reqs) == 0 {
		return ""
	}
	return string(reqs[0].Body)
}

// newAdmissionFixture admits one large-pool request at a time.
func newAdmissionFixture(t *testing.T, maxWait time.Duration) (*fixture, *arbitrator.Arbitrator) {
	t.Helper()
	arb := arbitrator.New(arbitrator.Config{
		Classes: []arbitrator.Class{
			{Name: "interactive", MaxWait: maxWait, MaxQueue: 8, MaxShare: 1},
			{Name: "user", MaxWait: maxWait, MaxQueue: 8, MaxShare: 1},
			{Name: "background", MaxWait: maxWait, MaxQueue: 8, MaxShare: 1},
		},
		RetryAfterMax: 5 * time.Second,
	}, []arbitrator.Pool{{Name: "large", PerReplica: 1}}, func(string) int { return 1 }, nil)
	return newFixture(t, "", Options{Admission: arb}), arb
}

func TestAdmissionRejectsWithRetryAfterAndReleases(t *testing.T) {
	f, arb := newAdmissionFixture(t, 0)
	body := []byte(`{"model":"large-model","max_tokens":1}`)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		f.post(t, PathChatCompletions, body, map[string]string{mockllm.HeaderTTFTMs: "400", mockllm.HeaderOutputTokens: "1"})
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if in, _ := arb.Demand("large"); in == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first request never admitted")
		}
		time.Sleep(time.Millisecond)
	}

	start := time.Now()
	resp, got := f.post(t, PathChatCompletions, body, nil)
	if took := time.Since(start); took > 50*time.Millisecond {
		t.Errorf("rejection took %s, want < 50ms", took)
	}
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status %d, want 429: %s", resp.StatusCode, got)
	}
	if ra, err := strconv.Atoi(resp.Header.Get("Retry-After")); err != nil || ra < 1 {
		t.Errorf("Retry-After = %q", resp.Header.Get("Retry-After"))
	}
	var e errorBody
	if json.Unmarshal(got, &e) != nil || e.Error.Code != telemetry.AdmissionQueueFull || e.Error.Type != "rate_limit_error" {
		t.Errorf("error body %s", got)
	}

	// The small pool has no admission limit.
	if resp, _ := f.post(t, PathChatCompletions, []byte(`{"model":"small-model","max_tokens":1}`), nil); resp.StatusCode != http.StatusOK {
		t.Errorf("unlimited pool status %d", resp.StatusCode)
	}
	wg.Wait()
	// The slot is released as the handler returns, which can be just after
	// the client has read the response.
	for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(time.Millisecond) {
		in, q := arb.Demand("large")
		if in == 0 && q == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after completion inflight %d queued %d, want 0 0", in, q)
		}
	}
	eventually(t, "the rejection counted", func() bool { return f.outcomes(t).AdmissionRejected == 1 }) // counted when the handler returns
}

func TestAdmissionReleasesOnClientAbort(t *testing.T) {
	f, arb := newAdmissionFixture(t, 5*time.Second)
	body := []byte(`{"model":"large-model","max_tokens":1}`)
	const n = 50
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(5+i%20)*time.Millisecond)
			defer cancel()
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, f.gw.URL+PathChatCompletions, bytes.NewReader(body))
			req.Header.Set(mockllm.HeaderTTFTMs, "200")
			if resp, err := f.gw.Client().Do(req); err == nil {
				_ = resp.Body.Close()
			}
		}()
	}
	wg.Wait()
	deadline := time.Now().Add(5 * time.Second)
	for {
		in, q := arb.Demand("large")
		if in == 0 && q == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after %d aborts inflight %d queued %d, want 0 0", n, in, q)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestNonStreamAnd4xxDoNotPoisonShedding(t *testing.T) {
	m := telemetry.NewMetrics()
	arb := arbitrator.New(arbitrator.Config{
		Classes: []arbitrator.Class{
			{Name: "interactive", MaxWait: time.Second, MaxQueue: 8, MaxShare: 1},
			{Name: "user", MaxWait: time.Second, MaxQueue: 8, MaxShare: 1},
			{Name: "background", MaxWait: time.Second, MaxQueue: 8, MaxShare: 1},
		},
		RetryAfterMax: 5 * time.Second,
		Shed: arbitrator.ShedConfig{
			Enabled:      true,
			Threshold:    50 * time.Millisecond,
			Window:       30 * time.Second,
			MinSamples:   3,
			RecoverRatio: 0.8,
			Classes:      []string{"background"},
		},
	}, []arbitrator.Pool{{Name: "large", PerReplica: 8}}, func(string) int { return 2 }, m)

	yamlCfg := `
priority:
  anonymous_max_class: interactive
`
	f := newFixture(t, yamlCfg, Options{Admission: arb, Metrics: m})

	// 1. Slow non-streaming interactive completions (80ms > 50ms threshold) must
	// NOT feed shedTracker or trigger false 429 shedding of background work.
	nonStreamBody := []byte(`{"model":"large-model","stream":false,"max_tokens":1}`)
	for range 5 {
		resp, _ := f.post(t, PathChatCompletions, nonStreamBody, map[string]string{
			HeaderPriority:             "interactive",
			mockllm.HeaderTTFTMs:       "80",
			mockllm.HeaderOutputTokens: "1",
		})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("non-stream interactive status = %d", resp.StatusCode)
		}
		time.Sleep(5 * time.Millisecond)
	}
	bgBody := []byte(`{"model":"large-model","stream":false,"max_tokens":1}`)
	if resp, got := f.post(t, PathChatCompletions, bgBody, map[string]string{HeaderPriority: "background", mockllm.HeaderOutputTokens: "1"}); resp.StatusCode != http.StatusOK {
		t.Fatalf("background falsely shed after non-streaming calls: status %d: %s", resp.StatusCode, got)
	}

	// 2. Slow streaming 200 interactive calls (> 50ms threshold) DO trigger shedding,
	// and fast 400 error responses must NOT dilute p95 to turn shedding off.
	streamBody := []byte(`{"model":"large-model","stream":true,"max_tokens":1}`)
	for range 4 {
		resp, _ := f.post(t, PathChatCompletions, streamBody, map[string]string{
			HeaderPriority:             "interactive",
			mockllm.HeaderTTFTMs:       "80",
			mockllm.HeaderOutputTokens: "1",
		})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("stream interactive status = %d", resp.StatusCode)
		}
		time.Sleep(260 * time.Millisecond) // advance past shedEvalInterval
	}
	if resp, _ := f.post(t, PathChatCompletions, bgBody, map[string]string{HeaderPriority: "background"}); resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("background status = %d, want 429 while streaming TTFT > threshold", resp.StatusCode)
	}

	// Send 100 fast 400 upstream errors under interactive; they must not enter shedTracker.
	for range 100 {
		resp, _ := f.post(t, PathChatCompletions, streamBody, map[string]string{
			HeaderPriority:      "interactive",
			mockllm.HeaderFault: "http_400",
		})
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("fault status = %d, want 400", resp.StatusCode)
		}
	}
	time.Sleep(260 * time.Millisecond)
	if resp, _ := f.post(t, PathChatCompletions, bgBody, map[string]string{HeaderPriority: "background"}); resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("background status = %d after 400 burst, want still 429 (4xx must not dilute shed TTFT)", resp.StatusCode)
	}
}
