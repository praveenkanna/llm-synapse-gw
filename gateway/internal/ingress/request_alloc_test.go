//go:build !race

package ingress

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex/bodyindextest"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/coalesce"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/compiler"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry/telemetrytest"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

// wideBody is a body of n top-level members before the request's own,
// each a short sampling-style member.
func wideBody(n int, tail string) string {
	var b strings.Builder
	b.WriteString(`{"model":"large-model"`)
	for i := range n {
		b.WriteString(`,"x` + strconv.Itoa(i) + `":0`)
	}
	return b.String() + `,"messages":[{"role":"user","content":"hi"}]` + tail + `}`
}

// TestRequestAllocations (G4, AD-15): a request's body is read, keyed and
// spliced through one bodyindex.Index that handleGenerate declares and
// passes down by pointer, so a request allocates no more than it did before
// the index.
//
// Every request runs the whole handler behind the telemetry middleware and
// is answered 503 by a pool with no replicas after its body was spliced, so
// nothing on the network is counted.
//
// What is compared is a request's cost over a baseline measured in the same
// process, because an absolute count depends on the toolchain: on main a
// coalesced request is 173 on go1.26.8 linux and 147 on go1.27.1 darwin, the
// same code. The baseline is a request with no body (GET /v1/models, the same
// middleware, the same answer), or, for a template that shares, the same
// body through a template that does not, so what matching a template costs
// on the toolchain cancels out and only the key and the flight are left.
//
// main is a case's cost before the index and max its ceiling now, the most
// it costs on either toolchain. Each main is the lower of the two
// toolchains', so a request stays under main's cost on both. The index
// staying on the stack is TestOneIndexStaysOnTheStack's to prove; a
// ceiling would only show it on the toolchain it is tight on.
//
// Not under -race: there sync.Pool drops items at random and the counts are
// unreliable. CI runs the tests under -race only, so run this one by hand
// when the request path changes.
func TestRequestAllocations(t *testing.T) {
	snap, errs := templates.NewSnapshot([]templates.Record{
		{ID: "faq", Version: 1, Status: templates.StatusActive, Spec: json.RawMessage(faqSpec)},
		{ID: "plain", Version: 1, Status: templates.StatusActive, Spec: json.RawMessage(plainSpec)},
	}, time.Now())
	if len(errs) > 0 {
		t.Fatal(errs)
	}
	handlers := func(t *testing.T, priorityPool bool) (generate, models http.Handler, tel *telemetry.Telemetry, group *coalesce.Group) {
		cfg := parseConfig(t, `
pools:
  - {name: large, priority_scheduling: `+strconv.FormatBool(priorityPool)+`, replicas: [{name: large-0, url: "http://127.0.0.1:1"}]}
models: {large-model: large}
default_pool: large
priority:
  upstream_values: {interactive: -10, user: 0, background: 10}
auth: {enabled: false}
`)
		tel = telemetry.New(telemetry.Options{})
		client := upstream.New(cfg.Upstream)
		t.Cleanup(client.CloseIdleConnections)
		group = coalesce.NewGroup(coalesce.Config{
			Coalescing: config.Coalescing{MaxFlights: 64, MaxSubscribers: 64, MaxReplayBytes: 1 << 20},
		})
		h := New(cfg, emptyDiscovery{}, client, roundRobin(cfg, emptyDiscovery{}, client, nil), Options{
			Metrics:          tel.Metrics(),
			PriorityVerified: func(string) bool { return true },
			Coalesce:         group,
			Compiler: compiler.New(staticSnap{snap}, compiler.Options{
				MaxTokens: config.MaxTokensPolicy{Enabled: true, Margin: 0.25, MinSamples: 1000},
				Ceilings:  map[string]int{"large": 256},
			}),
		})
		return tel.Middleware(http.HandlerFunc(h.handleGenerate)), tel.Middleware(http.HandlerFunc(h.handleModels)), tel, group
	}
	// allocations is what one request costs, on average.
	allocations := func(t *testing.T, h http.Handler, method, path string, body []byte, hdr map[string]string) (allocs float64, runs int) {
		allocs = testing.AllocsPerRun(200, func() {
			runs++
			r := httptest.NewRequest(method, path, bytes.NewReader(body))
			for k, v := range hdr {
				r.Header.Set(k, v)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusServiceUnavailable {
				t.Fatalf("answered %d, want 503 from a pool with no replicas", w.Code)
			}
		})
		return allocs, runs
	}
	faq := map[string]string{templates.HeaderTemplateID: "faq", templates.HeaderTemplateVersion: "1"}
	background := map[string]string{HeaderPriority: config.PriorityBackground}
	for _, tc := range []struct {
		name         string
		priorityPool bool
		hdr          map[string]string
		body         string
		// outcome is the gateway_upstream_priority_total outcome every
		// request records, if any; capped: the gateway sets max_tokens;
		// leads: it leads a flight; failOpen: the reason each one records.
		outcome   string
		capped    bool
		leads     bool
		failOpen  string
		main, max float64
	}{
		{name: "plain", body: `{"model":"large-model","messages":[{"role":"user","content":"hi"}],"temperature":0.7}`, main: 10, max: 8},
		{name: "priority stripped", body: `{"model":"large-model","priority":5,"messages":[{"role":"user","content":"hi"}]}`, outcome: PriorityStripped, main: 12, max: 10},
		{name: "priority replaced", priorityPool: true, hdr: background, body: `{"model":"large-model","priority":3,"messages":[{"role":"user","content":"hi"}]}`, outcome: PriorityReplaced, main: 17, max: 15},
		{name: "priority injected", priorityPool: true, hdr: background, body: `{"model":"large-model","messages":[{"role":"user","content":"hi"}]}`, outcome: PriorityInjected, main: 16, max: 14},
		{name: "coalesced template with max_tokens", hdr: faq, body: faqBody("where is my order?", false, ""), capped: true, leads: true, main: 65, max: 52},
		{name: "shared template, key refused", hdr: faq, capped: true, failOpen: telemetry.ReasonUnsupportedKey, main: 14, max: 4,
			body: strings.Replace(faqBody("where is my order?", false, ""), `"model"`, `"`+bodyindextest.Escaped("model")+`"`, 1)},
		{name: "24 members, priority stripped", body: wideBody(22, `,"priority":5`), outcome: PriorityStripped, main: 12, max: 12},
		{name: "past the index ceiling, priority stripped", body: wideBody(1100, `,"priority":5`), outcome: PriorityStripped, main: 22, max: 21},
		{name: "model too long, 300 bytes", failOpen: ReasonModelTooLong, main: 12, max: 12,
			body: `{"model":"` + strings.Repeat("m", 300) + `","messages":[{"role":"user","content":"hi"}]}`},
		{name: "model too long, 64 KiB", failOpen: ReasonModelTooLong, main: 27, max: 25,
			body: `{"model":"` + strings.Repeat("m", 65536) + `","messages":[{"role":"user","content":"hi"}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			generate, models, tel, group := handlers(t, tc.priorityPool)
			base, _ := allocations(t, models, http.MethodGet, PathModels, nil, nil)
			if tc.hdr[templates.HeaderTemplateID] == "faq" {
				// Matching a template costs far more than the rest, and what
				// it costs depends on the toolchain.
				plain := map[string]string{templates.HeaderTemplateID: "plain", templates.HeaderTemplateVersion: "1"}
				other, _, _, _ := handlers(t, tc.priorityPool)
				base, _ = allocations(t, other, http.MethodPost, PathChatCompletions, []byte(tc.body), plain)
			}
			allocs, runs := allocations(t, generate, http.MethodPost, PathChatCompletions, []byte(tc.body), tc.hdr)
			if tc.outcome != "" {
				if n := telemetrytest.Sum(t, tel.Metrics(), telemetry.MetricUpstreamPriority, `outcome="`+tc.outcome+`"`); n != float64(runs) {
					t.Fatalf("%v requests recorded priority %s, want all %d", n, tc.outcome, runs)
				}
			}
			if tc.capped {
				if n := telemetrytest.Sum(t, tel.Metrics(), telemetry.MetricTemplateMaxTokensSet); n != float64(runs) {
					t.Fatalf("%v requests got a gateway max_tokens, want all %d", n, runs)
				}
			}
			// A case that stopped taking its path would pass on the wrong count.
			wantFailOpens := 0.0
			if tc.failOpen != "" {
				wantFailOpens = float64(runs)
			}
			if n := telemetrytest.Sum(t, tel.Metrics(), telemetry.MetricFailOpen); n != wantFailOpens {
				t.Fatalf("%v fail-opens (%q wanted) over %d requests", n, tc.failOpen, runs)
			}
			if n := telemetrytest.Sum(t, tel.Metrics(), telemetry.MetricFailOpen, `reason="`+tc.failOpen+`"`); tc.failOpen != "" && n != wantFailOpens {
				t.Fatalf("%v requests failed open with %s, want all %d", n, tc.failOpen, runs)
			}
			if led := float64(group.Stats().Led); (tc.leads && led != float64(runs)) || (!tc.leads && led != 0) {
				t.Fatalf("%v flights led over %d requests, leads %v", led, runs, tc.leads)
			}
			cost := allocs - base
			t.Logf("%s: %.0f allocations, %.0f over its baseline (%.0f); main %.0f", tc.name, allocs, cost, base, tc.main)
			if cost > tc.max || tc.max > tc.main {
				t.Fatalf("%.0f allocations over the baseline, want at most %.0f, itself at most main's %.0f", cost, tc.max, tc.main)
			}
		})
	}
}
