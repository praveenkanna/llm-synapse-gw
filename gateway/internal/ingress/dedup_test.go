package ingress

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/coalesce"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/compiler"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/mockllm"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates"
)

// Two templates over the same prompt: "faq" shares generations (every
// variable is shared), "plain" does not opt in.
const (
	faqSpec = `{"model":"large-model","coalesce":true,"variables":[{"name":"q","scope":"shared"}],
 "messages":[{"role":"system","segments":[{"kind":"static","text":"You are a support assistant. Answer briefly."}]},
             {"role":"user","segments":[{"kind":"volatile","text":"{{q}}"}]}]}`
	plainSpec = `{"model":"large-model","variables":[{"name":"q"}],
 "messages":[{"role":"system","segments":[{"kind":"static","text":"You are a support assistant. Answer briefly."}]},
             {"role":"user","segments":[{"kind":"volatile","text":"{{q}}"}]}]}`
)

type dedupFixture struct {
	*fixture // its tel is the dedup fixture's own
	group    *coalesce.Group
}

// newDedupFixture is a gateway over two large replicas that generate at
// tokensPerSecond (so a generation outlasts the requests that join it).
func newDedupFixture(t *testing.T, tokensPerSecond float64, replayBytes int) *dedupFixture {
	t.Helper()
	return newDedupFixtureWith(t, "", tokensPerSecond, replayBytes)
}

// newDedupFixtureWith is newDedupFixture with yamlExtra added to the config.
func newDedupFixtureWith(t *testing.T, yamlExtra string, tokensPerSecond float64, replayBytes int) *dedupFixture {
	t.Helper()
	snap, errs := templates.NewSnapshot([]templates.Record{
		{ID: "faq", Version: 1, Status: templates.StatusActive, Spec: json.RawMessage(faqSpec)},
		{ID: "faq", Version: 2, Status: templates.StatusDeprecated, Spec: json.RawMessage(faqSpec)},
		{ID: "faq2", Version: 1, Status: templates.StatusActive, Spec: json.RawMessage(faqSpec)},
		{ID: "plain", Version: 1, Status: templates.StatusActive, Spec: json.RawMessage(plainSpec)},
		{ID: "fewshot", Version: 1, Status: templates.StatusActive, Spec: json.RawMessage(fewShotSpec)},
	}, time.Now())
	if len(errs) > 0 {
		t.Fatal(errs)
	}
	tel := telemetry.New(telemetry.Options{})
	group := coalesce.NewGroup(coalesce.Config{
		Coalescing: config.Coalescing{MaxFlights: 64, MaxSubscribers: 128, MaxReplayBytes: replayBytes},
		Observe:    tel.Metrics().CoalesceEvent,
	})
	tel.Metrics().SetCoalesceSource(func() (int, int64) {
		s := group.Stats()
		return s.Flights, s.RetainedBytes
	})
	opts := Options{
		Metrics:  tel.Metrics(),
		Coalesce: group,
		Compiler: compiler.New(staticSnap{snap}, compiler.Options{
			MaxTokens: config.MaxTokensPolicy{Enabled: false},
		}),
	}
	f := newFixtureWith(t, yamlExtra, opts, func(c *mockllm.Config) { c.TokensPerSecond = tokensPerSecond })
	mux := http.NewServeMux()
	f.h.Register(mux)
	f.gw.Close()
	f.gw, f.tel = httptest.NewServer(tel.Middleware(mux)), tel
	t.Cleanup(f.gw.Close)
	return &dedupFixture{fixture: f, group: group}
}

// upstreamRequests is the generations the large replicas received.
func (f *dedupFixture) upstreamRequests() int {
	return int(f.large0.Mock().Stats().Requests + f.large1.Mock().Stats().Requests)
}

// keyPartVariants change one key part of a request whose body stays the
// same: its template, its version, the pool its model maps to, the pool's
// weights version, or its Accept-Encoding. set applies the change and
// returns its undo.
var keyPartVariants = []struct {
	name string
	set  func(h *Handler, hdr map[string]string) (undo func())
}{
	{"template", func(_ *Handler, hdr map[string]string) func() {
		hdr[templates.HeaderTemplateID] += "2"
		return func() {}
	}},
	{"version", func(_ *Handler, hdr map[string]string) func() {
		hdr[templates.HeaderTemplateVersion] = "2"
		return func() {}
	}},
	{"pool", func(h *Handler, _ map[string]string) func() {
		old := h.poolFor
		h.poolFor = func(string) (string, bool) { return "small", true }
		return func() { h.poolFor = old }
	}},
	{"weights", func(h *Handler, _ map[string]string) func() {
		old := h.weights["large"]
		h.weights["large"] = "next"
		return func() { h.weights["large"] = old }
	}},
	// The client sends Accept-Encoding: gzip unless the header is set.
	{"encoding", func(_ *Handler, hdr map[string]string) func() {
		hdr["Accept-Encoding"] = "identity"
		return func() {}
	}},
}

func (f *dedupFixture) allGenerations() int {
	return f.upstreamRequests() + int(f.small.Mock().Stats().Requests)
}

func cloneHeaders(hdr map[string]string) map[string]string {
	out := make(map[string]string, len(hdr))
	for k, v := range hdr {
		out[k] = v
	}
	return out
}

// AC-M4-3 at the handler: a request with the body of a running flight but
// another template, version, pool, weights version or encoding runs its own
// call.
func TestKeyPartsKeepFlightsApart(t *testing.T) {
	for _, v := range keyPartVariants {
		t.Run(v.name, func(t *testing.T) {
			f := newDedupFixture(t, 100, 1<<20)
			body := faqBody("where is my order?", true, "")
			hdr := faqHeaders("faq", 40)
			leaderDone := make(chan answer, 1)
			go func() { leaderDone <- f.ask(context.Background(), body, hdr) }()
			eventually(t, "the leader's flight", func() bool { return f.group.Stats().Flights == 1 })

			other := cloneHeaders(hdr)
			undo := v.set(f.h, other)
			a := f.ask(context.Background(), body, other)
			undo()
			if a.err != nil || a.status != http.StatusOK {
				t.Fatalf("status %d err %v", a.status, a.err)
			}
			if lead := <-leaderDone; lead.status != http.StatusOK {
				t.Fatalf("leader status %d err %v", lead.status, lead.err)
			}
			if s := f.group.Stats(); s.Joined != 0 {
				t.Fatalf("a request under another %s joined the flight", v.name)
			}
			if n := f.allGenerations(); n != 2 {
				t.Fatalf("replicas ran %d generations, want 2", n)
			}
		})
	}
}

func (f *dedupFixture) upstreamCancelled() int {
	return int(f.large0.Mock().Stats().Cancelled + f.large1.Mock().Stats().Cancelled)
}

func (f *dedupFixture) metric(series string) float64 {
	rec := httptest.NewRecorder()
	f.tel.Metrics().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, telemetry.MetricsPath, nil))
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if v, ok := strings.CutPrefix(line, series+" "); ok {
			n, _ := strconv.ParseFloat(v, 64)
			return n
		}
	}
	return 0
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

type answer struct {
	status int
	header http.Header
	body   []byte
	err    error
}

func (f *dedupFixture) ask(ctx context.Context, body string, hdr map[string]string) answer {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.gw.URL+PathChatCompletions, strings.NewReader(body))
	if err != nil {
		return answer{err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := f.gw.Client().Do(req)
	if err != nil {
		return answer{err: err}
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	return answer{status: resp.StatusCode, header: resp.Header, body: b, err: err}
}

// askMany sends n requests at once and returns their answers.
func (f *dedupFixture) askMany(n int, body string, hdr map[string]string) []answer {
	out := make([]answer, n)
	var wg sync.WaitGroup
	for i := range out {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out[i] = f.ask(context.Background(), body, hdr)
		}()
	}
	wg.Wait()
	return out
}

func faqBody(question string, stream bool, extra string) string {
	return fmt.Sprintf(`{"model":"large-model","temperature":0,"stream":%t,%s"messages":[{"role":"system","content":"You are a support assistant. Answer briefly."},{"role":"user","content":%q}]}`,
		stream, extra, question)
}

func faqHeaders(template string, tokens int) map[string]string {
	return map[string]string{
		templates.HeaderTemplateID: template, templates.HeaderTemplateVersion: "1",
		mockllm.HeaderOutputTokens: strconv.Itoa(tokens),
	}
}

// AC-M4-1: 50 identical concurrent requests run one generation on the
// replicas and every caller gets the whole, identical response.
func TestIdenticalRequestsShareOneGeneration(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%t", stream), func(t *testing.T) {
			f := newDedupFixture(t, 100, 1<<20)
			body := faqBody("where is my order?", stream, "")
			// The leader goes first so the rest are sure to find its flight.
			first := make(chan answer, 1)
			go func() { first <- f.ask(context.Background(), body, faqHeaders("faq", 20)) }()
			eventually(t, "the leader's flight", func() bool { return f.group.Stats().Flights == 1 })
			answers := f.askMany(49, body, faqHeaders("faq", 20))
			answers = append(answers, <-first)

			if n := f.upstreamRequests(); n != 1 {
				t.Fatalf("replicas ran %d generations for 50 identical requests, want 1", n)
			}
			ref := answers[0]
			if ref.err != nil || ref.status != http.StatusOK || len(ref.body) == 0 {
				t.Fatalf("first answer: status %d, %d bytes, err %v", ref.status, len(ref.body), ref.err)
			}
			if stream && !bytes.HasSuffix(bytes.TrimSpace(ref.body), []byte("data: [DONE]")) {
				t.Fatalf("stream did not complete: %q", ref.body[max(0, len(ref.body)-80):])
			}
			for i, a := range answers {
				if a.err != nil || a.status != http.StatusOK || !bytes.Equal(a.body, ref.body) {
					t.Fatalf("answer %d differs: status %d, %d bytes (want %d), err %v", i, a.status, len(a.body), len(ref.body), a.err)
				}
				if got, want := a.header.Get("Content-Type"), ref.header.Get("Content-Type"); got != want {
					t.Fatalf("answer %d Content-Type %q, want %q", i, got, want)
				}
			}
			if s := f.group.Stats(); s.Led != 1 || s.Joined != 49 {
				t.Fatalf("flights led %d joined %d, want 1 and 49", s.Led, s.Joined)
			}
			series := telemetry.MetricCoalesced + `{template="faq",version="1"}`
			eventually(t, "coalesced counter", func() bool { return f.metric(series) == 49 })
			eventually(t, "flights to drain", func() bool { return f.group.Stats().Flights == 0 && f.group.Stats().RetainedBytes == 0 })
		})
	}
}

// AC-M4-2: of 50 clients sharing a call, the first leaving does not
// interrupt the other 49, and the replica keeps generating.
func TestLeaderLeavingDoesNotInterruptFollowers(t *testing.T) {
	f := newDedupFixture(t, 100, 1<<20)
	body := faqBody("where is my order?", true, "")
	hdr := faqHeaders("faq", 40)

	ctx, leave := context.WithCancel(context.Background())
	leaderDone := make(chan answer, 1)
	go func() { leaderDone <- f.ask(ctx, body, hdr) }()
	eventually(t, "the leader's flight", func() bool { return f.group.Stats().Flights == 1 })

	followers := make(chan []answer, 1)
	go func() { followers <- f.askMany(49, body, hdr) }()
	eventually(t, "the followers to join", func() bool { return f.group.Stats().Joined == 49 })

	leave()
	if a := <-leaderDone; a.err == nil {
		t.Fatalf("the leader's request should have failed once its client left, got status %d", a.status)
	}
	answers := <-followers
	ref := answers[0].body
	if !bytes.HasSuffix(bytes.TrimSpace(ref), []byte("data: [DONE]")) {
		t.Fatalf("a follower's stream did not complete: %q", ref[max(0, len(ref)-80):])
	}
	for i, a := range answers {
		if a.err != nil || a.status != http.StatusOK || !bytes.Equal(a.body, ref) {
			t.Fatalf("follower %d interrupted or different: status %d, %d bytes (want %d), err %v", i, a.status, len(a.body), len(ref), a.err)
		}
	}
	if n := f.upstreamRequests(); n != 1 {
		t.Fatalf("replicas ran %d generations, want 1", n)
	}
	if n := f.upstreamCancelled(); n != 0 {
		t.Fatalf("the replica's generation was cancelled %d times while followers were attached", n)
	}
	eventually(t, "the leader's disconnect to be counted", func() bool { return f.outcomes(t).ClientDisconnects >= 1 })
	eventually(t, "flights to drain", func() bool { return f.group.Stats().Flights == 0 })
}

// AC-M4-2 from the other side: the first follower leaving does not interrupt
// the leader or the other 48, streamed or not.
func TestFollowerLeavingDoesNotInterruptOthers(t *testing.T) {
	for _, stream := range []bool{true, false} {
		t.Run(fmt.Sprintf("stream=%v", stream), func(t *testing.T) {
			f := newDedupFixture(t, 100, 1<<20)
			body := faqBody("where is my order?", stream, "")
			hdr := faqHeaders("faq", 40)

			leaderDone := make(chan answer, 1)
			go func() { leaderDone <- f.ask(context.Background(), body, hdr) }()
			eventually(t, "the leader's flight", func() bool { return f.group.Stats().Flights == 1 })
			ctx, leave := context.WithCancel(context.Background())
			firstDone := make(chan answer, 1)
			go func() { firstDone <- f.ask(ctx, body, hdr) }()
			eventually(t, "the first follower to join", func() bool { return f.group.Stats().Joined == 1 })
			others := make(chan []answer, 1)
			go func() { others <- f.askMany(48, body, hdr) }()
			eventually(t, "the others to join", func() bool { return f.group.Stats().Joined == 49 })

			leave()
			<-firstDone
			lead := <-leaderDone
			if lead.err != nil || lead.status != http.StatusOK {
				t.Fatalf("leader interrupted: status %d, err %v", lead.status, lead.err)
			}
			if stream && !bytes.HasSuffix(bytes.TrimSpace(lead.body), []byte("data: [DONE]")) {
				t.Fatalf("the leader's stream did not complete: %q", lead.body[max(0, len(lead.body)-80):])
			}
			if !stream && !bytes.Contains(lead.body, []byte(`"finish_reason"`)) {
				t.Fatalf("the leader's body is not a complete response: %q", lead.body)
			}
			for i, a := range <-others {
				if a.err != nil || a.status != http.StatusOK || !bytes.Equal(a.body, lead.body) {
					t.Fatalf("follower %d interrupted or different: status %d, %d bytes (want %d), err %v", i, a.status, len(a.body), len(lead.body), a.err)
				}
			}
			if n := f.upstreamRequests(); n != 1 {
				t.Fatalf("replicas ran %d generations, want 1", n)
			}
			if n := f.upstreamCancelled(); n != 0 {
				t.Fatalf("the replica's generation was cancelled %d times while clients were attached", n)
			}
			eventually(t, "flights to drain", func() bool { return f.group.Stats().Flights == 0 })
		})
	}
}

// The upstream call is cancelled only when no client is left.
func TestUpstreamCancelledWhenEveryClientLeaves(t *testing.T) {
	f := newDedupFixture(t, 50, 1<<20)
	body := faqBody("where is my order?", true, "")
	hdr := faqHeaders("faq", 200)

	ctx, leave := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = f.ask(ctx, body, hdr)
		}()
		if i == 0 {
			eventually(t, "the leader's flight", func() bool { return f.group.Stats().Flights == 1 })
		}
	}
	eventually(t, "the followers to join", func() bool { return f.group.Stats().Joined == 3 })
	leave()
	wg.Wait()
	eventually(t, "the replica to see the cancel", func() bool { return f.upstreamCancelled() == 1 })
	eventually(t, "flights to drain", func() bool { return f.group.Stats().Flights == 0 })
	if n := f.upstreamRequests(); n != 1 {
		t.Fatalf("replicas ran %d generations, want 1", n)
	}
}

// AC-M4-3: requests that are not eligible never merge, however identical.
func TestIneligibleRequestsNeverMerge(t *testing.T) {
	cases := []struct {
		name     string
		body     string
		hdr      map[string]string
		failOpen string // gateway_failopen_total reason expected, if any
	}{
		{"template did not opt in", faqBody("where is my order?", false, ""), faqHeaders("plain", 10), ""},
		{"no template named", faqBody("where is my order?", false, ""), map[string]string{mockllm.HeaderOutputTokens: "10"}, ""},
		{"sampling not fixed", `{"model":"large-model","temperature":0.7,"messages":[{"role":"system","content":"You are a support assistant. Answer briefly."},{"role":"user","content":"where is my order?"}]}`, faqHeaders("faq", 10), "not_deterministic"},
		{"temperature absent", `{"model":"large-model","messages":[{"role":"system","content":"You are a support assistant. Answer briefly."},{"role":"user","content":"where is my order?"}]}`, faqHeaders("faq", 10), "not_deterministic"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newDedupFixture(t, 100, 1<<20)
			hdr := map[string]string{}
			for k, v := range tc.hdr {
				hdr[k] = v
			}
			hdr[mockllm.HeaderTTFTMs] = "300"
			const n = 50
			for i, a := range f.askMany(n, tc.body, hdr) {
				if a.err != nil || a.status != http.StatusOK {
					t.Fatalf("request %d: status %d err %v", i, a.status, a.err)
				}
			}
			if got := f.upstreamRequests(); got != n {
				t.Fatalf("replicas ran %d generations for %d requests that must not merge", got, n)
			}
			if s := f.group.Stats(); s.Joined != 0 {
				t.Fatalf("%d requests joined a flight", s.Joined)
			}
			if tc.failOpen != "" {
				series := fmt.Sprintf(`%s{stage="coalesce",reason=%q}`, telemetry.MetricFailOpen, tc.failOpen)
				if got := f.metric(series); got != n {
					t.Fatalf("%s = %v, want %d", series, got, n)
				}
			}
		})
	}
}

// Requests that differ in anything the replica sees never merge; those that
// differ only in priority class or caller label do not share either way the
// class changes the queue, so only "user" and "priority" are ignored.
func TestDifferentRequestsRunSeparately(t *testing.T) {
	f := newDedupFixture(t, 100, 1<<20)
	hdr := faqHeaders("faq", 10)
	hdr[mockllm.HeaderTTFTMs] = "300"
	bodies := []string{
		faqBody("where is my order?", false, ""),
		faqBody("where is my order", false, ""),
		faqBody("where is my order?", false, `"max_tokens":50,`),
		faqBody("where is my order?", false, `"seed":7,`),
	}
	var wg sync.WaitGroup
	for _, b := range bodies {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if a := f.ask(context.Background(), b, hdr); a.err != nil || a.status != http.StatusOK {
				t.Errorf("status %d err %v", a.status, a.err)
			}
		}()
	}
	wg.Wait()
	if n := f.upstreamRequests(); n != len(bodies) {
		t.Fatalf("replicas ran %d generations for %d different requests", n, len(bodies))
	}
}

// A caller's own "user" label and priority do not change the answer, so
// they do not split a flight (priority is part of the in-flight key only
// through its class).
func TestUserLabelDoesNotSplitAFlight(t *testing.T) {
	f := newDedupFixture(t, 100, 1<<20)
	hdr := faqHeaders("faq", 20)
	first := make(chan answer, 1)
	go func() {
		first <- f.ask(context.Background(), faqBody("where is my order?", false, `"user":"alice",`), hdr)
	}()
	eventually(t, "the leader's flight", func() bool { return f.group.Stats().Flights == 1 })
	b := f.ask(context.Background(), faqBody("where is my order?", false, `"user":"bob",`), hdr)
	a := <-first
	if a.err != nil || b.err != nil || !bytes.Equal(a.body, b.body) {
		t.Fatalf("answers differ: %v %v", a.err, b.err)
	}
	if n := f.upstreamRequests(); n != 1 {
		t.Fatalf("replicas ran %d generations, want 1", n)
	}
}

// A response past the replay bound is sealed and the flight never holds
// more than the bound; a follower that cannot keep up is evicted with one
// SSE error event rather than held in memory.
func TestReplayBoundIsEnforced(t *testing.T) {
	f := newDedupFixture(t, 2000, coalesce.MinReplayBytes)
	body := faqBody("where is my order?", true, "")
	hdr := faqHeaders("faq", 400)     // far more than the replay bound
	hdr[mockllm.HeaderTTFTMs] = "200" // the follower joins before the first byte

	var peak int64
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		for {
			peak = max(peak, f.group.Stats().RetainedBytes)
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()

	first := make(chan answer, 1)
	go func() { first <- f.ask(context.Background(), body, hdr) }()
	eventually(t, "the leader's flight", func() bool { return f.group.Stats().Flights == 1 })
	a := f.ask(context.Background(), body, hdr)
	l := <-first
	close(stop)
	<-sampled
	if l.err != nil || l.status != http.StatusOK || !bytes.HasSuffix(bytes.TrimSpace(l.body), []byte("data: [DONE]")) {
		t.Fatalf("the leader's stream is incomplete: status %d err %v", l.status, l.err)
	}
	if s := f.group.Stats(); s.Sealed == 0 || s.Joined != 1 || s.Led != 1 {
		t.Fatalf("a response of %d bytes with a %d byte replay bound: sealed %d, led %d, joined %d; want sealed, 1 and 1",
			len(l.body), coalesce.MinReplayBytes, s.Sealed, s.Led, s.Joined)
	}
	if bound := int64(coalesce.MinReplayBytes + 4<<10); peak > bound {
		t.Fatalf("one flight held %d bytes, over its bound of %d", peak, bound)
	}
	// Whatever the follower was or was not served, it never got a truncated
	// stream without an ending: it is complete, or it carries the error event.
	if a.err != nil || a.status != http.StatusOK {
		t.Fatalf("follower: status %d err %v", a.status, a.err)
	}
	tail := string(a.body[max(0, len(a.body)-300):])
	if !strings.Contains(tail, "[DONE]") && !strings.Contains(tail, `"error"`) {
		t.Fatalf("follower stream ended without [DONE] or an error event: %q", tail)
	}
	eventually(t, "flights to drain", func() bool { return f.group.Stats().RetainedBytes == 0 })
}

// A follower is relayed each event as the leader gets it, not the whole
// stream at the end.
func TestFollowerStreamIsRelayedLive(t *testing.T) {
	f := newDedupFixture(t, 50, 1<<20) // 40 tokens: about 800 ms of stream
	body := faqBody("where is my order?", true, "")
	hdr := faqHeaders("faq", 40)
	leader := make(chan answer, 1)
	go func() { leader <- f.ask(context.Background(), body, hdr) }()
	eventually(t, "the leader's flight", func() bool { return f.group.Stats().Flights == 1 })

	req, _ := http.NewRequest(http.MethodPost, f.gw.URL+PathChatCompletions, strings.NewReader(body))
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := f.gw.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	buf := make([]byte, 64<<10)
	if _, err := resp.Body.Read(buf); err != nil {
		t.Fatalf("follower's first read: %v", err)
	}
	select {
	case <-leader:
		t.Fatal("the follower's first bytes arrived only after the whole stream was done")
	default:
	}
	rest, err := io.ReadAll(resp.Body)
	if err != nil || !bytes.HasSuffix(bytes.TrimSpace(rest), []byte("data: [DONE]")) {
		t.Fatalf("follower stream incomplete: err %v", err)
	}
	if s := f.group.Stats(); s.Joined != 1 {
		t.Fatalf("joined %d, want 1", s.Joined)
	}
	<-leader
}

// Requests of different priority classes never share a call: a follower
// would otherwise wait in the leader's class.
func TestPriorityClassesDoNotShare(t *testing.T) {
	f := newDedupFixture(t, 100, 1<<20)
	body := faqBody("where is my order?", false, "")
	hdr := faqHeaders("faq", 20)
	first := make(chan answer, 1)
	go func() { first <- f.ask(context.Background(), body, hdr) }()
	eventually(t, "the leader's flight", func() bool { return f.group.Stats().Flights == 1 })
	bg := faqHeaders("faq", 20)
	bg[HeaderPriority] = "background"
	if a := f.ask(context.Background(), body, bg); a.err != nil || a.status != http.StatusOK {
		t.Fatalf("background request: status %d err %v", a.status, a.err)
	}
	<-first
	if n := f.upstreamRequests(); n != 2 {
		t.Fatalf("replicas ran %d generations for two priority classes, want 2", n)
	}
	if s := f.group.Stats(); s.Joined != 0 {
		t.Fatalf("%d requests joined across classes", s.Joined)
	}
}

// A body posted to /v1/completions never shares a call with the same body
// on /v1/chat/completions: the replica treats them differently.
func TestEndpointsDoNotShare(t *testing.T) {
	f := newDedupFixture(t, 100, 1<<20)
	body := faqBody("where is my order?", false, "")
	hdr := faqHeaders("faq", 20)
	first := make(chan answer, 1)
	go func() { first <- f.ask(context.Background(), body, hdr) }()
	eventually(t, "the leader's flight", func() bool { return f.group.Stats().Flights == 1 })
	req, _ := http.NewRequest(http.MethodPost, f.gw.URL+PathCompletions, strings.NewReader(body))
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := f.gw.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	<-first
	if s := f.group.Stats(); s.Joined != 0 {
		t.Fatalf("a /v1/completions request joined a chat flight")
	}
}
