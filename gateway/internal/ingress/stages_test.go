package ingress

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/coalesce"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/respcache"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry/telemetrytest"
)

// The stages' side of the progression (AD-11): each stage says what it
// decided, and these tests pin the step each outcome is. The progression's
// side (what a step records) is TestProgressRecordsEveryAnswer.

// errAfter is a request body that fails after its first bytes, as one
// whose caller stopped sending does.
type errAfter struct{ sent bool }

func (e *errAfter) Read(p []byte) (int, error) {
	if !e.sent {
		e.sent = true
		return copy(p, `{"model":`), nil
	}
	return 0, errors.New("caller stopped sending")
}

// failingWriter is a client that has gone: every write fails.
type failingWriter struct{ *httptest.ResponseRecorder }

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("client gone") }

func cancelledRequest(path, body string) *http.Request {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return httptest.NewRequestWithContext(ctx, http.MethodPost, path, strings.NewReader(body))
}

func TestReadRequestSteps(t *testing.T) {
	h := newFixture(t, "", Options{MaxRequestBodyBytes: 64}).h
	for _, tc := range []struct {
		name   string
		body   io.Reader
		want   step
		status int // written, when answered
	}{
		{"a body", strings.NewReader(`{"model":"m"}`), proceed, 0},
		{"over the limit", strings.NewReader(strings.Repeat("a", 65)), answered, http.StatusRequestEntityTooLarge},
		{"caller stopped sending", &errAfter{}, gone, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			_, got := h.readRequest(w, httptest.NewRequest(http.MethodPost, PathChatCompletions, tc.body))
			if got != tc.want {
				t.Fatalf("step = %v, want %v", got, tc.want)
			}
			if tc.status != 0 && (w.Code != tc.status || w.Header().Get("Connection") != "close") {
				t.Fatalf("answered %d (Connection %q), want %d and close", w.Code, w.Header().Get("Connection"), tc.status)
			}
			if tc.want == gone && w.Body.Len() != 0 {
				t.Fatalf("wrote %q to a caller that left", w.Body)
			}
		})
	}
}

func TestRefuseDuplicatePrioritySteps(t *testing.T) {
	h := newFixture(t, "", Options{}).h
	h.priorityPools["large"] = true
	for _, tc := range []struct {
		name, pool, body string
		want             step
	}{
		{"two priorities on a flagged pool", "large", `{"priority":1,"priority":2}`, answered},
		{"one priority", "large", `{"priority":1}`, proceed},
		{"two priorities on an unflagged pool", "small", `{"priority":1,"priority":2}`, proceed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info, ok := readBody(new(bodyindex.Index), []byte(tc.body))
			w := httptest.NewRecorder()
			if got := h.refuseDuplicatePriority(w, tc.pool, info, ok); got != tc.want {
				t.Fatalf("step = %v, want %v", got, tc.want)
			}
			if tc.want == answered && w.Code != http.StatusBadRequest {
				t.Fatalf("answered %d, want 400", w.Code)
			}
		})
	}
}

// TestServeCachedSteps: a cached entry answers the request, unless the
// caller left before it was written. Whether there is an entry to serve
// (a hit, a miss, a lookup that failed or that the caller left) is dedup's
// decision, tested there.
func TestServeCachedSteps(t *testing.T) {
	e := respcache.Entry{Status: http.StatusOK, ContentType: "application/json", Body: []byte(`{"ok":true}`)}
	w := httptest.NewRecorder()
	if got := serveCached(w, nil, e); got != answered || w.Body.String() != `{"ok":true}` || w.Header().Get(HeaderCache) != "hit" {
		t.Fatalf("step %v, body %q, %s %q; want answered with the entry", got, w.Body, HeaderCache, w.Header().Get(HeaderCache))
	}
	if got := serveCached(failingWriter{httptest.NewRecorder()}, nil, e); got != gone {
		t.Fatalf("step %v, want gone when the entry cannot be written", got)
	}
}

func TestAssembleContextSteps(t *testing.T) {
	f := newDedupFixtureWith(t, contextOn, 0, 1<<20)
	body := ctxBody(ctxTurns...)
	over := promptTokens(t, ctxBody(ctxTurns[2])) - 1
	for _, tc := range []struct {
		name  string
		r     *http.Request
		limit int
		want  step
	}{
		{"fits", httptest.NewRequest(http.MethodPost, PathChatCompletions, nil), 1 << 20, proceed},
		{"over budget", httptest.NewRequest(http.MethodPost, PathChatCompletions, nil), over, answered},
		{"caller left while measuring", cancelledRequest(PathChatCompletions, ""), 10, gone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			ix, _ := indexed(t, []byte(body))
			if got := f.h.assembleContext(w, tc.r, nil, "large", nil, tc.limit, ix); got != tc.want {
				t.Fatalf("step = %v, want %v", got, tc.want)
			}
			if tc.want == answered && w.Code != http.StatusUnprocessableEntity {
				t.Fatalf("answered %d, want 422", w.Code)
			}
		})
	}
}

func TestAdmitSteps(t *testing.T) {
	for _, tc := range []struct {
		name    string
		maxWait time.Duration
		full    bool // the pool's one slot is taken
		gone    bool
		want    step
	}{
		{name: "room", want: proceed},
		{name: "full, no queue", full: true, want: answered},
		{name: "caller left while queued", maxWait: 5 * time.Second, full: true, gone: true, want: gone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, arb := newAdmissionFixture(t, tc.maxWait)
			if tc.full {
				held, err := arb.Acquire(context.Background(), "large", "user")
				if err != nil {
					t.Fatal(err)
				}
				defer held.Release()
			}
			r := httptest.NewRequest(http.MethodPost, PathChatCompletions, nil)
			if tc.gone {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
				defer cancel()
				r = r.WithContext(ctx)
			}
			w := httptest.NewRecorder()
			slot, got := f.h.admit(w, r, "large")
			if slot != nil {
				defer slot.Release()
			}
			if got != tc.want {
				t.Fatalf("step = %v, want %v", got, tc.want)
			}
			if tc.want == answered && (w.Code != http.StatusTooManyRequests && w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") == "") {
				t.Fatalf("answered %d (Retry-After %q), want 429 or 503 with Retry-After", w.Code, w.Header().Get("Retry-After"))
			}
		})
	}
}

// TestFollowSteps drives a follower against a real flight: what it was
// served decides the step, and a follower its flight could not serve
// before its first byte runs as an ordinary request, having written
// nothing.
func TestFollowSteps(t *testing.T) {
	flight := func(t *testing.T) (*coalesce.Flight, *coalesce.Subscription) {
		t.Helper()
		g := coalesce.NewGroup(coalesce.Config{Coalescing: config.Coalescing{MaxFlights: 4, MaxSubscribers: 4, MaxReplayBytes: coalesce.MinReplayBytes}})
		f, _, _ := g.Acquire("k", true)
		sub := g.Join("k")
		if f == nil || sub == nil {
			t.Fatal("no flight to follow")
		}
		return f, sub
	}
	h := &Handler{}
	t.Run("served whole", func(t *testing.T) {
		f, sub := flight(t)
		lead := f.Writer(httptest.NewRecorder())
		lead.WriteHeader(http.StatusOK)
		_, _ = lead.Write([]byte(`{"ok":true}`))
		f.Finish(true, false)
		w := httptest.NewRecorder()
		if got := h.follow(w, httptest.NewRequest(http.MethodPost, PathChatCompletions, nil), nil, sub); got != answered || w.Body.String() != `{"ok":true}` {
			t.Fatalf("step %v body %q, want answered with the leader's response", got, w.Body)
		}
	})
	t.Run("flight failed before any response", func(t *testing.T) {
		f, sub := flight(t)
		f.Finish(false, false)
		w := httptest.NewRecorder()
		if got := h.follow(w, httptest.NewRequest(http.MethodPost, PathChatCompletions, nil), nil, sub); got != answered || w.Code != http.StatusBadGateway {
			t.Fatalf("step %v status %d, want answered with 502", got, w.Code)
		}
	})
	t.Run("failed mid-stream", func(t *testing.T) {
		f, sub := flight(t)
		lead := f.Writer(httptest.NewRecorder())
		lead.Header().Set("Content-Type", "text/event-stream")
		lead.WriteHeader(http.StatusOK)
		_, _ = lead.Write([]byte("data: {}\n\n"))
		f.Finish(false, false) // the leader's exchange was cut off mid-stream
		tel := telemetry.New(telemetry.Options{})
		var got step
		w := httptest.NewRecorder()
		tel.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got = h.follow(w, r, telemetry.FromContext(r.Context()), sub)
		})).ServeHTTP(w, httptest.NewRequest(http.MethodPost, PathChatCompletions, nil))
		if got != answered || strings.Count(w.Body.String(), `{"error":`) != 1 {
			t.Fatalf("step %v body %q, want answered with what was shared and one error event", got, w.Body)
		}
		if n := telemetrytest.Sum(t, tel.Metrics(), telemetry.MetricRequests, `status="`+telemetry.OutcomeStreamError+`"`); n != 1 {
			t.Fatalf("%v requests counted as %s, want 1", n, telemetry.OutcomeStreamError)
		}
	})
	t.Run("shared a response that failed", func(t *testing.T) {
		f, sub := flight(t)
		lead := f.Writer(httptest.NewRecorder())
		lead.Header().Set("Content-Type", "text/event-stream")
		lead.WriteHeader(http.StatusOK)
		_, _ = lead.Write([]byte("data: {}\n\ndata: {\"error\":{}}\n\n")) // the leader's relay wrote the error event
		f.FinishFailed()
		tel := telemetry.New(telemetry.Options{})
		var got step
		w := httptest.NewRecorder()
		tel.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got = h.follow(w, r, telemetry.FromContext(r.Context()), sub)
		})).ServeHTTP(w, httptest.NewRequest(http.MethodPost, PathChatCompletions, nil))
		if got != answered || strings.Count(w.Body.String(), `{"error":`) != 1 {
			t.Fatalf("step %v body %q, want answered with the leader's response and its one error event", got, w.Body)
		}
		if n := telemetrytest.Sum(t, tel.Metrics(), telemetry.MetricRequests, `status="`+telemetry.OutcomeStreamError+`"`); n != 1 {
			t.Fatalf("%v requests counted as %s, want 1", n, telemetry.OutcomeStreamError)
		}
	})
	t.Run("evicted before its first byte", func(t *testing.T) {
		f, sub := flight(t)
		lead := f.Writer(httptest.NewRecorder())
		lead.WriteHeader(http.StatusOK)
		_, _ = lead.Write([]byte(strings.Repeat("x", 3*coalesce.MinReplayBytes))) // past the bound: the unread follower is evicted
		defer f.Finish(true, false)
		w := httptest.NewRecorder()
		if got := h.follow(w, httptest.NewRequest(http.MethodPost, PathChatCompletions, nil), nil, sub); got != proceed || w.Body.Len() != 0 || len(w.Header()) != 0 {
			t.Fatalf("step %v, wrote %d bytes and %d headers; want proceed with nothing written", got, w.Body.Len(), len(w.Header()))
		}
	})
	t.Run("caller left", func(t *testing.T) {
		f, sub := flight(t)
		defer f.Finish(false, false)
		if got := h.follow(httptest.NewRecorder(), cancelledRequest(PathChatCompletions, ""), nil, sub); got != gone {
			t.Fatalf("step %v, want gone", got)
		}
	})
}

// String names a step in failures.
func (s step) String() string {
	switch s {
	case proceed:
		return "proceed"
	case answered:
		return "answered"
	case gone:
		return "gone"
	}
	return "step(" + strconv.Itoa(int(s)) + ")"
}
