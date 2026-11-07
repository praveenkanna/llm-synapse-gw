package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/mockllm"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates/pgstore"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

// fakeStore is an in-memory Store honouring pgstore's semantics: versions
// count up from max+1, templates.CanTransition, drafts-only delete.
type fakeStore struct {
	mu       sync.Mutex
	descs    map[string]string
	created  map[string]time.Time
	versions map[string]map[int]templates.Record
	err      error // returned by every method when set
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		descs:    map[string]string{},
		created:  map[string]time.Time{},
		versions: map[string]map[int]templates.Record{},
	}
}

func (f *fakeStore) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeStore) CreateVersion(_ context.Context, id, description string, spec, counts json.RawMessage) (templates.Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return templates.Record{}, f.err
	}
	if !templates.ValidID(id) {
		return templates.Record{}, pgstore.ErrInvalidID
	}
	if !json.Valid(spec) || (len(counts) > 0 && !json.Valid(counts)) {
		return templates.Record{}, errors.New("fake: invalid JSON")
	}
	if _, ok := f.versions[id]; !ok {
		f.versions[id] = map[int]templates.Record{}
		f.created[id] = time.Now()
	}
	if description != "" {
		f.descs[id] = description
	}
	next := 1
	for v := range f.versions[id] {
		next = max(next, v+1)
	}
	now := time.Now()
	rec := templates.Record{ID: id, Version: next, Status: templates.StatusDraft,
		Spec: slices.Clone(spec), Counts: slices.Clone(counts), CreatedAt: now, UpdatedAt: now}
	f.versions[id][next] = rec
	return rec, nil
}

func (f *fakeStore) Get(_ context.Context, id string, version int) (templates.Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return templates.Record{}, f.err
	}
	rec, ok := f.versions[id][version]
	if !ok {
		return templates.Record{}, pgstore.ErrNotFound
	}
	return rec, nil
}

func (f *fakeStore) List(context.Context) ([]pgstore.TemplateSummary, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	var out []pgstore.TemplateSummary
	for id, vs := range f.versions {
		t := pgstore.TemplateSummary{ID: id, Description: f.descs[id], CreatedAt: f.created[id], Versions: []pgstore.VersionSummary{}}
		for _, rec := range vs {
			t.Versions = append(t.Versions, pgstore.VersionSummary{Version: rec.Version, Status: rec.Status,
				CreatedAt: rec.CreatedAt, UpdatedAt: rec.UpdatedAt})
		}
		slices.SortFunc(t.Versions, func(a, b pgstore.VersionSummary) int { return a.Version - b.Version })
		out = append(out, t)
	}
	slices.SortFunc(out, func(a, b pgstore.TemplateSummary) int { return strings.Compare(a.ID, b.ID) })
	return out, nil
}

func (f *fakeStore) SetStatus(_ context.Context, id string, version int, to templates.Status) (templates.Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return templates.Record{}, f.err
	}
	rec, ok := f.versions[id][version]
	if !ok {
		return templates.Record{}, pgstore.ErrNotFound
	}
	if !templates.CanTransition(rec.Status, to) {
		return templates.Record{}, pgstore.ErrInvalidTransition
	}
	rec.Status, rec.UpdatedAt = to, time.Now()
	f.versions[id][version] = rec
	return rec, nil
}

func (f *fakeStore) SetCounts(_ context.Context, id string, version int, counts json.RawMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	rec, ok := f.versions[id][version]
	if !ok {
		return pgstore.ErrNotFound
	}
	rec.Counts, rec.UpdatedAt = slices.Clone(counts), time.Now()
	f.versions[id][version] = rec
	return nil
}

func (f *fakeStore) DeleteDraft(_ context.Context, id string, version int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	rec, ok := f.versions[id][version]
	if !ok {
		return pgstore.ErrNotFound
	}
	if rec.Status != templates.StatusDraft {
		return pgstore.ErrNotDraft
	}
	delete(f.versions[id], version)
	return nil
}

func (f *fakeStore) counts(t *testing.T, id string, version int) templates.Counts {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	var c templates.Counts
	if err := json.Unmarshal(f.versions[id][version].Counts, &c); err != nil {
		t.Fatalf("stored counts of %s v%d: %v", id, version, err)
	}
	return c
}

// mockSender serves upstream requests on a mockllm handler in process, so
// /tokenize counts with mockllm's stand-in tokenizer. Per-replica faults:
// a transport error before any response, or a forced status.
type mockSender struct {
	h http.Handler

	mu     sync.Mutex
	calls  []string // replica names, in call order
	fail   map[string]bool
	status map[string]int
}

func newMockSender(t *testing.T) *mockSender {
	t.Helper()
	cfg := mockllm.Defaults()
	cfg.Models = []string{testModel}
	m, err := mockllm.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return &mockSender{h: m, fail: map[string]bool{}, status: map[string]int{}}
}

func (m *mockSender) setFail(replica string, fail bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fail[replica] = fail
}

func (m *mockSender) setStatus(replica string, status int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.status[replica] = status
}

func (m *mockSender) callLog() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.calls)
}

func (m *mockSender) Do(ctx context.Context, rep discovery.Replica, req upstream.Request) (*http.Response, error) {
	m.mu.Lock()
	m.calls = append(m.calls, rep.Name)
	fail, status := m.fail[rep.Name], m.status[rep.Name]
	m.mu.Unlock()
	if fail {
		return nil, &upstream.Error{Kind: upstream.KindConnect, Replica: rep.Name, Err: errors.New("connection refused")}
	}
	if status != 0 {
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(`{"error":"echo of ` + leakMarker + `"}`))}, nil
	}
	hreq := httptest.NewRequestWithContext(ctx, req.Method, req.Path, bytes.NewReader(req.Body))
	hreq.Header = req.Header.Clone()
	rec := httptest.NewRecorder()
	m.h.ServeHTTP(rec, hreq)
	return rec.Result(), nil
}

// fakeDiscovery maps pools to replicas.
type fakeDiscovery map[string][]discovery.Replica

func (d fakeDiscovery) Replicas(pool string) []discovery.Replica { return d[pool] }

func replica(pool, name string) discovery.Replica {
	return discovery.Replica{Pool: pool, Name: name, BaseURL: &url.URL{Scheme: "http", Host: name + ":8000"}}
}

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

const (
	testModel     = "large-model"
	unmappedModel = "orphan-model"
	tokenCI       = "admin-token-ci-0123456789"
	tokenOps      = "admin-token-ops-0123456789"
	// leakMarker appears in spec text, variables and upstream error bodies;
	// it must never reach the logs.
	leakMarker = "LEAKMARKER-7f3a9c"
)

type fixture struct {
	t      *testing.T
	srv    *Server
	h      http.Handler
	store  *fakeStore
	sender *mockSender
	kicks  atomic.Int32
	logs   *syncBuffer
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{t: t, store: newFakeStore(), sender: newMockSender(t), logs: &syncBuffer{}}
	srv, err := New(f.store, Options{
		Tokens: []config.Caller{
			{Name: "ci", Token: config.Secret(tokenCI)},
			{Name: "ops", Token: config.Secret(tokenOps)},
		},
		TokenizeTimeout: time.Second,
		Discovery: fakeDiscovery{
			"large": {replica("large", "r1"), replica("large", "r2")},
			"empty": nil,
		},
		Sender: f.sender,
		PoolFor: func(model string) (string, bool) {
			switch model {
			case testModel:
				return "large", true
			case "empty-model":
				return "empty", true
			}
			return "", false
		},
		Kick:   func() { f.kicks.Add(1) },
		Logger: slog.New(slog.NewJSONHandler(f.logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	f.srv, f.h = srv, srv.Handler()
	return f
}

// do sends a request with token (none when empty).
func (f *fixture) do(method, path, body, token string) *httptest.ResponseRecorder {
	f.t.Helper()
	var r io.Reader = http.NoBody
	if body != "" {
		r = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, r)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	f.h.ServeHTTP(rec, req)
	return rec
}

// call sends an authenticated request and checks its status.
func (f *fixture) call(method, path, body string, want int) *httptest.ResponseRecorder {
	f.t.Helper()
	rec := f.do(method, path, body, tokenCI)
	if rec.Code != want {
		f.t.Fatalf("%s %s: status %d, want %d; body %s", method, path, rec.Code, want, rec.Body.String())
	}
	return rec
}

// register creates a version of id from spec and returns the response.
func (f *fixture) register(id, spec string) createResponse {
	f.t.Helper()
	rec := f.call(http.MethodPost, "/admin/v1/templates/"+id+"/versions", spec, http.StatusCreated)
	var out createResponse
	decode(f.t, rec, &out)
	return out
}

func decode(t *testing.T, rec *httptest.ResponseRecorder, v any) {
	t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), v); err != nil {
		t.Fatalf("decode %s: %v", rec.Body.String(), err)
	}
}

// errorOf decodes an error body.
func errorOf(t *testing.T, rec *httptest.ResponseRecorder) errorDetail {
	t.Helper()
	var body errorBody
	decode(t, rec, &body)
	return body.Error
}

func toMock(msgs []templates.RenderedMessage) []mockllm.Message {
	out := make([]mockllm.Message, len(msgs))
	for i, m := range msgs {
		out[i] = mockllm.Message{Role: m.Role, Content: m.Content}
	}
	return out
}
