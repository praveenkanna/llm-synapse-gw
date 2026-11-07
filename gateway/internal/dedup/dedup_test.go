package dedup_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/coalesce"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/dedup"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/respcache"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// recorder is what dedup told a request's telemetry.
type recorder struct {
	failOpens []string // "stage/reason"
	lookups   []bool
}

func (r *recorder) FailOpen(stage, reason string) {
	r.failOpens = append(r.failOpens, stage+"/"+reason)
}
func (r *recorder) CacheLookup(hit bool) { r.lookups = append(r.lookups, hit) }

// cache is a dedup.Cache in memory. getErr fails every lookup; puts are
// kept and reported queued, as respcache does, or as putOutcome when set.
type cache struct {
	mu         sync.Mutex
	entries    map[string]respcache.Entry
	getErr     error
	puts       []put
	putOutcome string
}

type put struct {
	key   string
	ttl   time.Duration
	entry respcache.Entry
}

func newCache() *cache { return &cache{entries: map[string]respcache.Entry{}} }

func (c *cache) Get(ctx context.Context, key string) (respcache.Entry, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.getErr != nil {
		return respcache.Entry{}, false, c.getErr
	}
	e, ok := c.entries[key]
	return e, ok, nil
}

func (c *cache) Put(key string, ttl time.Duration, e respcache.Entry) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.puts = append(c.puts, put{key, ttl, e})
	c.entries[key] = e
	if c.putOutcome != "" {
		return c.putOutcome
	}
	return respcache.StoreQueued
}

const body = `{"model":"m","messages":[{"role":"user","content":"hi"}],"temperature":0}`

func group() *coalesce.Group {
	return coalesce.NewGroup(coalesce.Config{Coalescing: config.Coalescing{MaxFlights: 4, MaxSubscribers: 4, MaxReplayBytes: 1 << 20}})
}

// pastCeiling is the request body with more top-level members than
// bodyindex.MaxMembers.
var pastCeiling = `{"model":"m","temperature":0` + strings.Repeat(`,"x":0`, bodyindex.MaxMembers) + `}`

// indexOf is body's index, as ingress reads it once per request.
func indexOf(body string) *bodyindex.Index {
	ix := new(bodyindex.Index)
	_ = ix.Read([]byte(body))
	return ix
}

// request is a templated request that may share and be cached.
func request() dedup.Request {
	return dedup.Request{
		Index:       indexOf(body),
		Endpoint:    "/v1/chat/completions",
		Template:    "kb",
		Version:     1,
		Pool:        "large",
		Coalescible: true,
		CacheTTL:    30 * time.Second,
	}
}

// TestBeginDecides: what a request gets, and what is recorded on the way.
func TestBeginDecides(t *testing.T) {
	cachedEntry := respcache.Entry{Status: 200, ContentType: "application/json", Body: []byte(`{"ok":true}`)}
	cut, cancel := context.WithCancel(context.Background())
	cancel()
	tests := []struct {
		name          string
		noGroup       bool
		noCache       bool
		getErr        error
		stored        bool // the cache holds this request's response
		running       bool // an identical request already leads a flight
		ctx           context.Context
		req           func(r *dedup.Request)
		want          dedup.Kind
		wantFailOpens []string
		wantLookups   []bool
	}{
		{name: "nothing on", noGroup: true, noCache: true, want: dedup.Pass},
		{name: "template opted out", req: func(r *dedup.Request) { r.Coalescible, r.CacheTTL = false, 0 }, want: dedup.Pass},
		{name: "sampling varies", req: func(r *dedup.Request) { r.Index = indexOf(`{"model":"m","temperature":0.7}`) },
			want: dedup.Pass, wantFailOpens: []string{telemetry.StageCoalesce + "/" + telemetry.ReasonNotDeterministic}},
		{name: "sampling varies, cache only", req: func(r *dedup.Request) { r.Index, r.Coalescible = indexOf(`{"model":"m"}`), false },
			want: dedup.Pass, wantFailOpens: []string{telemetry.StageCache + "/" + telemetry.ReasonNotDeterministic}},
		// E5: a body the index cannot hold is valid JSON the replica serves;
		// it runs alone and is never cached.
		{name: "past the member ceiling", req: func(r *dedup.Request) { r.Index = indexOf(pastCeiling) },
			want: dedup.Pass, wantFailOpens: []string{telemetry.StageCoalesce + "/" + telemetry.ReasonTooManyMembers}},
		{name: "past the member ceiling, cache only", req: func(r *dedup.Request) { r.Index, r.Coalescible = indexOf(pastCeiling), false },
			want: dedup.Pass, wantFailOpens: []string{telemetry.StageCache + "/" + telemetry.ReasonTooManyMembers}},
		{name: "first of its kind leads", want: dedup.Lead, wantLookups: []bool{false}},
		{name: "joins a running flight before asking the cache", running: true, stored: true, want: dedup.Follow},
		{name: "served from the cache", stored: true, want: dedup.Cached, wantLookups: []bool{true}},
		{name: "a lookup that fails is a miss", getErr: errors.New("down"), want: dedup.Lead,
			wantFailOpens: []string{telemetry.StageCache + "/" + telemetry.ReasonRedisError}, wantLookups: []bool{false}},
		{name: "with the cache's reason", getErr: &respcache.Error{Reason: respcache.ReasonTimeout, Err: errors.New("slow")}, want: dedup.Lead,
			wantFailOpens: []string{telemetry.StageCache + "/" + respcache.ReasonTimeout}, wantLookups: []bool{false}},
		{name: "caller left during the lookup", getErr: context.Canceled, ctx: cut, want: dedup.Gone},
		{name: "a miss with nothing to record it runs alone", noGroup: true, want: dedup.Pass, wantLookups: []bool{false}},
		{name: "budgeted skips the cache", stored: true, req: func(r *dedup.Request) { r.Budgeted = true }, want: dedup.Lead},
		{name: "cache only still leads, to record", req: func(r *dedup.Request) { r.Coalescible = false }, want: dedup.Lead, wantLookups: []bool{false}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := dedup.Config{KeyBodyBytes: 1 << 20, MaxTTL: time.Hour}
			c := newCache()
			c.getErr = tc.getErr
			if !tc.noGroup {
				cfg.Group = group()
			}
			if !tc.noCache {
				cfg.Cache = c
			}
			p := dedup.New(cfg)
			req := request()
			if tc.req != nil {
				tc.req(&req)
			}
			if tc.running {
				first := p.Begin(context.Background(), &recorder{}, request())
				if first.Kind != dedup.Lead {
					t.Fatalf("setup: %v, want a leader", first.Kind)
				}
				defer first.Leader.Finish(dedup.Delivery{})
			}
			if tc.stored {
				c.entries[cacheKey(t, request())] = cachedEntry
			}
			ctx := tc.ctx
			if ctx == nil {
				ctx = context.Background()
			}
			rec := &recorder{}
			d := p.Begin(ctx, rec, req)
			if d.Kind == dedup.Lead {
				defer d.Leader.Finish(dedup.Delivery{})
			}
			if d.Kind != tc.want {
				t.Fatalf("kind %v, want %v", d.Kind, tc.want)
			}
			if !slices.Equal(rec.failOpens, tc.wantFailOpens) || !slices.Equal(rec.lookups, tc.wantLookups) {
				t.Errorf("recorded fail-opens %v lookups %v, want %v and %v", rec.failOpens, rec.lookups, tc.wantFailOpens, tc.wantLookups)
			}
			if (d.Kind == dedup.Follow) != (d.Sub != nil) || (d.Kind == dedup.Lead) != (d.Leader != nil) {
				t.Errorf("decision %+v carries the wrong parts for its kind", d)
			}
			if d.Kind == dedup.Cached && string(d.Entry.Body) != string(cachedEntry.Body) {
				t.Errorf("served %q, want the stored entry", d.Entry.Body)
			}
		})
	}
}

// TestFlightFull: a request that can neither join nor lead runs alone,
// with the reason recorded.
func TestFlightFull(t *testing.T) {
	g := coalesce.NewGroup(coalesce.Config{Coalescing: config.Coalescing{MaxFlights: 1, MaxSubscribers: 4, MaxReplayBytes: 1 << 20}})
	p := dedup.New(dedup.Config{Group: g, KeyBodyBytes: 1 << 20})
	first := p.Begin(context.Background(), &recorder{}, request())
	defer first.Leader.Finish(dedup.Delivery{})
	other := request()
	other.Pool = "small"
	rec := &recorder{}
	if d := p.Begin(context.Background(), rec, other); d.Kind != dedup.Pass {
		t.Fatalf("kind %v, want pass", d.Kind)
	}
	if want := []string{telemetry.StageCoalesce + "/" + telemetry.ReasonFlightsFull}; !slices.Equal(rec.failOpens, want) {
		t.Fatalf("fail-opens %v, want %v", rec.failOpens, want)
	}
}

// TestKeys: the priority class and the budget narrow the flight a request
// may join, never the cache entry it may be served; every part narrows
// both.
func TestKeys(t *testing.T) {
	p := dedup.New(dedup.Config{Group: group(), Cache: newCache(), KeyBodyBytes: 1 << 20, MaxTTL: time.Hour})
	lead := p.Begin(context.Background(), &recorder{}, request())
	defer lead.Leader.Finish(dedup.Delivery{})
	for name, change := range map[string]func(*dedup.Request){
		"priority":  func(r *dedup.Request) { r.Priority = "batch" },
		"budget":    func(r *dedup.Request) { r.Budget = "500" },
		"pool":      func(r *dedup.Request) { r.Pool = "small" },
		"weights":   func(r *dedup.Request) { r.Weights = "v2" },
		"encoding":  func(r *dedup.Request) { r.Encodings = []string{"gzip"} },
		"encodings": func(r *dedup.Request) { r.Encodings = []string{"gzip", "br"} },
		"version":   func(r *dedup.Request) { r.Version = 2 },
		"template":  func(r *dedup.Request) { r.Template = "kb2" },
		"endpoint":  func(r *dedup.Request) { r.Endpoint = "/v1/completions" },
		"body":      func(r *dedup.Request) { r.Index = indexOf(`{"model":"m","messages":[],"temperature":0}`) },
	} {
		req := request()
		change(&req)
		if d := p.Begin(context.Background(), &recorder{}, req); d.Kind != dedup.Lead {
			t.Errorf("%s: %v, want its own flight", name, d.Kind)
		} else {
			d.Leader.Finish(dedup.Delivery{})
		}
	}

	c := newCache()
	p = dedup.New(dedup.Config{Group: group(), Cache: c, KeyBodyBytes: 1 << 20, MaxTTL: time.Hour})
	first := p.Begin(context.Background(), &recorder{}, request())
	finishWith(t, first.Leader, http.StatusOK, "application/json", `{}`, dedup.Delivery{End: dedup.Whole})
	req := request()
	req.Priority = "batch"
	if d := p.Begin(context.Background(), &recorder{}, req); d.Kind != dedup.Cached {
		t.Errorf("another priority class: %v, want the cached entry", d.Kind)
	}
}

// TestFinish: a leader's response is stored only when it was relayed
// whole, is a success sent as what it is, and was not cut by a cap the
// gateway set; followers end as the leader did.
func TestFinish(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		encoding  string
		delivery  dedup.Delivery
		wantStore bool
		wantSub   error // what a follower's Serve ends with
	}{
		{name: "whole success", status: 200, delivery: dedup.Delivery{End: dedup.Whole}, wantStore: true},
		{name: "identity encoding", status: 200, encoding: "identity", delivery: dedup.Delivery{End: dedup.Whole}, wantStore: true},
		{name: "runaway", status: 200, delivery: dedup.Delivery{End: dedup.Whole, Runaway: true}},
		{name: "not a success", status: 500, delivery: dedup.Delivery{End: dedup.Whole}},
		{name: "a success other than 200", status: 201, delivery: dedup.Delivery{End: dedup.Whole}},
		{name: "a 200 the gateway answered", status: 200, delivery: dedup.Delivery{End: dedup.Answered}},
		{name: "encoded", status: 200, encoding: "gzip", delivery: dedup.Delivery{End: dedup.Whole}},
		{name: "the gateway answered", status: 429, delivery: dedup.Delivery{End: dedup.Answered}},
		{name: "the response failed", status: 200, delivery: dedup.Delivery{End: dedup.Failed}, wantSub: coalesce.ErrResponseFailed},
		{name: "cut off", status: 200, delivery: dedup.Delivery{End: dedup.Cut}, wantSub: coalesce.ErrFlightFailed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newCache()
			var outcomes []string
			g := group()
			p := dedup.New(dedup.Config{Group: g, Cache: c, KeyBodyBytes: 1 << 20, MaxTTL: 10 * time.Second,
				OnStore: func(o string) { outcomes = append(outcomes, o) }})
			d := p.Begin(context.Background(), &recorder{}, request())
			sub := g.Join(flightKey(t, request()))
			_, w := d.Leader.Start(context.Background(), httptest.NewRecorder())
			w.Header().Set("Content-Type", "application/json")
			if tc.encoding != "" {
				w.Header().Set("Content-Encoding", tc.encoding)
			}
			w.WriteHeader(tc.status)
			_, _ = w.Write([]byte(`{"ok":true}`))
			d.Leader.Finish(tc.delivery)

			if got := len(c.puts) == 1; got != tc.wantStore {
				t.Fatalf("stored %d, want stored=%v", len(c.puts), tc.wantStore)
			}
			if tc.wantStore {
				if pt := c.puts[0]; pt.ttl != 10*time.Second || pt.entry.Status != 200 || pt.entry.ContentType != "application/json" || string(pt.entry.Body) != `{"ok":true}` {
					t.Errorf("stored %+v, want the response for the TTL capped at cache.max_ttl", pt)
				}
				if len(outcomes) != 0 {
					t.Errorf("outcomes %v counted for a store the cache reports itself", outcomes)
				}
			} else if !slices.Equal(outcomes, []string{respcache.StoreSkipped}) {
				t.Errorf("outcomes %v, want one skipped", outcomes)
			}
			if sub == nil {
				t.Fatal("no follower")
			}
			defer sub.Close()
			if err := sub.Serve(context.Background(), httptest.NewRecorder(), nil); !errors.Is(err, tc.wantSub) {
				t.Errorf("follower ended with %v, want %v", err, tc.wantSub)
			}
		})
	}
}

// TestFinishReportsAnImmediateOutcome: a store the cache could not queue
// is counted at once; one not eligible for the cache is not counted.
func TestFinishReportsAnImmediateOutcome(t *testing.T) {
	c := newCache()
	c.putOutcome = respcache.StoreDropped
	var outcomes []string
	p := dedup.New(dedup.Config{Group: group(), Cache: c, KeyBodyBytes: 1 << 20, MaxTTL: time.Hour,
		OnStore: func(o string) { outcomes = append(outcomes, o) }})
	finishWith(t, p.Begin(context.Background(), &recorder{}, request()).Leader, 200, "application/json", `{}`, dedup.Delivery{End: dedup.Whole})
	req := request()
	req.CacheTTL = 0
	req.Pool = "other"
	finishWith(t, p.Begin(context.Background(), &recorder{}, req).Leader, 200, "application/json", `{}`, dedup.Delivery{End: dedup.Whole})
	if !slices.Equal(outcomes, []string{respcache.StoreDropped}) {
		t.Fatalf("outcomes %v, want one dropped", outcomes)
	}
}

// finishWith has l's exchange write one response and end as d says.
func finishWith(t *testing.T, l *dedup.Leader, status int, contentType, body string, d dedup.Delivery) {
	t.Helper()
	if l == nil {
		t.Fatal("not a leader")
	}
	_, w := l.Start(context.Background(), httptest.NewRecorder())
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
	l.Finish(d)
}

// cacheKey is the cache key Begin gives req.
func cacheKey(t *testing.T, req dedup.Request) string {
	t.Helper()
	fp, reason := coalesce.FingerprintOf(req.Index, 1<<20)
	if reason != "" {
		t.Fatal(reason)
	}
	return fp.Key(parts(req))
}

// flightKey is the in-flight key Begin gives req.
func flightKey(t *testing.T, req dedup.Request) string {
	t.Helper()
	fp, reason := coalesce.FingerprintOf(req.Index, 1<<20)
	if reason != "" {
		t.Fatal(reason)
	}
	return fp.Key(parts(req), req.Priority, req.Budget)
}

// parts are req's key parts, as Begin builds them.
func parts(req dedup.Request) coalesce.Parts {
	return coalesce.Parts{Endpoint: req.Endpoint, Template: req.Template, Version: strconv.Itoa(req.Version),
		Pool: req.Pool, Weights: req.Weights, Encoding: strings.Join(req.Encodings, ",")}
}
