package ingress

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/coalesce"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/compiler"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/mockllm"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/respcache"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates"
)

// "kb" may be cached for 30s; it does not coalesce, so every miss runs its
// own call and only the cache can save one.
const kbSpec = `{"model":"large-model","cacheable":true,"cache_ttl":"30s","variables":[{"name":"q","scope":"shared"}],
 "messages":[{"role":"system","segments":[{"kind":"static","text":"You are a support assistant. Answer briefly."}]},
             {"role":"user","segments":[{"kind":"volatile","text":"{{q}}"}]}]}`

// memCache is a ResponseCache in memory. getErr, when set, fails every
// lookup; hold, when set, keeps every lookup until its caller leaves.
type memCache struct {
	mu      sync.Mutex
	entries map[string]respcache.Entry
	ttls    map[string]time.Duration
	getErr  error
	hold    bool
	gets    int
}

func newMemCache() *memCache {
	return &memCache{entries: map[string]respcache.Entry{}, ttls: map[string]time.Duration{}}
}

func (c *memCache) Get(ctx context.Context, key string) (respcache.Entry, bool, error) {
	c.mu.Lock()
	c.gets++
	hold := c.hold
	c.mu.Unlock()
	if hold {
		<-ctx.Done()
		return respcache.Entry{}, false, ctx.Err()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.getErr != nil {
		return respcache.Entry{}, false, c.getErr
	}
	e, ok := c.entries[key]
	return e, ok, nil
}

func (c *memCache) Put(key string, ttl time.Duration, e respcache.Entry) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = e
	c.ttls[key] = ttl
	return respcache.StoreStored
}

func (c *memCache) stored() (n int, ttls []time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, d := range c.ttls {
		ttls = append(ttls, d)
	}
	return len(c.entries), ttls
}

func (c *memCache) lookups() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gets
}

// newCacheFixture is a gateway with the response cache over a "kb"
// template. capTokens, when positive, is the max_tokens the gateway sets.
func newCacheFixture(t *testing.T, yamlExtra string, capTokens int) (*dedupFixture, *memCache) {
	t.Helper()
	snap, errs := templates.NewSnapshot([]templates.Record{
		{ID: "kb", Version: 1, Status: templates.StatusActive, Spec: json.RawMessage(kbSpec),
			Counts: json.RawMessage(`{"pool":"large","prefix_tokens":12}`)},
		{ID: "kb", Version: 2, Status: templates.StatusDeprecated, Spec: json.RawMessage(kbSpec)},
		{ID: "kb2", Version: 1, Status: templates.StatusActive, Spec: json.RawMessage(kbSpec)},
	}, time.Now())
	if len(errs) > 0 {
		t.Fatal(errs)
	}
	copts := compiler.Options{MaxTokens: config.MaxTokensPolicy{Enabled: false}}
	if capTokens > 0 {
		copts = compiler.Options{
			MaxTokens: config.MaxTokensPolicy{Enabled: true, Margin: 0.25, MinSamples: templateMinSamples},
			Ceilings:  map[string]int{"large": capTokens},
		}
	}
	tel := telemetry.New(telemetry.Options{})
	group := coalesce.NewGroup(coalesce.Config{Coalescing: config.Coalescing{MaxFlights: 64, MaxSubscribers: 128, MaxReplayBytes: 1 << 20}})
	cache := newMemCache()
	opts := Options{
		Metrics:  tel.Metrics(),
		Coalesce: group,
		Cache:    cache,
		Compiler: compiler.New(staticSnap{snap}, copts),
	}
	f := newFixtureWith(t, yamlExtra, opts, nil)
	mux := http.NewServeMux()
	f.h.Register(mux)
	f.gw.Close()
	f.gw, f.tel = httptest.NewServer(tel.Middleware(mux)), tel
	t.Cleanup(f.gw.Close)
	return &dedupFixture{fixture: f, group: group}, cache
}

// A miss runs on a replica and is stored with the template's TTL; the same
// request again is answered from the cache, byte for byte, with no replica.
func TestCacheMissStoresAndHitSkipsTheReplicas(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%t", stream), func(t *testing.T) {
			f, cache := newCacheFixture(t, "", 0)
			body := faqBody("where is my order?", stream, "")
			hdr := faqHeaders("kb", 20)

			miss := f.ask(context.Background(), body, hdr)
			if miss.err != nil || miss.status != http.StatusOK {
				t.Fatalf("miss: status %d err %v", miss.status, miss.err)
			}
			if got := miss.header.Get(HeaderCache); got != "" {
				t.Fatalf("a miss is marked %s: %q", HeaderCache, got)
			}
			eventually(t, "the store", func() bool { n, _ := cache.stored(); return n == 1 })
			if _, ttls := cache.stored(); ttls[0] != 30*time.Second {
				t.Fatalf("stored with TTL %s, want the template's 30s", ttls[0])
			}

			hit := f.ask(context.Background(), body, hdr)
			if hit.err != nil || hit.status != http.StatusOK || hit.header.Get(HeaderCache) != "hit" {
				t.Fatalf("hit: status %d %s=%q err %v", hit.status, HeaderCache, hit.header.Get(HeaderCache), hit.err)
			}
			if !bytes.Equal(hit.body, miss.body) {
				t.Fatalf("hit body differs from the original (%d vs %d bytes)", len(hit.body), len(miss.body))
			}
			if got, want := hit.header.Get("Content-Type"), miss.header.Get("Content-Type"); got != want {
				t.Fatalf("hit Content-Type %q, want %q", got, want)
			}
			if n := f.upstreamRequests(); n != 1 {
				t.Fatalf("replicas ran %d generations, want 1 (the hit must not reach them)", n)
			}
			labels := `{template="kb",version="1"}`
			if h, m := f.metric(telemetry.MetricCacheHits+labels), f.metric(telemetry.MetricCacheMisses+labels); h != 1 || m != 1 {
				t.Fatalf("hits %v misses %v, want 1 and 1", h, m)
			}
		})
	}
}

// AC-M4-3 for the cache: a stored answer is never served to a request with
// the same body under another template, version, pool, weights version or
// encoding (a weights rollout or a remapped model must not see the old
// answers).
func TestKeyPartsKeepCacheEntriesApart(t *testing.T) {
	for _, v := range keyPartVariants {
		t.Run(v.name, func(t *testing.T) {
			f, cache := newCacheFixture(t, "", 0)
			body := faqBody("where is my order?", false, "")
			hdr := faqHeaders("kb", 5)
			if a := f.ask(context.Background(), body, hdr); a.status != http.StatusOK {
				t.Fatalf("status %d err %v", a.status, a.err)
			}
			eventually(t, "the store", func() bool { n, _ := cache.stored(); return n == 1 })

			other := cloneHeaders(hdr)
			undo := v.set(f.h, other)
			a := f.ask(context.Background(), body, other)
			if a.status != http.StatusOK || a.header.Get(HeaderCache) != "" {
				t.Fatalf("under another %s: status %d %s=%q, want a miss", v.name, a.status, HeaderCache, a.header.Get(HeaderCache))
			}
			eventually(t, "its own store", func() bool { n, _ := cache.stored(); return n == 2 })
			undo()
			if a := f.ask(context.Background(), body, hdr); a.header.Get(HeaderCache) != "hit" {
				t.Fatalf("the original request missed after the %s variant", v.name)
			}
			if n := f.allGenerations(); n != 2 {
				t.Fatalf("replicas ran %d generations, want 2", n)
			}
		})
	}
}

// AC-M4-3: a template that may be cached but did not opt in to coalescing
// never shares a call, even when identical requests arrive together.
func TestCacheOnlyTemplateNeverMerges(t *testing.T) {
	f, _ := newCacheFixture(t, "", 0)
	hdr := faqHeaders("kb", 10)
	hdr[mockllm.HeaderTTFTMs] = "300"
	const n = 50
	hits := 0
	for i, a := range f.askMany(n, faqBody("where is my order?", false, ""), hdr) {
		if a.err != nil || a.status != http.StatusOK {
			t.Fatalf("request %d: status %d err %v", i, a.status, a.err)
		}
		if a.header.Get(HeaderCache) == "hit" {
			hits++
		}
	}
	if s := f.group.Stats(); s.Joined != 0 {
		t.Fatalf("%d requests joined a flight", s.Joined)
	}
	if got := f.upstreamRequests(); got+hits != n {
		t.Fatalf("replicas ran %d generations and the cache answered %d, for %d requests", got, hits, n)
	}
}

// cache.max_ttl caps a template's cache_ttl.
func TestCacheTTLIsCappedByMaxTTL(t *testing.T) {
	f, cache := newCacheFixture(t, "cache: {max_ttl: 10s}\n", 0)
	if a := f.ask(context.Background(), faqBody("where is my order?", false, ""), faqHeaders("kb", 5)); a.status != http.StatusOK {
		t.Fatalf("status %d err %v", a.status, a.err)
	}
	eventually(t, "the store", func() bool { n, _ := cache.stored(); return n == 1 })
	if _, ttls := cache.stored(); ttls[0] != 10*time.Second {
		t.Fatalf("stored with TTL %s, want cache.max_ttl 10s", ttls[0])
	}
}

// Redis failing is a miss with its reason counted: the request is served by
// a replica as if there were no cache.
func TestCacheFailureFailsOpen(t *testing.T) {
	f, cache := newCacheFixture(t, "", 0)
	cache.getErr = &respcache.Error{Reason: respcache.ReasonTimeout, Err: errors.New("slow")}
	for range 3 {
		a := f.ask(context.Background(), faqBody("where is my order?", false, ""), faqHeaders("kb", 5))
		if a.err != nil || a.status != http.StatusOK || a.header.Get(HeaderCache) != "" {
			t.Fatalf("status %d %s=%q err %v", a.status, HeaderCache, a.header.Get(HeaderCache), a.err)
		}
	}
	if n := f.upstreamRequests(); n != 3 {
		t.Fatalf("replicas ran %d generations, want 3", n)
	}
	series := fmt.Sprintf(`%s{stage="cache",reason=%q}`, telemetry.MetricFailOpen, respcache.ReasonTimeout)
	if got := f.metric(series); got != 3 {
		t.Fatalf("%s = %v, want 3", series, got)
	}
}

// Only a complete 200 that the replica itself ended is stored; a request
// whose sampling is not fixed is never even looked up.
func TestCacheStoresOnlyCompleteSuccesses(t *testing.T) {
	t.Run("not deterministic", func(t *testing.T) {
		f, cache := newCacheFixture(t, "", 0)
		body := `{"model":"large-model","temperature":0.7,` + tmplMessages + `}`
		for range 2 {
			if a := f.ask(context.Background(), body, faqHeaders("kb", 5)); a.status != http.StatusOK {
				t.Fatalf("status %d err %v", a.status, a.err)
			}
		}
		if cache.lookups() != 0 {
			t.Fatalf("%d lookups for a request whose sampling is not fixed", cache.lookups())
		}
		series := fmt.Sprintf(`%s{stage="cache",reason=%q}`, telemetry.MetricFailOpen, telemetry.ReasonNotDeterministic)
		if got := f.metric(series); got != 2 {
			t.Fatalf("%s = %v, want 2", series, got)
		}
	})
	t.Run("replica error", func(t *testing.T) {
		f, cache := newCacheFixture(t, "", 0)
		hdr := faqHeaders("kb", 5)
		hdr[mockllm.HeaderFault] = "5xx"
		if a := f.ask(context.Background(), faqBody("where is my order?", false, ""), hdr); a.status < 500 {
			t.Fatalf("status %d, want the replica's 5xx", a.status)
		}
		f.assertNothingStored(t, cache)
	})
	t.Run("stream cut mid-response", func(t *testing.T) {
		f, cache := newCacheFixture(t, "", 0)
		hdr := faqHeaders("kb", 20)
		hdr[mockllm.HeaderFault] = "drop"
		_ = f.ask(context.Background(), faqBody("where is my order?", true, ""), hdr)
		f.assertNothingStored(t, cache)
	})
	t.Run("cut by the gateway's max_tokens", func(t *testing.T) {
		f, cache := newCacheFixture(t, "", 3)
		if a := f.ask(context.Background(), faqBody("where is my order?", false, ""), faqHeaders("kb", 10)); a.status != http.StatusOK {
			t.Fatalf("status %d err %v", a.status, a.err)
		}
		f.assertNothingStored(t, cache)
	})
}

func (f *dedupFixture) assertNothingStored(t *testing.T, cache *memCache) {
	t.Helper()
	series := fmt.Sprintf(`%s{outcome=%q}`, telemetry.MetricCacheStores, respcache.StoreSkipped)
	eventually(t, "the skipped store", func() bool { return f.metric(series) == 1 })
	if n, _ := cache.stored(); n != 0 {
		t.Fatalf("%d entries stored, want none", n)
	}
}
