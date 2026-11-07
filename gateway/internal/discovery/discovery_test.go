package discovery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
)

func pools(spec map[string][]string) []config.Pool {
	var out []config.Pool
	for _, name := range []string{"large", "small", "extra"} {
		urls, ok := spec[name]
		if !ok {
			continue
		}
		p := config.Pool{Name: name}
		for _, u := range urls {
			p.Replicas = append(p.Replicas, config.Replica{Name: strings.TrimPrefix(u, "http://"), URL: u})
		}
		out = append(out, p)
	}
	return out
}

func names(reps []Replica) []string {
	out := make([]string, len(reps))
	for i, r := range reps {
		out[i] = r.Name
	}
	return out
}

func TestNewStatic(t *testing.T) {
	tests := []struct {
		name    string
		pools   []config.Pool
		wantErr string
	}{
		{name: "ok", pools: pools(map[string][]string{"large": {"http://a:1", "http://b:1"}, "small": {"http://c:1"}})},
		{name: "no pools", pools: nil, wantErr: "no pools"},
		{name: "empty pool", pools: []config.Pool{{Name: "large"}}, wantErr: "no replicas"},
		{name: "duplicate pool", pools: append(pools(map[string][]string{"large": {"http://a:1"}}), pools(map[string][]string{"large": {"http://b:1"}})...), wantErr: "duplicate pool"},
		{name: "bad url", pools: []config.Pool{{Name: "large", Replicas: []config.Replica{{Name: "x", URL: "::nope"}}}}, wantErr: "invalid replica URL"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := NewStatic(tc.pools)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := names(s.Replicas("large")); strings.Join(got, ",") != "a:1,b:1" {
				t.Fatalf("large replicas = %v", got)
			}
			r := s.Replicas("small")[0]
			if r.Pool != "small" || r.BaseURL.Host != "c:1" || r.BaseURL.Scheme != "http" {
				t.Fatalf("small replica = %+v", r)
			}
			if s.Replicas("unknown") != nil {
				t.Fatal("unknown pool should have no replicas")
			}
			if got := strings.Join(s.Pools(), ","); got != "large,small" {
				t.Fatalf("Pools() = %s", got)
			}
		})
	}
}

func TestUpdate(t *testing.T) {
	s, err := NewStatic(pools(map[string][]string{"large": {"http://a:1"}, "small": {"http://c:1"}}))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Update(pools(map[string][]string{"large": {"http://a:1", "http://b:1"}, "small": {"http://d:1"}})); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if got := strings.Join(names(s.Replicas("large")), ","); got != "a:1,b:1" {
		t.Fatalf("after update large = %s", got)
	}
	// Pool set changes are refused and the current set is kept.
	for _, p := range [][]config.Pool{
		pools(map[string][]string{"large": {"http://z:1"}}),
		pools(map[string][]string{"large": {"http://z:1"}, "small": {"http://z:2"}, "extra": {"http://z:3"}}),
		nil,
	} {
		if err := s.Update(p); err == nil {
			t.Fatalf("Update(%v) succeeded, want error", p)
		}
	}
	if got := strings.Join(names(s.Replicas("small")), ","); got != "d:1" {
		t.Fatalf("set changed after refused update: small = %s", got)
	}
}

func TestReload(t *testing.T) {
	s, err := NewStatic(pools(map[string][]string{"large": {"http://a:1"}}))
	if err != nil {
		t.Fatal(err)
	}
	same := func() (*config.Config, error) {
		return &config.Config{Pools: pools(map[string][]string{"large": {"http://a:1"}})}, nil
	}
	if changed, err := Reload(s, same); err != nil || changed {
		t.Fatalf("Reload(same) = %v, %v; want false, nil", changed, err)
	}
	grown := func() (*config.Config, error) {
		return &config.Config{Pools: pools(map[string][]string{"large": {"http://a:1", "http://b:1"}})}, nil
	}
	if changed, err := Reload(s, grown); err != nil || !changed {
		t.Fatalf("Reload(grown) = %v, %v; want true, nil", changed, err)
	}
	failing := func() (*config.Config, error) { return nil, errors.New("bad yaml") }
	if _, err := Reload(s, failing); err == nil {
		t.Fatal("Reload(failing) returned nil error")
	}
	if n := len(s.Replicas("large")); n != 2 {
		t.Fatalf("replicas after failed reload = %d, want 2 (kept)", n)
	}
}

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (w *syncBuffer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

func (w *syncBuffer) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.String()
}

// urls returns n replica URLs.
func urls(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("http://r%d:1", i)
	}
	return out
}

// A pool past the proven size is named once, with its size; a pool at it is
// not mentioned.
func TestWarnOversized(t *testing.T) {
	s, err := NewStatic(pools(map[string][]string{
		"large": urls(config.MaxPoolReplicas + 1),
		"small": urls(config.MaxPoolReplicas),
	}))
	if err != nil {
		t.Fatal(err)
	}
	var logs syncBuffer
	s.WarnOversized(slog.New(slog.NewTextHandler(&logs, nil)))
	out := logs.String()
	if c := strings.Count(out, "level=WARN"); c != 1 {
		t.Fatalf("%d warnings, want 1:\n%s", c, out)
	}
	if !strings.Contains(out, "pool=large") || !strings.Contains(out, "replicas=65") {
		t.Fatalf("the warning does not name the pool and its size:\n%s", out)
	}
}

// A reload past the proven size applies, warns once however many polls
// follow, and a reload back under it does not warn.
func TestWatchWarnsOnAReloadPastTheProvenSize(t *testing.T) {
	s, err := NewStatic(pools(map[string][]string{"large": {"http://a:1"}}))
	if err != nil {
		t.Fatal(err)
	}
	var want atomic.Int64
	want.Store(config.MaxPoolReplicas + 1)
	var polls atomic.Int64
	load := func() (*config.Config, error) {
		polls.Add(1)
		return &config.Config{Pools: pools(map[string][]string{"large": urls(int(want.Load()))})}, nil
	}
	var logs syncBuffer
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		Watch(ctx, s, load, time.Millisecond, slog.New(slog.NewTextHandler(&logs, nil)))
		close(done)
	}()
	waitUntil := func(what string, ok func() bool) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for !ok() {
			if time.Now().After(deadline) {
				cancel()
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(time.Millisecond)
		}
	}
	waitUntil("the large pool", func() bool { return len(s.Replicas("large")) == config.MaxPoolReplicas+1 })
	after := polls.Load()
	waitUntil("more polls", func() bool { return polls.Load() > after+5 })
	want.Store(2)
	waitUntil("the small pool", func() bool { return len(s.Replicas("large")) == 2 })
	cancel()
	<-done
	if c := strings.Count(logs.String(), "level=WARN"); c != 1 {
		t.Fatalf("%d warnings, want 1:\n%s", c, logs.String())
	}
}

func TestWatchAppliesChangesAndStops(t *testing.T) {
	s, err := NewStatic(pools(map[string][]string{"large": {"http://a:1"}}))
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	load := func() (*config.Config, error) {
		n := calls.Add(1)
		if n < 3 {
			return nil, errors.New("temporarily broken")
		}
		return &config.Config{Pools: pools(map[string][]string{"large": {"http://a:1", "http://b:1", "http://c:1"}})}, nil
	}
	var logs syncBuffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		Watch(ctx, s, load, 5*time.Millisecond, logger)
		close(done)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for len(s.Replicas("large")) != 3 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("watcher did not apply the new replica set")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Watch did not return after cancel")
	}
	out := logs.String()
	if c := strings.Count(out, "temporarily broken"); c != 1 {
		t.Fatalf("repeated error logged %d times, want once:\n%s", c, out)
	}
	if !strings.Contains(out, "replica set reloaded") {
		t.Fatalf("missing reload log:\n%s", out)
	}
}

func TestReplicasConcurrentWithUpdate(t *testing.T) {
	s, err := NewStatic(pools(map[string][]string{"large": {"http://a:1"}}))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				if len(s.Replicas("large")) == 0 {
					t.Error("empty replica set observed during update")
					return
				}
			}
		})
	}
	for i := range 200 {
		urls := []string{"http://a:1"}
		if i%2 == 0 {
			urls = append(urls, "http://b:1")
		}
		if err := s.Update(pools(map[string][]string{"large": urls})); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
}

// TestVersionAdvancesOnChange: Version is the change signal health tracking
// and the load view follow. It moves on every applied update and stays put
// when an update is refused or a reload finds nothing new.
func TestVersionAdvancesOnChange(t *testing.T) {
	s, err := NewStatic(pools(map[string][]string{"large": {"http://a:1", "http://b:1"}}))
	if err != nil {
		t.Fatal(err)
	}
	v0 := s.Version()
	if v0 == 0 {
		t.Fatal("initial version is 0; want a non-zero version for the first set")
	}

	// Refused update (pool set changed): version unchanged.
	if err := s.Update(pools(map[string][]string{"small": {"http://z:1"}})); err == nil {
		t.Fatal("pool change accepted")
	}
	if got := s.Version(); got != v0 {
		t.Fatalf("version after refused update = %d, want %d", got, v0)
	}

	// Reload with the same set: nothing applied, version unchanged.
	same := &config.Config{Pools: pools(map[string][]string{"large": {"http://a:1", "http://b:1"}})}
	if changed, err := Reload(s, func() (*config.Config, error) { return same, nil }); err != nil || changed {
		t.Fatalf("Reload(same) = %v, %v", changed, err)
	}
	if got := s.Version(); got != v0 {
		t.Fatalf("version after no-op reload = %d, want %d", got, v0)
	}

	// Reload with a changed set: version moves and the listing follows.
	next := &config.Config{Pools: pools(map[string][]string{"large": {"http://a:1", "http://c:1"}})}
	if changed, err := Reload(s, func() (*config.Config, error) { return next, nil }); err != nil || !changed {
		t.Fatalf("Reload(next) = %v, %v", changed, err)
	}
	if got := s.Version(); got <= v0 {
		t.Fatalf("version after change = %d, want > %d", got, v0)
	}
	if got := strings.Join(names(s.AllReplicas()), ","); got != "a:1,c:1" {
		t.Fatalf("AllReplicas after change = %s", got)
	}
}

// TestVersionUniqueUnderConcurrentUpdates: concurrent writers never publish
// two different sets under one version.
func TestVersionUniqueUnderConcurrentUpdates(t *testing.T) {
	s, err := NewStatic(pools(map[string][]string{"large": {"http://a:1"}}))
	if err != nil {
		t.Fatal(err)
	}
	const writers, each = 4, 50
	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			for i := range each {
				urls := []string{"http://a:1"}
				if (w+i)%2 == 0 {
					urls = append(urls, "http://b:1")
				}
				if err := s.Update(pools(map[string][]string{"large": urls})); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	if got, want := s.Version(), uint64(1+writers*each); got != want {
		t.Fatalf("version = %d after %d updates, want %d", got, writers*each, want)
	}
}

// TestChangedSignalsEachUpdate: Changed is closed by the next applied
// update, not by a refused one, and a fresh channel waits for the one after.
func TestChangedSignalsEachUpdate(t *testing.T) {
	s, err := NewStatic(pools(map[string][]string{"large": {"http://a:1"}}))
	if err != nil {
		t.Fatal(err)
	}
	isClosed := func(ch <-chan struct{}) bool {
		select {
		case <-ch:
			return true
		default:
			return false
		}
	}
	ch := s.Changed()
	if isClosed(ch) {
		t.Fatal("Changed closed before any update")
	}
	if err := s.Update(pools(map[string][]string{"small": {"http://z:1"}})); err == nil {
		t.Fatal("pool change accepted")
	}
	if isClosed(ch) {
		t.Fatal("refused update closed Changed")
	}
	v := s.Version()
	if err := s.Update(pools(map[string][]string{"large": {"http://b:1"}})); err != nil {
		t.Fatal(err)
	}
	if !isClosed(ch) {
		t.Fatal("applied update did not close Changed")
	}
	if s.Version() <= v {
		t.Fatal("version did not advance with Changed")
	}
	next := s.Changed()
	if isClosed(next) {
		t.Fatal("Changed after the update is already closed")
	}
	if err := s.Update(pools(map[string][]string{"large": {"http://c:1"}})); err != nil {
		t.Fatal(err)
	}
	if !isClosed(next) {
		t.Fatal("second update did not close the new Changed channel")
	}
}
