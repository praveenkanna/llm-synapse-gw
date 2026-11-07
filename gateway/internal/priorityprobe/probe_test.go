package priorityprobe

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

type fakeDisc map[string][]discovery.Replica

func (d fakeDisc) Replicas(pool string) []discovery.Replica { return d[pool] }

func rep(pool, name string) discovery.Replica {
	return discovery.Replica{Pool: pool, Name: name, BaseURL: &url.URL{Scheme: "http", Host: name}}
}

// fakeSender answers per replica with a status, or an error for status 0.
type fakeSender struct {
	mu     sync.Mutex
	status map[string]int
	bodies []string
	agents []string // User-Agent of each probe
}

func (s *fakeSender) Do(_ context.Context, r discovery.Replica, req upstream.Request) (*http.Response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bodies = append(s.bodies, string(req.Body))
	s.agents = append(s.agents, req.Header.Get("User-Agent"))
	st := s.status[r.Name]
	if st == 0 {
		return nil, errors.New("connect refused")
	}
	return &http.Response{StatusCode: st, Body: io.NopCloser(strings.NewReader("{}"))}, nil
}

func (s *fakeSender) set(name string, st int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status[name] = st
}

type countFailOpen struct {
	mu sync.Mutex
	n  map[string]int
}

func (c *countFailOpen) FailOpen(stage, reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n[stage+":"+reason]++
}

func newProber(s *fakeSender, f *countFailOpen, model string) *Prober {
	disc := fakeDisc{"large": {rep("large", "a"), rep("large", "b")}, "small": {rep("small", "c")}}
	return New(Config{
		Pools:         []string{"large"},
		Priority:      20,
		Model:         func(discovery.Replica) string { return model },
		PriorityProbe: config.PriorityProbe{Interval: time.Hour, Timeout: time.Second},
	}, disc, s, f)
}

func TestVerifiedOnlyWhenEveryReplicaAccepts(t *testing.T) {
	s := &fakeSender{status: map[string]int{"a": 200, "b": 400}}
	f := &countFailOpen{n: map[string]int{}}
	p := newProber(s, f, "large-model")
	if p.Verified("large") {
		t.Fatal("verified before any round")
	}
	p.Round(context.Background())
	if p.Verified("large") {
		t.Fatal("verified with one replica rejecting priority")
	}
	if f.n[telemetry.StageUpstreamPriority+":"+telemetry.ReasonProbeRejected] != 1 {
		t.Fatalf("fail-opens = %v, want one probe_rejected", f.n)
	}
	s.set("b", 200)
	p.Round(context.Background())
	if !p.Verified("large") {
		t.Fatal("not verified with every replica accepting")
	}
	if p.Verified("small") {
		t.Fatal("unflagged pool verified")
	}
	if !strings.Contains(s.bodies[0], `"priority":20`) || !strings.Contains(s.bodies[0], `"model":"large-model"`) {
		t.Fatalf("probe body = %s", s.bodies[0])
	}
	if s.agents[0] != upstream.ProbeUserAgent {
		t.Fatalf("probe User-Agent = %q, want %q (marks the gateway's own requests)", s.agents[0], upstream.ProbeUserAgent)
	}
}

func TestInconclusiveKeepsLastResult(t *testing.T) {
	s := &fakeSender{status: map[string]int{"a": 200, "b": 200}}
	p := newProber(s, nil, "m")
	p.Round(context.Background())
	s.set("a", 0)   // transport error
	s.set("b", 503) // overloaded
	p.Round(context.Background())
	if !p.Verified("large") {
		t.Fatal("inconclusive probes dropped a verified pool")
	}
}

func TestNoModelNeverVerifies(t *testing.T) {
	s := &fakeSender{status: map[string]int{"a": 200, "b": 200}}
	f := &countFailOpen{n: map[string]int{}}
	p := newProber(s, f, "")
	p.Round(context.Background())
	if p.Verified("large") || f.n[telemetry.StageUpstreamPriority+":"+telemetry.ReasonNoModel] != 1 {
		t.Fatalf("verified %v, fail-opens %v; want unverified, one no_model", p.Verified("large"), f.n)
	}
	if len(s.bodies) != 0 {
		t.Fatal("probe sent without a model")
	}
}

// gatedSender holds probes to one replica until release is closed.
type gatedSender struct {
	*fakeSender
	replica string
	reached chan struct{}
	release chan struct{}
}

func (g *gatedSender) Do(ctx context.Context, r discovery.Replica, req upstream.Request) (*http.Response, error) {
	if r.Name == g.replica {
		close(g.reached)
		<-g.release
	}
	return g.fakeSender.Do(ctx, r, req)
}

func TestNewReplicaUnverifiedWhileProbed(t *testing.T) {
	s := &fakeSender{status: map[string]int{"a": 200, "b": 200}}
	disc := fakeDisc{"large": {rep("large", "a")}}
	g := &gatedSender{fakeSender: s, replica: "b", reached: make(chan struct{}), release: make(chan struct{})}
	p := New(Config{Pools: []string{"large"}, Priority: 20, Model: func(discovery.Replica) string { return "m" },
		PriorityProbe: config.PriorityProbe{Interval: time.Hour, Timeout: time.Second}}, disc, g, nil)
	p.Round(context.Background())
	if !p.Verified("large") {
		t.Fatal("pool not verified after its replica accepted")
	}
	disc["large"] = append(disc["large"], rep("large", "b"))
	done := make(chan struct{})
	go func() { p.Round(context.Background()); close(done) }()
	<-g.reached
	if p.Verified("large") {
		t.Fatal("pool verified while its new replica was still being probed")
	}
	close(g.release)
	<-done
	if !p.Verified("large") {
		t.Fatal("pool not verified after the new replica accepted")
	}
}

func TestReplicaChangeProbesAtOnce(t *testing.T) {
	disc, err := discovery.NewStatic([]config.Pool{{Name: "large", Replicas: []config.Replica{{Name: "a", URL: "http://a:8000"}}}})
	if err != nil {
		t.Fatal(err)
	}
	s := &fakeSender{status: map[string]int{"a": 200, "b": 400}}
	p := New(Config{Pools: []string{"large"}, Priority: 20, Model: func(discovery.Replica) string { return "m" },
		PriorityProbe: config.PriorityProbe{Interval: time.Hour, Timeout: time.Second}}, disc, s, nil)
	p.Start(context.Background())
	defer p.Stop()
	waitFor(t, func() bool { return p.Verified("large") }, "first round")
	err = disc.Update([]config.Pool{{Name: "large", Replicas: []config.Replica{
		{Name: "a", URL: "http://a:8000"}, {Name: "b", URL: "http://b:8000"}}}})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return !p.Verified("large") }, "re-probe after the replica set changed")
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestStartStop(t *testing.T) {
	s := &fakeSender{status: map[string]int{"a": 200, "b": 200}}
	p := newProber(s, nil, "m")
	p.Start(context.Background())
	deadline := time.Now().Add(2 * time.Second)
	for !p.Verified("large") && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	p.Stop()
	if !p.Verified("large") {
		t.Fatal("first round did not run at start")
	}
	var nilProber *Prober
	if nilProber.Verified("large") {
		t.Fatal("nil prober verified")
	}
}
