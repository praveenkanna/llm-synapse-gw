package ingress

import (
	"context"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/mockllm"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/health"
)

// Passive ejection (M1-12): only a hard connect failure (refused,
// unreachable, NXDOMAIN) while the caller is still connected is reported to
// the router's health tracking, at most once per request and replica. Each
// test also checks that every lease is released.

// ejectOnFirst makes a single reported failure eject, so a wrongly
// reported one shows up as an ejection.
func ejectOnFirst(c *health.Config) { c.PassiveFailureThreshold = 1 }

// TestClientAbortDoesNotEject: callers giving up on a slow request
// (review B3: a client leaving after 200 ms ejected r0) report nothing.
func TestClientAbortDoesNotEject(t *testing.T) {
	for _, p := range routingPolicies {
		t.Run(p.name, func(t *testing.T) {
			mock := startMock(t, nil)
			g := newLeaseGatewayWith(t, p, []string{mock.URL()}, chatBody, leaseOptions{health: ejectOnFirst})
			const aborts = 3
			for i := range aborts {
				ctx, cancel := context.WithCancel(context.Background())
				req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.gw.URL+PathChatCompletions, strings.NewReader(chatBody))
				if err != nil {
					t.Fatal(err)
				}
				for k, v := range g.headers(map[string]string{mockllm.HeaderFault: "hang"}) {
					req.Header.Set(k, v)
				}
				done := make(chan struct{})
				go func() {
					defer close(done)
					if resp, err := g.gw.Client().Do(req); err == nil {
						_ = resp.Body.Close()
					}
				}()
				waitFor(t, "request reached the replica", func() bool { return mock.Mock().Stats().Requests == uint64(i+1) })
				cancel()
				<-done
				g.waitHandled(t, 1)
			}
			if got := g.counter.reported(); len(got) != 0 {
				t.Fatalf("failures reported for client aborts: %v", got)
			}
			if g.tracker != nil && !g.tracker.IsHealthy("r0") {
				t.Fatal("client aborts ejected r0")
			}
			if s := g.outcomes(t); s.ClientDisconnects != aborts {
				t.Errorf("stats = %+v, want %d client disconnects", s, aborts)
			}
			g.assertReleased(t, aborts)
		})
	}
}

// TestResponseHeaderTimeoutDoesNotEject: a replica too busy to answer
// within the response-header timeout gets a 504, not an ejection.
func TestResponseHeaderTimeoutDoesNotEject(t *testing.T) {
	for _, p := range routingPolicies {
		t.Run(p.name, func(t *testing.T) {
			mock := startMock(t, nil)
			g := newLeaseGatewayWith(t, p, []string{mock.URL()}, chatBody, leaseOptions{
				yaml:   "upstream:\n  timeouts:\n    response_header: 50ms\n",
				health: ejectOnFirst,
			})
			const requests = 3
			for range requests {
				resp, got, _ := g.do(t, chatBody, g.headers(map[string]string{mockllm.HeaderFault: "hang"}))
				if resp.StatusCode != http.StatusGatewayTimeout {
					t.Fatalf("status = %d, want 504 (%s)", resp.StatusCode, got)
				}
				g.waitHandled(t, 1)
			}
			if got := g.counter.reported(); len(got) != 0 {
				t.Fatalf("failures reported for response-header timeouts: %v", got)
			}
			if g.tracker != nil && !g.tracker.IsHealthy("r0") {
				t.Fatal("response-header timeouts ejected r0")
			}
			g.assertReleased(t, requests)
		})
	}
}

// TestFailureAfterRequestSentNotReported: a replica that drops the
// connection after receiving the request is not reported (not a connect
// failure).
func TestFailureAfterRequestSentNotReported(t *testing.T) {
	for _, p := range routingPolicies {
		t.Run(p.name, func(t *testing.T) {
			g := newLeaseGatewayWith(t, p, []string{startMock(t, nil).URL()}, chatBody, leaseOptions{health: ejectOnFirst})
			resp, got, _ := g.do(t, chatBody, g.headers(map[string]string{mockllm.HeaderFault: "drop"}))
			if resp.StatusCode != http.StatusBadGateway {
				t.Fatalf("status = %d, want 502 (%s)", resp.StatusCode, got)
			}
			g.waitHandled(t, 1)
			if got := g.counter.reported(); len(got) != 0 {
				t.Fatalf("failures reported after the request was sent: %v", got)
			}
			g.assertReleased(t, 1)
		})
	}
}

// TestRepeatedConnectFailuresEject: connect failures on r0 are reported
// each time and eject it at the threshold (3 by default); the next request
// for the same conversation goes straight to r1.
func TestRepeatedConnectFailuresEject(t *testing.T) {
	p := routingPolicy{name: "affinity_ring", affinity: true, ring: true}
	g := newLeaseGateway(t, p, []string{deadURL(t), startMock(t, nil).URL()}, chatBody)
	threshold := health.ConfigFrom(config.Defaults().Router).PassiveFailureThreshold
	for i := range threshold {
		if !g.tracker.IsHealthy("r0") {
			t.Fatalf("r0 ejected after %d connect failures, threshold %d", i, threshold)
		}
		resp, got, _ := g.do(t, chatBody, g.headers(nil))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200 from the retry on r1 (%s)", i+1, resp.StatusCode, got)
		}
		g.waitHandled(t, 1)
	}
	if got, want := g.counter.reported(), slices.Repeat([]string{"r0"}, threshold); !slices.Equal(got, want) {
		t.Fatalf("reported = %v, want %v", got, want)
	}
	if g.tracker.IsHealthy("r0") {
		t.Fatalf("r0 still in after %d connect failures", threshold)
	}

	resp, got, _ := g.do(t, chatBody, g.headers(nil))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("after ejection: status = %d (%s)", resp.StatusCode, got)
	}
	g.waitHandled(t, 1)
	if s := g.outcomes(t); s.ConnectRetries != float64(threshold) {
		t.Errorf("stats = %+v, want %d retries (none after the ejection)", s, threshold)
	}
	g.assertReleased(t, 2*threshold+1)
}

// TestAllEjectedPoolStillServes: with every replica of the pool ejected,
// requests fail open to round-robin over all of them and succeed.
func TestAllEjectedPoolStillServes(t *testing.T) {
	p := routingPolicy{name: "affinity_ring", affinity: true, ring: true}
	g := newLeaseGateway(t, p, []string{startMock(t, nil).URL(), startMock(t, nil).URL()}, chatBody)
	g.tracker.Eject("r0", health.ReasonStuckEngine)
	g.tracker.Eject("r1", health.ReasonTransportFailure)
	const requests = 4
	for range requests {
		resp, got, _ := g.do(t, chatBody, g.headers(nil))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d with every replica ejected, want 200 (fail open) (%s)", resp.StatusCode, got)
		}
		g.waitHandled(t, 1)
	}
	g.assertReleased(t, requests)
}

// TestDialTimeoutOnHealthyReplicaDoesNotCount: a connect budget that runs
// out against a healthy replica (ADR 0014: a saturated host) is retried but
// never reported, so it cannot eject the replica.
func TestDialTimeoutOnHealthyReplicaDoesNotCount(t *testing.T) {
	for _, p := range routingPolicies {
		t.Run(p.name, func(t *testing.T) {
			g := newLeaseGatewayWith(t, p, []string{startMock(t, nil).URL()}, chatBody, leaseOptions{
				yaml:    "upstream:\n  timeouts:\n    connect: 1ns\n", // every dial times out
				health:  ejectOnFirst,
				backoff: immediateBackoff,
			})
			resp, got, _ := g.do(t, chatBody, g.headers(nil))
			if resp.StatusCode != http.StatusBadGateway {
				t.Fatalf("status = %d, want 502 after the connect retry (%s)", resp.StatusCode, got)
			}
			assertErrorBody(t, got, "upstream_connect")
			g.waitHandled(t, 1)
			if got := g.counter.reported(); len(got) != 0 {
				t.Fatalf("dial timeouts reported as passive failures: %v", got)
			}
			if g.tracker != nil && !g.tracker.IsHealthy("r0") {
				t.Fatal("dial timeouts ejected a healthy replica")
			}
			g.assertReleased(t, 1)
		})
	}
}

// TestSameReplicaRetryReportsOnce: a refused connect and its refused
// same-replica retry are one request's failure on one replica: reported
// once, so passive_failure_threshold counts requests.
func TestSameReplicaRetryReportsOnce(t *testing.T) {
	for _, p := range routingPolicies {
		t.Run(p.name, func(t *testing.T) {
			g := newLeaseGatewayWith(t, p, []string{deadURL(t)}, chatBody, leaseOptions{backoff: immediateBackoff})
			resp, got, _ := g.do(t, chatBody, g.headers(nil))
			if resp.StatusCode != http.StatusBadGateway {
				t.Fatalf("status = %d, want 502 (%s)", resp.StatusCode, got)
			}
			g.waitHandled(t, 1)
			if s := g.outcomes(t); s.ConnectRetries != 1 {
				t.Fatalf("stats = %+v, want the one same-replica retry", s)
			}
			if got := g.counter.reported(); !slices.Equal(got, []string{"r0"}) {
				t.Fatalf("reported = %v, want [r0] once", got)
			}
			g.assertReleased(t, 1)
		})
	}
}

// TestRetryGoesToEjectedReplicaWhenOnlyFailedOneIsHealthy: ADR 0014 retries
// on another replica whenever the pool has one; health tracking only
// orders the candidates. With r1 ejected and r0 failing to connect, the
// retry goes to r1, not back to r0 after the backoff.
func TestRetryGoesToEjectedReplicaWhenOnlyFailedOneIsHealthy(t *testing.T) {
	p := routingPolicy{name: "affinity_ring", affinity: true, ring: true}
	live := startMock(t, nil)
	g := newLeaseGatewayWith(t, p, []string{deadURL(t), live.URL()}, chatBody, leaseOptions{
		backoff: func(context.Context, time.Duration) bool {
			t.Error("same-replica backoff used although the pool has another replica")
			return false
		},
	})
	g.tracker.Eject("r1", health.ReasonStuckEngine)
	resp, got, _ := g.do(t, chatBody, g.headers(nil))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 from the retry on r1 (%s)", resp.StatusCode, got)
	}
	g.waitHandled(t, 1)
	if s := g.outcomes(t); s.ConnectRetries != 1 || live.Mock().Stats().Requests != 1 {
		t.Fatalf("stats = %+v, r1 requests = %d; want 1 retry served by r1", s, live.Mock().Stats().Requests)
	}
	if got := g.counter.reported(); !slices.Equal(got, []string{"r0"}) {
		t.Fatalf("reported = %v, want [r0]", got)
	}
	g.assertReleased(t, 2)
}
