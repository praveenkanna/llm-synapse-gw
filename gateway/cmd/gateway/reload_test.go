package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
)

// namedReplica answers every request with its own name in the body, and
// /health with 200, so a test can see which replica served a request.
func namedReplica(t *testing.T, name string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/health":
			w.WriteHeader(http.StatusOK)
		case strings.HasPrefix(r.URL.Path, "/v1/completions"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"served_by":"`+name+`"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// writeReplicas atomically replaces the config file with pool "large" made
// of the given name=url replicas.
func writeReplicas(t *testing.T, path string, reps ...[2]string) {
	t.Helper()
	var b strings.Builder
	b.WriteString("pools:\n  - name: large\n    replicas:\n")
	for _, r := range reps {
		fmt.Fprintf(&b, "      - name: %s\n        url: %s\n", r[0], r[1])
	}
	b.WriteString("auth:\n  enabled: false\n")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

// startReloadGateway runs the gateway on the config file at path with the
// given extra environment and returns its address. It stops at test end.
func startReloadGateway(t *testing.T, path string, extra map[string]string) string {
	t.Helper()
	probe, err := net.Listen("tcp", anyPort)
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	_ = probe.Close()
	env := map[string]string{config.EnvConfigPath: path, "GATEWAY_LISTEN_ADDR": addr, envMetricsAddr: anyPort}
	for k, v := range extra {
		env[k] = v
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, func(k string) string { return env[k] }, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("run returned error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("run did not return after context cancel")
		}
	})
	return addr
}

var reloadClient = &http.Client{Timeout: 2 * time.Second}

// servedBy sends one keyed completion through the gateway at addr and
// returns the name of the replica that answered it.
func servedBy(addr, session string) (string, error) {
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/v1/completions",
		strings.NewReader(`{"model":"m","prompt":"hi","max_tokens":1}`))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Session-Id", session)
	resp, err := reloadClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status %d: %s", resp.StatusCode, body)
	}
	for _, n := range []string{"A", "B", "C", "D"} {
		if strings.Contains(string(body), `"served_by":"`+n+`"`) {
			return n, nil
		}
	}
	return "", fmt.Errorf("unexpected body %s", body)
}

// waitServed sends session until accept takes the replica that served it,
// failing after limit; it returns how long that took.
func waitServed(t *testing.T, addr, session, what string, limit time.Duration, accept func(string) bool) time.Duration {
	t.Helper()
	start := time.Now()
	deadline := start.Add(limit)
	for {
		got, err := servedBy(addr, session)
		if err == nil && accept(got) {
			return time.Since(start)
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: session %s last served by %q (err %v) after %v", what, session, got, err, limit)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// sessionOwners sends n sessions and returns which replica served each, plus
// the first session served by each replica.
func sessionOwners(t *testing.T, addr string, n int) (map[string]string, map[string]string) {
	t.Helper()
	owner := make(map[string]string, n)
	first := map[string]string{}
	for i := range n {
		s := fmt.Sprintf("sess-%d", i)
		got, err := servedBy(addr, s)
		if err != nil {
			t.Fatal(err)
		}
		owner[s] = got
		if _, ok := first[got]; !ok {
			first[got] = s
		}
	}
	return owner, first
}

// TestRunRoutingFollowsConfigReload drives a real config-file reload
// through run(): keyed (X-Session-Id) traffic on a removed replica moves to
// the remaining ones and never returns, and a replica re-pointed to a new
// URL is reached at the new URL.
func TestRunRoutingFollowsConfigReload(t *testing.T) {
	srvA, srvB, srvC, srvD := namedReplica(t, "A"), namedReplica(t, "B"), namedReplica(t, "C"), namedReplica(t, "D")
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	writeReplicas(t, path, [2]string{"a", srvA.URL}, [2]string{"b", srvB.URL}, [2]string{"c", srvC.URL})
	addr := startReloadGateway(t, path, map[string]string{
		"GATEWAY_DISCOVERY_RELOAD_INTERVAL":   "100ms",
		"GATEWAY_ROUTER_HEALTH_POLL_INTERVAL": "100ms",
		"GATEWAY_ROUTER_HEALTH_POLL_TIMEOUT":  "100ms",
	})
	waitServed(t, addr, "warm-up", "gateway never served", 10*time.Second, func(string) bool { return true })

	owner, first := sessionOwners(t, addr, 120)
	onB, onC := first["B"], first["C"]
	if onB == "" || onC == "" {
		t.Fatalf("sessions did not reach both B and C: %v", owner)
	}

	// Reload removes b.
	writeReplicas(t, path, [2]string{"a", srvA.URL}, [2]string{"c", srvC.URL})
	waitServed(t, addr, onB, "removed replica still keyed", 10*time.Second, func(got string) bool { return got != "B" })
	for s := range owner {
		if got, err := servedBy(addr, s); err != nil || got == "B" {
			t.Fatalf("session %s after removing b: served by %q, err %v", s, got, err)
		}
	}

	// Reload re-points c from C to D.
	writeReplicas(t, path, [2]string{"a", srvA.URL}, [2]string{"c", srvD.URL})
	waitServed(t, addr, onC, "re-pointed replica not reached at its new URL", 10*time.Second, func(got string) bool { return got == "D" })
	for s := range owner {
		if got, err := servedBy(addr, s); err != nil || got == "B" || got == "C" {
			t.Fatalf("session %s after re-pointing c: served by %q, err %v", s, got, err)
		}
	}
}

// TestRunRemovalRebalancesWithinDefaultReloadInterval proves AC-M1-4's
// timing with the shipped defaults (reload interval, health poll): after
// the config file drops a replica, keyed traffic leaves it within the
// default reload interval plus a small margin, well inside the 5 s target.
// The ring rebuild itself waits for no health tick (proven with an hour-long
// tick in router/health's TestReloadRebuildsRingWithoutWaitingForTick).
func TestRunRemovalRebalancesWithinDefaultReloadInterval(t *testing.T) {
	const ac4Target = 5 * time.Second
	const margin = time.Second
	limit := config.DefaultReloadInterval + margin
	if limit > ac4Target {
		t.Fatalf("default reload interval %v + %v exceeds AC-M1-4's %v", config.DefaultReloadInterval, margin, ac4Target)
	}
	srvA, srvB := namedReplica(t, "A"), namedReplica(t, "B")
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	writeReplicas(t, path, [2]string{"a", srvA.URL}, [2]string{"b", srvB.URL})
	addr := startReloadGateway(t, path, nil) // defaults throughout
	waitServed(t, addr, "warm-up", "gateway never served", 10*time.Second, func(string) bool { return true })
	_, first := sessionOwners(t, addr, 60)
	onB := first["B"]
	if onB == "" {
		t.Fatal("no session reached B")
	}

	writeReplicas(t, path, [2]string{"a", srvA.URL})
	took := waitServed(t, addr, onB, "removed replica still keyed", limit, func(got string) bool { return got != "B" })
	t.Logf("removal reached routing %v after the file edit (reload interval %v)", took, config.DefaultReloadInterval)
}
