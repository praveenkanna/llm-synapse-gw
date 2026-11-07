//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/mockllm"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates/pgstore"
)

// AC-M3-4: with Postgres unreachable the gateway keeps serving from the
// last registry snapshot, with no gateway-originated errors, and requests
// naming unknown templates still go through unchanged (ADR 0006).
//
// Runs the whole gateway in process against a real Postgres (env
// INTEGRATION_POSTGRES_DSN) reached through a TCP proxy the test cuts.

const integrationSpec = `{"model":"mock-model","variables":[{"name":"q"}],
 "messages":[{"role":"system","segments":[{"kind":"static","text":"You are the AC-M3-4 assistant. Be brief."}]},
             {"role":"user","segments":[{"kind":"volatile","text":"{{q}}"}]}]}`

const integrationBody = `{"model":"mock-model","max_tokens":2,"messages":[{"role":"system","content":"You are the AC-M3-4 assistant. Be brief."},{"role":"user","content":"hello"}]}`

// tcpProxy forwards connections to target until cut, which closes the
// listener's open connections and refuses new ones until restored.
type tcpProxy struct {
	ln     net.Listener
	target string
	mu     sync.Mutex
	down   bool
	conns  map[net.Conn]struct{}
}

func newTCPProxy(t *testing.T, target string) *tcpProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &tcpProxy{ln: ln, target: target, conns: map[net.Conn]struct{}{}}
	go p.serve()
	t.Cleanup(func() { _ = ln.Close(); p.cut() })
	return p
}

func (p *tcpProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		if p.down {
			p.mu.Unlock()
			_ = c.Close()
			continue
		}
		up, err := net.DialTimeout("tcp", p.target, 2*time.Second)
		if err != nil {
			p.mu.Unlock()
			_ = c.Close()
			continue
		}
		p.conns[c], p.conns[up] = struct{}{}, struct{}{}
		p.mu.Unlock()
		go func() { _, _ = io.Copy(up, c); _ = up.Close() }()
		go func() { _, _ = io.Copy(c, up); _ = c.Close() }()
	}
}

func (p *tcpProxy) cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.down = true
	for c := range p.conns {
		_ = c.Close()
	}
	clear(p.conns)
}

func (p *tcpProxy) restore() {
	p.mu.Lock()
	p.down = false
	p.mu.Unlock()
}

func TestRegistryServesLastSnapshotWhilePostgresDown(t *testing.T) {
	dsn := os.Getenv("INTEGRATION_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("INTEGRATION_POSTGRES_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := fmt.Sprintf("t_acm34_%d", rand.Uint32())
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		_ = admin.Close(context.Background())
	})

	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	proxy := newTCPProxy(t, u.Host)
	gwDSN := *u
	gwDSN.Host = proxy.ln.Addr().String()
	q := gwDSN.Query()
	q.Set("search_path", schema)
	gwDSN.RawQuery = q.Encode()

	mcfg := mockllm.Defaults()
	mcfg.ListenAddr = anyPort
	mcfg.TTFT, mcfg.TokensPerSecond = 0, 0
	mock, err := mockllm.Start(mcfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mock.Close() }()

	free := func() string {
		l, err := net.Listen("tcp", anyPort)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = l.Close() }()
		return l.Addr().String()
	}
	addr, metricsAddr := free(), free()
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	yaml := "pools:\n  - name: large\n    max_tokens_ceiling: 64\n    replicas:\n      - url: " + mock.URL() +
		"\nauth:\n  enabled: false\nadmin:\n  listen_addr: \"\"\ntemplates:\n  refresh_interval: 200ms\n  postgres:\n    query_timeout: 150ms\n    connect_timeout: 150ms\n"
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{
		config.EnvConfigPath: path, "GATEWAY_LISTEN_ADDR": addr, envMetricsAddr: metricsAddr,
		"GATEWAY_TEMPLATES_POSTGRES_DSN": gwDSN.String(),
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- run(runCtx, func(k string) string { return env[k] }, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	defer func() {
		stop()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("run: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("run did not return")
		}
	}()

	// The gateway migrates the schema in the background; register and
	// activate a version straight in the store once it has.
	store, err := pgstore.Open(ctx, dsn, pgstore.Options{SearchPath: schema})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	eventually(t, 20*time.Second, "schema migrated", func() bool { _, err := store.Stamp(ctx); return err == nil })
	rec, err := store.CreateVersion(ctx, "acm34", "", json.RawMessage(integrationSpec), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SetStatus(ctx, "acm34", rec.Version, templates.StatusActive); err != nil {
		t.Fatal(err)
	}
	metrics := func() string {
		resp, err := http.Get("http://" + metricsAddr + "/metrics")
		if err != nil {
			return ""
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		return string(b)
	}
	eventually(t, 10*time.Second, "snapshot loaded", func() bool {
		return strings.Contains(metrics(), "gateway_template_snapshot_versions 1")
	})

	send := func(id string) int {
		req, _ := http.NewRequest(http.MethodPost, "http://"+addr+"/v1/chat/completions", strings.NewReader(integrationBody))
		req.Header.Set(templates.HeaderTemplateID, id)
		req.Header.Set(templates.HeaderTemplateVersion, strconv.Itoa(rec.Version))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	if s := send("acm34"); s != http.StatusOK {
		t.Fatalf("before outage: status %d", s)
	}

	// Only refresh failures after the cut count as the outage: the migration
	// race at startup can fail a round too.
	failedBefore := registryFailures(metrics())
	proxy.cut()
	eventually(t, 10*time.Second, "a refresh round failing after the cut", func() bool {
		return registryFailures(metrics()) > failedBefore
	})

	// Spread the requests over several refresh intervals (200 ms), so they
	// overlap failed rounds and not only the first one.
	const n = 50
	for i := range n {
		id := "acm34"
		if i%5 == 0 {
			id = "never-registered"
		}
		if s := send(id); s != http.StatusOK {
			t.Fatalf("during outage request %d (%s): status %d, want 200", i, id, s)
		}
		time.Sleep(25 * time.Millisecond)
	}
	if failed := registryFailures(metrics()) - failedBefore; failed < 3 {
		t.Fatalf("only %d refresh rounds failed during the outage; want the requests to overlap at least 3", failed)
	}

	// A request's counters are recorded as it finishes, which can be just
	// after the client has read the response: poll instead of reading once.
	want := fmt.Sprintf(`gateway_template_requests_total{template="acm34",version="%d",status="200"} %d`, rec.Version, 1+n-n/5)
	eventually(t, 5*time.Second, "matched requests counted: "+want, func() bool {
		return strings.Contains(metrics(), want)
	})
	eventually(t, 5*time.Second, "unknown template IDs failing open, counted", func() bool {
		return strings.Contains(metrics(), `gateway_failopen_total{stage="compile",reason="unknown_template"} 10`)
	})
	if !strings.Contains(metrics(), "gateway_template_snapshot_versions 1") {
		t.Fatal("snapshot lost during the outage")
	}

	// Back up: a newly activated version reaches the snapshot.
	proxy.restore()
	rec2, err := store.CreateVersion(ctx, "acm34", "", json.RawMessage(integrationSpec), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SetStatus(ctx, "acm34", rec2.Version, templates.StatusActive); err != nil {
		t.Fatal(err)
	}
	eventually(t, 15*time.Second, "snapshot refreshed after recovery", func() bool {
		return strings.Contains(metrics(), "gateway_template_snapshot_versions 2")
	})
}

// registryFailures sums the template_registry fail-opens in a /metrics body.
func registryFailures(metrics string) int {
	var n int
	for line := range strings.SplitSeq(metrics, "\n") {
		if !strings.HasPrefix(line, `gateway_failopen_total{stage="template_registry"`) {
			continue
		}
		f := strings.Fields(line)
		if v, err := strconv.Atoi(f[len(f)-1]); err == nil {
			n += v
		}
	}
	return n
}

func eventually(t *testing.T, within time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for: %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
