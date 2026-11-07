package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
)

// lockedBuffer collects log output from run's goroutines.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// A pool past the size AC-M1-5 is proven for starts, with one warning that
// names it (M1-26).
func TestRunWarnsAboutAnOversizedPool(t *testing.T) {
	var yaml strings.Builder
	yaml.WriteString("pools:\n  - name: large\n    replicas:\n")
	for i := range config.MaxPoolReplicas + 1 {
		fmt.Fprintf(&yaml, "      - url: http://127.0.0.1:%d\n", 10000+i)
	}
	yaml.WriteString("auth:\n  enabled: false\n")
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	if err := os.WriteFile(path, []byte(yaml.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{config.EnvConfigPath: path, "GATEWAY_LISTEN_ADDR": anyPort, envMetricsAddr: anyPort}

	var logs lockedBuffer
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, func(k string) string { return env[k] }, slog.New(slog.NewTextHandler(&logs, nil)))
	}()
	const warning = "pool is larger than the route latency target is proven for"
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(logs.String(), warning) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return after context cancel")
	}
	out := logs.String()
	if n := strings.Count(out, warning); n != 1 {
		t.Fatalf("%d warnings, want 1:\n%s", n, out)
	}
	if !strings.Contains(out, "pool=large") || !strings.Contains(out, "replicas=65") {
		t.Fatalf("the warning does not name the pool and its size:\n%s", out)
	}
}
