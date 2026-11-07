package telemetry

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestServeMetrics(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	m := NewMetrics()
	m.clientDisconnects.with().inc()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ServeMetrics(ctx, ln, m, slog.New(slog.DiscardHandler)) }()

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + ln.Addr().String() + MetricsPath)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), MetricClientDisconnects+" 1\n") {
		t.Errorf("status %d body:\n%s", resp.StatusCode, body)
	}
	if resp, err := client.Post("http://"+ln.Addr().String()+MetricsPath, "text/plain", nil); err == nil {
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("POST /metrics status = %d", resp.StatusCode)
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ServeMetrics = %v, want nil after cancel", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ServeMetrics did not return after cancel")
	}
}

func TestServeMetricsClosedListener(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_ = ln.Close()
	err = ServeMetrics(context.Background(), ln, NewMetrics(), slog.New(slog.DiscardHandler))
	if err == nil {
		t.Fatal("ServeMetrics on a closed listener returned nil")
	}
}
