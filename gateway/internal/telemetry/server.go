package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// MetricsPath is where the internal metrics listener serves Prometheus text.
const MetricsPath = "/metrics"

const (
	// metricsReadHeaderTimeout bounds slow scrapers on the internal port.
	metricsReadHeaderTimeout = 5 * time.Second
	// metricsShutdownTimeout bounds the metrics server's shutdown; a scrape
	// renders in well under a millisecond.
	metricsShutdownTimeout = 2 * time.Second
)

// ServeMetrics serves m at MetricsPath on ln until ctx is cancelled. It is
// separate from the request path so metrics never share its port. It returns
// nil after a clean shutdown.
func ServeMetrics(ctx context.Context, ln net.Listener, m *Metrics, logger *slog.Logger) error {
	mux := http.NewServeMux()
	mux.Handle("GET "+MetricsPath, m)
	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: metricsReadHeaderTimeout,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()
	logger.Info("metrics listening", "addr", ln.Addr().String())

	select {
	case err := <-errCh:
		return fmt.Errorf("metrics serve %s: %w", ln.Addr(), err)
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), metricsShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		_ = srv.Close()
		<-errCh
		return fmt.Errorf("metrics shutdown: %w", err)
	}
	if err := <-errCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("metrics serve %s: %w", ln.Addr(), err)
	}
	return nil
}
