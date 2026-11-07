// Package health owns the gateway's lifecycle endpoints and graceful drain.
//
//   - /livez reports that the process is up. It is deliberately shallow: it
//     never depends on Redis, Postgres or replicas, and stays 200 while
//     draining so an orchestrator does not kill a draining process early.
//   - /readyz reports whether this gateway should receive new traffic. It turns
//     false (503) as soon as drain begins.
//   - Serve runs an http.Server until its context is cancelled, then drains:
//     readiness goes false, the listener closes (no new connections), open
//     requests and SSE streams get up to the drain timeout to finish, and
//     anything still open after that is closed.
package health

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

// Endpoint paths.
const (
	LivenessPath  = "/livez"
	ReadinessPath = "/readyz"
)

// Checker holds the process's readiness state. The zero value is not usable;
// call New.
type Checker struct {
	draining atomic.Bool
}

// New returns a Checker that reports ready until SetDraining is called.
func New() *Checker {
	return &Checker{}
}

// SetDraining marks the process as draining; readiness is false from now on.
// It is safe to call more than once and from any goroutine.
func (c *Checker) SetDraining() {
	c.draining.Store(true)
}

// Draining reports whether drain has begun.
func (c *Checker) Draining() bool {
	return c.draining.Load()
}

// Register adds GET (and implicitly HEAD) /livez and /readyz to mux.
func (c *Checker) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET "+LivenessPath, c.handleLiveness)
	mux.HandleFunc("GET "+ReadinessPath, c.handleReadiness)
}

// handleLiveness reports that the process is up, draining or not.
func (c *Checker) handleLiveness(w http.ResponseWriter, _ *http.Request) {
	writePlain(w, http.StatusOK, "ok\n")
}

// handleReadiness reports 200 while accepting traffic and 503 while draining.
func (c *Checker) handleReadiness(w http.ResponseWriter, _ *http.Request) {
	if c.Draining() {
		writePlain(w, http.StatusServiceUnavailable, "draining\n")
		return
	}
	writePlain(w, http.StatusOK, "ready\n")
}

func writePlain(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}

// Serve runs srv on ln until ctx is cancelled or the server fails, then drains.
//
// Drain order: readiness false, listener closed and idle keep-alive
// connections closed (srv.Shutdown), then wait up to drainTimeout for active
// requests and streams to finish. If the timeout expires, the remaining
// connections are closed (srv.Close), which cancels their request contexts
// and so the upstream requests behind them. Serve returns nil after a drain,
// whether or not it timed out; the timeout case is logged.
//
// srv must not set WriteTimeout: SSE streams may legitimately run for minutes.
func Serve(ctx context.Context, srv *http.Server, ln net.Listener, c *Checker, drainTimeout, preDrainDelay time.Duration, logger *slog.Logger) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(ln)
	}()
	logger.Info("gateway listening", "addr", ln.Addr().String())

	select {
	case err := <-errCh:
		c.SetDraining()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve %s: %w", ln.Addr(), err)
	case <-ctx.Done():
	}

	c.SetDraining()
	logger.Info("shutdown signal received, draining", "drain_timeout", drainTimeout.String(), "pre_drain_delay", preDrainDelay.String())
	start := time.Now()

	if preDrainDelay > 0 {
		select {
		case <-time.After(preDrainDelay):
		case err := <-errCh:
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return fmt.Errorf("serve %s: %w", ln.Addr(), err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		// Only the context can fail Shutdown here: the drain timed out.
		logger.Warn("drain timeout exceeded, closing remaining connections",
			"drain_timeout", drainTimeout.String(), "err", err.Error())
		if cerr := srv.Close(); cerr != nil {
			logger.Warn("closing remaining connections", "err", cerr.Error())
		}
	}
	if err := <-errCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve %s: %w", ln.Addr(), err)
	}
	logger.Info("gateway stopped", "drain_duration", time.Since(start).String())
	return nil
}
