package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/admin"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

const adminShutdownTimeout = 10 * time.Second

// startAdmin serves the template admin API on admin.listen_addr when it is
// enabled (listen address, a token and a Postgres DSN). It binds before it
// returns, so a port conflict stops startup, and the returned stop drains
// the listener; call it before closing the store. The admin port is never
// behind the public route, and nothing on the request path waits on it.
func startAdmin(cfg *config.Config, tmpl *templateRegistry, disc discovery.Discovery, client *upstream.Client, logger *slog.Logger) (stop func(), err error) {
	if !cfg.AdminEnabled() || tmpl.store == nil {
		return func() {}, nil
	}
	api, err := admin.New(tmpl.store, admin.Options{
		Tokens:          cfg.Admin.Tokens,
		TokenizeTimeout: cfg.Admin.TokenizeTimeout,
		Discovery:       disc,
		Sender:          client,
		PoolFor:         cfg.PoolForModel,
		Kick:            tmpl.registry.Kick,
		Logger:          logger.With("component", "admin"),
	})
	if err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", cfg.Admin.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", cfg.Admin.ListenAddr, err)
	}
	// No WriteTimeout: registering a template with many examples makes one
	// /tokenize call per example, each bounded by admin.tokenize_timeout.
	srv := &http.Server{
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("admin server stopped", "err", err)
		}
	}()
	logger.Info("template admin API listening", "addr", ln.Addr().String())
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), adminShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			_ = srv.Close()
		}
		<-done
	}, nil
}
