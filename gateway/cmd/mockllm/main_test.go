package main

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/mockllm"
)

func TestRunRejectsInvalidConfig(t *testing.T) {
	env := map[string]string{mockllm.EnvTTFT: "soon"}
	err := run(context.Background(), func(k string) string { return env[k] }, slog.New(slog.DiscardHandler))
	if err == nil || !strings.Contains(err.Error(), mockllm.EnvTTFT) {
		t.Fatalf("run = %v, want config error naming %s", err, mockllm.EnvTTFT)
	}
}

func TestRunStartsAndStopsOnCancel(t *testing.T) {
	env := map[string]string{mockllm.EnvListenAddr: "127.0.0.1:0"}
	var logs strings.Builder
	logger := slog.New(slog.NewJSONHandler(&logs, nil)) // slog serialises writes; logs is read after run returns
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, func(k string) string { return env[k] }, logger) }()

	time.Sleep(50 * time.Millisecond) // let it start listening
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run = %v, want nil after cancel", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after cancel")
	}
	if !strings.Contains(logs.String(), "mockllm listening") || !strings.Contains(logs.String(), "mockllm stopped") {
		t.Fatalf("logs = %s", logs.String())
	}
}

func TestRunFailsOnBusyAddress(t *testing.T) {
	srv, err := mockllm.Start(func() mockllm.Config {
		c := mockllm.Defaults()
		c.ListenAddr = "127.0.0.1:0"
		return c
	}(), nil)
	if err != nil {
		t.Fatalf("start blocker: %v", err)
	}
	defer srv.Close()
	env := map[string]string{mockllm.EnvListenAddr: srv.Addr()}
	if err := run(context.Background(), func(k string) string { return env[k] }, slog.New(slog.DiscardHandler)); err == nil {
		t.Fatal("run on a busy address: want error")
	}
}
