// Command mockllm is an OpenAI-wire-compatible mock of a vLLM replica for
// GPU-less development, CI and load-harness runs against the gateway
// (ADR 0010). The Compose dev profile runs it in place of vLLM.
//
// Configuration is read from the environment (defaults in brackets):
//
//	MOCKLLM_LISTEN_ADDR        listen address [:8000]
//	MOCKLLM_MODELS             comma-separated served model names [mock-model]
//	MOCKLLM_MAX_MODEL_LEN      context length reported and enforced [8192]
//	MOCKLLM_TTFT               time to first token, Go duration [50ms]
//	MOCKLLM_TOKENS_PER_SECOND  decode rate per request, <= 0 for no delay [50]
//	MOCKLLM_OUTPUT_TOKENS      fixed:N | uniform:MIN:MAX | normal:MEAN:STDDEV [uniform:16:128]
//	MOCKLLM_MAX_NUM_SEQS       concurrent running requests, 0 = unlimited [0]
//	MOCKLLM_FAULT_5XX_RATE     probability of a 5xx before any token [0]
//	MOCKLLM_FAULT_DROP_RATE    probability of cutting the connection mid-response [0]
//	MOCKLLM_FAULT_HANG_RATE    probability of never answering [0]
//	MOCKLLM_FAULT_STALL_RATE   probability of stopping mid-stream without closing [0]
//	MOCKLLM_ERROR_STATUS       status used by the 5xx fault [500]
//	MOCKLLM_CAPTURE_ENTRIES    received requests kept for /mock/requests, 0 = off [64]
//	MOCKLLM_CAPTURE_MAX_BYTES  body bytes kept per captured request [262144]
//	MOCKLLM_MAX_BODY_BYTES     request body limit, larger gets 413 [8388608]
//	MOCKLLM_CONTROL_ENABLED    serve the /mock/ control endpoints [true]
//	MOCKLLM_SEED               sampling seed, 0 = random [0]
//	MOCKLLM_VLLM_COMPAT        vLLM release family to imitate (ADR 0011) [current]:
//	                           current = {"error":{...}} errors, vllm:kv_cache_usage_perc,
//	                           vllm:prefix_cache_{queries,hits}_total;
//	                           legacy = {"object":"error",...} errors, vllm:gpu_cache_usage_perc,
//	                           vllm:gpu_prefix_cache_{queries,hits}_total
//	MOCKLLM_PREFIX_CACHE_BLOCKS  per-model prefix-cache capacity in 16-token blocks, 0 = off [32768]
//
// Connection refusal is injected with POST /mock/refuse?for=DURATION. Per
// request, the X-Mock-Fault, X-Mock-Output-Tokens and X-Mock-TTFT-Ms headers
// override the configuration. See package internal/mockllm.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/mockllm"
)

// shutdownTimeout bounds how long open requests get after SIGTERM before the
// remaining ones (hung requests included) are cut.
const shutdownTimeout = 5 * time.Second

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Getenv, logger); err != nil {
		logger.Error("mockllm stopped with error", "err", err)
		os.Exit(1)
	}
}

// run serves until ctx is cancelled or the server fails.
func run(ctx context.Context, getenv func(string) string, logger *slog.Logger) error {
	cfg, err := mockllm.FromEnv(getenv)
	if err != nil {
		return err
	}
	srv, err := mockllm.Start(cfg, logger)
	if err != nil {
		return fmt.Errorf("start: %w", err)
	}
	logger.Info("mockllm listening",
		"addr", srv.Addr(),
		"models", cfg.Models,
		"ttft", cfg.TTFT.String(),
		"tokens_per_second", cfg.TokensPerSecond,
		"output_tokens", cfg.OutputTokens.String(),
		"max_num_seqs", cfg.MaxNumSeqs,
		"prefix_cache_blocks", cfg.PrefixCacheBlocks,
	)

	select {
	case <-ctx.Done():
	case <-srv.Done():
		return srv.Close() // Serve stopped on its own; report why
	}
	logger.Info("shutdown signal received, draining")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		if !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("shutdown: %w", err)
		}
		// Expected with the hang/stall faults: those requests never finish on their own.
		logger.Warn("drain timed out, open requests were cut", "timeout", shutdownTimeout.String())
	}
	logger.Info("mockllm stopped")
	return nil
}
