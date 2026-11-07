package discovery

import (
	"context"
	"log/slog"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
)

// DefaultWatchInterval is how often Watch re-reads the configuration. The
// file is a few KiB, so polling is cheap and also follows the symlink swaps
// that mounted config maps use (which file-change notifications miss). A
// change reaches routing at most this long after the file is written:
// Static.Update signals Changed and the health tracker rebuilds rings at once.
const DefaultWatchInterval = 2 * time.Second

// LoadFunc returns a freshly loaded and validated configuration, typically
// func() (*config.Config, error) { return config.Load(os.Getenv) }.
type LoadFunc func() (*config.Config, error)

// Watch polls load every interval and applies a changed replica list to s.
// A load or update error keeps the current set (fail open) and is logged
// once per distinct error. Watch returns when ctx is done; the caller owns
// the goroutine it runs on.
func Watch(ctx context.Context, s *Static, load LoadFunc, interval time.Duration, logger *slog.Logger) {
	if interval <= 0 {
		interval = DefaultWatchInterval
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	var lastErr string
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		changed, err := Reload(s, load)
		switch {
		case err != nil:
			if msg := err.Error(); msg != lastErr {
				logger.Warn("replica reload failed; keeping current replicas", "err", err)
				lastErr = msg
			}
		case changed:
			lastErr = ""
			logger.Info("replica set reloaded", "pools", s.Pools())
			s.WarnOversized(logger)
		default:
			lastErr = ""
		}
	}
}

// Reload runs load once and applies its pools to s if they differ from the
// current set. It reports whether the set changed.
func Reload(s *Static, load LoadFunc) (bool, error) {
	cfg, err := load()
	if err != nil {
		return false, err
	}
	if s.equal(cfg.Pools) {
		return false, nil
	}
	if err := s.Update(cfg.Pools); err != nil {
		return false, err
	}
	return true, nil
}
