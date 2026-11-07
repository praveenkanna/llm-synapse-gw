package arbitrator

import "github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"

// ConfigFrom returns the arbitrator's configuration from the validated
// admission and priority sections, with one class per priority class,
// highest first, each with its resolved bounds. Config owns the defaults,
// bounds and per-class overrides (ADR 0022).
func ConfigFrom(cfg *config.Config) Config {
	a := cfg.Admission
	classes := make([]Class, 0, len(cfg.Priority.Classes))
	for _, name := range cfg.Priority.Classes {
		classes = append(classes, Class{
			Name:     name,
			MaxWait:  a.MaxWaitFor(name),
			MaxQueue: a.MaxQueueFor(name),
			MaxShare: a.MaxShareFor(name),
		})
	}
	return Config{
		Classes:       classes,
		RetryAfterMax: a.RetryAfterMax,
		EarlyReject:   a.EarlyReject,
		Shed: ShedConfig{
			Enabled:      a.Shed.Enabled,
			Threshold:    a.Shed.TTFTP95Threshold,
			Window:       a.Shed.Window,
			MinSamples:   a.Shed.MinSamples,
			RecoverRatio: a.Shed.RecoverRatio,
			Classes:      cfg.ShedClasses(),
		},
	}
}
