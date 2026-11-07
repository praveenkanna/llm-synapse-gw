# 0022 — Packages consume their config section

- Status: accepted; amended 2026-11-01 (AD-07 and AD-10 did change behaviour)
- Date: 2026-10-28
- Milestone: track AD (AD-06; architecture deepening, [plan](../plan/architecture-deepening.md), decisions C1–C3)
- Brief sections: none (how settings are owned in code; no knob, default or target changes)
- Builds on: [ADR 0005](0005-synthetic-workload-harness.md) (every capacity number is a calibrated knob)

## Context

Every tunable comes from `internal/config` (`.claude/rules/go-code.md`). But several packages also keep a `Config` of their own that mirrors config's section, with defaults and bounds of its own. `cmd/gateway` copies one into the other field by field. One knob, `router.health.stuck_engine_max_waits`, shows the cost:

- Its default (65536) is written in `config.DefaultHealthStuckEngineMaxWaits` and again in `health.DefaultConfig()`.
- Its bounds are checked twice, by different rules: config rejects anything outside [1, 2^20], while `health.Config.withDefaults` turns any value ≤ 0 into the default.
- The value travels through `cmd/gateway/main.go`'s `healthConfig`, which also renames fields (`StuckEngineMaxWaits` becomes `MaxFirstChunkWaits`, `FailureThreshold` becomes `ActiveFailureThreshold`).
- Drift tests (`TestConfigDefaultsMatchPackages`, `TestRouterConfigDefaultsMatchPackages`) exist only to catch the two copies disagreeing.

The same pattern covers the load view (`loadViewConfig`, `loadview.DefaultConfig`), admission (`admissionConfig`), coalescing (`newCoalesceGroup`) and the response cache (`newResponseCache`). Adding or changing one knob touches about six production files.

The mirrors were kept to avoid an import cycle. That only rules out config importing a package. Packages importing config is already common: upstream, discovery, dispatch, ingress, compiler, auth, admin, and loadview (for `config.Secret`).

## Options

1. **Keep the mirrors, and test that they agree.** No code moves. But every knob still has two defaults and two bounds checks, and a third copy is only one edit away. The drift tests catch a different default, not a different bounds rule.
2. **Packages take their config section; config owns each setting end to end (chosen).** One default, one bounds check and one document per knob. A package reads a validated section and keeps nothing of its own. The cost: the package depends on `internal/config`, and its tests build settings from `config.Defaults()` rather than from a local default.
3. **Generate the mirrors from config.** One source, but it adds a generator and a build step for what is plain struct passing. Field renames would still need a hand-written map.

## Decision

`internal/config` owns every setting end to end: its YAML path, its env name, its one default, its bounds, the rules that span sections, and its documentation in `deploy/compose/config/gateway.example.yaml`. It also owns the facts derived only from settings, such as pool names, the priority-scheduling pool set and the probe-model rule (AD-07). A package takes its validated config section (for example `config.RouterHealth`) and keeps no defaults, bounds or field-by-field copy of its own.

- **C1.** Packages consume their config section directly. A package-level `DefaultConfig`, `withDefaults` or mirrored struct for a config section is not added again. `cmd/gateway` passes sections; it does not translate them.
- **C2.** Env names stay hand-listed in `config/env.go`. They are not mechanical (`GATEWAY_LISTEN_ADDR` is `server.listen_addr`). The env-count and example-YAML tests stay.
- **C3.** Dependencies that are not knobs stay constructor arguments: the clock, the logger, the metrics, the stuck-probe sender, and the out-of-order pool set. Config holds settings, not collaborators.

## Consequences

- AD-07 to AD-10 apply this to pool facts, router health, the load view, admission, coalescing, the response cache and the priority prober. Each deletes a mapping function in `cmd/gateway` and the drift test that guarded it. A knob's default and bounds then exist once, checked by config's own tests.
- No existing knob, default, bound or env name changes. The example config and the env-count test show that. See the amendment for what was added and for one metric change.
- A package that needs a setting config does not have yet adds it to config (path, env, default, bounds, example YAML) instead of keeping a local constant.
- Tests of a package build their settings from `config.Defaults()` and change only the fields they exercise. A package test can no longer pass with settings config would reject.
- Revisit if a package must be used outside the gateway binary, for example as a library with its own defaults. That package would then get its own option type, built from the config section at one seam.

## Amendment 2026-11-01 (the track's final review)

Two things in the track went past "no knob or metric changes":

- AD-10 added `priority.probe.interval` and `priority.probe.timeout` (`GATEWAY_PRIORITY_PROBE_INTERVAL`, `GATEWAY_PRIORITY_PROBE_TIMEOUT`). The prober used to follow the load view's interval and the stuck probe's timeout. The defaults match what it ran at, so only an operator who changed those two settings sees a difference.
- AD-07's one probe-model rule changes what a stuck replica with no served model does: it gets a generation probe with the pool's mapped model, where it used to come back on `/health` alone. `gateway_stuck_probe_total` and `gateway_failopen_total` values change with it.
