# 0017 — Prove AC-M1-5 by a CPU-portable cost ratio in CI and the absolute p99 on reference hardware

- Status: accepted; amended by 0016 (the ratio gate becomes blocking after three clean sessions on GitHub-hosted runners) and by the M1-28 amendment below, which awaits the owner's review
- Date: 2026-10-18
- Milestone: M1 (post-ship hardening, task M1-24)
- Brief sections: none (how a latency criterion is evidenced; the target does not change)

## Context

AC-M1-5 says the route decision costs at most 100 µs p99 (benchmark). The key scan that bounds that cost prices every step of its work in units (`gateway/internal/router/affinity/scan.go`), and the prices were fitted to one CPU, the AMD EPYC 7763 on the GitHub-hosted runners. While the Actions minutes were used up, the advisory bench ran on shared CI pods instead: Intel Xeon Cascade Lake, 16 vCPU under KVM, shared with other builds.

The first bench run there (its second build) showed that the EPYC-based evidence does not carry over:

| Measure | EPYC runners | CI pod |
| --- | --- | --- |
| Extract runs over 100 µs p99 | 0 of 1,180 | 26 of 165 (11 shapes) |
| Whole-route runs over 100 µs p99 | 0 | 30 of 130 (11 shapes) |
| `TestExtractP99` | pass | fail (`escaped_keys`, 100.8 µs) |
| Cost per unit against the calibrated shape | at most 1.20× | 4 shapes above 1.15×; sweep points up to 1.75× |
| `empty_arrays` and `empty_objects_in_content`, Route against Extract (p50) | equal | 2.4× (147 µs against 61 µs) |

The slowest shapes sit at 45 to 60 µs p50 on the pods, so a 100 µs p99 leaves little room against interference on a shared host. The GPU server that runs the hardware gate will be a fourth CPU.

## Options

1. **Gate the absolute 100 µs p99 on whatever runner CI has.** It fails on noise and on CPU differences, not on regressions, and every CPU change would need a refit.
2. **Drop the CI check and rely on the hardware gate alone.** A regression that lets a shape cost far more than its price would go unseen until Release 1.
3. **Split the proof (chosen).** CI gates the part that does not depend on the CPU: no shape costs more than 1.15× the calibrated shape's time per unit in the same pass. The absolute p99 is measured in CI as information and proven on dedicated reference hardware.

## Decision

- **The target is unchanged:** route decision ≤ 100 µs p99. Only how it is evidenced changes (this ADR is that record; nothing here edits the roadmap's target table).
- **CI gate (task M1-24):** every gated shape and every cost-sweep point costs ≤ 1.15× the calibrated shape's time per unit, as the median of three passes in the same session. The check becomes blocking after three consecutive clean sessions, so it does not start life as a failing gate. The absolute p99 per run stays advisory (the run warns), with every attempt archived and reported, never only the last.
- **Reference hardware:** the absolute p99 is proven in the hardware gate on the GPU server's host (with M1-23), with enough runs to judge its tail.
- **Which CPU the prices follow:** the worst CPU seen. The prices are refitted to minimise the largest ratio over the EPYC 7763 data and the Cascade Lake pod data, not tuned to one of them.
- **The budget becomes a config knob** with today's value as the default, so a host can be calibrated by the harness (ADR 0005) and M1-23 records the value used on the GPU server.
- **Coverage against latency:** if the refit cannot bring the pod tail near 100 µs, lowering the budget (some large tools-first bodies would fail open to round-robin instead of keeping affinity) is a separate decision, taken with the measured numbers. It is not decided here.

## Consequences

- M1-24 is re-scoped and runs after M1 shipped (its evidence platform, the GitHub runners, was unavailable). AC-M1-5 passed on EPYC data and is not claimed on the pods until this lands; the M1 verification report already says the runner data cannot be re-checked here.
- A Route-only slowdown on two shapes (`empty_arrays`, `empty_objects_in_content`) is diagnosed first, since it may be plain overhead rather than pricing.
- `scripts/bench_p99.py` and the bench stage change as the owner approved here; the gate is not loosened to make a run pass.
- The M1 verification evidence for AC-M1-5 is refreshed once M1-24 is done.

## Amendment 2026-10-19 (M1-24 evidence)

Measured on the pods in `docs/results/2026-10-19-m1-24-key-scan-pods.md`:

- **The gate statistic is the minimum over the passes, not the median.** The median of three passes exceeded 1.15× on small-string and scalar sweep points in all three sessions, and the points that did changed from session to session. Timing noise only adds time, so the minimum tracks the cost. A shape that is really under-priced has a minimum above 1.15× too, so the gate still catches it. The gate in `scripts/bench_p99.py` uses the minimum.
- **Blocking needs three consecutive clean sessions.** With the minimum, two of three sessions were clean and the third had two points at 1.17 to 1.18×, so the gate stays advisory (its blocking switch stays off).
- **The budget default stays at 40,960.** On the pods, p99 is about 2.1× p50 and every swept shape stays under 100 µs p99 only up to a 32K budget. The default is sized for the reference core, where the EPYC data had 0 of 1,180 runs over, and the reference host decides in M1-23. A lower budget costs coverage (about 23 KiB instead of 33 KiB of dense tools ahead of the messages at 28K), which is the loss this ADR said not to take without the numbers. The example config tells operators of slow or shared cores to set `router.key_scan_budget` to 28,672 and how to recalibrate. If M1-23 shows the reference host over 100 µs, the default drops in a new ADR.
- **The dense-tools guarantee is 30 KiB, not 32 KiB**, so it keeps a 10% margin at the default budget (it had 2.7%). The measured limit is unchanged.

## Amendment 2026-11-02 (M1-28 evidence)

Awaiting the owner's review. Evidence in `docs/results/2026-11-02-m1-28-cost-ratio.md`. The first fix ran on an EPYC 7763 (run 41546762611); the second has not run on x86 yet, so its effect there is unmeasured: that run and the three sessions wait for the GPU server or the next GitHub Actions quota.

- **The check failed for a code slowdown and a wrong price together, and the first hid the second.** M1-24's `skipDepth` kept its byte-loop counters in stack slots: the calibrated shape went from 34.8 to 41.4 µs on the 7763, which lowered every ratio. `n08` had been at 1.18 to 1.20 since M1-22 and only came down to 1.15 that way.
- **The slowdown is partly fixed, and that is measured.** The first fix reached 38.2 µs, about 48% of the way to the 34.4 µs of full recovery at the new prices, with a ratio verdict of PASS at worst 1.05. The residual had two named causes, and the second fix (`shortRun` 9 and `skipRun`'s argument order) removes both as far as arm64, Rosetta and the amd64 instructions can show. Whether that closes it on x86 is open until a run says so.
- **A gate that stops at 3-byte stretches could not see one path.** Stretch shapes are gated now.
- **No limit changed.** 1.15 is met by the measured fit below, and 1.5 stays: its one failure was a single binary out of seven, with the same scan code at 1.06 one commit later and no cause found. A limit moved for that would hide the effect it exists to catch.
- **Prices raised, one added, none lowered.** A string's time is a step at the second word load on all three CPUs with data, though the Xeon 6973P sits higher: `shortStringQ` gets a floor of 37 quarters from 8 bytes. A `skipRun` costs a fixed 50 to 60 quarters: `runQ` is 15 units. It pays for that step, about 9 ns at 9 bytes, by failing bodies with 9 to 16-byte stretches open about 45% sooner; it does not remove it, and removing it is the open code fix. `memberQ` goes from 18 to 20 units for `escaped_keys`. On the first fix's code `memberQ` 20 is not needed to pass; it earns its cost only once the residual is fixed.
- **Prices do not yet follow the worst CPU seen, as this ADR asks.** The Xeon 6973P is projected at 1.16 on 0 to 6-byte strings, 7 points over 1.15, from one old run. Raising that floor mostly raises the calibrated shape with it: a net 2.2%, for 1.9% on every other point and another 1 KiB of the guarantee. It is left until a session on that CPU gives current data.
- **The fit does not include the Cascade Lake pods as this ADR asked.** Their raw data is not committed and not available to this work. A 5 to 8-byte stretch of brackets is back on the per-byte branch M1-24 removed for them, and that is unmeasured there.
- **The dense-tools guarantee is 29 KiB, not 30 KiB**, so it keeps its 10% margin: the calibrated shape's price rises 1.1%, and 30 KiB would have 8.9%. The measured limit goes from 33.1 to 32.6 KiB; a 30 KiB body still keys. This takes coverage rather than raising the budget 1.03%; the default stays at 40,960.
- **The 100 µs target and the blocking rule do not change.** The gate stays advisory until three consecutive clean sessions on GitHub-hosted runners. Decided by the owner: any GitHub-hosted runner counts, reported per CPU; the 1.15 and 1.5 gates are the criteria and the rest are diagnostics; the sessions should span different builds, because the 1.51 effect is per binary; a run from before the last code change does not count.
