# 0016 — CI on GitHub Actions: lean triggers, local evidence when minutes run out

- Status: accepted
- Date: 2026-10-24
- Milestone: all (amends 0017 on where the cost-ratio gate becomes blocking)
- Brief sections: none (how changes are checked; no request-path behaviour changes)

## Context

The repository is private on GitHub's free plan: 2,000 Actions minutes a month, no branch protection and no auto-merge (the API answers 403). The October minutes ran out mid-month, and the benchmark and soak proofs for AC-M1-5 and AC-M4-5 were taken on shared CI pods meanwhile (ADR 0017, `docs/results/2026-10-19-m1-24-key-scan-pods.md`, `docs/results/2026-10-22-m4-07-soak.md`). The minutes reset on 2026-11-01.

The checks those proofs and the merge rule rely on have to run on GitHub-hosted runners again: the repo checks, gofmt, vet, staticcheck, build and race tests for both modules, the compose build, the integration tests against the dev stack (including the template registry on Postgres), the phase gate, the route p99 bench with the cost-ratio gate, and the one-hour soak. A run of all of them on every push would spend the month's minutes again.

## Options

1. **GitHub Actions with lean triggers (chosen).** The same checks, but drafts and docs-only changes run only the repo checks, the bench runs only for router changes or by hand, the soak only by hand, and a newer push cancels the run it supersedes. Costs nothing beyond the free minutes. When the minutes are gone, nothing checks a PR on GitHub until the reset.
2. **A self-hosted runner on a workstation.** No minute limit. The machine must be awake for every PR, it runs PR code with the owner's credentials nearby, and its numbers are not comparable with the earlier runner data.
3. **Make the repository public.** Unlimited minutes and branch protection. Publishes the code, which is not decided.

## Decision

CI is GitHub Actions, in `.github/workflows/`:

- `ci.yml`: `detect`, `repo-checks`, `go` (per module), `compose`, `integration` and `ci-ok`, the single check a protected branch would require. `go`, `compose` and `integration` skip draft PRs and changes confined to `docs/`, `tasks/`, `.claude/` or Markdown; skipped jobs count as passing. The integration job creates the env files with `make compose-env` and reaches the stack's Postgres with the credentials in the generated `postgres.env`. A second step runs `gateway/integration/run-in-network.sh`, which runs the tests that need the Compose network in a container there: the admin API tests, the abort test's gateway-metrics check and the Redis tests in `internal/redis` and `internal/arbitrator`.
- `phase-gate.yml`: blocks a `phase/mN` PR until the milestone is verified (`scripts/check_repo.py --phase-gate`).
- `bench.yml`: AC-M1-5 on router changes (drafts skip it) or by hand. `bench_p99.py` bit 1 (an absolute p99 over 100 µs, a missing shape or a failed `TestExtractP99`) is a warning; bit 2 (a cost ratio over 1.15×, or whole route over 1.5× the key scan) fails the run only when blocking is on: the `ratio_gate_blocking` input of a manual run, or the repository variable `RATIO_GATE_BLOCKING=true` for every run.
- `soak.yml`: AC-M4-5 by hand only (about 70 minutes), with the soak results file written from its artifact. The recorded soak ran on a 16-CPU pod; a hosted runner is far smaller, so the first run on one is `soak_seconds: 900` (Redis reaches `maxmemory` at about 840 s; a shorter run fails that check by design).
- `dependabot.yml`: weekly grouped updates for actions and Go modules.

When the minutes are used up, a PR runs the same checks locally (`make build test lint`, `python3 scripts/check_repo.py`, `node scripts/check_workflows.mjs`, the integration tests against `make up-dev`), states in its body that GitHub CI did not run and pastes the results, and merges on that evidence. After the reset, `gh workflow run ci.yml --ref main` re-checks what merged meanwhile.

Without branch protection, `ci-ok` and `phase-gate` cannot block a merge; whoever merges reads them first (`gh pr checks`).

**Amends ADR 0017:** the cost-ratio gate becomes blocking after three consecutive clean `bench.yml` sessions on GitHub-hosted runners. Until then blocking stays off; once they are in, set the repository variable `RATIO_GATE_BLOCKING=true` (the input only covers manual runs).

## Consequences

- GitHub bills each job in whole minutes. A docs-only or draft push spends about 4 (detect, repo checks, `ci-ok`, phase gate). A code push spends roughly 25: the two Go jobs and the compose build run side by side, then the integration job builds the images again and runs the stack. The bench adds about 15 minutes, the soak about 70, and each weekly dependabot group triggers a full code run.
- The shared-pod measurements stay valid as evidence: they are a slower, noisier CPU than the GitHub runners (AMD EPYC 7763 and Intel Xeon 8370C so far), which is why ADR 0017 gates on CPU-portable ratios.
- Revisit if the minutes run out again before month end (tighten the triggers or add a self-hosted runner), or if the repository becomes public (turn on branch protection with `scripts/protect-main.sh`).
