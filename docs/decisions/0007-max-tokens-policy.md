# 0007 — max_tokens policy without truncation

- Status: accepted
- Date: 2026-09-24
- Milestone: M3
- Brief sections: 5.6

## Context

The brief sets each template's `max_tokens` from its observed p95 output, which truncates about 5% of answers and contradicts its own "never truncate mid-answer". vLLM allocates KV blocks as tokens are generated, so a lower cap mainly limits runaway generations, not normal ones.

## Decision

- Never lower a `max_tokens` the caller sent.
- When the caller sent none, apply the template's cap: observed p99.9 output length plus a margin (configurable, default 25%), with a hard ceiling per pool.
- Keep stop sequences from the template.
- Flag generations that hit the cap as `runaway` in metrics and logs for the template owner to review.

## Consequences

- Needs per-template output-length histograms (M3-10) before caps are set; until then, the pool ceiling only.
