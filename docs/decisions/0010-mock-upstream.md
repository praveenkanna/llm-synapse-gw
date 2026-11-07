# 0010 — Mock upstream for GPU-less development and CI

- Status: accepted
- Date: 2026-09-24
- Milestone: M0
- Brief sections: 10

## Context

Development happens on machines without GPUs, and tests need deterministic upstream behaviour including failures. Real vLLM is only on the GPU server.

## Decision

Build `mockllm` in the gateway module: OpenAI-compatible endpoints, configurable TTFT, tokens/s and output length, failure injection, vLLM-named `/metrics`, a cancel counter and request capture. The compose `dev` profile uses it in place of vLLM.

## Consequences

- All correctness, streaming and failure tests run anywhere.
- Mockllm can't prove prefix-cache or GPU behaviour; milestone proofs about those require the `gpu` profile.
- Keep mockllm's `/metrics` names in sync with the vLLM version in use.
