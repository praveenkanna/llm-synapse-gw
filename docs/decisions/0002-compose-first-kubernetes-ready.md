# 0002 — Docker Compose on one server first, Kubernetes-ready code

- Status: accepted
- Date: 2026-09-24
- Milestone: all
- Brief sections: 8, 9

## Context

The requester deploys with Docker Compose on a single GPU server today and plans a Kubernetes cluster later. The brief requires at least three gateway replicas on separate hosts, which one server can't satisfy.

## Options

1. **Compose now, Kubernetes-ready code.** Discovery and routing behind interfaces, config from env/files, liveness/readiness split, graceful drain. No manifests yet.
2. **Build for Kubernetes now.** Premature: no cluster exists.
3. **Compose with Compose-specific assumptions.** Cheapest now, rewrite later.

## Decision

Option 1. Two gateway containers behind nginx on one server; replicas addressed by service name via static discovery config.

## Consequences

- Accepted risk: the server is a single point of failure until multi-host. Callers must keep the direct vLLM URL as a fallback (brief section 8).
- Multi-host or Kubernetes becomes a discovery implementation plus manifests, not a code change on the request path.
- When on Kubernetes, revisit brief section 9: llm-d / Gateway API Inference Extension become viable (see ADR 0003).
