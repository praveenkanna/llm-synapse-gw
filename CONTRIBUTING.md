# Contributing to LLM Synapse Gateway

Thank you for your interest in contributing to **LLM Synapse Gateway (`llm-synapse-gw`)**!

This project is a high-performance, prefix-cache-aware inference proxy for distributed LLM backends (vLLM, SGLang, Ollama, etc.). We welcome contributions that improve throughput, reduce tail latency, expand model and provider support, or strengthen operational reliability.

---

## Code of Conduct

Please maintain a collaborative, respectful, and inclusive environment. All interactions in this repository adhere to standard open-source community standards.

---

## Development Prerequisites

- **Go**: Version `1.26+`
- **Docker & Docker Compose**: For local stack validation and integration testing
- **Python 3.10+**: For benchmark verification scripts (`bench_p99.py`)
- **Make**: For running standard build, test, and lint targets

---

## Getting Started

### 1. Clone & Setup

```sh
git clone https://github.com/praveenkanna/llm-synapse-gw.git
cd llm-synapse-gw
```

### 2. Build

The repository contains two Go modules: `gateway/` (the core proxy and mock engine) and `harness/` (the load generator).

```sh
# Compile all packages across both modules
make build
```

Compiled binaries land in `gateway/bin/` and `harness/bin/`.

### 3. Run Tests

All tests run with Go's race detector (`-race`), followed by strict allocation verification:

```sh
make test
```

### 4. Code Formatting and Linting

Before opening a pull request, verify that your code adheres to Go standards:

```sh
make lint
```

This runs `gofmt` checks, `go vet`, and `staticcheck` (v0.8.1).

---

## Architecture & Design Principles

Before making major design changes, please review:
- [Architecture Overview](docs/architecture.md)
- [Architecture Decision Records (ADRs)](docs/decisions/)

### Key Invariants:
1. **Zero-Allocation Hot Path**: Request ingestion, body parsing (`bodyindex`), routing key extraction, and telemetry recording must not introduce heap allocations per request on the fast path.
2. **OpenAI Wire Parity**: The `/v1/chat/completions` API must remain strictly byte-compatible with upstream engines. Never rewrite prompt text or alter streaming SSE chunk formats.
3. **Fail Open & Non-Blocking**: If an optimization stage (such as template matching or caching) encounters an unexpected condition, it should safely fail open rather than dropping client requests.
4. **No Hidden Retries**: Idempotency is strictly respected via `Idempotency-Key` or `X-Idempotency-Key` headers.

---

## Testing with Docker Compose

To test the full stack locally with mock LLM upstreams:

```sh
# Generate local environment files (never committed)
make compose-env

# Validate Docker Compose configuration
make compose-config

# Launch local dev stack with mock upstreams
make up-dev

# Test a streaming chat completion
make smoke

# Tear down the stack
make down
```

---

## Pull Request Guidelines

1. **Keep PRs focused**: Address a single bug fix, performance optimization, or feature addition.
2. **Include Tests**: Add unit tests or integration tests covering edge cases.
3. **Verify Allocation Count**: For hot-path changes, ensure `go test -run Alloc` passes without new allocations.
4. **Sign Your Work**: Commit messages should follow the conventional commit format:
   - `feat: add adaptive priority queue shedding`
   - `fix: resolve race in singleflight channel notification`
   - `perf: optimize SWAR byte scanner for UTF-8 validation`
   - `docs: update deployment guidelines for Kubernetes`

---

## License

By contributing to **LLM Synapse Gateway**, you agree that your contributions will be licensed under the project's [Apache 2.0 License](LICENSE).
