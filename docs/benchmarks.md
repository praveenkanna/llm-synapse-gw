# Performance Benchmarks

Performance, latency, and throughput verification for **LLM Synapse Gateway (`llm-synapse-gw`)**.

Benchmarks evaluate micro-benchmarks (body indexing, hash calculation, routing), tail latency gates (p99 decision costs), and live hardware verification against a hosted RunPod vLLM cluster running `Qwen/Qwen3-32B-AWQ`.

---

## 1. Live Hardware Verification (vLLM on GPU)

Tested head-to-head comparing direct vLLM access against gateway routing over public WAN to RunPod:

- **Model**: `Qwen/Qwen3-32B-AWQ`
- **Context Length**: 32,768 tokens (`max_model_len`)
- **Protocol**: OpenAI-compatible streaming SSE (`/v1/chat/completions`)
- **Hardware**: Dedicated NVIDIA GPU instance on RunPod

### Results Summary

| Benchmark Scenario | Direct RunPod vLLM | LLM Synapse Gateway | Impact & Efficiency |
| :--- | :--- | :--- | :--- |
| **Stream TTFT (p50)** | 277.5 ms | **123.3 ms** | **-55.6% latency** via persistent warm connection pooling |
| **Stream Inter-Token Latency (p50)** | 26.9 ms | **26.9 ms** | **0.0 ms overhead** — zero relay degradation |
| **16-Stream Burst Completion** | 651.6 ms | **545.7 ms** | **+16.3% throughput gain** |
| **16-Stream Burst TTFT (p50)** | 347.4 ms | **241.3 ms** | **-30.5% latency** under burst load |
| **50-Way Request Coalescing** | 50 separate GPU flights | **1 GPU flight** (1,248 ms total) | **50 concurrent callers served from 1 single generation** |
| **Wire Parity & SSE Framing** | Native vLLM | **100% Byte-Identical** | Full OpenAI compatibility, chunk-for-chunk relay |

---

## 2. Micro-Benchmarks & Subsystem Throughput

Gateway hot paths are designed with zero-allocation guarantees, single-pass SIMD/SWAR scanning, and lock-free concurrency structures.

| Subsystem / Benchmark | Throughput / Latency | Allocations | Optimization Technique |
| :--- | :--- | :--- | :--- |
| **Body Indexer (`1 MB` payload)** | **6,863 MB/s** (152 µs) | **0 B/op, 0 allocs** | 8-byte SWAR string scanning + fused RFC 8259 validator |
| **Body Indexer (`1 KB` payload)** | **2,915 MB/s** (376 ns) | **0 B/op, 0 allocs** | Zero-copy byte indexing on stack-allocated struct |
| **Coalescing Key Hasher** | **< 2 µs** | **0 B/op, 0 allocs** | Canonical streaming SHA-256 hasher with pooled hash state |
| **Route Selection Ring** | **< 35 µs p99** | **0 B/op, 0 allocs** | 512 virtual nodes consistent hash ring with bounded load |
| **Response Cache Decoder** | Zero-alloc bulk reader | **0 B/op, 0 allocs** | Direct byte slicing bypassing string allocations |
| **Metrics Family Lookup** | **~3 ns** | **0 B/op, 0 allocs** | Lock-free typed atomic pointer arrays |
| **Telemetry Stdout Logger** | Non-blocking | **0 B/op, 0 allocs** | 64 KiB bounded ring buffer batching kernel syscalls |

---

## 3. Tail-Latency Routing Gate (`bench_p99.py`)

A rigorous automated gate verifies route extraction and selection latency across 88 distinct request shapes (varying body sizes, nested bracket structures, system prompts, and multi-turn conversations):

```
================================================================================
Pass 3 of 3: Verification Run
================================================================================
Total extraction shapes: 40 (5 runs each = 200 runs)
Total route shapes:      48 (5 runs each = 240 runs)
Worst p99 across all shapes: 34.7 µs (limit: 100.0 µs)
Runs over 100 µs limit: 0 / 440 (0.00%)
Worst Cost Ratio: 1.00 (limit: 1.15)
Route/Extract Ratio: 0.99x - 1.04x (limit: 1.50x)

VERDICT: PASS
```

- **Worst p99 latency**: **34.7 µs** (well within the strict `< 100 µs` SLA).
- **Zero degradation** on pathological nested payloads up to 16 MB.

---

## 4. Reproducing Benchmarks

### Running the Micro-Benchmarks
```sh
cd gateway
go test -bench=BenchmarkRead -benchmem ./internal/bodyindex
go test -bench=BenchmarkKey -benchmem ./internal/coalesce
go test -bench=. -benchmem ./internal/router/ring
```

### Running the Routing Gate
```sh
python3 scripts/bench_p99.py
```

### Running Load Tests with the Harness
The included `harness` CLI can simulate synthetic and burst traffic:
```sh
make build
./harness/bin/loadgen -config harness/profiles/burst.yaml -target http://localhost:8080
```

