# YYYY-MM-DD · M? · <profile> · <load>%

## Setup

- Git SHA:
- Upstream: mockllm | vLLM <version>
- Hardware: <server, GPUs>
- Pools and replicas:
- Gateway config diff from defaults:
- Profile file and seed:
- Duration and warm-up:
- Commands:

```
<exact commands>
```

## Results

| Metric | Baseline (<direct / round-robin>) | Target (gateway) | Delta |
| --- | --- | --- | --- |
| TTFT p50 interactive (ms) | | | |
| TTFT p95 interactive (ms) | | | |
| TTFT p99 interactive (ms) | | | |
| TTFT p95 background (ms) | | | |
| Throughput (req/s) | | | |
| Output tokens/s | | | |
| Error rate (%) | | | |
| Shed / 429 rate (%) | | | |
| Prefix-cache hit rate (%) | | | |
| Preemptions | | | |
| Gateway RSS max (MB) | | | |

## Verdict

Which acceptance criterion this supports, and whether it passes.

## Notes

Anything surprising, limitations (e.g. mockllm can't prove cache behaviour).
