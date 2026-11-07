# 0008 — Tokenizer binding and chat-template rendering

- Status: accepted 2026-10-03 (tokenizer part decided by spike M0-16, chat-template part by spike M0-17). The one-time recheck on the final models (M0-27) is replaced by ADR 0011's runtime self-check against each replica's `/tokenize` (M3-04/M3-05).
- Date: 2026-09-24 (tokenizer findings added 2026-09-25, chat-template findings added 2026-09-25)
- Milestone: M3, M5
- Brief sections: 6.1, 5.1, 5.2

## Context

Exact token counts (M3) need the serving model's tokenizer; the brief requires HuggingFace tokenizers via cgo, never a reimplementation. Token-block routing (M5) additionally needs the exact post-chat-template token sequence, including injected `tools`.

## Options

Tokenizer:
1. cgo binding to HuggingFace `tokenizers` (Rust) — exact, in-process; complicates the build.
2. Call vLLM's `/tokenize` endpoint — exact, no cgo; adds a network hop and load on replicas.

Chat template:
1. Port/render the Jinja template in Go (e.g. a Jinja-compatible Go library) — in-process; fidelity risk.
2. vLLM `/tokenize` with messages — exact; extra hop.
3. Hash at message boundaries — no rendering; coarser.

## Decision

### Tokenizer (spike M0-16, `spikes/tokenizer/`)

**Option 1: cgo binding `github.com/daulet/tokenizers` v1.27.0** (Rust `tokenizers` crate 0.22.0, prebuilt static `libtokenizers.a` from the same release tag), loading each model's `tokenizer.json`. One tokenizer instance per model per process, shared by all goroutines.

Evidence (reports in `spikes/tokenizer/results/`):

| Check | Result |
| --- | --- |
| Models (stand-ins until Q2) | `Qwen/Qwen2.5-7B-Instruct`, `Llama-3.1-8B-Instruct` (unsloth ungated mirror), plus `mistralai/Mistral-7B-Instruct-v0.3` as a SentencePiece-derived stress case; revisions pinned in `spikes/tokenizer/models.json` |
| Agreement with Python `transformers` | **50/50 samples, 100% exact** for all three models, in both `add_special_tokens` modes, against both transformers 4.57.6 (tokenizers 0.22) and 5.17.0 (tokenizers 0.23.2). Compared on full ID sequences (SHA-256 of the IDs), not just counts. Samples cover empty input, whitespace/CRLF, code, JSON tool schemas, numbers, CJK/RTL/Indic/Thai, emoji ZWJ sequences, NFD combining marks, zero-width and control characters, literal special-token strings, lookalikes, and a 31 KB prompt. |
| Platforms | darwin/arm64 native; linux/arm64 Docker (glibc builder, `distroless/cc` runtime, 58 MB image); linux/amd64 Docker (emulated, agreement only) |
| Latency per `Encode` call (IDs only, single thread) | p50 12–28 µs over the sample mix; ~1 µs per token (≈1 M tokens/s, ≈5 MB/s); 31 KB / ~4.8 k-token prompt ≈ 5 ms |
| Concurrency | 8 goroutines on one shared tokenizer: 19–31 k calls/s, every result identical to sequential |
| Load time and memory (linux, resident after load) | Qwen2.5 ≈ 70 MiB, Llama 3.1 ≈ 140 MiB (≈190 MiB peak while parsing), Mistral ≈ 20 MiB; load 40–900 ms |

Findings that bind the M3 implementation (`internal/tokenizer`):

1. **Build.** `CGO_ENABLED=1`, glibc builder (`golang:<ver>-bookworm`) and `gcr.io/distroless/cc-debian12` runtime. The Rust static library needs libgcc/libstdc++/libm, so `distroless/static` is out. Pin the library tarball to the same release tag as the Go module: the Go side has a link-time version symbol check, so a mismatch fails the build, not at run time.
2. **Not musl.** A fully static musl build on `distroless/static` works and agrees, but concurrent throughput drops ~10× (1.9–5.2 k calls/s vs 19–20 k on glibc) from musl's allocator. Don't use it for the gateway.
3. **NUL bytes truncate input.** The binding passes text as a C string, so everything after a `\x00` is dropped (sample `e01`: Go 1 token, Python 3). JSON `"\u0000"` in message content decodes to exactly that. The tokenizer wrapper must check for a NUL byte and fail open (record `gateway_failopen_total{stage="tokenize",reason="nul_byte"}`), never return a short count.
4. **Cost scales with prompt length.** At ~1 µs/token, a 32 k-token prompt costs ~30 ms of CPU, far more than the M0 TTFT-overhead budget (≤ 5 ms p50). M3 must not put exact counting of long prompts on the TTFT path unbounded: configurable byte ceiling above which it skips or estimates and fails open, and a bound on concurrent tokenize calls.
5. **Memory is a per-process fixed cost.** Two tiers ≈ 200–300 MiB resident, and glibc did not give memory back to the OS after `Close`. Load at startup (or on registry change, out of band), never per request, and size the container memory limit for it.
6. **Reference = what vLLM uses.** vLLM tokenizes with `transformers` `AutoTokenizer`. Both current transformers lines agree here, but transformers 4.x rebuilds Mistral's fast tokenizer from `tokenizer.model` (because of `legacy: false`/`add_prefix_space`), and vLLM may use `mistral_common` for Mistral models. Once Q2/Q4 name the models and vLLM version, rerun the spike against that version and against a live replica's `/tokenize` before M3 ships.
7. **Special tokens.** The binding's default (special-token strings in text are matched as specials) matches `transformers`' default (`split_special_tokens=False`); literal `<|im_start|>` etc. in user text therefore count as one token each, in both.

Option 2 (vLLM `/tokenize`) remains the fallback if the build constraint in finding 1 becomes unacceptable; it would also be the ground truth for the recheck in finding 6.

### Chat template (spike M0-17, `spikes/chattemplate/`)

**vLLM is the oracle; Go renders only where it has proven it matches.**

1. **Option 2 (vLLM `/tokenize` with `messages`) is the ground truth and is used off the request path**: M3 template registration (token counts that include the chat template, which AC-M3-1 compares with `usage.prompt_tokens`), per-template conformance checks, and the M0-27 recheck. It is not used per request.
2. **Option 1 is the in-process path for M5, if M5 is built**: `github.com/ardanlabs/jinja` with our Python-compatible `tojson` (upstream PR or pinned fork; patch in `spikes/chattemplate/patches/`), fed by a Go copy of vLLM's message preprocessing (`spikes/chattemplate/internal/vllmprep`), then the M0-16 tokenizer. A pool gets token-block routing only after its template passes the conformance check against option 2; anything outside the checked subset fails open to affinity routing (AC-M5-3).
3. **Option 3 (message-boundary hashing) does not replace rendering.** It yields no tokens, so it can't align to vLLM's KV blocks, and its prefix claim is wrong for templates that aren't prefix-stable. It remains a cheap, template-free candidate for the M1 affinity key (ADR 0003).
4. **gonja is rejected**: it loses dict key order and emits Go-style JSON, so every request with `tools` renders wrong.

Evidence (`spikes/chattemplate/results/2026-09-25-darwin-arm64-native.md`):

| Check | Result |
| --- | --- |
| Models (stand-ins until Q2) | The three M0-16 models plus `Qwen/Qwen3-8B` (reasoning-history stripping, the hardest current template); revisions pinned in `spikes/chattemplate/models.json` |
| Ground truth | vLLM 0.30.0 (CPU wheel, transformers 5.17.0) `vllm launch render`, which runs the same renderer as `vllm serve` without a GPU. 55 synthetic requests per model: tools (nested schemas, no description, `strict`, odd key order, floats, non-ASCII, HTML characters), tool calls and results (parallel, invalid/empty arguments, text-part results, long IDs), `developer` role, text parts, `continue_final_message`, `response_format`, `chat_template_kwargs`, reasoning history, 20 KB prompt, and four conversation families |
| Option 2 fidelity | `/tokenize` token IDs identical to `/v1/chat/completions/render` for **216/216** renderable requests (55, 54, 52, 55); the remaining 4 are rejected by the templates themselves on both paths |
| Option 1 fidelity: ardanlabs/jinja v1.6.0 as shipped | Exact (identical IDs, or both reject) 52, 31, 52, 52 of 55 for Qwen2.5, Llama 3.1, Mistral v0.3, Qwen3, plus the one declined request below. Every wrong prompt comes from `tojson`: floats lose `.0` (`1.0` → `1`), and `tojson(indent=4)`, which Llama 3.1 uses for every tool, falls back to Go's `encoding/json` (sorted keys, HTML escaping) |
| Option 1 fidelity: ardanlabs/jinja + `tojson` patch | **54/55 exact for every model, 0 wrong prompts**; the 55th (integers beyond int64 in a tool schema) is declined and fails open, because the engine's value type is int64 and Python keeps big ints exact |
| Option 1 fidelity: gonja/v2 v2.9.0 | 30, 31, 31, 30 of 55; wrong on every request with `tools` (Go maps lose key order, compact `tojson`; Mistral's `message == user_messages[-1]` dict comparison fails so tools are dropped) |
| Latency, option 1 (patched ardanlabs, one core) | Render p50 8–12 µs, p99 39–70 µs; tokenizing the result p50 22–95 µs, p99 3.2–4.6 ms (the 20 KB prompt, as in M0-16); 69–91 k renders/s with 8 goroutines on one compiled template |
| Latency, option 2 (idle render server, loopback) | p50 0.77–0.87 ms, p99 5.7–6.8 ms sequential; with 8 concurrent clients p50 3.9–4.1 ms, p99 9.6–13.2 ms, 1.7–1.8 k requests/s per server process; 490–680 MiB resident per model |
| Latency, option 3 | p50 1.7 µs, p99 ≈ 70 µs (SHA-256 over sorted-key JSON per message) |
| Option 3 vs token truth (consecutive turns, same conversation) | Qwen2.5, Qwen3, Llama 3.1: all earlier messages shared and 100% of the earlier prompt's tokens shared. Mistral v0.3: all earlier messages shared, yet only 7%, 45%, 58% (plain chat) and 91%, **0%**, 93% (with tools) of the earlier prompt's tokens shared, because its template renders the system prompt and `[AVAILABLE_TOOLS]` just before the *last* user message |

Findings that bind the implementation:

1. **Preprocessing is half the fidelity.** A correct Jinja engine still renders the wrong prompt unless its inputs are what vLLM builds. vLLM 0.30 joins text parts with `\n`, parses tool-call `arguments` into objects (invalid or non-object JSON becomes `{}`), rebuilds tool calls as `id, function{arguments, name}, type`, dumps tools as `type, function{name, description, parameters[, strict]}` with `"description": null` when absent and unknown keys dropped, turns `developer` into `system` (merging system messages) when the template has no `developer` role, and passes special tokens plus `chat_template_kwargs` as variables. Sources: `vllm/entrypoints/chat_utils.py`, `vllm/renderers/hf.py`, `vllm/entrypoints/openai/chat_completion/protocol.py` at tag v0.30.0. The Go copy is `internal/vllmprep`, verified end to end on 216 requests.
2. **JSON must be Python's.** Keep request key order (tool schemas are rendered in client order), keep int vs float (`1.0` stays `1.0`), use `", "`/`": "` separators, no HTML escaping, raw U+2028/U+2029, short escapes for `\b` and `\f`. `internal/ojson` does this and is tested against CPython output.
3. **Mistral-family models may not use the Jinja template at all.** With the default `--tokenizer-mode auto`, vLLM 0.30 uses `mistral_common` for a Mistral repo that ships `consolidated*.safetensors` and `tokenizer.model.v*`/`tekken.json` (true for Mistral-7B-Instruct-v0.3; `vllm/tokenizers/registry.py`). The spike rendered that model in HF mode. Option 1 applies only to pools served with `tokenizer_mode=hf`; option 2 covers both. Whether `mistral_common` matches the HF template was not tested (unverified).
4. **Prefix stability is a property of the template, not the conversation.** Templates that move the system prompt or tools toward the latest turn (Mistral v0.3) defeat prefix caching across turns whatever the router does. The M1 harness should report prefix-cache hit rate per pool, so a pool like this isn't counted against affinity routing.
5. **Server flags change the prompt.** `--chat-template`, `--default-chat-template-kwargs`, `--exclude-tools-when-tool-choice-none` and `--tokenizer-mode` must be mirrored in the gateway's pool config for option 1; a request carrying its own `chat_template` is declined.
6. **Option 2 is too slow and too coupled for the request path.** Even idle and on loopback, its p50 is about a sixth of the M0 TTFT-overhead budget (≤ 5 ms p50), and it rises about fivefold with 8 concurrent clients on one process. It would also make every optimized request depend on a replica's (or sidecar's) Python API server. Latency against a loaded GPU replica was not measured.

Deliberately left out: multimodal content parts, named/`tool_use` chat templates, the Responses API, and output re-tokenization (a turn's generated tokens can differ from how the same text is rendered as history; Qwen3 strips `<think>` from history). All are declined or out of scope until a pool needs them.

## Consequences

- The gateway image becomes a cgo build with a glibc runtime base; CI's Go job needs `libtokenizers.a` available for the tokenizer package (only when M3 adds it; M0 code stays cgo-free).
- `internal/tokenizer` gets: NUL-byte check, length ceiling, bounded concurrency, fail-open reasons, and load/memory metrics.
- Final agreement must be re-run once Q2 (model names) and Q4 (vLLM version) are answered: `cd spikes/tokenizer && make fetch reference run docker` after editing `models.json`.
- M3 registration computes token counts through vLLM `/tokenize` with `messages` (out of band, cached per template version), never on the request path; a replica or `vllm launch render` sidecar must be reachable from the admin surface.
- M5, if started, adds: the patched ardanlabs/jinja (or an upstream release with the `tojson` fix), a production version of `vllmprep`, per-pool conformance checks against `/tokenize` at startup and on template or vLLM changes, and `gateway_failopen_total{stage="render",reason=...}` for declined requests (big ints, unsupported content, mismatched pool).
- Rerun `cd spikes/chattemplate && make fetch reference run` when Q2/Q4 are answered and on every vLLM upgrade; preprocessing is version-specific.
- Revisit if option 2 turns out cheap on a loaded replica (it would remove the Jinja dependency), or if vLLM exposes a stable Rust/Go-callable renderer.
