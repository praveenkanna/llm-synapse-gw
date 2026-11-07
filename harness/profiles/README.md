# Workload profiles

Synthetic traffic for `loadgen -profile <file>`. Each profile mirrors one of
the brief's traffic shapes (section 2); `mixed` runs all four at once.

| Profile | Shape | Stream | Prefix sharing | Turns |
| --- | --- | --- | --- | --- |
| `chat.yaml` | interactive chat, longest outputs | yes | 4 system prompts, 90% of conversations | 2–10, growing history |
| `structured.yaml` | ~20 single-shot JSON pipelines | no | one per pipeline, 100% | 1 |
| `classify.yaml` | short, high-frequency, small tier | no | one per classifier, 100% | 1 |
| `background.yaml` | long inputs, bursty, latency-insensitive | no | 3 job types, 70% | 1 |
| `mixed.yaml` | all four; classify fires 2–4 times after each chat turn; bursts add background only | mixed | per class | per class |
| `m3-misordered.yaml` | AC-M3-6 before: ~4,900-char templated prompt, volatile text at char 732; plain requests | yes | first 732 chars (15%) | 1 |
| `m3-compiled.yaml` | AC-M3-6 after: the same text compiled (static first); `X-Template-Id: support-assistant`, version 1 | yes | first 4,617 chars (~94%) | 1 |
| `m4-cache.yaml` | AC-M4-4: cacheable trail FAQ (`m4-faq-cache`), temperature 0, 200 keys per class | both | whole prompt but one number | 1 |
| `m4-cache-baseline.yaml` | the same requests on `m4-faq-plain`, which caches nothing | both | as above | 1 |
| `m4-soak.yaml` | AC-M4-5: 200% load; coalescable short and long streams, cacheable unique and hot keys, ordinary chat | mostly | per class | 1 |

All text is generated from a fixed syllable table and fixed by the seed. No
real prompt is read, stored or sent. Profiles hold no text, and template
specs (`templates/`) hold only invented text.

```sh
loadgen -profile harness/profiles/mixed.yaml -duration 5m -seed 7
loadgen -profile harness/profiles/chat.yaml -rate 16          # 2x the profile rate
loadgen -profile harness/profiles/classify.yaml -url http://127.0.0.1:8002   # small tier, direct
```

The profile's `arrival` sets the default rate and burst; `-rate`,
`-concurrency` and `-burst-*` override it. `-model` replaces every class's
model. Rates are placeholders until fleet capacity is benchmarked; express
runs as multiples of it (50%, 100%, 200%) with `-rate`.

## Schema

Top level: `name`, `description`, `api` (`chat` default, or `completions`),
`model` (default for classes), `arrival`, `classes`.

`arrival`: `rate` (mean requests/s, Poisson) and optional `burst`:
`every`, `length` (durations, e.g. `60s`), `factor` (rate multiplier inside a
window; windows start at `every`, `2*every`, ...) and `classes`. With
`classes`, a burst's extra arrivals go only to those classes and every other
class keeps its base rate, triggered classes included; without it every class
scales. A burst class must not trigger another class.

Per class:

| Key | Meaning |
| --- | --- |
| `name`, `model` | class label in results; model sent (overrides the profile's) |
| `weight` | share of arrivals; `0` only with `triggered_by` |
| `stream` | SSE with `stream_options.include_usage` |
| `response_format` | `json_object` for JSON-constrained output (chat API) |
| `ignore_eos` | vLLM only: generate exactly `max_tokens` |
| `priority` | optional gateway priority class (e.g. `interactive`, `user`, `background`), sent as the `X-Priority` header on every request of the class; 1–32 of `a-z`, `0-9`, `-`, `_`. Unset sends no header |
| `temperature`, `seed` | optional; sent in every body. The gateway coalesces or caches a request only with `temperature: 0` or an integer `seed` (ADR 0019) |
| `headers` | optional map of extra request headers, e.g. `{X-Mock-Output-Tokens: "400"}` to force a mockllm output length; at most 16, none the harness sets itself (`Content-Type`, `X-Priority`, `X-Template-*`, ...) |
| `system_words` | system prompt length |
| `shared_prefixes` | distinct shared system prompts, fixed by the seed |
| `shared_prefix_ratio` | share of conversations starting with a shared system prompt; the rest get a unique one |
| `prompt_words` | user message length per turn |
| `max_tokens` | `max_tokens` per request |
| `turns` | turns per conversation (default 1); each turn resends system + history + new user message |
| `reply_words` | synthetic assistant reply added to the history after each turn (multi-turn only) |
| `open_conversations` | conversations in progress (multi-turn only); each request continues a random one or starts one in a free slot |
| `max_context_words` | a conversation ends early rather than exceed this many words |
| `triggered_by` | `{class, count}`: after each request of `class`, the next `count` arrivals are this class |
| `template` | render messages from a template spec instead of synthetic words; see below |

Lengths are distributions: a plain integer, or `{dist: fixed, value: n}`,
`{dist: uniform, min, max}` (inclusive), `{dist: lognormal, median, sigma, min,
max}` (clamped). Prompt lengths are in synthetic words, `max_tokens` in tokens;
tokens per word depend on the tokenizer, and the measured count is
`prompt_tokens` in `results.jsonl`. Unknown keys are rejected.

## Template classes

A class with `template` sends chat messages rendered from a template version
spec, the way a Level 2 caller does (ADR 0006): the caller renders the
messages and, once the version is registered, adds `X-Template-Id` and
`X-Template-Version`.

```yaml
classes:
  - name: support
    weight: 1
    stream: true
    max_tokens: {dist: uniform, min: 16, max: 48}
    template:
      spec: templates/m3-prompt-compiled.json   # relative to this profile file
      id: support-assistant                      # id and version: sent as the two headers
      version: 1                                 # omit both to send plain requests
      variables:                                 # one generator per declared variable
        request_id: {gen: uuid}
        current_time: {gen: timestamp}
        user_name: {gen: pick, values: [Avery Lindqvist, Mei Tanaka]}
        question: {gen: words, length: 170}
        tenant: acme                             # plain string = {gen: fixed, value: acme}
```

- **Spec file:** JSON in exactly the gateway's registry format (`model`,
  `description`, `variables: [{name}]`, `messages: [{role, segments: [{kind,
  text}]}]`, `examples`, `cacheable`, `cache_ttl`, `coalesce`;
  `docs/plan/phase-3-compiled.md`), so the same file can be posted to the
  admin API. The harness applies the registry's checks except segment
  order, so a misordered spec still runs as plain requests.
- **Rendering:** each message's content is its segments' texts joined with
  nothing in between. Static text is copied literally. In `semi_static` and
  `volatile` text, every `{{name}}` (name `[a-z][a-z0-9_]{0,63}`, no spaces)
  is replaced by the variable's value, verbatim and without re-expansion;
  anything else in braces is literal. Every declared variable needs a
  generator and every generator a declared variable.
- **Generators:** `fixed` (`value`), `pick` (one of `values`), `hex`
  (`length` digits), `uuid` (version 4), `words` (synthetic words, exactly
  `length` characters), `timestamp` (an RFC 3339 UTC second in 2026), `int`
  (a decimal integer in `[min, max]`, for a bounded key space). All are
  fixed by the seed.
- **Scope:** a spec variable may declare `"scope": "shared"` or `"user"`. A
  spec that sets `coalesce` or `cacheable` must mark every variable shared,
  as the registry requires (422 otherwise).
- **Headers:** sent only when both `id` and `version` are set.
- **Other keys:** a template class is single-shot on the chat API. Leave
  `system_words`, `shared_prefixes`, `shared_prefix_ratio`, `prompt_words`,
  `turns` and `max_context_words` unset. The model is the class's, else the
  profile's, else the spec's.
- `run.json` records the spec path and its `spec_sha256`.

### AC-M3-6 specs (`templates/`)

Both specs are the same invented support-assistant prompt (role, policies,
escalation, formatting, tools, privacy). The volatile block holds the request
ID, current time and customer name, and the user message is a volatile
question. Rendered, each prompt is ~4,890 characters.

- `m3-prompt-misordered.json`: the volatile block starts at character 732 of
  the system message, and ~3,900 characters of static text follow it. Only
  the first 732 characters (15%) are shared between requests. The registry
  must reject it (static after volatile), which is the AC-M3-2 demonstration.
- `m3-prompt-compiled.json`: the same segments reordered. All static text
  comes first, the volatile block ends the system message, then the
  volatile user message. 4,617 characters (~94%) are a static prefix. Valid;
  register it as `support-assistant` and activate version 1 before running
  `m3-compiled.yaml`.

`internal/workload/template_test.go` pins these offsets. Run both profiles
with `-scrape` on the upstream. The cached share is `prefix_cache_hit_rate`
in `metrics.json`: Δ`vllm:prefix_cache_hits_total` / Δ`vllm:prefix_cache_queries_total`
over the run, in tokens.

### M4 specs and tools (coalescing and the response cache)

Three specs of one invented trail-conditions FAQ, whose only variable,
`section`, is `"scope": "shared"`: `m4-faq-coalesce.json` (`coalesce: true`),
`m4-faq-cache.json` (`cacheable: true`, `cache_ttl: 30m`) and
`m4-faq-plain.json` (no opt-in, the baseline). Register and activate each
from the Compose network:

```sh
GATEWAY_ADMIN_TOKEN=... harness/scripts/register-template.sh m4-faq-cache harness/profiles/templates/m4-faq-cache.json
```

- `results.jsonl` records the gateway's `X-Gateway-Cache` response header as
  `cache` (`hit`); `harness/scripts/cache-latency-stats.py` splits client
  latency by it.
- `cmd/burst` sends N identical requests at once (optionally one first, and
  some reading slowly) and reports each response's status, completeness and
  SHA-256; `harness/scripts/m4-coalesce-proof.py` wraps it with the
  replica's `mockllm_requests_total{endpoint="chat"}` before and after
  (AC-M4-1).
- `harness/scripts/m4-soak-sampler.py` samples coalescing retained bytes and
  flights per gateway, and Redis memory, evictions and TTLs, during a soak;
  `harness/scripts/m4-slow-readers.sh` adds slow coalescing followers so
  the replay bound's eviction runs (AC-M4-5).
