package ingress

import (
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex/bodyindextest"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/contextbudget"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/mockllm"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry/telemetrytest"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates"
)

// escapedName spells a name of n bytes with every byte a \uXXXX escape, the
// longest spelling JSON has.
func escapedName(n int) string { return strings.Repeat(`\u0061`, n) }

// pastCeiling wraps members in a body past bodyindex.MaxMembers, which the
// index holds no members of.
func pastCeiling(members string) string {
	return `{` + strings.Repeat(`"x":0,`, bodyindex.MaxMembers) + members + `}`
}

func TestModelBoundConstants(t *testing.T) {
	if config.MaxModelNameBytes != 256 {
		t.Errorf("MaxModelNameBytes = %d: a change moves what maps, and the results file", config.MaxModelNameBytes)
	}
	if maxModelValueBytes != len(`"`+escapedName(config.MaxModelNameBytes)+`"`) {
		t.Errorf("maxModelValueBytes = %d is not the longest spelling of the longest name", maxModelValueBytes)
	}
}

func TestReadBodyModelBound(t *testing.T) {
	const limit = config.MaxModelNameBytes
	atLimit, overLimit := strings.Repeat("a", limit), strings.Repeat("a", limit+1)
	huge := strings.Repeat("a", 1<<16)
	tests := []struct {
		name      string
		body      string
		wantModel string
		tooLong   bool
		wantOK    bool
	}{
		{name: "at the limit", body: `{"model":"` + atLimit + `"}`, wantModel: atLimit, wantOK: true},
		{name: "one over", body: `{"model":"` + overLimit + `"}`, tooLong: true, wantOK: true},
		{name: "at the limit in the longest spelling", body: `{"model":"` + escapedName(limit) + `"}`, wantModel: atLimit, wantOK: true},
		{name: "one over in escapes", body: `{"model":"` + escapedName(limit+1) + `"}`, tooLong: true, wantOK: true},
		{name: "one byte past the longest spelling", body: `{"model":"` + escapedName(limit) + `a"}`, tooLong: true, wantOK: true},
		{name: "raw long, decodes short", body: `{"model":"` + strings.Repeat(`\/`, limit) + `"}`, wantModel: strings.Repeat("/", limit), wantOK: true},
		{name: "huge", body: `{"model":"` + huge + `"}`, tooLong: true, wantOK: true},
		{name: "the last of a repeated key wins: long last", body: `{"model":"a","model":"` + huge + `"}`, tooLong: true, wantOK: true},
		{name: "the last of a repeated key wins: short last", body: `{"model":"` + huge + `","model":"a"}`, wantModel: "a", wantOK: true},
		{name: "an escaped key", body: `{"m\u006fdel":"` + huge + `"}`, tooLong: true, wantOK: true},
		{name: "a long string in another member", body: `{"model":"a","user":"` + huge + `"}`, wantModel: "a", wantOK: true},
		{name: "huge array", body: `{"model":["` + huge + `"]}`},
		{name: "huge object", body: `{"model":{"a":"` + huge + `"}}`},
		{name: "huge number", body: `{"model":1` + strings.Repeat("0", 1<<16) + `}`},
		{name: "null", body: `{"model":null}`, wantOK: true},
		{name: "past the ceiling: one over", body: pastCeiling(`"model":"` + overLimit + `"`), tooLong: true, wantOK: true},
		{name: "past the ceiling: at the limit", body: pastCeiling(`"model":"` + atLimit + `"`), wantModel: atLimit, wantOK: true},
		{name: "past the ceiling: huge array", body: pastCeiling(`"model":["` + huge + `"]`)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := readBody(new(bodyindex.Index), []byte(tc.body))
			if got.model != tc.wantModel || got.modelTooLong != tc.tooLong || ok != tc.wantOK {
				t.Errorf("model %.40q (%d bytes), too long %v, ok %v; want %.40q, %v, %v",
					got.model, len(got.model), got.modelTooLong, ok, tc.wantModel, tc.tooLong, tc.wantOK)
			}
		})
	}
}

// A body keeps what else it says when its model is too long: the priority a
// pool must handle, the stream flag and the caller's cap.
func TestReadBodyOverLongModelKeepsTheRest(t *testing.T) {
	long := strings.Repeat("a", config.MaxModelNameBytes+1)
	for _, body := range []string{
		`{"model":"` + long + `","stream":true,"priority":5,"max_tokens":1}`,
		pastCeiling(`"model":"` + long + `","stream":true,"priority":5,"max_tokens":1`),
	} {
		got, ok := readBody(new(bodyindex.Index), []byte(body))
		if !ok || !got.stream || got.priorities != 1 || string(got.priority) != "5" || !got.hasCap {
			t.Errorf("read %+v, ok %v", got, ok)
		}
	}
}

// bytesPerRead is what one readBody of body allocates.
func bytesPerRead(body []byte) uint64 {
	const runs = 5
	var ix bodyindex.Index
	readBody(&ix, body) // warm up
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range runs {
		readBody(&ix, body)
	}
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc) / runs
}

// TestOverLongModelIsNotDecoded: a decode copies the string, so an over-long
// model that is decoded shows up as megabytes.
func TestOverLongModelIsNotDecoded(t *testing.T) {
	const size = 4 << 20
	for name, body := range map[string][]byte{
		"plain":        []byte(`{"model":"` + strings.Repeat("a", size) + `"}`),
		"past ceiling": []byte(pastCeiling(`"model":"` + strings.Repeat("a", size) + `"`)),
		"escaped":      []byte(`{"model":"` + strings.Repeat(`\/`, size/2) + `"}`),
	} {
		t.Run(name, func(t *testing.T) {
			if got, limit := bytesPerRead(body), uint64(len(body)/64); got > limit {
				t.Fatalf("readBody allocated %d bytes on a %d-byte body, want under %d", got, len(body), limit)
			}
		})
	}
}

// readCost is the fastest of runs readBody calls.
func readCost(body []byte, runs int) time.Duration {
	var ix bodyindex.Index
	best := time.Hour
	for range runs {
		start := time.Now()
		readBody(&ix, body)
		if d := time.Since(start); d < best {
			best = d
		}
	}
	return best
}

// TestModelThatIsNotAStringIsNotDecoded: such a value allocates next to
// nothing on its way to the type error, so only its time shows it: decoding
// validates it a second time. The body is read once either way, so the
// same bytes in another member are the baseline.
func TestModelThatIsNotAStringIsNotDecoded(t *testing.T) {
	const maxRatio = 1.5
	value := `["` + strings.Repeat("a", 1<<20) + `"]`
	model, other := []byte(`{"model":`+value+`}`), []byte(`{"modek":`+value+`}`)
	var base, got time.Duration
	for range 5 {
		base, got = readCost(other, 20), readCost(model, 20)
		if float64(got) <= maxRatio*float64(base) {
			return
		}
	}
	t.Fatalf("a read took %v with a 1 MiB array as model, %v with it in another member", got, base)
}

// TestOverLongModelFailsOpenToTheDefaultPool: the model is treated as
// unmapped, counted once with its own reason, and the body reaches the
// default pool byte for byte.
func TestOverLongModelFailsOpenToTheDefaultPool(t *testing.T) {
	long := strings.Repeat("0123456789abcdef", 1<<12) // 64 KiB
	for name, body := range map[string][]byte{
		"indexed":          []byte(`{"model":"` + long + `","prompt":"x","max_tokens":1}`),
		"past the ceiling": []byte(pastCeiling(`"model":"` + long + `","prompt":"x","max_tokens":1`)),
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, "", Options{})
			resp, got := f.post(t, PathCompletions, body, nil)
			if resp.StatusCode >= http.StatusInternalServerError {
				t.Fatalf("status %d: %s", resp.StatusCode, got)
			}
			if n := count(f.large0) + count(f.large1); n != 1 || count(f.small) != 0 {
				t.Fatalf("default pool got %d, small %d; want 1, 0", n, count(f.small))
			}
			reqs := append(f.large0.Mock().Requests(), f.large1.Mock().Requests()...)
			if string(reqs[0].Body) != string(body) {
				t.Fatalf("the body changed on its way upstream")
			}
			waitFor(t, "the fail-open count", func() bool {
				return telemetrytest.Sum(t, f.tel.Metrics(), telemetry.MetricFailOpen, `stage="pool_resolve"`, `reason="model_too_long"`) == 1
			})
			if s := f.outcomes(t); s.UnknownModel != 0 || s.UnparseableBody != 0 {
				t.Fatalf("also counted as %+v", s)
			}
			if n := telemetrytest.Sum(t, f.tel.Metrics(), telemetry.MetricFailOpen); n != 1 {
				t.Fatalf("%v fail-opens, want 1", n)
			}
		})
	}
}

// An over-long model changes the pool and nothing else: a caller's non-zero
// priority still leaves a body bound for a pool without priority scheduling.
func TestOverLongModelStillHasItsPriorityHandled(t *testing.T) {
	long := strings.Repeat("a", config.MaxModelNameBytes+1)
	f := newFixture(t, "", Options{})
	f.post(t, PathCompletions, []byte(`{"model":"`+long+`","priority":5,"prompt":"x","max_tokens":1}`), nil)
	reqs := append(f.large0.Mock().Requests(), f.large1.Mock().Requests()...)
	want := `{"model":"` + long + `","prompt":"x","max_tokens":1}`
	if len(reqs) != 1 || string(reqs[0].Body) != want {
		t.Fatalf("upstream got %d requests; want one with the priority stripped, as for any body", len(reqs))
	}
}

// A mapped name reaches its pool in the longest spelling JSON has of it.
func TestMappedModelInItsLongestSpellingIsRouted(t *testing.T) {
	name := strings.Repeat("a", config.MaxModelNameBytes)
	f := newFixture(t, "", Options{})
	f.h.poolFor = (&config.Config{Models: map[string]string{name: "small"}, DefaultPool: "large"}).PoolForModel
	f.post(t, PathCompletions, []byte(`{"model":"`+escapedName(config.MaxModelNameBytes)+`","prompt":"x","max_tokens":1}`), nil)
	if count(f.small) != 1 {
		t.Fatalf("small got %d requests, want 1", count(f.small))
	}
}

// TestOverLongHeadersThroughTheGateway: over-long X-Priority and
// X-Template-Id headers change nothing the replica sees. The priority
// resolves as absent, and the template fails open as unknown, once.
func TestOverLongHeadersThroughTheGateway(t *testing.T) {
	longPriority := "interactive" + strings.Repeat("!", config.MaxPriorityHeaderBytes)
	longID := strings.Repeat("a", templates.MaxIDBytes+1)
	f := newTemplateFixture(t, 512, "", Options{}, func(*mockllm.Config) {})
	body := []byte(`{"model":"large-model",` + tmplMessages + `}`)
	hdr := map[string]string{HeaderPriority: longPriority, templates.HeaderTemplateID: longID, templates.HeaderTemplateVersion: "2"}
	if resp, got := f.post(t, PathChatCompletions, body, hdr); resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %s", resp.StatusCode, got)
	}
	reqs := append(f.large0.Mock().Requests(), f.large1.Mock().Requests()...)
	if len(reqs) != 1 || string(reqs[0].Body) != string(body) {
		t.Fatalf("upstream got %d requests, want one with the body unchanged", len(reqs))
	}
	if got := reqs[0].Header.Get(HeaderPriority); got != longPriority {
		t.Errorf("X-Priority did not reach the replica unchanged (%d bytes)", len(got))
	}
	if got := reqs[0].Header.Get(templates.HeaderTemplateID); got != longID {
		t.Errorf("X-Template-Id did not reach the replica unchanged (%d bytes)", len(got))
	}
	series := telemetry.MetricFailOpen + `{stage="compile",reason="` + templates.ReasonUnknownTemplate + `"}`
	if n := f.metric(t, series, 1); n != 1 {
		t.Errorf("%s = %v, want 1", series, n)
	}
}

// BenchmarkModelAtItsLimit times reading a body's model at and past the
// limits (docs/results). Sizes are literal, so a run before the bound
// compares like for like; other_member is the same body without a model to
// decode, what the read costs anyway.
func BenchmarkModelAtItsLimit(b *testing.B) {
	for _, size := range []int{256, 1 << 10, 1 << 20, 16 << 20} {
		model := []byte(`{"model":"` + strings.Repeat("a", size) + `"}`)
		other := []byte(`{"modek":"` + strings.Repeat("a", size) + `"}`)
		b.Run("model_"+strconv.Itoa(size), func(b *testing.B) { benchRead(b, model) })
		b.Run("other_member_"+strconv.Itoa(size), func(b *testing.B) { benchRead(b, other) })
	}
	spelled := []byte(`{"model":"` + escapedName(256) + `"}`)
	b.Run("model_256_escaped", func(b *testing.B) { benchRead(b, spelled) })
	array := []byte(`{"model":["` + strings.Repeat("a", 16<<20) + `"]}`)
	b.Run("model_array_16777216", func(b *testing.B) { benchRead(b, array) })
}

// hugeEscapedKey is a body with one key of size bytes, all of it escapes.
func hugeEscapedKey(size int) []byte {
	return []byte(`{"` + escapedName(size/6) + `":1,"model":"m"}`)
}

// shortEscapedKeys is a body of about size bytes of one-letter escaped keys,
// past the index's ceiling once size is over about 11 KiB.
func shortEscapedKeys(size int) []byte {
	return []byte(`{` + strings.Repeat(`"`+escapedName(1)+`":0,`, size/11) + `"model":"m"}`)
}

// wideEscapedKeys is a body of bodyindex.MaxMembers keys sharing size bytes,
// each too long to name anything the gateway reads.
func wideEscapedKeys(size int) []byte {
	key := escapedName(size / bodyindex.MaxMembers / 6)
	return []byte(`{` + strings.Repeat(`"`+key+`":0,`, bodyindex.MaxMembers-1) + `"model":"m"}`)
}

// TestEscapedKeysAreNotDecoded: a decode allocates, so an escaped key that is
// decoded shows up in bytes. Each shape is what a body can do through nginx.
func TestEscapedKeysAreNotDecoded(t *testing.T) {
	const size = 2 << 20
	for name, body := range map[string][]byte{
		"one huge key":          hugeEscapedKey(size),
		"short keys":            shortEscapedKeys(size),
		"as wide as it indexes": wideEscapedKeys(size),
	} {
		t.Run(name, func(t *testing.T) {
			info, ok := readBody(new(bodyindex.Index), body)
			if !ok || info.model != "m" {
				t.Fatalf("read %+v, ok %v", info, ok)
			}
			if got, limit := bytesPerRead(body), uint64(len(body)/64); got > limit {
				t.Fatalf("readBody allocated %d bytes on a %d-byte body, want under %d", got, len(body), limit)
			}
		})
	}
}

// An escape changes what a key costs, not what it means: every name the
// reader asks for is found in its longest spelling, on both of its paths.
func TestReadBodyEscapedKeysMeanWhatTheySay(t *testing.T) {
	long := bodyindextest.Longest
	members := `"` + long("model") + `":"m","` + long("stream") + `":true,"` + long("priority") + `":5,"` + long("max_completion_tokens") + `":1`
	for name, body := range map[string]string{"indexed": `{` + members + `}`, "past the ceiling": pastCeiling(members)} {
		t.Run(name, func(t *testing.T) {
			got, ok := readBody(new(bodyindex.Index), []byte(body))
			if !ok || got.model != "m" || !got.stream || got.priorities != 1 || !got.hasCap {
				t.Fatalf("read %+v, ok %v", got, ok)
			}
		})
	}
	got, _ := readBody(new(bodyindex.Index), []byte(`{"`+long("model")+`a":"m"}`))
	if got.model != "" {
		t.Fatalf("a key one byte past the longest spelling was read as model: %q", got.model)
	}
}

// The over-long header resolves as any invalid budget does: forwarded,
// skipped, and counted by the handler. Only the Atoi is spared.
func TestBudgetHeaderBound(t *testing.T) {
	if maxBudgetHeaderBytes != 32 {
		t.Errorf("maxBudgetHeaderBytes = %d: a change moves what is read, and the results file", maxBudgetHeaderBytes)
	}
	h := &Handler{tokenizer: new(contextbudget.Tokenizer)}
	budget := func(v string) (string, int) {
		r := httptest.NewRequest(http.MethodPost, PathChatCompletions, nil)
		r.Header.Set(HeaderContextBudget, v)
		return h.contextBudget(r)
	}
	atLimit := strings.Repeat("0", maxBudgetHeaderBytes-1) + "7"
	if header, limit := budget(atLimit); header != atLimit || limit != 7 {
		t.Errorf("at the limit: %q, %d", header, limit)
	}
	over := "0" + atLimit
	if header, limit := budget(over); header != over || limit != 0 {
		t.Errorf("one over: %q, %d; want the header back with no budget, which is invalid_budget", header, limit)
	}
}

// BenchmarkEscapedKeys times reading the bodies an escaped top-level key
// makes costly (docs/results). plain_key is the same size unescaped, what
// the read costs anyway.
func BenchmarkEscapedKeys(b *testing.B) {
	for _, size := range []int{1 << 20, 16 << 20} {
		plain := []byte(`{"` + strings.Repeat("a", size) + `":1,"model":"m"}`)
		s := strconv.Itoa(size)
		for name, body := range map[string][]byte{
			"plain_key_" + s: plain, "huge_key_" + s: hugeEscapedKey(size),
			"short_keys_" + s: shortEscapedKeys(size), "wide_keys_" + s: wideEscapedKeys(size),
		} {
			b.Run(name, func(b *testing.B) { benchRead(b, body) })
		}
	}
}

func benchRead(b *testing.B, body []byte) {
	b.ReportAllocs()
	var ix bodyindex.Index
	for b.Loop() {
		readBody(&ix, body)
	}
}
