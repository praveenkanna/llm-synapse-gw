package bodyindex_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex/bodyindextest"
)

// readerNames are the names the gateway's readers ask an index about, and two
// that are not ASCII.
var readerNames = []string{"model", "stream", "priority", "max_tokens", "max_completion_tokens",
	"messages", "tools", "documents", "reasoning_effort", "role", "content", "type", "", "a", "café", "�"}

// sameMeaning fails t unless Is and KeyIs answer for m's key as its decoded
// name decides. name is what the reference reads it as.
func sameMeaning(t *testing.T, x *bodyindex.Index, m bodyindex.Member, name string, body []byte) {
	t.Helper()
	key := body[m.Key[0]:m.Key[1]]
	check := func(asked string) {
		t.Helper()
		want := asked == name
		if got := x.Is(m, asked); got != want {
			t.Fatalf("Is(%q) = %v for key %q, want %v, body %q", asked, got, key, want, body)
		}
		if got := bodyindex.KeyIs(key, asked); got != want {
			t.Fatalf("KeyIs(%q) = %v for key %q, want %v, body %q", asked, got, key, want, body)
		}
	}
	for _, asked := range readerNames {
		check(asked)
	}
	check(name)
	check(name + "x")
}

func readOne(t *testing.T, key string) (*bodyindex.Index, bodyindex.Member, []byte) {
	t.Helper()
	body := []byte(`{"` + key + `":1}`)
	var x bodyindex.Index
	if err := x.Read(body); err != nil {
		t.Fatalf("Read(%q): %v", body, err)
	}
	return &x, x.Member(0), body
}

func TestEscapedKeyMeaning(t *testing.T) {
	b := string(rune(0x5c)) // one backslash
	tests := []struct {
		name string
		key  string
		is   string
		want bool
	}{
		{name: "the longest spelling", key: bodyindextest.Longest("model"), is: "model", want: true},
		{name: "upper-case hex", key: b + "u006D" + b + "u006F" + "del", is: "model", want: true},
		{name: "one escape", key: "m" + b + "u006fdel", is: "model", want: true},
		{name: "the longest spelling of the longest name", key: bodyindextest.Longest("max_completion_tokens"), is: "max_completion_tokens", want: true},
		{name: "one byte past the longest spelling", key: bodyindextest.Longest("model") + "a", is: "model"},
		{name: "a prefix", key: bodyindextest.Longest("mode"), is: "model"},
		{name: "the name and more", key: bodyindextest.Longest("models"), is: "model"},
		{name: "the name, then more, within the length bound", key: "m" + b + "u006fdels", is: "model"},
		{name: "a short escape that is not the name", key: b + "n", is: "model"},
		{name: "short escapes", key: "a" + b + "tb", is: "a\tb", want: true},
		{name: "a quote", key: "a" + b + `"b`, is: `a"b`, want: true},
		{name: "a backslash", key: "a" + b + b + "b", is: "a" + b + "b", want: true},
		{name: "a slash", key: "a" + b + "/b", is: "a/b", want: true},
		{name: "every short escape", key: b + "b" + b + "f" + b + "n" + b + "r" + b + "t", is: "\b\f\n\r\t", want: true},
		{name: "NUL", key: "a" + b + "u0000b", is: "a\x00b", want: true},
		{name: "not the same as its spelling", key: b + "u006d", is: b + "u006d"},
		{name: "a non-ASCII name", key: "caf" + b + "u00e9", is: "café", want: true},
		{name: "a surrogate pair", key: b + "ud83d" + b + "ude00", is: "\U0001f600", want: true},
		{name: "a lone surrogate reads as U+FFFD", key: b + "ud83d", is: "�", want: true},
		{name: "non-ASCII never reads as ASCII", key: "a" + b + "u0100", is: "a\x00"},
		{name: "non-ASCII escaped against ASCII", key: b + "u00e9", is: "e"},
		{name: "raw non-ASCII beside an escape", key: "café" + b + "n", is: "café\n", want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			x, m, body := readOne(t, tc.key)
			if got := x.Is(m, tc.is); got != tc.want {
				t.Errorf("Is(%q) = %v for key %s, want %v", tc.is, got, tc.key, tc.want)
			}
			if got := bodyindex.KeyIs(body[m.Key[0]:m.Key[1]], tc.is); got != tc.want {
				t.Errorf("KeyIs(%q) = %v for key %s, want %v", tc.is, got, tc.key, tc.want)
			}
			var decoded string
			if err := json.Unmarshal(body[m.Key[0]:m.Key[1]], &decoded); err != nil {
				t.Fatal(err)
			}
			if (decoded == tc.is) != tc.want {
				t.Errorf("the table is wrong: the key decodes to %q", decoded)
			}
		})
	}
}

// keyAgrees fails t unless KeyIs answers for key, a JSON string with its
// quotes, as encoding/json's decode of it does, about each name and about
// what it decodes to.
func keyAgrees(t *testing.T, key string, names ...string) {
	t.Helper()
	var decoded string
	if err := json.Unmarshal([]byte(key), &decoded); err != nil {
		t.Fatalf("%s: %v", key, err)
	}
	for _, name := range append(names, decoded, decoded+"x") {
		if got, want := bodyindex.KeyIs([]byte(key), name), decoded == name; got != want {
			t.Fatalf("KeyIs(%s, %q) = %v; it decodes to %q", key, name, got, decoded)
		}
	}
}

// TestKeyIsEveryEscape: every Unicode escape, in both cases of hex, in place
// of the first byte of model and of priority, and every ASCII byte against
// itself and its other case, beside an escape so that the compare walks.
func TestKeyIsEveryEscape(t *testing.T) {
	const backslash = 0x5c
	for _, name := range []string{"model", "priority"} {
		for u := range 0x10000 {
			for _, format := range []string{"%cu%04x", "%cu%04X"} {
				keyAgrees(t, `"`+fmt.Sprintf(format, backslash, u)+name[1:]+`"`, name)
			}
		}
	}
	for c := range byte(0x80) {
		spelled := fmt.Sprintf("%cu%04x", backslash, c)
		if c >= 0x20 && c != '"' && c != backslash {
			spelled = string(rune(c))
		}
		twin := c
		switch {
		case c >= 'a' && c <= 'z':
			twin = c - 'a' + 'A'
		case c >= 'A' && c <= 'Z':
			twin = c - 'A' + 'a'
		}
		m := bodyindextest.Longest("m")
		keyAgrees(t, `"`+m+spelled+`del"`, "m"+string(rune(c))+"del", "m"+string(rune(twin))+"del", "model")
		keyAgrees(t, `"`+m+spelled+`"`, "m"+string(rune(c)), "m"+string(rune(twin)))
	}
}

// Find counts every spelling of a name, and only those.
func TestFindSeesEverySpelling(t *testing.T) {
	long := bodyindextest.Longest("model")
	body := []byte(fmt.Sprintf(`{"model":1,"%s":2,"%sa":3,"mode":4}`, long, long))
	var x bodyindex.Index
	if err := x.Read(body); err != nil {
		t.Fatal(err)
	}
	m, n := x.Find("model")
	if n != 2 || string(x.Value(m)) != "2" {
		t.Fatalf("Find = %s, %d; want the second, of 2", x.Value(m), n)
	}
}

func isCost(x *bodyindex.Index, m bodyindex.Member, name string, runs int) time.Duration {
	best := time.Hour
	for range runs {
		start := time.Now()
		x.Is(m, name)
		if d := time.Since(start); d < best {
			best = d
		}
	}
	return best
}

// TestIsCostStopsAtTheBound: a question about a key costs the same at 1 KiB
// as at 1 MiB. Each shape gets past what stops the others: a key that agrees
// with the name for as long as it can, then goes on, and a plain run ahead of
// its one escape, which a search for the escape walks. A decode, or that
// search, grows with the key.
func TestIsCostStopsAtTheBound(t *testing.T) {
	const maxRatio = 8
	const floor = 200 * time.Nanosecond // near a clock tick, a ratio means nothing
	b := string(rune(0x5c))             // one backslash
	long := func(n int) string { return strings.Repeat(bodyindextest.Longest("a"), n/6) }
	for _, tc := range []struct {
		name string
		is   string
		key  func(n int) string
	}{
		{"escapes", "model", long},
		{"plain then an escape", "model", func(n int) string { return strings.Repeat("a", n) + b + "n" }},
		{"agrees past ASCII", "caf\u00e9", func(n int) string { return "caf" + b + "u00e9" + long(n) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sx, sm, _ := readOne(t, tc.key(1<<10))
			hx, hm, _ := readOne(t, tc.key(1<<20))
			var base, huge time.Duration
			for range 5 {
				base, huge = isCost(sx, sm, tc.is, 100), isCost(hx, hm, tc.is, 100)
				if huge <= maxRatio*max(base, floor) {
					return
				}
			}
			t.Fatalf("Is took %v on a 1 MiB key, %v on 1 KiB", huge, base)
		})
	}
}
