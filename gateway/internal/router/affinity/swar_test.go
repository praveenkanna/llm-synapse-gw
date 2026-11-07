package affinity_test

import (
	"encoding/binary"
	"math/rand/v2"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/affinity"
)

// TestQuoteOrBackslash: the 8-byte mask flags exactly the bytes of the
// word that are a quote or a backslash, for crafted words and random ones
// dense in quotes, backslashes and the bytes next to them (which a
// borrowing zero-byte test would flag by mistake).
func TestQuoteOrBackslash(t *testing.T) {
	alphabet := []byte{'"', 92, 0, 1, 0x21, 0x23, 0x5b, 0x5d, 0x5c ^ 0x80, 0x22 ^ 0x80, 0xff, 'a', 'z'}
	r := rand.New(rand.NewPCG(1, 2))
	check := func(word []byte) {
		var want uint64
		for i, c := range word {
			if c == '"' || c == 92 {
				want |= 0x80 << (8 * i)
			}
		}
		if got := affinity.QuoteOrBackslash(binary.LittleEndian.Uint64(word)); got != want {
			t.Fatalf("word %q: mask %#x, want %#x", word, got, want)
		}
	}
	for pos := range 8 {
		for _, c := range []byte{'"', 92} {
			w := []byte("abcdefgh")
			w[pos] = c
			check(w)
		}
	}
	for range 200_000 {
		w := make([]byte, 8)
		for i := range w {
			w[i] = alphabet[r.IntN(len(alphabet))]
		}
		check(w)
	}
}

// TestSkipStringMatchesBytewise: the word-at-a-time string scan ends every
// string where a byte-by-byte reference does (a backslash escapes the next
// byte, an unescaped quote closes the string), for random strings dense in
// quotes and backslashes, of every length up to a few windows, at every
// alignment.
func TestSkipStringMatchesBytewise(t *testing.T) {
	ref := func(b []byte, i int) (int, bool) {
		for j := i + 1; j < len(b); j++ {
			switch b[j] {
			case 92:
				j++
			case '"':
				return j + 1, true
			}
		}
		return len(b), false
	}
	alphabet := []byte{'a', 'b', '"', 92, 92, ' ', 'n', 0x23, 0x5d}
	r := rand.New(rand.NewPCG(3, 4))
	for range 200_000 {
		n := r.IntN(80)
		b := make([]byte, n+1)
		b[0] = '"'
		for i := 1; i <= n; i++ {
			b[i] = alphabet[r.IntN(len(alphabet))]
		}
		wantEnd, wantOK := ref(b, 0)
		gotEnd, gotOK := affinity.SkipStringAll(b, 0)
		if gotOK != wantOK || (wantOK && gotEnd != wantEnd) {
			t.Fatalf("string %q: end %d ok %v, reference %d ok %v", b, gotEnd, gotOK, wantEnd, wantOK)
		}
	}
}
