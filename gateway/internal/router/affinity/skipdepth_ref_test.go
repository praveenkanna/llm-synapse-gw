package affinity

import (
	"math/rand"
	"testing"
)

// skipDepthRef is skipDepth as it was before M1-24: a switch on each byte's
// class. skipDepth now reads a long stretch by words, to avoid a branch per
// byte; this copy is the behaviour it must keep. Its one addition is runQ, the
// price of that path (M1-28): a segment past the byte loop's shortRun bytes
// whose stretch reached them pays it once, whatever the path taken.
const (
	refOther uint8 = iota
	refOpen
	refClose
	refQuote
)

var refClass = [256]uint8{'{': refOpen, '[': refOpen, '}': refClose, ']': refClose, '"': refQuote}

func (s *scanner) skipDepthRef(i, depth int) (int, scanErr) {
	b := s.b
	for {
		if s.spent() {
			return i, scanBudget
		}
		seg := b[i:min(len(b), i+s.left()>>3+1)]
		k, brackets := 0, 0
	run:
		for ; k < len(seg); k++ {
			cls := refClass[seg[k]]
			if cls == refOther {
				continue
			}
			switch cls {
			case refOpen:
				depth++
				brackets++
			case refClose:
				depth--
				brackets++
				if depth == 0 {
					s.work += refRun(k, seg) + (k+1)*structQ + brackets*(bracketQ-structQ)
					return i + k + 1, scanOK
				}
			default:
				break run
			}
		}
		s.work += refRun(k, seg) + k*structQ + brackets*(bracketQ-structQ)
		i += k
		if k == len(seg) {
			if i >= len(b) {
				return i, s.fail()
			}
			continue
		}
		var err scanErr
		if i, err = s.skipString(i); err != scanOK {
			return i, err
		}
		if i < len(b) && b[i] == '"' {
			return i, scanSyntax
		}
	}
}

// refRun is runQ when a stretch that stopped at seg[k] went past the byte loop.
func refRun(k int, seg []byte) int {
	if k >= shortRun && shortRun < len(seg) {
		return runQ
	}
	return 0
}

func TestSkipDepthMatchesReference(t *testing.T) {
	check := func(t *testing.T, b []byte, i, depth, work, limit int) {
		t.Helper()
		got := scanner{b: b, work: work, limit: limit}
		want := got
		gi, gerr := got.skipDepth(i, depth)
		wi, werr := want.skipDepthRef(i, depth)
		if gi != wi || gerr != werr || got.work != want.work {
			t.Fatalf("skipDepth(%q, i=%d, depth=%d, work=%d, limit=%d) = (%d, %v, work %d), reference (%d, %v, work %d)",
				b, i, depth, work, limit, gi, gerr, got.work, wi, werr, want.work)
		}
	}

	t.Run("fixed", func(t *testing.T) {
		for _, tc := range []struct {
			name        string
			body        string
			i, depth    int
			work, limit int
		}{
			{"empty array", `[]`, 0, 0, 0, 1 << 16},
			{"nested", `[[{}],[]]x`, 0, 0, 0, 1 << 16},
			{"close first", `]]][`, 0, 0, 0, 1 << 16},
			{"close then open", `][]`, 0, 0, 0, 1 << 16},
			{"depth one", `]x`, 0, 1, 0, 1 << 16},
			{"string inside", `["a","b"]`, 0, 0, 0, 1 << 16},
			{"two strings in a row", `[""""]`, 0, 0, 0, 1 << 16},
			{"commas and colons", `{"a":[1,2,{"b":[]}]}`, 0, 0, 0, 1 << 16},
			{"unterminated", `[[[[`, 0, 0, 0, 1 << 16},
			{"starts past the end", `[]`, 2, 0, 0, 1 << 16},
			{"budget spent mid-run", `[],[],[],[],[],[],[],[],`, 0, 0, 0, 40},
			{"budget already spent", `[]`, 0, 0, 100, 10},
			{"skips a leading comma at depth zero", `,[]`, 0, 0, 0, 1 << 16},
		} {
			t.Run(tc.name, func(t *testing.T) { check(t, []byte(tc.body), tc.i, tc.depth, tc.work, tc.limit) })
		}
	})

	// Inputs that keep skipDepth on its eight-bytes-at-a-time path: no quotes
	// (a quote sends a word to the byte loop), mostly brackets, at every
	// alignment and for depths from below zero to above the word window.
	t.Run("bracket heavy without quotes", func(t *testing.T) {
		alphabet := []byte("[]{}[]{}[]{},,:a1 ")
		rng := rand.New(rand.NewSource(7))
		for n := 0; n < 200000; n++ {
			b := make([]byte, rng.Intn(300))
			for k := range b {
				b[k] = alphabet[rng.Intn(len(alphabet))]
			}
			check(t, b, rng.Intn(len(b)+1), rng.Intn(16)-2, rng.Intn(50), 600+rng.Intn(1<<17))
		}
	})

	t.Run("repeated patterns at every alignment", func(t *testing.T) {
		for _, pat := range []string{"[],", "{},", "[[]]", "[{}]", "],[", "[]", "{}", "][", "]]]]]]]]", "[[[[[[[[", "[],[],[],[],[],[],[],[],]"} {
			for lead := 0; lead < 9; lead++ {
				for reps := 0; reps < 40; reps++ {
					var b []byte
					for k := 0; k < lead; k++ {
						b = append(b, ',')
					}
					for k := 0; k < reps; k++ {
						b = append(b, pat...)
					}
					for _, tail := range []string{"", "]", "]]", `"x"`, "x"} {
						bb := append(append([]byte(nil), b...), tail...)
						for depth := -1; depth <= 11; depth++ {
							check(t, bb, 0, depth, 0, 1<<20)
						}
					}
				}
			}
		}
	})

	t.Run("random", func(t *testing.T) {
		alphabet := []byte(`[]{}"",:a1 \`)
		rng := rand.New(rand.NewSource(20261018))
		for n := 0; n < 300000; n++ {
			b := make([]byte, rng.Intn(80))
			for k := range b {
				b[k] = alphabet[rng.Intn(len(alphabet))]
			}
			i := 0
			if len(b) > 0 && rng.Intn(4) == 0 {
				i = rng.Intn(len(b) + 1)
			}
			check(t, b, i, rng.Intn(6)-2, rng.Intn(40), 8+rng.Intn(600))
		}
	})
}
