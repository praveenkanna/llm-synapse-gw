package affinity

import (
	"bytes"
	"strings"
	"testing"
)

// The prices are fitted to measured time, and a price that drifts is a
// budget that no longer bounds the time, with nothing to show for it: the
// bench only sees it after the fact. So these tests use the numbers themselves,
// not the constants, and charge real bodies. A change here is a refit, and
// needs its evidence recorded in ADR 0017.
const (
	wantStringQ  = 16
	wantStructQ  = 3
	wantBracketQ = 6
	wantRunQ     = 60
	wantMemberQ  = 80
	wantElementQ = 64
	wantScalarQ  = 28
	wantByteQ    = 4
)

// wantShortStringQ is the fit of M1-22 and M1-28, in quarters per string
// length: the two floors at 0-6 and 8-10 bytes, then 12 and 2.5 a byte.
var wantShortStringQ = [shortString]int{28, 28, 28, 28, 28, 28, 28, 30, 37, 37, 37, 40, 42, 45, 47, 50}

// TestShortStringCharge: an array of strings of one length is charged the
// table's entry for each, and nothing else.
func TestShortStringCharge(t *testing.T) {
	for n := range shortString {
		item := `"` + strings.Repeat("a", n) + `",`
		body := []byte(`[` + strings.Repeat(item, 100) + `""]`)
		s := scanner{b: body, limit: 1 << 40}
		if _, err := s.skipDepth(0, 0); err != scanOK {
			t.Fatalf("n=%d: scan stopped with %v", n, err)
		}
		// 100 items at a string and a comma each, the closing string, and the
		// brackets around them.
		want := 100*(wantStringQ+wantShortStringQ[n]+wantStructQ) + wantStringQ + wantShortStringQ[0] + 2*wantBracketQ
		if s.work != want {
			t.Errorf("n=%d: charged %d quarters, want %d", n, s.work, want)
		}
	}
}

// TestMemberCharge: each member walked costs memberQ on top of its key and
// value, at the top level of the body and inside a leading message.
func TestMemberCharge(t *testing.T) {
	key := wantStringQ + wantShortStringQ[1]
	value := wantScalarQ + wantByteQ
	member := wantMemberQ + key + value
	work := func(n int) int {
		body := []byte(`{` + strings.Repeat(`"k":1,`, n) + `"z":1}`)
		s := scanner{b: body, limit: 1 << 40}
		if _, err := s.walkObject(0, func(_ []byte, val int) (int, scanErr) { return s.skipValue(val) }); err != scanOK {
			t.Fatalf("n=%d: scan stopped with %v", n, err)
		}
		return s.work
	}
	if got := work(11) - work(1); got != 10*member {
		t.Errorf("10 members charged %d quarters, want %d", got, 10*member)
	}
}

// TestElementCharge: each element of messages costs elementQ on top of its
// value, charged once the key is decided so only skipValue is left to pay.
func TestElementCharge(t *testing.T) {
	work := func(n int) int {
		body := []byte(`[{"role":"user","content":"Q"}` + strings.Repeat(`,1`, n) + `]`)
		s := scanner{b: body, limit: 1 << 40}
		if _, _, err := s.readMessages(0); err != scanOK {
			t.Fatalf("n=%d: scan stopped with %v", n, err)
		}
		return s.work
	}
	element := wantElementQ + wantScalarQ + wantByteQ
	if got := work(11) - work(1); got != 10*element {
		t.Errorf("10 elements charged %d quarters, want %d", got, 10*element)
	}
}

// TestRunCharge: a stretch of up to 8 bytes between strings stays in the byte
// loop, and one of 9 or more pays runQ once.
func TestRunCharge(t *testing.T) {
	for n := 1; n <= 40; n++ {
		item := `"",` + strings.Repeat("1", n-1)
		body := []byte(`[` + strings.Repeat(item, 50) + `""]`)
		s := scanner{b: body, limit: 1 << 40}
		if _, err := s.skipDepth(0, 0); err != scanOK {
			t.Fatalf("n=%d: scan stopped with %v", n, err)
		}
		run := 0
		if n > 8 {
			run = wantRunQ
		}
		empty := wantStringQ + wantShortStringQ[0]
		want := 50*(empty+n*wantStructQ+run) + empty + 2*wantBracketQ
		if s.work != want {
			t.Errorf("stretch of %d: charged %d quarters, want %d", n, s.work, want)
		}
	}
}

// TestLongRunThenStringMatchesReference covers skipRun's hand-back to the byte
// loop: a stretch past shortRun that ends at a quote, at every length around
// the prefix and the word boundaries, with brackets at every place in it, and
// one the budget cuts inside the prefix.
func TestLongRunThenStringMatchesReference(t *testing.T) {
	check := func(t *testing.T, b []byte, depth int) {
		t.Helper()
		for _, limit := range []int{1 << 16, 8, 12, 20, 24, 32, 40, 64, 100} {
			got := scanner{b: bytes.Clone(b), limit: limit}
			want := got
			gi, gerr := got.skipDepth(0, depth)
			wi, werr := want.skipDepthRef(0, depth)
			if gi != wi || gerr != werr || got.work != want.work {
				t.Fatalf("%q depth=%d limit=%d: skipDepth (%d, %v, work %d), reference (%d, %v, work %d)", b, depth, limit, gi, gerr, got.work, wi, werr, want.work)
			}
		}
	}
	for n := 0; n < 40; n++ {
		check(t, []byte(`[`+strings.Repeat("1", n)+`"x"]`), 0)
		for at := range n {
			for _, c := range []byte("[]{}") {
				b := []byte(`[` + strings.Repeat("1", n) + `"x"]]]`)
				b[1+at] = c
				for depth := -1; depth <= 10; depth++ {
					check(t, b, depth)
				}
			}
		}
	}
}
