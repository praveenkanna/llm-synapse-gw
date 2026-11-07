package affinity

import (
	"bytes"
	"encoding/binary"
	"math/bits"
)

// Work accounting. Every Extract call stops after preKeyBudget units of
// work, and after postKeyBudget more once a messages key is decided, so its
// cost is bounded whatever the body holds. Work is counted in quarters of a
// unit, so a price can be a fraction of a unit. Each price is the time of
// the step it pays for, measured (M1-22): a cost sweep over every string
// length and scalar in every position (BenchmarkExtractCostSweep) and the
// gated shapes (BenchmarkExtractCostPerUnit) were fitted per CPU to the
// count of each step, and the prices follow the AMD EPYC 7763, the core the
// budget was sized on, scaled so the calibrated shape
// (tools_1MB_before_messages_failopen) keeps its time. M1-28 raised the prices
// the runners showed were too low (ADR 0017) and took about 1% of the coverage
// that shape had; no price was lowered.
const (
	// unit is the number of quarters in a work unit.
	unit = 4
	// preKeyBudget bounds the work of one Extract call, and so the content
	// read before the key is decided (the limits in the package doc). The
	// costliest shapes spend all of it: about 28 µs on an Apple M1, about
	// 35 µs at p50 on an AMD EPYC 7763 GitHub runner. 40K units is the largest
	// budget whose every BenchmarkExtractBudgetSweep run stayed ≤ 100 µs p99
	// on that core (AC-M1-5); 44K and above went over (M1-21). It was 64K.
	preKeyBudget = 40 << 10 * unit
	// postKeyBudget caps the work spent after a messages key is decided,
	// looking for a duplicate "messages" or "prompt" member, which fails
	// open (M1-25). Past this cap the key found is used: the normal SDK
	// order (messages, then tools) and long conversations no longer pay the
	// whole budget.
	postKeyBudget = 8 << 10 * unit

	// Prices, in quarters. stringQ is charged per string; a string that
	// closes within its first inline window adds shortStringQ[n] for its n
	// bytes. Longer strings pay inlineByteQ per byte of that window and of
	// any escape window, indexQ per bytes.IndexByte call, a unit per 32
	// bytes it jumps (skipShift) and backslashQ per byte of a backslash run.
	stringQ     = 4 * unit
	inlineByteQ = unit
	indexQ      = 13 * unit
	skipShift   = 3
	backslashQ  = unit
	// structQ is charged per structural byte skipDepth skips (punctuation,
	// digits, literals inside a skipped value) and bracketQ per bracket
	// byte, which also tracks depth. runQ is charged per skipRun that reads
	// past shortRun: a fixed 50-60 quarters of time, the same at 9 and at 24
	// bytes (M1-28), and nothing to a long run, which is one call a segment.
	structQ  = 3
	bracketQ = 6
	runQ     = 15 * unit
	// scalarQ is charged per number or literal skipped by skipScalar, plus
	// scalarByteQ per byte: the call and delimiter test cost about 7 units.
	scalarQ     = 7 * unit
	scalarByteQ = unit
	// memberQ is charged per object member walked (walkObject: the key
	// match, the visit and the separators) and elementQ per messages
	// element (readMessages), on top of their strings and values. 18 units
	// left short escaped keys at 1.16-1.17x on the EPYC 7763, the one shape
	// over the limit that no string price reaches (M1-28).
	memberQ  = 20 * unit
	elementQ = 16 * unit
	// keyDecodeQ is charged per byte of a key compared by keyIs when it may
	// need decoding (longer than the key wanted, but no longer than its
	// longest escaping): the backslash search and the in-place decode.
	keyDecodeQ = 2 * unit
	// hashByteQ is charged per hashed byte (a quarter of a unit), spaceQ per
	// whitespace byte.
	hashByteQ = 1
	spaceQ    = unit
	// shortString is how many bytes of a string are scanned inline before
	// switching to bytes.IndexByte; most keys and enum values are shorter.
	shortString = 16
	// maxWindow caps the inline window in text dense with escaped quotes.
	maxWindow = 256
)

// skipDepth bounds a segment with a shift by 3, which is safe only while no
// byte costs more than 8 quarters.
const _ = uint(8 - bracketQ)

// shortStringQ[n] is charged, on top of stringQ, for a string whose n
// content bytes close within its first inline window: 12 quarters and 2.5
// per byte, rounded up, with two floors. The time is a step, not a slope:
// skipString reads eight bytes at a time, and every CPU whose sweep data is
// still available (EPYC 7763, EPYC 9V74, Xeon 6973P) takes about the same time
// for any array element of 0-7 bytes and about a fifth more for one of 8-15
// (49 and 59-61 quarters on the two EPYCs). So the floor is 28 quarters
// to 6 bytes (empty to 4-byte strings were at 1.2-1.65x without it, run
// 38496867019) and 37 from 8 bytes, where the second word starts (8 bytes were
// at 1.15-1.20x without it, M1-28). The longest ones stay above the step:
// nothing has shown they are safe to lower on the Cascade Lake pods.
var shortStringQ = [shortString]int{28, 28, 28, 28, 28, 28, 28, 30, 37, 37, 37, 40, 42, 45, 47, 50}

// scanErr says why a scan stopped early.
type scanErr uint8

const (
	scanOK        scanErr = iota
	scanSyntax            // the bytes read are not well-formed JSON
	scanBudget            // the work limit was spent before the scan finished
	scanDuplicate         // a second top-level "messages" or "prompt" member (M1-25)
)

// scanner walks JSON bytes without decoding or copying them. It is not a
// validator: it checks only the structure it needs to find values.
type scanner struct {
	b     []byte
	work  int
	limit int // work allowed: the pre-key budget, lowered by keyDecided
}

func (s *scanner) spent() bool { return s.work > s.limit }

// left is the work still available, never negative.
func (s *scanner) left() int { return max(s.limit-s.work, 0) }

// exhaust spends the rest of the work allowed.
func (s *scanner) exhaust() { s.work = s.limit + 1 }

// keyDecided lowers the limit to postKeyBudget past the work done so far,
// never raising it: a messages key is decided and only a duplicate found
// within the cap could change the outcome (it fails open).
func (s *scanner) keyDecided() {
	s.limit = min(s.limit, s.work+postKeyBudget)
}

// fail classifies a scan that hit unexpected bytes or the end of the body:
// a spent budget explains it better than a syntax error.
func (s *scanner) fail() scanErr {
	if s.spent() {
		return scanBudget
	}
	return scanSyntax
}

func isSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }

// skipSpace returns the index of the first non-whitespace byte at or after i,
// stopping early (with the budget spent) on an absurdly long whitespace run.
// A caller about to read a value checks s.spent() first: an early stop leaves
// it on whitespace, and a value read from there would depend on the budget
// (bug #1397).
func (s *scanner) skipSpace(i int) int {
	b := s.b
	start := i
	lim := min(len(b), i+s.left()/spaceQ+1)
	for i < lim && isSpace(b[i]) {
		i++
	}
	s.work += (i - start) * spaceQ
	return i
}

// skipString returns the index just past the string starting at b[i] == '"'.
// It scans shortString bytes inline (cheap for keys, enum values and text
// dense with escapes), then jumps to the next quote with bytes.IndexByte.
func (s *scanner) skipString(i int) (int, scanErr) {
	b := s.b
	open := i
	s.work += stringQ
	i++
	window := shortString
	for {
		start := i
		end := min(len(b), i+window)
		for i < end {
			// Eight bytes at a time: every quote and backslash in the word
			// is found at once, and the next word does not wait on them, so
			// text dense with escapes costs a load per 8 bytes, and the cost
			// does not depend on where the compiler places this loop.
			if i+8 <= end {
				next, esc := i+8, i // esc: the first byte not escaped
				for m := quoteOrBackslash(binary.LittleEndian.Uint64(b[i:])); m != 0; m &= m - 1 {
					p := i + bits.TrailingZeros64(m)>>3
					if p < esc {
						continue // escaped by the backslash just before it
					}
					if b[p] == '"' {
						if start == open+1 { // closed within the first window
							s.work += shortStringQ[p-start]
						} else {
							s.work += (p - start) * inlineByteQ
						}
						return p + 1, scanOK
					}
					esc = p + 2 // a backslash: the byte after it is escaped
					next = max(next, esc)
				}
				i = next
				continue
			}
			switch b[i] {
			case '"':
				if start == open+1 {
					s.work += shortStringQ[i-start]
				} else {
					s.work += (i - start) * inlineByteQ
				}
				return i + 1, scanOK
			case '\\':
				i += 2
			default:
				i++
			}
		}
		s.work += (i - start) * inlineByteQ
		if i >= len(b) || s.spent() {
			return len(b), s.fail()
		}
		// Search only as far as the budget can pay for, so one huge string
		// (an inline image, an unterminated value) stays bounded too.
		lim := min(len(b), i+(s.left()+1)<<skipShift)
		j := bytes.IndexByte(b[i:lim], '"')
		if j < 0 {
			s.work += indexQ + (lim-i)>>skipShift
			if lim < len(b) {
				s.exhaust()
				return len(b), scanBudget
			}
			return len(b), scanSyntax
		}
		j += i
		s.work += indexQ + (j-i)>>skipShift
		// The quote is escaped when an odd run of backslashes precedes it.
		// The run never crosses the opening quote, and is bounded by the
		// budget so a megabyte of backslashes cannot stall the scan.
		k := j
		lo := max(open+1, j-s.left()/backslashQ-1)
		for k > lo && b[k-1] == '\\' {
			k--
		}
		s.work += (j - k) * backslashQ
		if k == lo && k > open+1 && b[k-1] == '\\' {
			s.exhaust()
			return len(b), scanBudget
		}
		if (j-k)%2 == 0 {
			return j + 1, scanOK
		}
		// An escaped quote right after the last search means text dense
		// with escapes (JSON examples): scan a growing window of it inline
		// instead of paying for a bytes.IndexByte call per quote. An
		// isolated escaped quote (prose) goes straight back to IndexByte.
		if j-i < shortString {
			window = min(max(window*2, shortString), maxWindow)
		} else {
			window = 0
		}
		i = j + 1
	}
}

const (
	lowBytes  = 0x0101010101010101
	highBytes = 0x8080808080808080
)

// quoteOrBackslash returns a mask with the high bit set in every byte of w
// that is a quote or a backslash, exactly (no borrow between bytes).
func quoteOrBackslash(w uint64) uint64 {
	return zeroBytes(w^(lowBytes*'"')) | zeroBytes(w^(lowBytes*'\\'))
}

// zeroBytes returns a mask with the high bit set in every zero byte of v.
func zeroBytes(v uint64) uint64 {
	const low7 = 0x7f7f7f7f7f7f7f7f
	return ^((v&low7 + low7) | v | low7)
}

// skipValue returns the index just past the JSON value starting at b[i].
func (s *scanner) skipValue(i int) (int, scanErr) {
	if i >= len(s.b) {
		return i, s.fail()
	}
	switch s.b[i] {
	case '"':
		return s.skipString(i)
	case '{', '[':
		return s.skipContainer(i)
	default:
		return s.skipScalar(i)
	}
}

// skipContainer returns the index just past the object or array at b[i]. It
// tracks depth only; it does not check that brackets pair by type.
func (s *scanner) skipContainer(i int) (int, scanErr) { return s.skipDepth(i, 0) }

// shortRun is how many bytes skipDepth reads one by one before it hands a
// stretch of structural bytes to skipRun: a stretch of up to 8 stays here, with
// its closing quote as the ninth. 4 sent "strict":true, to skipRun, whose call
// costs more than the bytes it saves, and the calibrated shape has stretches of
// 6 and 7 bytes (M1-28).
const shortRun = 9

// Byte classes for skipDepth: everything else is skipped with one compare.
const (
	clsOther uint8 = iota
	clsOpen
	clsClose
	clsQuote
)

var byteClass = [256]uint8{'{': clsOpen, '[': clsOpen, '}': clsClose, ']': clsClose, '"': clsQuote}

// kDepth[d] has byte j equal to (j+1-d) mod 256: with the running sums of a
// word's bracket steps (see skipRun) it finds where the depth is zero.
var kDepth = func() (k [9]uint64) {
	for d := range k {
		for j := range 8 {
			k[d] |= uint64(uint8(j+1-d)) << (8 * j)
		}
	}
	return k
}()

// How a stretch of structural bytes stopped (skipRun).
const (
	stopEnd    uint8 = iota // the segment ran out
	stopQuote               // seg[k] is a quote
	stopClosed              // seg[k] is the bracket that brought the depth to zero
)

// skipRun finishes a stretch of structural bytes that skipDepth's prefix read
// to its end: k bytes from b[i], with the depth and brackets so far and nothing
// charged yet. It reads eight bytes at a time, and a word that holds a quote
// ends the stretch there, without going back to single bytes; only a bracket
// that may close the value goes byte by byte. It charges the stretch and
// returns the index to go on from, the depth and how the stretch stopped: at a
// quote, at the end of the segment, or past the bracket that closed the value
// (stopClosed).
//
// The segment is short enough that its bytes cannot overspend even if every one
// is a bracket (at most bracketQ <= 8 quarters a byte); a long run is read in
// several segments. runQ can take the work past the limit, by at most its 15
// units: skipDepth's loop sees the budget spent at its next turn, and the bound
// tests allow it within workSlack. The bound is a shift, not a division by bracketQ: the division's
// register pressure spilled this loop on amd64 and slowed it by a third on an
// EPYC 7763.
//
// The parameters are in this order for skipDepth's byte loop: the compiler
// lines the loop's registers up with them, and with i first it shuffled four
// registers on every byte (M1-28). Check the amd64 code if this changes.
func (s *scanner) skipRun(k, i, depth, brackets int) (int, int, uint8) {
	seg := s.b[i:min(len(s.b), i+s.left()>>3+1)]
	stop := stopEnd
	run := 0
	if k < len(seg) {
		run = runQ
	}
words:
	for k+8 <= len(seg) && depth >= 0 {
		w := binary.LittleEndian.Uint64(seg[k:])
		quote := zeroBytes(w ^ (lowBytes * '"'))
		// Clearing bit 5 folds '{' onto '[' and '}' onto ']' and nothing else.
		u := w &^ (lowBytes * 0x20)
		open, closing := zeroBytes(u^(lowBytes*'[')), zeroBytes(u^(lowBytes*']'))
		n := 8
		if quote != 0 {
			// Only the bytes before the first quote count: below ends just
			// under that quote's top bit, and the masks hold only top bits.
			below := quote&-quote - 1
			open, closing = open&below, closing&below
			n = bits.TrailingZeros64(quote) >> 3
		}
		// The depth reaches zero inside this word only on a closing bracket
		// whose running sum puts it there. Per byte, a step is 2 for an open,
		// 0 for a close and 1 otherwise; multiplying by 0x0101.. makes byte j
		// the sum of steps 0..j, which is the depth change plus j+1. It
		// equals (j+1)-depth exactly where the depth is zero. A depth above 8
		// cannot reach zero within eight bytes.
		if closing != 0 && depth <= 8 && zeroBytes((lowBytes+open>>7-closing>>7)*lowBytes^kDepth[depth])&closing != 0 {
			break // the byte steps below stop at that bracket
		}
		if open|closing != 0 {
			no, nc := bits.OnesCount64(open), bits.OnesCount64(closing)
			depth += no - nc
			brackets += no + nc
		}
		k += n
		if quote != 0 {
			stop = stopQuote
			break words
		}
	}
	if stop == stopEnd {
	bytes:
		for ; k < len(seg); k++ {
			switch seg[k] {
			case '[', '{':
				depth++
				brackets++
			case ']', '}':
				depth--
				brackets++
				if depth == 0 {
					stop = stopClosed
					k++ // the closing bracket is read too
					break bytes
				}
			case '"':
				stop = stopQuote
				break bytes
			}
		}
	}
	s.work += run + k*structQ + brackets*(bracketQ-structQ)
	return i + k, depth, stop
}

// skipDepth scans from b[i], already depth containers deep, and returns the
// index just past the bracket that brings the depth back to zero.
//
// Most stretches of structural bytes between strings are a byte or two, and
// are read one by one as they always were. A stretch longer than shortRun goes
// to skipRun, which reads it eight bytes at a time, so its time does not depend
// on the order of the bytes. The first version switched on each byte's class
// alone: on a pattern such as "[],[],..." the outcome changes every byte, and
// on the Intel Xeon Cascade Lake pods the same loop took 150 us in one binary
// and 60 us in another, same bytes and core (M1-24, ADR 0017).
//
// Nothing the byte loop uses may stay live across the call to skipRun. When
// the call took the segment and returned the loop's own counters, the compiler
// kept k and brackets in stack slots, stored and reloaded per byte, and every
// string in a skipped value cost about 1.9 ns more on an EPYC 7763: the
// calibrated shape took 41 us instead of 35 (M1-28, ADR 0017).
func (s *scanner) skipDepth(i, depth int) (int, scanErr) {
	b := s.b
	for {
		// Every string costs work, so a run of them ends here once the
		// budget is spent, even strings with nothing between them.
		if s.spent() {
			return i, scanBudget
		}
		// The start of the structural bytes up to the next string, no further
		// than skipRun's segment. The loop bound is the prefix's own length,
		// so the compiler can drop the bounds check on each byte.
		pre := b[i:min(len(b), i+s.left()>>3+1, i+shortRun)]
		k, brackets := 0, 0
	run:
		for ; k < len(pre); k++ {
			cls := byteClass[pre[k]]
			if cls == clsOther {
				continue
			}
			switch cls {
			case clsOpen:
				depth++
				brackets++
			case clsClose:
				depth--
				brackets++
				if depth == 0 {
					s.work += (k+1)*structQ + brackets*(bracketQ-structQ)
					return i + k + 1, scanOK
				}
			default:
				break run
			}
		}
		if k == len(pre) {
			var stop uint8
			if i, depth, stop = s.skipRun(k, i, depth, brackets); stop == stopClosed {
				return i, scanOK
			}
			if stop == stopEnd {
				if i >= len(b) {
					return i, s.fail()
				}
				continue
			}
		} else {
			s.work += k*structQ + brackets*(bracketQ-structQ)
			i += k
		}
		var err scanErr
		if i, err = s.skipString(i); err != scanOK {
			return i, err
		}
		// Two strings with nothing between them are never valid JSON (vLLM
		// rejects the body), and a run of them costs more time per unit
		// than any valid shape: stop here and fail open as unparseable.
		if i < len(b) && b[i] == '"' {
			return i, scanSyntax
		}
	}
}

// skipScalar returns the index just past a number or literal at b[i].
func (s *scanner) skipScalar(i int) (int, scanErr) {
	b := s.b
	start := i
	lim := min(len(b), i+s.left()/scalarByteQ+1)
	for i < lim && !isDelim(b[i]) {
		i++
	}
	s.work += scalarQ + (i-start)*scalarByteQ
	if i == start || s.spent() {
		return i, s.fail()
	}
	return i, scanOK
}

func isDelim(c byte) bool {
	switch c {
	case ',', '}', ']', ':', '"', '{', '[', ' ', '\t', '\n', '\r':
		return true
	}
	return false
}

// walkObject walks the members of the object at b[i] == '{'. visit gets each
// raw (quoted) key and the index of its value, and returns the index just
// past that value. walkObject returns the index just past the closing '}'.
func (s *scanner) walkObject(i int, visit func(key []byte, val int) (int, scanErr)) (int, scanErr) {
	b := s.b
	i = s.skipSpace(i + 1)
	if i < len(b) && b[i] == '}' {
		return i + 1, scanOK
	}
	for {
		// Charge each member up front and stop once the budget is spent:
		// a member made of short tokens must not be walked for free.
		s.work += memberQ
		if s.spent() {
			return i, scanBudget
		}
		if i >= len(b) || b[i] != '"' {
			return i, s.fail()
		}
		keyStart := i
		var err scanErr
		if i, err = s.skipString(i); err != scanOK {
			return i, err
		}
		key := b[keyStart:i]
		i = s.skipSpace(i)
		if i >= len(b) || b[i] != ':' {
			return i, s.fail()
		}
		val := s.skipSpace(i + 1)
		if s.spent() {
			return val, scanBudget
		}
		if i, err = visit(key, val); err != scanOK {
			return i, err
		}
		i = s.skipSpace(i)
		if i >= len(b) {
			return i, s.fail()
		}
		switch b[i] {
		case ',':
			i = s.skipSpace(i + 1)
		case '}':
			return i + 1, scanOK
		default:
			return i, s.fail()
		}
	}
}

// keyIs is isKey charged for its work: a key that may need decoding (longer
// than want but no longer than its longest escaping) is searched for a
// backslash and decoded, at keyDecodeQ per byte.
func (s *scanner) keyIs(raw, want []byte) bool {
	if n := len(raw) - 2; n > len(want) && n <= 6*len(want) {
		s.work += n * keyDecodeQ
	}
	return isKey(raw, want)
}

// isKey reports whether the quoted JSON string raw decodes to want, which
// is ASCII. Escaped strings (e.g. "m\u0065ssages") are rare and decoded in
// place, without allocating; anything longer than the longest escaping of
// want cannot match and is rejected without decoding.
func isKey(raw, want []byte) bool {
	if len(raw) < 2 || raw[0] != '"' || raw[len(raw)-1] != '"' {
		return false
	}
	// An escape is at least two bytes for one, so a key no longer than want
	// matches only byte for byte, and most keys are decided by length.
	inner := raw[1 : len(raw)-1]
	switch {
	case len(inner) < len(want):
		return false
	case len(inner) == len(want):
		return bytes.Equal(inner, want)
	case len(inner) > 6*len(want): // every byte as \u00XX at most
		return false
	case bytes.IndexByte(inner, '\\') < 0:
		return false
	}
	n := 0
	for i := 0; i < len(inner); i++ {
		c := inner[i]
		if c == '\\' {
			var ok bool
			if c, i, ok = unescape(inner, i); !ok {
				return false
			}
		}
		if n >= len(want) || want[n] != c {
			return false
		}
		n++
	}
	return n == len(want)
}

// unescape decodes the JSON escape at s[i] == '\\' to one ASCII byte and
// returns it with the index of the escape's last byte. ok is false for a
// malformed escape or one that decodes to a non-ASCII rune, which no ASCII
// key can equal.
func unescape(s []byte, i int) (c byte, last int, ok bool) {
	if i+1 >= len(s) {
		return 0, i, false
	}
	switch s[i+1] {
	case '"', '\\', '/':
		return s[i+1], i + 1, true
	case 'b':
		return '\b', i + 1, true
	case 'f':
		return '\f', i + 1, true
	case 'n':
		return '\n', i + 1, true
	case 'r':
		return '\r', i + 1, true
	case 't':
		return '\t', i + 1, true
	case 'u':
		if i+6 > len(s) {
			return 0, i, false
		}
		r := 0
		for _, h := range s[i+2 : i+6] {
			switch {
			case h >= '0' && h <= '9':
				r = r<<4 | int(h-'0')
			case h >= 'a' && h <= 'f':
				r = r<<4 | int(h-'a'+10)
			case h >= 'A' && h <= 'F':
				r = r<<4 | int(h-'A'+10)
			default:
				return 0, i, false
			}
		}
		if r >= 0x80 {
			return 0, i, false
		}
		return byte(r), i + 5, true
	}
	return 0, i, false
}
