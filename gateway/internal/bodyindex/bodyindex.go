// Package bodyindex reads a JSON object request body's top level once:
// its members in order, each with the byte ranges of its key and value,
// whether the key was written with an escape, and the kind of its value.
// Readers apply their own rules to the index (ADR 0019's key rules stay
// theirs); the splices the gateway makes (a priority, a max_tokens, a
// trimmed messages array) return the new body with its index, so no offset
// is kept valid by convention (AD-14).
//
// The index points into the body and never copies it. It holds up to
// inlineMembers members inside itself, so an Index declared in a function
// and passed down by pointer to calls that do not keep it stays on the
// stack: reading a body allocates nothing. A body with more than
// MaxMembers members is not indexed (ErrTooManyMembers), so its memory has
// a ceiling. Such a body is still valid JSON the replica serves: Walk reads
// it one member at a time, and the byte-level splices InsertFirst and
// Remove, which the Index's own splices are built on, edit it.
package bodyindex

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"strconv"
	"unicode/utf8"
)

const (
	// inlineMembers is how many members an Index holds without allocating.
	inlineMembers = 16
	// MaxMembers is the most top-level members a body may have to be
	// indexed. Request bodies have a few dozen at most.
	MaxMembers = 1 << 10
	// maxNestingDepth matches Go's encoding/json scanner depth ceiling.
	maxNestingDepth = 10000
)

// SWAR constants for 8-byte word scanning.
const (
	swarLo01 = 0x0101010101010101
	swarLo20 = 0x2020202020202020
	swarHi80 = 0x8080808080808080
	swarQt22 = 0x2222222222222222
	swarBs5c = 0x5c5c5c5c5c5c5c5c
)

// Errors Read reports.
var (
	// ErrNotObject: the body is not exactly one valid JSON object.
	ErrNotObject = errors.New("bodyindex: not a JSON object")
	// ErrTooManyMembers: the body is a valid JSON object of more than
	// MaxMembers members. Unlike a body that is not an object, the replica
	// would accept it, so a reader that cannot index it must not assume the
	// replica rejects it.
	ErrTooManyMembers = errors.New("bodyindex: more than MaxMembers top-level members")
)

// Kind is the kind of a member's value; Invalid is the zero Member's.
type Kind uint8

const (
	Invalid Kind = iota
	String
	Number
	Object
	Array
	Bool
	Null
)

// Member is one top-level member.
type Member struct {
	Key     [2]int // the key with its quotes
	Value   [2]int // the value
	Comma   int    // the ',' before the member, or -1 for the first
	Escaped bool   // the key was written with an escape
	Kind    Kind
}

// Index is the top level of a JSON object body. Copies share the members
// past inlineMembers, so a copy is valid until either it or the original
// is read or spliced into.
type Index struct {
	body        []byte
	open, close int
	n           int
	inline      [inlineMembers]Member
	extra       []Member // members past inlineMembers
	err         error    // why the last Read indexed no members
	validUTF8   bool     // whether body is valid UTF-8 (when Err != ErrNotObject)
}

// Read indexes body into x, validating RFC 8259 syntax and UTF-8 in a single
// SWAR-accelerated pass. It fails with ErrNotObject unless body is exactly one
// valid JSON object, and with ErrTooManyMembers when it has more than
// MaxMembers members. Either way x then holds body but no members, and Err
// says why; past the ceiling Open is still the object's '{' and Walk reads
// the members. x is reused: what it held before is dropped.
func (x *Index) Read(body []byte) error {
	x.reset(body)
	i := SkipSpace(body, 0)
	if i >= len(body) || body[i] != '{' {
		x.err = ErrNotObject
		return x.err
	}
	open := i
	validUTF8 := true
	i = SkipSpace(body, i+1)
	if i >= len(body) {
		x.err = ErrNotObject
		return x.err
	}
	if body[i] == '}' {
		if SkipSpace(body, i+1) != len(body) {
			x.err = ErrNotObject
			return x.err
		}
		x.open, x.close, x.validUTF8 = open, i, validUTF8
		return nil
	}
	comma := -1
	for {
		if body[i] != '"' {
			return x.failNotObject()
		}
		ks := i
		ke, escaped, vUTF8, ok := scanString(body, i, validUTF8)
		if !ok {
			return x.failNotObject()
		}
		validUTF8 = vUTF8
		colon := SkipSpace(body, ke)
		if colon >= len(body) || body[colon] != ':' {
			return x.failNotObject()
		}
		vs := SkipSpace(body, colon+1)
		if vs >= len(body) {
			return x.failNotObject()
		}
		ve, vUTF8, ok := scanValue(body, vs, 1, validUTF8)
		if !ok {
			return x.failNotObject()
		}
		validUTF8 = vUTF8
		m := Member{
			Key:     [2]int{ks, ke},
			Value:   [2]int{vs, ve},
			Comma:   comma,
			Escaped: escaped,
			Kind:    kindOf(body[vs]),
		}
		next := SkipSpace(body, ve)
		if next >= len(body) {
			return x.failNotObject()
		}
		if x.n == inlineMembers {
			if cap(x.extra) < inlineMembers {
				x.extra = make([]Member, 0, inlineMembers)
			}
		} else if x.n == 2*inlineMembers {
			tailCount, closePos, tailUTF8, valid := scanObjectTail(body, next, validUTF8)
			if !valid {
				return x.failNotObject()
			}
			total := x.n + 1 + tailCount
			if total > MaxMembers {
				x.n, x.extra = 0, x.extra[:0]
				x.open, x.validUTF8, x.err = open, tailUTF8, ErrTooManyMembers
				return x.err
			}
			if need := total - inlineMembers; cap(x.extra) < need {
				x.extra = append(make([]Member, 0, need), x.extra...)
			}
			x.add(m)
			c := cursor{body: body, i: next, comma: comma}
			for tm, ok := c.next(); ok; tm, ok = c.next() {
				x.add(tm)
			}
			x.open, x.close, x.validUTF8 = open, closePos, tailUTF8
			return nil
		}
		x.add(m)
		switch body[next] {
		case ',':
			comma = next
			i = SkipSpace(body, next+1)
			if i >= len(body) || body[i] != '"' {
				return x.failNotObject()
			}
		case '}':
			if SkipSpace(body, next+1) != len(body) {
				return x.failNotObject()
			}
			x.open, x.close, x.validUTF8 = open, next, validUTF8
			return nil
		default:
			return x.failNotObject()
		}
	}
}

func (x *Index) failNotObject() error {
	x.n, x.extra, x.err, x.validUTF8 = 0, x.extra[:0], ErrNotObject, false
	return x.err
}

// ValidUTF8 reports whether x's body is valid UTF-8 when Read found a valid
// JSON object (including past MaxMembers).
func (x *Index) ValidUTF8() bool { return x.validUTF8 }

// ValidObject reports whether body is a single RFC 8259 JSON object and
// returns the byte offset of its opening '{'. It allocates nothing.
func ValidObject(body []byte) (open int, ok bool) {
	i := SkipSpace(body, 0)
	if i >= len(body) || body[i] != '{' {
		return 0, false
	}
	end, _, valid := scanObject(body, i, 1, true)
	if !valid || SkipSpace(body, end) != len(body) {
		return 0, false
	}
	return i, true
}

// FindLast returns the last top-level member named name in the valid JSON
// object whose '{' is at body[open], without allocating an Index.
func FindLast(body []byte, open int, name string) (last Member, found bool) {
	c := cursorAt(body, open)
	for m, ok := c.next(); ok; m, ok = c.next() {
		var match bool
		if !m.Escaped {
			match = string(body[m.Key[0]+1:m.Key[1]-1]) == name
		} else {
			match = KeyIs(body[m.Key[0]:m.Key[1]], name)
		}
		if match {
			last, found = m, true
		}
	}
	return last, found
}

// Err is why the last Read indexed no members (ErrNotObject or
// ErrTooManyMembers), or nil when the body is indexed. An Index that was
// never read holds no object: ErrNotObject.
func (x *Index) Err() error {
	if x.err == nil && x.body == nil {
		return ErrNotObject
	}
	return x.err
}

func (x *Index) reset(body []byte) {
	x.body, x.open, x.close, x.n, x.err, x.validUTF8 = body, 0, 0, 0, nil, false
	x.extra = x.extra[:0]
}

func (x *Index) add(m Member) {
	if x.n < inlineMembers {
		x.inline[x.n] = m
	} else {
		x.extra = append(x.extra, m)
	}
	x.n++
}

func (x *Index) at(i int) *Member {
	if i < inlineMembers {
		return &x.inline[i]
	}
	return &x.extra[i-inlineMembers]
}

// scanObjectTail validates the rest of the top-level JSON object starting at
// body[next] (which must be ',' or '}'), plus trailing whitespace to EOF, and
// counts how many additional members remain.
func scanObjectTail(body []byte, next int, validUTF8 bool) (count, closePos int, outUTF8, ok bool) {
	for {
		switch body[next] {
		case '}':
			if SkipSpace(body, next+1) != len(body) {
				return 0, 0, false, false
			}
			return count, next, validUTF8, true
		case ',':
			i := SkipSpace(body, next+1)
			if i >= len(body) || body[i] != '"' {
				return 0, 0, false, false
			}
			ke, _, vUTF8, valid := scanString(body, i, validUTF8)
			if !valid {
				return 0, 0, false, false
			}
			validUTF8 = vUTF8
			colon := SkipSpace(body, ke)
			if colon >= len(body) || body[colon] != ':' {
				return 0, 0, false, false
			}
			vs := SkipSpace(body, colon+1)
			if vs >= len(body) {
				return 0, 0, false, false
			}
			ve, vUTF8, valid := scanValue(body, vs, 1, validUTF8)
			if !valid {
				return 0, 0, false, false
			}
			validUTF8 = vUTF8
			count++
			next = SkipSpace(body, ve)
			if next >= len(body) {
				return 0, 0, false, false
			}
		default:
			return 0, 0, false, false
		}
	}
}

func scanValue(b []byte, i, depth int, validUTF8 bool) (int, bool, bool) {
	switch b[i] {
	case '"':
		end, _, vUTF8, ok := scanString(b, i, validUTF8)
		return end, vUTF8, ok
	case '{':
		return scanObject(b, i, depth+1, validUTF8)
	case '[':
		return scanArray(b, i, depth+1, validUTF8)
	case 't':
		if i+4 <= len(b) && b[i+1] == 'r' && b[i+2] == 'u' && b[i+3] == 'e' {
			return i + 4, validUTF8, true
		}
		return 0, false, false
	case 'f':
		if i+5 <= len(b) && b[i+1] == 'a' && b[i+2] == 'l' && b[i+3] == 's' && b[i+4] == 'e' {
			return i + 5, validUTF8, true
		}
		return 0, false, false
	case 'n':
		if i+4 <= len(b) && b[i+1] == 'u' && b[i+2] == 'l' && b[i+3] == 'l' {
			return i + 4, validUTF8, true
		}
		return 0, false, false
	default:
		if b[i] == '-' || (b[i] >= '0' && b[i] <= '9') {
			end, ok := scanNumber(b, i)
			return end, validUTF8, ok
		}
		return 0, false, false
	}
}

func scanObject(b []byte, i, depth int, validUTF8 bool) (int, bool, bool) {
	if depth > maxNestingDepth {
		return 0, false, false
	}
	i = SkipSpace(b, i+1)
	if i >= len(b) {
		return 0, false, false
	}
	if b[i] == '}' {
		return i + 1, validUTF8, true
	}
	for {
		if b[i] != '"' {
			return 0, false, false
		}
		ke, _, vUTF8, ok := scanString(b, i, validUTF8)
		if !ok {
			return 0, false, false
		}
		validUTF8 = vUTF8
		colon := SkipSpace(b, ke)
		if colon >= len(b) || b[colon] != ':' {
			return 0, false, false
		}
		vs := SkipSpace(b, colon+1)
		if vs >= len(b) {
			return 0, false, false
		}
		ve, vUTF8, ok := scanValue(b, vs, depth, validUTF8)
		if !ok {
			return 0, false, false
		}
		validUTF8 = vUTF8
		next := SkipSpace(b, ve)
		if next >= len(b) {
			return 0, false, false
		}
		switch b[next] {
		case ',':
			i = SkipSpace(b, next+1)
			if i >= len(b) || b[i] != '"' {
				return 0, false, false
			}
		case '}':
			return next + 1, validUTF8, true
		default:
			return 0, false, false
		}
	}
}

func scanArray(b []byte, i, depth int, validUTF8 bool) (int, bool, bool) {
	if depth > maxNestingDepth {
		return 0, false, false
	}
	i = SkipSpace(b, i+1)
	if i >= len(b) {
		return 0, false, false
	}
	if b[i] == ']' {
		return i + 1, validUTF8, true
	}
	for {
		ve, vUTF8, ok := scanValue(b, i, depth, validUTF8)
		if !ok {
			return 0, false, false
		}
		validUTF8 = vUTF8
		next := SkipSpace(b, ve)
		if next >= len(b) {
			return 0, false, false
		}
		switch b[next] {
		case ',':
			i = SkipSpace(b, next+1)
			if i >= len(b) {
				return 0, false, false
			}
		case ']':
			return next + 1, validUTF8, true
		default:
			return 0, false, false
		}
	}
}

// scanString validates the JSON string starting with '"' at b[i], using 8-byte
// SWAR loads to skip runs of plain unescaped ASCII (0x20..0x7f except '"' and '\').
func scanString(b []byte, i int, validUTF8 bool) (end int, escaped, outUTF8, ok bool) {
	i++ // past opening '"'
	for {
		for i+16 <= len(b) {
			w0 := binary.LittleEndian.Uint64(b[i:])
			w1 := binary.LittleEndian.Uint64(b[i+8:])
			m0 := (w0 | (w0 - swarLo20) | ((w0 ^ swarQt22) - swarLo01) | ((w0 ^ swarBs5c) - swarLo01)) & swarHi80
			m1 := (w1 | (w1 - swarLo20) | ((w1 ^ swarQt22) - swarLo01) | ((w1 ^ swarBs5c) - swarLo01)) & swarHi80
			if (m0 | m1) != 0 {
				break
			}
			i += 16
		}
		for i+8 <= len(b) {
			w := binary.LittleEndian.Uint64(b[i:])
			if ((w | (w - swarLo20) | ((w ^ swarQt22) - swarLo01) | ((w ^ swarBs5c) - swarLo01)) & swarHi80) != 0 {
				break
			}
			i += 8
		}
		if i >= len(b) {
			return 0, false, false, false
		}
		c := b[i]
		switch {
		case c == '"':
			return i + 1, escaped, validUTF8, true
		case c == '\\':
			escaped = true
			if i+1 >= len(b) {
				return 0, false, false, false
			}
			switch b[i+1] {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
				i += 2
			case 'u':
				if i+6 > len(b) || !isHexByte(b[i+2]) || !isHexByte(b[i+3]) || !isHexByte(b[i+4]) || !isHexByte(b[i+5]) {
					return 0, false, false, false
				}
				i += 6
			default:
				return 0, false, false, false
			}
		case c < 0x20:
			return 0, false, false, false
		case c < utf8.RuneSelf:
			i++
		default:
			if validUTF8 {
				r, width := utf8.DecodeRune(b[i:])
				if r == utf8.RuneError && width == 1 {
					validUTF8 = false
					i++
				} else {
					i += width
				}
			} else {
				i++
			}
		}
	}
}

func isHexByte(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

func scanNumber(b []byte, i int) (int, bool) {
	if b[i] == '-' {
		i++
		if i >= len(b) {
			return 0, false
		}
	}
	switch {
	case b[i] == '0':
		i++
	case b[i] >= '1' && b[i] <= '9':
		i++
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			i++
		}
	default:
		return 0, false
	}
	if i < len(b) && b[i] == '.' {
		i++
		if i >= len(b) || b[i] < '0' || b[i] > '9' {
			return 0, false
		}
		i++
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			i++
		}
	}
	if i < len(b) && (b[i] == 'e' || b[i] == 'E') {
		i++
		if i < len(b) && (b[i] == '+' || b[i] == '-') {
			i++
		}
		if i >= len(b) || b[i] < '0' || b[i] > '9' {
			return 0, false
		}
		i++
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			i++
		}
	}
	return i, true
}

// Walk calls fn with each top-level member of x's body, in order, however
// many there are: it reads the body again and holds one member at a time,
// so its memory is constant. It is how a body past MaxMembers is read. It
// does nothing when Read found no JSON object.
func (x *Index) Walk(fn func(Member)) {
	if x.Err() == ErrNotObject {
		return
	}
	c := cursorAt(x.body, x.open)
	for m, ok := c.next(); ok; m, ok = c.next() {
		fn(m)
	}
}

// cursor reads the members of a valid JSON object one at a time.
type cursor struct {
	body  []byte
	i     int // the next member's key (or the ',' before it), or the object's '}'
	comma int // the ',' before the next member, or -1 for the first
}

// cursorAt starts a cursor on the object whose '{' is at body[open].
func cursorAt(body []byte, open int) cursor {
	return cursor{body: body, i: SkipSpace(body, open+1), comma: -1}
}

// next returns the next member, or false at the object's '}' (c.i is then
// its offset).
func (c *cursor) next() (Member, bool) {
	if c.body[c.i] == '}' {
		return Member{}, false
	}
	if c.body[c.i] == ',' {
		c.comma, c.i = c.i, SkipSpace(c.body, c.i+1)
	}
	m, end := member(c.body, c.i)
	m.Comma = c.comma
	c.i = SkipSpace(c.body, end)
	return m, true
}

// member reads the member whose key starts at body[i] and returns it with
// the index just past its value.
func member(body []byte, i int) (Member, int) {
	ks := i
	i = SkipString(body, i)
	m := Member{Key: [2]int{ks, i}, Comma: -1, Escaped: bytes.IndexByte(body[ks:i], '\\') >= 0}
	vs := SkipSpace(body, SkipSpace(body, i)+1) // past ':'
	i = SkipValue(body, vs)
	m.Value, m.Kind = [2]int{vs, i}, kindOf(body[vs])
	return m, i
}

// Body is the body x last read or was spliced into, indexed or not.
func (x *Index) Body() []byte { return x.body }

// Open is the offset of the object's '{', also past MaxMembers.
func (x *Index) Open() int { return x.open }

// Close is the offset of the object's '}' when the body is indexed.
func (x *Index) Close() int { return x.close }

// Len is the number of members.
func (x *Index) Len() int { return x.n }

// Member returns member i, in body order, or the zero Member when there is
// no member i.
func (x *Index) Member(i int) Member {
	if i < 0 || i >= x.n {
		return Member{}
	}
	return *x.at(i)
}

// Value returns m's value bytes; nil for the zero Member.
func (x *Index) Value(m Member) []byte {
	if m.Kind == Invalid {
		return nil
	}
	return x.body[m.Value[0]:m.Value[1]]
}

// RawKey returns m's key as written, without its quotes; nil for the zero
// Member.
func (x *Index) RawKey(m Member) []byte {
	if m.Kind == Invalid {
		return nil
	}
	return x.body[m.Key[0]+1 : m.Key[1]-1]
}

// Name returns m's decoded key. Only an escaped key costs a decode; an
// unescaped one is its bytes as written (where encoding/json would replace
// invalid UTF-8, which no name the gateway reads has). It is linear in the
// key: a reader asking about a key the caller wrote uses Is.
func (x *Index) Name(m Member) string {
	if !m.Escaped {
		return string(x.RawKey(m))
	}
	return decodeKey(x.body[m.Key[0]:m.Key[1]])
}

// decodeKey decodes key, a valid JSON string with its quotes.
func decodeKey(key []byte) string {
	var s string
	_ = json.Unmarshal(key, &s) // the body is valid JSON
	return s
}

// Is reports whether m's decoded key is name. See KeyIs for what it costs.
func (x *Index) Is(m Member, name string) bool {
	if m.Kind == Invalid {
		return false
	}
	if !m.Escaped {
		return string(x.RawKey(m)) == name
	}
	return KeyIs(x.body[m.Key[0]:m.Key[1]], name)
}

// maxEscapeBytes is the longest spelling JSON has of one decoded byte: a
// Unicode escape.
const maxEscapeBytes = 6

// KeyIs reports whether key, a valid JSON string with its quotes, decodes to
// name, as encoding/json reads it but for an unescaped key, which is its
// bytes as written (see Name). What a caller writes in a key cannot make it
// costly: nothing is allocated, a key too long to spell name is refused by
// its length, and any other is read only as far as it agrees with name, so
// the names a reader asks about, not the body, set the cost. Only a key that
// agrees up to a byte of name past ASCII costs a decode, of a key that fits;
// no reader of a request asks about such a name.
func KeyIs(key []byte, name string) bool {
	if len(key) > maxEscapeBytes*len(name)+2 {
		return false
	}
	raw := key[1 : len(key)-1]
	if bytes.IndexByte(raw, '\\') < 0 {
		return string(raw) == name
	}
	n := 0
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		if c == '\\' {
			c, i = escapedByte(raw, i)
		}
		switch {
		case n >= len(name):
			return false
		case name[n] >= utf8.RuneSelf:
			return decodeKey(key) == name
		case c != name[n]: // what is past ASCII in key differs here too
			return false
		}
		n++
	}
	return n == len(name)
}

// escapedByte reads the escape at raw[i], valid JSON, for KeyIs, and returns
// the index of its last byte. A short escape returns its byte, and \u00XX
// the code point XX. Any other Unicode escape, a surrogate included, returns
// utf8.RuneSelf. So a byte below utf8.RuneSelf is the decoded byte, and any
// other is only past ASCII, not what the escape decodes to: that is all
// KeyIs's compare needs.
func escapedByte(raw []byte, i int) (byte, int) {
	switch c := raw[i+1]; c {
	case 'b':
		return '\b', i + 1
	case 'f':
		return '\f', i + 1
	case 'n':
		return '\n', i + 1
	case 'r':
		return '\r', i + 1
	case 't':
		return '\t', i + 1
	case 'u':
		if raw[i+2] != '0' || raw[i+3] != '0' {
			return utf8.RuneSelf, i + 5
		}
		return hexValue(raw[i+4])<<4 | hexValue(raw[i+5]), i + 5
	default: // a quote, a backslash or a slash
		return c, i + 1
	}
}

// hexValue is the value of c, a hex digit.
func hexValue(c byte) byte {
	switch {
	case c >= 'a':
		return c - 'a' + 10
	case c >= 'A':
		return c - 'A' + 10
	}
	return c - '0'
}

// Find returns the last member named name (decoded, so every spelling
// counts), as vLLM's parser reads a repeated name, and how many members
// have that name. With none, or no members indexed, it returns the zero
// Member, which the splices refuse.
func (x *Index) Find(name string) (m Member, count int) {
	for i := range x.n {
		if c := x.at(i); x.Is(*c, name) {
			m, count = *c, count+1
		}
	}
	return m, count
}

// The splices return a new body and index it into out, by shifting the
// members around the change rather than reading the body again. out may be
// x: x is read whole before out is written. A zero Member is refused, and
// so is any splice of an Index that indexed no members (its Read failed):
// the body comes back unchanged, and out holds what x does. Each one is a
// byte-level splice (InsertFirst, Remove, replaceValue) plus the shift, so
// there is one splice of each kind.

// InsertFirst returns body with entry (`"key":value`, valid JSON whose
// name the body does not have: a later duplicate would win in vLLM) as
// the object's first member, indexed into out. An empty entry is refused
// as the zero Member is.
func (x *Index) InsertFirst(entry []byte, out *Index) []byte {
	if x.Err() != nil || len(entry) == 0 {
		return x.unchanged(out)
	}
	b := InsertFirst(x.body, x.open, entry)
	at, delta := x.open+1, len(b)-len(x.body)
	ms := newMembers(x.n + 1)
	first, _ := member(b, SkipSpace(b, at))
	ms.add(first)
	for i := range x.n {
		m := shifted(*x.at(i), at, delta)
		if i == 0 {
			m.Comma = at + len(entry) // the separator after the new member
		}
		ms.add(m)
	}
	ms.into(out, b, x.open, x.close+delta, x.validUTF8 && utf8.Valid(entry))
	return b
}

// Remove returns body without member m (one of x's), indexed into out. The
// object stays valid JSON: the separator before m goes with it, or the one
// after it when m is the first.
func (x *Index) Remove(m Member, out *Index) []byte {
	if m.Kind == Invalid || x.Err() != nil {
		return x.unchanged(out)
	}
	from, to := removal(x.body, m)
	b := Remove(x.body, m)
	ms := newMembers(x.n - 1)
	first := m.Comma < 0
	for i := range x.n {
		c := *x.at(i)
		switch {
		case c.Key == m.Key:
			continue
		case c.Key[0] > m.Key[0]:
			c = shifted(c, from, from-to)
			if first {
				c.Comma, first = -1, false // the member after a removed first one is now the first
			}
		}
		ms.add(c)
	}
	validUTF8 := x.validUTF8
	if !validUTF8 {
		validUTF8 = utf8.Valid(b)
	}
	ms.into(out, b, x.open, x.close+from-to, validUTF8)
	return b
}

// ReplaceValue returns body with m's value replaced by value (valid JSON;
// whitespace around it is dropped), indexed into out. An empty value is
// refused as the zero Member is.
func (x *Index) ReplaceValue(m Member, value []byte, out *Index) []byte {
	value = trimSpace(value)
	if m.Kind == Invalid || len(value) == 0 || x.Err() != nil {
		return x.unchanged(out)
	}
	b := replaceValue(x.body, m, value)
	delta := len(value) - (m.Value[1] - m.Value[0])
	ms := newMembers(x.n)
	for i := range x.n {
		c := *x.at(i)
		switch {
		case c.Key == m.Key:
			c.Value[1] += delta
			c.Kind = kindOf(value[0])
		case c.Key[0] > m.Key[0]:
			c = shifted(c, m.Value[1], delta)
		}
		ms.add(c)
	}
	validUTF8 := x.validUTF8 && utf8.Valid(value)
	if !x.validUTF8 && utf8.Valid(value) {
		validUTF8 = utf8.Valid(b)
	}
	ms.into(out, b, x.open, x.close+delta, validUTF8)
	return b
}

// InsertFirst returns a copy of body, a valid JSON object whose '{' is at
// body[open], with entry (`"key":value`, valid JSON) as its first member;
// every other byte is body's. No splice inside the object moves its '{',
// so open stays valid from one splice to the next. An empty entry is
// refused: body comes back as it is.
func InsertFirst(body []byte, open int, entry []byte) []byte {
	if len(entry) == 0 {
		return body
	}
	sep := body[SkipSpace(body, open+1)] != '}' // a member follows the new one
	b := make([]byte, 0, len(body)+len(entry)+1)
	b = append(b, body[:open+1]...)
	b = append(b, entry...)
	if sep {
		b = append(b, ',')
	}
	return append(b, body[open+1:]...)
}

// Remove returns a copy of body, a valid JSON object, without m, one of
// its top-level members (from Walk, or from an Index of body). The object
// stays valid JSON: the separator before m goes with it, or the one after
// it when m is the first. The zero Member is refused: body comes back as
// it is.
func Remove(body []byte, m Member) []byte {
	if m.Kind == Invalid {
		return body
	}
	from, to := removal(body, m)
	b := make([]byte, 0, len(body)-(to-from))
	b = append(b, body[:from]...)
	return append(b, body[to:]...)
}

// Splice returns body with remove cut out (if valid) and first1, first2 (each
// optional `"key":value` JSON member) prepended in order at body[open+1],
// allocating at most one output slice. Its byte output equals Remove(remove)
// followed by InsertFirst(first2) followed by InsertFirst(first1).
func Splice(body []byte, open int, first1, first2 []byte, remove Member) []byte {
	if len(first1) == 0 && len(first2) == 0 {
		return Remove(body, remove)
	}
	from, to, hasRemaining := spliceLayout(body, open, remove)
	insertLen := len(first1) + len(first2)
	if len(first1) > 0 && len(first2) > 0 {
		insertLen++
	}
	if hasRemaining {
		insertLen++
	}
	b := make([]byte, 0, len(body)-(to-from)+insertLen)
	b = append(b, body[:open+1]...)
	if len(first1) > 0 {
		b = append(b, first1...)
		if len(first2) > 0 {
			b = append(b, ',')
		}
	}
	if len(first2) > 0 {
		b = append(b, first2...)
	}
	if hasRemaining {
		b = append(b, ',')
	}
	b = append(b, body[open+1:from]...)
	return append(b, body[to:]...)
}

// SpliceLen returns len(Splice(body, open, first1, nil, remove)) without
// allocating.
func SpliceLen(body []byte, open int, first1Len int, remove Member) int {
	if first1Len == 0 && remove.Kind == Invalid {
		return len(body)
	}
	from, to, hasRemaining := spliceLayout(body, open, remove)
	n := len(body) - (to - from) + first1Len
	if first1Len > 0 && hasRemaining {
		n++
	}
	return n
}

func spliceLayout(body []byte, open int, remove Member) (from, to int, hasRemaining bool) {
	if remove.Kind == Invalid {
		return open + 1, open + 1, body[SkipSpace(body, open+1)] != '}'
	}
	from, to = removal(body, remove)
	if remove.Comma >= 0 {
		return from, to, true
	}
	return from, to, body[SkipSpace(body, to)] != '}'
}

// removal is the byte range Remove cuts out of body for m.
func removal(body []byte, m Member) (from, to int) {
	from, to = m.Key[0], m.Value[1]
	if m.Comma >= 0 {
		from = m.Comma // the separator before it
	} else if next := SkipSpace(body, to); body[next] == ',' {
		to = next + 1 // the first member: the separator after it
	}
	return from, to
}

// replaceValue returns a copy of body with m's value replaced by value.
func replaceValue(body []byte, m Member, value []byte) []byte {
	b := make([]byte, 0, len(body)-(m.Value[1]-m.Value[0])+len(value))
	b = append(b, body[:m.Value[0]]...)
	b = append(b, value...)
	return append(b, body[m.Value[1]:]...)
}

// members collects a splice's members before they are written into out,
// so out may be the index being spliced. It is sized for n members, so
// the members past inlineMembers are allocated once, and handed to out.
type members struct {
	buf   [inlineMembers]Member
	extra []Member
	n     int
}

func newMembers(n int) members {
	var ms members
	if n > inlineMembers {
		ms.extra = make([]Member, 0, n-inlineMembers)
	}
	return ms
}

func (ms *members) add(m Member) {
	if ms.n < inlineMembers {
		ms.buf[ms.n] = m
	} else {
		ms.extra = append(ms.extra, m)
	}
	ms.n++
}

// into indexes body b into out with the collected members: the first
// inlineMembers are copied, the rest are out's from now on.
func (ms *members) into(out *Index, b []byte, open, close int, validUTF8 bool) {
	out.reset(b)
	out.open, out.close, out.n, out.validUTF8 = open, close, ms.n, validUTF8
	out.inline = ms.buf
	out.extra = ms.extra
}

// unchanged is a refused splice: x's body, with out holding what x does.
func (x *Index) unchanged(out *Index) []byte {
	b := x.body
	switch {
	case out == x:
	case x.err != nil: // no members indexed: copy why, never read b again
		out.reset(b)
		out.open, out.validUTF8, out.err = x.open, x.validUTF8, x.err
	case b == nil:
		out.reset(nil)
	default:
		ms := newMembers(x.n)
		for i := range x.n {
			ms.add(*x.at(i))
		}
		ms.into(out, b, x.open, x.close, x.validUTF8)
	}
	return b
}

// trimSpace drops JSON whitespace around b.
func trimSpace(b []byte) []byte {
	b = b[SkipSpace(b, 0):]
	for len(b) > 0 {
		switch b[len(b)-1] {
		case ' ', '\t', '\n', '\r':
			b = b[:len(b)-1]
			continue
		}
		break
	}
	return b
}

// shifted is m with every offset at or past at moved by delta.
func shifted(m Member, at, delta int) Member {
	m.Key = [2]int{move(m.Key[0], at, delta), move(m.Key[1], at, delta)}
	m.Value = [2]int{move(m.Value[0], at, delta), move(m.Value[1], at, delta)}
	if m.Comma >= 0 {
		m.Comma = move(m.Comma, at, delta)
	}
	return m
}

func move(o, at, delta int) int {
	if o >= at {
		return o + delta
	}
	return o
}

func kindOf(c byte) Kind {
	switch c {
	case '"':
		return String
	case '{':
		return Object
	case '[':
		return Array
	case 't', 'f':
		return Bool
	case 'n':
		return Null
	}
	return Number
}

// IsZeroNumber reports whether raw, a JSON value, is a number equal to
// zero as vLLM's JSON parser reads it: 0, -0, 0.0, 0e5, and a non-zero
// literal that underflows to zero, such as 1e-400. Anything else, a
// missing value included, is false. It is the gateway's one zero rule
// (coalescing's temperature, ingress's priority).
func IsZeroNumber(raw []byte) bool {
	if len(raw) == 0 || (raw[0] != '-' && (raw[0] < '0' || raw[0] > '9')) {
		return false
	}
	if len(raw) == 1 && raw[0] == '0' {
		return true
	}
	if len(raw) == 3 && raw[0] == '0' && raw[1] == '.' && raw[2] == '0' {
		return true
	}
	// A positive number starting with 1..9 without an exponent cannot be 0.
	if raw[0] >= '1' && raw[0] <= '9' && bytes.IndexByte(raw, 'e') < 0 && bytes.IndexByte(raw, 'E') < 0 {
		return false
	}
	f, err := strconv.ParseFloat(string(raw), 64)
	return err == nil && f == 0
}

// SkipSpace returns the index of the first non-whitespace byte at or after
// i.
func SkipSpace(b []byte, i int) int {
	for i < len(b) && (b[i] == ' ' || b[i] == '\t' || b[i] == '\n' || b[i] == '\r') {
		i++
	}
	return i
}

// SkipString returns the index just past the valid JSON string at b[i].
func SkipString(b []byte, i int) int {
	i++
	for {
		for i+8 <= len(b) {
			w := binary.LittleEndian.Uint64(b[i:])
			q := w ^ swarQt22
			bs := w ^ swarBs5c
			if (((q-swarLo01) & ^q)|((bs-swarLo01) & ^bs))&swarHi80 != 0 {
				break
			}
			i += 8
		}
		if b[i] == '"' {
			return i + 1
		}
		if b[i] == '\\' {
			i += 2
			continue
		}
		i++
	}
}

// SkipValue returns the index just past the valid JSON value at b[i] (the
// end of the value, before any whitespace that follows it).
func SkipValue(b []byte, i int) int {
	switch b[i] {
	case '"':
		return SkipString(b, i)
	case '{', '[':
		for depth := 0; ; i++ {
			switch b[i] {
			case '"':
				i = SkipString(b, i) - 1
			case '{', '[':
				depth++
			case '}', ']':
				if depth--; depth == 0 {
					return i + 1
				}
			}
		}
	}
	for i < len(b) && b[i] != ',' && b[i] != '}' && b[i] != ']' &&
		b[i] != ' ' && b[i] != '\t' && b[i] != '\n' && b[i] != '\r' {
		i++
	}
	return i
}
