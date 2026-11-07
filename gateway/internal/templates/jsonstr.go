package templates

import (
	"unicode/utf16"
	"unicode/utf8"
)

// jsonStringMatch reports whether raw, a JSON string literal, decodes to
// want (whole) or to a string that starts with want. It decodes as
// encoding/json does (escapes, surrogate pairs, lone surrogates as U+FFFD)
// without allocating. raw is assumed syntactically valid; anything odd is
// a mismatch.
func jsonStringMatch(raw []byte, want string, whole bool) bool {
	if len(raw) < 2 || raw[0] != '"' {
		return false
	}
	i, j := 1, 0
	for i < len(raw) {
		if j == len(want) && !whole {
			return true
		}
		c := raw[i]
		switch c {
		case '"':
			return j == len(want)
		case '\\':
			r, n := decodeEscape(raw[i:])
			if n == 0 {
				return false
			}
			i += n
			var buf [utf8.UTFMax]byte
			w := utf8.EncodeRune(buf[:], r)
			if len(want)-j < w || want[j:j+w] != string(buf[:w]) {
				return false
			}
			j += w
		default:
			// Unescaped run up to the next quote or backslash, bounded by
			// len(want)-j+1 so a huge mismatched message never scans past want.
			limit := min(len(raw), i+(len(want)-j)+1)
			k := i + 1
			for k < limit && raw[k] != '"' && raw[k] != '\\' {
				k++
			}
			n := min(k-i, len(want)-j)
			if n == 0 || string(raw[i:i+n]) != want[j:j+n] {
				return false
			}
			i += n
			j += n
		}
	}
	return false
}

// decodeEscape decodes the escape at the start of b (which begins with a
// backslash) and returns its rune and length; n is 0 when it is malformed.
func decodeEscape(b []byte) (r rune, n int) {
	if len(b) < 2 {
		return 0, 0
	}
	switch b[1] {
	case '"', '\\', '/':
		return rune(b[1]), 2
	case 'b':
		return '\b', 2
	case 'f':
		return '\f', 2
	case 'n':
		return '\n', 2
	case 'r':
		return '\r', 2
	case 't':
		return '\t', 2
	case 'u':
		r1, ok := hex4(b[2:])
		if !ok {
			return 0, 0
		}
		if !utf16.IsSurrogate(r1) {
			return r1, 6
		}
		if len(b) >= 12 && b[6] == '\\' && b[7] == 'u' {
			if r2, ok := hex4(b[8:]); ok {
				if r := utf16.DecodeRune(r1, r2); r != utf8.RuneError {
					return r, 12
				}
			}
		}
		return utf8.RuneError, 6
	}
	return 0, 0
}

func hex4(b []byte) (rune, bool) {
	if len(b) < 4 {
		return 0, false
	}
	var r rune
	for _, c := range b[:4] {
		switch {
		case c >= '0' && c <= '9':
			c -= '0'
		case c >= 'a' && c <= 'f':
			c = c - 'a' + 10
		case c >= 'A' && c <= 'F':
			c = c - 'A' + 10
		default:
			return 0, false
		}
		r = r<<4 | rune(c)
	}
	return r, true
}
