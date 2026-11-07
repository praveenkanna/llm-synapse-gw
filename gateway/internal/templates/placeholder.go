package templates

import "strings"

// maxNameLen bounds a variable name: [a-z][a-z0-9_]{0,63}.
const maxNameLen = 64

// ValidName reports whether name is a well-formed variable name.
func ValidName(name string) bool {
	if len(name) == 0 || len(name) > maxNameLen || !isLower(name[0]) {
		return false
	}
	for i := 1; i < len(name); i++ {
		if !isNameByte(name[i]) {
			return false
		}
	}
	return true
}

func isLower(c byte) bool    { return c >= 'a' && c <= 'z' }
func isNameByte(c byte) bool { return isLower(c) || c >= '0' && c <= '9' || c == '_' }

// nextPlaceholder finds the first {{name}} in s at or after from, with the
// same leftmost semantics as the regexp \{\{[a-z][a-z0-9_]{0,63}\}\}: any
// other "{{" is literal text. start is -1 when there is none.
func nextPlaceholder(s string, from int) (start, end int, name string) {
	for from < len(s) {
		k := strings.Index(s[from:], "{{")
		if k < 0 {
			break
		}
		start = from + k
		if name, n := placeholderAt(s[start:]); n > 0 {
			return start, start + n, name
		}
		from = start + 1
	}
	return -1, -1, ""
}

// placeholderAt parses a placeholder at the start of s, which begins with
// "{{". n is its length, 0 when s doesn't open with one.
func placeholderAt(s string) (name string, n int) {
	i := 2
	if i >= len(s) || !isLower(s[i]) {
		return "", 0
	}
	for i++; i < len(s) && i-2 < maxNameLen && isNameByte(s[i]); i++ {
	}
	if !strings.HasPrefix(s[i:], "}}") {
		return "", 0
	}
	return s[2:i], i + 2
}

// placeholders calls fn with each placeholder name in s, in order.
func placeholders(s string, fn func(name string)) {
	for from := 0; ; {
		start, end, name := nextPlaceholder(s, from)
		if start < 0 {
			return
		}
		fn(name)
		from = end
	}
}
