// Package bodyindextest is the reference reading of a request body's top
// level, built on encoding/json, that the gateway's own readers are fuzzed
// against (AD-14): if each agrees with it, they agree with each other.
package bodyindextest

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
)

// Escaped is name with its first "o" written as a JSON escape: the same
// name to a JSON parser, another one to a comparison of bytes. Tests build
// escaped keys with it, so a key that must be escaped cannot quietly be
// written as plain text.
func Escaped(name string) string {
	const backslash = 0x5c
	return strings.Replace(name, "o", string(rune(backslash))+"u006f", 1)
}

// Member is one top-level member of a JSON object body.
type Member struct {
	Key     string // decoded name
	RawKey  string // the name as written, between its quotes
	Escaped bool   // the name was written with an escape
	Start   int    // offset of the key's opening quote
	Value   [2]int // byte range of the value
	// Comma is the offset of the ',' before the member, or -1 for the first.
	Comma int
}

// Members returns body's top-level members in order, duplicates included,
// and the offset of the object's '{'. ok is false unless body is exactly
// one valid JSON object.
func Members(body []byte) (members []Member, open int, ok bool) {
	if !json.Valid(body) {
		return nil, 0, false
	}
	open = bytes.IndexFunc(body, func(r rune) bool { return r != ' ' && r != '\t' && r != '\n' && r != '\r' })
	if open < 0 || body[open] != '{' {
		return nil, 0, false
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	if _, err := dec.Token(); err != nil { // '{'
		return nil, 0, false
	}
	prevEnd := open + 1
	for dec.More() {
		before := int(dec.InputOffset())
		tok, err := dec.Token()
		if err != nil {
			return nil, 0, false
		}
		key, _ := tok.(string)
		afterKey := int(dec.InputOffset())
		m := Member{Key: key, Comma: -1}
		// The key's quotes end at afterKey; find where they start.
		m.Start = keyStart(body, before, afterKey)
		m.RawKey = string(body[m.Start+1 : afterKey-1])
		m.Escaped = bytes.IndexByte(body[m.Start:afterKey], '\\') >= 0
		if c := bytes.IndexByte(body[prevEnd:m.Start], ','); c >= 0 {
			m.Comma = prevEnd + c
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return nil, 0, false
		}
		end := int(dec.InputOffset())
		m.Value = [2]int{end - len(raw), end}
		members = append(members, m)
		prevEnd = end
	}
	if _, err := dec.Token(); err != nil && err != io.EOF { // '}'
		return nil, 0, false
	}
	return members, open, true
}

// keyStart returns the offset of the opening quote of the string that
// ends at end (just past its closing quote), scanning from from.
func keyStart(body []byte, from, end int) int {
	for i := from; i < end; i++ {
		if body[i] == '"' {
			return i
		}
	}
	return from
}

// Last returns the last member named key (decoded), as vLLM's parser
// reads a repeated name, and how many members have that name.
func Last(members []Member, key string) (m Member, count int) {
	for _, x := range members {
		if x.Key == key {
			m, count = x, count+1
		}
	}
	return m, count
}
