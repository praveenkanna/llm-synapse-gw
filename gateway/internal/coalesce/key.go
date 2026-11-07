// Package coalesce shares one upstream generation between identical,
// deterministic requests of templates that opted in, and records what a
// generation sent so a late joiner can replay it and the response cache can
// store it (ADR 0019).
//
// This file computes the key. A key is a digest of the whole request, not
// of a variable map: under ADR 0006 the gateway never renders a template,
// so it sees only the rendered body. The rule that matters is that two
// requests vLLM would answer differently never share a key. So values are
// hashed as bytes, nothing is decoded and re-encoded (a decoder maps
// distinct byte strings to one value, for example a lone surrogate escape
// and U+FFFD), and an unusual body simply has no key. The cost of that
// caution is a missed merge, never a wrong one.
package coalesce

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash"
	"slices"
	"strconv"
	"sync"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// ignorable reports whether a top-level member does not change the answer,
// so it is left out of the key. "priority" only orders the request in the
// replica's queue (ADR 0018) and "user" is an opaque caller label, but only
// a single member of a type vLLM accepts is harmless: a repeated priority,
// or one that is not a number, is forwarded for the replica to refuse, and
// so is a user that is not a string. Every other member, known or not, is
// part of the key.
func ignorable(name, raw []byte, count int) bool {
	if count != 1 || len(raw) == 0 {
		return false
	}
	switch string(name) {
	case "priority":
		return raw[0] == '-' || (raw[0] >= '0' && raw[0] <= '9')
	case "user":
		return raw[0] == '"' || string(raw) == "null"
	}
	return false
}

// Fingerprint is the part of a key that comes from the request body.
type Fingerprint struct {
	digest [sha256.Size]byte
	// Stream is true when the body asks for an SSE response. A stream and a
	// body response never share a key: "stream" is a body member.
	Stream bool
	// Deterministic is true when the body fixes its sampling: temperature
	// exactly 0, or an integer seed. Only such a request has one answer.
	Deterministic bool
}

type keyHasher struct {
	h      hash.Hash
	varint [binary.MaxVarintLen64]byte
	strBuf [128]byte
	field  [32]byte
	sum    [sha256.Size]byte
}

var keyHasherPool = sync.Pool{
	New: func() any {
		return &keyHasher{h: sha256.New()}
	},
}

func getKeyHasher() *keyHasher {
	kh := keyHasherPool.Get().(*keyHasher)
	kh.h.Reset()
	return kh
}

func putKeyHasher(kh *keyHasher) {
	keyHasherPool.Put(kh)
}

func (kh *keyHasher) writeLen(n int) {
	k := binary.PutUvarint(kh.varint[:], uint64(n))
	_, _ = kh.h.Write(kh.varint[:k])
}

func (kh *keyHasher) writeField(b []byte) {
	kh.writeLen(len(b))
	_, _ = kh.h.Write(b)
}

func (kh *keyHasher) writeStringField(s string) {
	kh.writeLen(len(s))
	for len(s) > 0 {
		n := copy(kh.strBuf[:], s)
		_, _ = kh.h.Write(kh.strBuf[:n])
		s = s[n:]
	}
}

// writeCompactField writes raw, a validated JSON value with no leading or
// trailing whitespace, into kh with insignificant whitespace outside strings
// stripped — bit-for-bit identical to json.Compact without allocating.
func (kh *keyHasher) writeCompactField(raw []byte) {
	if len(raw) == 0 || (raw[0] != '{' && raw[0] != '[') {
		kh.writeField(raw)
		return
	}
	spaces := countInsignificantSpaces(raw)
	if spaces == 0 {
		kh.writeField(raw)
		return
	}
	kh.writeLen(len(raw) - spaces)
	start := 0
	for i := 0; i < len(raw); i++ {
		switch raw[i] {
		case '"':
			i = bodyindex.SkipString(raw, i) - 1
		case ' ', '\t', '\n', '\r':
			if start < i {
				_, _ = kh.h.Write(raw[start:i])
			}
			start = i + 1
		}
	}
	if start < len(raw) {
		_, _ = kh.h.Write(raw[start:])
	}
}

func countInsignificantSpaces(raw []byte) int {
	spaces := 0
	for i := 0; i < len(raw); i++ {
		switch raw[i] {
		case '"':
			i = bodyindex.SkipString(raw, i) - 1
		case ' ', '\t', '\n', '\r':
			spaces++
		}
	}
	return spaces
}

// FingerprintOf digests the body ix indexes (the request's one index,
// read by ingress, AD-15). reason is empty on success; otherwise the
// request has no key and runs as an ordinary request. A body past
// bodyindex.MaxMembers has no key either: the index holds none of its
// members (too_many_members).
func FingerprintOf(ix *bodyindex.Index, maxBytes int) (fp Fingerprint, reason string) {
	body := ix.Body()
	if len(body) > maxBytes {
		return Fingerprint{}, telemetry.ReasonBodyTooLarge
	}
	err := ix.Err()
	if errors.Is(err, bodyindex.ErrNotObject) || !ix.ValidUTF8() {
		return Fingerprint{}, telemetry.ReasonUnparseableBody
	}
	if err != nil {
		return Fingerprint{}, telemetry.ReasonTooManyMembers
	}
	var buf [inlineNames]named
	names, reason := topLevel(ix, buf[:0])
	if reason != "" {
		return Fingerprint{}, reason
	}
	kh := getKeyHasher()
	defer putKeyHasher(kh)
	var stream, temperature, seed []byte
	for _, n := range names {
		switch string(n.key) {
		case "stream":
			stream = n.value
		case "temperature":
			temperature = n.value
		case "seed":
			seed = n.value
		}
		if ignorable(n.key, n.value, n.count) {
			continue
		}
		field := n.key
		if n.count > 1 && (string(n.key) == "priority" || string(n.key) == "user") {
			// The repetition itself changes what the replica is sent. A NUL
			// cannot appear in an unescaped key, so this names no real member.
			fb := append(kh.field[:0], n.key...)
			fb = append(fb, 0)
			field = strconv.AppendInt(fb, int64(n.count), 10)
		}
		kh.writeField(field)
		kh.writeCompactField(n.value)
	}
	kh.h.Sum(kh.sum[:0])
	fp.digest = kh.sum
	fp.Stream = string(stream) == "true"
	fp.Deterministic = bodyindex.IsZeroNumber(temperature) || isInteger(seed)
	return fp, ""
}

// Parts are the inputs of a key that are not in the request body.
type Parts struct {
	Endpoint string // request path: one body means different things on /v1/completions and /v1/chat/completions
	Template string // template ID
	Version  string // template version
	Pool     string // pool the request resolved to
	Weights  string // pool's weights_version
	Encoding string // the caller's Accept-Encoding, forwarded to the replica
}

var keyDomainPrefix = []byte("gateway-key-v2")

// Key returns the hex digest of the request's fingerprint and parts. extra
// values are mixed in when the caller needs a narrower key: the in-flight
// key adds the priority class, and a budgeted request adds its budget.
func (f Fingerprint) Key(p Parts, extra ...string) string {
	kh := getKeyHasher()
	defer putKeyHasher(kh)
	kh.writeField(keyDomainPrefix)
	kh.writeStringField(p.Endpoint)
	kh.writeStringField(p.Template)
	kh.writeStringField(p.Version)
	kh.writeStringField(p.Pool)
	kh.writeStringField(p.Weights)
	kh.writeStringField(p.Encoding)
	kh.field = [32]byte(f.digest)
	kh.writeField(kh.field[:])
	for _, s := range extra {
		kh.writeStringField(s)
	}
	kh.h.Sum(kh.sum[:0])
	var hexBuf [sha256.Size * 2]byte
	hex.Encode(hexBuf[:], kh.sum[:])
	return string(hexBuf[:])
}

// writeField hashes b with its length first, so no two field lists hash the
// same stream.
func writeField(h hash.Hash, b []byte) {
	var n [binary.MaxVarintLen64]byte
	h.Write(n[:binary.PutUvarint(n[:], uint64(len(b)))])
	h.Write(b)
}

// inlineNames is how many names FingerprintOf reads without allocating.
const inlineNames = 16

// named is one name of a body's top level: the last member of that name,
// which vLLM's parser reads, and how many members have it.
type named struct {
	key   []byte // the name as written, between its quotes (never escaped)
	value []byte // the last member's value
	count int
}

// topLevel appends the names of the body ix indexes to buf, in byte order.
// A body with a key written with an escape has none (unsupported_key): its
// raw bytes would not be its name, and such bodies are rare and not worth
// a decoder's ambiguities (ADR 0019).
func topLevel(ix *bodyindex.Index, buf []named) ([]named, string) {
	for i := range ix.Len() {
		m := ix.Member(i)
		if m.Escaped {
			return nil, telemetry.ReasonUnsupportedKey
		}
		buf = append(buf, named{key: ix.RawKey(m), value: ix.Value(m), count: 1})
	}
	// A stable sort keeps a repeated name's members in body order, so the
	// last of each run is the one vLLM reads.
	slices.SortStableFunc(buf, func(a, b named) int { return bytes.Compare(a.key, b.key) })
	out := buf[:0]
	for _, n := range buf {
		if k := len(out) - 1; k >= 0 && bytes.Equal(out[k].key, n.key) {
			out[k].value, out[k].count = n.value, out[k].count+1
			continue
		}
		out = append(out, n)
	}
	return out, ""
}

// isInteger reports whether raw is a JSON integer literal (digits with an
// optional minus, no fraction or exponent).
func isInteger(raw []byte) bool {
	if len(raw) > 0 && raw[0] == '-' {
		raw = raw[1:]
	}
	if len(raw) == 0 || len(raw) > 19 {
		return false
	}
	for _, c := range raw {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
