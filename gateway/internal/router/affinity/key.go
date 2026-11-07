package affinity

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

const (
	// maxHashedMessageBytes caps how much of one message's raw JSON is
	// hashed; the message's full length is hashed too.
	maxHashedMessageBytes = 8 << 10
	// promptPrefixBytes is how much of a /v1/completions prompt is hashed.
	// The prompt grows every turn, so its length is not hashed.
	promptPrefixBytes = 2 << 10
)

var (
	keyRole       = []byte("role")
	roleSystem    = []byte("system")
	roleDeveloper = []byte("developer")
)

func none(reason string) Result { return Result{Tier: TierNone, Reason: reason} }

// keyHash is FNV-1a over 64-bit little-endian words (bytes for the tail),
// inline and allocation-free. Word steps make it ~7x faster than byte-wise
// FNV-1a; the ring re-hashes keys with its own finalizer, so key bits need
// not be well mixed, only stable and distinct.
type keyHash uint64

const (
	fnvOffset64 keyHash = 14695981039346656037
	fnvPrime64  keyHash = 1099511628211
)

func (h keyHash) write(p []byte) keyHash {
	for len(p) >= 8 {
		h ^= keyHash(binary.LittleEndian.Uint64(p))
		h *= fnvPrime64
		p = p[8:]
	}
	for _, c := range p {
		h ^= keyHash(c)
		h *= fnvPrime64
	}
	return h
}

func (h keyHash) writeLen(n int) keyHash {
	h ^= keyHash(uint64(n))
	return h * fnvPrime64
}

// result is a decided prefix-hash key, not yet encoded: only the one a scan
// returns is encoded, by encoded, so deciding a key never allocates.
func (h keyHash) result() Result { return Result{Tier: TierPrefixHash, hash: h} }

// encoded returns r with its key hex-encoded and the raw hash dropped, so
// callers see only Key, Tier and Reason.
func (r Result) encoded() Result {
	if r.Tier != TierPrefixHash {
		return r
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(r.hash))
	return Result{Key: hex.EncodeToString(buf[:]), Tier: TierPrefixHash}
}

// addMessage hashes the first maxHashedMessageBytes of one message's raw JSON
// and its full length. Earlier turns are re-sent byte-identically, so the
// key is the same on every turn of a conversation.
func (s *scanner) addMessage(h keyHash, raw []byte) keyHash {
	n := min(len(raw), maxHashedMessageBytes)
	s.work += n * hashByteQ
	return h.write(raw[:n]).writeLen(len(raw))
}

// prefixKey keys a completions prompt on its first promptPrefixBytes.
func (s *scanner) prefixKey(content []byte) Result {
	if len(content) == 0 {
		return none(ReasonNoKey)
	}
	n := min(len(content), promptPrefixBytes)
	s.work += n * hashByteQ
	return fnvOffset64.write(content[:n]).result()
}

// readMessages reads the messages value at b[i]. The key hashes every leading
// preamble message (role system or developer) and the first other message.
// The returned Result is the zero value when the key could not be decided
// (scan error or budget spent before the key was complete); otherwise it is
// returned even when the budget ran out while skipping the remaining turns.
func (s *scanner) readMessages(i int) (Result, int, scanErr) {
	b := s.b
	if i >= len(b) {
		return Result{}, i, s.fail()
	}
	switch b[i] {
	case '[':
	case 'n':
		end, err := s.skipScalar(i)
		if err == scanOK && !bytes.Equal(b[i:end], []byte("null")) {
			err = scanSyntax
		}
		if err != scanOK {
			return Result{}, end, err
		}
		return none(ReasonNoKey), end, scanOK
	default: // a string, number or object: vLLM rejects it
		end, err := s.skipValue(i)
		return none(telemetry.ReasonUnparseableBody), end, err
	}
	start := i
	i = s.skipSpace(i + 1)
	if s.spent() {
		return Result{}, i, scanBudget
	}
	if i < len(b) && b[i] == ']' {
		return none(ReasonEmptyMessages), i + 1, scanOK
	}
	h, keyed := fnvOffset64, false
	for {
		// Charge each turn up front and stop once the budget is spent: a
		// run of short messages must not be walked for free.
		s.work += elementQ
		if s.spent() {
			return decided(h, keyed), i, scanBudget
		}
		var (
			end int
			err scanErr
		)
		switch {
		case keyed:
			end, err = s.skipValue(i)
		case i < len(b) && b[i] == '{':
			var preamble bool
			if end, preamble, err = s.readMessage(i); err == scanOK {
				h = s.addMessage(h, b[i:end])
				if keyed = !preamble; keyed {
					s.keyDecided()
				}
			}
		default: // a turn that is not an object: vLLM rejects it
			end, err = s.skipContainer(start)
			return none(telemetry.ReasonUnparseableBody), end, err
		}
		if err != scanOK {
			return decided(h, keyed), end, err
		}
		i = s.skipSpace(end)
		if i >= len(b) {
			return decided(h, keyed), i, s.fail()
		}
		switch b[i] {
		case ',':
			i = s.skipSpace(i + 1)
		case ']':
			return h.result(), i + 1, scanOK
		default:
			return decided(h, keyed), i, s.fail()
		}
	}
}

// decided returns the key if it is complete, else the zero Result.
func decided(h keyHash, keyed bool) Result {
	if keyed {
		return h.result()
	}
	return Result{}
}

// readMessage walks the message object at b[i] and reports whether its
// top-level role is a preamble role. A repeated role key keeps its last value.
func (s *scanner) readMessage(i int) (end int, preamble bool, err scanErr) {
	end, err = s.walkObject(i, func(key []byte, val int) (int, scanErr) {
		end, err := s.skipValue(val)
		if err == scanOK && s.keyIs(key, keyRole) {
			role := s.b[val:end]
			preamble = s.keyIs(role, roleSystem) || s.keyIs(role, roleDeveloper)
		}
		return end, err
	})
	return end, preamble, err
}

// readPrompt reads the /v1/completions prompt value at b[i]: a string, a list
// of strings, a token-ID list or a list of token-ID lists. The key is the
// leading prefix of the text (or of the first list element), so a prompt
// that grows by appending keeps its key once it is promptPrefixBytes long.
// The prefix is decided from the first promptPrefixBytes alone, so the key
// is kept even when the budget runs out skipping the rest of the prompt.
func (s *scanner) readPrompt(i int) (Result, int, scanErr) {
	b := s.b
	if i >= len(b) {
		return Result{}, i, s.fail()
	}
	switch b[i] {
	case '"':
		res := s.stringPrefixKey(i)
		end, err := s.skipString(i)
		return res, end, err
	case '[':
	default: // null, number, object: nothing to key on
		end, err := s.skipValue(i)
		return none(ReasonNoKey), end, err
	}
	j := s.skipSpace(i + 1)
	if j >= len(b) || s.spent() {
		return Result{}, j, s.fail()
	}
	var res Result
	switch b[j] {
	case ']':
		return none(ReasonNoKey), j + 1, scanOK
	case '"': // list of strings: key on the first, skip the rest from its end
		res = s.stringPrefixKey(j)
		end, err := s.skipString(j)
		if err != scanOK {
			return res, end, err
		}
		end, err = s.skipDepth(end, 1)
		return res, end, err
	case '[': // list of token lists: key on the first
		res = s.tokenPrefixKey(j)
	default: // token IDs
		res = s.tokenPrefixKey(i)
	}
	end, err := s.skipContainer(i)
	return res, end, err
}

// stringPrefixKey keys on the first promptPrefixBytes of the string at b[i],
// reading no further than that. It returns the zero Result if the body ends
// inside that prefix.
func (s *scanner) stringPrefixKey(i int) Result {
	b := s.b
	window := min(len(b), i+2+promptPrefixBytes) // the quotes and the prefix
	sub := scanner{b: b[:window], limit: s.left()}
	end, err := sub.skipString(i)
	s.work += sub.work
	switch {
	case err == scanOK:
		return s.prefixKey(b[i+1 : end-1])
	case err == scanSyntax && window < len(b): // longer than the prefix
		return s.prefixKey(b[i+1 : i+1+promptPrefixBytes])
	}
	// The budget ran out inside the prefix, or the body ended there: the
	// string's end is not known, so its prefix is not either (bug #1397).
	return Result{}
}

// tokenPrefixKey keys on the first promptPrefixBytes of the flat token-ID
// list at b[i] == '['; its first ']' closes it.
func (s *scanner) tokenPrefixKey(i int) Result {
	content := s.b[i+1 : min(len(s.b), i+1+promptPrefixBytes)]
	if k := bytes.IndexByte(content, ']'); k >= 0 {
		content = content[:k]
	}
	return s.prefixKey(bytes.TrimRight(content, " \t\r\n"))
}
