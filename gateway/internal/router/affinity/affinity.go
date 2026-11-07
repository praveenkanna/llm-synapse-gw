// Package affinity extracts conversation affinity keys from incoming requests.
//
// It implements a tiered extraction model:
//   - Tier 1 (Header): X-Session-Id or X-Conversation-Id (no body scan), up to
//     MaxSessionHeaderBytes. A longer header is ignored for routing, as if it
//     were absent, and Result.HeaderTooLong says so; it is still forwarded
//     upstream as it came.
//   - Tier 2 (Prefix Hash): a hash of the conversation's leading turns, read
//     from the raw body bytes without decoding or re-serializing it.
//   - Tier 3 (Fail Open): no key (TierNone with a reason); the caller falls
//     back to round-robin and the request is forwarded unchanged.
//
// Tier 2 for /v1/chat/completions hashes every leading preamble message (a
// top-level "role" of "system" or "developer") and the first other message:
// the first 8 KiB of each message's raw JSON plus its length. Earlier turns
// are re-sent byte-identically, so every turn of a conversation gets the same
// key. Only the first 8 KiB of a message count, so conversations whose turns
// share their first 8 KiB but differ later, at the same length, share a key.
//
// Tier 2 for /v1/completions hashes the first 2 KiB of "prompt" (of its first
// element when it is a list). A multi-turn completions prompt grows by
// appending, so it keeps its key once it is 2 KiB long; shorter prompts get a
// new key each turn. The tradeoff: conversations that share a preamble longer
// than 2 KiB (a common template) all get one key, so they land on one replica
// until bounded load spills them. Callers that need per-conversation
// stickiness on completions should send X-Session-Id.
//
// Top-level keys are matched case-sensitively, and a second top-level
// "messages" or "prompt" member fails open (ReasonDuplicateKey). Each call is bounded by a fixed work
// budget, about 28 µs on an Apple M1 for the costliest shapes: every string,
// object member, message and structural byte is charged, and every loop
// stops once the budget is spent (M1-22), whatever the body is made of. The
// prices make a unit cost about the same time on every path, so the budget
// bounds the time too. The per-CPU maxima of the cost sweep (string
// lengths 0-40 in every position, scalars, the gated shapes) are in the
// M1-22 results file. Two strings with nothing between them are never valid
// JSON, so the scan stops there and fails open as telemetry.ReasonUnparseableBody.
// Skipping long strings with bytes.IndexByte costs a fraction of scanning
// structure or escapes byte by byte. If the budget runs out before the key
// is decided the result is TierNone with ReasonScanBudget and the request is
// routed round-robin. Once a messages key is decided, the scan goes on for a
// small post-key cap only, looking for a duplicate: a second top-level
// "messages" or "prompt" member anywhere the scan reaches fails open with
// ReasonDuplicateKey at its key (M1-25), so the key is computed at most once
// per field; members beyond the cap are not read and the key stands. So
// bodies in the usual SDK order (messages, then tools), long conversations
// and runs of duplicates cost little more than reading their key.
//
// Measured limits (the largest of each shape that still gets a key; bigger
// fails open; 1 KiB = 1,024 bytes). Everything that must be read before the
// key is decided counts: members ahead of "messages", and the leading
// messages themselves.
//   - dense "tools" (JSON schemas) ahead of "messages": about 32.6 KiB
//   - system prompt of escaped JSON examples (a backslash every few bytes):
//     about 37.4 KiB
//   - system prompt of prose (a quoted phrase every few lines): about 490 KiB
//   - short content parts in a leading message: about 565 (26.1 KiB)
//   - an inline base64 image in a leading message: about 1.24 MiB
//   - short system messages ahead of the user turn: about 334
//   - short members ahead of "messages" ("k":"v"): about 971
//   - items with a 9-16 byte stretch of structural bytes between strings,
//     skipped ("k":12345678, or "k":[1,2,3], in an object ahead of the
//     messages, "",1234567, in an array): about 1,190-1,250, down from
//     2,120-2,300 without runQ. That price pays for a fixed step of about
//     9 ns at 9 bytes by failing open sooner; it does not remove it (M1-28).
//     "strict":true, and dense tools have no such stretch.
//
// The dense-tools guarantee is 29 KiB: it keys from at most 1/1.1 of the
// budget, so a 10% price rise cannot take it away (TestScanBudget, M1-24,
// M1-28).
//
// The budget was 64K units (about 59.3 KiB, 63.5 KiB, 884 KiB, 1,050 parts
// and 2.0 MiB) until M1-21 cut it for AC-M1-5 (docs/review.md #13). M1-22
// made it a hard bound on every path and priced each path by its time,
// which moved the limits above from 36.9 KiB, 39.7 KiB, 553 KiB, 650 parts
// and 1.24 MiB. Callers that need stickiness for larger bodies send
// X-Session-Id of at most MaxSessionHeaderBytes, which is never scanned.
//
// Members after "messages" (e.g. "tools" as openai-python sends them) and
// turns after the first user message never prevent a key.
package affinity

import (
	"net/http"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// Tier identifies which tier resolved the affinity key.
type Tier string

const (
	// TierHeader indicates the key was provided explicitly via an HTTP header.
	TierHeader Tier = "header"
	// TierPrefixHash indicates the key was derived from hashing the conversation's initial turn(s).
	TierPrefixHash Tier = "prefix_hash"
	// TierNone indicates no key could be extracted; routing should fail open to round-robin.
	TierNone Tier = "none"
)

const (
	// ReasonNoKey indicates the request had no session header and no usable prompt/messages
	// (absent, null, or an empty prompt).
	ReasonNoKey = "no_affinity_key"
	// ReasonEmptyMessages indicates the messages array was present but empty.
	ReasonEmptyMessages = "empty_messages"
	// ReasonScanBudget indicates the work budget ran out before a key was decided.
	ReasonScanBudget = "key_scan_budget"
	// ReasonDuplicateKey indicates a second top-level "messages" or "prompt"
	// member (M1-25).
	ReasonDuplicateKey = "duplicate_key"
	// ReasonSessionHeaderTooLong is the stage affinity_header reason for a
	// session header over MaxSessionHeaderBytes (M1-26). It is never a
	// Result's Reason: Result.HeaderTooLong carries it, beside whatever the
	// next tier decided.
	ReasonSessionHeaderTooLong = "session_header_too_long"
)

const (
	// HeaderSessionID is the primary session identifier header.
	HeaderSessionID = "X-Session-Id"
	// HeaderConversationID is the secondary alias for session identifier.
	HeaderConversationID = "X-Conversation-Id"

	// MaxSessionHeaderBytes is the longest session header used as a key. The
	// ring hashes a key byte by byte, so without a limit the route decision
	// grows with the header: 1 MiB cost about 1.3 ms against AC-M1-5's 100 µs
	// (bug #1137). It is a fixed bound on an ID, not a capacity to calibrate,
	// so it is not a config knob; raising it needs AC-M1-5 measured again.
	MaxSessionHeaderBytes = 256
)

var (
	keyMessages = []byte("messages")
	keyPrompt   = []byte("prompt")
)

// Result represents the outcome of affinity key extraction.
type Result struct {
	Key    string
	Tier   Tier
	Reason string
	// HeaderTooLong reports that a session header over MaxSessionHeaderBytes
	// was ignored, whichever tier then gave the key (or none did).
	HeaderTooLong bool

	hash keyHash // a decided prefix-hash key while scanning; zero once encoded
}

// DefaultKeyScanBudget is the work, in units, one Extract call may spend
// reading the body before its messages key is decided (router.key_scan_budget).
const DefaultKeyScanBudget = preKeyBudget / unit

// Extract extracts an affinity key from the request and body bytes with the
// default scan budget. It never modifies body.
func Extract(r *http.Request, body []byte) Result {
	return ExtractBudget(r, body, DefaultKeyScanBudget)
}

// ExtractBudget is Extract with the body scan limited to budget work units
// before the key is decided; a budget of 0 or less means
// DefaultKeyScanBudget. A body that spends it fails open
// (ReasonScanBudget). It never modifies body.
func ExtractBudget(r *http.Request, body []byte, budget int) Result {
	// Tier 1: Check HTTP headers (fast path).
	key, tooLong := headerKey(r)
	if key != "" {
		return Result{Key: key, Tier: TierHeader, HeaderTooLong: tooLong}
	}
	res := bodyKey(body, budget)
	res.HeaderTooLong = tooLong
	return res
}

// headerKey returns the first session header that can be a key, in tier
// order, and whether one before it was over MaxSessionHeaderBytes. An
// over-long header counts as absent: only its length is read, so its cost
// does not grow with it. The header's first value is the only one read.
func headerKey(r *http.Request) (key string, tooLong bool) {
	if r == nil {
		return "", false
	}
	for _, name := range [...]string{HeaderSessionID, HeaderConversationID} {
		v := r.Header.Get(name)
		if len(v) > MaxSessionHeaderBytes {
			tooLong = true
			continue
		}
		if v != "" {
			return v, tooLong
		}
	}
	return "", tooLong
}

// bodyKey is Tier 2: it hashes the leading turn(s) read from the body bytes,
// or fails open.
func bodyKey(body []byte, budget int) Result {
	if len(body) == 0 {
		return none(ReasonNoKey)
	}
	if budget <= 0 {
		budget = DefaultKeyScanBudget
	}
	return extractFromBody(body, budget*unit)
}

// extractFromBody walks the top-level object, reading the "messages" and
// "prompt" values and skipping the rest, within budget work units
// (postKeyBudget past a decided messages key). A second "messages" or
// "prompt" member stops the scan and fails open (ReasonDuplicateKey): JSON
// parsers disagree on which duplicate wins, real clients never send one, and
// reading each again would repeat the key computation (M1-25).
func extractFromBody(body []byte, budget int) Result {
	s := scanner{b: body, limit: budget}
	return s.extract()
}

// extract scans s.b for the key.
func (s *scanner) extract() Result {
	body := s.b
	// A zero Result after a member was seen means its value was not decided
	// (the budget ran out reading it).
	var (
		msgs, prompt       Result
		sawMsgs, sawPrompt bool
	)
	i := s.skipSpace(0)
	if s.spent() {
		return none(ReasonScanBudget)
	}
	if i >= len(body) || body[i] != '{' {
		return none(telemetry.ReasonUnparseableBody)
	}
	_, err := s.walkObject(i, func(key []byte, val int) (int, scanErr) {
		var (
			end int
			err scanErr
		)
		switch {
		case s.keyIs(key, keyMessages):
			if sawMsgs {
				return val, scanDuplicate
			}
			msgs, end, err = s.readMessages(val)
			sawMsgs = true
			if msgs.Tier == TierPrefixHash {
				s.keyDecided() // also when every message was preamble
			}
		case s.keyIs(key, keyPrompt):
			if sawPrompt {
				return val, scanDuplicate
			}
			prompt, end, err = s.readPrompt(val)
			sawPrompt = true
		default:
			end, err = s.skipValue(val)
		}
		return end, err
	})
	switch err {
	case scanSyntax:
		return none(telemetry.ReasonUnparseableBody)
	case scanDuplicate:
		return none(ReasonDuplicateKey)
	case scanBudget:
		undecided := (sawMsgs && msgs == (Result{})) ||
			(!sawMsgs && (!sawPrompt || prompt == (Result{})))
		if undecided {
			return none(ReasonScanBudget)
		}
	}
	return pick(msgs, prompt).encoded()
}

// pick prefers a messages key, then a prompt key, then the reason messages
// had none, then prompt's.
func pick(msgs, prompt Result) Result {
	switch {
	case msgs.Tier == TierPrefixHash:
		return msgs
	case prompt.Tier == TierPrefixHash:
		return prompt
	case msgs != (Result{}):
		return msgs
	case prompt != (Result{}):
		return prompt
	}
	return none(ReasonNoKey)
}
