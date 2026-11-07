package ingress

import "bytes"

// usageScan reads a response's token usage and finish reasons from the bytes
// as they are relayed, for matched template requests (M3-09, M3-10). It
// works on a JSON body and on an SSE stream of JSON events alike, and holds
// no response text: it tracks only whether it is inside a string, the first
// few bytes of the string it is in (to recognise the three keys it wants)
// and the number it is reading. A key name inside generated text cannot
// match: text quotes arrive escaped, so a quote inside it never closes a
// string.
//
// The last "prompt_tokens" and "completion_tokens" values win (vLLM's
// continuous usage stats repeat them cumulatively); any "finish_reason"
// equal to "length" marks the response as cut by max_tokens.
type usageScan struct {
	inString, escaped bool
	str               [usageKeyMax]byte
	strLen            int  // bytes of the current string kept in str
	strLong           bool // the current string is longer than str
	keyCandidate      bool // a string just closed; a ':' makes it a key
	want              usageField
	inNumber          bool
	number            int

	promptTokens, completionTokens int
	hasPrompt, hasCompletion       bool
	finishedLength                 bool
}

// usageKeyMax is the longest string usageScan keeps: its longest key.
const usageKeyMax = len("completion_tokens")

type usageField uint8

const (
	fieldNone usageField = iota
	fieldPrompt
	fieldCompletion
	fieldFinish
	fieldOther // a key we do not want: its value is skipped
)

// observe scans bytes just relayed to the caller.
func (u *usageScan) observe(p []byte) {
	for i := 0; i < len(p); {
		if u.inString {
			if !u.escaped && u.strLong {
				rem := p[i:]
				q := bytes.IndexByte(rem, '"')
				if q < 0 {
					k := len(rem) - 1
					for k >= 0 && rem[k] == '\\' {
						k--
					}
					u.escaped = (len(rem)-1-k)&1 == 1
					return
				}
				k := q - 1
				for k >= 0 && rem[k] == '\\' {
					k--
				}
				if (q-1-k)&1 == 1 {
					// Escaped quote inside string; stay in long string.
					i += q + 1
					continue
				}
				// Unescaped closing quote of a long string (cannot equal "length" or any key).
				u.inString = false
				if u.want == fieldFinish {
					u.want = fieldNone
				} else {
					u.want = fieldNone
					u.keyCandidate = true
				}
				i += q + 1
				continue
			}
			u.stringByte(p[i])
			i++
			continue
		}
		b := p[i]
		i++
		if u.inNumber {
			if b >= '0' && b <= '9' {
				if u.number < 1<<40 {
					u.number = u.number*10 + int(b-'0')
				}
				continue
			}
			u.endNumber()
		}
		switch {
		case b == '"':
			u.inString, u.strLen, u.strLong = true, 0, false
		case b == ':' && u.keyCandidate:
			u.keyCandidate = false
			u.want = u.keyField()
		case b == ' ' || b == '\t' || b == '\r' || b == '\n':
		case b >= '0' && b <= '9' && (u.want == fieldPrompt || u.want == fieldCompletion):
			u.inNumber, u.number = true, int(b-'0')
		default:
			u.keyCandidate = false
			u.want = fieldNone
		}
	}
}

func (u *usageScan) stringByte(b byte) {
	switch {
	case u.escaped:
		u.escaped = false
		u.strLong = true // an escaped key is none of ours
	case b == '\\':
		u.escaped = true
	case b == '"':
		u.inString = false
		if u.want == fieldFinish {
			if !u.strLong && string(u.str[:u.strLen]) == "length" {
				u.finishedLength = true
			}
			u.want = fieldNone
			return
		}
		u.want = fieldNone
		u.keyCandidate = true
	case u.strLen < len(u.str):
		u.str[u.strLen] = b
		u.strLen++
	default:
		u.strLong = true
	}
}

func (u *usageScan) keyField() usageField {
	if u.strLong {
		return fieldOther
	}
	switch string(u.str[:u.strLen]) {
	case "prompt_tokens":
		return fieldPrompt
	case "completion_tokens":
		return fieldCompletion
	case "finish_reason":
		return fieldFinish
	}
	return fieldOther
}

func (u *usageScan) endNumber() {
	u.inNumber = false
	switch u.want {
	case fieldPrompt:
		u.promptTokens, u.hasPrompt = u.number, true
	case fieldCompletion:
		u.completionTokens, u.hasCompletion = u.number, true
	}
	u.want = fieldNone
}

// usage returns the token counts seen, ok false unless both were.
func (u *usageScan) usage() (prompt, completion int, ok bool) {
	if u.inNumber {
		u.endNumber()
	}
	return u.promptTokens, u.completionTokens, u.hasPrompt && u.hasCompletion
}
