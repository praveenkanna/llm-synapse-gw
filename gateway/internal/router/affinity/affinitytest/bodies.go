// Package affinitytest holds the synthetic request bodies that the affinity
// extraction and whole-route latency benchmarks share (AC-M1-5), plus the
// quantile helpers they report with. It is imported only by tests and
// benchmarks; the bodies are synthetic, never captured prompts.
package affinitytest

import (
	"fmt"
	"strings"
	"time"
)

// Text returns n bytes of JSON-escaped prose, varied by seed: sentences with
// a \n line break each and a \"quoted\" phrase every few lines, like a
// system prompt.
func Text(n int, seed string) string {
	var sb strings.Builder
	for i := 0; sb.Len() < n; i++ {
		fmt.Fprintf(&sb, `Rule %d for %s: answer in a friendly, concise tone, cite the relevant policy section and never promise what the policy does not allow.\n`, i, seed)
		if i%4 == 3 {
			sb.WriteString(`When unsure, say \"I will check with the team\" and stop.\n`)
		}
	}
	return sb.String()[:n]
}

// EscapedJSON returns n bytes of a string holding escaped JSON examples
// (a backslash every few bytes): the costliest string content to skip.
func EscapedJSON(n int) string {
	return strings.Repeat(`{\"name\": \"get_weather\", \"arguments\": {\"city\": \"Paris\"}}\n`, n/64+1)[:n]
}

// DenseTools returns a JSON array of about n bytes of tool definitions: short
// strings and deep nesting, the costliest shape for a byte scanner. Each
// schema has a nested "messages" property and "role":"system" enum value, to
// prove that neither is mistaken for a top-level key or a message role.
func DenseTools(n int) string {
	var sb strings.Builder
	sb.WriteByte('[')
	for i := 0; sb.Len() < n; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, `{"type":"function","function":{"name":"lookup_%d","description":"Look up a record by id and return stock, location and the last update.","parameters":{"type":"object","properties":{"id":{"type":"string","description":"Record id, e.g. SKU-12345"},"messages":{"type":"array","items":{"type":"object","properties":{"role":{"type":"string","enum":["system","user"]}}}},"limit":{"type":"integer","minimum":1,"maximum":100}},"required":["id"]}}}`, i)
	}
	sb.WriteByte(']')
	return sb.String()
}

// Chat builds a chat body: an optional raw prefix of top-level members, a
// system prompt of sysBytes, then turns user/assistant pairs.
func Chat(prefix string, sysBytes, turns int) []byte {
	var sb strings.Builder
	sb.WriteString(`{"model":"meta-llama/Llama-3.1-8B-Instruct",`)
	sb.WriteString(prefix)
	fmt.Fprintf(&sb, `"messages":[{"role":"system","content":"%s"}`, Text(sysBytes, "sys"))
	for t := range turns {
		fmt.Fprintf(&sb, `,{"role":"user","content":"question %d: %s"}`, t, Text(200, "user"))
		if t < turns-1 {
			fmt.Fprintf(&sb, `,{"role":"assistant","content":"%s"}`, Text(400, "asst"))
		}
	}
	sb.WriteString(`],"stream":true}`)
	return []byte(sb.String())
}

// WithMember appends a raw top-level member to the JSON object body.
func WithMember(body []byte, member string) []byte {
	out := append([]byte(nil), body[:len(body)-1]...)
	return append(append(append(out, ','), member...), '}')
}

// ImageMessage is a chat body whose only message carries an inline base64
// image of n bytes.
func ImageMessage(n int) string {
	return `{"messages":[{"role":"user","content":[{"type":"text","text":"what is this"},{"type":"image_url","image_url":{"url":"data:image/png;base64,` +
		strings.Repeat("iVBORw0KGgo", n/11+1)[:n] + `"}}]}]}`
}

// Case is a named request body. Names ending in _failopen measure the
// fail-open path (the work budget runs out before a key is decided).
type Case struct {
	Name string
	Body []byte
}

// LatencyCases are the sizes AC-M1-5 is measured at (M1-14): 1, 32 and 256
// KiB system prompts and 1 MiB of tools ahead of the messages.
func LatencyCases() []Case {
	return []Case{
		{"system_1KB", Chat("", 1<<10, 3)},
		{"system_32KB", Chat("", 32<<10, 3)},
		{"system_256KB", Chat("", 256<<10, 3)},
		{"tools_1MB_before_messages_failopen", Chat(`"tools":`+DenseTools(1<<20)+`,`, 1<<10, 3)},
	}
}

// WorstCases are shapes chosen to cost the most per byte or to be as large
// as a body may be (32 MiB by default); each must still finish within the
// work budget.
func WorstCases() []Case {
	long := Chat("", 1<<10, 3000) // ~1.8 MB of short turns after the key
	esc := []byte(`{"messages":[{"role":"system","content":"` + EscapedJSON(256<<10) + `"},{"role":"user","content":"hi"}]}`)
	return append([]Case{
		{"tools_1MB_after_messages", WithMember(Chat("", 1<<10, 3), `"tools":`+DenseTools(1<<20))},
		{"conversation_1.8MB", long},
		{"system_256KB_escaped_json_failopen", esc},
		{"tools_4MB_before_messages_failopen", Chat(`"tools":`+DenseTools(4<<20)+`,`, 1<<10, 3)},
		{"string_16MB_before_messages_failopen", []byte(`{"x":"` + strings.Repeat("A", 16<<20) + `","messages":[{"role":"user","content":"Q"}]}`)},
		{"image_16MB_in_first_user_message_failopen", []byte(ImageMessage(16 << 20))},
	}, ShortTokenCases()...)
}

// ShortTokenCases are bodies made only of keys and strings of 16 bytes or
// less, the scanner's inline path (M1-22): many top-level members ahead of
// the messages (1.6 MB and 32 MiB), many short system messages ahead of the
// user turn, many duplicate "messages" members after a decided key, and
// escaped short keys. Each must stop within the work budget.
func ShortTokenCases() []Case {
	return []Case{
		{"members_1.6MB_before_messages_failopen", ShortMembers(`"k":"v",`, 200_000)},
		{"members_32MB_before_messages_failopen", ShortMembers(`"k":"v",`, (32<<20)/8)},
		{"escaped_keys_1.6MB_before_messages_failopen", ShortMembers(`"`+Escaped("k")+`":"v",`, 125_000)},
		{"escaped_near_miss_keys_1.6MB_before_messages_failopen", ShortMembers(`"`+Escaped("messagez")+`":1,`, 31_000)},
		{"system_messages_50k_failopen", ShortSystemMessages(50_000)},
		{"duplicate_messages_50k_failopen", DuplicateMessages(`[{"role":"user","content":"x"}]`, 50_000)},
		// Repeated top-level keys (M1-25): each fails open at its second
		// occurrence, so the key is computed at most once per field.
		{"duplicate_prompt_tokens_1.6MB_failopen", DuplicateMembers(`"prompt":[1],`, 200_000)},
		{"duplicate_prompt_token_lists_1.6MB_failopen", DuplicateMembers(`"prompt":[[1]],`, 180_000)},
		{"duplicate_prompt_token_pairs_1.6MB_failopen", DuplicateMembers(`"prompt":[1,1],`, 150_000)},
		{"duplicate_prompt_empty_1.6MB_failopen", DuplicateMembers(`"prompt":[],`, 200_000)},
		{"duplicate_messages_objects_1.6MB_failopen", DuplicateMembers(`"messages":{},`, 120_000)},
		{"empty_strings_1.6MB_before_messages_failopen", ArrayAhead(`"",`, 533_000)},
		{"adjacent_empty_strings_1.6MB_before_messages_failopen", ArrayAhead(`""`, 800_000)},
		{"empty_objects_1.6MB_in_content_failopen", EmptyObjectsInContent(533_000)},
		{"adjacent_3byte_strings_1.6MB_before_messages_failopen", ArrayAhead(`"abc"`, 320_000)},
		{"short_arrays_1.6MB_before_messages_failopen", ArrayAhead(`["abc"],`, 200_000)},
		// Runs of empty containers (M1-25 review: 1.33x per unit on an M1).
		{"empty_arrays_1.6MB_before_messages_failopen", ArrayAhead(`[],`, 533_000)},
		{"short_objects_1.6MB_before_messages_failopen", ArrayAhead(`{"abc":"abc"},`, 115_000)},
		{"bool_members_1.6MB_before_messages_failopen", ShortMembers(`"abc":true,`, 145_000)},
		// The costliest points of the cost sweep (M1-22): strings of 11 to
		// 15 bytes, the longest that close within the inline window, as
		// array elements and as prompt-list items (which key on the prompt).
		{"str15_array_1.6MB_before_messages_failopen", ArrayAhead(`"`+plain(15)+`",`, 1_600_000/18)},
		{"str13_array_1.6MB_before_messages_failopen", ArrayAhead(`"`+plain(13)+`",`, 1_600_000/16)},
		{"str15_prompt_1.6MB", PromptList(plain(15), 1_600_000/18)},
		{"str11_prompt_1.6MB", PromptList(plain(11), 1_600_000/14)},
		// Stretches of structural bytes between strings (M1-28): skipDepth
		// reads up to 8 bytes one by one and hands longer ones to skipRun, so
		// these sit on either side of that line. Nothing above passes 3 bytes.
		{"stretch4_1.6MB_before_messages_failopen", ArrayAhead(`"",12,`, 1_600_000/6)},
		{"stretch6_1.6MB_before_messages_failopen", ArrayAhead(`"",1234,`, 1_600_000/8)},
		{"stretch7_1.6MB_before_messages_failopen", ArrayAhead(`"",12345,`, 1_600_000/9)},
		{"stretch9_1.6MB_before_messages_failopen", ArrayAhead(`"",1234567,`, 1_600_000/11)},
		{"stretch9_brackets_1.6MB_before_messages_failopen", ArrayAhead(`"",[1,2,3],`, 1_600_000/11)},
		// A 7-byte stretch of brackets stays in the byte loop, on the per-byte
		// class branch that made the Cascade Lake pods' Route and Extract
		// binaries differ 2.4x (M1-24): timed in both, it shows that again.
		{"stretch7_brackets_1.6MB_before_messages_failopen", ArrayAhead(`"",[],[],`, 1_600_000/9)},
		{"strict_members_1.6MB_before_messages_failopen", SkippedMembers(`"k":true,`, 1_600_000/9)},
	}
}

// plain returns n bytes of plain text (no quote or backslash).
func plain(n int) string {
	return strings.Repeat("abcdefghijklmnopqrstuvwxyz0123456789", n/36+1)[:n]
}

// PromptList returns a completions body whose prompt is a list of n copies
// of the string s after a first prompt, which the key is taken from.
func PromptList(s string, n int) []byte {
	return []byte(`{"prompt":["first prompt text",` + strings.Repeat(`"`+s+`",`, n) + `""]}`)
}

// bs is one backslash, built at run time so that no editor or tool can turn
// an escape written in this file into the character it encodes.
var bs = string(rune(92))

// Escaped returns s with every byte written as a JSON \u00XX escape (lower
// case hex), e.g. Escaped("k") is the 6 bytes of a \u006b escape.
func Escaped(s string) string {
	var sb strings.Builder
	for i := 0; i < len(s); i++ {
		fmt.Fprintf(&sb, "%su%04x", bs, s[i])
	}
	return sb.String()
}

// ArrayAhead returns a chat body whose first member is an array holding n
// copies of item (raw JSON, e.g. a string and a comma), ahead of messages.
// The items need not be valid JSON together: adjacent strings test the
// scanner on bytes vLLM would reject.
func ArrayAhead(item string, n int) []byte {
	return []byte(`{"x":[` + strings.Repeat(item, n) + `""],"messages":[{"role":"user","content":"Q"}]}`)
}

// SkippedMembers returns a chat body whose first member is an object holding
// n copies of member (raw JSON), ahead of messages, so they are skipped as one
// value rather than walked.
func SkippedMembers(member string, n int) []byte {
	return []byte(`{"x":{` + strings.Repeat(member, n) + `"z":0},"messages":[{"role":"user","content":"Q"}]}`)
}

// EmptyObjectsInContent returns a chat body whose only message carries n
// empty objects as content parts, so the key needs all of them.
func EmptyObjectsInContent(n int) []byte {
	return []byte(`{"messages":[{"role":"user","content":[` + strings.Repeat(`{},`, n) + `{}]}]}`)
}

// ShortMembers returns a chat body with n copies of the raw top-level
// member text member (ending in a comma) ahead of a one-turn messages.
func ShortMembers(member string, n int) []byte {
	return []byte(`{"model":"m",` + strings.Repeat(member, n) + `"messages":[{"role":"user","content":"Q"}]}`)
}

// ShortSystemMessages returns a chat body with n short system messages
// ahead of the user turn, so the key needs all of them.
func ShortSystemMessages(n int) []byte {
	return []byte(`{"messages":[` + strings.Repeat(`{"role":"system","content":"s"},`, n) + `{"role":"user","content":"Q"}]}`)
}

// DuplicateMembers returns a body of n copies of member (a top-level member
// with its trailing comma) followed by a user turn.
func DuplicateMembers(member string, n int) []byte {
	return []byte(`{` + strings.Repeat(member, n) + `"model":"m"}`)
}

// DuplicateMessages returns a body with n "messages" members, each holding
// the raw JSON array msgs.
func DuplicateMessages(msgs string, n int) []byte {
	return []byte(`{` + strings.Repeat(`"messages":`+msgs+`,`, n) + `"model":"m"}`)
}

// Quantile returns the q-quantile of sorted (nearest rank, rounding down).
func Quantile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[min(len(sorted)-1, int(q*float64(len(sorted))))]
}

// Micros converts d to microseconds.
func Micros(d time.Duration) float64 { return float64(d) / float64(time.Microsecond) }
