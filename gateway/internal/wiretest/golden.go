// Package wiretest proves the gateway is wire-transparent (AC-M0-1, AC-M0-2).
//
// Golden requests in gateway/testdata/wire are sent byte for byte twice: once
// straight to mockllm, as a caller talking to vLLM would, and once through the
// gateway. The tests assert that
//   - mockllm receives exactly the golden body bytes on both paths;
//   - upstream headers match the direct call, except for the two intended
//     differences: Accept-Encoding: identity on streaming requests (so nothing
//     compresses an SSE stream) and the gateway's own Traceparent span;
//   - responses match the direct call in status, headers, framing and body,
//     once volatile fields (response IDs, timestamps) are normalised, with
//     X-Accel-Buffering: no as the one added header on streams.
//
// This file holds the golden loader and normalisation; check.go holds the
// sender and the assertions, shared by the in-process tests beside them and
// the dev-stack run in gateway/integration (which sends through nginx, so
// Expect names what nginx adds or consumes).
package wiretest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
)

// ManifestFile names the golden index inside the golden directory.
const ManifestFile = "manifest.json"

// Case is one golden request as described in the manifest.
type Case struct {
	Name   string            `json:"name"`
	Method string            `json:"method"`
	Path   string            `json:"path"`
	File   string            `json:"body"`   // body file name; empty for no body
	Stream bool              `json:"stream"` // the caller asked for SSE
	Header map[string]string `json:"header"`
}

// Golden is a Case with its body loaded.
type Golden struct {
	Case
	Body []byte // exact bytes to send; nil when the case has no body
}

// HTTPHeader returns the case's caller headers in canonical form.
func (g Golden) HTTPHeader() http.Header {
	h := make(http.Header, len(g.Header))
	for k, v := range g.Header {
		h.Set(k, v)
	}
	return h
}

type manifest struct {
	Cases []Case `json:"cases"`
}

// Load reads the manifest in dir and every body it names. Bodies are returned
// exactly as stored on disk.
func Load(dir string) ([]Golden, error) {
	raw, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	var m manifest
	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	if len(m.Cases) == 0 {
		return nil, errors.New("manifest has no cases")
	}
	seen := make(map[string]bool, len(m.Cases))
	out := make([]Golden, 0, len(m.Cases))
	for _, c := range m.Cases {
		if err := c.validate(seen); err != nil {
			return nil, err
		}
		g := Golden{Case: c}
		if c.File != "" {
			if g.Body, err = os.ReadFile(filepath.Join(dir, filepath.Base(c.File))); err != nil {
				return nil, fmt.Errorf("case %s: read body: %w", c.Name, err)
			}
		}
		out = append(out, g)
	}
	return out, nil
}

func (c Case) validate(seen map[string]bool) error {
	switch {
	case c.Name == "":
		return errors.New("manifest case without a name")
	case seen[c.Name]:
		return fmt.Errorf("duplicate case %s", c.Name)
	case c.Method != http.MethodGet && c.Method != http.MethodPost:
		return fmt.Errorf("case %s: method %q must be GET or POST", c.Name, c.Method)
	case c.Path == "" || c.Path[0] != '/':
		return fmt.Errorf("case %s: path %q must start with /", c.Name, c.Path)
	case c.Method == http.MethodPost && c.File == "":
		return fmt.Errorf("case %s: POST needs a body file", c.Name)
	}
	seen[c.Name] = true
	return nil
}

// Volatile response fields: generated IDs and creation timestamps differ
// between two otherwise identical calls.
var (
	volatileID      = regexp.MustCompile(`"id":"(chatcmpl|cmpl)-[0-9A-Za-z]+"`)
	volatileCreated = regexp.MustCompile(`"created":[0-9]+`)
)

// Normalize replaces response IDs and "created" timestamps with fixed
// placeholders of the same byte length, so two responses to the same request
// compare byte for byte. Everything else, including key order and
// whitespace, is left untouched.
func Normalize(b []byte) []byte {
	b = volatileID.ReplaceAllFunc(b, func(m []byte) []byte { return fill(m, len(`"id":"`), 'X') })
	return volatileCreated.ReplaceAllFunc(b, func(m []byte) []byte { return fill(m, len(`"created":`), '0') })
}

// fill returns a copy of m with every byte from offset on (except a closing
// quote) replaced by c.
func fill(m []byte, offset int, c byte) []byte {
	out := bytes.Clone(m)
	for i := offset; i < len(out); i++ {
		if out[i] != '"' {
			out[i] = c
		}
	}
	return out
}

// SSE framing, as vLLM writes it.
const (
	sseDataPrefix = "data: "
	sseDone       = "[DONE]"
	sseSeparator  = "\n\n"
)

// SSEEvents checks that body is a well-framed OpenAI SSE stream: one or more
// "data: <json object>\n\n" events followed by exactly one final
// "data: [DONE]\n\n". It returns the JSON payloads in order.
func SSEEvents(body []byte) ([][]byte, error) {
	if !bytes.HasSuffix(body, []byte(sseDataPrefix+sseDone+sseSeparator)) {
		return nil, errors.New("stream does not end with data: [DONE]")
	}
	parts := bytes.Split(bytes.TrimSuffix(body, []byte(sseSeparator)), []byte(sseSeparator))
	events := make([][]byte, 0, len(parts)-1)
	for i, p := range parts {
		payload, ok := bytes.CutPrefix(p, []byte(sseDataPrefix))
		if !ok {
			return nil, fmt.Errorf("event %d is not a data line: %q", i, p)
		}
		if i == len(parts)-1 {
			break // the [DONE] sentinel, checked above
		}
		if bytes.Equal(payload, []byte(sseDone)) {
			return nil, fmt.Errorf("event %d: [DONE] before the end of the stream", i)
		}
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(payload, &obj); err != nil {
			return nil, fmt.Errorf("event %d is not a JSON object: %w", i, err)
		}
		events = append(events, payload)
	}
	if len(events) == 0 {
		return nil, errors.New("stream has no data events")
	}
	return events, nil
}
