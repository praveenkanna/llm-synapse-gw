package templates

import (
	"slices"
	"strings"
)

// Prefix is a version's static prefix: the part of every render that is
// the same for every caller, and what the request path matches.
type Prefix struct {
	// Messages are the fully static messages before the first non-static
	// segment.
	Messages []RenderedMessage
	// Role is the role of the message holding the first non-static
	// segment; empty when the template is entirely static.
	Role string
	// Text is that message's static text before the segment; may be empty.
	Text string
}

// StaticPrefix returns s's static prefix. s is assumed valid: no static
// segment follows a non-static one.
func StaticPrefix(s Spec) Prefix {
	var p Prefix
	var b strings.Builder
	for _, m := range s.Messages {
		b.Reset()
		for _, seg := range m.Segments {
			if seg.Kind != KindStatic {
				p.Role, p.Text = m.Role, b.String()
				return p
			}
			b.WriteString(seg.Text)
		}
		p.Messages = append(p.Messages, RenderedMessage{Role: m.Role, Content: b.String()})
	}
	return p
}

// Bytes is the static content's total length.
func (p Prefix) Bytes() int {
	n := len(p.Text)
	for _, m := range p.Messages {
		n += len(m.Content)
	}
	return n
}

// Equal reports whether p and q are the same prefix.
func (p Prefix) Equal(q Prefix) bool {
	return p.Role == q.Role && p.Text == q.Text && slices.Equal(p.Messages, q.Messages)
}
