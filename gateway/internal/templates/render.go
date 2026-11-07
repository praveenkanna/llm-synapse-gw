package templates

import (
	"fmt"
	"slices"
	"strings"
)

// RenderedMessage is one chat message as a caller sends it.
type RenderedMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Render builds a version's messages from vars, which must give every
// declared variable and nothing else. Values are substituted verbatim;
// static text is copied literally.
func Render(s Spec, vars map[string]string) ([]RenderedMessage, error) {
	declared := make(map[string]bool, len(s.Variables))
	var missing, unknown []string
	for _, v := range s.Variables {
		declared[v.Name] = true
		if _, ok := vars[v.Name]; !ok {
			missing = append(missing, v.Name)
		}
	}
	for name := range vars {
		if !declared[name] {
			unknown = append(unknown, name)
		}
	}
	if len(missing) > 0 || len(unknown) > 0 {
		return nil, varsError(missing, unknown)
	}

	out := make([]RenderedMessage, len(s.Messages))
	var b strings.Builder
	for i, m := range s.Messages {
		b.Reset()
		for _, seg := range m.Segments {
			if seg.Kind == KindStatic {
				b.WriteString(seg.Text)
				continue
			}
			if err := substitute(&b, seg.Text, vars); err != nil {
				return nil, err
			}
		}
		out[i] = RenderedMessage{Role: m.Role, Content: b.String()}
	}
	return out, nil
}

// substitute writes text to b with each placeholder replaced by its value.
func substitute(b *strings.Builder, text string, vars map[string]string) error {
	from := 0
	for {
		start, end, name := nextPlaceholder(text, from)
		if start < 0 {
			b.WriteString(text[from:])
			return nil
		}
		val, ok := vars[name]
		if !ok {
			return fmt.Errorf("render: placeholder {{%s}} is not a declared variable", name)
		}
		b.WriteString(text[from:start])
		b.WriteString(val)
		from = end
	}
}

func varsError(missing, unknown []string) error {
	slices.Sort(missing)
	slices.Sort(unknown)
	var parts []string
	for _, n := range missing {
		parts = append(parts, fmt.Sprintf("missing variable %.64q", n))
	}
	for _, n := range unknown {
		parts = append(parts, fmt.Sprintf("unknown variable %.64q", n))
	}
	return fmt.Errorf("render: %s", strings.Join(parts, "; "))
}
