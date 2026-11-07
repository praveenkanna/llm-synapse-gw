package templates

import (
	"fmt"
	"slices"
)

// Change ops and fields.
const (
	OpAdded   = "added"
	OpRemoved = "removed"
	OpChanged = "changed"

	FieldModel     = "model"
	FieldVariables = "variables"
	FieldMessage   = "message"
	FieldRole      = "role"
	FieldSegment   = "segment"
	FieldKind      = "kind"
	FieldText      = "text"
)

// DiffResult is how one version differs from another.
type DiffResult struct {
	// PrefixChanged reports that the static prefix differs: the change
	// breaks every cached prefix of the version.
	PrefixChanged bool     `json:"prefix_changed"`
	Changes       []Change `json:"changes"`
}

// Change is one difference. From and To are a model, variable name, role
// or kind, or a segment's text for a text change. An added or removed
// message carries its role; an added or removed segment its kind.
type Change struct {
	Path  string `json:"path"`
	Op    string `json:"op"`
	Field string `json:"field"`
	From  string `json:"from,omitempty"`
	To    string `json:"to,omitempty"`
}

// Diff compares two specs by message and segment index.
func Diff(from, to Spec) DiffResult {
	d := DiffResult{
		PrefixChanged: !StaticPrefix(from).Equal(StaticPrefix(to)),
		Changes:       []Change{},
	}
	if from.Model != to.Model {
		d.add("model", OpChanged, FieldModel, from.Model, to.Model)
	}
	d.variables(from.Variables, to.Variables)
	for i := range max(len(from.Messages), len(to.Messages)) {
		path := fmt.Sprintf("messages[%d]", i)
		switch {
		case i >= len(from.Messages):
			d.add(path, OpAdded, FieldMessage, "", to.Messages[i].Role)
		case i >= len(to.Messages):
			d.add(path, OpRemoved, FieldMessage, from.Messages[i].Role, "")
		default:
			d.message(i, from.Messages[i], to.Messages[i])
		}
	}
	return d
}

func (d *DiffResult) add(path, op, field, from, to string) {
	d.Changes = append(d.Changes, Change{Path: path, Op: op, Field: field, From: from, To: to})
}

func (d *DiffResult) variables(from, to []Variable) {
	names := func(vs []Variable) []string {
		out := make([]string, 0, len(vs))
		for _, v := range vs {
			out = append(out, v.Name)
		}
		slices.Sort(out)
		return slices.Compact(out)
	}
	a, b := names(from), names(to)
	for _, n := range a {
		if _, found := slices.BinarySearch(b, n); !found {
			d.add("variables", OpRemoved, FieldVariables, n, "")
		}
	}
	for _, n := range b {
		if _, found := slices.BinarySearch(a, n); !found {
			d.add("variables", OpAdded, FieldVariables, "", n)
		}
	}
}

func (d *DiffResult) message(i int, from, to Message) {
	if from.Role != to.Role {
		d.add(fmt.Sprintf("messages[%d]", i), OpChanged, FieldRole, from.Role, to.Role)
	}
	for j := range max(len(from.Segments), len(to.Segments)) {
		path := segPath(i, j)
		switch {
		case j >= len(from.Segments):
			d.add(path, OpAdded, FieldSegment, "", string(to.Segments[j].Kind))
		case j >= len(to.Segments):
			d.add(path, OpRemoved, FieldSegment, string(from.Segments[j].Kind), "")
		default:
			a, b := from.Segments[j], to.Segments[j]
			if a.Kind != b.Kind {
				d.add(path, OpChanged, FieldKind, string(a.Kind), string(b.Kind))
			}
			if a.Text != b.Text {
				d.add(path, OpChanged, FieldText, a.Text, b.Text)
			}
		}
	}
}
