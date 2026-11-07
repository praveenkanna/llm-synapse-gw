package templates

import (
	"encoding/json"
	"reflect"
	"testing"
)

func seg(k Kind, text string) Segment { return Segment{Kind: k, Text: text} }

func msg(role string, segs ...Segment) Message { return Message{Role: role, Segments: segs} }

func TestStaticPrefix(t *testing.T) {
	cases := []struct {
		name string
		msgs []Message
		want Prefix
	}{
		{"static then semi_static in one message",
			[]Message{msg("system", seg(KindStatic, "A"), seg(KindStatic, "B"), seg(KindSemiStatic, "{{t}}")), msg("user", seg(KindVolatile, "{{q}}"))},
			Prefix{Role: "system", Text: "AB"}},
		{"fully static messages first",
			[]Message{msg("system", seg(KindStatic, "S")), msg("developer", seg(KindStatic, "D1"), seg(KindStatic, "D2")), msg("user", seg(KindStatic, "U"), seg(KindVolatile, "{{q}}"))},
			Prefix{Messages: []RenderedMessage{{"system", "S"}, {"developer", "D1D2"}}, Role: "user", Text: "U"}},
		{"non-static opens a message",
			[]Message{msg("system", seg(KindStatic, "S")), msg("user", seg(KindVolatile, "{{q}}"))},
			Prefix{Messages: []RenderedMessage{{"system", "S"}}, Role: "user"}},
		{"volatile first",
			[]Message{msg("user", seg(KindVolatile, "{{q}}"))},
			Prefix{Role: "user"}},
		{"entirely static",
			[]Message{msg("system", seg(KindStatic, "S")), msg("user", seg(KindStatic, "U"))},
			Prefix{Messages: []RenderedMessage{{"system", "S"}, {"user", "U"}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := StaticPrefix(Spec{Messages: tc.msgs})
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("StaticPrefix = %+v, want %+v", got, tc.want)
			}
			if !got.Equal(tc.want) {
				t.Fatal("Equal = false for equal prefixes")
			}
		})
	}
}

func TestPrefixBytes(t *testing.T) {
	p := Prefix{Messages: []RenderedMessage{{"system", "abc"}, {"developer", "de"}}, Role: "user", Text: "fgh"}
	if got := p.Bytes(); got != 8 {
		t.Fatalf("Bytes = %d, want 8", got)
	}
	if got := StaticPrefix(testSpec()).Bytes(); got != len("You are a support assistant. ") {
		t.Fatalf("test spec Bytes = %d", got)
	}
}

func TestDiff(t *testing.T) {
	cases := []struct {
		name          string
		mutate        func(*Spec)
		prefixChanged bool
		want          []Change
	}{
		{"identical", func(*Spec) {}, false, []Change{}},
		{"model", func(s *Spec) { s.Model = "small-model" }, false,
			[]Change{{Path: "model", Op: OpChanged, Field: FieldModel, From: "large-model", To: "small-model"}}},
		{"static text", func(s *Spec) { s.Messages[0].Segments[0].Text = "You are terse. " }, true,
			[]Change{{Path: "messages[0].segments[0]", Op: OpChanged, Field: FieldText, From: "You are a support assistant. ", To: "You are terse. "}}},
		{"semi_static text keeps prefix", func(s *Spec) { s.Messages[0].Segments[1].Text = "Org: {{tenant}}" }, false,
			[]Change{{Path: "messages[0].segments[1]", Op: OpChanged, Field: FieldText, From: "Tenant: {{tenant}}.", To: "Org: {{tenant}}"}}},
		{"kind", func(s *Spec) { s.Messages[0].Segments[1].Kind = KindVolatile }, false,
			[]Change{{Path: "messages[0].segments[1]", Op: OpChanged, Field: FieldKind, From: "semi_static", To: "volatile"}}},
		{"kind and text of the first static", func(s *Spec) {
			s.Messages[0].Segments[0] = seg(KindSemiStatic, "Hi {{tenant}}")
		}, true, []Change{
			{Path: "messages[0].segments[0]", Op: OpChanged, Field: FieldKind, From: "static", To: "semi_static"},
			{Path: "messages[0].segments[0]", Op: OpChanged, Field: FieldText, From: "You are a support assistant. ", To: "Hi {{tenant}}"},
		}},
		{"role", func(s *Spec) { s.Messages[0].Role = "developer" }, true,
			[]Change{{Path: "messages[0]", Op: OpChanged, Field: FieldRole, From: "system", To: "developer"}}},
		{"segment added", func(s *Spec) { s.Messages[1].Segments = append(s.Messages[1].Segments, seg(KindVolatile, "!")) }, false,
			[]Change{{Path: "messages[1].segments[1]", Op: OpAdded, Field: FieldSegment, To: "volatile"}}},
		{"segment removed", func(s *Spec) { s.Messages[0].Segments = s.Messages[0].Segments[:1] }, true,
			[]Change{{Path: "messages[0].segments[1]", Op: OpRemoved, Field: FieldSegment, From: "semi_static"}}},
		{"message added", func(s *Spec) { s.Messages = append(s.Messages, msg("assistant", seg(KindVolatile, "x"))) }, false,
			[]Change{{Path: "messages[2]", Op: OpAdded, Field: FieldMessage, To: "assistant"}}},
		{"message removed", func(s *Spec) { s.Messages = s.Messages[:1] }, false,
			[]Change{{Path: "messages[1]", Op: OpRemoved, Field: FieldMessage, From: "user"}}},
		{"variables", func(s *Spec) { s.Variables = []Variable{{Name: "question"}, {Name: "lang"}} }, false,
			[]Change{
				{Path: "variables", Op: OpRemoved, Field: FieldVariables, From: "tenant"},
				{Path: "variables", Op: OpAdded, Field: FieldVariables, To: "lang"},
			}},
		{"variable order is not a change", func(s *Spec) { s.Variables = []Variable{{Name: "question"}, {Name: "tenant"}} }, false, []Change{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			to := testSpec()
			tc.mutate(&to)
			got := Diff(testSpec(), to)
			if got.PrefixChanged != tc.prefixChanged {
				t.Errorf("PrefixChanged = %v, want %v", got.PrefixChanged, tc.prefixChanged)
			}
			if !reflect.DeepEqual(got.Changes, tc.want) {
				t.Errorf("Changes = %+v\nwant %+v", got.Changes, tc.want)
			}
		})
	}
}

func TestDiffJSON(t *testing.T) {
	to := testSpec()
	to.Messages = to.Messages[:1]
	b, err := json.Marshal(Diff(testSpec(), to))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"prefix_changed":false,"changes":[{"path":"messages[1]","op":"removed","field":"message","from":"user"}]}`
	if string(b) != want {
		t.Fatalf("json = %s, want %s", b, want)
	}
	b, _ = json.Marshal(Diff(testSpec(), testSpec()))
	if want := `{"prefix_changed":false,"changes":[]}`; string(b) != want {
		t.Fatalf("json = %s, want %s", b, want)
	}
}
