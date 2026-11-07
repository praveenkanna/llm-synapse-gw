package templates

import (
	"fmt"
	"strings"
	"testing"
)

func TestValidateAcceptsValidSpecs(t *testing.T) {
	cases := map[string]func(*Spec){
		"test spec": func(*Spec) {},
		"entirely static": func(s *Spec) {
			s.Variables, s.Examples = nil, nil
			s.Messages = []Message{{Role: "system", Segments: []Segment{{Kind: KindStatic, Text: "Be brief."}}}}
		},
		"volatile first": func(s *Spec) {
			s.Variables, s.Examples = []Variable{{Name: "q"}}, nil
			s.Messages = []Message{{Role: "user", Segments: []Segment{{Kind: KindVolatile, Text: "{{q}}"}}}}
		},
		"literal braces in non-static": func(s *Spec) {
			s.Messages[1].Segments[0].Text = "{{question}} {{ spaced }} {{Upper}} {{9x}} {{ {{}"
		},
		"literal braces in static": func(s *Spec) {
			s.Messages[0].Segments[0].Text = `JSON like {"a":{"b":1}} and {{ x }} and {{X}}`
		},
		"name of 64 chars is literal-free": func(s *Spec) {
			long := "a" + strings.Repeat("b", 63)
			s.Variables = append(s.Variables, Variable{Name: long})
			s.Messages[1].Segments[0].Text = "{{question}}{{" + long + "}}"
			s.Examples[0][long] = "v"
		},
		"65-char placeholder is literal in static": func(s *Spec) {
			s.Messages[0].Segments[0].Text = "{{a" + strings.Repeat("b", 64) + "}}"
		},
		"developer and assistant roles": func(s *Spec) {
			s.Messages = append([]Message{{Role: "developer", Segments: []Segment{{Kind: KindStatic, Text: "d"}}}}, s.Messages...)
			s.Messages = append(s.Messages, Message{Role: "assistant", Segments: []Segment{{Kind: KindVolatile, Text: "ok"}}})
		},
		"variable used twice": func(s *Spec) {
			s.Messages[1].Segments[0].Text = "{{question}} {{question}} {{tenant}}"
		},
		"cache ttl": func(s *Spec) { s.CacheTTL = "90s" },
		"zero ttl":  func(s *Spec) { s.CacheTTL = "0s" },
		"no examples": func(s *Spec) {
			s.Examples = nil
		},
		"at the limits": func(s *Spec) {
			s.Variables, s.Examples = nil, nil
			s.Messages = nil
			for range MaxMessages {
				s.Messages = append(s.Messages, Message{Role: "system", Segments: []Segment{
					{Kind: KindStatic, Text: strings.Repeat("x", MaxTextBytes/MaxSegments)},
					{Kind: KindStatic, Text: strings.Repeat("x", MaxTextBytes/MaxSegments)},
					{Kind: KindStatic, Text: strings.Repeat("x", MaxTextBytes/MaxSegments)},
					{Kind: KindStatic, Text: strings.Repeat("x", MaxTextBytes/MaxSegments)},
				}})
			}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			s := testSpec()
			mutate(&s)
			if errs := Validate(s); errs != nil {
				t.Fatalf("Validate = %v, want nil", errs)
			}
		})
	}
}

func TestValidateRejects(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Spec)
		want   []string // each as "path: substring", in order
	}{
		{"no model", func(s *Spec) { s.Model = " " }, []string{"model: required"}},
		{"no messages", func(s *Spec) {
			s.Messages, s.Variables, s.Examples = nil, nil, nil
		}, []string{"messages: at least one message"}},
		{"message without segments", func(s *Spec) {
			s.Messages = append(s.Messages, Message{Role: "user"})
		}, []string{"messages[2]: at least one segment"}},
		{"unknown role", func(s *Spec) { s.Messages[0].Role = "tool" }, []string{`messages[0]: unknown role "tool"`}},
		{"unknown kind", func(s *Spec) { s.Messages[0].Segments[0].Kind = "dynamic" }, []string{`messages[0].segments[0]: unknown kind "dynamic"`}},
		{"static after volatile", func(s *Spec) {
			s.Messages = append(s.Messages, Message{Role: "user", Segments: []Segment{{Kind: KindStatic, Text: "x"}}})
		}, []string{"messages[2].segments[0]: static segment after volatile segment messages[1].segments[0]; only text before the first volatile segment can be cached"}},
		{"semi_static after volatile", func(s *Spec) {
			s.Messages[1].Segments = append(s.Messages[1].Segments, Segment{Kind: KindSemiStatic, Text: "x"})
		}, []string{"messages[1].segments[1]: semi_static segment after volatile segment messages[1].segments[0]; only text before the first volatile"}},
		{"static after semi_static", func(s *Spec) {
			s.Messages[0].Segments = append(s.Messages[0].Segments, Segment{Kind: KindStatic, Text: "x"})
		}, []string{"messages[0].segments[2]: static segment after semi_static segment messages[0].segments[1]; only static text before the first semi_static"}},
		{"each offending segment once, named against the first volatile", func(s *Spec) {
			s.Messages[1].Segments = append(s.Messages[1].Segments,
				Segment{Kind: KindVolatile, Text: "y"}, Segment{Kind: KindStatic, Text: "a"}, Segment{Kind: KindSemiStatic, Text: "b"})
		}, []string{
			"messages[1].segments[2]: static segment after volatile segment messages[1].segments[0]",
			"messages[1].segments[3]: semi_static segment after volatile segment messages[1].segments[0]",
		}},
		{"placeholder in static", func(s *Spec) {
			s.Messages[0].Segments[0].Text = "Hello {{tenant}}"
		}, []string{"messages[0].segments[0]: static segment contains placeholder {{tenant}}; static text is literal, so mark the segment semi_static or volatile"}},
		{"undeclared placeholder", func(s *Spec) {
			s.Messages[1].Segments[0].Text = "{{question}} {{lang}} {{lang}}"
		}, []string{"messages[1].segments[0]: placeholder {{lang}} is not declared"}},
		{"unused variable", func(s *Spec) {
			s.Variables = append(s.Variables, Variable{Name: "lang"})
			s.Examples[0]["lang"] = "en"
		}, []string{`variables[2]: variable "lang" is never used`}},
		{"duplicate variable", func(s *Spec) {
			s.Variables = append(s.Variables, Variable{Name: "tenant"})
		}, []string{`variables[2]: variable "tenant" is already declared at variables[0]`}},
		{"bad variable names", func(s *Spec) {
			s.Variables = append(s.Variables, Variable{Name: "Tenant"}, Variable{Name: ""}, Variable{Name: "a" + strings.Repeat("b", 64)})
		}, []string{
			`variables[2]: name "Tenant" must match`,
			`variables[3]: name "" must match`,
			`variables[4]: name "abbb`,
		}},
		{"example missing and unknown", func(s *Spec) {
			s.Examples = append(s.Examples, map[string]string{"question": "q", "zeta": "z", "alpha": "a"})
		}, []string{
			`examples[1]: missing variable "tenant"`,
			`examples[1]: unknown variable "alpha"`,
			`examples[1]: unknown variable "zeta"`,
		}},
		{"example value too large", func(s *Spec) {
			s.Examples[0]["question"] = strings.Repeat("q", MaxExampleValueBytes+1)
		}, []string{`examples[0]: value of "question" is 65537 bytes, over the limit of 65536`}},
		{"too many messages", func(s *Spec) {
			for range MaxMessages - 1 {
				s.Messages = append(s.Messages, Message{Role: "user", Segments: []Segment{{Kind: KindVolatile, Text: "x"}}})
			}
		}, []string{"messages: 65 messages, over the limit of 64"}},
		{"too many segments", func(s *Spec) {
			for range MaxSegments - 2 {
				s.Messages[1].Segments = append(s.Messages[1].Segments, Segment{Kind: KindVolatile, Text: "x"})
			}
		}, []string{"messages: 257 segments, over the limit of 256"}},
		{"too much text", func(s *Spec) {
			s.Messages[1].Segments[0].Text += strings.Repeat("x", MaxTextBytes)
		}, []string{"messages: 262204 bytes of segment text, over the limit of 262144"}},
		{"too many variables", func(s *Spec) {
			text := "{{question}}"
			for i := range MaxVariables - 1 {
				name := fmt.Sprintf("v%d", i)
				s.Variables = append(s.Variables, Variable{Name: name})
				text += "{{" + name + "}}"
				s.Examples[0][name] = ""
			}
			s.Messages[1].Segments[0].Text = text
		}, []string{"variables: 65 variables, over the limit of 64"}},
		{"too many examples", func(s *Spec) {
			for range MaxExamples {
				s.Examples = append(s.Examples, map[string]string{"tenant": "t", "question": "q"})
			}
		}, []string{"examples: 17 examples, over the limit of 16"}},
		{"bad ttl", func(s *Spec) { s.CacheTTL = "soon" }, []string{`cache_ttl: "soon" is not a duration`}},
		{"negative ttl", func(s *Spec) { s.CacheTTL = "-1s" }, []string{"cache_ttl: must not be negative"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := testSpec()
			tc.mutate(&s)
			errs := Validate(s)
			if len(errs) != len(tc.want) {
				t.Fatalf("Validate = %d problems %v, want %d", len(errs), errs, len(tc.want))
			}
			got := errs.Error()
			for _, w := range tc.want {
				if !strings.Contains(got, w) {
					t.Errorf("Validate = %q\nwant containing %q", got, w)
				}
			}
		})
	}
}

func TestValidateReportsEveryProblem(t *testing.T) {
	s := Spec{
		Variables: []Variable{{Name: "a"}, {Name: "a"}, {Name: "unused"}},
		Messages: []Message{
			{Role: "robot", Segments: []Segment{{Kind: KindVolatile, Text: "{{a}}{{b}}"}}},
			{Role: "user", Segments: []Segment{{Kind: KindStatic, Text: "{{a}}"}}},
		},
		Examples: []map[string]string{{}},
		CacheTTL: "x",
	}
	errs := Validate(s)
	want := []string{
		"model",
		"variables[1]",            // duplicate
		"messages[0]",             // role
		"messages[0].segments[0]", // undeclared b
		"messages[1].segments[0]", // order
		"messages[1].segments[0]", // static placeholder
		"variables[2]",            // unused
		"examples[0]",             // missing a
		"examples[0]",             // missing unused
		"cache_ttl",
	}
	if len(errs) != len(want) {
		t.Fatalf("Validate = %d problems:\n%v\nwant %d", len(errs), errs, len(want))
	}
	for i, w := range want {
		if errs[i].Path != w {
			t.Errorf("problem %d path = %q, want %q (%v)", i, errs[i].Path, w, errs[i])
		}
	}
}

func TestValidateBoundsReportedProblems(t *testing.T) {
	s := testSpec()
	for range 3 * maxReported {
		s.Messages = append(s.Messages, Message{Role: "nobody"})
	}
	errs := Validate(s)
	if len(errs) != maxReported+1 {
		t.Fatalf("Validate = %d problems, want %d", len(errs), maxReported+1)
	}
	if last := errs[len(errs)-1]; !strings.HasSuffix(last.Msg, "more problems not shown") {
		t.Fatalf("last problem = %v", last)
	}
}

func TestValidateQuotesNoSegmentText(t *testing.T) {
	secret := "SECRET-PROMPT-TEXT"
	s := testSpec()
	s.Messages[0].Segments[0].Text = secret + " {{tenant}}"
	s.Messages = append(s.Messages, Message{Role: "user", Segments: []Segment{{Kind: KindStatic, Text: secret}}})
	s.Messages[1].Segments[0].Text = secret + "{{question}}{{nope}}"
	errs := Validate(s)
	if errs == nil {
		t.Fatal("Validate = nil, want problems")
	}
	if strings.Contains(errs.Error(), secret) {
		t.Fatalf("Validate quotes segment text: %v", errs)
	}
}
