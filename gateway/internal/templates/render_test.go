package templates

import (
	"reflect"
	"regexp"
	"strings"
	"testing"
)

func TestRender(t *testing.T) {
	got, err := Render(testSpec(), map[string]string{"tenant": "acme", "question": " What is {{tenant}}?\n"})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	want := []RenderedMessage{
		{Role: "system", Content: "You are a support assistant. Tenant: acme."},
		{Role: "user", Content: " What is {{tenant}}?\n"}, // verbatim: no trimming, no re-substitution
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Render = %q, want %q", got, want)
	}
}

func TestRenderCopiesStaticLiterally(t *testing.T) {
	s := testSpec()
	s.Messages[0].Segments[0].Text = "literal {{ tenant }} {{X}} "
	got, err := Render(s, map[string]string{"tenant": "acme", "question": "q"})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if want := "literal {{ tenant }} {{X}} Tenant: acme."; got[0].Content != want {
		t.Fatalf("content = %q, want %q", got[0].Content, want)
	}
}

func TestRenderLiteralBracesInNonStatic(t *testing.T) {
	s := testSpec()
	s.Messages[1].Segments[0].Text = "{{{question}}} {{ question }} {{}} {{question}"
	got, err := Render(s, map[string]string{"tenant": "t", "question": "Q"})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if want := "{Q} {{ question }} {{}} {{question}"; got[1].Content != want {
		t.Fatalf("content = %q, want %q", got[1].Content, want)
	}
}

func TestRenderRejectsVariables(t *testing.T) {
	cases := []struct {
		name string
		vars map[string]string
		want []string
	}{
		{"missing", map[string]string{"tenant": "t"}, []string{`missing variable "question"`}},
		{"unknown", map[string]string{"tenant": "t", "question": "q", "extra": "x"}, []string{`unknown variable "extra"`}},
		{"both", map[string]string{"zz": "x"}, []string{`missing variable "question"`, `missing variable "tenant"`, `unknown variable "zz"`}},
		{"nil", nil, []string{`missing variable "question"`}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Render(testSpec(), tc.vars)
			if err == nil {
				t.Fatal("Render: nil error")
			}
			for _, w := range tc.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("err = %v, want containing %q", err, w)
				}
			}
		})
	}
}

func TestRenderEmptyValue(t *testing.T) {
	got, err := Render(testSpec(), map[string]string{"tenant": "", "question": ""})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if got[1].Content != "" || got[0].Content != "You are a support assistant. Tenant: ." {
		t.Fatalf("Render = %q", got)
	}
}

// placeholderRE is the contract's placeholder grammar.
var placeholderRE = regexp.MustCompile(`\{\{([a-z][a-z0-9_]{0,63})\}\}`)

func TestPlaceholdersMatchRegexp(t *testing.T) {
	inputs := []string{
		"", "{{", "}}", "{{}}", "{{a}}", "{{a}}{{b}}", "{{{a}}}", "{{{{a}}}}", "{{a}", "{a}}",
		"{{ a}}", "{{a }}", "{{A}}", "{{_a}}", "{{1a}}", "{{a_1}}", "{{a-b}}", "x{{y}}z{{",
		"{{" + strings.Repeat("a", 64) + "}}", "{{" + strings.Repeat("a", 65) + "}}",
		"{{" + strings.Repeat("a", 65) + "}}{{b}}", "{{a{{b}}", "{{a}}}}", "é{{é}}{{é_ok}}{{ok}}",
	}
	for _, in := range inputs {
		var got []string
		placeholders(in, func(n string) { got = append(got, n) })
		var want []string
		for _, m := range placeholderRE.FindAllStringSubmatch(in, -1) {
			want = append(want, m[1])
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("placeholders(%q) = %q, want %q", in, got, want)
		}
	}
}

func FuzzPlaceholdersMatchRegexp(f *testing.F) {
	for _, s := range []string{"{{a}}", "{{{a}}}", "{{a}", "x{{b_1}}y{{C}}"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, in string) {
		var got []string
		placeholders(in, func(n string) { got = append(got, n) })
		var want []string
		for _, m := range placeholderRE.FindAllStringSubmatch(in, -1) {
			want = append(want, m[1])
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("placeholders(%q) = %q, want %q", in, got, want)
		}
	})
}

func TestValidName(t *testing.T) {
	for name, want := range map[string]bool{
		"a": true, "tenant_id": true, "a9": true, "a" + strings.Repeat("z", 63): true,
		"": false, "A": false, "9a": false, "_a": false, "a-b": false, "a b": false, "a" + strings.Repeat("z", 64): false,
	} {
		if got := ValidName(name); got != want {
			t.Errorf("ValidName(%q) = %v, want %v", name, got, want)
		}
	}
}
