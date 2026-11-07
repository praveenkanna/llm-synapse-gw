package templates

import (
	"os"
	"strings"
	"testing"
)

// The AC-M3-6 harness specs are registered on the dev stack as they are:
// the compiled one must pass registration, and the misordered one (today's
// prompt, volatile text at character 732) must be rejected for its order
// (AC-M3-2).
func TestHarnessSpecs(t *testing.T) {
	load := func(name string) Spec {
		t.Helper()
		b, err := os.ReadFile("../../../harness/profiles/templates/" + name)
		if err != nil {
			t.Fatal(err)
		}
		s, err := ParseSpec(b)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return s
	}
	if errs := Validate(load("m3-prompt-compiled.json")); len(errs) > 0 {
		t.Fatalf("compiled spec rejected: %v", errs)
	}
	errs := Validate(load("m3-prompt-misordered.json"))
	if len(errs) == 0 {
		t.Fatal("misordered spec accepted")
	}
	for _, e := range errs {
		if !strings.Contains(e.Msg, "move volatile text after") {
			t.Errorf("unexpected error %s: %s", e.Path, e.Msg)
		}
	}
}
