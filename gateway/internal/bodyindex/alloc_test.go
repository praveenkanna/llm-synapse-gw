//go:build !race

package bodyindex_test

import (
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex"
)

// TestAllocations: an Index declared in a function stays on its stack, so
// reading a body allocates nothing and a splice allocates only its new
// body (ingress's reader made one allocation per request before AD-15). A
// wider body costs one table, and a body past twice the inline members one
// more, sized by counting the rest; a body past MaxMembers is refused
// with no more than the first. A splice of a wide body allocates its new
// body and its new table. Not under -race: there sync.Pool drops items at
// random, so json.Valid's pooled scanner is sometimes allocated anew.
func TestAllocations(t *testing.T) {
	body := []byte(requestBody)
	if n := testing.AllocsPerRun(1000, func() { readAndFind(body) }); n != 0 {
		t.Fatalf("%v allocations to read a body, want 0", n)
	}
	if n := testing.AllocsPerRun(1000, func() { readAndStrip(body) }); n != 1 {
		t.Fatalf("%v allocations for a splice, want 1 (the new body)", n)
	}
	wider := append([]byte(`{"priority":3,`), wide(23)[1:]...)
	if n := testing.AllocsPerRun(1000, func() { readAndFind(wider) }); n != 1 {
		t.Fatalf("%v allocations to read 24 members, want 1 (the table past the inline members)", n)
	}
	if n := testing.AllocsPerRun(1000, func() { readAndStrip(wider) }); n != 3 {
		t.Fatalf("%v allocations to read and splice 24 members, want 3 (the table, the new body, its table)", n)
	}
	widest := wide(bodyindex.MaxMembers)
	if n := testing.AllocsPerRun(100, func() { readAndFind(widest) }); n != 2 {
		t.Fatalf("%v allocations to read MaxMembers members, want 2 (the first table, then one to size)", n)
	}
	past := wide(bodyindex.MaxMembers + 1)
	if n := testing.AllocsPerRun(100, func() { readAndFind(past) }); n != 1 {
		t.Fatalf("%v allocations to refuse a body past MaxMembers, want 1 (the first table)", n)
	}
}
