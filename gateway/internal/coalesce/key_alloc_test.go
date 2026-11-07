//go:build !race

package coalesce

import (
	"strings"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex/bodyindextest"
)

// TestKeyAllocatesLessThanTheMapDid (G4): the key reads the request's index
// where it built a map of the members. Both are measured here, in one
// process, so the comparison holds on any toolchain. The map is given its
// names ready made, so its count is lower than it really was. Not under
// -race: there sync.Pool drops items at random.
func TestKeyAllocatesLessThanTheMapDid(t *testing.T) {
	body := []byte(`{"model":"m","temperature":0,"seed":7,"stream":false,"max_tokens":16,"user":"u",` +
		`"messages":[{"role":"system","content":"` + strings.Repeat("x", 200) + `"},{"role":"user","content":"q"}]}`)
	var ix bodyindex.Index
	if err := ix.Read(body); err != nil {
		t.Fatal(err)
	}
	ref, _, ok := bodyindextest.Members(body)
	if !ok {
		t.Fatal("not an object")
	}
	var now, was Fingerprint
	allocsNow := testing.AllocsPerRun(200, func() { now, _ = FingerprintOf(&ix, 1<<20) })
	allocsWas := testing.AllocsPerRun(200, func() { was, _ = legacyFingerprint(body, ref, true) })
	if now != was {
		t.Fatalf("the key changed: %x, was %x", now.digest, was.digest)
	}
	t.Logf("%.0f allocations for a key, %.0f with the map", allocsNow, allocsWas)
	if allocsNow != 0 {
		t.Fatalf("%.0f allocations for a key, want 0", allocsNow)
	}
	if allocsNow >= allocsWas {
		t.Fatalf("%.0f allocations for a key, want fewer than the map's %.0f", allocsNow, allocsWas)
	}
}
