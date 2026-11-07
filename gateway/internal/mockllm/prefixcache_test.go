package mockllm

import (
	"sync"
	"testing"
)

// seqTokens returns n distinct token ids starting at from.
func seqTokens(from, n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = from + i
	}
	return out
}

func TestBlockHashesAreChained(t *testing.T) {
	a := seqTokens(0, 3*PrefixBlockTokens+5)
	ha := blockHashes(a)
	if len(ha) != 3 {
		t.Fatalf("%d hashes for 3 full blocks and a partial one", len(ha))
	}
	// Same second block after a different first block: different hash.
	b := append(seqTokens(1000, PrefixBlockTokens), a[PrefixBlockTokens:]...)
	hb := blockHashes(b)
	if hb[0] == ha[0] || hb[1] == ha[1] {
		t.Fatalf("block hashes ignore the prefix: %x vs %x", ha, hb)
	}
	// A change in block 2 leaves blocks 0 and 1 alone.
	c := append([]int(nil), a...)
	c[2*PrefixBlockTokens] = -1
	hc := blockHashes(c)
	if hc[0] != ha[0] || hc[1] != ha[1] || hc[2] == ha[2] {
		t.Fatalf("change in block 2: %x vs %x", ha, hc)
	}
	if blockHashes(seqTokens(0, PrefixBlockTokens-1)) != nil {
		t.Fatal("a partial block must not be hashed")
	}
}

func TestPrefixCacheQuery(t *testing.T) {
	const bt = PrefixBlockTokens
	tests := []struct {
		name   string
		first  []int
		second []int
		hits   int
	}{
		{name: "cold miss", first: nil, second: seqTokens(0, 5*bt), hits: 0},
		{name: "shared prefix", first: append(seqTokens(0, 4*bt), seqTokens(500, 2*bt)...), second: append(seqTokens(0, 4*bt), seqTokens(900, 2*bt)...), hits: 4 * bt},
		{name: "shared partial block not counted", first: append(seqTokens(0, 2*bt+7), seqTokens(500, bt)...), second: append(seqTokens(0, 2*bt+7), seqTokens(900, bt)...), hits: 2 * bt},
		{name: "first block differs", first: seqTokens(0, 4*bt), second: append([]int{-1}, seqTokens(1, 4*bt-1)...), hits: 0},
		// vLLM always computes the last prompt token, so an identical
		// block-aligned prompt hits one block less than its length.
		{name: "identical block-aligned", first: seqTokens(0, 2*bt), second: seqTokens(0, 2*bt), hits: bt},
		{name: "identical with tail", first: seqTokens(0, 2*bt+1), second: seqTokens(0, 2*bt+1), hits: 2 * bt},
		{name: "shorter than a block", first: seqTokens(0, bt-1), second: seqTokens(0, bt-1), hits: 0},
		{name: "empty", first: nil, second: nil, hits: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newPrefixCache(1024)
			if tc.first != nil {
				c.query(tc.first)
			}
			if got := c.query(tc.second); got != tc.hits {
				t.Fatalf("hits = %d, want %d", got, tc.hits)
			}
		})
	}
}

func TestPrefixCacheDisabled(t *testing.T) {
	var c *prefixCache
	tokens := seqTokens(0, 4*PrefixBlockTokens)
	c.query(tokens)
	if got := c.query(tokens); got != 0 || c.len() != 0 {
		t.Fatalf("nil cache: hits %d len %d, want 0 0", got, c.len())
	}
	c.reset()
}

func TestPrefixCacheLRUBound(t *testing.T) {
	const capacity = 8
	c := newPrefixCache(capacity)
	a := seqTokens(0, 4*PrefixBlockTokens)
	c.query(a)
	for i := range 50 {
		c.query(seqTokens(10_000*(i+1), 3*PrefixBlockTokens))
		if n := c.len(); n > capacity || len(c.entries) != n {
			t.Fatalf("after %d requests: %d blocks in list, %d in map, capacity %d", i+2, n, len(c.entries), capacity)
		}
	}
	if got := c.query(append(a, 1)); got != 0 {
		t.Fatalf("evicted prefix still hit %d tokens", got)
	}

	// Recently used blocks survive: touching a keeps it while others churn.
	c = newPrefixCache(capacity)
	c.query(a)
	for i := range 10 {
		c.query(seqTokens(10_000*(i+1), 2*PrefixBlockTokens))
		c.query(append(a, 1))
	}
	if got := c.query(append(a, 1)); got != 4*PrefixBlockTokens {
		t.Fatalf("hot prefix hits = %d, want %d", got, 4*PrefixBlockTokens)
	}

	c.reset()
	if c.len() != 0 || c.query(append(a, 1)) != 0 {
		t.Fatal("reset left blocks behind")
	}
}

// TestPrefixCacheKeepsHeadOfLongPrompt: a prompt longer than the cache keeps
// its first blocks, as vLLM evicts a request's tail blocks first, so the
// shared start of the next request still hits.
func TestPrefixCacheKeepsHeadOfLongPrompt(t *testing.T) {
	const capacity = 4
	c := newPrefixCache(capacity)
	long := seqTokens(0, 6*PrefixBlockTokens)
	c.query(long)
	if c.len() != capacity {
		t.Fatalf("len = %d, want %d", c.len(), capacity)
	}
	if got := c.query(append(seqTokens(0, 6*PrefixBlockTokens), 1)); got != capacity*PrefixBlockTokens {
		t.Fatalf("hits = %d, want the %d head blocks (%d tokens)", got, capacity, capacity*PrefixBlockTokens)
	}
}

func TestPrefixCacheConcurrent(t *testing.T) {
	const capacity = 64
	c := newPrefixCache(capacity)
	shared := seqTokens(0, 8*PrefixBlockTokens)
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Go(func() {
			for i := range 200 {
				tokens := append(append([]int(nil), shared...), seqTokens(1_000_000*(g+1)+i*100, 3*PrefixBlockTokens)...)
				if hits := c.query(tokens); hits > len(tokens) || hits%PrefixBlockTokens != 0 {
					t.Errorf("hits %d for %d tokens", hits, len(tokens))
					return
				}
			}
		})
	}
	wg.Wait()
	if n := c.len(); n > capacity || len(c.entries) != n {
		t.Fatalf("after concurrent use: %d blocks in list, %d in map, capacity %d", n, len(c.entries), capacity)
	}
}
