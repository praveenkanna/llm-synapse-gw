package mockllm

import (
	"container/list"
	"sync"
)

// PrefixBlockTokens is the prefix-cache block size in tokens, vLLM's default
// --block-size.
const PrefixBlockTokens = 16

const (
	fnv64Offset = 14695981039346656037
	fnv64Prime  = 1099511628211
)

// blockHashes returns the chained hash of every full PrefixBlockTokens block
// of tokens, as vLLM's automatic prefix caching keys blocks: block i's hash
// covers block i-1's hash and block i's tokens, so equal hashes mean equal
// prefixes. A trailing partial block is not cached.
func blockHashes(tokens []int) []uint64 {
	n := len(tokens) / PrefixBlockTokens
	if n == 0 {
		return nil
	}
	out := make([]uint64, n)
	prev := uint64(fnv64Offset)
	for i := range n {
		h := uint64(fnv64Offset)
		h = fnvMix64(h, prev)
		for _, t := range tokens[i*PrefixBlockTokens : (i+1)*PrefixBlockTokens] {
			h = fnvMix32(h, uint32(t))
		}
		out[i] = h
		prev = h
	}
	return out
}

func fnvMix64(h, v uint64) uint64 {
	for range 8 {
		h ^= v & 0xff
		h *= fnv64Prime
		v >>= 8
	}
	return h
}

func fnvMix32(h uint64, v uint32) uint64 {
	for range 4 {
		h ^= uint64(v & 0xff)
		h *= fnv64Prime
		v >>= 8
	}
	return h
}

// prefixCache is a bounded LRU set of block hashes for one model. It stands
// in for the KV blocks a vLLM replica keeps cached; the capacity is in blocks.
type prefixCache struct {
	capacity int

	mu      sync.Mutex
	order   *list.List // of uint64, most recently used at the front
	entries map[uint64]*list.Element
}

func newPrefixCache(capacity int) *prefixCache {
	return &prefixCache{capacity: capacity, order: list.New(), entries: make(map[uint64]*list.Element)}
}

// query returns how many of tokens are served from the cache, then caches
// every full block of tokens. Like vLLM, hits are the leading run of cached
// blocks, and the last prompt token is always computed, so a fully cached
// block-aligned prompt hits one block less. A nil cache (disabled) never hits.
func (c *prefixCache) query(tokens []int) int {
	if c == nil {
		return 0
	}
	hashes := blockHashes(tokens)
	lookup := max(0, len(tokens)-1) / PrefixBlockTokens

	c.mu.Lock()
	defer c.mu.Unlock()
	hit := 0
	for hit < lookup {
		if _, ok := c.entries[hashes[hit]]; !ok {
			break
		}
		hit++
	}
	// Touch the tail first so the head of the prefix is the most recently
	// used: vLLM frees a request's blocks in reverse order, so under pressure
	// the end of a prompt is evicted before the shared start of it.
	for i := len(hashes) - 1; i >= 0; i-- {
		c.touchLocked(hashes[i])
	}
	return hit * PrefixBlockTokens
}

func (c *prefixCache) touchLocked(h uint64) {
	if e, ok := c.entries[h]; ok {
		c.order.MoveToFront(e)
		return
	}
	c.entries[h] = c.order.PushFront(h)
	for c.order.Len() > c.capacity {
		oldest := c.order.Back()
		c.order.Remove(oldest)
		delete(c.entries, oldest.Value.(uint64))
	}
}

// reset drops every cached block.
func (c *prefixCache) reset() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.order.Init()
	clear(c.entries)
	c.mu.Unlock()
}

// len returns the number of cached blocks.
func (c *prefixCache) len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}
