package compiler

import (
	"math/bits"
	"sync"
)

// lengthStats is the completion-length distribution of each template
// version, for the max_tokens policy's p99.9 (ADR 0007). It is bounded:
// at most maxEntries versions (only versions in the registry snapshot are
// observed, and Retain drops the rest), each a fixed array of buckets. Old
// observations fade: once a version has seen decayAt completions, every
// bucket is halved, so the percentile follows the version's recent output.
type lengthStats struct {
	mu         sync.Mutex
	entries    map[versionKey]*lengthHist
	maxEntries int
}

type versionKey struct {
	id      string
	version int
}

// Bucket layout: exact counts below linearMax, then subBuckets buckets per
// power of two (about 4% wide) up to 2^maxExp tokens.
const (
	linearMax  = 64
	subBits    = 4
	subBuckets = 1 << subBits
	maxExp     = 21 // 2M tokens: beyond any context window
	numBuckets = linearMax + (maxExp-6)*subBuckets + 1
	decayAt    = 100_000
)

type lengthHist struct {
	counts [numBuckets]uint32
	total  uint32
}

func newLengthStats(maxEntries int) *lengthStats {
	return &lengthStats{entries: make(map[versionKey]*lengthHist), maxEntries: maxEntries}
}

// observe records one completion of n tokens for the version.
func (s *lengthStats) observe(k versionKey, n int) {
	if n < 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.entries[k]
	if h == nil {
		if len(s.entries) >= s.maxEntries {
			return // ceiling: the version keeps the pool-ceiling cap
		}
		h = new(lengthHist)
		s.entries[k] = h
	}
	h.counts[bucketOf(n)]++
	h.total++
	if h.total >= decayAt {
		h.total = 0
		for i := range h.counts {
			h.counts[i] /= 2
			h.total += h.counts[i]
		}
	}
}

// quantile returns the q-quantile completion length (the upper bound of
// its bucket) and whether the version has at least minSamples.
func (s *lengthStats) quantile(k versionKey, q float64, minSamples int) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.entries[k]
	if h == nil || int(h.total) < minSamples || h.total == 0 {
		return 0, false
	}
	rank := uint32(float64(h.total)*q + 0.5)
	rank = max(rank, 1)
	var cum uint32
	for i, c := range h.counts {
		cum += c
		if cum >= rank {
			return bucketUpper(i), true
		}
	}
	return bucketUpper(numBuckets - 1), true
}

// retain drops the versions for which keep is false.
func (s *lengthStats) retain(keep func(versionKey) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k := range s.entries {
		if !keep(k) {
			delete(s.entries, k)
		}
	}
}

func bucketOf(n int) int {
	if n < linearMax {
		return n
	}
	exp := bits.Len(uint(n)) - 1 // n in [2^exp, 2^(exp+1))
	if exp >= maxExp {
		return numBuckets - 1
	}
	sub := (n >> (exp - subBits)) & (subBuckets - 1)
	return linearMax + (exp-6)*subBuckets + sub
}

// bucketUpper is the largest length in bucket i.
func bucketUpper(i int) int {
	if i < linearMax {
		return i
	}
	if i >= numBuckets-1 {
		return 1 << maxExp
	}
	j := i - linearMax
	exp := j/subBuckets + 6
	sub := j % subBuckets
	lo := 1<<exp + sub<<(exp-subBits)
	return lo + 1<<(exp-subBits) - 1
}
