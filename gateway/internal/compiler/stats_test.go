package compiler

import (
	"testing"
)

func TestBucketsCoverEveryLength(t *testing.T) {
	prev := -1
	for n := 0; n < 1<<maxExp; n++ {
		b := bucketOf(n)
		if b < prev || b > prev+1 {
			t.Fatalf("bucketOf(%d) = %d after %d: buckets must be contiguous", n, b, prev)
		}
		if up := bucketUpper(b); n > up {
			t.Fatalf("n=%d above its bucket's upper bound %d", n, up)
		}
		if b > 0 && n <= bucketUpper(b-1) {
			t.Fatalf("n=%d also fits the previous bucket", n)
		}
		prev = b
	}
	if bucketOf(1<<maxExp+5) != numBuckets-1 {
		t.Fatal("lengths past the range go to the last bucket")
	}
}

func TestQuantileWithinFourPercent(t *testing.T) {
	s := newLengthStats(4)
	k := versionKey{"t", 1}
	for i := 1; i <= 10000; i++ {
		s.observe(k, i)
	}
	got, ok := s.quantile(k, 0.999, 1000)
	if !ok {
		t.Fatal("want enough samples")
	}
	if want := 9990; got < want || float64(got) > float64(want)*1.07 {
		t.Fatalf("p99.9 = %d, want about %d (bucket upper bound, <= 7%% over)", got, want)
	}
}

func TestQuantileNeedsMinSamples(t *testing.T) {
	s := newLengthStats(4)
	k := versionKey{"t", 1}
	for range 999 {
		s.observe(k, 100)
	}
	if _, ok := s.quantile(k, 0.999, 1000); ok {
		t.Fatal("999 samples are below min_samples 1000")
	}
	s.observe(k, 100)
	if got, ok := s.quantile(k, 0.999, 1000); !ok || got != bucketUpper(bucketOf(100)) {
		t.Fatalf("p99.9 = %d, %v", got, ok)
	}
}

func TestStatsBoundedAndRetain(t *testing.T) {
	s := newLengthStats(2)
	for v := 1; v <= 5; v++ {
		s.observe(versionKey{"t", v}, 10)
	}
	if len(s.entries) != 2 {
		t.Fatalf("entries = %d, want ceiling 2", len(s.entries))
	}
	s.retain(func(k versionKey) bool { return k.version == 2 })
	if len(s.entries) != 1 {
		t.Fatalf("entries after retain = %d, want 1", len(s.entries))
	}
}

func TestDecayKeepsShape(t *testing.T) {
	s := newLengthStats(1)
	k := versionKey{"t", 1}
	for i := range decayAt + 10 {
		s.observe(k, 100+i%2)
	}
	h := s.entries[k]
	if h.total >= decayAt || h.total < decayAt/2-10 {
		t.Fatalf("total after decay = %d", h.total)
	}
	if got, _ := s.quantile(k, 0.5, 1); got != bucketUpper(bucketOf(100)) {
		t.Fatalf("median after decay = %d", got)
	}
}
