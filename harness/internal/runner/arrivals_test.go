package runner

import (
	"math"
	"math/rand/v2"
	"slices"
	"testing"
	"time"
)

func arrivals(t *testing.T, rate float64, b Burst, seed uint64, n int) []time.Duration {
	t.Helper()
	a, err := NewArrivals(rate, b, rand.New(rand.NewPCG(seed, 0)))
	if err != nil {
		t.Fatal(err)
	}
	out := make([]time.Duration, n)
	for i := range out {
		out[i] = a.Next()
	}
	return out
}

func TestArrivalsDeterministic(t *testing.T) {
	b := Burst{Every: time.Second, Length: 200 * time.Millisecond, Factor: 5}
	x := arrivals(t, 100, b, 7, 5000)
	y := arrivals(t, 100, b, 7, 5000)
	z := arrivals(t, 100, b, 8, 5000)
	if !slices.Equal(x, y) {
		t.Fatal("same seed gave different schedules")
	}
	if slices.Equal(x, z) {
		t.Fatal("different seeds gave the same schedule")
	}
	if !slices.IsSorted(x) {
		t.Fatal("arrival offsets are not non-decreasing")
	}
}

func TestArrivalsPoissonRate(t *testing.T) {
	const rate, n = 200.0, 100000
	xs := arrivals(t, rate, Burst{}, 1, n)
	got := float64(n) / xs[n-1].Seconds()
	if math.Abs(got-rate)/rate > 0.02 {
		t.Fatalf("mean rate = %.1f/s, want %.0f/s ±2%%", got, rate)
	}
	// Exponential gaps: coefficient of variation ~1 (vs 0 for a fixed rate).
	var sum, sq float64
	prev := time.Duration(0)
	for _, x := range xs {
		g := (x - prev).Seconds()
		sum += g
		sq += g * g
		prev = x
	}
	mean := sum / n
	cv := math.Sqrt(sq/n-mean*mean) / mean
	if cv < 0.95 || cv > 1.05 {
		t.Fatalf("gap coefficient of variation = %.3f, want ~1 (Poisson)", cv)
	}
}

func TestArrivalsBurstRate(t *testing.T) {
	const rate, factor = 100.0, 5.0
	b := Burst{Every: time.Second, Length: 250 * time.Millisecond, Factor: factor}
	xs := arrivals(t, rate, b, 3, 200000)
	var inBurst, outBurst int
	end := xs[len(xs)-1]
	periods := math.Floor(end.Seconds())
	for _, x := range xs {
		if x.Seconds() >= periods {
			break // partial last period
		}
		if frac := math.Mod(x.Seconds(), 1); x >= time.Second && frac < 0.25 {
			inBurst++
		} else {
			outBurst++
		}
	}
	burstTime := (periods - 1) * 0.25
	baseTime := periods - burstTime
	gotBurst, gotBase := float64(inBurst)/burstTime, float64(outBurst)/baseTime
	if math.Abs(gotBase-rate)/rate > 0.03 {
		t.Errorf("base rate = %.1f/s, want %.0f ±3%%", gotBase, rate)
	}
	if want := rate * factor; math.Abs(gotBurst-want)/want > 0.03 {
		t.Errorf("burst rate = %.1f/s, want %.0f ±3%%", gotBurst, want)
	}
}

func TestArrivalsValidation(t *testing.T) {
	tests := []struct {
		name  string
		rate  float64
		burst Burst
	}{
		{"zero rate", 0, Burst{}},
		{"negative rate", -1, Burst{}},
		{"infinite rate", math.Inf(1), Burst{}},
		{"NaN rate", math.NaN(), Burst{}},
		{"burst without length", 1, Burst{Every: time.Second, Factor: 2}},
		{"burst as long as period", 1, Burst{Every: time.Second, Length: time.Second, Factor: 2}},
		{"burst zero factor", 1, Burst{Every: time.Second, Length: time.Millisecond}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewArrivals(tc.rate, tc.burst, rand.New(rand.NewPCG(1, 0))); err == nil {
				t.Fatal("want error")
			}
		})
	}
}

// TestArrivalBurstsFollowTheWindowRule: the arrival process raises its rate
// exactly inside Burst.Windows(), the rule the records' in_burst and every
// reader use.
func TestArrivalBurstsFollowTheWindowRule(t *testing.T) {
	b := Burst{Every: 10 * time.Second, Length: 3 * time.Second, Factor: 4}
	a, err := NewArrivals(1, b, rand.New(rand.NewPCG(1, 2)))
	if err != nil {
		t.Fatal(err)
	}
	for ms := 0; ms < 60_000; ms += 250 {
		at := time.Duration(ms) * time.Millisecond
		rate, _ := a.rateAt(at.Seconds())
		if inBurst := rate > 1; inBurst != b.Windows().Contains(at) {
			t.Fatalf("at %v: arrival rate %v, Windows().Contains %v", at, rate, b.Windows().Contains(at))
		}
	}
}

// TestArrivalSegmentsAlwaysEndAfterT: at and around every segment boundary,
// reached as a running sum or as a product (they can differ by a rounding
// step), rateAt ends the segment strictly after t. A segment ending at t
// would leave Next spending nothing, forever. The explicit conversions stop
// arm64 fusing the sums, which hides the difference.
func TestArrivalSegmentsAlwaysEndAfterT(t *testing.T) {
	for _, b := range []Burst{
		{Every: 100 * time.Millisecond, Length: 50 * time.Millisecond, Factor: 2},
		{Every: time.Second, Length: 300 * time.Millisecond, Factor: 3},
		{Every: 60 * time.Second, Length: 10 * time.Second, Factor: 2},
		{Every: 7 * time.Millisecond, Length: 3 * time.Millisecond, Factor: 1.5},
		{Every: 333 * time.Millisecond, Length: 111 * time.Millisecond, Factor: 4},
	} {
		a, err := NewArrivals(100, b, rand.New(rand.NewPCG(1, 2)))
		if err != nil {
			t.Fatal(err)
		}
		every, length := b.Every.Seconds(), b.Length.Seconds()
		for k := range 5000 {
			start := float64(float64(k) * every)
			for _, at := range []float64{start, float64(start + every), float64(start + length), float64(float64(k+1) * every)} {
				for _, tt := range []float64{math.Nextafter(at, 0), at, math.Nextafter(at, math.Inf(1))} {
					if _, end := a.rateAt(tt); !(end > tt) {
						t.Fatalf("%+v: at t=%v the segment ends at %v", b, tt, end)
					}
				}
			}
		}
	}
}

// TestArrivalsCrossManyBoundaries: Next passes boundaries with a rate low
// enough to cross several per arrival, and its offsets never go back.
func TestArrivalsCrossManyBoundaries(t *testing.T) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		a, err := NewArrivals(1, Burst{Every: 100 * time.Millisecond, Length: 50 * time.Millisecond, Factor: 2}, rand.New(rand.NewPCG(4, 0x5eed)))
		if err != nil {
			t.Error(err)
			return
		}
		var last time.Duration
		for range 2000 {
			at := a.Next()
			if at < last {
				t.Errorf("arrival at %v after one at %v", at, last)
				return
			}
			last = at
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Next did not finish: stuck at a segment boundary")
	}
}
