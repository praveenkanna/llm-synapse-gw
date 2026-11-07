package workload

import (
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"go.yaml.in/yaml/v3"
)

func TestDistYAML(t *testing.T) {
	tests := []struct {
		in      string
		want    Dist
		wantErr bool
	}{
		{in: "12", want: Fixed(12)},
		{in: "{dist: uniform, min: 2, max: 9}", want: Dist{Kind: DistUniform, Min: 2, Max: 9}},
		{in: "{dist: lognormal, median: 50, sigma: 0.5, min: 1, max: 400}", want: Dist{Kind: DistLognormal, Median: 50, Sigma: 0.5, Min: 1, Max: 400}},
		{in: "abc", wantErr: true},
		{in: "1.5", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			var d Dist
			err := yaml.Unmarshal([]byte(tc.in), &d)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error, got %+v", d)
				}
				return
			}
			if err != nil || d != tc.want {
				t.Fatalf("got %+v, %v; want %+v", d, err, tc.want)
			}
		})
	}
}

func TestDistValidate(t *testing.T) {
	tests := []struct {
		name string
		d    Dist
		lo   int
		ok   bool
	}{
		{"fixed", Fixed(3), 1, true},
		{"fixed zero allowed", Fixed(0), 0, true},
		{"fixed below lo", Fixed(0), 1, false},
		{"fixed above cap", Fixed(maxDistValue + 1), 0, false},
		{"uniform", Dist{Kind: DistUniform, Min: 1, Max: 1}, 1, true},
		{"uniform inverted", Dist{Kind: DistUniform, Min: 5, Max: 4}, 1, false},
		{"uniform unbounded", Dist{Kind: DistUniform, Min: 1, Max: maxDistValue + 1}, 1, false},
		{"lognormal", Dist{Kind: DistLognormal, Median: 10, Sigma: 1, Min: 1, Max: 100}, 1, true},
		{"lognormal no max", Dist{Kind: DistLognormal, Median: 10, Sigma: 1, Min: 1}, 1, false},
		{"lognormal zero median", Dist{Kind: DistLognormal, Sigma: 1, Min: 1, Max: 9}, 1, false},
		{"lognormal negative sigma", Dist{Kind: DistLognormal, Median: 3, Sigma: -1, Min: 1, Max: 9}, 1, false},
		{"lognormal NaN sigma", Dist{Kind: DistLognormal, Median: 3, Sigma: math.NaN(), Min: 1, Max: 9}, 1, false},
		{"missing kind", Dist{}, 0, false},
		{"unknown kind", Dist{Kind: "zipf", Min: 1, Max: 2}, 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.d.Validate(tc.lo); (err == nil) != tc.ok {
				t.Fatalf("Validate = %v, want ok=%v", err, tc.ok)
			}
		})
	}
}

func TestDistSample(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	const n = 50000
	tests := []struct {
		name     string
		d        Dist
		wantMean float64
	}{
		{"fixed", Fixed(7), 7},
		{"uniform", Dist{Kind: DistUniform, Min: 10, Max: 20}, 15},
		{"lognormal clamped", Dist{Kind: DistLognormal, Median: 100, Sigma: 0.8, Min: 20, Max: 400}, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			xs := make([]int, n)
			sum := 0.0
			for i := range xs {
				xs[i] = tc.d.Sample(rng)
				sum += float64(xs[i])
				lo := tc.d.Min
				if tc.d.Kind == DistFixed {
					lo = tc.d.Value
				}
				if xs[i] < lo || xs[i] > tc.d.Upper() {
					t.Fatalf("sample %d outside [%d, %d]", xs[i], lo, tc.d.Upper())
				}
			}
			mean := sum / n
			if want := tc.d.mean(); math.Abs(mean-want)/want > 0.02 {
				t.Errorf("sample mean %.2f, analytic mean %.2f", mean, want)
			}
			if tc.wantMean > 0 && math.Abs(mean-tc.wantMean)/tc.wantMean > 0.02 {
				t.Errorf("sample mean %.2f, want %.2f", mean, tc.wantMean)
			}
			if tc.d.Kind == DistLognormal {
				slices.Sort(xs)
				if med := float64(xs[n/2]); math.Abs(med-tc.d.Median)/tc.d.Median > 0.03 {
					t.Errorf("median %.0f, want %.0f", med, tc.d.Median)
				}
				if xs[0] != tc.d.Min || xs[n-1] != tc.d.Max {
					t.Errorf("clamp not reached: min %d max %d", xs[0], xs[n-1])
				}
			}
		})
	}
}
