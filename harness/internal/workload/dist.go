package workload

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"

	"go.yaml.in/yaml/v3"
)

// Distribution kinds.
const (
	DistFixed     = "fixed"
	DistUniform   = "uniform"
	DistLognormal = "lognormal"
)

// maxDistValue caps every sampled length. Lengths are words or tokens; no
// request the harness builds needs more, and the cap bounds memory.
const maxDistValue = 100_000

// Dist is a distribution of non-negative integer lengths (words, tokens or
// turns). In YAML it is either a plain integer (fixed) or a mapping:
//
//	{dist: fixed, value: 8}
//	{dist: uniform, min: 2, max: 16}              # inclusive
//	{dist: lognormal, median: 200, sigma: 0.6, min: 20, max: 2000}
//
// Lognormal samples are clamped to [min, max], so every distribution is
// bounded.
type Dist struct {
	Kind   string  `yaml:"dist" json:"dist"`
	Value  int     `yaml:"value,omitempty" json:"value,omitempty"`
	Min    int     `yaml:"min,omitempty" json:"min,omitempty"`
	Max    int     `yaml:"max,omitempty" json:"max,omitempty"`
	Median float64 `yaml:"median,omitempty" json:"median,omitempty"`
	Sigma  float64 `yaml:"sigma,omitempty" json:"sigma,omitempty"`
}

// Fixed returns a distribution that always yields v.
func Fixed(v int) Dist { return Dist{Kind: DistFixed, Value: v} }

// UnmarshalYAML accepts a plain integer as shorthand for a fixed value.
func (d *Dist) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind == yaml.ScalarNode {
		var v int
		if n.ShortTag() != "!!int" || n.Decode(&v) != nil {
			return fmt.Errorf("line %d: want an integer or a {dist: ...} mapping", n.Line)
		}
		*d = Fixed(v)
		return nil
	}
	type plain Dist // no UnmarshalYAML: avoids recursion
	var p plain
	if err := n.Decode(&p); err != nil {
		return err
	}
	*d = Dist(p)
	return nil
}

// IsZero reports whether the distribution is unset.
func (d Dist) IsZero() bool { return d == Dist{} }

// Validate checks the parameters and that every sample lies in [lo, maxDistValue].
func (d Dist) Validate(lo int) error {
	inRange := func(v int) bool { return v >= lo && v <= maxDistValue }
	switch d.Kind {
	case DistFixed:
		if !inRange(d.Value) {
			return fmt.Errorf("fixed value %d must be in [%d, %d]", d.Value, lo, maxDistValue)
		}
	case DistUniform:
		if !inRange(d.Min) || !inRange(d.Max) || d.Min > d.Max {
			return fmt.Errorf("uniform needs %d <= min <= max <= %d, got min %d max %d", lo, maxDistValue, d.Min, d.Max)
		}
	case DistLognormal:
		if !inRange(d.Min) || !inRange(d.Max) || d.Min > d.Max {
			return fmt.Errorf("lognormal needs %d <= min <= max <= %d, got min %d max %d", lo, maxDistValue, d.Min, d.Max)
		}
		if !(d.Median > 0) || math.IsInf(d.Median, 0) {
			return errors.New("lognormal median must be a positive number")
		}
		if !(d.Sigma >= 0) || math.IsInf(d.Sigma, 0) {
			return errors.New("lognormal sigma must be a non-negative number")
		}
	case "":
		return errors.New("missing dist (fixed, uniform or lognormal)")
	default:
		return fmt.Errorf("unknown dist %q (want fixed, uniform or lognormal)", d.Kind)
	}
	return nil
}

// Upper returns the largest value the distribution can yield.
func (d Dist) Upper() int {
	if d.Kind == DistFixed {
		return d.Value
	}
	return d.Max
}

// Sample draws one value. d must be valid.
func (d Dist) Sample(rng *rand.Rand) int {
	switch d.Kind {
	case DistUniform:
		return d.Min + rng.IntN(d.Max-d.Min+1)
	case DistLognormal:
		v := math.Round(d.Median * math.Exp(d.Sigma*rng.NormFloat64()))
		return int(max(float64(d.Min), min(float64(d.Max), v)))
	default:
		return d.Value
	}
}
