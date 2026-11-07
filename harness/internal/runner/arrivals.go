package runner

import (
	"errors"
	"math"
	"math/rand/v2"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/harness/internal/outcome"
)

// Burst raises the arrival rate periodically. Windows start at k*Every for
// k >= 1 and last Length; inside a window the rate is Factor times the base
// rate. A zero Burst (Every == 0) disables bursts.
type Burst struct {
	Every  time.Duration `json:"every_ns"`
	Length time.Duration `json:"length_ns"`
	Factor float64       `json:"factor"`
}

// Enabled reports whether bursts are configured.
func (b Burst) Enabled() bool { return b.Every > 0 }

// Windows returns the burst windows, the rule the arrival process and every
// reader share (outcome.BurstWindows).
func (b Burst) Windows() outcome.BurstWindows {
	return outcome.BurstWindows{Every: b.Every, Length: b.Length}
}

// Validate checks the burst shape.
func (b Burst) Validate() error {
	if !b.Enabled() {
		return nil
	}
	if b.Length <= 0 || b.Length >= b.Every {
		return errors.New("burst length must be > 0 and shorter than the burst period")
	}
	if b.Factor <= 0 || math.IsInf(b.Factor, 0) || math.IsNaN(b.Factor) {
		return errors.New("burst factor must be a positive number")
	}
	return nil
}

// Arrivals generates open-loop arrival times: a Poisson process with base
// rate Rate (per second), piecewise-scaled by Burst. The sequence depends only
// on the rng, so a fixed seed gives a fixed schedule.
type Arrivals struct {
	rate  float64 // per second
	burst Burst
	rng   *rand.Rand
	t     float64 // seconds since start of the last arrival
}

// NewArrivals returns an arrival process. rate must be > 0 and burst valid.
func NewArrivals(rate float64, burst Burst, rng *rand.Rand) (*Arrivals, error) {
	if !(rate > 0) || math.IsInf(rate, 0) {
		return nil, errors.New("arrival rate must be a positive number")
	}
	if err := burst.Validate(); err != nil {
		return nil, err
	}
	return &Arrivals{rate: rate, burst: burst, rng: rng}, nil
}

// rateAt returns the instantaneous rate at t and the end of the constant-rate
// segment containing t (+Inf when there are no bursts).
func (a *Arrivals) rateAt(t float64) (rate, segEnd float64) {
	if !a.burst.Enabled() {
		return a.rate, math.Inf(1)
	}
	every, length := a.burst.Every.Seconds(), a.burst.Length.Seconds()
	k := math.Floor(t / every)
	if t >= (k+1)*every { // float rounding at a boundary; keep segments strictly advancing
		k++
	}
	start := k * every
	if k >= 1 && t < start+length {
		return a.rate * a.burst.Factor, start + length
	}
	return a.rate, (k + 1) * every // the bound checked above, not start + every: rounding can put that at t
}

// Next returns the offset of the next arrival from the start of the run.
// Arrival offsets are non-decreasing.
func (a *Arrivals) Next() time.Duration {
	// Inverse of the integrated rate function: draw unit-rate exponential
	// work and spend it across constant-rate segments.
	need := a.rng.ExpFloat64()
	for {
		r, end := a.rateAt(a.t)
		if dt := need / r; a.t+dt < end {
			a.t += dt
			break
		}
		need -= r * (end - a.t)
		a.t = end
	}
	return time.Duration(a.t * float64(time.Second))
}
