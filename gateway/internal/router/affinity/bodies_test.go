package affinity_test

import (
	"slices"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/affinity"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/affinity/affinitytest"
)

// The body generators live in affinitytest so the whole-route benchmark in
// package router measures the same shapes; these are short local names.
var (
	text         = affinitytest.Text
	escapedJSON  = affinitytest.EscapedJSON
	denseTools   = affinitytest.DenseTools
	imageMessage = affinitytest.ImageMessage
	latencyCases = affinitytest.LatencyCases
	worstCases   = affinitytest.WorstCases
	quantile     = affinitytest.Quantile
	micros       = affinitytest.Micros
)

// measure times n calls of Extract on body, one by one, into a preallocated
// slice and returns them sorted.
func measure(body []byte, n int) []time.Duration {
	lat := make([]time.Duration, n)
	for i := range lat {
		start := time.Now()
		res := affinity.Extract(nil, body)
		lat[i] = time.Since(start)
		if res.Tier == "" {
			panic("unreachable: empty tier")
		}
	}
	slices.Sort(lat)
	return lat
}

// assertResult fails t unless got has the wanted tier (and reason for TierNone).
func assertResult(t *testing.T, got affinity.Result, tier affinity.Tier, reason string) {
	t.Helper()
	if got.Tier != tier || (tier == affinity.TierNone && got.Reason != reason) || (tier != affinity.TierNone && got.Key == "") {
		t.Fatalf("got %+v, want tier %s reason %q", got, tier, reason)
	}
}
