package ring_test

import (
	"fmt"
	"math"
	"math/rand"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/ring"
)

func makeReplicas(names ...string) []discovery.Replica {
	reps := make([]discovery.Replica, len(names))
	for i, name := range names {
		u, _ := url.Parse(fmt.Sprintf("http://127.0.0.1:800%d", i+1))
		reps[i] = discovery.Replica{
			Pool:    "llama-pool",
			Name:    name,
			BaseURL: u,
		}
	}
	return reps
}

func TestRingDeterminism(t *testing.T) {
	repsA := makeReplicas("alpha", "beta", "gamma", "delta")
	repsB := makeReplicas("delta", "gamma", "beta", "alpha")

	cfg := ring.DefaultConfig()
	ringA := ring.New(repsA, cfg)
	ringB := ring.New(repsB, cfg)

	keys := []string{"sess-1", "sess-2", "turn-hash-xyz", "conv-999"}
	for _, k := range keys {
		candsA := ringA.Candidates(k)
		candsB := ringB.Candidates(k)

		if len(candsA) != len(candsB) {
			t.Fatalf("mismatched candidate lengths for key %s", k)
		}
		for i := range candsA {
			if candsA[i].Name != candsB[i].Name {
				t.Fatalf("determinism violated at index %d for key %s: %s vs %s", i, k, candsA[i].Name, candsB[i].Name)
			}
		}
	}
}

func TestUniformSpread(t *testing.T) {
	reps := makeReplicas("rep-0", "rep-1", "rep-2", "rep-3")
	r := ring.New(reps, ring.Config{VirtualNodes: 256, Epsilon: 0.25})

	counts := make(map[string]int)
	totalKeys := 20000

	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("conversation-key-%d", i)
		picked, err := r.Pick(key, nil, nil, "")
		if err != nil {
			t.Fatalf("route failed: %v", err)
		}
		counts[picked.Replica.Name]++
	}

	expected := float64(totalKeys) / float64(len(reps))
	for name, count := range counts {
		fraction := float64(count) / float64(totalKeys)
		deviation := math.Abs(float64(count)-expected) / expected
		t.Logf("Replica %s: %d keys (%.2f%%, dev: %.2f%%)", name, count, fraction*100, deviation*100)
		// With 256 vnodes, deviation should be well under 15%
		if deviation > 0.15 {
			t.Errorf("Replica %s spread deviation too high: %.2f%%", name, deviation*100)
		}
	}
}

func TestBoundedLoadOverflow(t *testing.T) {
	reps := makeReplicas("rep-0", "rep-1", "rep-2")
	r := ring.New(reps, ring.Config{VirtualNodes: 128, Epsilon: 0.25})

	key := "test-session-overflow"
	cands := r.Candidates(key)
	if len(cands) != 3 {
		t.Fatalf("expected 3 candidates, got %d", len(cands))
	}
	owner, backup, third := cands[0], cands[1], cands[2]

	tests := []struct {
		name    string
		loads   map[string]int64
		want    string
		cap     int64
		primary bool
	}{
		// total 0: cap = ceil(1.25 × 1 / 3) = 1; owner 0 + 1 ≤ 1.
		{name: "idle pool picks the owner", loads: map[string]int64{}, want: owner.Name, cap: 1, primary: true},
		// total 24: cap = ceil(1.25 × 25 / 3) = 11; owner 20 + 1 > 11.
		{name: "loaded owner overflows to the next candidate", loads: map[string]int64{owner.Name: 20, backup.Name: 2, third.Name: 2}, want: backup.Name, cap: 11},
		// total 42: cap = ceil(1.25 × 43 / 3) = 18; owner and backup 20 + 1 > 18.
		{name: "two loaded candidates overflow to the third", loads: map[string]int64{owner.Name: 20, backup.Name: 20, third.Name: 2}, want: third.Name, cap: 18},
		// total 2: cap = ceil(1.25 × 3 / 3) = 2; owner 1 + 1 ≤ 2: the bound counts
		// the request being placed, and the owner may reach the cap exactly.
		{name: "owner may reach the cap exactly", loads: map[string]int64{owner.Name: 1, backup.Name: 1}, want: owner.Name, cap: 2, primary: true},
		// total 3: cap = ceil(1.25 × 4 / 3) = 2; owner 2 + 1 > 2.
		{name: "owner may not exceed the cap", loads: map[string]int64{owner.Name: 2, backup.Name: 1}, want: backup.Name, cap: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := r.Pick(key, func(rep discovery.Replica) int64 { return tc.loads[rep.Name] }, nil, "")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Replica.Name != tc.want || got.Cap != tc.cap || got.Primary != tc.primary || got.OverCap {
				t.Fatalf("Route = %s cap %d primary %v overCap %v, want %s cap %d primary %v overCap false",
					got.Replica.Name, got.Cap, got.Primary, got.OverCap, tc.want, tc.cap, tc.primary)
			}
		})
	}
}

// The review case: the old bound computed ceil((1+ε)·avg) before placement
// and accepted load ≤ threshold, so an owner at 32 with others at 23, 23, 22
// went to 33 > (1 + 0.25) × 101 / 4 = 31.6.
func TestBoundCountsTheRequestBeingPlaced(t *testing.T) {
	reps := makeReplicas("w-0", "w-1", "w-2", "w-3")
	r := ring.New(reps, ring.Config{VirtualNodes: 128, Epsilon: 0.25})
	key := "review-s1"
	cands := r.Candidates(key)
	loads := map[string]int64{cands[0].Name: 32, cands[1].Name: 23, cands[2].Name: 23, cands[3].Name: 22}

	got, err := r.Pick(key, func(rep discovery.Replica) int64 { return loads[rep.Name] }, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	// cap = ceil(1.25 × 101 / 4) = 32: the owner (32 + 1) is over it.
	if got.Cap != 32 {
		t.Fatalf("cap = %d, want 32", got.Cap)
	}
	if got.Replica.Name == cands[0].Name || got.Primary {
		t.Fatalf("owner at 32 picked with cap 32: it would hold 33")
	}
	if got.Replica.Name != cands[1].Name {
		t.Fatalf("picked %s, want the next candidate %s", got.Replica.Name, cands[1].Name)
	}
}

// exactCap is the bound the tests check against, in exact integer
// arithmetic with ε = num/den: ceil((den + num) × (total + 1) / (den × n)).
func exactCap(num, den, total int64, n int) int64 {
	p := (den + num) * (total + 1)
	q := den * int64(n)
	return (p + q - 1) / q
}

// Property: for random loads, ring sizes, keys, exclusions, ε and slack, the
// picked replica's load after placement never exceeds cap + slack, and with
// a single consistent snapshot the bound is always met (no OverCap).
func TestBoundedLoadProperty(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	epsilons := []struct{ num, den int64 }{{1, 20}, {1, 10}, {1, 4}, {1, 2}, {1, 1}}
	for _, e := range epsilons {
		eps := float64(e.num) / float64(e.den)
		for _, slack := range []int{0, 1, 3} {
			for _, n := range []int{1, 2, 3, 4, 7, 16, 64, 65, 100} {
				names := make([]string, n)
				for i := range names {
					names[i] = fmt.Sprintf("r-%03d", i)
				}
				r := ring.New(makeReplicas(names...), ring.Config{VirtualNodes: 32, Epsilon: eps, OverflowSlack: slack})
				for trial := 0; trial < 120; trial++ {
					loads := make(map[string]int64, n)
					maxLoad := rng.Int63n(200) + 1
					for _, name := range names {
						loads[name] = rng.Int63n(maxLoad)
					}
					if trial%10 == 0 { // one hot replica
						loads[names[rng.Intn(n)]] += 1000
					}
					exclude := ""
					if n > 1 && trial%3 == 0 {
						exclude = names[rng.Intn(n)]
					}
					var total int64
					active := 0
					for name, l := range loads {
						if name != exclude {
							total += l
							active++
						}
					}
					wantCap := exactCap(e.num, e.den, total, active)
					wantLimit := wantCap + int64(slack)

					got, err := r.Pick(fmt.Sprintf("key-%d", trial), func(rep discovery.Replica) int64 { return loads[rep.Name] }, nil, exclude)
					if err != nil {
						t.Fatalf("eps %v slack %d n %d trial %d: %v", eps, slack, n, trial, err)
					}
					if got.Replica.Name == exclude {
						t.Fatalf("eps %v slack %d n %d trial %d: picked the excluded replica", eps, slack, n, trial)
					}
					if got.Cap != wantCap || got.Limit != wantLimit {
						t.Fatalf("eps %v slack %d n %d trial %d: cap %d limit %d, want %d and %d", eps, slack, n, trial, got.Cap, got.Limit, wantCap, wantLimit)
					}
					if after := loads[got.Replica.Name] + 1; after > wantLimit || got.OverCap {
						t.Fatalf("eps %v slack %d n %d trial %d: %s holds %d after placement, cap %d + slack %d (overCap %v)",
							eps, slack, n, trial, got.Replica.Name, after, wantCap, slack, got.OverCap)
					}
				}
			}
		}
	}
}

// Floating-point error must not bump the cap: (1 + 0.1) × 10 / 1 is
// 11.000000000000002 in float64, and the cap is 11, not 12.
func TestBoundedCapIgnoresFloatError(t *testing.T) {
	r := ring.New(makeReplicas("solo"), ring.Config{Epsilon: 0.1})
	got, err := r.Pick("k", func(discovery.Replica) int64 { return 9 }, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if got.Cap != 11 {
		t.Fatalf("cap = %d, want 11", got.Cap)
	}
	for _, c := range []struct {
		num, den, total int64
		n               int
	}{{1, 10, 9, 1}, {1, 10, 19, 2}, {3, 10, 9, 1}, {1, 4, 3, 1}, {1, 20, 19, 1}} {
		eps := float64(c.num) / float64(c.den)
		r := ring.New(makeReplicas("solo"), ring.Config{Epsilon: eps})
		total := c.total
		reps := []string{"solo"}
		if c.n == 2 {
			r = ring.New(makeReplicas("a", "b"), ring.Config{Epsilon: eps})
			reps = []string{"a", "b"}
		}
		per := total / int64(len(reps))
		loads := map[string]int64{}
		for i, name := range reps {
			loads[name] = per
			if i == 0 {
				loads[name] += total - per*int64(len(reps))
			}
		}
		got, _ := r.Pick("k", func(rep discovery.Replica) int64 { return loads[rep.Name] }, nil, "")
		if want := exactCap(c.num, c.den, total, c.n); got.Cap != want {
			t.Errorf("ε %v total %d n %d: cap %d, want %d", eps, total, c.n, got.Cap, want)
		}
	}
}

// The low-load case behind ADR 0015: an idle 3-replica pool whose owner
// already serves one request of a conversation. total 1, so cap =
// ceil(1.25 × 2 / 3) = 1: the strict bound sends the conversation's second
// request elsewhere (losing its prefix cache); one slot of slack keeps it.
func TestOverflowSlackKeepsLowLoadConversation(t *testing.T) {
	reps := makeReplicas("l-0", "l-1", "l-2")
	key := "low-load-conversation"
	for _, tc := range []struct {
		slack       int
		wantPrimary bool
	}{{slack: 0, wantPrimary: false}, {slack: 1, wantPrimary: true}} {
		r := ring.New(reps, ring.Config{Epsilon: 0.25, OverflowSlack: tc.slack})
		owner := r.Candidates(key)[0]
		got, err := r.Pick(key, func(rep discovery.Replica) int64 {
			if rep.Name == owner.Name {
				return 1
			}
			return 0
		}, nil, "")
		if err != nil {
			t.Fatal(err)
		}
		if got.Cap != 1 || got.Limit != 1+int64(tc.slack) || got.Primary != tc.wantPrimary || (got.Replica.Name == owner.Name) != tc.wantPrimary {
			t.Fatalf("slack %d: %+v, want cap 1, limit %d, primary %v", tc.slack, got, 1+tc.slack, tc.wantPrimary)
		}
	}
	if ring.DefaultConfig().OverflowSlack != 1 {
		t.Fatalf("default overflow slack = %d, want 1 (ADR 0015)", ring.DefaultConfig().OverflowSlack)
	}
}

// AC-M1-3 under skew: 80% of requests share one key. Every placement keeps
// the picked replica at or under the cap + slack of its decision.
func TestMathematicalBoundsUnderSkew(t *testing.T) {
	for _, slack := range []int{0, ring.DefaultOverflowSlack} {
		t.Run(fmt.Sprintf("slack %d", slack), func(t *testing.T) { boundsUnderSkew(t, slack) })
	}
}

func boundsUnderSkew(t *testing.T, slack int) {
	reps := makeReplicas("worker-0", "worker-1", "worker-2", "worker-3")
	r := ring.New(reps, ring.Config{VirtualNodes: 256, Epsilon: 0.25, OverflowSlack: slack})

	inFlight := make(map[string]int64)
	loadFn := func(rep discovery.Replica) int64 { return inFlight[rep.Name] }
	rng := rand.New(rand.NewSource(42))

	for i := 0; i < 2000; i++ {
		key := "hot-conversation"
		if rng.Float64() >= 0.80 {
			key = fmt.Sprintf("random-key-%d", rng.Intn(100))
		}
		var total int64
		for _, rep := range reps {
			total += inFlight[rep.Name]
		}
		limit := exactCap(1, 4, total, len(reps)) + int64(slack)

		picked, err := r.Pick(key, loadFn, nil, "")
		if err != nil {
			t.Fatalf("route failed: %v", err)
		}
		inFlight[picked.Replica.Name]++
		if got := inFlight[picked.Replica.Name]; got > limit {
			t.Fatalf("step %d: %s holds %d after placement, cap + slack %d (total before %d)", i, picked.Replica.Name, got, limit, total)
		}
		if i%2 == 0 { // complete one request
			for _, rep := range reps {
				if inFlight[rep.Name] > 0 {
					inFlight[rep.Name]--
					break
				}
			}
		}
	}
	t.Logf("final in-flight loads: %v", inFlight)
}

// Loads are read once per replica per decision, and that snapshot gives
// both the cap and the walk: a load function that changes between calls
// cannot make the decision inconsistent.
func TestRouteReadsEachLoadOnce(t *testing.T) {
	for _, n := range []int{4, 80} {
		names := make([]string, n)
		for i := range names {
			names[i] = fmt.Sprintf("s-%03d", i)
		}
		r := ring.New(makeReplicas(names...), ring.DefaultConfig())
		key := "snapshot-key"
		cands := r.Candidates(key)
		for _, exclude := range []string{"", cands[0].Name} {
			calls := map[string]int{}
			// Each read reports more load than the last: the owner reads 0
			// the first time and 1000 after, so a second read would push it
			// over the cap computed from the first.
			got, err := r.Pick(key, func(rep discovery.Replica) int64 {
				calls[rep.Name]++
				if calls[rep.Name] > 1 {
					return 1000
				}
				return 0
			}, nil, exclude)
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range names {
				want := 1
				if name == exclude {
					want = 0
				}
				if calls[name] != want {
					t.Fatalf("n %d exclude %q: load of %s read %d times, want %d", n, exclude, name, calls[name], want)
				}
			}
			wantPick := cands[0]
			if exclude != "" {
				wantPick = cands[1]
			}
			if got.Replica.Name != wantPick.Name || got.Cap != 1 {
				t.Fatalf("n %d exclude %q: picked %s cap %d, want %s cap 1", n, exclude, got.Replica.Name, got.Cap, wantPick.Name)
			}
		}
	}
}

func TestRoutePrimaryFlag(t *testing.T) {
	r := ring.New(makeReplicas("p-0", "p-1", "p-2"), ring.DefaultConfig())
	key := "primary-key"
	cands := r.Candidates(key)

	got, _ := r.Pick(key, nil, nil, "")
	if !got.Primary || got.Replica.Name != cands[0].Name {
		t.Fatalf("pure hashing: %+v, want primary %s", got, cands[0].Name)
	}
	got, _ = r.Pick(key, nil, nil, cands[0].Name)
	if got.Primary || got.Replica.Name != cands[1].Name {
		t.Fatalf("primary excluded: %+v, want non-primary %s", got, cands[1].Name)
	}
	got, _ = r.Pick(key, nil, nil, cands[1].Name)
	if !got.Primary {
		t.Fatalf("excluding a non-primary: %+v, want the primary", got)
	}
	loads := map[string]int64{cands[0].Name: 50}
	got, _ = r.Pick(key, func(rep discovery.Replica) int64 { return loads[rep.Name] }, nil, "")
	if got.Primary || got.Replica.Name != cands[1].Name {
		t.Fatalf("loaded owner: %+v, want overflow to %s", got, cands[1].Name)
	}
}

// acquireLog is a fake AcquireFunc that refuses the replicas in full, as if
// concurrent routes filled them after the snapshot was taken.
type acquireLog struct {
	full  map[string]bool
	calls []string
	lims  []int64
}

func (a *acquireLog) acquire(rep discovery.Replica, limit int64) bool {
	a.calls = append(a.calls, rep.Name)
	a.lims = append(a.lims, limit)
	return limit == ring.NoLimit || !a.full[rep.Name]
}

func TestRouteAcquireMovesOnWhenFull(t *testing.T) {
	r := ring.New(makeReplicas("q-0", "q-1", "q-2"), ring.DefaultConfig())
	key := "acquire-key"
	cands := r.Candidates(key)
	zero := func(discovery.Replica) int64 { return 0 }

	// The owner filled up between snapshot and acquisition: move on.
	acq := &acquireLog{full: map[string]bool{cands[0].Name: true}}
	got, err := r.Pick(key, zero, acq.acquire, "")
	if err != nil {
		t.Fatal(err)
	}
	if got.Replica.Name != cands[1].Name || got.Primary || got.OverCap {
		t.Fatalf("got %+v, want %s within the cap", got, cands[1].Name)
	}
	if fmt.Sprint(acq.calls) != fmt.Sprint([]string{cands[0].Name, cands[1].Name}) || acq.lims[0] != got.Limit || acq.lims[1] != got.Limit || got.Limit != got.Cap+ring.DefaultOverflowSlack {
		t.Fatalf("acquire calls %v limits %v, want owner then backup at cap %d + slack", acq.calls, acq.lims, got.Cap)
	}
}

// The over-cap fallback is reachable only through acquisition: every
// eligible replica refused in every snapshot round, so the least-loaded
// replica of the last snapshot is taken without a limit rather than failing
// the request.
func TestRouteOverCapFallsBackToLeastLoaded(t *testing.T) {
	r := ring.New(makeReplicas("f-0", "f-1", "f-2", "f-3"), ring.DefaultConfig())
	key := "fallback-key"
	cands := r.Candidates(key)
	// total 8: cap = ceil(1.25 × 9 / 4) = 3, limit 4 with the default slack.
	// Eligible (load + 1 ≤ 4): cands[1] (2), cands[2] (1), cands[3] (0); not the owner (5).
	loads := map[string]int64{cands[0].Name: 5, cands[1].Name: 2, cands[2].Name: 1, cands[3].Name: 0}
	reads := 0
	full := map[string]bool{}
	for _, c := range cands {
		full[c.Name] = true
	}
	acq := &acquireLog{full: full}
	got, err := r.Pick(key, func(rep discovery.Replica) int64 { reads++; return loads[rep.Name] }, acq.acquire, "")
	if err != nil {
		t.Fatal(err)
	}
	if !got.OverCap || got.Replica.Name != cands[3].Name || got.Cap != 3 || got.Primary {
		t.Fatalf("got %+v, want over-cap pick of least-loaded %s with cap 3", got, cands[3].Name)
	}
	// Three rounds, each one snapshot (4 reads) offering only the eligible
	// candidates (never the owner at 5), then the fallback without a limit.
	var want []string
	for round := 0; round < 3; round++ {
		want = append(want, cands[1].Name, cands[2].Name, cands[3].Name)
	}
	want = append(want, cands[3].Name)
	if fmt.Sprint(acq.calls) != fmt.Sprint(want) || acq.lims[len(acq.lims)-1] != ring.NoLimit || reads != 12 {
		t.Fatalf("acquire calls %v limits %v reads %d, want %v ending with NoLimit and 12 reads", acq.calls, acq.lims, reads, want)
	}
}

// When concurrent routes filled every eligible replica, the next round
// decides on a fresh snapshot: their slots raise the total and so the cap.
func TestRouteRetriesOnFreshSnapshot(t *testing.T) {
	r := ring.New(makeReplicas("g-0", "g-1"), ring.DefaultConfig())
	key := "fresh-key"
	cands := r.Candidates(key)
	loads := map[string]int64{}
	refused := 0
	acquire := func(rep discovery.Replica, limit int64) bool {
		if refused < 2 { // a burst lands on both replicas after the first snapshot
			refused++
			loads[cands[0].Name], loads[cands[1].Name] = 1, 1
			return false
		}
		return true
	}
	got, err := r.Pick(key, func(rep discovery.Replica) int64 { return loads[rep.Name] }, acquire, "")
	if err != nil {
		t.Fatal(err)
	}
	// Round 1: total 0, cap 1, both refused. Round 2: total 2, cap ceil(1.25 × 3 / 2) = 2: the owner.
	if got.OverCap || !got.Primary || got.Cap != 2 || got.Replica.Name != cands[0].Name {
		t.Fatalf("got %+v, want the owner %s within cap 2 on the second snapshot", got, cands[0].Name)
	}
}

// Every gateway replica builds the same ring from the same replicas in any
// order and, given the same loads, makes the same decision.
func TestRouteDeterministicAcrossGatewayReplicas(t *testing.T) {
	a := ring.New(makeReplicas("alpha", "beta", "gamma", "delta"), ring.DefaultConfig())
	b := ring.New(makeReplicas("delta", "beta", "alpha", "gamma"), ring.DefaultConfig())
	loads := map[string]int64{"alpha": 7, "beta": 3, "gamma": 9, "delta": 1}
	loadFn := func(rep discovery.Replica) int64 { return loads[rep.Name] }
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("k-%d", i)
		ga, _ := a.Pick(key, loadFn, nil, "")
		gb, _ := b.Pick(key, loadFn, nil, "")
		if ga.Replica.Name != gb.Replica.Name || ga.Cap != gb.Cap || ga.Primary != gb.Primary || ga.OverCap != gb.OverCap {
			t.Fatalf("key %s: %+v vs %+v", key, ga, gb)
		}
	}
}

func TestReplicasReturnsACopy(t *testing.T) {
	r := ring.New(makeReplicas("c-0", "c-1"), ring.DefaultConfig())
	got := r.Replicas()
	got[0].Name = "mutated"
	if r.Replicas()[0].Name != "c-0" || r.Len() != 2 {
		t.Fatalf("mutating Replicas() changed the ring: %+v", r.Replicas())
	}
}

func TestExcludeReplica(t *testing.T) {
	reps := makeReplicas("rep-A", "rep-B", "rep-C")
	r := ring.New(reps, ring.DefaultConfig())

	key := "test-key"
	cands := r.Candidates(key)
	primary := cands[0]

	// Route excluding primary
	picked, err := r.Pick(key, nil, nil, primary.Name)
	if err != nil {
		t.Fatalf("route failed: %v", err)
	}
	if picked.Replica.Name == primary.Name {
		t.Errorf("expected primary %s to be excluded, but got it", primary.Name)
	}
	if picked.Replica.Name != cands[1].Name {
		t.Errorf("expected secondary candidate %s, got %s", cands[1].Name, picked.Replica.Name)
	}

	// Single replica ring with exclusion -> ErrNoReplicas
	single := ring.New(makeReplicas("solo"), ring.DefaultConfig())
	_, err = single.Pick(key, nil, nil, "solo")
	if err != ring.ErrNoReplicas {
		t.Errorf("expected ErrNoReplicas, got %v", err)
	}
}

func TestEmptyRing(t *testing.T) {
	r := ring.New(nil, ring.DefaultConfig())
	if len(r.Replicas()) != 0 {
		t.Errorf("expected 0 replicas")
	}
	if c := r.Candidates("foo"); c != nil {
		t.Errorf("expected nil candidates, got %+v", c)
	}
	_, err := r.Pick("foo", nil, nil, "")
	if err != ring.ErrNoReplicas {
		t.Errorf("expected ErrNoReplicas, got %v", err)
	}
}

func TestConcurrentReadsLockFree(t *testing.T) {
	reps := makeReplicas("alpha", "beta", "gamma", "delta")
	r := ring.New(reps, ring.DefaultConfig())

	var ops atomic.Uint64
	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < 5000; j++ {
				key := fmt.Sprintf("worker-%d-key-%d", workerID, j)
				picked, err := r.Pick(key, func(rep discovery.Replica) int64 {
					return int64(j % 10)
				}, nil, "")
				if err != nil || picked.Replica.Name == "" {
					t.Errorf("concurrent read error: %v", err)
				}
				ops.Add(1)
			}
		}(i)
	}

	wg.Wait()
	if ops.Load() != 40000 {
		t.Errorf("expected 40000 ops, got %d", ops.Load())
	}
}

// Route is Pick without acquisition, returning only the replica.
func TestRouteMatchesPick(t *testing.T) {
	r := ring.New(makeReplicas("m-0", "m-1", "m-2"), ring.DefaultConfig())
	loads := map[string]int64{"m-0": 9, "m-1": 1}
	loadFn := func(rep discovery.Replica) int64 { return loads[rep.Name] }
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("rk-%d", i)
		for _, exclude := range []string{"", "m-1"} {
			got, err := r.Route(key, loadFn, exclude)
			want, werr := r.Pick(key, loadFn, nil, exclude)
			if err != werr || got.Name != want.Replica.Name {
				t.Fatalf("key %s exclude %q: Route %s %v, Pick %s %v", key, exclude, got.Name, err, want.Replica.Name, werr)
			}
		}
	}
}
