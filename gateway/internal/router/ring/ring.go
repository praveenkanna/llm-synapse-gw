// Package ring implements a consistent hash ring with virtual nodes and bounded load.
//
// The bounded load algorithm is based on Mirrokni et al. (Google Research, 2017):
// requests with affinity keys are directed to their primary hash owner on the ring
// unless placing the request there would take that replica's load above
// cap + slack, where cap = ceil((1 + ε) × (total + 1) / n), total is the load
// (in-flight plus queued) across the n available replicas before placement,
// and slack is a small configured allowance (ADR 0015; 0 is strict). Otherwise
// routing overflows to the next candidate replica clockwise along the ring.
//
// A Ring is immutable. Concurrent reads are lock-free and, for rings of up to
// 64 replicas, Route and Pick allocate nothing. Updates are performed by constructing
// a new Ring and replacing it via atomic pointer (RCU).
package ring

import (
	"errors"
	"math"
	"slices"
	"sort"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
)

var (
	// ErrNoReplicas indicates that no replicas are available in the ring.
	ErrNoReplicas = errors.New("no replicas available in ring")
)

const (
	// DefaultVirtualNodes is the default number of virtual nodes per replica.
	// 512 keeps a 3-replica pool's arc shares within about 3% of even;
	// 128 left one replica 15% over (M1-18).
	DefaultVirtualNodes = 512
	// DefaultEpsilon is the default bounded-load headroom parameter ε (25%).
	DefaultEpsilon = 0.25
	// DefaultOverflowSlack is the default number of requests a replica may
	// hold above the bounded-load cap before routing overflows (ADR 0015).
	DefaultOverflowSlack = 1
)

// Config configures consistent hash ring construction.
type Config struct {
	VirtualNodes int
	// Epsilon is the bounded-load headroom ε; ≤ 0 means DefaultEpsilon.
	Epsilon float64
	// OverflowSlack is the number of requests a replica may hold above the
	// cap (ADR 0015). 0 is the strict bound; negative counts as 0. The zero
	// Config is strict: use DefaultConfig for the gateway's default.
	OverflowSlack int
}

// DefaultConfig returns the standard ring configuration.
func DefaultConfig() Config {
	return Config{
		VirtualNodes:  DefaultVirtualNodes,
		Epsilon:       DefaultEpsilon,
		OverflowSlack: DefaultOverflowSlack,
	}
}

type virtualNode struct {
	hash         uint64
	replicaIndex int
}

// Ring is an immutable consistent hash ring.
type Ring struct {
	replicas []discovery.Replica
	nodes    []virtualNode
	epsilon  float64
	slack    int64
	vnodes   int
}

// New builds an immutable consistent hash ring from a replica slice.
// Replicas are sorted by name so construction is strictly deterministic.
func New(replicas []discovery.Replica, cfg Config) *Ring {
	if cfg.VirtualNodes <= 0 {
		cfg.VirtualNodes = DefaultVirtualNodes
	}
	if cfg.Epsilon <= 0 {
		cfg.Epsilon = DefaultEpsilon
	}
	if cfg.OverflowSlack < 0 {
		cfg.OverflowSlack = 0
	}
	if int64(cfg.OverflowSlack) > maxCap {
		cfg.OverflowSlack = int(maxCap)
	}

	// Deduplicate and sort replicas by Name for determinism
	unique := make(map[string]discovery.Replica, len(replicas))
	for _, rep := range replicas {
		unique[rep.Name] = rep
	}

	sortedReps := make([]discovery.Replica, 0, len(unique))
	for _, rep := range unique {
		sortedReps = append(sortedReps, rep)
	}
	slices.SortFunc(sortedReps, func(a, b discovery.Replica) int {
		if a.Name < b.Name {
			return -1
		}
		if a.Name > b.Name {
			return 1
		}
		return 0
	})

	if len(sortedReps) == 0 {
		return &Ring{
			replicas: nil,
			nodes:    nil,
			epsilon:  cfg.Epsilon,
			slack:    int64(cfg.OverflowSlack),
			vnodes:   cfg.VirtualNodes,
		}
	}

	totalNodes := len(sortedReps) * cfg.VirtualNodes
	nodes := make([]virtualNode, 0, totalNodes)

	for idx, rep := range sortedReps {
		for v := 0; v < cfg.VirtualNodes; v++ {
			h := hashVNode(rep.Name, v)
			nodes = append(nodes, virtualNode{
				hash:         h,
				replicaIndex: idx,
			})
		}
	}

	// Sort virtual nodes by hash ascending. Break ties deterministically.
	slices.SortFunc(nodes, func(a, b virtualNode) int {
		if a.hash < b.hash {
			return -1
		}
		if a.hash > b.hash {
			return 1
		}
		if a.replicaIndex < b.replicaIndex {
			return -1
		}
		if a.replicaIndex > b.replicaIndex {
			return 1
		}
		return 0
	})

	return &Ring{
		replicas: sortedReps,
		nodes:    nodes,
		epsilon:  cfg.Epsilon,
		slack:    int64(cfg.OverflowSlack),
		vnodes:   cfg.VirtualNodes,
	}
}

// Replicas returns a copy of the ring's replicas, sorted by name.
func (r *Ring) Replicas() []discovery.Replica {
	return slices.Clone(r.replicas)
}

// Len returns the number of replicas in the ring without copying them.
func (r *Ring) Len() int {
	return len(r.replicas)
}

// Candidates returns the distinct replicas in clockwise ring order for key.
// It allocates the returned slice; the request path uses Pick instead.
func (r *Ring) Candidates(key string) []discovery.Replica {
	if len(r.replicas) == 0 {
		return nil
	}
	out := make([]discovery.Replica, 0, len(r.replicas))
	w := r.walk(key, -1)
	for idx := w.next(); idx >= 0; idx = w.next() {
		out = append(out, r.replicas[idx])
	}
	return out
}

// LoadFunc returns a replica's current load (in-flight plus queued work).
// Negative values count as 0.
type LoadFunc func(replica discovery.Replica) int64

// AcquireFunc claims one in-flight slot on replica and reports whether the
// replica's load, counting that slot, is at most limit. When it reports
// false it must already have given the slot back. Pick calls it with
// limit = NoLimit for a pick that must succeed.
type AcquireFunc func(replica discovery.Replica, limit int64) bool

// NoLimit is the limit Pick passes to an AcquireFunc when the pick must not
// be refused: the bounded walk found no replica within the limit.
const NoLimit = int64(math.MaxInt64)

// Result is the outcome of Pick.
type Result struct {
	// Replica is the replica picked (and acquired, when Pick was given an
	// AcquireFunc).
	Replica discovery.Replica
	// Cap is the bounded-load cap of the decision:
	// ceil((1 + ε) × (total + 1) / n). 0 when Pick had no LoadFunc.
	Cap int64
	// Limit is Cap plus the ring's overflow slack: the most requests the
	// picked replica may hold once this one is placed. 0 when Pick had no
	// LoadFunc.
	Limit int64
	// Primary reports that Replica is the key's primary owner on the ring
	// (its first replica clockwise from the key's hash, exclude ignored).
	Primary bool
	// OverCap reports that no replica could be acquired within Limit, so
	// the least-loaded replica of the last snapshot was taken past it. It
	// only happens when concurrent routes filled every eligible replica
	// between each of the decision's load snapshots and its acquisitions.
	OverCap bool
}

// smallRing is the replica count up to which Pick keeps its load snapshot
// and seen-set on the stack.
const smallRing = 64

// Route picks a replica for key using consistent hashing with bounded load,
// without acquiring it. It is Pick with no AcquireFunc.
func (r *Ring) Route(key string, loadFn LoadFunc, exclude string) (discovery.Replica, error) {
	res, err := r.Pick(key, loadFn, nil, exclude)
	return res.Replica, err
}

// Pick picks a replica for key using consistent hashing with bounded load
// and, given an AcquireFunc, claims its in-flight slot.
//
// Parameters:
//   - key: affinity key extracted from the request (session ID or prefix hash).
//   - loadFn: the current load of a replica. If nil, pure consistent hashing
//     is used and acquire is ignored.
//   - acquire: claims the picked replica's in-flight slot and re-checks it
//     against Limit (see AcquireFunc). If nil, Pick only decides.
//   - exclude: optional replica name to skip (e.g. on connect retry).
//
// Loads are read once per decision into a snapshot, which gives both the cap
// and the walk, so one decision is self-consistent. The cap is
// ceil((1 + ε) × (total + 1) / n) over the n replicas not excluded, and a
// replica is eligible only if its load + 1 ≤ cap + slack (Result.Limit):
// after this request is placed the replica holds at most the cap plus the
// configured overflow slack (ADR 0015; 0 is the strict bound). Walking clockwise from the key's
// hash, the first eligible replica that acquire accepts is picked. With a
// consistent snapshot the least-loaded replica is always eligible (its load
// is at most floor(total/n) and cap ≥ floor(total/n) + 1), so the walk only
// comes up empty when acquire refused every eligible replica because
// concurrent routes filled it. Pick then decides again on a fresh snapshot,
// up to acquireRounds snapshots, and after the last takes the least-loaded
// replica of that snapshot past the limit (Result.OverCap) rather than failing.
//
// The decision is a pure function of the ring, key, exclude and loads, so
// every gateway replica makes the same pick from the same loads.
//
// Returns ErrNoReplicas if no eligible replicas exist in the ring.
func (r *Ring) Pick(key string, loadFn LoadFunc, acquire AcquireFunc, exclude string) (Result, error) {
	n := len(r.replicas)
	if n == 0 {
		return Result{}, ErrNoReplicas
	}
	excludeIdx := r.indexOf(exclude)
	numActive := n
	if excludeIdx >= 0 {
		numActive--
	}
	if numActive <= 0 {
		return Result{}, ErrNoReplicas
	}

	if loadFn == nil {
		w := r.walk(key, excludeIdx)
		idx := w.next()
		return Result{Replica: r.replicas[idx], Primary: idx == w.primary}, nil
	}

	// One snapshot per decision round: the cap and the walk see the same loads.
	var buf [smallRing]int64
	var loads []int64
	if n > smallRing {
		loads = make([]int64, n)
	} else {
		loads = buf[:n]
	}
	for round := 1; ; round++ {
		capacity := r.snapshotCap(loads, loadFn, excludeIdx, numActive)
		limit := capacity + r.slack
		w := r.walk(key, excludeIdx)
		minIdx := -1
		for idx := w.next(); idx >= 0; idx = w.next() {
			if minIdx < 0 || loads[idx] < loads[minIdx] {
				minIdx = idx
			}
			if loads[idx]+1 > limit {
				continue
			}
			if acquire == nil || acquire(r.replicas[idx], limit) {
				return Result{Replica: r.replicas[idx], Cap: capacity, Limit: limit, Primary: idx == w.primary}, nil
			}
		}
		if acquire != nil && round < acquireRounds {
			continue // concurrent routes moved the loads: decide again on fresh ones
		}
		// Every eligible replica was filled by concurrent routes after each
		// snapshot. Fail open to the least-loaded replica past the limit.
		rep := r.replicas[minIdx]
		if acquire != nil {
			acquire(rep, NoLimit)
		}
		return Result{Replica: rep, Cap: capacity, Limit: limit, Primary: minIdx == w.primary, OverCap: true}, nil
	}
}

// acquireRounds is how many load snapshots Pick decides on before it takes a
// replica past the limit. A round only repeats when concurrent routes filled
// every eligible replica between the snapshot and the acquisition; the next
// snapshot sees their slots and so a higher cap.
const acquireRounds = 3

// snapshotCap reads every replica's load once into loads (excluded replica
// skipped, negatives as 0) and returns the decision's cap.
func (r *Ring) snapshotCap(loads []int64, loadFn LoadFunc, excludeIdx, numActive int) int64 {
	var total int64
	for i := range r.replicas {
		if i == excludeIdx {
			continue
		}
		l := loadFn(r.replicas[i])
		if l < 0 {
			l = 0
		}
		loads[i] = l
		total += l
	}
	return boundedCap(r.epsilon, total, numActive)
}

// maxCap bounds boundedCap so the float-to-int conversion and cap + slack
// cannot overflow, whatever ε and the loads are.
const maxCap = int64(1) << 53

// capTolerance absorbs floating-point error in (1 + ε) × (total + 1) / n, so
// a product that is mathematically an integer (ε = 0.1, total + 1 = 10, n = 1
// gives 11.000000000000002) is not rounded up to the next one. It is a few
// ulps relative to x: a looser relative tolerance would swallow real
// fractional parts at large totals and return a cap one below the exact one.
const capTolerance = 8 * 2.220446049250313e-16

// boundedCap returns ceil((1 + ε) × (total + 1) / n): the most requests any
// replica may hold, before slack, once the request being routed is placed.
func boundedCap(epsilon float64, total int64, n int) int64 {
	x := (1.0 + epsilon) * float64(total+1) / float64(n)
	if !(x < float64(maxCap)) { // also catches NaN
		return maxCap
	}
	f := math.Floor(x)
	if x-f <= capTolerance*math.Max(1, x) {
		return int64(f)
	}
	return int64(f) + 1
}

// indexOf returns the index of the replica named name, or -1.
func (r *Ring) indexOf(name string) int {
	if name == "" {
		return -1
	}
	for i := range r.replicas {
		if r.replicas[i].Name == name {
			return i
		}
	}
	return -1
}

// ringWalk yields the distinct replica indexes of a ring in clockwise order
// from a key's hash, skipping one excluded index. Rings of up to smallRing
// replicas track the seen set in a word, so walking allocates nothing.
type ringWalk struct {
	r       *Ring
	pos     int // next virtual node to visit, relative to start
	start   int
	left    int // distinct replicas still to yield
	primary int // index of the key's primary owner, exclude ignored
	small   uint64
	large   []bool
}

func (r *Ring) walk(key string, exclude int) ringWalk {
	h := hash64(key)
	start := sort.Search(len(r.nodes), func(i int) bool {
		return r.nodes[i].hash >= h
	})
	if start >= len(r.nodes) {
		start = 0
	}
	w := ringWalk{
		r:       r,
		start:   start,
		left:    len(r.replicas),
		primary: r.nodes[start].replicaIndex,
	}
	if len(r.replicas) > smallRing {
		w.large = make([]bool, len(r.replicas))
	}
	if exclude >= 0 {
		w.left--
		w.markSeen(exclude)
	}
	return w
}

// markSeen records idx as yielded and reports whether it was new.
func (w *ringWalk) markSeen(idx int) bool {
	if w.large != nil {
		if w.large[idx] {
			return false
		}
		w.large[idx] = true
		return true
	}
	mask := uint64(1) << uint(idx)
	if w.small&mask != 0 {
		return false
	}
	w.small |= mask
	return true
}

// next returns the next distinct replica index clockwise, or -1 when every
// replica not excluded has been yielded.
func (w *ringWalk) next() int {
	nodes := w.r.nodes
	for w.left > 0 && w.pos < len(nodes) {
		idx := nodes[(w.start+w.pos)%len(nodes)].replicaIndex
		w.pos++
		if w.markSeen(idx) {
			w.left--
			return idx
		}
	}
	return -1
}

// 64-bit FNV-1a constants with fmix64 bit-mixer for high-entropy distribution.
const (
	offset64 = 14695981039346656037
	prime64  = 1099511628211
)

func hash64(s string) uint64 {
	var h uint64 = offset64
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	return fmix64(h)
}

func hashVNode(name string, v int) uint64 {
	var h uint64 = offset64
	for i := 0; i < len(name); i++ {
		h ^= uint64(name[i])
		h *= prime64
	}
	h ^= uint64('#')
	h *= prime64
	h ^= uint64(v)
	h *= prime64
	return fmix64(h)
}

func fmix64(k uint64) uint64 {
	k ^= k >> 33
	k *= 0xff51afd7ed558ccd
	k ^= k >> 33
	k *= 0xc4ceb9fe1a85ec53
	k ^= k >> 33
	return k
}
