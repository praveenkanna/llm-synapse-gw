package health

import (
	"maps"
	"slices"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
)

// replicaLister lists every replica of every pool. discovery.Inventory adds
// a version to it; a lister without one is compared in full on every sync.
type replicaLister interface {
	AllReplicas() []discovery.Replica
}

// membership is an immutable view of the replica set the tracker last
// applied from discovery: rings are built from it and health state exists
// for exactly its replicas. It is swapped whole (atomic pointer), so the
// request path reads it without locks.
type membership struct {
	version uint64
	byName  map[string]discovery.Replica
	byPool  map[string][]discovery.Replica // discovery order
	// progress holds each replica's first-chunk and stall counts
	// (stuck.go). A replica that stayed at the same address keeps them
	// across reloads; a new or re-pointed one starts with fresh ones.
	progress map[string]*replicaProgress
}

// newMembership builds the set all, carrying over prev's counts
// for replicas still at the same address.
func newMembership(version uint64, all []discovery.Replica, prev *membership) *membership {
	m := &membership{
		version:  version,
		byName:   make(map[string]discovery.Replica, len(all)),
		byPool:   make(map[string][]discovery.Replica),
		progress: make(map[string]*replicaProgress, len(all)),
	}
	for _, rep := range all {
		m.byName[rep.Name] = rep
		m.byPool[rep.Pool] = append(m.byPool[rep.Pool], rep)
		if prev != nil {
			if old, ok := prev.byName[rep.Name]; ok && sameEndpoint(old, rep) {
				m.progress[rep.Name] = prev.progress[rep.Name]
				continue
			}
		}
		m.progress[rep.Name] = &replicaProgress{}
	}
	return m
}

// sameEndpoint reports whether a and b are the same replica at the same
// address in the same pool. A replica re-pointed under its old name is a
// different endpoint.
func sameEndpoint(a, b discovery.Replica) bool {
	if a.Name != b.Name || a.Pool != b.Pool {
		return false
	}
	if a.BaseURL == b.BaseURL {
		return true // the same parsed URL: leases carry the applied set's pointers
	}
	if a.BaseURL == nil || b.BaseURL == nil {
		return false
	}
	return a.BaseURL.String() == b.BaseURL.String()
}

// SyncDiscovery applies the current replica set from discovery, so routing
// follows a hot reload. Pools whose replicas were added, removed or
// re-pointed get a new ring built from the new set (new base URLs
// included); added replicas start healthy; removed replicas lose their
// health state; replicas that stayed at the same address keep theirs,
// ejections included. A replica re-pointed to a new URL starts healthy
// again: its old health was about the old address.
//
// SyncDiscovery does no I/O and never runs on the request path: the
// background loop calls it as soon as discovery signals a change (and every
// poll interval), and PollOnce before it probes. With a discovery.Inventory
// it returns at once unless Version moved past the applied one. It reports
// whether membership changed.
func (t *Tracker) SyncDiscovery() bool {
	if t.lister == nil {
		return false
	}
	var ver uint64
	if t.inv != nil {
		ver = t.inv.Version() // before listing: see discovery.Inventory
		if m := t.members.Load(); m != nil && m.version >= ver {
			return false
		}
	}
	// List outside statesMu, so passive reports on the request path never
	// wait on discovery.
	all := t.lister.AllReplicas()

	t.statesMu.Lock()
	defer t.statesMu.Unlock()
	prev := t.members.Load()
	if t.inv != nil && prev != nil && prev.version >= ver {
		return false // a concurrent sync already applied this version or a newer one
	}
	next := newMembership(ver, all, prev)

	rebuild := make(map[string]bool)
	now := t.now()
	for name, rep := range next.byName {
		var old discovery.Replica
		known := false
		if prev != nil {
			old, known = prev.byName[name]
		}
		if known && sameEndpoint(old, rep) {
			continue // same replica, same address: keep its health
		}
		t.states.Store(name, &replicaHealth{name: name, pool: rep.Pool, healthy: true, lastTransition: now})
		rebuild[rep.Pool] = true
		if known {
			rebuild[old.Pool] = true
		}
	}
	if prev != nil {
		for name, old := range prev.byName {
			if _, ok := next.byName[name]; !ok {
				rebuild[old.Pool] = true
			}
		}
		for pool := range prev.byPool {
			if _, ok := next.byPool[pool]; !ok {
				t.views.Delete(pool)
				delete(rebuild, pool)
			}
		}
	}
	// Drop every state outside the new set: removed replicas, and any left
	// by a passive report that raced with an earlier reload.
	t.states.Range(func(key, _ any) bool {
		if _, ok := next.byName[key.(string)]; !ok {
			t.states.Delete(key)
		}
		return true
	})
	for pool := range next.byPool {
		if _, ok := t.views.Load(pool); !ok {
			rebuild[pool] = true
		}
	}

	t.members.Store(next)
	for _, pool := range slices.Sorted(maps.Keys(rebuild)) {
		t.rebuildPoolRing(pool)
	}
	return len(rebuild) > 0 || prev == nil
}

// isMember reports whether replica is in the applied replica set. Without a
// lister there is no set, and every replica counts (legacy behaviour).
func (t *Tracker) isMember(replica string) bool {
	m := t.members.Load()
	if m == nil {
		return true
	}
	_, ok := m.byName[replica]
	return ok
}

// memberReplica returns the current replica named name and whether it is a
// member (without a replica set every name is, with a zero replica).
func (t *Tracker) memberReplica(name string) (discovery.Replica, bool) {
	m := t.members.Load()
	if m == nil {
		return discovery.Replica{}, true
	}
	rep, ok := m.byName[name]
	return rep, ok
}

// isCurrent reports whether rep, as probed, is still a member at the same
// address, so a probe that raced with a reload is not applied to the new set.
func (t *Tracker) isCurrent(rep discovery.Replica) bool {
	_, ok := t.current(rep)
	return ok
}

// current reports whether rep is still a member at the same address and
// returns its first-chunk and stall counts (nil without a lister: there is
// no set, every replica counts as current, and none is watched for stalls).
func (t *Tracker) current(rep discovery.Replica) (*replicaProgress, bool) {
	m := t.members.Load()
	if m == nil {
		return nil, true
	}
	cur, ok := m.byName[rep.Name]
	if !ok || !sameEndpoint(cur, rep) {
		return nil, false
	}
	return m.progress[rep.Name], true
}

// poolReplicas returns pool's replicas from the applied set, or straight from
// discovery when there is none.
func (t *Tracker) poolReplicas(pool string) []discovery.Replica {
	if m := t.members.Load(); m != nil {
		return m.byPool[pool]
	}
	if t.disc == nil {
		return nil
	}
	return t.disc.Replicas(pool)
}

// syncLoop applies discovery changes as soon as discovery signals one
// (Inventory.Changed), and every poll interval as a backstop (and for
// listers without a signal), independently of how long a health poll takes.
func (t *Tracker) syncLoop() {
	defer t.wg.Done()
	ticker := time.NewTicker(t.cfg.PollInterval)
	defer ticker.Stop()
	for {
		var changed <-chan struct{} // nil without a signal: never ready
		if t.inv != nil {
			changed = t.inv.Changed() // before syncing: see discovery.Inventory
		}
		t.SyncDiscovery()
		select {
		case <-t.ctx.Done():
			return
		case <-changed:
		case <-ticker.C:
		}
	}
}
