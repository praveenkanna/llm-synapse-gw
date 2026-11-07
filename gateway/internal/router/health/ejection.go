package health

import (
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
)

// Ejection and re-admission.
//
// A replica is ejected by PassiveFailureThreshold requests that failed hard
// to connect (RecordTransportFailure) without a good probe in between, by
// FailureThreshold failed /health probes in a row, or as a stuck
// engine: enough first-chunk stalls within StuckEngineWindow (stuck.go), or
// Eject. The two failure counts are kept apart, so neither kind
// completes the other's threshold.
//
// Whatever the reason, an ejected replica stays out for its ejection time:
// MinEjection, multiplied by EjectionBackoffMultiplier for every ejection
// in a row before it, capped at MaxEjection. It comes back only through
// /health: on the first good probe at or after the ejection time with
// SuccessThreshold good probes in a row since it was ejected. So a
// single good /health never re-admits early, and a stuck engine whose
// /health keeps passing still sits out its ejection time. A replica that
// stays in for MaxEjection after re-admission starts over at MinEjection.
//
// A stuck engine's /health passes, so a replica ejected as stuck_engine
// owes a generation probe as well (probe.go): when /health would re-admit
// it, a probe starts instead, and only a passing one brings it back. A
// failed probe renews the ejection as if the replica had been ejected
// again (backoff included). Without a probe sender, or when the probe
// cannot be built (no model known, request rejected), /health alone
// re-admits it, as for every other reason (fail open, counted).
//
// Every change runs under statesMu and stores a modified copy of the
// replica's state, then republishes its pool's view when the replica
// joined or left the ring.

// maxEjectionSteps bounds the ejections-in-a-row counter; the ejection time
// reaches MaxEjection long before.
const maxEjectionSteps = 64

// RecordTransportFailure records a passive failure of a request on rep: a
// hard connect failure (refused, unreachable, NXDOMAIN) while the caller was
// still connected, once per request. Callers report nothing else: dial
// timeouts (a saturated but healthy host, ADR 0014), cancellations, other
// timeouts and failures after the request was sent say nothing certain
// about the replica's reachability. err is the failure, for future use.
//
// The report is ignored unless rep is still the current replica of that
// name at the same address: after a reload re-pointed or removed it, a
// failure on the old address says nothing about the new endpoint.
func (t *Tracker) RecordTransportFailure(rep discovery.Replica, err error) {
	t.statesMu.Lock()
	defer t.statesMu.Unlock()
	if !t.isCurrent(rep) {
		return
	}
	rh := t.stateFor(rep.Name, rep.Pool)
	rh.passiveFails++
	if rh.healthy && rh.passiveFails >= t.cfg.PassiveFailureThreshold {
		t.eject(rh, ReasonTransportFailure)
		return
	}
	t.states.Store(rh.name, rh)
}

// Eject marks replica as unhealthy and rebuilds the corresponding pool ring.
// A replica no longer in discovery is ignored. A replica already ejected is
// not ejected (or counted) again: it takes reason, and its ejection time is
// extended to a full one from now when that ends later (a stuck engine
// reported again stays out), never shortened.
func (t *Tracker) Eject(replicaName string, reason string) {
	rep, snap, fresh := t.ejectLocked(replicaName, reason)
	if fresh && reason == ReasonStuckEngine {
		t.cancelHung(rep, t.since(t.now()), snap) // as when the stall scan ejects it
	}
}

// ejectLocked is Eject under statesMu. It returns the replica's current
// endpoint, its receipts from before the ejection reset them, and whether
// this call ejected it now (a member that was healthy), as the stall scan
// does; a replica already out had its receipts reset by that ejection.
func (t *Tracker) ejectLocked(replicaName string, reason string) (discovery.Replica, receiptSnapshot, bool) {
	t.statesMu.Lock()
	defer t.statesMu.Unlock()
	rep, ok := t.memberReplica(replicaName)
	if !ok {
		return discovery.Replica{}, receiptSnapshot{}, false
	}
	snap, wasHealthy := t.receiptsOf(replicaName), t.IsHealthy(replicaName)
	t.ejectMember(replicaName, reason)
	return rep, snap, wasHealthy
}

func (t *Tracker) ejectMember(replicaName string, reason string) {
	rh := t.stateFor(replicaName, "")
	if !rh.healthy {
		until := t.now().Add(t.ejectionDuration(max(rh.ejections-1, 0))) // the current ejection's length
		if rh.ejectionReason == reason && !until.After(rh.ejectedUntil) {
			return
		}
		rh.ejectionReason = reason
		rh.ejectedUntil = later(rh.ejectedUntil, until)
		rh.probeOwed = rh.probeOwed || reason == ReasonStuckEngine // once stuck, the probe stays owed
		t.states.Store(rh.name, rh)
		return
	}
	t.eject(rh, reason)
}

func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// Restore re-admits replica at once, bypassing the ejection time: an
// explicit override, never called on its own by the tracker. A replica no
// longer in discovery is ignored.
func (t *Tracker) Restore(replicaName string) {
	t.statesMu.Lock()
	defer t.statesMu.Unlock()
	if !t.isMember(replicaName) {
		return
	}
	rh := t.stateFor(replicaName, "")
	if rh.healthy {
		return
	}
	t.readmit(rh)
}

// recordProbe applies one /health result for rep. A probe that raced with a
// reload removing or re-pointing rep is dropped. It reports whether rep
// now waits only on a generation probe: the caller starts one.
func (t *Tracker) recordProbe(rep discovery.Replica, ok bool) (probe bool) {
	t.statesMu.Lock()
	defer t.statesMu.Unlock()
	if !t.isCurrent(rep) {
		return false // removed or re-pointed while the probe ran: not about the current set
	}
	rh := t.stateFor(rep.Name, rep.Pool)
	now := t.now()
	rh.lastChecked = now
	if ok {
		rh.activePasses++
		rh.activeFails = 0
		rh.passiveFails = 0 // reachable again: earlier connect failures are not "in a row"
		if t.readmissible(rh, now) {
			if t.probeOwed(rh) {
				t.states.Store(rh.name, rh)
				return true
			}
			t.readmit(rh)
			return false
		}
	} else {
		rh.activeFails++
		rh.activePasses = 0
		if rh.healthy && rh.activeFails >= t.cfg.FailureThreshold {
			t.eject(rh, ReasonHealthCheckFail)
			return false
		}
	}
	t.states.Store(rh.name, rh)
	return false
}

// readmissible reports whether the ejected rh meets M1-12's re-admission
// rules at now: its ejection time is over and its last
// SuccessThreshold /health probes passed.
func (t *Tracker) readmissible(rh *replicaHealth, now time.Time) bool {
	return !rh.healthy && rh.activePasses >= t.cfg.SuccessThreshold && !now.Before(rh.ejectedUntil)
}

// probeOwed reports whether rh's re-admission waits on a generation probe.
func (t *Tracker) probeOwed(rh *replicaHealth) bool {
	return rh.probeOwed && t.cfg.StuckProbeSender != nil
}

// stateFor returns a copy of replica's state to modify and store, or a new
// healthy state when there is none. pool, when set, is recorded on it.
func (t *Tracker) stateFor(replica, pool string) *replicaHealth {
	var rh replicaHealth
	if val, ok := t.states.Load(replica); ok {
		rh = *val.(*replicaHealth)
	} else {
		rh = replicaHealth{name: replica, healthy: true, lastTransition: t.now()}
	}
	if pool != "" {
		rh.pool = pool
	}
	return &rh
}

// eject takes the healthy replica rh out of its pool's ring for its
// ejection time, stores it and counts the ejection.
func (t *Tracker) eject(rh *replicaHealth, reason string) {
	now := t.now()
	if now.Sub(rh.lastTransition) >= t.cfg.MaxEjection {
		rh.ejections = 0 // in long enough since the last re-admission: start over
	}
	rh.healthy = false
	rh.ejectionReason = reason
	rh.lastTransition = now
	rh.ejectedUntil = now.Add(t.ejectionDuration(rh.ejections))
	rh.ejections = min(rh.ejections+1, maxEjectionSteps)
	rh.activePasses = 0 // re-admission needs good probes after this
	rh.activeFails = 0
	rh.passiveFails = 0
	rh.probeOwed = reason == ReasonStuckEngine
	t.resetStuckCounts(rh.name)
	t.states.Store(rh.name, rh)
	t.republish(rh)
	t.countEjection(rh.pool, reason)
}

// readmit puts rh back into its pool's ring and stores it.
func (t *Tracker) readmit(rh *replicaHealth) {
	rh.healthy = true
	rh.ejectionReason = ""
	rh.lastTransition = t.now()
	rh.ejectedUntil = time.Time{}
	rh.activeFails = 0
	rh.passiveFails = 0
	rh.probeOwed = false
	t.resetStuckCounts(rh.name)
	t.states.Store(rh.name, rh)
	t.republish(rh)
}

// renewEjection keeps the ejected rh out for another ejection time, as if
// it had been ejected again: its generation probe failed. It is not counted
// as an ejection (rh never re-entered the ring), and re-admission needs
// SuccessThreshold passing /health probes again. Callers hold
// statesMu.
func (t *Tracker) renewEjection(rh *replicaHealth) {
	rh.ejectedUntil = t.now().Add(t.ejectionDuration(rh.ejections))
	rh.ejections = min(rh.ejections+1, maxEjectionSteps)
	rh.activePasses = 0
	t.states.Store(rh.name, rh)
}

// republish rebuilds the view of the pool rh belongs to.
func (t *Tracker) republish(rh *replicaHealth) {
	if rh.pool != "" {
		t.rebuildPoolRing(rh.pool)
	} else if t.disc != nil {
		t.rebuildAllPoolsForReplica(rh.name)
	}
}

// ejectionDuration is the ejection time after prior ejections in a row:
// MinEjection × EjectionBackoffMultiplier^prior, capped at MaxEjection.
func (t *Tracker) ejectionDuration(prior int) time.Duration {
	d := float64(t.cfg.MinEjection)
	limit := float64(t.cfg.MaxEjection)
	for i := 0; i < prior && d < limit; i++ {
		d *= t.cfg.EjectionBackoffMultiplier
	}
	return time.Duration(min(d, limit))
}
