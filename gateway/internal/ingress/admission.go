package ingress

import (
	"bytes"
	"errors"
	"net/http"
	"strconv"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/arbitrator"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/priorityprobe"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// Fail-open reason for a body whose top-level "priority" appears more than
// once: the gateway cannot remove it without choosing which one vLLM reads.
const ReasonDuplicatePriority = "duplicate_priority"

// Upstream priority outcomes for gateway_upstream_priority_total.
const (
	PriorityInjected = "injected"
	PriorityReplaced = "replaced"
	PriorityStripped = "stripped"
)

// CodeDuplicatePriority is the 400 error code for a body with more than one
// top-level "priority" sent to a priority_scheduling pool.
const CodeDuplicatePriority = "duplicate_priority"

// admit takes an admission slot in pool for the request's priority class,
// waiting in the class queue while the pool is full. The request is
// answered when refused (429 or 503 with a Retry-After header) and gone
// when its caller left while queued. Without an arbitrator every request is
// admitted with a nil slot. The caller must Release the slot.
func (h *Handler) admit(w http.ResponseWriter, r *http.Request, pool string) (*arbitrator.Slot, step) {
	if h.admission == nil {
		return nil, proceed
	}
	rec := telemetry.FromContext(r.Context())
	slot, err := h.admission.Acquire(r.Context(), pool, PriorityFrom(r.Context()))
	if err == nil {
		if wait := slot.QueueWait(); wait > 0 {
			rec.Queued(wait)
		}
		return slot, proceed
	}
	var rej *arbitrator.Rejection
	if !errors.As(err, &rej) {
		return nil, gone // the caller left while queued
	}
	w.Header().Set("Retry-After", strconv.Itoa(rej.RetryAfterSeconds()))
	if rej.Status == http.StatusServiceUnavailable {
		writeError(w, rej.Status, "request waited too long for capacity in pool "+pool, "service_unavailable", rej.Outcome)
		return nil, answered
	}
	writeError(w, rej.Status, "pool "+pool+" is at capacity", "rate_limit_error", rej.Outcome)
	return nil, answered
}

// refuseDuplicatePriority answers 400 to a body for a priority_scheduling
// pool whose top-level "priority" appears more than once. vLLM reads the
// last one, so such a body could carry a caller-chosen priority past the
// class check (ADR 0018).
func (h *Handler) refuseDuplicatePriority(w http.ResponseWriter, pool string, info bodyInfo, parsed bool) step {
	if !parsed || !h.priorityPools[pool] || info.priorities <= 1 {
		return proceed
	}
	writeError(w, http.StatusBadRequest, `request body has more than one top-level "priority"`, "invalid_request_error", CodeDuplicatePriority)
	return answered
}

// upstreamBody returns the body to send to pool (ADRs 0009, 0018). On a
// pool with priority_scheduling the gateway owns "priority": it sends the
// class's vLLM value when that is non-zero and the pool's probe has
// verified its replicas accept one, and a caller's own value never
// overrides the class, so it is replaced, or removed when no class value
// is sent. On any other pool it removes a caller's non-zero "priority",
// which vLLM would reject without priority scheduling. Nothing else in
// the body changes: the member is spliced in or cut out as bytes, through
// ix, the body's index. When the body cannot be read, or anything is in
// doubt, it goes unchanged. A priority vLLM reads as zero (bodyindex's
// zero rule, "1e-400" included) is accepted by every replica, so it stays.
func (h *Handler) upstreamBody(rec *telemetry.Request, pool, class string, ix *bodyindex.Index, info bodyInfo, parsed bool) []byte {
	body := ix.Body()
	if !parsed {
		return body
	}
	if h.priorityPools[pool] {
		if info.priorities > 1 {
			return body // refused before admission (refuseDuplicatePriority)
		}
		v, send := h.classPriority(rec, pool, class)
		if info.priorities == 0 {
			if !send {
				return body
			}
			h.metrics.UpstreamPriority(pool, PriorityInjected)
			return insertFirst(ix, body, priorityMember(v))
		}
		if (send && string(info.priority) == strconv.Itoa(v)) || (!send && bodyindex.IsZeroNumber(info.priority)) {
			return body // already what the class sends
		}
		stripped := stripPriority(ix)
		if !send {
			h.metrics.UpstreamPriority(pool, PriorityStripped)
			return stripped
		}
		h.metrics.UpstreamPriority(pool, PriorityReplaced)
		return insertFirst(ix, stripped, priorityMember(v))
	}
	if info.priorities == 0 || !isNumber(info.priority) || bodyindex.IsZeroNumber(info.priority) {
		return body
	}
	if info.priorities > 1 {
		rec.FailOpen(telemetry.StageUpstreamPriority, ReasonDuplicatePriority)
		return body
	}
	h.metrics.UpstreamPriority(pool, PriorityStripped)
	return stripPriority(ix)
}

// classPriority returns class's vLLM priority for pool and whether to send
// it: non-zero (zero is vLLM's default) and the pool's probe has verified.
func (h *Handler) classPriority(rec *telemetry.Request, pool, class string) (int, bool) {
	p := h.priority.Load()
	if p == nil {
		return 0, false
	}
	v := p.UpstreamPriority(class)
	if v == 0 {
		return 0, false
	}
	if h.priorityVerified == nil || !h.priorityVerified(pool) {
		rec.FailOpen(telemetry.StageUpstreamPriority, priorityprobe.ReasonPending)
		return 0, false
	}
	return v, true
}

// priorityMember is the member that sends vLLM priority v.
func priorityMember(v int) []byte {
	return strconv.AppendInt([]byte(`"priority":`), int64(v), 10)
}

func appendPriorityMember(dst []byte, v int) []byte {
	dst = append(dst, `"priority":`...)
	return strconv.AppendInt(dst, int64(v), 10)
}

func equalIntBytes(raw []byte, v int) bool {
	var tmp [24]byte
	return bytes.Equal(raw, strconv.AppendInt(tmp[:0], int64(v), 10))
}

func findPriorityMember(ix *bodyindex.Index) bodyindex.Member {
	if ix.Err() == nil {
		m, _ := ix.Find("priority")
		return m
	}
	var last bodyindex.Member
	ix.Walk(func(m bodyindex.Member) {
		if ix.Is(m, "priority") {
			last = m
		}
	})
	return last
}

// planPriority determines the priority member to prepend and/or the existing
// priority member to remove without allocating a body copy, allowing priority
// and max_tokens splices to be fused into a single allocation.
func (h *Handler) planPriority(rec *telemetry.Request, pool, class string, ix *bodyindex.Index, info bodyInfo, parsed bool, buf []byte) (entry []byte, remove bodyindex.Member) {
	if !parsed {
		return nil, bodyindex.Member{}
	}
	if h.priorityPools[pool] {
		if info.priorities > 1 {
			return nil, bodyindex.Member{} // refused before admission (refuseDuplicatePriority)
		}
		v, send := h.classPriority(rec, pool, class)
		if info.priorities == 0 {
			if !send {
				return nil, bodyindex.Member{}
			}
			h.metrics.UpstreamPriority(pool, PriorityInjected)
			return appendPriorityMember(buf, v), bodyindex.Member{}
		}
		if (send && equalIntBytes(info.priority, v)) || (!send && bodyindex.IsZeroNumber(info.priority)) {
			return nil, bodyindex.Member{} // already what the class sends
		}
		prio := findPriorityMember(ix)
		if !send {
			h.metrics.UpstreamPriority(pool, PriorityStripped)
			return nil, prio
		}
		h.metrics.UpstreamPriority(pool, PriorityReplaced)
		return appendPriorityMember(buf, v), prio
	}
	if info.priorities == 0 || !isNumber(info.priority) || bodyindex.IsZeroNumber(info.priority) {
		return nil, bodyindex.Member{}
	}
	if info.priorities > 1 {
		rec.FailOpen(telemetry.StageUpstreamPriority, ReasonDuplicatePriority)
		return nil, bodyindex.Member{}
	}
	h.metrics.UpstreamPriority(pool, PriorityStripped)
	return nil, findPriorityMember(ix)
}

// The splices of the body sent upstream. While the body is indexed they go
// through ix, which then indexes the new body, so the next splice reads
// fresh offsets. A body past bodyindex.MaxMembers has no members in its
// index: it is spliced as bytes, with the byte-level splices the index's
// own are built on, and ix keeps its first version, whose '{' (ix.Open)
// every later version shares, since no splice inside the object moves it.

// stripPriority returns the body without its one top-level "priority".
func stripPriority(ix *bodyindex.Index) []byte {
	if ix.Err() == nil {
		m, _ := ix.Find("priority")
		return ix.Remove(m, ix)
	}
	var last bodyindex.Member // nothing has spliced a body past the ceiling yet: ix holds it
	ix.Walk(func(m bodyindex.Member) {
		if ix.Is(m, "priority") {
			last = m
		}
	})
	return bodyindex.Remove(ix.Body(), last)
}

// insertFirst returns body, the upstream body as built so far, with member
// (`"key":value`) as its object's first member. While the body is indexed,
// body is ix's own.
func insertFirst(ix *bodyindex.Index, body, member []byte) []byte {
	if ix.Err() == nil {
		return ix.InsertFirst(member, ix)
	}
	return bodyindex.InsertFirst(body, ix.Open(), member)
}

// isNumber reports whether raw, a valid JSON value, is a number. Any other
// "priority" is left for the replica to reject.
func isNumber(raw []byte) bool {
	return len(raw) > 0 && (raw[0] == '-' || (raw[0] >= '0' && raw[0] <= '9'))
}
