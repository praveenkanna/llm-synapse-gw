package ingress

import "github.com/praveenkanna/llm-synapse-gw/gateway/internal/dedup"

// A generation request's progression. handleGenerate runs the stages in
// order (read the body, resolve the pool, refuse a duplicate priority,
// compile, dedup, assemble context, admit, forward), and each stage only
// says what it decided: go on, answered, or the caller left. What an answer
// records is decided here, once, so no stage can forget part of it:
//
//   - the pool label: recorded as soon as the pool is resolved, so whatever
//     ends the request after that (an answer, a caller that left, a panic)
//     is counted under it; dispatch replaces it with the replica's route
//     when it sends the request;
//   - a caller that left: counted once;
//   - a flight's shared fate: when the request leads a flight, how its
//     exchange ended, whatever stage ended it, is what the flight's
//     followers get and decides whether the response is stored.

// step is what a stage decided.
type step int

const (
	proceed  step = iota // go on to the next stage
	answered             // the request was answered: an error, a cached or a shared response
	gone                 // the caller left before its answer was complete
)

// recorder is what an answer records on (*telemetry.Request).
type recorder interface {
	Route(pool, replica string)
	ClientDisconnected()
}

// progress is one generation request's way through the stages.
type progress struct {
	rec  recorder
	lead *leadership // the flight the request leads, if any
}

// resolved records the request's pool, once it is known.
func (p *progress) resolved(pool string) { p.rec.Route(pool, "") }

// ended records the answer of a stage that ran before dispatch and reports
// whether the request ended there. Such an answer is the gateway's own; a
// caller that left there was sent nothing.
func (p *progress) ended(s step) bool {
	if s == proceed {
		return false
	}
	d := dedup.Delivery{End: dedup.Answered}
	if s == gone {
		d.End = dedup.Cut
	}
	p.finish(s, d)
	return true
}

// sent records the end of a request dispatch sent, with what its relay
// delivered: its route is dispatch's, so only a caller that left and the
// flight's fate remain.
func (p *progress) sent(s step, d dedup.Delivery) { p.finish(s, d) }

func (p *progress) finish(s step, d dedup.Delivery) {
	if s == gone {
		p.rec.ClientDisconnected() // counted as 499, or client_closed when it left mid-response
	}
	if p.lead != nil {
		p.lead.delivered = d
	}
}
