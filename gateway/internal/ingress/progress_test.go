package ingress

import (
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/dedup"
)

// fakeRecorder records what a request's progression recorded.
type fakeRecorder struct {
	routes      []string // "pool/replica"
	disconnects int
}

func (f *fakeRecorder) Route(pool, replica string) { f.routes = append(f.routes, pool+"/"+replica) }
func (f *fakeRecorder) ClientDisconnected()        { f.disconnects++ }

// TestProgressRecordsEveryAnswer: the pool is counted as soon as it is
// resolved, whatever ends the request after; a caller that left is counted
// once; and a flight the request leads is told how its exchange ended:
// the gateway's own answer, cut off when the caller left before dispatch,
// or what the relay delivered.
func TestProgressRecordsEveryAnswer(t *testing.T) {
	tests := []struct {
		name      string
		pool      string // resolved, when set
		leads     bool
		sent      bool // the request went through dispatch
		step      step
		wantEnded bool
		wantGone  int
		delivered dedup.Delivery // what the relay reports, when sent
		wantEnd   dedup.End      // what the flight is told, when leading
	}{
		{name: "goes on", pool: "p", leads: true, step: proceed, wantEnd: dedup.Cut},
		{name: "refused before the pool is known", step: answered, wantEnded: true},
		{name: "caller left before the pool is known", step: gone, wantEnded: true, wantGone: 1},
		{name: "refused", pool: "p", step: answered, wantEnded: true},
		{name: "refused while leading", pool: "p", leads: true, step: answered, wantEnded: true, wantEnd: dedup.Answered},
		{name: "caller left while leading", pool: "p", leads: true, step: gone, wantEnded: true, wantGone: 1, wantEnd: dedup.Cut},
		{name: "sent and relayed whole", pool: "p", leads: true, sent: true, step: answered, delivered: dedup.Delivery{End: dedup.Whole}, wantEnd: dedup.Whole},
		{name: "sent, the response failed", pool: "p", leads: true, sent: true, step: answered, delivered: dedup.Delivery{End: dedup.Failed}, wantEnd: dedup.Failed},
		{name: "sent, caller left", pool: "p", leads: true, sent: true, step: gone, wantGone: 1, wantEnd: dedup.Cut},
		{name: "sent, not leading", pool: "p", sent: true, step: answered, delivered: dedup.Delivery{End: dedup.Whole}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := &fakeRecorder{}
			p := &progress{rec: rec}
			if tc.pool != "" {
				p.resolved(tc.pool)
			}
			if tc.leads {
				p.lead = &leadership{}
			}
			if tc.sent {
				p.sent(tc.step, tc.delivered)
			} else if got := p.ended(tc.step); got != tc.wantEnded {
				t.Fatalf("ended = %v, want %v", got, tc.wantEnded)
			}
			wantRoutes := 0
			if tc.pool != "" {
				wantRoutes = 1
			}
			if len(rec.routes) != wantRoutes || (wantRoutes == 1 && rec.routes[0] != tc.pool+"/") {
				t.Errorf("routes = %v, want the resolved pool once (dispatch records a replica's)", rec.routes)
			}
			if rec.disconnects != tc.wantGone {
				t.Errorf("disconnects = %d on the record, want %d", rec.disconnects, tc.wantGone)
			}
			if tc.leads && p.lead.delivered.End != tc.wantEnd {
				t.Errorf("flight told %v, want %v", p.lead.delivered.End, tc.wantEnd)
			}
		})
	}
}
