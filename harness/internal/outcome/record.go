package outcome

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
)

// Record is one line of results.jsonl: the measurements of one request. It
// holds no prompt or response text and no credentials. The load generator
// writes it and every reader (report, the proof scripts) reads it; OK,
// Complete, TTFTMS, FirstByteMS and InBurst are the derived facts readers
// use instead of re-deriving them (ADR 0021). Go readers judge success with
// Succeeded, the same rule as OK, so a record written before the ok field
// existed (absent reads as false) is still judged correctly.
type Record struct {
	Seq          int64   `json:"seq"`
	Class        string  `json:"class"`
	Stream       bool    `json:"stream"`
	TraceID      string  `json:"trace_id,omitempty"` // matches the gateway's log line
	ScheduledMS  float64 `json:"scheduled_ms"`
	StartMS      float64 `json:"start_ms"`
	LagMS        float64 `json:"lag_ms"` // start - scheduled: generator falling behind
	Status       int     `json:"status"`
	Error        string  `json:"error,omitempty"`
	OK           bool    `json:"ok"`       // OK(Status, Error)
	Complete     bool    `json:"complete"` // the response was a whole one
	InBurst      bool    `json:"in_burst"` // scheduled inside a burst window
	TTFTMS       float64 `json:"ttft_ms"`  // first token-bearing event; the whole response for a body
	FirstByteMS  float64 `json:"first_byte_ms"`
	DurationMS   float64 `json:"duration_ms"`
	ITLMeanMS    float64 `json:"itl_mean_ms"`
	ITLMaxMS     float64 `json:"itl_max_ms"`
	OutputTokens int     `json:"output_tokens"`
	PromptTokens int     `json:"prompt_tokens"`
	Chunks       int     `json:"chunks"`
	Cache        string  `json:"cache,omitempty"` // X-Gateway-Cache response header, e.g. hit
}

// Succeeded applies the success rule to the record's status and error, so a
// record written before the ok field existed is judged the same way.
func (r Record) Succeeded() bool { return OK(r.Status, r.Error) }

// maxRecordLine bounds one results.jsonl line.
const maxRecordLine = 1 << 20

// ReadRecords calls each for every record in r, in order. It stops at the
// first error: a malformed line, more than limit records (limit > 0), or an
// error from each.
func ReadRecords(r io.Reader, limit int, each func(Record) error) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), maxRecordLine)
	n := 0
	for sc.Scan() {
		if n++; limit > 0 && n > limit {
			return fmt.Errorf("more than %d records", limit)
		}
		var rec Record
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			return fmt.Errorf("line %d: %w", n, err)
		}
		if err := each(rec); err != nil {
			return err
		}
	}
	return sc.Err()
}
