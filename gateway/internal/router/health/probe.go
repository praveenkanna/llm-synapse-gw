package health

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

// Generation probe.
//
// A stuck engine answers /health while it generates nothing (stuck.go), so
// /health alone would re-admit it after every ejection and hang every
// stream routed to it until the next one. A replica ejected as
// stuck_engine therefore comes back only after a generation probe passes:
// once M1-12's rules hold (ejection time over, SuccessThreshold
// passing /health probes), the poll that found them starts the probe
// instead of re-admitting.
//
// The probe is one streaming completion of one token ("max_tokens": 1,
// "stream": true) with a fixed synthetic prompt (never caller data),
// posted to StuckProbePath through StuckProbeSender, which injects the
// gateway's upstream API key; neither the key nor the probe's request or
// response body is ever logged. It passes when the replica answers 200 and,
// within StuckProbeTimeout and the first maxProbeEventBytes, sends an event
// that shows generation: a choice with non-empty text, content,
// reasoning_content or tool_calls, or a non-null finish_reason (one token
// may be end-of-sequence: text "" with finish_reason "stop" passes).
// Role-only and empty chunks and SSE comments are skipped; [DONE], the end
// of the response or an error event (vLLM reports some failures as
// data: {"error":...} in a 200 stream) before such an event fails it. A
// 200 body that is not an event stream passes only as a completion or
// chat response with choices. It is bounded: at most one probe per replica is in flight, each
// within its own timeout and the poll loop's context, in the background, so
// polling, liveness and requests never wait on it. It takes no lease and
// no first-chunk wait: routing load and stuck-engine counts are untouched.
//
// Outcomes, counted in gateway_stuck_probe_total{pool,outcome}:
//
//	pass      re-admitted, if M1-12's rules still hold (otherwise a later
//	          poll probes again)
//	fail      any status but 200 and those below (5xx, 3xx, 408, 429, other
//	          4xx), transport error, error event, or no generation before
//	          [DONE] or the end: the ejection is renewed (ejection.go)
//	timeout   no generation within StuckProbeTimeout: renewed likewise
//	rejected  400, 401, 403, 404, 405, 415 or 422: the probe itself is
//	          wrong (model name, path, credentials, schema), which says
//	          nothing about the engine, so the replica is re-admitted on
//	          /health alone (fail open), with one warning log per replica
//	          and status (status, path, pool, replica; never a body or key)
//
// The model comes from StuckProbeModel (cmd/gateway wires config's rule,
// config.ProbeModels: the pool's pinned model, else the replica's first
// served model, else the pool's first mapped model by name). With no
// model known the probe cannot be built: the replica is re-admitted on
// /health alone and the fail-open counted. Both fail-opens count as
// gateway_failopen_total{stage="stuck_probe",reason="no_model"|"probe_rejected"}.

// Generation probe paths (router.health.stuck_probe_path).
const (
	PathCompletions     = config.StuckProbePathCompletions
	PathChatCompletions = config.StuckProbePathChatCompletions
)

// Generation probe outcomes (gateway_stuck_probe_total{outcome}).
const (
	ProbePass     = "pass"
	ProbeFail     = "fail"
	ProbeTimeout  = "timeout"
	ProbeRejected = "rejected"
	// probeCanceled: the poll loop stopped; nothing is recorded.
	probeCanceled = ""
)

const (
	// ProbePrompt is the probe's fixed synthetic prompt: never caller data.
	ProbePrompt = "Say OK."
	// maxProbeEventBytes bounds what is read of the probe's response.
	maxProbeEventBytes = 64 << 10
)

// rejectedStatuses are the 4xx answers about the probe request itself
// (bad request, credentials, unknown model or path, method, media type,
// schema): they say nothing about the engine, so the probe fails open.
var rejectedStatuses = map[int]bool{
	http.StatusBadRequest:           true,
	http.StatusUnauthorized:         true,
	http.StatusForbidden:            true,
	http.StatusNotFound:             true,
	http.StatusMethodNotAllowed:     true,
	http.StatusUnsupportedMediaType: true,
	http.StatusUnprocessableEntity:  true,
}

// probeResult is a probe's outcome and the replica's HTTP status (0 when
// no response arrived).
type probeResult struct {
	outcome string
	status  int
}

// ProbeSender sends one upstream request. *upstream.Client implements it,
// adding the gateway's upstream API key.
type ProbeSender interface {
	Do(ctx context.Context, replica discovery.Replica, req upstream.Request) (*http.Response, error)
}

// startGenerationProbe starts rep's generation probe in the background,
// unless one is already in flight. With no model known it re-admits rep on
// /health alone instead (fail open).
func (t *Tracker) startGenerationProbe(ctx context.Context, rep discovery.Replica) {
	model := ""
	if t.cfg.StuckProbeModel != nil {
		model = t.cfg.StuckProbeModel(rep)
	}
	if model == "" {
		t.applyProbeOutcome(rep, probeResult{outcome: telemetry.ReasonNoModel})
		return
	}
	if _, busy := t.probing.LoadOrStore(rep.Name, struct{}{}); busy {
		return
	}
	t.probes.Add(1)
	go func() {
		defer t.probes.Done()
		defer t.probing.Delete(rep.Name)
		t.applyProbeOutcome(rep, t.generationProbe(ctx, rep, model))
	}()
}

// applyProbeOutcome applies a probe result (or the no_model fail-open) to
// rep, if rep is still current, out and owing its probe.
func (t *Tracker) applyProbeOutcome(rep discovery.Replica, res probeResult) {
	outcome := res.outcome
	if outcome == probeCanceled {
		return
	}
	warn := false
	defer func() {
		if warn { // logged outside statesMu
			t.cfg.Logger.Warn("stuck-engine generation probe rejected; re-admitting on /health (fail open)",
				"pool", rep.Pool, "replica", rep.Name, "status", res.status, "path", t.cfg.StuckProbePath)
		}
	}()
	t.statesMu.Lock()
	defer t.statesMu.Unlock()
	if !t.isCurrent(rep) {
		return // removed or re-pointed while the probe ran
	}
	if outcome != telemetry.ReasonNoModel {
		t.countProbe(rep.Pool, outcome)
	}
	rh := t.stateFor(rep.Name, rep.Pool)
	if rh.healthy || !rh.probeOwed {
		return // re-admitted meanwhile (Restore), or no longer owing a probe
	}
	if outcome == ProbeRejected {
		warn = rh.rejectedStatus != res.status // once per replica and status
		rh.rejectedStatus = res.status
	} else if outcome != telemetry.ReasonNoModel {
		rh.rejectedStatus = 0 // a later rejection warns again
	}
	switch outcome {
	case ProbePass:
		if t.readmissible(rh, t.now()) {
			t.readmit(rh)
			return
		}
		t.states.Store(rh.name, rh)
	case ProbeFail, ProbeTimeout:
		t.renewEjection(rh)
	case telemetry.ReasonNoModel, ProbeRejected:
		if !t.readmissible(rh, t.now()) {
			t.states.Store(rh.name, rh)
			return
		}
		reason := telemetry.ReasonNoModel
		if outcome == ProbeRejected {
			reason = telemetry.ReasonProbeRejected
		}
		t.failOpen(reason)
		t.readmit(rh)
	}
}

// generationProbe sends rep one streaming one-token completion for model
// and returns its result; outcome probeCanceled when ctx ended first.
func (t *Tracker) generationProbe(ctx context.Context, rep discovery.Replica, model string) probeResult {
	pctx, cancel := context.WithTimeout(ctx, t.cfg.StuckProbeTimeout)
	defer cancel()
	resp, err := t.cfg.StuckProbeSender.Do(pctx, rep, upstream.Request{
		Method: http.MethodPost,
		Path:   t.cfg.StuckProbePath,
		Header: http.Header{"Content-Type": {"application/json"}, "Accept": {"text/event-stream"}, "User-Agent": {upstream.ProbeUserAgent}},
		Body:   probeBody(t.cfg.StuckProbePath, model),
		Stream: true,
	})
	if err != nil {
		return probeResult{outcome: probeErrOutcome(ctx, pctx, err)}
	}
	defer resp.Body.Close()
	res := probeResult{status: resp.StatusCode}
	switch {
	case resp.StatusCode == http.StatusOK:
	case rejectedStatuses[resp.StatusCode]:
		res.outcome = ProbeRejected
		return res
	default: // 5xx, 3xx, 408/429 (overloaded or slow) and other 4xx: not proven
		res.outcome = ProbeFail
		return res
	}
	generated, err := readGeneration(resp.Body, isEventStream(resp.Header))
	switch {
	case err != nil:
		res.outcome = probeErrOutcome(ctx, pctx, err)
	case generated:
		res.outcome = ProbePass
	default:
		res.outcome = ProbeFail
	}
	return res
}

// probeErrOutcome classifies a probe that failed with err: canceled when
// the poll loop's ctx ended, timeout when the probe's own deadline (or an
// upstream timeout) cut it off, fail otherwise.
func probeErrOutcome(ctx, pctx context.Context, err error) string {
	switch {
	case ctx.Err() != nil:
		return probeCanceled
	case errors.Is(pctx.Err(), context.DeadlineExceeded), upstream.KindOf(err) == upstream.KindTimeout:
		return ProbeTimeout
	default:
		return ProbeFail
	}
}

// probeBody returns the probe's request body for path: the model, the
// fixed prompt, one token, streamed. Nothing else is set, so the request
// is valid for any model a replica serves (ADR 0011); never a priority.
func probeBody(path, model string) []byte {
	var v any
	if path == PathChatCompletions {
		v = struct {
			Model     string              `json:"model"`
			Messages  []map[string]string `json:"messages"`
			MaxTokens int                 `json:"max_tokens"`
			Stream    bool                `json:"stream"`
		}{model, []map[string]string{{"role": "user", "content": ProbePrompt}}, 1, true}
	} else {
		v = struct {
			Model     string `json:"model"`
			Prompt    string `json:"prompt"`
			MaxTokens int    `json:"max_tokens"`
			Stream    bool   `json:"stream"`
		}{model, ProbePrompt, 1, true}
	}
	b, _ := json.Marshal(v) // strings and ints only: never fails
	return b
}

// readGeneration reads the probe's response, at most maxProbeEventBytes,
// until it shows generation (true), or fails it (false): an error event,
// [DONE], or the end of the response (or of the cap) first. A body that is
// not an event stream is read whole and judged as one response. err is a
// read error other than the end (a timeout or cancellation).
func readGeneration(body io.Reader, sse bool) (bool, error) {
	r := bufio.NewReader(io.LimitReader(body, maxProbeEventBytes))
	if !sse {
		// A proxy can strip or rewrite Content-Type on a real stream; judge
		// a body that starts like SSE as SSE, or every probe would fail.
		head, _ := r.Peek(len("data:"))
		sse = bytes.HasPrefix(head, []byte("data:")) || bytes.HasPrefix(head, []byte(":"))
	}
	if !sse {
		b, err := io.ReadAll(r)
		if err != nil {
			return false, err
		}
		return isResponseWithChoices(b), nil
	}
	var data []byte
	seen := false
	for {
		line, err := r.ReadBytes('\n')
		trimmed := bytes.TrimRight(line, "\r\n")
		switch {
		case len(trimmed) == 0 && len(line) > 0: // blank line: end of event
			if seen {
				if done, ok := judgeEvent(data); done {
					return ok, nil
				}
			}
			data, seen = data[:0], false
		case bytes.HasPrefix(trimmed, []byte("data:")):
			if seen {
				data = append(data, '\n')
			}
			data = append(data, bytes.TrimSpace(trimmed[len("data:"):])...)
			seen = true
		}
		if err == io.EOF {
			if seen { // the last event, cut off or capped
				_, ok := judgeEvent(data)
				return ok, nil
			}
			return false, nil
		}
		if err != nil {
			return false, err
		}
	}
}

// probeChoice is the part of a completion or chat choice (streamed or not)
// that shows generation.
type probeChoice struct {
	Text         string        `json:"text"`
	FinishReason *string       `json:"finish_reason"`
	Delta        *probeMessage `json:"delta"`
	Message      *probeMessage `json:"message"`
}

type probeMessage struct {
	Content          string            `json:"content"`
	ReasoningContent string            `json:"reasoning_content"`
	ToolCalls        []json.RawMessage `json:"tool_calls"`
}

type probeDoc struct {
	Error   json.RawMessage `json:"error"`
	Choices []probeChoice   `json:"choices"`
}

// judgeEvent judges one SSE event's data: done reports whether it decides
// the probe, ok whether it passes. An error event or [DONE] fails it; an
// event with a generating choice passes it; anything else (role-only or
// empty chunks, choices: [], usage, non-JSON) is skipped.
func judgeEvent(data []byte) (done, ok bool) {
	if bytes.Equal(data, []byte("[DONE]")) {
		return true, false
	}
	var doc probeDoc
	if json.Unmarshal(data, &doc) != nil {
		return false, false
	}
	if isSet(doc.Error) {
		return true, false
	}
	for _, c := range doc.Choices {
		if c.generated() {
			return true, true
		}
	}
	return false, false
}

// generated reports whether a choice shows generation: text, content,
// reasoning or tool calls, or a finish reason (one end-of-sequence token
// has empty text but finish_reason "stop").
func (c probeChoice) generated() bool {
	if c.Text != "" || c.FinishReason != nil {
		return true
	}
	for _, m := range []*probeMessage{c.Delta, c.Message} {
		if m != nil && (m.Content != "" || m.ReasoningContent != "" || len(m.ToolCalls) > 0) {
			return true
		}
	}
	return false
}

// isResponseWithChoices reports whether a non-streamed body is a
// completion or chat response (no error, at least one choice).
func isResponseWithChoices(b []byte) bool {
	var doc probeDoc
	if json.Unmarshal(b, &doc) != nil {
		return false
	}
	return !isSet(doc.Error) && len(doc.Choices) > 0
}

// isSet reports whether a raw JSON value is present and not null.
func isSet(v json.RawMessage) bool {
	return len(v) > 0 && !bytes.Equal(v, []byte("null"))
}

// isEventStream reports whether h declares an SSE response.
func isEventStream(h http.Header) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(h.Get("Content-Type"))), "text/event-stream")
}

// countProbe counts a probe outcome in gateway_stuck_probe_total.
func (t *Tracker) countProbe(pool, outcome string) {
	if t.metrics == nil {
		return
	}
	if pool == "" {
		pool = telemetry.PoolNone
	}
	t.metrics.StuckProbe(pool, outcome)
}

// failOpen counts a generation probe that could not gate re-admission.
func (t *Tracker) failOpen(reason string) {
	if t.metrics != nil {
		t.metrics.FailOpen(telemetry.StageStuckProbe, reason)
	}
}
