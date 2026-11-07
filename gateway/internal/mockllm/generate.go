package mockllm

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	sseContentType  = "text/event-stream; charset=utf-8"
	sseDone         = "data: [DONE]\n\n"
	chatIDPrefix    = "chatcmpl-"
	cmplIDPrefix    = "cmpl-"
	chatObject      = "chat.completion"
	chatChunkObject = "chat.completion.chunk"
	cmplObject      = "text_completion"
	chunkBufferSize = 512
)

// tokenWords are the synthetic output tokens, cycled. ASCII only, so their
// JSON encoding is fixed and precomputed.
var (
	tokenWords     = []string{" the", " quick", " brown", " fox", " jumps", " over", " a", " lazy", " dog", "."}
	tokenWordsJSON = func() [][]byte {
		out := make([][]byte, len(tokenWords))
		for i, w := range tokenWords {
			out[i], _ = json.Marshal(w)
		}
		return out
	}()
)

// pacer sleeps with one reusable timer per request and honours cancellation.
type pacer struct{ t *time.Timer }

// wait sleeps d (no-op if d <= 0) and returns ctx.Err() if ctx ends first.
func (p *pacer) wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	if p.t == nil {
		p.t = time.NewTimer(d)
	} else {
		p.t.Reset(d)
	}
	select {
	case <-p.t.C:
		return nil
	case <-ctx.Done():
		p.t.Stop()
		return ctx.Err()
	}
}

func (p *pacer) stop() {
	if p.t != nil {
		p.t.Stop()
	}
}

func (m *Mock) tokenInterval() time.Duration {
	if m.cfg.TokensPerSecond <= 0 {
		return 0
	}
	return time.Duration(float64(time.Second) / m.cfg.TokensPerSecond)
}

// serve runs one planned generation: admission, faults, TTFT, then the body.
// A streaming response's headers go out first, before admission and TTFT,
// as vLLM sends them before the engine schedules the request; only the
// 5xx and hang faults (which never start a stream) skip that.
func (m *Mock) serve(ctx context.Context, w http.ResponseWriter, p plan, start time.Time) {
	mm := m.metrics.models[p.model]
	if p.stream && p.fault != fault5xx && p.fault != faultHang {
		startStream(w)
	}
	if !m.acquire(ctx, mm, p.priority) {
		m.metrics.cancelled.Add(1)
		return
	}
	defer m.release(mm)

	if p.fault != faultNone {
		m.metrics.faults[p.fault].Add(1)
	}
	switch p.fault {
	case fault5xx:
		m.writeError(w, m.cfg.ErrorStatus, "mockllm injected fault", "InternalServerError")
		return
	case faultHang:
		<-ctx.Done() // never answer; the caller's timeout or disconnect ends this
		m.metrics.cancelled.Add(1)
		return
	}

	var pc pacer
	defer pc.stop()
	if pc.wait(ctx, p.ttft) != nil {
		m.metrics.cancelled.Add(1)
		return
	}
	mm.ttft.observe(time.Since(start).Seconds())

	var ok bool
	if p.stream {
		ok = m.stream(ctx, w, p, mm, &pc)
	} else {
		ok = m.respond(ctx, w, p, mm, &pc)
	}
	if !ok {
		m.metrics.cancelled.Add(1)
		return
	}
	m.metrics.completed.Add(1)
	mm.success(p.finish)
	mm.e2e.observe(time.Since(start).Seconds())
}

// injectMidResponseFault performs a drop or stall. Drop aborts the handler
// so net/http closes the connection without finishing the body (the client
// sees an unexpected EOF); stall holds the connection open until the client
// leaves. It returns only for stall, with false (cancelled).
func injectMidResponseFault(ctx context.Context, fault string) bool {
	if fault == faultDrop {
		panic(http.ErrAbortHandler) // the documented way to abort a response; net/http recovers it silently
	}
	<-ctx.Done()
	return false
}

func isMidResponseFault(f string) bool { return f == faultDrop || f == faultStall }

// respond writes a non-streaming response after the decode time. It reports
// success only if the client is still connected and the whole body was
// written, so a caller that left is counted as cancelled, not completed.
func (m *Mock) respond(ctx context.Context, w http.ResponseWriter, p plan, mm *modelMetrics, pc *pacer) bool {
	if pc.wait(ctx, m.tokenInterval()*time.Duration(p.tokens-1)) != nil {
		return false
	}
	if isMidResponseFault(p.fault) {
		return injectMidResponseFault(ctx, p.fault)
	}
	if ctx.Err() != nil {
		return false
	}
	mm.genTokens.Add(uint64(p.tokens * p.choices))
	return sendJSON(w, http.StatusOK, m.buildResponse(p)) == nil
}

// buildResponse builds the non-streaming body for p.
func (m *Mock) buildResponse(p plan) any {
	text := outputText(p.tokens)
	u := usageFor(p)
	if p.endpoint == endpointChat {
		resp := chatResponse{ID: m.newID(chatIDPrefix), Object: chatObject, Created: time.Now().Unix(), Model: p.model, Usage: u}
		for i := range p.choices {
			resp.Choices = append(resp.Choices, chatChoice{
				Index:        i,
				Message:      chatMessage{Role: "assistant", Content: text, ToolCalls: []json.RawMessage{}},
				FinishReason: p.finish,
			})
		}
		return resp
	}
	resp := completionResponse{ID: m.newID(cmplIDPrefix), Object: cmplObject, Created: time.Now().Unix(), Model: p.model, Usage: u}
	for i := range p.choices {
		resp.Choices = append(resp.Choices, completionChoice{Index: i, Text: text, FinishReason: p.finish})
	}
	return resp
}

// startStream sends a streaming response's status and headers at once.
func startStream(w http.ResponseWriter) {
	w.Header().Set("Content-Type", sseContentType)
	w.WriteHeader(http.StatusOK)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

// stream writes SSE events, one per token per choice, flushing each step.
// serve has already sent the headers (startStream).
func (m *Mock) stream(ctx context.Context, w http.ResponseWriter, p plan, mm *modelMetrics, pc *pacer) bool {
	enc := newChunkEncoder(p, m.newID(idPrefix(p.endpoint)), time.Now().Unix())
	flusher, _ := w.(http.Flusher)
	buf := make([]byte, 0, chunkBufferSize)
	emit := func() bool {
		_, err := w.Write(buf)
		buf = buf[:0]
		if err != nil {
			return false
		}
		if flusher != nil {
			flusher.Flush()
		}
		return true
	}

	if p.endpoint == endpointChat {
		for c := range p.choices {
			buf = enc.role(buf, c)
		}
		if !emit() {
			return false
		}
	}
	cut := p.tokens / 2 // token index where drop/stall strikes
	interval := m.tokenInterval()
	firstAt := time.Now()
	for i := range p.tokens {
		if ctx.Err() != nil {
			return false
		}
		if isMidResponseFault(p.fault) && i == cut {
			return injectMidResponseFault(ctx, p.fault)
		}
		if i > 0 && pc.wait(ctx, time.Until(firstAt.Add(interval*time.Duration(i)))) != nil {
			return false
		}
		finish := ""
		if i == p.tokens-1 {
			finish = p.finish
		}
		for c := range p.choices {
			buf = enc.token(buf, c, i, finish)
		}
		if !emit() {
			return false
		}
		mm.genTokens.Add(uint64(p.choices))
	}
	if p.includeUsage {
		buf = enc.usage(buf, usageFor(p))
	}
	buf = append(buf, sseDone...)
	return emit()
}

func idPrefix(endpoint string) string {
	if endpoint == endpointChat {
		return chatIDPrefix
	}
	return cmplIDPrefix
}

func outputText(tokens int) string {
	var b strings.Builder
	for i := range tokens {
		b.WriteString(tokenWords[i%len(tokenWords)])
	}
	return b.String()
}

func usageFor(p plan) usage {
	completion := p.tokens * p.choices
	return usage{PromptTokens: p.promptTokens, CompletionTokens: completion, TotalTokens: p.promptTokens + completion}
}

// chunkEncoder appends SSE events without per-chunk JSON encoding.
type chunkEncoder struct {
	chat bool
	head []byte // `data: {"id":...,"choices":[`
}

func newChunkEncoder(p plan, id string, created int64) chunkEncoder {
	object := cmplObject
	if p.endpoint == endpointChat {
		object = chatChunkObject
	}
	model, _ := json.Marshal(p.model) // a string always marshals
	head := make([]byte, 0, 128)
	head = append(head, `data: {"id":"`...)
	head = append(head, id...)
	head = append(head, `","object":"`...)
	head = append(head, object...)
	head = append(head, `","created":`...)
	head = strconv.AppendInt(head, created, 10)
	head = append(head, `,"model":`...)
	head = append(head, model...)
	head = append(head, `,"choices":[`...)
	return chunkEncoder{chat: p.endpoint == endpointChat, head: head}
}

func (e chunkEncoder) role(buf []byte, choice int) []byte {
	buf = append(buf, e.head...)
	buf = append(buf, `{"index":`...)
	buf = strconv.AppendInt(buf, int64(choice), 10)
	return append(buf, `,"delta":{"role":"assistant","content":""},"logprobs":null,"finish_reason":null}]}`+"\n\n"...)
}

func (e chunkEncoder) token(buf []byte, choice, i int, finish string) []byte {
	buf = append(buf, e.head...)
	buf = append(buf, `{"index":`...)
	buf = strconv.AppendInt(buf, int64(choice), 10)
	word := tokenWordsJSON[i%len(tokenWordsJSON)]
	if e.chat {
		buf = append(buf, `,"delta":{"content":`...)
		buf = append(buf, word...)
		buf = append(buf, '}')
	} else {
		buf = append(buf, `,"text":`...)
		buf = append(buf, word...)
	}
	buf = append(buf, `,"logprobs":null,"finish_reason":`...)
	if finish == "" {
		buf = append(buf, "null"...)
	} else {
		buf = append(buf, '"')
		buf = append(buf, finish...)
		buf = append(buf, `","stop_reason":null`...)
	}
	return append(buf, "}]}\n\n"...)
}

func (e chunkEncoder) usage(buf []byte, u usage) []byte {
	buf = append(buf, e.head...)
	buf = append(buf, `],"usage":{"prompt_tokens":`...)
	buf = strconv.AppendInt(buf, int64(u.PromptTokens), 10)
	buf = append(buf, `,"total_tokens":`...)
	buf = strconv.AppendInt(buf, int64(u.TotalTokens), 10)
	buf = append(buf, `,"completion_tokens":`...)
	buf = strconv.AppendInt(buf, int64(u.CompletionTokens), 10)
	return append(buf, "}}\n\n"...)
}

type usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	TotalTokens      int `json:"total_tokens"`
	CompletionTokens int `json:"completion_tokens"`
}

type chatResponse struct {
	ID      string       `json:"id"`
	Object  string       `json:"object"`
	Created int64        `json:"created"`
	Model   string       `json:"model"`
	Choices []chatChoice `json:"choices"`
	Usage   usage        `json:"usage"`
}

type chatChoice struct {
	Index        int         `json:"index"`
	Message      chatMessage `json:"message"`
	Logprobs     *struct{}   `json:"logprobs"`
	FinishReason string      `json:"finish_reason"`
	StopReason   *string     `json:"stop_reason"`
}

type chatMessage struct {
	Role      string            `json:"role"`
	Content   string            `json:"content"`
	ToolCalls []json.RawMessage `json:"tool_calls"`
}

type completionResponse struct {
	ID      string             `json:"id"`
	Object  string             `json:"object"`
	Created int64              `json:"created"`
	Model   string             `json:"model"`
	Choices []completionChoice `json:"choices"`
	Usage   usage              `json:"usage"`
}

type completionChoice struct {
	Index        int       `json:"index"`
	Text         string    `json:"text"`
	Logprobs     *struct{} `json:"logprobs"`
	FinishReason string    `json:"finish_reason"`
	StopReason   *string   `json:"stop_reason"`
}
