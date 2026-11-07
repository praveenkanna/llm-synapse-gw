package mockllm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"
	"time"
)

const (
	testModel   = "test-model"
	testTimeout = 5 * time.Second
)

// startMock runs a Server on a free port with fast, deterministic defaults.
func startMock(t *testing.T, mutate func(*Config)) *Server {
	t.Helper()
	cfg := Defaults()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.Models = []string{testModel, "other-model"}
	cfg.TTFT = 0
	cfg.TokensPerSecond = 0
	cfg.OutputTokens = LengthDist{Kind: DistFixed, A: 4}
	cfg.Seed = 1
	if mutate != nil {
		mutate(&cfg)
	}
	srv, err := Start(cfg, nil)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return srv
}

func newClient() *http.Client {
	return &http.Client{Timeout: testTimeout, Transport: &http.Transport{DisableKeepAlives: true}}
}

func post(t *testing.T, ctx context.Context, url, body string, hdr map[string]string) (*http.Response, error) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	return newClient().Do(req)
}

func mustPost(t *testing.T, url, body string, hdr map[string]string) (*http.Response, []byte) {
	t.Helper()
	resp, err := post(t, context.Background(), url, body, hdr)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp, b
}

type sseEvent struct {
	data string
	at   time.Time
}

// readEvents reads SSE events until EOF or error, timestamping each.
func readEvents(r io.Reader) ([]sseEvent, error) {
	br := bufio.NewReader(r)
	var events []sseEvent
	var cur strings.Builder
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return events, err
		}
		if line == "\n" {
			events = append(events, sseEvent{data: cur.String(), at: time.Now()})
			cur.Reset()
			continue
		}
		cur.WriteString(strings.TrimPrefix(strings.TrimSuffix(line, "\n"), "data: "))
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// settle waits up to 2 s for the mock's counters to satisfy ok. A request is
// counted as finished only after its response is written, so a client can
// see the response before the counters move. The caller still asserts, so a
// timeout here reports through the caller's own message.
func settle(srv *Server, ok func(Stats) bool) {
	for deadline := time.Now().Add(2 * time.Second); !ok(srv.Mock().Stats()) && time.Now().Before(deadline); {
		time.Sleep(5 * time.Millisecond)
	}
}

func TestNonStreamingResponses(t *testing.T) {
	srv := startMock(t, nil)
	tests := []struct {
		name, path, body, object, finish string
		tokens                           int
	}{
		{name: "chat stop", path: "/v1/chat/completions", body: `{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`, object: "chat.completion", finish: "stop", tokens: 4},
		{name: "chat max_tokens", path: "/v1/chat/completions", body: `{"model":"test-model","messages":[],"max_tokens":2}`, object: "chat.completion", finish: "length", tokens: 2},
		{name: "chat max_completion_tokens", path: "/v1/chat/completions", body: `{"model":"test-model","messages":[],"max_completion_tokens":3,"max_tokens":9}`, object: "chat.completion", finish: "length", tokens: 3},
		{name: "chat default model", path: "/v1/chat/completions", body: `{"messages":[]}`, object: "chat.completion", finish: "stop", tokens: 4},
		{name: "completion", path: "/v1/completions", body: `{"model":"other-model","prompt":"once upon"}`, object: "text_completion", finish: "stop", tokens: 4},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, b := mustPost(t, srv.URL()+tc.path, tc.body, nil)
			if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "application/json" {
				t.Fatalf("status %d ct %q body %s", resp.StatusCode, resp.Header.Get("Content-Type"), b)
			}
			var got struct {
				ID      string `json:"id"`
				Object  string `json:"object"`
				Model   string `json:"model"`
				Choices []struct {
					Index        int     `json:"index"`
					Text         *string `json:"text"`
					FinishReason string  `json:"finish_reason"`
					Message      *struct {
						Role    string `json:"role"`
						Content string `json:"content"`
					} `json:"message"`
				} `json:"choices"`
				Usage usage `json:"usage"`
			}
			if err := json.Unmarshal(b, &got); err != nil {
				t.Fatalf("decode %s: %v", b, err)
			}
			if got.Object != tc.object || got.ID == "" || len(got.Choices) != 1 || got.Choices[0].FinishReason != tc.finish {
				t.Fatalf("response = %s", b)
			}
			text := ""
			if got.Choices[0].Message != nil {
				text = got.Choices[0].Message.Content
				if got.Choices[0].Message.Role != "assistant" {
					t.Fatalf("role = %q", got.Choices[0].Message.Role)
				}
			} else if got.Choices[0].Text != nil {
				text = *got.Choices[0].Text
			}
			if text != outputText(tc.tokens) {
				t.Fatalf("text = %q, want %q", text, outputText(tc.tokens))
			}
			u := got.Usage
			if u.CompletionTokens != tc.tokens || u.PromptTokens < 1 || u.TotalTokens != u.PromptTokens+u.CompletionTokens {
				t.Fatalf("usage = %+v", u)
			}
		})
	}
	settle(srv, func(s Stats) bool { return s.Completed == uint64(len(tests)) && s.Running == 0 })
	if s := srv.Mock().Stats(); s.Completed != uint64(len(tests)) || s.Running != 0 || s.Cancelled != 0 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestNChoicesNonStreaming(t *testing.T) {
	srv := startMock(t, nil)
	_, b := mustPost(t, srv.URL()+"/v1/chat/completions", `{"model":"test-model","messages":[],"n":3}`, nil)
	var got chatResponse
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Choices) != 3 || got.Choices[2].Index != 2 || got.Usage.CompletionTokens != 12 {
		t.Fatalf("n=3 response = %s", b)
	}
}

func TestStreamingChat(t *testing.T) {
	const (
		ttft     = 80 * time.Millisecond
		tps      = 20.0 // 50 ms between tokens
		tokens   = 5
		interval = 50 * time.Millisecond
		slack    = 20 * time.Millisecond
	)
	srv := startMock(t, func(c *Config) {
		c.TTFT = ttft
		c.TokensPerSecond = tps
		c.OutputTokens = LengthDist{Kind: DistFixed, A: tokens}
	})
	start := time.Now()
	resp, err := post(t, context.Background(), srv.URL()+"/v1/chat/completions",
		`{"model":"test-model","messages":[],"stream":true,"stream_options":{"include_usage":true}}`, nil)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	headersAt := time.Since(start)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != sseContentType {
		t.Fatalf("status %d ct %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	// Like vLLM, a stream's headers go out at once; the first token waits TTFT.
	if headersAt >= ttft {
		t.Fatalf("headers after %s, want before TTFT %s (vLLM sends stream headers before scheduling)", headersAt, ttft)
	}
	events, err := readEvents(resp.Body)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("read events: %v", err)
	}
	if first := events[0].at.Sub(start); first < ttft {
		t.Fatalf("first event after %s, want >= TTFT %s", first, ttft)
	}
	// role + 5 tokens + usage + [DONE]
	if len(events) != 1+tokens+2 {
		t.Fatalf("got %d events: %+v", len(events), events)
	}
	if !strings.Contains(events[0].data, `"delta":{"role":"assistant","content":""}`) {
		t.Fatalf("first event is not the role chunk: %s", events[0].data)
	}
	var text strings.Builder
	for i, ev := range events[1 : 1+tokens] {
		var chunk struct {
			Object  string `json:"object"`
			Model   string `json:"model"`
			Choices []struct {
				Delta        struct{ Content string } `json:"delta"`
				FinishReason *string                  `json:"finish_reason"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(ev.data), &chunk); err != nil {
			t.Fatalf("token event %d not JSON: %v: %s", i, err, ev.data)
		}
		if chunk.Object != chatChunkObject || chunk.Model != testModel || len(chunk.Choices) != 1 {
			t.Fatalf("token event %d = %s", i, ev.data)
		}
		last := i == tokens-1
		if fr := chunk.Choices[0].FinishReason; (fr != nil) != last || (last && *fr != "stop") {
			t.Fatalf("token event %d finish_reason = %v", i, fr)
		}
		text.WriteString(chunk.Choices[0].Delta.Content)
	}
	if text.String() != outputText(tokens) {
		t.Fatalf("streamed text %q, want %q", text.String(), outputText(tokens))
	}
	// Incremental delivery: the token events are spread over (tokens-1) intervals.
	spread := events[tokens].at.Sub(events[1].at)
	if spread < time.Duration(tokens-1)*interval-slack {
		t.Fatalf("tokens arrived within %s, want about %s (not one write)", spread, time.Duration(tokens-1)*interval)
	}
	var u struct {
		Choices []any `json:"choices"`
		Usage   usage `json:"usage"`
	}
	if err := json.Unmarshal([]byte(events[tokens+1].data), &u); err != nil || len(u.Choices) != 0 || u.Usage.CompletionTokens != tokens {
		t.Fatalf("usage event = %s (%v)", events[tokens+1].data, err)
	}
	if events[len(events)-1].data != "[DONE]" {
		t.Fatalf("last event = %q, want [DONE]", events[len(events)-1].data)
	}
	waitFor(t, "completion counted", func() bool { return srv.Mock().Stats().Completed == 1 })
}

func TestStreamingCompletionsAndN(t *testing.T) {
	srv := startMock(t, nil)
	resp, err := post(t, context.Background(), srv.URL()+"/v1/completions", `{"model":"test-model","prompt":"x","stream":true,"n":2,"max_tokens":3}`, nil)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	events, err := readEvents(resp.Body)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("read: %v", err)
	}
	// 3 steps x 2 choices, no role chunk, no usage (not requested), [DONE].
	if len(events) != 3*2+1 || events[len(events)-1].data != "[DONE]" {
		t.Fatalf("events = %+v", events)
	}
	seen := map[int]int{}
	for _, ev := range events[:6] {
		var c struct {
			Object  string `json:"object"`
			Choices []struct {
				Index        int     `json:"index"`
				Text         string  `json:"text"`
				FinishReason *string `json:"finish_reason"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(ev.data), &c); err != nil || c.Object != cmplObject {
			t.Fatalf("event %s (%v)", ev.data, err)
		}
		seen[c.Choices[0].Index]++
		if seen[c.Choices[0].Index] == 3 && (c.Choices[0].FinishReason == nil || *c.Choices[0].FinishReason != "length") {
			t.Fatalf("final event for choice %d lacks finish_reason length: %s", c.Choices[0].Index, ev.data)
		}
	}
	if seen[0] != 3 || seen[1] != 3 {
		t.Fatalf("per-choice counts = %v", seen)
	}
}

func TestRejections(t *testing.T) {
	srv := startMock(t, func(c *Config) {
		c.MaxBodyBytes = 256
		c.MaxModelLen = 40
	})
	tests := []struct {
		name   string
		body   string
		hdr    map[string]string
		status int
		errMsg string
	}{
		{name: "bad json", body: `{"model":`, status: 400, errMsg: "invalid JSON"},
		{name: "not an object", body: `[]`, status: 400, errMsg: "invalid JSON"},
		{name: "unknown model", body: `{"model":"nope"}`, status: 404, errMsg: "does not exist"},
		{name: "n too big", body: `{"model":"test-model","n":99}`, status: 400, errMsg: "n must be"},
		{name: "n zero", body: `{"model":"test-model","n":0}`, status: 400, errMsg: "n must be"},
		{name: "max_tokens zero", body: `{"model":"test-model","max_tokens":0}`, status: 400, errMsg: "max_tokens"},
		{name: "context overflow", body: `{"model":"test-model","prompt":"` + strings.Repeat("a", 200) + `"}`, status: 400, errMsg: "maximum context length"},
		{name: "body too large", body: `{"model":"test-model","prompt":"` + strings.Repeat("a", 400) + `"}`, status: 413, errMsg: "too large"},
		{name: "bad fault header", body: `{"model":"test-model"}`, hdr: map[string]string{HeaderFault: "explode"}, status: 400, errMsg: HeaderFault},
		{name: "bad tokens header", body: `{"model":"test-model"}`, hdr: map[string]string{HeaderOutputTokens: "0"}, status: 400, errMsg: HeaderOutputTokens},
		{name: "bad ttft header", body: `{"model":"test-model"}`, hdr: map[string]string{HeaderTTFTMs: "-5"}, status: 400, errMsg: HeaderTTFTMs},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, b := mustPost(t, srv.URL()+"/v1/completions", tc.body, tc.hdr)
			var eb errorBody
			if err := json.Unmarshal(b, &eb); err != nil {
				t.Fatalf("error body not JSON: %s", b)
			}
			if resp.StatusCode != tc.status || eb.Error.Code != tc.status || !strings.Contains(eb.Error.Message, tc.errMsg) {
				t.Fatalf("got %d %s, want %d containing %q", resp.StatusCode, b, tc.status, tc.errMsg)
			}
		})
	}
	settle(srv, func(s Stats) bool { return s.Rejected == uint64(len(tests)) })
	if s := srv.Mock().Stats(); s.Rejected != uint64(len(tests)) || s.Completed != 0 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestHeaderOverrides(t *testing.T) {
	srv := startMock(t, func(c *Config) { c.TTFT = time.Hour })
	start := time.Now()
	_, b := mustPost(t, srv.URL()+"/v1/chat/completions", `{"model":"test-model"}`,
		map[string]string{HeaderTTFTMs: "0", HeaderOutputTokens: "7", HeaderFault: "none"})
	if time.Since(start) > time.Second {
		t.Fatal("X-Mock-TTFT-Ms did not override the configured TTFT")
	}
	var got chatResponse
	if err := json.Unmarshal(b, &got); err != nil || got.Usage.CompletionTokens != 7 {
		t.Fatalf("response %s (%v)", b, err)
	}
}

func TestFault5xx(t *testing.T) {
	srv := startMock(t, func(c *Config) {
		c.Faults.Error5xx = 1
		c.ErrorStatus = http.StatusServiceUnavailable
	})
	for _, stream := range []bool{false, true} {
		resp, b := mustPost(t, srv.URL()+"/v1/chat/completions", fmt.Sprintf(`{"model":"test-model","stream":%v}`, stream), nil)
		var eb errorBody
		if resp.StatusCode != http.StatusServiceUnavailable || json.Unmarshal(b, &eb) != nil || eb.Error.Code != 503 {
			t.Fatalf("stream=%v: %d %s", stream, resp.StatusCode, b)
		}
	}
	settle(srv, func(s Stats) bool { return s.Faults[fault5xx] == 2 && s.Running == 0 })
	if s := srv.Mock().Stats(); s.Faults[fault5xx] != 2 || s.Completed != 0 || s.Running != 0 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestFaultDrop(t *testing.T) {
	srv := startMock(t, func(c *Config) { c.OutputTokens = LengthDist{Kind: DistFixed, A: 6} })
	hdr := map[string]string{HeaderFault: faultDrop}

	resp, err := post(t, context.Background(), srv.URL()+"/v1/chat/completions", `{"model":"test-model","stream":true}`, hdr)
	if err != nil {
		t.Fatalf("POST stream: %v", err)
	}
	events, err := readEvents(resp.Body)
	_ = resp.Body.Close()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("stream read err = %v, want unexpected EOF (connection cut mid-body)", err)
	}
	if len(events) != 1+3 { // role + half of 6 tokens
		t.Fatalf("got %d events before the drop, want 4: %+v", len(events), events)
	}
	for _, ev := range events {
		if ev.data == "[DONE]" {
			t.Fatal("dropped stream must not end with [DONE]")
		}
	}

	if _, err := post(t, context.Background(), srv.URL()+"/v1/completions", `{"model":"test-model"}`, hdr); err == nil {
		t.Fatal("non-streaming drop: want transport error, got response")
	}
	waitFor(t, "running back to zero", func() bool { return srv.Mock().Stats().Running == 0 })
	if s := srv.Mock().Stats(); s.Faults[faultDrop] != 2 || s.Completed != 0 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestFaultHangAndCancel(t *testing.T) {
	srv := startMock(t, nil)
	for _, stream := range []bool{false, true} {
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		_, err := post(t, ctx, srv.URL()+"/v1/chat/completions", fmt.Sprintf(`{"model":"test-model","stream":%v}`, stream), map[string]string{HeaderFault: faultHang})
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("stream=%v: hang err = %v, want deadline exceeded", stream, err)
		}
	}
	waitFor(t, "both hung requests counted as cancelled", func() bool {
		s := srv.Mock().Stats()
		return s.Cancelled == 2 && s.Running == 0
	})
	if s := srv.Mock().Stats(); s.Faults[faultHang] != 2 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestFaultStall(t *testing.T) {
	srv := startMock(t, func(c *Config) { c.OutputTokens = LengthDist{Kind: DistFixed, A: 4} })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resp, err := post(t, ctx, srv.URL()+"/v1/completions", `{"model":"test-model","stream":true}`, map[string]string{HeaderFault: faultStall})
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	for range 2 { // half of 4 tokens arrive
		if _, err := br.ReadString('\n'); err != nil {
			t.Fatalf("read token: %v", err)
		}
		if _, err := br.ReadString('\n'); err != nil {
			t.Fatalf("read separator: %v", err)
		}
	}
	gotMore := make(chan struct{})
	go func() {
		if _, err := br.ReadByte(); err == nil {
			close(gotMore)
		}
	}()
	select {
	case <-gotMore:
		t.Fatal("stalled stream sent more bytes")
	case <-time.After(150 * time.Millisecond):
	}
	if s := srv.Mock().Stats(); s.Running != 1 {
		t.Fatalf("stalled request should still be running: %+v", s)
	}
	cancel()
	waitFor(t, "stall cancelled", func() bool {
		s := srv.Mock().Stats()
		return s.Cancelled == 1 && s.Running == 0
	})
}

func TestClientDisconnectMidStreamIsCounted(t *testing.T) {
	srv := startMock(t, func(c *Config) {
		c.TokensPerSecond = 100
		c.OutputTokens = LengthDist{Kind: DistFixed, A: 1000}
	})
	ctx, cancel := context.WithCancel(context.Background())
	resp, err := post(t, ctx, srv.URL()+"/v1/chat/completions", `{"model":"test-model","stream":true}`, nil)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	br := bufio.NewReader(resp.Body)
	if _, err := br.ReadString('\n'); err != nil {
		t.Fatalf("read first event: %v", err)
	}
	abortedAt := time.Now()
	cancel()
	_ = resp.Body.Close()
	waitFor(t, "cancel counted", func() bool { return srv.Mock().Stats().Cancelled == 1 })
	if d := time.Since(abortedAt); d > time.Second {
		t.Fatalf("cancel seen after %s, want < 1s", d)
	}
	settle(srv, func(s Stats) bool { return s.Running == 0 })
	if s := srv.Mock().Stats(); s.Running != 0 || s.Completed != 0 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestFaultRatesDeterministic(t *testing.T) {
	run := func() []int {
		srv := startMock(t, func(c *Config) {
			c.Faults = FaultRates{Error5xx: 0.5}
			c.Seed = 7
		})
		var statuses []int
		for range 20 {
			resp, _ := mustPost(t, srv.URL()+"/v1/completions", `{"model":"test-model"}`, nil)
			statuses = append(statuses, resp.StatusCode)
		}
		return statuses
	}
	a, b := run(), run()
	if fmt.Sprint(a) != fmt.Sprint(b) {
		t.Fatalf("same seed gave different fault sequences:\n%v\n%v", a, b)
	}
	var fails int
	for _, s := range a {
		if s == 500 {
			fails++
		}
	}
	if fails == 0 || fails == len(a) {
		t.Fatalf("rate 0.5 gave %d/%d failures", fails, len(a))
	}
}

func TestMaxNumSeqsQueues(t *testing.T) {
	srv := startMock(t, func(c *Config) { c.MaxNumSeqs = 1 })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := post(t, ctx, srv.URL()+"/v1/completions", `{"model":"test-model"}`, map[string]string{HeaderFault: faultHang})
			errs <- err
		}()
	}
	waitFor(t, "one running, one waiting", func() bool {
		s := srv.Mock().Stats()
		return s.Running == 1 && s.Waiting == 1
	})
	body := getBody(t, srv.URL()+"/metrics")
	for _, want := range []string{
		`vllm:num_requests_running{model_name="test-model"} 1`,
		`vllm:num_requests_waiting{model_name="test-model"} 1`,
		`vllm:kv_cache_usage_perc{model_name="test-model"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics missing %q", want)
		}
	}
	cancel()
	for range 2 {
		<-errs
	}
	waitFor(t, "queue drained", func() bool {
		s := srv.Mock().Stats()
		return s.Running == 0 && s.Waiting == 0 && s.Cancelled == 2
	})
}

// TestStreamHeadersBeforeAdmission: vLLM answers a streaming request with
// headers before the engine schedules it, so a queued stream already has its
// headers; its first token waits for a running slot.
func TestStreamHeadersBeforeAdmission(t *testing.T) {
	srv := startMock(t, func(c *Config) { c.MaxNumSeqs = 1 })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	held := make(chan error, 1)
	go func() { // occupies the only slot until cancelled
		_, err := post(t, ctx, srv.URL()+"/v1/completions", `{"model":"test-model"}`, map[string]string{HeaderFault: faultHang})
		held <- err
	}()
	waitFor(t, "slot taken", func() bool { return srv.Mock().Stats().Running == 1 })

	resp, err := post(t, ctx, srv.URL()+"/v1/completions", `{"model":"test-model","prompt":"x","stream":true}`, nil)
	if err != nil {
		t.Fatalf("POST stream while queued: %v (headers must not wait for admission)", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != sseContentType {
		t.Fatalf("status %d ct %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	waitFor(t, "stream waiting", func() bool { return srv.Mock().Stats().Waiting == 1 })
	cancel()
	<-held
}

// TestPriorityField checks the "priority" body field under each scheduling
// policy: fcfs rejects non-zero values the way vLLM does, priority accepts
// any integer, and a non-integer is a 400 under both.
func TestPriorityField(t *testing.T) {
	tests := []struct {
		name         string
		policy       SchedulingPolicy
		body         string
		status       int
		errMsg       string
		withPriority uint64 // expected PriorityRequests
	}{
		{name: "fcfs absent", policy: PolicyFCFS, body: `{"model":"test-model"}`, status: 200},
		{name: "fcfs null", policy: PolicyFCFS, body: `{"model":"test-model","priority":null}`, status: 400, errMsg: "priority must be an integer"},
		{name: "fcfs zero float", policy: PolicyFCFS, body: `{"model":"test-model","priority":0.0}`, status: 200},
		{name: "fcfs underflow", policy: PolicyFCFS, body: `{"model":"test-model","priority":1e-400}`, status: 200},
		{name: "fcfs false", policy: PolicyFCFS, body: `{"model":"test-model","priority":false}`, status: 200},
		{name: "fcfs integral float", policy: PolicyFCFS, body: `{"model":"test-model","priority":10.0}`, status: 400, errMsg: "Priority scheduling is not enabled", withPriority: 1},
		{name: "fcfs true", policy: PolicyFCFS, body: `{"model":"test-model","priority":true}`, status: 400, errMsg: "Priority scheduling is not enabled", withPriority: 1},
		{name: "fcfs numeric string", policy: PolicyFCFS, body: `{"model":"test-model","priority":"1"}`, status: 400, errMsg: "Priority scheduling is not enabled", withPriority: 1},
		{name: "fcfs zero", policy: PolicyFCFS, body: `{"model":"test-model","priority":0}`, status: 200},
		{name: "fcfs non-zero", policy: PolicyFCFS, body: `{"model":"test-model","priority":5}`, status: 400, errMsg: "Got priority 5 but Priority scheduling is not enabled.", withPriority: 1},
		{name: "fcfs negative", policy: PolicyFCFS, body: `{"model":"test-model","priority":-1}`, status: 400, errMsg: "Got priority -1 but", withPriority: 1},
		{name: "fcfs fraction", policy: PolicyFCFS, body: `{"model":"test-model","priority":1.5}`, status: 400, errMsg: "priority must be an integer"},
		{name: "priority non-zero", policy: PolicyPriority, body: `{"model":"test-model","priority":5}`, status: 200, withPriority: 1},
		{name: "priority negative", policy: PolicyPriority, body: `{"model":"test-model","priority":-3}`, status: 200, withPriority: 1},
		{name: "priority zero", policy: PolicyPriority, body: `{"model":"test-model","priority":0}`, status: 200},
		{name: "priority string", policy: PolicyPriority, body: `{"model":"test-model","priority":"high"}`, status: 400, errMsg: "priority must be an integer"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := startMock(t, func(c *Config) { c.SchedulingPolicy = tc.policy })
			for _, path := range []string{"/v1/chat/completions", "/v1/completions"} {
				resp, b := mustPost(t, srv.URL()+path, tc.body, nil)
				if resp.StatusCode != tc.status {
					t.Fatalf("%s: status %d, want %d (%s)", path, resp.StatusCode, tc.status, b)
				}
				if tc.status != 200 {
					assertErrorShape(t, CompatCurrent, b, tc.status, tc.errMsg)
				}
			}
			if got := srv.Mock().Stats().PriorityRequests; got != 2*tc.withPriority {
				t.Fatalf("PriorityRequests = %d, want %d", got, 2*tc.withPriority)
			}
		})
	}
}

// TestSchedulingOrder: with one slot taken, queued requests are admitted
// lowest priority value first and in arrival order among equals; fcfs keeps
// arrival order. Each admitted request holds the slot for its TTFT, so the
// order responses finish in is the admission order.
func TestSchedulingOrder(t *testing.T) {
	const hold = 60 // ms each admitted request keeps the slot
	tests := []struct {
		name       string
		policy     SchedulingPolicy
		priorities []int // queued in this arrival order
		want       []int // request indices in completion order
	}{
		{name: "fcfs arrival order", policy: PolicyFCFS, priorities: []int{0, 0, 0}, want: []int{0, 1, 2}},
		{name: "lower value first", policy: PolicyPriority, priorities: []int{10, 1}, want: []int{1, 0}},
		{name: "fifo among equal priorities", policy: PolicyPriority, priorities: []int{5, 1, 1, 0}, want: []int{3, 1, 2, 0}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := startMock(t, func(c *Config) {
				c.MaxNumSeqs = 1
				c.SchedulingPolicy = tc.policy
			})
			holdCtx, release := context.WithCancel(context.Background())
			defer release()
			held := make(chan error, 1)
			go func() {
				_, err := post(t, holdCtx, srv.URL()+"/v1/completions", `{"model":"test-model"}`, map[string]string{HeaderFault: faultHang})
				held <- err
			}()
			waitFor(t, "slot taken", func() bool { return srv.Mock().Stats().Running == 1 })

			done := make(chan int, len(tc.priorities))
			for i, prio := range tc.priorities {
				go func() {
					body := fmt.Sprintf(`{"model":"test-model","priority":%d}`, prio)
					resp, err := post(t, context.Background(), srv.URL()+"/v1/completions", body, map[string]string{HeaderTTFTMs: fmt.Sprint(hold)})
					if err == nil {
						_, _ = io.Copy(io.Discard, resp.Body)
						_ = resp.Body.Close()
						if resp.StatusCode != http.StatusOK {
							t.Errorf("request %d: status %d", i, resp.StatusCode)
						}
					} else {
						t.Errorf("request %d: %v", i, err)
					}
					done <- i
				}()
				waitFor(t, fmt.Sprintf("request %d queued", i), func() bool { return srv.Mock().Stats().Waiting == int64(i+1) })
			}
			release()
			<-held

			var got []int
			for range tc.priorities {
				select {
				case i := <-done:
					got = append(got, i)
				case <-time.After(testTimeout):
					t.Fatalf("timed out; finished so far %v", got)
				}
			}
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Fatalf("completion order %v, want %v (priorities %v)", got, tc.want, tc.priorities)
			}
			settle(srv, func(s Stats) bool { return s.Running == 0 && s.Waiting == 0 })
			if s := srv.Mock().Stats(); s.Running != 0 || s.Waiting != 0 {
				t.Fatalf("stats after drain = %+v", s)
			}
		})
	}
}

// TestCancelledWaiterFreesQueue: a queued request whose client leaves is
// dropped from the queue; the slot then goes to the next waiter and is free
// once that one finishes.
func TestCancelledWaiterFreesQueue(t *testing.T) {
	srv := startMock(t, func(c *Config) {
		c.MaxNumSeqs = 1
		c.SchedulingPolicy = PolicyPriority
	})
	holdCtx, release := context.WithCancel(context.Background())
	defer release()
	held := make(chan error, 1)
	go func() {
		_, err := post(t, holdCtx, srv.URL()+"/v1/completions", `{"model":"test-model"}`, map[string]string{HeaderFault: faultHang})
		held <- err
	}()
	waitFor(t, "slot taken", func() bool { return srv.Mock().Stats().Running == 1 })

	leaveCtx, leave := context.WithCancel(context.Background())
	left := make(chan error, 1)
	go func() { // best priority, so it would be admitted first if it stayed
		_, err := post(t, leaveCtx, srv.URL()+"/v1/completions", `{"model":"test-model","priority":-10}`, nil)
		left <- err
	}()
	waitFor(t, "first waiter queued", func() bool { return srv.Mock().Stats().Waiting == 1 })
	stays := make(chan int, 1)
	go func() {
		resp, err := post(t, context.Background(), srv.URL()+"/v1/completions", `{"model":"test-model","priority":3}`, nil)
		if err != nil {
			t.Errorf("staying waiter: %v", err)
			stays <- 0
			return
		}
		_ = resp.Body.Close()
		stays <- resp.StatusCode
	}()
	waitFor(t, "second waiter queued", func() bool { return srv.Mock().Stats().Waiting == 2 })

	leave()
	<-left
	waitFor(t, "cancelled waiter dropped", func() bool { return srv.Mock().Stats().Waiting == 1 })
	release()
	<-held
	if status := <-stays; status != http.StatusOK {
		t.Fatalf("staying waiter status %d, want 200", status)
	}
	waitFor(t, "slots free", func() bool {
		s := srv.Mock().Stats()
		inUse, queued := srv.Mock().slots.state()
		return s.Running == 0 && s.Waiting == 0 && s.Cancelled == 2 && s.Completed == 1 && inUse == 0 && queued == 0
	})
}

func getBody(t *testing.T, url string) string {
	t.Helper()
	resp, err := newClient().Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s = %d %s", url, resp.StatusCode, b)
	}
	return string(b)
}

func TestModelsHealthMetrics(t *testing.T) {
	srv := startMock(t, func(c *Config) { c.MaxModelLen = 1234 })
	var models struct {
		Object string      `json:"object"`
		Data   []modelCard `json:"data"`
	}
	if err := json.Unmarshal([]byte(getBody(t, srv.URL()+"/v1/models")), &models); err != nil {
		t.Fatalf("decode models: %v", err)
	}
	if models.Object != "list" || len(models.Data) != 2 || models.Data[0].ID != testModel || models.Data[0].MaxModelLen != 1234 || models.Data[0].OwnedBy != "vllm" {
		t.Fatalf("models = %+v", models)
	}
	if b := getBody(t, srv.URL()+"/health"); b != "" {
		t.Fatalf("/health body = %q, want empty", b)
	}
	mustPost(t, srv.URL()+"/v1/chat/completions", `{"model":"test-model"}`, nil)
	wants := []string{
		`vllm:generation_tokens_total{model_name="test-model"} 4`,
		`vllm:request_success_total{finished_reason="stop",model_name="test-model"} 1`,
		`vllm:time_to_first_token_seconds_count{model_name="test-model"} 1`,
		`vllm:e2e_request_latency_seconds_count{model_name="test-model"} 1`,
		`mockllm_requests_total{endpoint="chat"} 1`,
	}
	// The request is recorded after its body is written, which can be just
	// after the client has read it, so poll until every series shows up.
	var missing []string
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		metrics := getBody(t, srv.URL()+"/metrics")
		missing = missing[:0]
		for _, want := range wants {
			if !strings.Contains(metrics, want) {
				missing = append(missing, want)
			}
		}
		if len(missing) == 0 || time.Now().After(deadline) {
			break
		}
	}
	for _, want := range missing {
		t.Errorf("/metrics missing %q", want)
	}
}

func TestCaptureIsByteExact(t *testing.T) {
	srv := startMock(t, nil)
	// Odd spacing, key order, unicode escapes and tools: must arrive untouched.
	body := "{ \"model\" : \"test-model\",\"tools\":[{\"type\":\"function\",\"function\":{\"name\":\"f\",\"parameters\":{}}}],\n" +
		"\"messages\":[{\"role\":\"user\",\"content\":\"caf\\u00e9 ☃\"}], \"response_format\":{\"type\":\"json_object\"} }"
	mustPost(t, srv.URL()+"/v1/chat/completions?x=1", body, map[string]string{"X-Trace": "abc"})

	got := srv.Mock().Requests()
	if len(got) != 1 || !bytes.Equal(got[0].Body, []byte(body)) || got[0].Path != "/v1/chat/completions" || got[0].Query != "x=1" || got[0].Header.Get("X-Trace") != "abc" {
		t.Fatalf("captured = %+v", got)
	}

	var viaHTTP []RecordedRequest
	if err := json.Unmarshal([]byte(getBody(t, srv.URL()+"/mock/requests")), &viaHTTP); err != nil {
		t.Fatalf("decode /mock/requests: %v", err)
	}
	if len(viaHTTP) != 1 || !bytes.Equal(viaHTTP[0].Body, []byte(body)) {
		t.Fatalf("/mock/requests = %+v", viaHTTP)
	}

	req, _ := http.NewRequest(http.MethodDelete, srv.URL()+"/mock/requests", nil)
	resp, err := newClient().Do(req)
	if err != nil || resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE /mock/requests = %v %v", resp, err)
	}
	_ = resp.Body.Close()
	if n := len(srv.Mock().Requests()); n != 0 {
		t.Fatalf("after reset %d captured", n)
	}

	// The handler counts a completion after its last write, so the client can
	// read the whole body first.
	settle(srv, func(s Stats) bool { return s.Completed == 1 })
	var stats Stats
	if err := json.Unmarshal([]byte(getBody(t, srv.URL()+"/mock/stats")), &stats); err != nil || stats.Requests != 1 || stats.Completed != 1 {
		t.Fatalf("/mock/stats = %+v (%v)", stats, err)
	}
}

func TestRefuseEndpoint(t *testing.T) {
	srv := startMock(t, nil)
	tests := []struct {
		query  string
		status int
	}{
		{"", 400}, {"?for=abc", 400}, {"?for=-1s", 400}, {"?for=1h", 400},
	}
	for _, tc := range tests {
		resp, _ := mustPost(t, srv.URL()+"/mock/refuse"+tc.query, "", nil)
		if resp.StatusCode != tc.status {
			t.Fatalf("refuse%s = %d, want %d", tc.query, resp.StatusCode, tc.status)
		}
	}

	resp, _ := mustPost(t, srv.URL()+"/mock/refuse?for=300ms", "", nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("refuse = %d", resp.StatusCode)
	}
	if !srv.Refusing() {
		t.Fatal("Refusing() = false")
	}
	_, err := post(t, context.Background(), srv.URL()+"/v1/completions", `{"model":"test-model"}`, nil)
	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("request while refusing: %v, want ECONNREFUSED", err)
	}
	waitFor(t, "listening again", func() bool { return !srv.Refusing() })
	if resp, _ := mustPost(t, srv.URL()+"/v1/completions", `{"model":"test-model"}`, nil); resp.StatusCode != 200 {
		t.Fatalf("after refusal window: %d", resp.StatusCode)
	}

	// Programmatic control, and a pending window cancelled by SetRefusing(false).
	if err := srv.RefuseFor(time.Hour); err != nil {
		t.Fatalf("RefuseFor: %v", err)
	}
	if _, err := net.DialTimeout("tcp", srv.Addr(), dialTimeout); !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("dial while refusing: %v", err)
	}
	if err := srv.SetRefusing(false); err != nil {
		t.Fatalf("SetRefusing(false): %v", err)
	}
	if resp, _ := mustPost(t, srv.URL()+"/v1/completions", `{"model":"test-model"}`, nil); resp.StatusCode != 200 {
		t.Fatalf("after SetRefusing(false): %d", resp.StatusCode)
	}
}

func TestRefuseWithoutServer(t *testing.T) {
	m, err := New(func() Config { c := Defaults(); c.ListenAddr = "unused"; return c }())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	req, _ := http.NewRequest(http.MethodPost, "/mock/refuse?for=1s", nil)
	rec := httptest.NewRecorder()
	m.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", rec.Code)
	}
}

func TestControlDisabled(t *testing.T) {
	srv := startMock(t, func(c *Config) { c.ControlEnabled = false })
	resp, err := newClient().Get(srv.URL() + "/mock/stats")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("/mock/stats with control disabled = %d, want 404", resp.StatusCode)
	}
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	c := Defaults()
	c.Models = nil
	if _, err := New(c); err == nil {
		t.Fatal("New with no models: want error")
	}
	if _, err := Start(c, nil); err == nil {
		t.Fatal("Start with no models: want error")
	}
}

func TestShutdownCutsHungRequests(t *testing.T) {
	srv := startMock(t, nil)
	errs := make(chan error, 1)
	go func() {
		_, err := post(t, context.Background(), srv.URL()+"/v1/completions", `{"model":"test-model"}`, map[string]string{HeaderFault: faultHang})
		errs <- err
	}()
	waitFor(t, "hang running", func() bool { return srv.Mock().Stats().Running == 1 })
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := srv.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown = %v, want deadline exceeded", err)
	}
	select {
	case err := <-errs:
		if err == nil {
			t.Fatal("hung request got a response after shutdown")
		}
	case <-time.After(testTimeout):
		t.Fatal("hung request not cut by shutdown")
	}
	select {
	case <-srv.Done():
	default:
		t.Fatal("Done not closed after Shutdown")
	}
}

// TestVLLMCompatModes checks the error body shape and the /metrics name set
// end to end for each MOCKLLM_VLLM_COMPAT mode (ADR 0011).
func TestVLLMCompatModes(t *testing.T) {
	tests := []struct {
		mode        VLLMCompat
		wantNames   []string
		absentNames []string
	}{
		{
			mode:        CompatCurrent,
			wantNames:   []string{"vllm:kv_cache_usage_perc{", "vllm:prefix_cache_queries_total{", "vllm:prefix_cache_hits_total{"},
			absentNames: []string{"vllm:gpu_cache_usage_perc", "vllm:gpu_prefix_cache_"},
		},
		{
			mode:        CompatLegacy,
			wantNames:   []string{"vllm:gpu_cache_usage_perc{", "vllm:gpu_prefix_cache_queries_total{", "vllm:gpu_prefix_cache_hits_total{"},
			absentNames: []string{"vllm:kv_cache_usage_perc", "\nvllm:prefix_cache_", " vllm:prefix_cache_"},
		},
	}
	for _, tc := range tests {
		t.Run(string(tc.mode), func(t *testing.T) {
			srv := startMock(t, func(c *Config) { c.VLLMCompat = tc.mode })

			// An injected 5xx and a 4xx rejection both use the mode's shape.
			cases := []struct {
				body   string
				hdr    map[string]string
				status int
				errMsg string
			}{
				{body: `{"model":"test-model"}`, hdr: map[string]string{HeaderFault: fault5xx}, status: 500, errMsg: "injected fault"},
				{body: `{"model":"nope"}`, status: 404, errMsg: "does not exist"},
			}
			for _, ec := range cases {
				resp, b := mustPost(t, srv.URL()+"/v1/chat/completions", ec.body, ec.hdr)
				if resp.StatusCode != ec.status {
					t.Fatalf("status = %d, want %d (%s)", resp.StatusCode, ec.status, b)
				}
				assertErrorShape(t, tc.mode, b, ec.status, ec.errMsg)
			}

			metrics := getBody(t, srv.URL()+"/metrics")
			for _, want := range tc.wantNames {
				if !strings.Contains(metrics, want) {
					t.Errorf("/metrics missing %q", want)
				}
			}
			for _, absent := range tc.absentNames {
				if strings.Contains(metrics, absent) {
					t.Errorf("/metrics in %s mode contains %q", tc.mode, absent)
				}
			}
		})
	}
}

// assertErrorShape decodes b generically and checks it has exactly the
// top-level keys of the mode's vLLM error shape.
func assertErrorShape(t *testing.T, mode VLLMCompat, b []byte, status int, msg string) {
	t.Helper()
	var top map[string]json.RawMessage
	if err := json.Unmarshal(b, &top); err != nil {
		t.Fatalf("error body not a JSON object: %s", b)
	}
	detail := top
	switch mode {
	case CompatCurrent:
		if len(top) != 1 || top["error"] == nil {
			t.Fatalf("current error body = %s, want only an \"error\" object", b)
		}
		if err := json.Unmarshal(top["error"], &detail); err != nil {
			t.Fatalf("error member not an object: %s", b)
		}
	case CompatLegacy:
		if string(top["object"]) != `"error"` || top["error"] != nil {
			t.Fatalf("legacy error body = %s, want flat object with \"object\":\"error\"", b)
		}
	}
	for _, k := range []string{"message", "type", "param", "code"} {
		if _, ok := detail[k]; !ok {
			t.Errorf("error body %s missing %q", b, k)
		}
	}
	if string(detail["code"]) != fmt.Sprint(status) || !strings.Contains(string(detail["message"]), msg) {
		t.Errorf("error body %s, want code %d and message containing %q", b, status, msg)
	}
}
