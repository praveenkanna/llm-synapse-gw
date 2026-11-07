package runner

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/harness/internal/outcome"
)

func newClient(url string) *Client {
	return &Client{HTTP: &http.Client{Transport: NewTransport(16)}, BaseURL: url, Timeout: 5 * time.Second}
}

func streamReq() Request {
	return Request{Class: "c", Path: pathChat, Stream: true, Body: []byte(`{"stream":true}`)}
}

func TestClientStreamingMeasurements(t *testing.T) {
	f := &fakeUpstream{ttft: 40 * time.Millisecond, roleGap: 30 * time.Millisecond, gap: 10 * time.Millisecond, tokens: 6}
	srv := f.start(t)
	c := newClient(srv.URL)
	c.APIKey = "secret-token"

	var mu sync.Mutex
	var gaps []time.Duration
	res := c.Do(context.Background(), streamReq(), func(d time.Duration) {
		mu.Lock()
		gaps = append(gaps, d)
		mu.Unlock()
	})

	if !res.OK() || res.Status != 200 {
		t.Fatalf("result = %+v, want OK", res)
	}
	if want := f.ttft + f.roleGap; res.TTFT < want || res.TTFT > want+200*time.Millisecond {
		t.Errorf("TTFT = %v, want about %v (the first token, not the role chunk)", res.TTFT, want)
	}
	// The first body byte is the role-only chunk, a roleGap before the first
	// token. A late read of it shortens the gap under load, so only the
	// order is asserted.
	if res.FirstByte < f.ttft || res.TTFT <= res.FirstByte {
		t.Errorf("first byte %v, TTFT %v: want the first byte (the role chunk) before the first token", res.FirstByte, res.TTFT)
	}
	if !res.Complete {
		t.Errorf("result not complete: %+v", res)
	}
	if res.Chunks != 6 || res.OutputTokens != 6 || res.PromptTokens != 7 {
		t.Errorf("chunks/output/prompt = %d/%d/%d, want 6/6/7", res.Chunks, res.OutputTokens, res.PromptTokens)
	}
	if len(gaps) != 5 {
		t.Fatalf("got %d gaps, want 5", len(gaps))
	}
	// The server sends the last token no earlier than ttft + 5 gaps after the
	// request. Check the observed span against that, not each gap against the
	// sleep: a first token that arrives late shortens every gap after it.
	var span time.Duration
	for _, g := range gaps {
		span += g
	}
	if want := f.ttft + 5*f.gap; res.TTFT+span < want {
		t.Errorf("TTFT %v + gaps %v = %v, want >= %v", res.TTFT, span, res.TTFT+span, want)
	}
	if res.ITLMean <= 0 || res.ITLMax < res.ITLMean {
		t.Errorf("ITL mean/max = %v/%v, want 0 < mean <= max", res.ITLMean, res.ITLMax)
	}
	// Duration is read after the last token, so it can't be shorter than the first
	// token's arrival plus the observed gaps. The server's nominal gap is no bound:
	// tokens that bunch up after a late first one make observed gaps shorter.
	if res.Duration < res.TTFT+span {
		t.Errorf("duration %v shorter than TTFT %v + gaps %v", res.Duration, res.TTFT, span)
	}
	if auth, _, _ := f.snapshot(); auth[0] != "Bearer secret-token" {
		t.Errorf("Authorization = %q", auth[0])
	}
}

func TestClientNonStreaming(t *testing.T) {
	f := &fakeUpstream{ttft: 20 * time.Millisecond, tokens: 9}
	srv := f.start(t)
	res := newClient(srv.URL).Do(context.Background(), Request{Class: "c", Path: pathChat, Body: []byte(`{}`)}, nil)
	if !res.OK() || res.OutputTokens != 9 || res.PromptTokens != 7 {
		t.Fatalf("result = %+v", res)
	}
	if res.TTFT != res.Duration && res.Duration-res.TTFT > time.Millisecond {
		t.Errorf("non-streaming TTFT %v should equal duration %v", res.TTFT, res.Duration)
	}
	if res.TTFT < f.ttft {
		t.Errorf("TTFT = %v < server delay %v", res.TTFT, f.ttft)
	}
	if auth, _, _ := f.snapshot(); auth[0] != "" {
		t.Errorf("Authorization sent without a key: %q", auth[0])
	}
}

func TestClientSendsPriorityHeader(t *testing.T) {
	tests := []struct {
		name     string
		priority string
		want     []string
	}{
		{name: "set", priority: "interactive", want: []string{"interactive"}},
		{name: "unset sends none", priority: "", want: nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeUpstream{tokens: 2}
			srv := f.start(t)
			req := streamReq()
			req.Priority = tc.priority
			if res := newClient(srv.URL).Do(context.Background(), req, nil); !res.OK() {
				t.Fatalf("result = %+v, want OK", res)
			}
			got := f.priorityHeaders()
			if len(got) != 1 || !slices.Equal(got[0], tc.want) {
				t.Fatalf("X-Priority = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestClientSendsTemplateHeaders: both template headers go out only when
// the request names a template ID and a version.
func TestClientSendsTemplateHeaders(t *testing.T) {
	tests := []struct {
		name            string
		id              string
		version         int
		wantID, wantVer []string
	}{
		{name: "set", id: "support-assistant", version: 3, wantID: []string{"support-assistant"}, wantVer: []string{"3"}},
		{name: "unset sends none", id: "", version: 0},
		{name: "id without version sends none", id: "support-assistant", version: 0},
		{name: "version without id sends none", id: "", version: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeUpstream{tokens: 2}
			srv := f.start(t)
			req := streamReq()
			req.TemplateID, req.TemplateVersion = tc.id, tc.version
			if res := newClient(srv.URL).Do(context.Background(), req, nil); !res.OK() {
				t.Fatalf("result = %+v, want OK", res)
			}
			h := f.requestHeaders()
			if len(h) != 1 {
				t.Fatalf("got %d requests, want 1", len(h))
			}
			if got := h[0].Values("X-Template-Id"); !slices.Equal(got, tc.wantID) {
				t.Errorf("X-Template-Id = %q, want %q", got, tc.wantID)
			}
			if got := h[0].Values("X-Template-Version"); !slices.Equal(got, tc.wantVer) {
				t.Errorf("X-Template-Version = %q, want %q", got, tc.wantVer)
			}
		})
	}
}

// TestClientExtraHeadersAndCache: a request's extra headers go out, never
// replacing one the client sets, and the gateway's X-Gateway-Cache response
// header is recorded.
func TestClientExtraHeadersAndCache(t *testing.T) {
	for _, stream := range []bool{false, true} {
		f := &fakeUpstream{tokens: 2, cache: "hit"}
		srv := f.start(t)
		req := streamReq()
		req.Stream = stream
		if !stream {
			req.Body = []byte(`{"stream":false}`)
		}
		req.Headers = map[string]string{"X-Mock-Output-Tokens": "400", "Content-Type": "text/plain"}
		res := newClient(srv.URL).Do(context.Background(), req, nil)
		if !res.OK() || res.Cache != "hit" {
			t.Fatalf("stream=%t: result = %+v, want OK with cache hit", stream, res)
		}
		h := f.requestHeaders()[0]
		if got := h.Get("X-Mock-Output-Tokens"); got != "400" {
			t.Errorf("X-Mock-Output-Tokens = %q, want 400", got)
		}
		if got := h.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q: an extra header replaced the client's", got)
		}
	}
	f := &fakeUpstream{tokens: 2}
	if res := newClient(f.start(t).URL).Do(context.Background(), streamReq(), nil); res.Cache != "" {
		t.Errorf("cache = %q with no header, want empty", res.Cache)
	}
}

func TestClientFailures(t *testing.T) {
	// A loopback port below 1024 that refuses connections. A listener on port
	// 0 closed again would leave an ephemeral port that a server one of the
	// cases below starts could be handed, and then "refused" would answer.
	refused := func() string {
		for p := 1; p < 1024; p++ {
			addr := fmt.Sprintf("127.0.0.1:%d", p)
			c, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
			if err == nil {
				_ = c.Close()
				continue
			}
			if errors.Is(err, syscall.ECONNREFUSED) {
				return "http://" + addr
			}
		}
		t.Fatal("no loopback port below 1024 refuses connections")
		return ""
	}
	tests := []struct {
		name       string
		fault      string
		url        string
		stream     bool
		timeout    time.Duration
		wantErr    string
		wantStatus int
	}{
		{name: "5xx", fault: "500", stream: true, wantErr: outcome.ErrHTTPStatus, wantStatus: 500},
		{name: "mid-stream error event", fault: "stream_error", stream: true, wantErr: outcome.ErrStreamError, wantStatus: 200},
		{name: "stream without DONE", fault: "truncate", stream: true, wantErr: outcome.ErrTruncated, wantStatus: 200},
		{name: "bad non-stream body", fault: "bad_json", wantErr: outcome.ErrBadResponse, wantStatus: 200},
		{name: "hang times out", fault: "hang", stream: true, timeout: 100 * time.Millisecond, wantErr: outcome.ErrTimeout},
		{name: "connection refused", url: refused(), wantErr: outcome.ErrConnect},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			url := tc.url
			if url == "" {
				url = (&fakeUpstream{tokens: 4, fault: tc.fault}).start(t).URL
			}
			c := newClient(url)
			if tc.timeout > 0 {
				c.Timeout = tc.timeout
			}
			req := streamReq()
			req.Stream = tc.stream
			res := c.Do(context.Background(), req, nil)
			if res.Err != tc.wantErr || res.Status != tc.wantStatus || res.OK() {
				t.Fatalf("err/status = %q/%d, want %q/%d", res.Err, res.Status, tc.wantErr, tc.wantStatus)
			}
		})
	}
}

func TestClientCanceled(t *testing.T) {
	srv := (&fakeUpstream{fault: "hang"}).start(t)
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	if res := newClient(srv.URL).Do(ctx, streamReq(), nil); res.Err != outcome.ErrCanceled {
		t.Fatalf("err = %q, want %q", res.Err, outcome.ErrCanceled)
	}
}
