package runner

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/harness/internal/outcome"
)

// Request headers the gateway reads.
const (
	headerPriority        = "X-Priority"         // Request.Priority
	headerTemplateID      = "X-Template-Id"      // Request.TemplateID
	headerTemplateVersion = "X-Template-Version" // Request.TemplateVersion
)

// headerCache is the gateway's response header marking a response served
// from its response cache ("hit").
const headerCache = "X-Gateway-Cache"

// maxErrorBody bounds what is read (and dropped) of a non-2xx response, to
// let the transport reuse the connection.
const maxErrorBody = 16 << 20

// Result is the measurement of one request.
type Result struct {
	Seq       int64
	Class     string
	Stream    bool
	Scheduled time.Duration // intended send time, from run start
	Start     time.Duration // actual send time, from run start
	Status    int           // HTTP status; 0 when no response
	Err       string        // error kind; "" on success
	TraceID   string        // trace ID of the traceparent sent; "" when not sent
	// TTFT is time from send to the first token. For non-streaming requests
	// it equals Duration: the caller sees nothing before the whole body.
	TTFT time.Duration
	// FirstByte is time from send to the first body byte; zero when none.
	FirstByte    time.Duration
	Duration     time.Duration
	ITLMean      time.Duration
	ITLMax       time.Duration
	OutputTokens int
	PromptTokens int
	Chunks       int    // token-bearing SSE events
	Complete     bool   // the response was a whole one (ADR 0021)
	Cache        string // X-Gateway-Cache response header; "" when absent
}

// OK reports whether the request succeeded (outcome.OK).
func (r Result) OK() bool { return outcome.OK(r.Status, r.Err) }

// Client sends generated requests to one OpenAI-compatible base URL.
type Client struct {
	HTTP    *http.Client
	BaseURL string // e.g. http://localhost:8080; Request.Path is appended
	APIKey  string // sent as a bearer token when non-empty; never recorded
	Timeout time.Duration
}

// NewTransport returns an HTTP transport sized for maxConns concurrent
// requests, with compression off so streams arrive as sent.
func NewTransport(maxConns int) *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = maxConns
	t.MaxIdleConnsPerHost = maxConns
	t.DisableCompression = true
	return t
}

// Do sends req and measures it. onGap is called with every inter-token gap.
func (c *Client) Do(ctx context.Context, req Request, onGap func(time.Duration)) Result {
	res := Result{Class: req.Class, Stream: req.Stream}
	ctx, cancel := outcome.WithRequestTimeout(ctx, c.Timeout)
	defer cancel()
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+req.Path, bytes.NewReader(req.Body))
	if err != nil {
		res.Err = outcome.ErrRequest
		return res
	}
	for k, v := range req.Headers {
		hreq.Header.Set(k, v)
	}
	hreq.Header.Set("Content-Type", "application/json")
	tp, traceID := newTraceparent()
	hreq.Header.Set(headerTraceparent, tp)
	res.TraceID = traceID
	if req.Stream {
		hreq.Header.Set("Accept", "text/event-stream")
	}
	if req.Priority != "" {
		hreq.Header.Set(headerPriority, req.Priority)
	}
	if req.TemplateID != "" && req.TemplateVersion > 0 {
		hreq.Header.Set(headerTemplateID, req.TemplateID)
		hreq.Header.Set(headerTemplateVersion, strconv.Itoa(req.TemplateVersion))
	}
	if c.APIKey != "" {
		hreq.Header.Set("Authorization", "Bearer "+c.APIKey)
	}

	start := time.Now()
	resp, err := c.HTTP.Do(hreq)
	if err != nil {
		res.Err = outcome.SendErr(ctx, err)
		res.Duration = time.Since(start)
		return res
	}
	defer resp.Body.Close()
	res.Status = resp.StatusCode
	res.Cache = resp.Header.Get(headerCache)
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxErrorBody)) // reuse the connection
		res.Err = outcome.ErrHTTPStatus
		res.Duration = time.Since(start)
		return res
	}
	body := outcome.NewBody(req.Stream, start, onGap)
	readErr := readInto(ctx, resp.Body, body)
	out := body.End(time.Now(), readErr)
	res.Err, res.Complete = out.Err, out.Complete
	res.TTFT, res.FirstByte = out.TTFT, out.FirstByte
	res.ITLMean, res.ITLMax, res.Chunks = out.ITLMean, out.ITLMax, out.Chunks
	res.OutputTokens, res.PromptTokens = out.OutputTokens, out.PromptTokens
	if req.Stream && res.Err == "" { // let the transport reuse the connection
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	}
	res.Duration = time.Since(start)
	if !req.Stream {
		res.TTFT = res.Duration // the caller sees nothing before the whole body
	}
	return res
}

// readInto feeds body every read of r until it ends or body is Done. It
// returns the kind of a read failure, or "".
func readInto(ctx context.Context, r io.Reader, body *outcome.Body) string {
	buf := make([]byte, 32<<10)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			body.Write(buf[:n], time.Now())
		}
		switch {
		case body.Done(), err == io.EOF:
			return ""
		case err != nil:
			return outcome.ReadErr(ctx)
		}
	}
}
