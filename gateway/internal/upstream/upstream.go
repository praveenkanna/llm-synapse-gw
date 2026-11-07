// Package upstream sends requests to replicas over pooled keep-alive
// HTTP/1.1 connections with explicit timeouts.
//
// Timeouts (all from config.UpstreamTimeouts):
//   - Connect: TCP connect (and TLS handshake) to a replica, once its address
//     is known. Replica host names resolve through a bounded cache with its
//     own lookup timeout (config.UpstreamDNS), never inside Connect.
//   - ResponseHeader: wait for response headers after a non-streaming request
//     is written; 0 (default) means bounded by Total only, since vLLM sends
//     non-streaming headers after the whole generation.
//   - StreamResponseHeader: the same for a streaming request; vLLM sends
//     those headers at once, so a long wait means a hung replica.
//   - FirstChunk: wait for the first response body byte after the headers;
//     0 (default) means bounded by Total only, since vLLM sends a stream's
//     headers at once and its first chunk only after queueing and prefill.
//   - IdleBetweenChunks: maximum gap between response body reads once the
//     first byte arrived; catches hung replicas without cutting long
//     generations.
//   - Total: the whole exchange, headers and body.
//
// The request body is sent exactly as given. Hop-by-hop headers and the
// caller's credentials are never forwarded; the gateway's own upstream API
// key (if configured) is sent instead.
package upstream

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
)

const (
	// tcpKeepAlive is the TCP keep-alive probe period on replica connections.
	tcpKeepAlive = 30 * time.Second
	// idleConnTimeout closes pooled connections unused for this long.
	idleConnTimeout = 90 * time.Second
)

// ErrIdleTimeout is the cause of a request cancelled because the replica sent
// nothing for longer than the idle-between-chunks timeout.
var ErrIdleTimeout = errors.New("upstream: idle between chunks timeout")

// ErrTotalTimeout is the cause of a request cancelled by the total timeout.
var ErrTotalTimeout = errors.New("upstream: total timeout")

// ErrFirstChunkTimeout is the cause of a request cancelled because the
// replica sent no body byte within the first-chunk timeout after headers.
var ErrFirstChunkTimeout = errors.New("upstream: first chunk timeout")

// ErrResponseHeaderTimeout is the cause of a request cancelled because the
// replica sent no response headers within the (stream) response-header
// timeout after the request was written.
var ErrResponseHeaderTimeout = errors.New("upstream: response header timeout")

// ErrReplicaStuck is the cause of a request cancelled by the gateway because
// its replica was ejected as a stuck engine while the request still waited
// for its first body byte (M1-20). Callers cancel the request's context with
// it (context.WithCancelCause); Do and the response body report it as
// KindStuck.
var ErrReplicaStuck = errors.New("upstream: replica ejected as stuck engine")

// Kind classifies an upstream failure for status mapping, retries and metrics.
type Kind string

// Failure kinds. Only KindConnect and KindDNS are safe to retry: no byte
// reached the replica.
const (
	KindConnect  Kind = "connect"      // could not open a connection
	KindDNS      Kind = "dns"          // replica host never resolved (no known address)
	KindTimeout  Kind = "timeout"      // response header, idle or total timeout
	KindCanceled Kind = "canceled"     // the caller went away
	KindStuck    Kind = "stuck_engine" // replica ejected as stuck while the stream hung
	KindOther    Kind = "other"        // any other transport error
)

// Error is a failed upstream exchange.
type Error struct {
	Kind    Kind
	Replica string
	Err     error
}

func (e *Error) Error() string {
	return fmt.Sprintf("upstream %s: %s: %v", e.Replica, e.Kind, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// ProbeUserAgent is the User-Agent of the requests the gateway sends on its
// own (the priority and stuck-engine probes), so a replica's logs and a
// test's capture tell them from callers' requests.
const ProbeUserAgent = "llm-synapse-gw-probe"

// Request is what the gateway forwards. Header holds the caller's headers;
// Do filters them and never modifies the map.
type Request struct {
	Method   string
	Path     string // e.g. /v1/chat/completions
	RawQuery string
	Header   http.Header
	Body     []byte // sent byte for byte; nil for no body
	// Stream marks a streaming call. The replica is then asked for an
	// uncompressed response (Accept-Encoding: identity), so neither it nor a
	// proxy in front of it compresses the stream, which would force buffering.
	Stream bool
}

// Accept-Encoding sent upstream for a streaming call.
const (
	headerAcceptEncoding = "Accept-Encoding"
	encodingIdentity     = "identity"
)

var identityHeader = []string{encodingIdentity}

// Client sends requests to replicas. It is safe for concurrent use.
type Client struct {
	hc         *http.Client
	res        *resolver
	timeouts   config.UpstreamTimeouts
	apiKey     config.Secret
	authHeader []string // precomputed ["Bearer <key>"] when apiKey is set
}

// Option configures a Client.
type Option func(*options)

type options struct {
	lookup     lookupFunc
	onFailOpen func(stage, reason string)
}

// WithFailOpen sets the function told when an upstream stage fails open
// outside any one request's handler: a replica host's lookup failed and its
// last known addresses were used (telemetry.StageUpstreamDNS, ReasonLookupFailed). It is called
// from a lookup goroutine, so it must be safe for concurrent use.
func WithFailOpen(f func(stage, reason string)) Option {
	return func(o *options) { o.onFailOpen = f }
}

// withLookup replaces the system resolver (tests).
func withLookup(f lookupFunc) Option {
	return func(o *options) { o.lookup = f }
}

// New builds a Client from the upstream configuration.
func New(cfg config.Upstream, opts ...Option) *Client {
	res := newResolverFor(cfg, opts)
	var authHeader []string
	if cfg.APIKey.IsSet() {
		authHeader = []string{"Bearer " + cfg.APIKey.Reveal()}
	}
	return &Client{
		hc:         &http.Client{Transport: newTransport(cfg, res)},
		res:        res,
		timeouts:   cfg.Timeouts,
		apiKey:     cfg.APIKey,
		authHeader: authHeader,
	}
}

// NewTransport returns the pooled HTTP/1.1 keep-alive transport used towards
// replicas. Compression is disabled so bytes relay unchanged, and no proxy
// from the environment is used: replicas are addressed directly. Host names
// resolve through a resolver cache outside the connect timeout. Response
// header timeouts are applied by Client.Do, not by the transport.
func NewTransport(cfg config.Upstream, opts ...Option) *http.Transport {
	return newTransport(cfg, newResolverFor(cfg, opts))
}

func newResolverFor(cfg config.Upstream, opts []Option) *resolver {
	o := options{lookup: systemLookup}
	for _, opt := range opts {
		opt(&o)
	}
	return newResolver(cfg.DNS, o.lookup, o.onFailOpen)
}

func newTransport(cfg config.Upstream, res *resolver) *http.Transport {
	idleTimeout := cfg.IdleConnTimeout
	if idleTimeout <= 0 {
		idleTimeout = idleConnTimeout
	}
	dialer := &net.Dialer{Timeout: cfg.Timeouts.Connect, KeepAlive: tcpKeepAlive}
	tr := &http.Transport{
		Proxy:       nil,
		DialContext: res.dialFunc(dialer.DialContext, cfg.Timeouts.Connect),
		TLSClientConfig: &tls.Config{
			MinVersion:         tls.VersionTLS12,
			ClientSessionCache: tls.NewLRUClientSessionCache(256),
		},
		TLSHandshakeTimeout: cfg.Timeouts.Connect,
		MaxIdleConns:        0, // bounded per host below
		MaxIdleConnsPerHost: cfg.MaxIdleConnsPerHost,
		IdleConnTimeout:     idleTimeout,
		DisableCompression:  true,
		WriteBufferSize:     16 << 10,
		ReadBufferSize:      8 << 10,
	}
	if cfg.EnableHTTP2 {
		tr.ForceAttemptHTTP2 = true
	} else {
		tr.ForceAttemptHTTP2 = false
		tr.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{} // HTTP/1.1 only
	}
	return tr
}

// ProbeClient returns an *http.Client sharing the upstream transport (its
// cached DNS resolver, TLS session cache, and warm keep-alive pool), with an
// optional per-exchange timeout (0 for callers that use context deadlines).
func (c *Client) ProbeClient(timeout time.Duration) *http.Client {
	return &http.Client{Transport: c.hc.Transport, Timeout: timeout}
}

// Prewarm starts background lookups of the replica host names in pools, so
// the request path rarely meets a host with no known address. Call it at
// startup and after each replica reload. It never blocks on DNS.
func (c *Client) Prewarm(pools []config.Pool) { c.res.prewarmPools(pools) }

// CloseIdleConnections closes pooled connections, e.g. on shutdown.
func (c *Client) CloseIdleConnections() { c.hc.CloseIdleConnections() }

// Do sends req to replica. On success the caller must close resp.Body, which
// also releases the request's timers and context. The response body enforces
// the idle-between-chunks and total timeouts. ctx should be the caller's
// request context so a client disconnect cancels the upstream request.
func (c *Client) Do(ctx context.Context, replica discovery.Replica, req Request) (*http.Response, error) {
	ctx, cancel := context.WithCancelCause(ctx)
	total := time.AfterFunc(c.timeouts.Total, func() { cancel(ErrTotalTimeout) })
	release := func() {
		total.Stop()
		cancel(context.Canceled)
	}
	headerLimit := c.timeouts.ResponseHeader
	if req.Stream {
		headerLimit = c.timeouts.StreamResponseHeader
	}
	var ht headerTimer
	if headerLimit > 0 {
		ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
			WroteRequest: func(httptrace.WroteRequestInfo) { ht.start(headerLimit, cancel) },
		})
	}

	hreq, err := c.newRequest(ctx, replica, req)
	if err != nil {
		release()
		return nil, &Error{Kind: KindOther, Replica: replica.Name, Err: err}
	}
	resp, err := c.hc.Do(hreq)
	ht.stop()
	if err != nil {
		cause := context.Cause(ctx)
		release()
		return nil, &Error{Kind: classify(err, cause), Replica: replica.Name, Err: errors.Join(err, causeIfTimeout(cause), causeIfStuck(cause))}
	}
	resp.Body = newIdleBody(ctx, replica.Name, resp.Body, c.timeouts.FirstChunk, c.timeouts.IdleBetweenChunks, cancel, release)
	return resp, nil
}

func (c *Client) newRequest(ctx context.Context, replica discovery.Replica, req Request) (*http.Request, error) {
	if replica.BaseURL == nil {
		return nil, errors.New("replica has no URL")
	}
	method := req.Method
	if method == "" {
		method = http.MethodGet
	} else if method != http.MethodPost && method != http.MethodGet && !validMethod(method) {
		return nil, fmt.Errorf("build request: net/http: invalid method %q", method)
	}
	path := req.Path
	if basePath := strings.TrimSuffix(replica.BaseURL.Path, "/"); basePath != "" {
		path = basePath + req.Path
	}
	u := &url.URL{
		Scheme:   replica.BaseURL.Scheme,
		Host:     replica.BaseURL.Host,
		Path:     path,
		RawQuery: req.RawQuery,
	}
	var body io.ReadCloser = http.NoBody
	var getBody func() (io.ReadCloser, error)
	var contentLength int64
	if req.Body != nil {
		if len(req.Body) == 0 {
			getBody = emptyBodyFunc
		} else {
			raw := req.Body
			body = &byteBody{b: raw}
			contentLength = int64(len(raw))
			getBody = func() (io.ReadCloser, error) { return &byteBody{b: raw}, nil }
		}
	}
	hdr := forwardHeaders(req.Header) // the only copy of the caller's headers
	if req.Stream {
		hdr[headerAcceptEncoding] = identityHeader
	}
	if len(c.authHeader) > 0 {
		hdr["Authorization"] = c.authHeader
	}
	base := http.Request{
		Method:        method,
		URL:           u,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        hdr,
		Body:          body,
		GetBody:       getBody,
		ContentLength: contentLength,
		Host:          u.Host,
	}
	return base.WithContext(ctx), nil
}

func emptyBodyFunc() (io.ReadCloser, error) { return http.NoBody, nil }

type byteBody struct {
	b   []byte
	off int
}

func (r *byteBody) Read(p []byte) (int, error) {
	if r.off >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.off:])
	r.off += n
	return n, nil
}

func (r *byteBody) WriteTo(w io.Writer) (int64, error) {
	if r.off >= len(r.b) {
		return 0, nil
	}
	n, err := w.Write(r.b[r.off:])
	r.off += n
	return int64(n), err
}

func (r *byteBody) Close() error { return nil }

func validMethod(method string) bool {
	if len(method) == 0 {
		return false
	}
	for i := 0; i < len(method); i++ {
		if !isTokenByte(method[i]) {
			return false
		}
	}
	return true
}

func isTokenByte(c byte) bool {
	if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
		return true
	}
	switch c {
	case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
		return true
	}
	return false
}

// headerTimer cancels a request whose response headers do not arrive in time
// after the request was written. start runs on the transport's write
// goroutine and stop on the caller's, so a stopped timer never fires.
type headerTimer struct {
	mu      sync.Mutex
	t       *time.Timer
	stopped bool
}

func (h *headerTimer) start(d time.Duration, cancel context.CancelCauseFunc) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.stopped || h.t != nil {
		return
	}
	h.t = time.AfterFunc(d, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if !h.stopped {
			cancel(ErrResponseHeaderTimeout)
		}
	})
}

func (h *headerTimer) stop() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.stopped = true
	if h.t != nil {
		h.t.Stop()
	}
}

// isTimeoutCause reports whether cause is one of the gateway's timeouts.
func isTimeoutCause(cause error) bool {
	return errors.Is(cause, ErrTotalTimeout) || errors.Is(cause, ErrIdleTimeout) ||
		errors.Is(cause, ErrResponseHeaderTimeout) || errors.Is(cause, ErrFirstChunkTimeout)
}

// classify maps a transport error to a Kind. cause is the context cause.
func classify(err, cause error) Kind {
	switch {
	case isTimeoutCause(cause):
		return KindTimeout
	case errors.Is(cause, ErrReplicaStuck):
		return KindStuck
	case cause != nil || errors.Is(err, context.Canceled):
		return KindCanceled
	}
	var dns *net.DNSError
	if errors.As(err, &dns) {
		return KindDNS
	}
	var op *net.OpError
	if errors.As(err, &op) && op.Op == "dial" {
		return KindConnect
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return KindTimeout
	}
	return KindOther
}

func causeIfStuck(cause error) error {
	if errors.Is(cause, ErrReplicaStuck) {
		return cause
	}
	return nil
}

func causeIfTimeout(cause error) error {
	if isTimeoutCause(cause) {
		return cause
	}
	return nil
}

// KindOf returns the Kind of an error from Do or from reading its response
// body, or KindOther if err carries none.
func KindOf(err error) Kind {
	var ue *Error
	if errors.As(err, &ue) {
		return ue.Kind
	}
	switch {
	case isTimeoutCause(err):
		return KindTimeout
	case errors.Is(err, ErrReplicaStuck):
		return KindStuck
	case errors.Is(err, context.Canceled):
		return KindCanceled
	}
	return KindOther
}

// Retryable reports whether a failed Do may be sent again, to another
// replica: only when no connection was opened (connect or name lookup
// failed), so no byte of the request reached a replica. Timeouts,
// cancellations and failures after the request was written are never
// retryable (a generation must not run twice), and neither is any error from
// reading a response body.
func Retryable(err error) bool {
	var ue *Error
	return errors.As(err, &ue) && (ue.Kind == KindConnect || ue.Kind == KindDNS)
}

// IsHardConnectFailure reports whether a failed Do says the replica is
// unreachable, quickly and unambiguously: the connection was refused
// (ECONNREFUSED), the host or network is unreachable (EHOSTUNREACH,
// ENETUNREACH), or its name authoritatively does not exist (NXDOMAIN or an
// empty answer). Such a failure counts towards passive ejection.
//
// Other connect and DNS failures, dial timeouts and connect budgets that ran
// out above all, are Retryable but not hard: a healthy replica on a
// saturated host produces them (ADR 0014), so they must not eject it; the
// active /health probe still catches a dead host within a few polls.
func IsHardConnectFailure(err error) bool {
	var ue *Error
	if !errors.As(err, &ue) {
		return false
	}
	switch ue.Kind {
	case KindDNS:
		var de *net.DNSError
		return errors.As(err, &de) && de.IsNotFound && !de.IsTimeout && !de.IsTemporary
	case KindConnect:
		return errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EHOSTUNREACH) ||
			errors.Is(err, syscall.ENETUNREACH)
	}
	return false
}

// idleBody enforces the first-chunk and idle-between-chunks timeouts on a
// response body. Until the first byte arrives only the first-chunk timer
// runs (none if its limit is 0); once streaming chunks begin, Read records a
// lock-free monotonic timestamp and the idle timer re-arms itself lazily only
// when its window expires.
type idleBody struct {
	rc           io.ReadCloser
	replica      string
	ctx          context.Context
	idle         time.Duration
	epoch        time.Time
	cancel       context.CancelCauseFunc
	first        *time.Timer  // nil if the first-chunk limit is 0
	started      bool         // a byte arrived; only Read touches it
	lastReadNano atomic.Int64 // monotonic offset since epoch of last n > 0 read

	timerMu sync.Mutex
	timer   *time.Timer // idle timer, armed once when streaming chunks remain
	closed  bool

	release func()
	once    sync.Once
}

func newIdleBody(ctx context.Context, replica string, rc io.ReadCloser, firstChunk, idle time.Duration, cancel context.CancelCauseFunc, release func()) *idleBody {
	b := &idleBody{
		rc:      rc,
		replica: replica,
		ctx:     ctx,
		idle:    idle,
		epoch:   time.Now(),
		cancel:  cancel,
		release: release,
	}
	if firstChunk > 0 { // else bounded by the total timeout only
		b.first = time.AfterFunc(firstChunk, func() { cancel(ErrFirstChunkTimeout) })
	}
	return b
}

func (b *idleBody) onIdleTimer() {
	last := time.Duration(b.lastReadNano.Load())
	elapsed := time.Since(b.epoch) - last
	if elapsed >= b.idle {
		b.cancel(ErrIdleTimeout)
		return
	}
	rem := b.idle - elapsed
	b.timerMu.Lock()
	if !b.closed && b.timer != nil {
		b.timer.Reset(rem)
	}
	b.timerMu.Unlock()
}

func (b *idleBody) Read(p []byte) (int, error) {
	n, err := b.rc.Read(p)
	if n > 0 && b.ctx.Err() == nil {
		if !b.started {
			b.started = true
			b.stopFirst()
			if err == nil && b.idle > 0 {
				b.lastReadNano.Store(int64(time.Since(b.epoch)))
				b.timerMu.Lock()
				if !b.closed {
					b.timer = time.AfterFunc(b.idle, b.onIdleTimer)
				}
				b.timerMu.Unlock()
			}
		} else if err == nil && b.idle > 0 {
			b.lastReadNano.Store(int64(time.Since(b.epoch)))
		}
	}
	if err != nil && err != io.EOF {
		if cause := causeIfTimeout(context.Cause(b.ctx)); cause != nil {
			err = &Error{Kind: KindTimeout, Replica: b.replica, Err: errors.Join(cause, err)}
		} else if cause := context.Cause(b.ctx); errors.Is(cause, ErrReplicaStuck) {
			err = &Error{Kind: KindStuck, Replica: b.replica, Err: errors.Join(cause, err)}
		}
	}
	return n, err
}

func (b *idleBody) stopFirst() {
	if b.first != nil {
		b.first.Stop()
	}
}

func (b *idleBody) Close() error {
	err := b.rc.Close()
	b.once.Do(func() {
		b.stopFirst()
		b.timerMu.Lock()
		b.closed = true
		if b.timer != nil {
			b.timer.Stop()
		}
		b.timerMu.Unlock()
		b.release()
	})
	return err
}
