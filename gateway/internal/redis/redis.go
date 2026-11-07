// Package redis is a minimal Redis client speaking RESP2 over one TCP
// connection.
//
// It exists for the gateway's background state sync (admission counters every
// ~100 ms, ADR 0004): one goroutine per gateway, never a request-path caller.
// That shape needs no pooling, pipelining or pub/sub, so a few hundred lines of
// standard library beat a third-party client and its dependency tree. Calls
// are serialized with a mutex on a single lazily dialed connection.
//
// Failure rules: any I/O or protocol error closes the connection so the next
// call redials; a half-read reply is never reused. A Redis error reply
// (*Error) is a normal answer and keeps the connection. Every reply is parsed
// against fixed bounds (bulk size, array length, nesting depth).
package redis

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultDialTimeout = 100 * time.Millisecond

	// readBufferSize also caps the length of a simple-string, error or
	// integer line: a longer line is a protocol error.
	readBufferSize = 64 << 10

	maxBulkBytes  = 16 << 20
	maxArrayLen   = 1 << 20
	maxArrayDepth = 8

	// arrayPrealloc caps the up-front allocation for an array reply so a
	// hostile length header cannot force a large allocation before any
	// element arrives.
	arrayPrealloc = 1024
)

var (
	errClosed   = errors.New("redis: client closed")
	errProtocol = errors.New("redis: protocol error")
)

// Options configures a Client.
type Options struct {
	Addr        string        // host:port
	Username    string        // optional ACL user; used only with Password
	Password    string        // optional; AUTH is sent after dial when set
	DB          int           // SELECT is sent after dial when != 0
	DialTimeout time.Duration // default 100ms when 0
}

// Kind is the type of a reply Value. Error replies are returned as *Error,
// never as a Value.
type Kind uint8

const (
	KindNil    Kind = iota // nil bulk string or nil array
	KindString             // simple or bulk string
	KindInt
	KindArray
	KindError // Redis error reply in pipelined execution
)

// Value is a decoded RESP2 reply.
type Value struct {
	Kind  Kind
	Str   string
	Bytes []byte // raw bytes for bulk strings; aliases reader buffer or response slice
	Int   int64
	Array []Value
	Err   error // non-nil for KindError
}

// Error is a Redis error reply such as "NOSCRIPT ..." or "WRONGTYPE ...".
// Do returns it as the error; the connection stays usable.
type Error struct {
	Msg string
}

func (e *Error) Error() string { return "redis: " + e.Msg }

// Client is a single-connection Redis client, safe for concurrent use. Calls
// are serialized; Close waits for an in-flight call to finish.
type Client struct {
	opts Options

	mu     sync.Mutex
	closed bool
	conn   net.Conn
	br     *bufio.Reader
	bw     *bufio.Writer
}

// New returns a client for opts. It does not dial; the first Do does.
func New(opts Options) *Client {
	if opts.DialTimeout <= 0 {
		opts.DialTimeout = defaultDialTimeout
	}
	return &Client{opts: opts}
}

// Do sends one command and returns its reply. The context deadline bounds the
// dial, write and read; cancellation interrupts a blocked call. A Redis error
// reply is returned as *Error.
func (c *Client) Do(ctx context.Context, args ...string) (Value, error) {
	if len(args) == 0 {
		return Value{}, errors.New("redis: empty command")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return Value{}, errClosed
	}
	if err := ctx.Err(); err != nil {
		return Value{}, fmt.Errorf("redis: %s: %w", args[0], err)
	}
	if c.conn == nil {
		if err := c.connect(ctx); err != nil {
			return Value{}, err
		}
	}
	return c.call(ctx, args)
}

// Close closes the connection. It is idempotent; Do after Close fails.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn, c.br, c.bw = nil, nil, nil
	return err
}

// Script is a Lua script addressed by its SHA1 for EVALSHA.
type Script struct {
	src string
	sha string
}

// NewScript returns a Script for src.
func NewScript(src string) *Script {
	sum := sha1.Sum([]byte(src))
	return &Script{src: src, sha: hex.EncodeToString(sum[:])}
}

// SHA returns the script's hex SHA1 checksum.
func (s *Script) SHA() string { return s.sha }

// EvalArgs returns the wire argument slice for cmd ("EVALSHA" or "EVAL") with keys and args.
func EvalArgs(cmd, body string, keys, args []string) []string {
	return evalArgs(cmd, body, keys, args)
}

// Eval runs EVALSHA and falls back to EVAL on a NOSCRIPT error, which also
// loads the script so later calls hit EVALSHA.
func (c *Client) Eval(ctx context.Context, s *Script, keys []string, args ...string) (Value, error) {
	v, err := c.Do(ctx, evalArgs("EVALSHA", s.sha, keys, args)...)
	var rerr *Error
	if errors.As(err, &rerr) && strings.HasPrefix(rerr.Msg, "NOSCRIPT") {
		return c.Do(ctx, evalArgs("EVAL", s.src, keys, args)...)
	}
	return v, err
}

func evalArgs(cmd, body string, keys, args []string) []string {
	out := make([]string, 0, 3+len(keys)+len(args))
	out = append(out, cmd, body, strconv.Itoa(len(keys)))
	out = append(out, keys...)
	return append(out, args...)
}

// connect dials and runs AUTH/SELECT. The caller holds c.mu.
func (c *Client) connect(ctx context.Context) error {
	d := net.Dialer{Timeout: c.opts.DialTimeout}
	conn, err := d.DialContext(ctx, "tcp", c.opts.Addr)
	if err != nil {
		return wrapErr(ctx, "dial "+c.opts.Addr, err)
	}
	c.conn = conn
	c.br = bufio.NewReaderSize(conn, readBufferSize)
	c.bw = bufio.NewWriter(conn)

	var handshake [][]string
	if c.opts.Password != "" {
		if c.opts.Username != "" {
			handshake = append(handshake, []string{"AUTH", c.opts.Username, c.opts.Password})
		} else {
			handshake = append(handshake, []string{"AUTH", c.opts.Password})
		}
	}
	if c.opts.DB != 0 {
		handshake = append(handshake, []string{"SELECT", strconv.Itoa(c.opts.DB)})
	}
	for _, args := range handshake {
		if _, err := c.call(ctx, args); err != nil {
			c.closeConn()
			return fmt.Errorf("redis: connect: %s failed: %w", args[0], err)
		}
	}
	return nil
}

// call runs one round trip on the open connection, closing it on any error
// other than a Redis error reply. The caller holds c.mu.
func (c *Client) call(ctx context.Context, args []string) (Value, error) {
	v, err := c.roundTrip(ctx, args)
	if err == nil {
		return v, nil
	}
	var rerr *Error
	if errors.As(err, &rerr) {
		return Value{}, rerr
	}
	c.closeConn()
	return Value{}, wrapErr(ctx, args[0], err)
}

// DoPipeline sends a batch of commands in a single round-trip and returns their replies.
// Redis error replies are stored in the Value with KindError. Transport errors close the connection.
func (c *Client) DoPipeline(ctx context.Context, cmds [][]string) ([]Value, error) {
	if len(cmds) == 0 {
		return nil, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("redis: pipeline: %w", err)
	}
	if c.conn == nil {
		if err := c.connect(ctx); err != nil {
			return nil, err
		}
	}
	return c.callPipeline(ctx, cmds)
}

func (c *Client) callPipeline(ctx context.Context, cmds [][]string) ([]Value, error) {
	conn := c.conn
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return nil, err
	}
	if ctx.Done() != nil {
		fired := make(chan struct{})
		stop := context.AfterFunc(ctx, func() {
			_ = conn.SetDeadline(time.Unix(1, 0))
			close(fired)
		})
		defer func() {
			if !stop() {
				<-fired
			}
		}()
	}

	for _, args := range cmds {
		if err := writeCommand(c.bw, args); err != nil {
			c.closeConn()
			return nil, wrapErr(ctx, "pipeline write", err)
		}
	}
	replies := make([]Value, len(cmds))
	for i := range cmds {
		v, err := readValue(c.br, 0)
		if err != nil {
			var rerr *Error
			if errors.As(err, &rerr) {
				replies[i] = Value{Kind: KindError, Str: rerr.Msg, Err: rerr}
				continue
			}
			c.closeConn()
			return nil, wrapErr(ctx, "pipeline read", err)
		}
		replies[i] = v
	}
	return replies, nil
}

func (c *Client) roundTrip(ctx context.Context, args []string) (Value, error) {
	conn := c.conn
	deadline, _ := ctx.Deadline() // zero time clears any previous deadline
	if err := conn.SetDeadline(deadline); err != nil {
		return Value{}, err
	}
	if ctx.Done() != nil {
		fired := make(chan struct{})
		stop := context.AfterFunc(ctx, func() {
			_ = conn.SetDeadline(time.Unix(1, 0))
			close(fired)
		})
		defer func() {
			// Wait for a running AfterFunc so its past deadline cannot land
			// after the next call has set its own.
			if !stop() {
				<-fired
			}
		}()
	}

	if err := writeCommand(c.bw, args); err != nil {
		return Value{}, err
	}
	return readValue(c.br, 0)
}

func (c *Client) closeConn() {
	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.conn, c.br, c.bw = nil, nil, nil
}

func wrapErr(ctx context.Context, op string, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil && !errors.Is(err, ctxErr) {
		return fmt.Errorf("redis: %s: %w: %w", op, ctxErr, err)
	}
	return fmt.Errorf("redis: %s: %w", op, err)
}

// writeCommand encodes args as a RESP array of bulk strings and flushes.
func writeCommand(bw *bufio.Writer, args []string) error {
	var num [20]byte
	_ = bw.WriteByte('*')
	_, _ = bw.Write(strconv.AppendInt(num[:0], int64(len(args)), 10))
	_, _ = bw.WriteString("\r\n")
	for _, a := range args {
		_ = bw.WriteByte('$')
		_, _ = bw.Write(strconv.AppendInt(num[:0], int64(len(a)), 10))
		_, _ = bw.WriteString("\r\n")
		_, _ = bw.WriteString(a)
		_, _ = bw.WriteString("\r\n")
	}
	// bufio.Writer errors are sticky, so Flush reports any earlier failure.
	return bw.Flush()
}

// readValue decodes one reply. depth is the number of enclosing arrays. An
// error reply is returned as *Error after the whole reply has been consumed,
// so the stream stays in sync.
func readValue(br *bufio.Reader, depth int) (Value, error) {
	line, err := readLine(br)
	if err != nil {
		return Value{}, err
	}
	body := line[1:]
	switch line[0] {
	case '+':
		return Value{Kind: KindString, Str: string(body)}, nil
	case '-':
		return Value{}, &Error{Msg: string(body)}
	case ':':
		n, err := parseInt(body)
		if err != nil {
			return Value{}, err
		}
		return Value{Kind: KindInt, Int: n}, nil
	case '$':
		n, err := parseInt(body)
		if err != nil {
			return Value{}, err
		}
		switch {
		case n == -1:
			return Value{Kind: KindNil}, nil
		case n < 0 || n > maxBulkBytes:
			return Value{}, fmt.Errorf("%w: bulk length %d outside [-1, %d]", errProtocol, n, maxBulkBytes)
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(br, buf); err != nil {
			return Value{}, err
		}
		if buf[n] != '\r' || buf[n+1] != '\n' {
			return Value{}, fmt.Errorf("%w: bulk string not terminated by CRLF", errProtocol)
		}
		data := buf[:n]
		return Value{Kind: KindString, Str: string(data), Bytes: data}, nil
	case '*':
		n, err := parseInt(body)
		if err != nil {
			return Value{}, err
		}
		switch {
		case n == -1:
			return Value{Kind: KindNil}, nil
		case n < 0 || n > maxArrayLen:
			return Value{}, fmt.Errorf("%w: array length %d outside [-1, %d]", errProtocol, n, maxArrayLen)
		case depth >= maxArrayDepth:
			return Value{}, fmt.Errorf("%w: arrays nested deeper than %d", errProtocol, maxArrayDepth)
		}
		arr := make([]Value, 0, min(n, arrayPrealloc))
		var replyErr *Error
		for range n {
			v, err := readValue(br, depth+1)
			var rerr *Error
			switch {
			case errors.As(err, &rerr):
				if replyErr == nil {
					replyErr = rerr
				}
			case err != nil:
				return Value{}, err
			}
			arr = append(arr, v)
		}
		if replyErr != nil {
			return Value{}, replyErr
		}
		return Value{Kind: KindArray, Array: arr}, nil
	default:
		return Value{}, fmt.Errorf("%w: unknown reply type %q", errProtocol, line[0])
	}
}

// readLine returns the next CRLF-terminated line without the CRLF. The slice
// aliases the reader's buffer and is valid until the next read.
func readLine(br *bufio.Reader) ([]byte, error) {
	line, err := br.ReadSlice('\n')
	if err != nil {
		if errors.Is(err, bufio.ErrBufferFull) {
			return nil, fmt.Errorf("%w: line longer than %d bytes", errProtocol, br.Size())
		}
		return nil, err
	}
	if len(line) < 3 || line[len(line)-2] != '\r' {
		return nil, fmt.Errorf("%w: malformed line", errProtocol)
	}
	return line[:len(line)-2], nil
}

func parseInt(b []byte) (int64, error) {
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: bad integer %q", errProtocol, b)
	}
	return n, nil
}
