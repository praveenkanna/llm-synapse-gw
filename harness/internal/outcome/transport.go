package outcome

import (
	"context"
	"errors"
	"net"
	"time"
)

// errRequestTimeout is the cause of a per-request timeout, telling it apart
// from the caller's own context ending (which may also be a deadline).
var errRequestTimeout = errors.New("request timeout")

// WithRequestTimeout bounds ctx by the per-request timeout d (none when
// d <= 0). SendErr and ReadErr on the returned context tell its expiry
// (timeout) from ctx itself ending (canceled).
func WithRequestTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeoutCause(ctx, d, errRequestTimeout)
}

// SendErr is the error kind of a request that failed before its response
// headers, err being what the HTTP client returned on ctx: timeout or
// canceled when ctx ended, connect when the dial failed, else request.
func SendErr(ctx context.Context, err error) string {
	if kind, done := ctxErr(ctx); done {
		return kind
	}
	var op *net.OpError
	if errors.As(err, &op) && op.Op == "dial" {
		return ErrConnect
	}
	return ErrRequest
}

// ReadErr is the error kind of a response body read that failed on ctx:
// timeout or canceled when ctx ended, else read.
func ReadErr(ctx context.Context) string {
	if kind, done := ctxErr(ctx); done {
		return kind
	}
	return ErrRead
}

// ctxErr reports whether the request context ended, and why: the request's
// own timeout, or the caller stopping.
func ctxErr(ctx context.Context) (kind string, done bool) {
	switch {
	case ctx.Err() == nil:
		return "", false
	case errors.Is(context.Cause(ctx), errRequestTimeout):
		return ErrTimeout, true
	default:
		return ErrCanceled, true
	}
}
