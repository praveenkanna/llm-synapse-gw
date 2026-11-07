package outcome

import (
	"context"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"testing"
	"time"
)

// TestTransportKinds: the request context decides first (its own timeout,
// or the caller stopping), then a failed dial is connect, and anything else
// is request before the headers and read after them. A dial that timed out
// on its own is connect, not timeout: the request's timeout had not fired.
func TestTransportKinds(t *testing.T) {
	live := func() context.Context { return context.Background() }
	timedOut := func() context.Context {
		ctx, cancel := WithRequestTimeout(context.Background(), time.Nanosecond)
		t.Cleanup(cancel)
		<-ctx.Done()
		return ctx
	}
	canceled := func() context.Context {
		parent, stop := context.WithCancel(context.Background())
		ctx, cancel := WithRequestTimeout(parent, time.Hour)
		t.Cleanup(cancel)
		stop()
		return ctx
	}
	dial := &url.Error{Op: "Post", Err: &net.OpError{Op: "dial", Err: os.ErrDeadlineExceeded}}
	closed := &url.Error{Op: "Post", Err: io.EOF}
	tests := []struct {
		name       string
		ctx        func() context.Context
		err        error
		send, read string
	}{
		{"dial failure", live, dial, ErrConnect, ErrRead},
		{"closed before the response", live, closed, ErrRequest, ErrRead},
		{"request timeout", timedOut, dial, ErrTimeout, ErrTimeout},
		{"caller stopped", canceled, errors.New("x"), ErrCanceled, ErrCanceled},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := tc.ctx()
			if got := SendErr(ctx, tc.err); got != tc.send {
				t.Errorf("SendErr = %q, want %q", got, tc.send)
			}
			if got := ReadErr(ctx); got != tc.read {
				t.Errorf("ReadErr = %q, want %q", got, tc.read)
			}
		})
	}
}

// TestRequestTimeoutOff: d <= 0 sets no deadline, but still ends with ctx.
func TestRequestTimeoutOff(t *testing.T) {
	parent, stop := context.WithCancel(context.Background())
	ctx, cancel := WithRequestTimeout(parent, 0)
	defer cancel()
	if _, ok := ctx.Deadline(); ok {
		t.Fatal("deadline set for a zero timeout")
	}
	stop()
	if got := ReadErr(ctx); got != ErrCanceled {
		t.Fatalf("ReadErr = %q, want %q", got, ErrCanceled)
	}
}
