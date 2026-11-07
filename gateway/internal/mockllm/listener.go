package mockllm

import (
	"errors"
	"fmt"
	"net"
	"sync"
)

// refusableListener can stop listening (so dials get "connection refused")
// and start again on the same address, without the http.Server noticing.
// This is the only honest way to inject a connect failure: a request is never
// written, so a gateway may treat it as safe to retry on another replica.
type refusableListener struct {
	addr net.Addr // bound address, fixed for the listener's life

	mu      sync.Mutex
	inner   net.Listener  // nil while refusing
	closed  bool          // permanently closed
	changed chan struct{} // closed and replaced on every state change
}

func newRefusableListener(addr string) (*refusableListener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", addr, err)
	}
	return &refusableListener{addr: ln.Addr(), inner: ln, changed: make(chan struct{})}, nil
}

// broadcast wakes every Accept waiting on a state change. Caller holds mu.
func (l *refusableListener) broadcast() {
	close(l.changed)
	l.changed = make(chan struct{})
}

// Accept waits for a connection. While refusing it blocks until listening
// resumes or the listener is closed.
func (l *refusableListener) Accept() (net.Conn, error) {
	for {
		l.mu.Lock()
		if l.closed {
			l.mu.Unlock()
			return nil, net.ErrClosed
		}
		inner, changed := l.inner, l.changed
		l.mu.Unlock()

		if inner == nil {
			<-changed
			continue
		}
		conn, err := inner.Accept()
		if err == nil {
			return conn, nil
		}
		l.mu.Lock()
		current := l.inner == inner && !l.closed
		l.mu.Unlock()
		if current {
			return nil, err
		}
		// The inner listener was closed by SetRefusing or Close; re-check state.
	}
}

// SetRefusing closes (true) or reopens (false) the socket on the same address.
func (l *refusableListener) SetRefusing(refuse bool) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return net.ErrClosed
	}
	switch {
	case refuse && l.inner != nil:
		err := l.inner.Close()
		l.inner = nil
		l.broadcast()
		if err != nil && !errors.Is(err, net.ErrClosed) {
			return fmt.Errorf("close listener: %w", err)
		}
	case !refuse && l.inner == nil:
		ln, err := net.Listen("tcp", l.addr.String())
		if err != nil {
			return fmt.Errorf("re-listen %s: %w", l.addr, err)
		}
		l.inner = ln
		l.broadcast()
	}
	return nil
}

// Refusing reports whether the socket is currently closed.
func (l *refusableListener) Refusing() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner == nil && !l.closed
}

// Close closes the listener permanently.
func (l *refusableListener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	var err error
	if l.inner != nil {
		err = l.inner.Close()
		l.inner = nil
	}
	l.broadcast()
	return err
}

// Addr returns the bound address.
func (l *refusableListener) Addr() net.Addr { return l.addr }
