package upstream

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// trackedConn records Close.
type trackedConn struct {
	net.Conn
	closed atomic.Bool
}

func (c *trackedConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

func newTrackedConn(t *testing.T) *trackedConn {
	t.Helper()
	a, b := net.Pipe()
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
	return &trackedConn{Conn: a}
}

// attempt is one call to the fake dialer.
type attempt struct {
	addr   string
	at     time.Duration // since the dial started
	budget time.Duration // time left on the attempt's context
}

// fakeDialer answers per address: "blackhole" blocks until the attempt's
// context ends, "refuse" fails at once, "ok" connects after delay, and
// "ok-ignore" connects after delay even if cancelled (to exercise closing
// the losing connection).
type fakeDialer struct {
	t      *testing.T
	start  time.Time
	behave map[string]string
	delay  map[string]time.Duration
	mu     sync.Mutex
	tries  []attempt
	conns  []*trackedConn
}

func (f *fakeDialer) dial(ctx context.Context, _, address string) (net.Conn, error) {
	host, _, _ := net.SplitHostPort(address)
	a := attempt{addr: host, at: time.Since(f.start)}
	if dl, ok := ctx.Deadline(); ok {
		a.budget = time.Until(dl)
	}
	f.mu.Lock()
	f.tries = append(f.tries, a)
	f.mu.Unlock()
	switch f.behave[host] {
	case "blackhole":
		<-ctx.Done()
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: ctx.Err()}
	case "refuse":
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	case "ok", "ok-ignore":
		if f.behave[host] == "ok" {
			select {
			case <-time.After(f.delay[host]):
			case <-ctx.Done():
				return nil, &net.OpError{Op: "dial", Net: "tcp", Err: ctx.Err()}
			}
		} else {
			time.Sleep(f.delay[host])
		}
		c := newTrackedConn(f.t)
		f.mu.Lock()
		f.conns = append(f.conns, c)
		f.mu.Unlock()
		return c, nil
	}
	return nil, errors.New("unexpected address " + host)
}

func (f *fakeDialer) attempts() []attempt {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]attempt(nil), f.tries...)
}

func addrs(s ...string) []netip.Addr {
	out := make([]netip.Addr, len(s))
	for i, a := range s {
		out[i] = netip.MustParseAddr(a)
	}
	return out
}

// TestDialSerialSharesConnectBudget: with two addresses of one family, an
// unresponsive first address gets half the connect timeout, not all of it.
func TestDialSerialSharesConnectBudget(t *testing.T) {
	f := &fakeDialer{t: t, start: time.Now(), behave: map[string]string{"10.0.0.1": "blackhole", "10.0.0.2": "ok"}}
	conn, err := dialAddrs(context.Background(), f.dial, 400*time.Millisecond, "tcp", addrs("10.0.0.1", "10.0.0.2"), "8000")
	if err != nil {
		t.Fatalf("dial: %v, want the second address to connect", err)
	}
	_ = conn.Close()
	tries := f.attempts()
	if len(tries) != 2 {
		t.Fatalf("attempts = %+v, want 2", tries)
	}
	if b := tries[0].budget; b > 250*time.Millisecond {
		t.Fatalf("first address got %s of a 400ms connect timeout, want about half", b)
	}
	if tries[1].at < 150*time.Millisecond || tries[1].at > 300*time.Millisecond {
		t.Fatalf("second address tried at %s, want after the first's ~200ms share", tries[1].at)
	}
}

// TestDialHappyEyeballs: a silent first family does not hold up the other
// one beyond fallbackDelay.
func TestDialHappyEyeballs(t *testing.T) {
	f := &fakeDialer{t: t, start: time.Now(), behave: map[string]string{"fd00::1": "blackhole", "10.0.0.2": "ok"}}
	start := time.Now()
	conn, err := dialAddrs(context.Background(), f.dial, 2*time.Second, "tcp", addrs("fd00::1", "10.0.0.2"), "8000")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = conn.Close()
	if d := time.Since(start); d < fallbackDelay-20*time.Millisecond || d > fallbackDelay+300*time.Millisecond {
		t.Fatalf("connected after %s, want about fallbackDelay %s", d, fallbackDelay)
	}
}

func TestDialFallbackStartsAtOnceWhenPrimaryFails(t *testing.T) {
	f := &fakeDialer{t: t, start: time.Now(), behave: map[string]string{"fd00::1": "refuse", "10.0.0.2": "ok"}}
	start := time.Now()
	conn, err := dialAddrs(context.Background(), f.dial, 2*time.Second, "tcp", addrs("fd00::1", "10.0.0.2"), "8000")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = conn.Close()
	if d := time.Since(start); d > fallbackDelay/2 {
		t.Fatalf("connected after %s, want the fallback started as soon as the primary failed", d)
	}
}

func TestDialAllFailReturnsPrimaryError(t *testing.T) {
	f := &fakeDialer{t: t, start: time.Now(), behave: map[string]string{"fd00::1": "refuse", "10.0.0.2": "blackhole"}}
	_, err := dialAddrs(context.Background(), f.dial, 100*time.Millisecond, "tcp", addrs("fd00::1", "10.0.0.2"), "8000")
	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("err = %v, want the primary family's error", err)
	}
}

func TestDialLosingConnectionIsClosed(t *testing.T) {
	f := &fakeDialer{t: t, start: time.Now(),
		behave: map[string]string{"fd00::1": "ok-ignore", "10.0.0.2": "ok"},
		delay:  map[string]time.Duration{"fd00::1": 500 * time.Millisecond, "10.0.0.2": 10 * time.Millisecond}}
	conn, err := dialAddrs(context.Background(), f.dial, 2*time.Second, "tcp", addrs("fd00::1", "10.0.0.2"), "8000")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		f.mu.Lock()
		n := len(f.conns)
		var loser *trackedConn
		for _, c := range f.conns {
			if c != conn {
				loser = c
			}
		}
		f.mu.Unlock()
		if n == 2 && loser != nil && loser.closed.Load() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the losing connection was not closed")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if conn.(*trackedConn).closed.Load() {
		t.Fatal("the winning connection was closed")
	}
}

func TestSplitByFamily(t *testing.T) {
	p, f := splitByFamily(addrs("10.0.0.1", "fd00::1", "10.0.0.2", "::ffff:10.0.0.3", "fd00::2"))
	if len(p) != 3 || len(f) != 2 || p[2] != netip.MustParseAddr("::ffff:10.0.0.3") {
		t.Fatalf("primaries %v, fallbacks %v", p, f)
	}
	if p, f := splitByFamily(nil); p != nil || f != nil {
		t.Fatal("empty input")
	}
}

// TestDialExpiredBudgetIsConnect: a connect budget that ran out before an
// attempt could start is a connect failure (502, retryable), never a
// response timeout (M0-75 re-run: a bare context.DeadlineExceeded was
// classified as timeout and answered 504).
func TestDialExpiredBudgetIsConnect(t *testing.T) {
	f := &fakeDialer{t: t, start: time.Now(), behave: map[string]string{"10.0.0.1": "ok"}}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	for _, tc := range []struct {
		name  string
		addrs []netip.Addr
	}{
		{name: "one family", addrs: addrs("10.0.0.1", "10.0.0.2")},
		{name: "both families", addrs: addrs("fd00::1", "10.0.0.1")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dialAddrs(ctx, f.dial, time.Second, "tcp", tc.addrs, "8000")
			var op *net.OpError
			if !errors.As(err, &op) || op.Op != "dial" || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("err = %#v, want a dial *net.OpError wrapping the expired deadline", err)
			}
			if k := classify(err, nil); k != KindConnect {
				t.Fatalf("classify = %s, want %s", k, KindConnect)
			}
			if !Retryable(&Error{Kind: classify(err, nil), Replica: "r", Err: err}) {
				t.Fatal("expired connect budget must be retryable: nothing was sent")
			}
		})
	}
	if n := len(f.attempts()); n != 0 {
		t.Fatalf("dial attempts = %d, want 0 with no budget left", n)
	}
	if _, err := dialSerial(context.Background(), f.dial, "tcp", nil, "8000"); err == nil {
		t.Fatal("dialSerial with no addresses returned no error")
	}
}
