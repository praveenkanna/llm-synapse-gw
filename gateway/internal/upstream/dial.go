package upstream

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"time"
)

// fallbackDelay is how long the first address family gets on its own before
// the other family's addresses are tried in parallel (Happy Eyeballs, RFC
// 8305; the same value as net.Dialer's default).
const fallbackDelay = 300 * time.Millisecond

// dialAddrs connects to one of addrs within the connect timeout, like
// net.Dialer does for a host name: the addresses of the first one's family
// are tried in order, each with its share of the remaining time; if both
// families are present, the other family starts after fallbackDelay (or as
// soon as the first family fails), and the first connection wins.
func dialAddrs(ctx context.Context, dial dialFn, connect time.Duration, network string, addrs []netip.Addr, port string) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, connect)
	defer cancel()
	primaries, fallbacks := splitByFamily(addrs)
	if len(fallbacks) == 0 {
		return dialSerial(ctx, dial, network, primaries, port)
	}

	type result struct {
		conn    net.Conn
		err     error
		primary bool
	}
	results := make(chan result)
	returned := make(chan struct{})
	defer close(returned)
	race := func(ctx context.Context, addrs []netip.Addr, primary bool) {
		conn, err := dialSerial(ctx, dial, network, addrs, port)
		select {
		case results <- result{conn, err, primary}:
		case <-returned:
			if conn != nil {
				_ = conn.Close() // lost the race
			}
		}
	}
	pctx, pcancel := context.WithCancel(ctx)
	defer pcancel()
	fctx, fcancel := context.WithCancel(ctx)
	defer fcancel()

	go race(pctx, primaries, true)
	fallback := time.NewTimer(fallbackDelay)
	defer fallback.Stop()
	fallbackStarted := false
	startFallback := func() {
		if !fallbackStarted {
			fallbackStarted = true
			fallback.Stop()
			go race(fctx, fallbacks, false)
		}
	}
	var primaryErr, fallbackErr error
	for {
		select {
		case <-fallback.C:
			startFallback()
		case r := <-results:
			if r.err == nil {
				return r.conn, nil // the deferred cancels stop the other family
			}
			if r.primary {
				primaryErr = r.err
				startFallback()
			} else {
				fallbackErr = r.err
			}
			if primaryErr != nil && fallbackErr != nil {
				return nil, primaryErr
			}
		}
	}
}

// splitByFamily splits addrs into those of the first address's family and
// the rest, keeping their order.
func splitByFamily(addrs []netip.Addr) (primaries, fallbacks []netip.Addr) {
	if len(addrs) == 0 {
		return nil, nil
	}
	is4 := addrs[0].Unmap().Is4()
	for _, a := range addrs {
		if a.Unmap().Is4() == is4 {
			primaries = append(primaries, a)
		} else {
			fallbacks = append(fallbacks, a)
		}
	}
	return primaries, fallbacks
}

// dialSerial tries addrs in order. Each attempt gets an equal share of the
// time left before ctx's deadline, so one unresponsive address cannot use
// the whole connect timeout. Every failure is a *net.OpError with Op "dial",
// including a connect budget that ran out before an attempt could start, so
// it is classified as a connect failure (retryable: nothing was sent), never
// as a response timeout (ADR 0014).
func dialSerial(ctx context.Context, dial dialFn, network string, addrs []netip.Addr, port string) (net.Conn, error) {
	var first error
	for i, a := range addrs {
		if err := ctx.Err(); err != nil {
			if first == nil {
				first = dialError(network, a, port, err)
			}
			break
		}
		actx, cancel := ctx, context.CancelFunc(func() {})
		if deadline, ok := ctx.Deadline(); ok {
			share := time.Until(deadline) / time.Duration(len(addrs)-i)
			actx, cancel = context.WithTimeout(ctx, share)
		}
		conn, err := dial(actx, network, net.JoinHostPort(a.String(), port))
		cancel()
		if err == nil {
			return conn, nil
		}
		if first == nil {
			first = err
		}
	}
	if first == nil {
		first = &net.OpError{Op: "dial", Net: network, Err: errors.New("no addresses to dial")}
	}
	return nil, first
}

// dialError reports a dial to a:port that never started.
func dialError(network string, a netip.Addr, port string, err error) error {
	addr, _ := net.ResolveTCPAddr("tcp", net.JoinHostPort(a.String(), port)) // literal: no lookup
	op := &net.OpError{Op: "dial", Net: network, Err: err}
	if addr != nil {
		op.Addr = addr
	}
	return op
}
