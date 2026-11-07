package upstream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/testnet"
)

// dialErr is a connect failure as the dialer reports it.
func dialErr(err error) error {
	return &Error{Kind: KindConnect, Replica: "r", Err: &net.OpError{Op: "dial", Net: "tcp", Err: err}}
}

func TestIsHardConnectFailure(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "refused", err: dialErr(os.NewSyscallError("connect", syscall.ECONNREFUSED)), want: true},
		{name: "host unreachable", err: dialErr(os.NewSyscallError("connect", syscall.EHOSTUNREACH)), want: true},
		{name: "network unreachable", err: dialErr(os.NewSyscallError("connect", syscall.ENETUNREACH)), want: true},
		{name: "wrapped refused", err: fmt.Errorf("forward: %w", dialErr(syscall.ECONNREFUSED)), want: true},
		{name: "nxdomain", err: &Error{Kind: KindDNS, Replica: "r", Err: &net.OpError{Op: "dial", Err: &net.DNSError{Err: "no such host", Name: "r", IsNotFound: true}}}, want: true},
		{name: "empty answer", err: &Error{Kind: KindDNS, Replica: "r", Err: &net.DNSError{Err: errNoAddressesFound, Name: "r", IsNotFound: true}}, want: true},

		{name: "dial i/o timeout", err: dialErr(&timeoutErr{})},
		{name: "connect budget ran out", err: dialErr(context.DeadlineExceeded)},
		{name: "connection reset during connect", err: dialErr(syscall.ECONNRESET)},
		{name: "dns timeout", err: &Error{Kind: KindDNS, Replica: "r", Err: &net.DNSError{Err: "i/o timeout", Name: "r", IsTimeout: true}}},
		{name: "dns server failure", err: &Error{Kind: KindDNS, Replica: "r", Err: &net.DNSError{Err: "server misbehaving", Name: "r", IsTemporary: true}}},
		{name: "dns other", err: &Error{Kind: KindDNS, Replica: "r", Err: &net.DNSError{Err: "lookup failed", Name: "r"}}},
		{name: "refused errno on a timeout kind", err: &Error{Kind: KindTimeout, Replica: "r", Err: syscall.ECONNREFUSED}},
		{name: "canceled", err: &Error{Kind: KindCanceled, Replica: "r", Err: context.Canceled}},
		{name: "other after send", err: &Error{Kind: KindOther, Replica: "r", Err: io.ErrUnexpectedEOF}},
		{name: "bare refused errno", err: syscall.ECONNREFUSED},
		{name: "nil", err: nil},
	}
	for _, tc := range tests {
		if got := IsHardConnectFailure(tc.err); got != tc.want {
			t.Errorf("%s: IsHardConnectFailure = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// timeoutErr is a net.Error timeout, as a dial that hit its deadline returns.
type timeoutErr struct{}

func (*timeoutErr) Error() string   { return "i/o timeout" }
func (*timeoutErr) Timeout() bool   { return true }
func (*timeoutErr) Temporary() bool { return true }

// TestHardConnectFailureFromDo classifies real Do failures.
func TestHardConnectFailureFromDo(t *testing.T) {
	t.Run("refused", func(t *testing.T) {
		dead := replicaFor(t, testnet.RefusedURL(t))
		c := New(testUpstream(nil))
		defer c.CloseIdleConnections()
		err := doGet(c, dead)
		if !Retryable(err) || !IsHardConnectFailure(err) {
			t.Fatalf("refused dial: Retryable %v, hard %v (%v); want both", Retryable(err), IsHardConnectFailure(err), err)
		}
	})
	t.Run("connect budget too short for a live replica", func(t *testing.T) {
		c := New(testUpstream(func(u *config.Upstream) { u.Timeouts.Connect = time.Nanosecond }))
		defer c.CloseIdleConnections()
		err := doGet(c, replicaFor(t, okServer(t).URL))
		if KindOf(err) != KindConnect || IsHardConnectFailure(err) {
			t.Fatalf("dial timeout: kind %s, hard %v (%v); want a connect failure that is not hard", KindOf(err), IsHardConnectFailure(err), err)
		}
	})
	t.Run("nxdomain", func(t *testing.T) {
		fl := &fakeLookup{}
		fl.set(nil, &net.DNSError{Err: "no such host", Name: "replica.test", IsNotFound: true})
		c := clientFor(fl, nil, nil)
		defer c.CloseIdleConnections()
		if err := doGet(c, namedReplica(t, okServer(t))); !IsHardConnectFailure(err) {
			t.Fatalf("NXDOMAIN: hard = false (%v)", err)
		}
	})
	t.Run("lookup failure without an answer", func(t *testing.T) {
		fl := &fakeLookup{}
		fl.set(nil, errors.New("server misbehaving"))
		c := clientFor(fl, nil, nil)
		defer c.CloseIdleConnections()
		err := doGet(c, namedReplica(t, okServer(t)))
		if KindOf(err) != KindDNS || IsHardConnectFailure(err) {
			t.Fatalf("failed lookup: kind %s, hard %v (%v); want a dns failure that is not hard", KindOf(err), IsHardConnectFailure(err), err)
		}
	})
}
