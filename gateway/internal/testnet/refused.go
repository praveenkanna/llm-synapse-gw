// Package testnet holds network helpers shared by tests. It is imported only
// by tests.
package testnet

import (
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// lowPorts is the count of ports below 1024. bind(127.0.0.1:0) only hands
// out ports from the ephemeral range (1024 and up), so no server a test
// starts can be given one of them.
const lowPorts = 1023

// refusedCheckTimeout bounds the dial that confirms nothing listens on a port.
const refusedCheckTimeout = 500 * time.Millisecond

// lastLowPort is the low port RefusedAddr handed out last, shared by every
// test in the binary so consecutive calls get distinct addresses.
var lastLowPort atomic.Uint32

// RefusedAddr returns a loopback host:port where connecting is refused for the
// whole test, distinct from the addresses earlier calls returned (until the
// low ports wrap around). Opening a listener on port 0 and closing it again
// leaves an ephemeral port that the next server the test starts (or a
// parallel test) can be handed, and then the "refused" address answers. Low
// ports that something listens on (sshd, CUPS) are skipped.
func RefusedAddr(t testing.TB) string {
	t.Helper()
	for i := 0; i < lowPorts; i++ {
		addr := fmt.Sprintf("127.0.0.1:%d", lastLowPort.Add(1)%lowPorts+1)
		c, err := net.DialTimeout("tcp", addr, refusedCheckTimeout)
		if err == nil {
			_ = c.Close()
			continue
		}
		if errors.Is(err, syscall.ECONNREFUSED) {
			return addr
		}
	}
	t.Fatalf("no loopback port below 1024 refuses connections")
	return ""
}

// RefusedURL is RefusedAddr as an http:// URL.
func RefusedURL(t testing.TB) string {
	t.Helper()
	return "http://" + RefusedAddr(t)
}
