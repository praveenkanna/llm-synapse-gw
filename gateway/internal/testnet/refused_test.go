package testnet_test

import (
	"errors"
	"net"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/testnet"
)

// freePortTries is how many free ports the test asks the kernel for: none
// of them may be RefusedAddr's port.
const freePortTries = 500

func TestRefusedAddrRefusesConnections(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 3; i++ { // a pool of dead replicas needs one address each
		addr := testnet.RefusedAddr(t)
		if seen[addr] {
			t.Fatalf("call %d returned %s again, want a distinct address per call", i+1, addr)
		}
		seen[addr] = true
		c, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err == nil {
			_ = c.Close()
			t.Fatalf("dial %s succeeded, want connection refused", addr)
		}
		if !errors.Is(err, syscall.ECONNREFUSED) {
			t.Fatalf("dial %s: %v, want connection refused", addr, err)
		}
	}
}

// A port the kernel can hand to bind(127.0.0.1:0) can be taken by a server
// the test starts next; a port below 1024 never is.
func TestRefusedAddrIsNeverAFreePort(t *testing.T) {
	addr := testnet.RefusedAddr(t)
	_, p, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(p)
	if err != nil {
		t.Fatal(err)
	}
	if port <= 0 || port >= 1024 {
		t.Fatalf("RefusedAddr = %s: port %d is in the range free ports come from, want 1..1023", addr, port)
	}
	for i := 0; i < freePortTries; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		got := ln.Addr().String()
		_ = ln.Close()
		if got == addr {
			t.Fatalf("free port %d of %d was the refused address %s", i+1, freePortTries, addr)
		}
	}
}

func TestRefusedURL(t *testing.T) {
	u := testnet.RefusedURL(t)
	if !strings.HasPrefix(u, "http://127.0.0.1:") {
		t.Fatalf("RefusedURL = %q, want http://127.0.0.1:<port>", u)
	}
}
