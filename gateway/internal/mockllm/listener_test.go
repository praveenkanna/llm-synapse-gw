package mockllm

import (
	"errors"
	"net"
	"syscall"
	"testing"
	"time"
)

const dialTimeout = 2 * time.Second

func TestRefusableListener(t *testing.T) {
	ln, err := newRefusableListener("127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	accepted := make(chan net.Conn, 4)
	acceptErr := make(chan error, 1)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				acceptErr <- err
				return
			}
			accepted <- c
		}
	}()
	addr := ln.Addr().String()

	dialOK := func() {
		t.Helper()
		c, err := net.DialTimeout("tcp", addr, dialTimeout)
		if err != nil {
			t.Fatalf("dial while listening: %v", err)
		}
		_ = c.Close()
		select {
		case sc := <-accepted:
			_ = sc.Close()
		case <-time.After(dialTimeout):
			t.Fatal("connection not accepted")
		}
	}

	dialOK()
	if err := ln.SetRefusing(true); err != nil {
		t.Fatalf("SetRefusing(true): %v", err)
	}
	if !ln.Refusing() {
		t.Fatal("Refusing() = false after SetRefusing(true)")
	}
	if err := ln.SetRefusing(true); err != nil { // idempotent
		t.Fatalf("second SetRefusing(true): %v", err)
	}
	_, err = net.DialTimeout("tcp", addr, dialTimeout)
	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("dial while refusing = %v, want ECONNREFUSED", err)
	}
	if err := ln.SetRefusing(false); err != nil {
		t.Fatalf("SetRefusing(false): %v", err)
	}
	dialOK()

	if err := ln.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-acceptErr:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("Accept after Close = %v, want net.ErrClosed", err)
		}
	case <-time.After(dialTimeout):
		t.Fatal("Accept did not return after Close")
	}
	if err := ln.SetRefusing(false); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("SetRefusing after Close = %v, want net.ErrClosed", err)
	}
}

func TestRefusableListenerCloseWhileRefusing(t *testing.T) {
	ln, err := newRefusableListener("127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	if err := ln.SetRefusing(true); err != nil {
		t.Fatalf("SetRefusing: %v", err)
	}
	done := make(chan error, 1)
	go func() { _, err := ln.Accept(); done <- err }()
	time.Sleep(20 * time.Millisecond) // let Accept block on the refusing state
	_ = ln.Close()
	select {
	case err := <-done:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("Accept = %v, want net.ErrClosed", err)
		}
	case <-time.After(dialTimeout):
		t.Fatal("Accept blocked after Close while refusing")
	}
}
