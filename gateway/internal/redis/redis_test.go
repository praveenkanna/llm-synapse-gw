package redis

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/testnet"
)

// fakeReply tells the fake server how to answer one command.
type fakeReply struct {
	raw   string // bytes written verbatim
	hang  bool   // write nothing and keep the connection open
	close bool   // close the connection without replying
}

func ok() fakeReply { return fakeReply{raw: "+OK\r\n"} }

// fakeServer is an in-process RESP server on 127.0.0.1:0. It decodes each
// command with readValue, records it, and answers with handle.
type fakeServer struct {
	ln     net.Listener
	handle func(cmd []string) fakeReply

	mu       sync.Mutex
	cmds     [][]string
	conns    []net.Conn
	accepted int

	wg sync.WaitGroup
}

func newFakeServer(t *testing.T, handle func(cmd []string) fakeReply) *fakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &fakeServer{ln: ln, handle: handle}
	s.wg.Add(1)
	go s.acceptLoop()
	t.Cleanup(s.close)
	return s
}

func (s *fakeServer) addr() string { return s.ln.Addr().String() }

func (s *fakeServer) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.conns = append(s.conns, conn)
		s.accepted++
		s.mu.Unlock()
		s.wg.Add(1)
		go s.serve(conn)
	}
}

func (s *fakeServer) serve(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()
	br := bufio.NewReader(conn)
	for {
		v, err := readValue(br, 0)
		if err != nil || v.Kind != KindArray {
			return
		}
		cmd := make([]string, len(v.Array))
		for i, a := range v.Array {
			if a.Kind != KindString {
				return
			}
			cmd[i] = a.Str
		}
		s.mu.Lock()
		s.cmds = append(s.cmds, cmd)
		s.mu.Unlock()

		r := s.handle(cmd)
		switch {
		case r.close:
			return
		case r.hang:
			continue
		}
		if _, err := conn.Write([]byte(r.raw)); err != nil {
			return
		}
	}
}

func (s *fakeServer) close() {
	_ = s.ln.Close()
	s.mu.Lock()
	for _, c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *fakeServer) commands() [][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]string(nil), s.cmds...)
}

func (s *fakeServer) acceptedConns() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.accepted
}

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func newTestClient(t *testing.T, opts Options) *Client {
	t.Helper()
	c := New(opts)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// rawServer answers "RAW <key>" with replies[key] and anything else with +OK.
func rawServer(t *testing.T, replies map[string]string) *fakeServer {
	return newFakeServer(t, func(cmd []string) fakeReply {
		if len(cmd) == 2 && cmd[0] == "RAW" {
			return fakeReply{raw: replies[cmd[1]]}
		}
		return ok()
	})
}

func TestDoReplyParsing(t *testing.T) {
	nest := func(levels int) string {
		return strings.Repeat("*1\r\n", levels) + ":7\r\n"
	}
	nestedValue := func(levels int) Value {
		v := Value{Kind: KindInt, Int: 7}
		for range levels {
			v = Value{Kind: KindArray, Array: []Value{v}}
		}
		return v
	}
	tests := []struct {
		name string
		raw  string
		want Value
	}{
		{"simple string", "+PONG\r\n", Value{Kind: KindString, Str: "PONG"}},
		{"empty simple string", "+\r\n", Value{Kind: KindString}},
		{"bulk string", "$5\r\nhello\r\n", Value{Kind: KindString, Str: "hello", Bytes: []byte("hello")}},
		{"empty bulk string", "$0\r\n\r\n", Value{Kind: KindString, Bytes: []byte("")}},
		{"binary bulk string", "$4\r\na\r\nb\r\n", Value{Kind: KindString, Str: "a\r\nb", Bytes: []byte("a\r\nb")}},
		{"nil bulk string", "$-1\r\n", Value{Kind: KindNil}},
		{"integer", ":42\r\n", Value{Kind: KindInt, Int: 42}},
		{"negative integer", ":-3\r\n", Value{Kind: KindInt, Int: -3}},
		{"nil array", "*-1\r\n", Value{Kind: KindNil}},
		{"empty array", "*0\r\n", Value{Kind: KindArray, Array: []Value{}}},
		{
			"flat array of mixed kinds",
			"*4\r\n+a\r\n$1\r\nb\r\n:1\r\n$-1\r\n",
			Value{Kind: KindArray, Array: []Value{
				{Kind: KindString, Str: "a"},
				{Kind: KindString, Str: "b", Bytes: []byte("b")},
				{Kind: KindInt, Int: 1},
				{Kind: KindNil},
			}},
		},
		{
			"nested arrays with nil array inside",
			"*2\r\n*2\r\n:1\r\n:2\r\n*-1\r\n",
			Value{Kind: KindArray, Array: []Value{
				{Kind: KindArray, Array: []Value{{Kind: KindInt, Int: 1}, {Kind: KindInt, Int: 2}}},
				{Kind: KindNil},
			}},
		},
		{"nesting at the depth limit", nest(maxArrayDepth), nestedValue(maxArrayDepth)},
	}
	replies := make(map[string]string, len(tests))
	for _, tc := range tests {
		replies[tc.name] = tc.raw
	}
	srv := rawServer(t, replies)
	c := newTestClient(t, Options{Addr: srv.addr()})

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := c.Do(testCtx(t), "RAW", tc.name)
			if err != nil {
				t.Fatalf("Do: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
	if n := srv.acceptedConns(); n != 1 {
		t.Fatalf("valid replies used %d connections, want 1", n)
	}
}

func TestDoErrorReplyKeepsConnection(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		wantMsg string
	}{
		{"top-level error", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
			"WRONGTYPE Operation against a key holding the wrong kind of value"},
		{"error nested in array", "*3\r\n:1\r\n-ERR inner\r\n$2\r\nok\r\n", "ERR inner"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := rawServer(t, map[string]string{"x": tc.raw})
			c := newTestClient(t, Options{Addr: srv.addr()})

			_, err := c.Do(testCtx(t), "RAW", "x")
			var rerr *Error
			if !errors.As(err, &rerr) {
				t.Fatalf("err = %v, want *Error", err)
			}
			if rerr.Msg != tc.wantMsg {
				t.Fatalf("Msg = %q, want %q", rerr.Msg, tc.wantMsg)
			}
			got, err := c.Do(testCtx(t), "PING")
			if err != nil || got.Str != "OK" {
				t.Fatalf("follow-up Do = %+v, %v; want OK", got, err)
			}
			if n := srv.acceptedConns(); n != 1 {
				t.Fatalf("accepted %d connections, want 1 (error reply must keep the connection)", n)
			}
		})
	}
}

func TestDoProtocolErrorClosesAndRedials(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		// No body follows the oversized header: the client must reject the
		// length itself instead of waiting to read 16 MiB.
		{"oversized bulk length", "$16777217\r\n"},
		{"bulk length below -1", "$-2\r\n"},
		{"oversized array length", "*1048577\r\n"},
		{"nesting past the depth limit", strings.Repeat("*1\r\n", maxArrayDepth+1) + ":1\r\n"},
		{"unknown type byte", "?what\r\n"},
		{"non-numeric integer", ":abc\r\n"},
		{"bulk without trailing CRLF", "$2\r\nokXX"},
		{"line without CR", "+OK\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := rawServer(t, map[string]string{"bad": tc.raw})
			c := newTestClient(t, Options{Addr: srv.addr()})

			start := time.Now()
			_, err := c.Do(testCtx(t), "RAW", "bad")
			if !errors.Is(err, errProtocol) {
				t.Fatalf("err = %v, want protocol error", err)
			}
			if el := time.Since(start); el > 500*time.Millisecond {
				t.Fatalf("protocol error took %v; bound must be checked before reading", el)
			}
			got, err := c.Do(testCtx(t), "PING")
			if err != nil || got.Str != "OK" {
				t.Fatalf("Do after protocol error = %+v, %v; want OK", got, err)
			}
			if n := srv.acceptedConns(); n != 2 {
				t.Fatalf("accepted %d connections, want 2 (redial after protocol error)", n)
			}
		})
	}
}

func TestDoIOErrorThenRedial(t *testing.T) {
	var mu sync.Mutex
	dropped := false
	srv := newFakeServer(t, func(cmd []string) fakeReply {
		mu.Lock()
		defer mu.Unlock()
		if !dropped {
			dropped = true
			return fakeReply{close: true}
		}
		return fakeReply{raw: ":1\r\n"}
	})
	c := newTestClient(t, Options{Addr: srv.addr()})

	_, err := c.Do(testCtx(t), "INCR", "k")
	if err == nil {
		t.Fatal("Do on dropped connection succeeded, want error")
	}
	var rerr *Error
	if errors.As(err, &rerr) {
		t.Fatalf("err = %v, want I/O error, not a Redis error reply", err)
	}
	got, err := c.Do(testCtx(t), "INCR", "k")
	if err != nil || got.Int != 1 {
		t.Fatalf("Do after redial = %+v, %v; want 1", got, err)
	}
	if n := srv.acceptedConns(); n != 2 {
		t.Fatalf("accepted %d connections, want 2", n)
	}
}

func TestDoDialError(t *testing.T) {
	addr := testnet.RefusedAddr(t)

	c := newTestClient(t, Options{Addr: addr})
	if _, err := c.Do(testCtx(t), "PING"); err == nil {
		t.Fatal("Do against closed port succeeded, want dial error")
	}
}

func TestDoContextOnSilentServer(t *testing.T) {
	tests := []struct {
		name    string
		ctx     func() (context.Context, context.CancelFunc)
		wantErr []error // err must wrap at least one
	}{
		{
			name: "deadline exceeded",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 30*time.Millisecond)
			},
			wantErr: []error{context.DeadlineExceeded, os.ErrDeadlineExceeded},
		},
		{
			name: "cancelled without deadline",
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				time.AfterFunc(30*time.Millisecond, cancel)
				return ctx, cancel
			},
			wantErr: []error{context.Canceled},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			silent := true
			srv := newFakeServer(t, func(cmd []string) fakeReply {
				mu.Lock()
				defer mu.Unlock()
				if silent {
					silent = false
					return fakeReply{hang: true}
				}
				return ok()
			})
			c := newTestClient(t, Options{Addr: srv.addr()})

			ctx, cancel := tc.ctx()
			defer cancel()
			start := time.Now()
			_, err := c.Do(ctx, "GET", "k")
			elapsed := time.Since(start)
			if err == nil {
				t.Fatal("Do on silent server succeeded, want error")
			}
			if elapsed > 500*time.Millisecond {
				t.Fatalf("Do returned after %v, want well under 1s", elapsed)
			}
			matched := false
			for _, want := range tc.wantErr {
				matched = matched || errors.Is(err, want)
			}
			if !matched {
				t.Fatalf("err = %v, want one of %v", err, tc.wantErr)
			}

			got, err := c.Do(testCtx(t), "PING")
			if err != nil || got.Str != "OK" {
				t.Fatalf("Do after timeout = %+v, %v; want OK", got, err)
			}
			if n := srv.acceptedConns(); n != 2 {
				t.Fatalf("accepted %d connections, want 2 (timed-out connection must not be reused)", n)
			}
		})
	}
}

func TestDoExpiredContextSkipsNetwork(t *testing.T) {
	srv := rawServer(t, nil)
	c := newTestClient(t, Options{Addr: srv.addr()})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Do(ctx, "PING"); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if n := srv.acceptedConns(); n != 0 {
		t.Fatalf("accepted %d connections, want 0", n)
	}
}

func TestEvalFallsBackToEvalOnNoScript(t *testing.T) {
	const src = "return redis.call('INCRBY', KEYS[1], ARGV[1])"
	var mu sync.Mutex
	loaded := map[string]bool{}
	srv := newFakeServer(t, func(cmd []string) fakeReply {
		mu.Lock()
		defer mu.Unlock()
		switch cmd[0] {
		case "EVALSHA":
			if !loaded[cmd[1]] {
				return fakeReply{raw: "-NOSCRIPT No matching script. Please use EVAL.\r\n"}
			}
			return fakeReply{raw: ":5\r\n"}
		case "EVAL":
			sum := sha1.Sum([]byte(cmd[1]))
			loaded[hex.EncodeToString(sum[:])] = true
			return fakeReply{raw: ":5\r\n"}
		}
		return fakeReply{raw: "-ERR unexpected\r\n"}
	})
	c := newTestClient(t, Options{Addr: srv.addr()})
	s := NewScript(src)

	for i := range 2 {
		got, err := c.Eval(testCtx(t), s, []string{"counter"}, "5")
		if err != nil || got.Int != 5 {
			t.Fatalf("Eval #%d = %+v, %v; want 5", i+1, got, err)
		}
	}

	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])
	want := [][]string{
		{"EVALSHA", sha, "1", "counter", "5"},
		{"EVAL", src, "1", "counter", "5"},
		{"EVALSHA", sha, "1", "counter", "5"},
	}
	if got := srv.commands(); !reflect.DeepEqual(got, want) {
		t.Fatalf("commands = %q, want %q", got, want)
	}
}

func TestEvalPassesThroughOtherErrors(t *testing.T) {
	srv := newFakeServer(t, func(cmd []string) fakeReply {
		return fakeReply{raw: "-ERR user_script:1: boom\r\n"}
	})
	c := newTestClient(t, Options{Addr: srv.addr()})

	_, err := c.Eval(testCtx(t), NewScript("error('boom')"), nil)
	var rerr *Error
	if !errors.As(err, &rerr) || !strings.HasPrefix(rerr.Msg, "ERR") {
		t.Fatalf("err = %v, want *Error ERR", err)
	}
	if n := len(srv.commands()); n != 1 {
		t.Fatalf("sent %d commands, want 1 (no EVAL fallback for non-NOSCRIPT errors)", n)
	}
}

func TestConnectHandshake(t *testing.T) {
	tests := []struct {
		name string
		opts Options
		want [][]string // commands before PING
	}{
		{"no auth no db", Options{}, nil},
		{"password only", Options{Password: "pw"}, [][]string{{"AUTH", "pw"}}},
		{"user and password", Options{Username: "gw", Password: "pw"}, [][]string{{"AUTH", "gw", "pw"}}},
		{"username without password is ignored", Options{Username: "gw"}, nil},
		{"select db", Options{DB: 3}, [][]string{{"SELECT", "3"}}},
		{"auth then select", Options{Username: "gw", Password: "pw", DB: 2},
			[][]string{{"AUTH", "gw", "pw"}, {"SELECT", "2"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := newFakeServer(t, func(cmd []string) fakeReply { return ok() })
			opts := tc.opts
			opts.Addr = srv.addr()
			c := newTestClient(t, opts)

			if _, err := c.Do(testCtx(t), "PING"); err != nil {
				t.Fatalf("Do: %v", err)
			}
			if _, err := c.Do(testCtx(t), "PING"); err != nil {
				t.Fatalf("second Do: %v", err)
			}
			want := append(append([][]string(nil), tc.want...), []string{"PING"}, []string{"PING"})
			if got := srv.commands(); !reflect.DeepEqual(got, want) {
				t.Fatalf("commands = %q, want %q", got, want)
			}
		})
	}
}

func TestConnectAuthFailureClosesConnection(t *testing.T) {
	srv := newFakeServer(t, func(cmd []string) fakeReply {
		if cmd[0] == "AUTH" {
			return fakeReply{raw: "-WRONGPASS invalid username-password pair\r\n"}
		}
		return ok()
	})
	c := newTestClient(t, Options{Addr: srv.addr(), Password: "secret"})

	for i := range 2 {
		_, err := c.Do(testCtx(t), "PING")
		var rerr *Error
		if !errors.As(err, &rerr) || !strings.HasPrefix(rerr.Msg, "WRONGPASS") {
			t.Fatalf("Do #%d err = %v, want WRONGPASS", i+1, err)
		}
		if strings.Contains(err.Error(), "secret") {
			t.Fatalf("error %q leaks the password", err)
		}
	}
	if n := srv.acceptedConns(); n != 2 {
		t.Fatalf("accepted %d connections, want 2 (failed handshake must not keep the connection)", n)
	}
	for _, cmd := range srv.commands() {
		if cmd[0] == "PING" {
			t.Fatal("PING sent on an unauthenticated connection")
		}
	}
}

func TestClose(t *testing.T) {
	srv := newFakeServer(t, func(cmd []string) fakeReply { return ok() })
	c := New(Options{Addr: srv.addr()})
	if _, err := c.Do(testCtx(t), "PING"); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := c.Do(testCtx(t), "PING"); !errors.Is(err, errClosed) {
		t.Fatalf("Do after Close err = %v, want errClosed", err)
	}
	if err := New(Options{Addr: srv.addr()}).Close(); err != nil {
		t.Fatalf("Close on never-dialed client: %v", err)
	}
}

func TestDoEmptyCommand(t *testing.T) {
	c := newTestClient(t, Options{Addr: "127.0.0.1:1"})
	if _, err := c.Do(testCtx(t)); err == nil {
		t.Fatal("Do with no args succeeded, want error")
	}
}

func TestDoConcurrentCallsStayInSync(t *testing.T) {
	srv := newFakeServer(t, func(cmd []string) fakeReply {
		return fakeReply{raw: "$" + strconv.Itoa(len(cmd[1])) + "\r\n" + cmd[1] + "\r\n"}
	})
	c := newTestClient(t, Options{Addr: srv.addr()})

	const workers, calls = 8, 50
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := range workers {
		wg.Go(func() {
			for i := range calls {
				msg := strconv.Itoa(w) + "-" + strconv.Itoa(i)
				got, err := c.Do(context.Background(), "ECHO", msg)
				if err != nil {
					errs <- err
					return
				}
				if got.Str != msg {
					errs <- errors.New("reply " + got.Str + " for request " + msg)
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if n := srv.acceptedConns(); n != 1 {
		t.Fatalf("accepted %d connections, want 1", n)
	}
}
