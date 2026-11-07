package respcache

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
)

// fakeRedis speaks enough RESP2 for GET and SET ... EX, and records the
// AUTH and SELECT a connection opens with.
type fakeRedis struct {
	ln         net.Listener
	mu         sync.Mutex
	data       map[string]string
	ttl        map[string]int
	handshakes []string      // "AUTH user pass", "SELECT db"
	delay      time.Duration // before each reply
	block      chan struct{} // when non-nil, replies wait for it to close
}

func newFake(t *testing.T) *fakeRedis {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeRedis{ln: ln, data: map[string]string{}, ttl: map[string]int{}}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serve(c)
		}
	}()
	return f
}

func (f *fakeRedis) addr() string { return f.ln.Addr().String() }

func (f *fakeRedis) serve(c net.Conn) {
	defer func() { _ = c.Close() }()
	br := bufio.NewReader(c)
	for {
		args, err := readCmd(br)
		if err != nil {
			return
		}
		f.mu.Lock()
		delay, block := f.delay, f.block
		f.mu.Unlock()
		if block != nil {
			<-block
		}
		time.Sleep(delay)
		var reply string
		f.mu.Lock()
		switch strings.ToUpper(args[0]) {
		case "GET":
			if v, ok := f.data[args[1]]; ok {
				reply = fmt.Sprintf("$%d\r\n%s\r\n", len(v), v)
			} else {
				reply = "$-1\r\n"
			}
		case "SET":
			f.data[args[1]] = args[2]
			if len(args) == 5 && strings.EqualFold(args[3], "EX") {
				f.ttl[args[1]], _ = strconv.Atoi(args[4])
			}
			reply = "+OK\r\n"
		case "AUTH", "SELECT":
			f.handshakes = append(f.handshakes, strings.Join(args, " "))
			reply = "+OK\r\n"
		default:
			reply = "-ERR unknown command\r\n"
		}
		f.mu.Unlock()
		if _, err := io.WriteString(c, reply); err != nil {
			return
		}
	}
}

func readCmd(br *bufio.Reader) ([]string, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return nil, err
	}
	n, _ := strconv.Atoi(strings.TrimSpace(line[1:]))
	args := make([]string, 0, n)
	for range n {
		l, err := br.ReadString('\n')
		if err != nil {
			return nil, err
		}
		size, _ := strconv.Atoi(strings.TrimSpace(l[1:]))
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(br, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:size]))
	}
	return args, nil
}

func (f *fakeRedis) stored(key string) (string, int, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.data[key]
	return v, f.ttl[key], ok
}

func testConfig(addr string) Config {
	return Config{Cache: config.Cache{Redis: config.CacheRedis{Addr: addr}, Connections: 2, Timeout: 300 * time.Millisecond,
		MaxEntryBytes: 1 << 10, WriteQueue: 4, WriteWorkers: 1, KeyPrefix: "gw:cache"}}
}

// TestCredentialsReachRedis: the cache section's Redis user, password and
// database are what a connection opens with.
func TestCredentialsReachRedis(t *testing.T) {
	f := newFake(t)
	cfg := testConfig(f.addr())
	cfg.Redis.Username, cfg.Redis.Password, cfg.Redis.DB = "u", config.Secret("p"), 3
	c := New(cfg)
	defer c.Close()
	if _, _, err := c.Get(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	got := strings.Join(f.handshakes, ",")
	f.mu.Unlock()
	if !strings.Contains(got, "AUTH u p") || !strings.Contains(got, "SELECT 3") {
		t.Fatalf("handshakes %q, want AUTH u p and SELECT 3", got)
	}
}

func TestPutThenGetRoundTrips(t *testing.T) {
	f := newFake(t)
	stored := make(chan string, 1)
	cfg := testConfig(f.addr())
	cfg.OnStore = func(o string) { stored <- o }
	c := New(cfg)
	defer c.Close()

	e := Entry{Status: 200, ContentType: "text/event-stream", Body: []byte("data: {\"id\":\"x\"}\n\ndata: [DONE]\n\n")}
	if got := c.Put("k1", 90*time.Second+500*time.Millisecond, e); got != StoreQueued {
		t.Fatalf("Put = %s", got)
	}
	if o := <-stored; o != StoreStored {
		t.Fatalf("store outcome %s", o)
	}
	if _, ttl, ok := f.stored("gw:cache:k1"); !ok || ttl != 90 {
		t.Fatalf("stored under the prefix with TTL %d (ok=%v), want 90: every entry needs a TTL", ttl, ok)
	}
	got, hit, err := c.Get(context.Background(), "k1")
	if err != nil || !hit {
		t.Fatalf("Get: hit=%v err=%v", hit, err)
	}
	if got.Status != e.Status || got.ContentType != e.ContentType || string(got.Body) != string(e.Body) {
		t.Fatalf("entry changed in storage: %+v", got)
	}
}

func TestMissIsNotAnError(t *testing.T) {
	f := newFake(t)
	c := New(testConfig(f.addr()))
	defer c.Close()
	if _, hit, err := c.Get(context.Background(), "absent"); hit || err != nil {
		t.Fatalf("hit=%v err=%v, want a plain miss", hit, err)
	}
}

func reasonOfErr(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.Reason
	}
	return ""
}

func TestFailuresFailOpenWithAReason(t *testing.T) {
	t.Run("redis down", func(t *testing.T) {
		f := newFake(t)
		addr := f.addr()
		_ = f.ln.Close()
		c := New(testConfig(addr))
		defer c.Close()
		_, hit, err := c.Get(context.Background(), "k")
		if hit || err == nil || reasonOfErr(err) == "" {
			t.Fatalf("hit=%v err=%v", hit, err)
		}
	})
	t.Run("slow", func(t *testing.T) {
		f := newFake(t)
		f.delay = time.Second
		c := New(testConfig(f.addr()))
		defer c.Close()
		start := time.Now()
		_, _, err := c.Get(context.Background(), "k")
		if reasonOfErr(err) != ReasonTimeout {
			t.Fatalf("err = %v, want reason %s", err, ReasonTimeout)
		}
		if d := time.Since(start); d > time.Second {
			t.Fatalf("lookup took %v against a 300ms timeout", d)
		}
	})
	t.Run("garbage entry", func(t *testing.T) {
		f := newFake(t)
		f.data["gw:cache:k"] = "not an entry"
		c := New(testConfig(f.addr()))
		defer c.Close()
		if _, hit, err := c.Get(context.Background(), "k"); hit || reasonOfErr(err) != ReasonCorrupt {
			t.Fatalf("hit=%v err=%v, want %s", hit, err, ReasonCorrupt)
		}
	})
	t.Run("busy", func(t *testing.T) {
		f := newFake(t)
		f.block = make(chan struct{})
		cfg := testConfig(f.addr())
		cfg.Timeout = 2 * time.Second
		c := New(cfg)
		defer c.Close()
		var wg sync.WaitGroup
		for range cfg.Connections {
			wg.Add(1)
			go func() { defer wg.Done(); _, _, _ = c.Get(context.Background(), "k") }()
		}
		time.Sleep(100 * time.Millisecond) // both connections are inside Do
		start := time.Now()
		_, _, err := c.Get(context.Background(), "k")
		if reasonOfErr(err) != ReasonBusy {
			t.Fatalf("err = %v, want %s", err, ReasonBusy)
		}
		if d := time.Since(start); d > 50*time.Millisecond {
			t.Fatalf("a busy pool made the lookup wait %v", d)
		}
		close(f.block)
		wg.Wait()
	})
}

func TestEveryEntryHasABoundAndATTL(t *testing.T) {
	f := newFake(t)
	c := New(testConfig(f.addr()))
	defer c.Close()
	big := Entry{Status: 200, Body: make([]byte, 2<<10)}
	if got := c.Put("big", time.Minute, big); got != StoreSkipped {
		t.Fatalf("an entry over max_entry_bytes: %s", got)
	}
	if got := c.Put("short", 500*time.Millisecond, Entry{Status: 200}); got != StoreSkipped {
		t.Fatalf("a TTL under a second: %s", got)
	}
}

func TestFullWriteQueueDropsTheStore(t *testing.T) {
	f := newFake(t)
	f.block = make(chan struct{})
	cfg := testConfig(f.addr())
	cfg.Timeout = 5 * time.Second
	c := New(cfg)
	e := Entry{Status: 200, ContentType: "application/json", Body: []byte("{}")}
	// One store is held by the worker, WriteQueue wait; the rest drop.
	var queued, dropped int
	for i := range 20 {
		switch c.Put(fmt.Sprint("k", i), time.Minute, e) {
		case StoreQueued:
			queued++
		case StoreDropped:
			dropped++
		}
		if i == 0 {
			time.Sleep(50 * time.Millisecond) // let the worker take the first
		}
	}
	if want := cfg.WriteQueue + 1; queued != want || dropped != 20-want {
		t.Fatalf("queued %d dropped %d, want %d queued", queued, dropped, want)
	}
	close(f.block)
	c.Close()
	if _, _, ok := f.stored("gw:cache:k0"); !ok {
		t.Fatal("a queued store was lost on Close")
	}
	if got := c.Put("late", time.Minute, e); got != StoreDropped {
		t.Fatalf("Put after Close = %s", got)
	}
}

func TestDecodeRejectsWhatItDidNotWrite(t *testing.T) {
	good := encode(Entry{Status: 200, ContentType: "application/json", Body: []byte("{}")})
	for name, v := range map[string]string{
		"empty":        "",
		"wrong magic":  "XXXX" + good[4:],
		"truncated":    good[:6],
		"bad status":   good[:4] + "\x00\x01" + good[6:],
		"not a 200":    good[:4] + "\x01\xf4" + good[6:], // 500
		"ct too long":  good[:6] + "\x7f" + good[7:],
		"only magic":   magic,
		"status 0x700": good[:4] + "\x07\x00" + good[6:],
	} {
		if _, err := decode([]byte(v)); err == nil {
			t.Errorf("%s: decoded", name)
		}
	}
	if _, err := decode([]byte(good)); err != nil {
		t.Fatalf("good entry: %v", err)
	}
}
