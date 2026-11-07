// Package respcache stores finished responses of deterministic requests in
// Redis and serves them back (ADR 0019).
//
// Nothing here may slow or fail a request. A lookup has a deadline and never
// queues for a connection: Redis down, slow, busy or returning garbage is a
// miss with a reason, and the request goes on to the replicas. A store is
// handed to a bounded queue and written by background workers; a full queue
// drops it. Every entry has a TTL, and Redis's own maxmemory is the memory
// ceiling. The package never logs a key or a response.
package respcache

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/redis"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// Reasons a lookup failed open, for gateway_failopen_total{stage="cache"}.
const (
	ReasonBusy    = "pool_busy"     // every lookup connection was in use
	ReasonTimeout = "timeout"       // Redis did not answer within cache.timeout
	ReasonCorrupt = "corrupt_entry" // the stored value is not an entry this gateway wrote
	ReasonClosed  = "closed"
)

// Outcomes of a store, for gateway_cache_stores_total.
const (
	StoreQueued  = "queued"  // accepted by the writer; its result comes later
	StoreStored  = "stored"  // Redis accepted it
	StoreDropped = "dropped" // the write queue was full
	StoreFailed  = "failed"  // Redis refused it or did not answer in time
	StoreSkipped = "skipped" // not storable: not a whole success, too large, no usable TTL
)

// Config configures a Cache with the cache section, validated by config,
// which owns its defaults and bounds (ADR 0022): Redis to reach; a lookup
// pool of Connections, plus a connection per WriteWorkers for the
// background writer; Timeout per lookup or store; MaxEntryBytes, the largest
// body stored; WriteQueue stores waiting for a worker; KeyPrefix in front
// of every Redis key. Enabled and MaxTTL are read by the caller.
type Config struct {
	config.Cache
	// OnStore, if set, is told the result of each queued store (stored or
	// failed) from a worker goroutine.
	OnStore func(outcome string)
}

// Entry is a stored response: its status, Content-Type and body.
type Entry struct {
	Status      int
	ContentType string
	Body        []byte
}

// Error is a failed lookup; Reason is one of the Reason constants.
type Error struct {
	Reason string
	Err    error
}

func (e *Error) Error() string { return "respcache: " + e.Reason + ": " + e.Err.Error() }
func (e *Error) Unwrap() error { return e.Err }

// Cache is safe for concurrent use.
type Cache struct {
	cfg    Config
	lookup chan *redis.Client // idle lookup connections
	all    []*redis.Client

	queue chan store
	wg    sync.WaitGroup

	mu     sync.Mutex
	closed bool
}

type store struct {
	key string
	ttl time.Duration
	val string
}

// New returns a Cache and starts its writer. It does not dial.
func New(cfg Config) *Cache {
	c := &Cache{cfg: cfg, lookup: make(chan *redis.Client, cfg.Connections), queue: make(chan store, cfg.WriteQueue)}
	dial := cfg.Timeout
	mk := func() *redis.Client {
		cl := redis.New(redis.Options{Addr: cfg.Redis.Addr, Username: cfg.Redis.Username, Password: cfg.Redis.Password.Reveal(), DB: cfg.Redis.DB, DialTimeout: dial})
		c.all = append(c.all, cl)
		return cl
	}
	for range cfg.Connections {
		c.lookup <- mk()
	}
	for range cfg.WriteWorkers {
		cl := mk()
		c.wg.Add(1)
		go c.write(cl)
	}
	return c
}

// Close stops the writer after it drains its queue (bounded by Timeout per
// store) and closes every connection.
func (c *Cache) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	close(c.queue)
	c.mu.Unlock()
	c.wg.Wait()
	for _, cl := range c.all {
		_ = cl.Close()
	}
}

func (c *Cache) redisKey(key string) string { return c.cfg.KeyPrefix + ":" + key }

// Get looks key up within the configured timeout. A miss is (zero, false,
// nil). Any error is an *Error carrying the reason the lookup failed open.
func (c *Cache) Get(ctx context.Context, key string) (Entry, bool, error) {
	var cl *redis.Client
	select {
	case cl = <-c.lookup:
		defer func() { c.lookup <- cl }()
	default:
		return Entry{}, false, &Error{Reason: ReasonBusy, Err: errors.New("every lookup connection is in use")}
	}
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	v, err := cl.Do(ctx, "GET", c.redisKey(key))
	if err != nil {
		return Entry{}, false, &Error{Reason: reasonOf(err), Err: err}
	}
	switch v.Kind {
	case redis.KindNil:
		return Entry{}, false, nil
	case redis.KindString:
		raw := v.Bytes
		if raw == nil {
			raw = []byte(v.Str)
		}
		e, derr := decode(raw)
		if derr != nil {
			return Entry{}, false, &Error{Reason: ReasonCorrupt, Err: derr}
		}
		return e, true, nil
	default:
		return Entry{}, false, &Error{Reason: ReasonCorrupt, Err: errors.New("unexpected reply type")}
	}
}

func reasonOf(err error) string {
	var ne net.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &ne) && ne.Timeout():
		return ReasonTimeout
	default:
		return telemetry.ReasonRedisError
	}
}

// Put hands a response to the background writer for ttl, rounded down to
// whole seconds (Redis EX). It never blocks. The result is StoreQueued (the
// final outcome arrives through OnStore), StoreDropped for a full queue or
// StoreSkipped when the entry is too large or ttl is under a second. Which
// responses may be stored is the caller's decision (package dedup).
func (c *Cache) Put(key string, ttl time.Duration, e Entry) string {
	if ttl < time.Second || len(e.Body) > c.cfg.MaxEntryBytes {
		return StoreSkipped
	}
	encoded := encode(e)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return StoreDropped
	}
	select {
	case c.queue <- store{key: c.redisKey(key), ttl: ttl, val: encoded}:
		return StoreQueued
	default:
		return StoreDropped
	}
}

func (c *Cache) write(cl *redis.Client) {
	defer c.wg.Done()
	for s := range c.queue {
		ctx, cancel := context.WithTimeout(context.Background(), c.cfg.Timeout)
		_, err := cl.Do(ctx, "SET", s.key, s.val, "EX", fmt.Sprint(int64(s.ttl/time.Second)))
		cancel()
		if c.cfg.OnStore != nil {
			if err != nil {
				c.cfg.OnStore(StoreFailed)
			} else {
				c.cfg.OnStore(StoreStored)
			}
		}
	}
}

// Entry layout: "GWC1", the status as 2 bytes, the Content-Type's length as
// a uvarint, the Content-Type, then the body. decode checks all of it: a
// value this gateway did not write is a corrupt entry, never a response.
const magic = "GWC1"

func encode(e Entry) string {
	b := make([]byte, 0, len(magic)+2+binary.MaxVarintLen64+len(e.ContentType)+len(e.Body))
	b = append(b, magic...)
	b = binary.BigEndian.AppendUint16(b, uint16(e.Status))
	b = binary.AppendUvarint(b, uint64(len(e.ContentType)))
	b = append(b, e.ContentType...)
	b = append(b, e.Body...)
	return string(b)
}

func decode(raw []byte) (Entry, error) {
	if len(raw) < len(magic)+3 || string(raw[:len(magic)]) != magic {
		return Entry{}, errors.New("bad header")
	}
	rest := raw[len(magic):]
	status := int(binary.BigEndian.Uint16(rest[:2]))
	if status != 200 { // the format: only a success is ever written (dedup stores no other)
		return Entry{}, errors.New("bad status")
	}
	n, k := binary.Uvarint(rest[2:])
	if k <= 0 || n > uint64(len(rest)-2-k) {
		return Entry{}, errors.New("bad content type length")
	}
	ct := string(rest[2+k : 2+k+int(n)])
	return Entry{Status: status, ContentType: ct, Body: rest[2+k+int(n):]}, nil
}
