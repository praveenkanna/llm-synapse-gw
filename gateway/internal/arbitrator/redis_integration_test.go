//go:build integration

package arbitrator

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/redis"
)

// TestIntegrationRedisStore runs the demand script on a real Redis at
// REDIS_ADDR (optionally REDIS_PASSWORD), e.g. the dev compose stack's.
func TestIntegrationRedisStore(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}
	c := redis.New(redis.Options{Addr: addr, Password: os.Getenv("REDIS_PASSWORD")})
	t.Cleanup(func() { _ = c.Close() })
	prefix := "gw:adm-it:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	store := RedisStore{Client: c, Prefix: prefix}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _ = c.Do(ctx, "DEL", prefix+":{p}:members", prefix+":{p}:demand")
	})
	sync := func(member string, own int, lease time.Duration) (int, int) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		total, n, err := store.Sync(ctx, "p", member, own, lease)
		if err != nil {
			t.Fatalf("Sync(%s): %v", member, err)
		}
		return total, n
	}

	if total, n := sync("gw-a", 3, 300*time.Millisecond); total != 3 || n != 1 {
		t.Fatalf("first sync = %d over %d, want 3 over 1", total, n)
	}
	if total, n := sync("gw-b", 5, 10*time.Second); total != 8 || n != 2 {
		t.Fatalf("second sync = %d over %d, want 8 over 2", total, n)
	}
	if total, n := sync("gw-a", 1, 300*time.Millisecond); total != 6 || n != 2 {
		t.Fatalf("update = %d over %d, want 6 over 2", total, n)
	}
	time.Sleep(400 * time.Millisecond) // gw-a's lease lapses
	if total, n := sync("gw-b", 5, 10*time.Second); total != 5 || n != 1 {
		t.Fatalf("after gw-a's lease = %d over %d, want 5 over 1", total, n)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if v, err := c.Do(ctx, "PTTL", prefix+":{p}:demand"); err != nil || v.Int <= 0 {
		t.Fatalf("demand hash PTTL = %+v, %v; want a TTL", v, err)
	}
}
