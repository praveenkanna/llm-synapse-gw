//go:build integration

package redis_test

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/redis"
)

// TestIntegrationRealRedis runs against a real Redis at REDIS_ADDR
// (optionally REDIS_PASSWORD), e.g. the dev compose stack's Redis.
func TestIntegrationRealRedis(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}
	c := redis.New(redis.Options{Addr: addr, Password: os.Getenv("REDIS_PASSWORD")})
	t.Cleanup(func() { _ = c.Close() })

	do := func(args ...string) (redis.Value, error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		return c.Do(ctx, args...)
	}
	prefix := "gateway-redis-it:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	str, counter := prefix+":str", prefix+":counter"
	t.Cleanup(func() { _, _ = do("DEL", str, counter) })

	if v, err := do("SET", str, "hello", "PX", "60000"); err != nil || v.Str != "OK" {
		t.Fatalf("SET = %+v, %v", v, err)
	}
	if v, err := do("GET", str); err != nil || v.Kind != redis.KindString || v.Str != "hello" {
		t.Fatalf("GET = %+v, %v", v, err)
	}
	if v, err := do("GET", prefix+":missing"); err != nil || v.Kind != redis.KindNil {
		t.Fatalf("GET missing = %+v, %v", v, err)
	}

	_, err := do("INCR", str)
	var rerr *redis.Error
	if !errors.As(err, &rerr) || !strings.HasPrefix(rerr.Msg, "ERR") {
		t.Fatalf("INCR on string err = %v, want *redis.Error ERR", err)
	}
	if v, err := do("PING"); err != nil || v.Str != "PONG" {
		t.Fatalf("PING after error reply = %+v, %v", v, err)
	}

	script := redis.NewScript(`
local n = redis.call('INCRBY', KEYS[1], ARGV[1])
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return {n, redis.call('PTTL', KEYS[1]) > 0 and 1 or 0}`)
	for i, want := range []int64{5, 10} {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		v, err := c.Eval(ctx, script, []string{counter}, "5", "60000")
		cancel()
		if err != nil {
			t.Fatalf("Eval #%d: %v", i+1, err)
		}
		if v.Kind != redis.KindArray || len(v.Array) != 2 || v.Array[0].Int != want || v.Array[1].Int != 1 {
			t.Fatalf("Eval #%d = %+v, want [%d 1]", i+1, v, want)
		}
	}
}
