package pgstore

import (
	"context"
	"errors"
	"io/fs"
	"net"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/testnet"
)

const testPassword = "s3cret-pw-do-not-leak"

func TestOpenMalformedDSNHidesPassword(t *testing.T) {
	for _, dsn := range []string{
		"postgres://gateway:" + testPassword + "@db:notaport/gateway",
		"postgres://gateway:" + testPassword + "@[::1/gateway",
		"host=db password=" + testPassword + " port=abc",
		"host=db password='" + testPassword,
		"postgres://gateway:" + testPassword + "@db/gateway?connect_timeout=soon",
	} {
		s, err := Open(context.Background(), dsn, Options{})
		if err == nil {
			s.Close()
			t.Errorf("Open(%q): want error", dsn)
			continue
		}
		if strings.Contains(err.Error(), testPassword) {
			t.Errorf("Open(%q) error leaks the password: %v", dsn, err)
		}
		if !errors.Is(err, errMalformedDSN) {
			t.Errorf("Open(%q) err = %v, want errMalformedDSN", dsn, err)
		}
	}
	if _, err := Open(context.Background(), "", Options{}); !errors.Is(err, errEmptyDSN) {
		t.Errorf("Open(\"\") err = %v, want errEmptyDSN", err)
	}
}

func TestOptionsDefaults(t *testing.T) {
	s, err := Open(context.Background(), "postgres://gateway:"+testPassword+"@127.0.0.1:1/gateway", Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	cfg := s.pool.Config()
	if s.queryTimeout != defaultQueryTimeout || cfg.MaxConns != defaultMaxConns ||
		cfg.ConnConfig.ConnectTimeout != defaultConnectTimeout || cfg.MinConns != 0 {
		t.Fatalf("defaults: query %s, max conns %d, connect %s, min conns %d",
			s.queryTimeout, cfg.MaxConns, cfg.ConnConfig.ConnectTimeout, cfg.MinConns)
	}
	s2, err := Open(context.Background(), "postgres://gateway@127.0.0.1:1/gateway",
		Options{ConnectTimeout: time.Second, QueryTimeout: 3 * time.Second, MaxConns: 7, SearchPath: "t_x"})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	cfg = s2.pool.Config()
	if s2.queryTimeout != 3*time.Second || cfg.MaxConns != 7 || cfg.ConnConfig.ConnectTimeout != time.Second ||
		cfg.ConnConfig.RuntimeParams["search_path"] != "t_x" {
		t.Fatalf("options not applied: %+v", cfg)
	}
}

// refusedAddr returns a local address with nothing listening on it (see
// testnet.RefusedAddr).
func refusedAddr(t *testing.T) string {
	t.Helper()
	return testnet.RefusedAddr(t)
}

// silentAddr returns a local address that accepts connections and never
// answers, like a hung server.
func silentAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		<-done
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
	})
	return ln.Addr().String()
}

func TestOpenWithPostgresDownIsLazy(t *testing.T) {
	dsn := "postgres://gateway:" + testPassword + "@" + refusedAddr(t) + "/gateway?sslmode=disable"
	start := time.Now()
	s, err := Open(context.Background(), dsn, Options{ConnectTimeout: 5 * time.Second, QueryTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("Open with Postgres down: %v", err)
	}
	defer s.Close()
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("Open took %s, want no connection attempt", d)
	}
	if st := s.pool.Stat(); st.TotalConns() != 0 {
		t.Fatalf("Open made %d connections, want 0", st.TotalConns())
	}

	_, err = s.Stamp(context.Background())
	if err == nil {
		t.Fatal("Stamp with Postgres down: want error")
	}
	if strings.Contains(err.Error(), testPassword) {
		t.Fatalf("Stamp error leaks the password: %v", err)
	}
	if !strings.HasPrefix(err.Error(), "pgstore: stamp: ") {
		t.Fatalf("Stamp err = %v, want pgstore: stamp: prefix", err)
	}
}

func TestOperationsFailWithinQueryTimeout(t *testing.T) {
	dsn := "postgres://gateway:" + testPassword + "@" + silentAddr(t) + "/gateway?sslmode=disable"
	const queryTimeout = 200 * time.Millisecond
	s, err := Open(context.Background(), dsn, Options{ConnectTimeout: 10 * time.Second, QueryTimeout: queryTimeout})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	for name, op := range map[string]func() error{
		"stamp":       func() error { _, err := s.Stamp(ctx); return err },
		"load served": func() error { _, err := s.LoadServed(ctx, 10); return err },
		"migrate":     func() error { return s.Migrate(ctx) },
		"create":      func() error { _, err := s.CreateVersion(ctx, "t", "", []byte(`{}`), nil); return err },
		"set status":  func() error { _, err := s.SetStatus(ctx, "t", 1, "active"); return err },
		"list":        func() error { _, err := s.List(ctx); return err },
	} {
		start := time.Now()
		err := op()
		d := time.Since(start)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("%s: err = %v, want context.DeadlineExceeded", name, err)
		}
		if d < queryTimeout/2 || d > queryTimeout+time.Second {
			t.Errorf("%s: failed after %s, want about %s", name, d, queryTimeout)
		}
		if err != nil && strings.Contains(err.Error(), testPassword) {
			t.Errorf("%s: error leaks the password: %v", name, err)
		}
	}
}

func TestCallerDeadlineShorterThanQueryTimeout(t *testing.T) {
	dsn := "postgres://gateway@" + silentAddr(t) + "/gateway?sslmode=disable"
	s, err := Open(context.Background(), dsn, Options{QueryTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := s.Stamp(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stamp err = %v, want context.DeadlineExceeded", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Stamp ignored the caller's deadline: %s", d)
	}
}

func TestCreateVersionRejectsBeforeConnecting(t *testing.T) {
	s, err := Open(context.Background(), "postgres://gateway@"+refusedAddr(t)+"/gateway", Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	for _, tc := range []struct {
		name, id     string
		spec, counts []byte
		want         error
	}{
		{"bad id", "Bad ID", []byte(`{}`), nil, ErrInvalidID},
		{"empty id", "", []byte(`{}`), nil, ErrInvalidID},
		{"no spec", "t", nil, nil, errInvalidSpec},
		{"bad spec", "t", []byte(`{"model":`), nil, errInvalidSpec},
		{"bad counts", "t", []byte(`{}`), []byte(`[1,`), errInvalidCounts},
	} {
		if _, err := s.CreateVersion(ctx, tc.id, "", tc.spec, tc.counts); !errors.Is(err, tc.want) {
			t.Errorf("%s: err = %v, want %v", tc.name, err, tc.want)
		}
	}
	if err := s.SetCounts(ctx, "t", 1, []byte(`nope`)); !errors.Is(err, errInvalidCounts) {
		t.Errorf("SetCounts bad JSON: err = %v, want errInvalidCounts", err)
	}
}

func TestEmbeddedMigrations(t *testing.T) {
	if len(migrations) == 0 || migrations[0].version != 1 {
		t.Fatalf("embedded migrations = %+v, want first version 1", migrations)
	}
	for i, m := range migrations {
		if m.version != i+1 {
			t.Errorf("migration %s: version %d, want %d (no gaps)", m.name, m.version, i+1)
		}
		if strings.TrimSpace(m.sql) == "" {
			t.Errorf("migration %s is empty", m.name)
		}
	}
}

func TestLoadMigrationsRejectsBadNames(t *testing.T) {
	file := &fstest.MapFile{Data: []byte("SELECT 1;")}
	for name, fsys := range map[string]fstest.MapFS{
		"no prefix":  {"migrations/templates.sql": file},
		"zero":       {"migrations/0000_x.sql": file},
		"not number": {"migrations/abcd_x.sql": file},
		"duplicate":  {"migrations/0001_a.sql": file, "migrations/01_b.sql": file},
		"empty":      {"migrations/": &fstest.MapFile{Mode: fs.ModeDir | 0o755}},
	} {
		if _, err := loadMigrations(fsys); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
	ms, err := loadMigrations(fstest.MapFS{"migrations/0002_b.sql": file, "migrations/0001_a.sql": file})
	if err != nil || len(ms) != 2 || ms[0].version != 1 || ms[1].version != 2 {
		t.Fatalf("loadMigrations = %+v, %v", ms, err)
	}
}
