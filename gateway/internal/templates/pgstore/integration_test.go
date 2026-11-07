//go:build integration

package pgstore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates"
)

// The integration tests run against a real Postgres at
// INTEGRATION_POSTGRES_DSN, e.g. the dev compose stack's. Each test works in
// a fresh schema dropped at cleanup.

const itTimeout = 10 * time.Second

func itDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("INTEGRATION_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("INTEGRATION_POSTGRES_DSN not set")
	}
	return dsn
}

// testSchema creates a schema t_<random> and drops it at cleanup.
func testSchema(t *testing.T) (dsn, schema string) {
	t.Helper()
	dsn = itDSN(t)
	var b [8]byte
	_, _ = rand.Read(b[:])
	schema = "t_" + hex.EncodeToString(b[:])
	admin, err := Open(context.Background(), dsn, Options{QueryTimeout: itTimeout})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), itTimeout)
	defer cancel()
	if _, err := admin.pool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		admin.Close()
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		defer admin.Close()
		ctx, cancel := context.WithTimeout(context.Background(), itTimeout)
		defer cancel()
		if _, err := admin.pool.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("drop schema %s: %v", schema, err)
		}
	})
	return dsn, schema
}

func openIn(t *testing.T, dsn, schema string) *Store {
	t.Helper()
	s, err := Open(context.Background(), dsn, Options{QueryTimeout: itTimeout, MaxConns: 8, SearchPath: schema})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

// migrated returns a store on a fresh, migrated schema.
func migrated(t *testing.T) *Store {
	t.Helper()
	dsn, schema := testSchema(t)
	s := openIn(t, dsn, schema)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return s
}

func appliedVersions(t *testing.T, s *Store) []int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), itTimeout)
	defer cancel()
	rows, err := s.pool.Query(ctx, `SELECT version FROM schema_migrations ORDER BY version`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func wantAllMigrations() []int {
	out := make([]int, len(migrations))
	for i, m := range migrations {
		out[i] = m.version
	}
	return out
}

func TestIntegrationMigrateIdempotent(t *testing.T) {
	s := migrated(t)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
	if got, want := appliedVersions(t, s), wantAllMigrations(); !reflect.DeepEqual(got, want) {
		t.Fatalf("applied = %v, want %v", got, want)
	}
	if _, err := s.Stamp(context.Background()); err != nil {
		t.Fatalf("Stamp after migrate: %v", err)
	}
}

func TestIntegrationMigrateConcurrent(t *testing.T) {
	dsn, schema := testSchema(t)
	stores := []*Store{openIn(t, dsn, schema), openIn(t, dsn, schema), openIn(t, dsn, schema)}
	var wg sync.WaitGroup
	errs := make([]error, len(stores))
	for i, s := range stores {
		wg.Go(func() { errs[i] = s.Migrate(context.Background()) })
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("Migrate #%d: %v", i, err)
		}
	}
	if got, want := appliedVersions(t, stores[0]), wantAllMigrations(); !reflect.DeepEqual(got, want) {
		t.Fatalf("applied = %v, want %v", got, want)
	}
}

func spec(model string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"model": %q, "messages": [{"role": "system", "segments": [{"kind": "static", "text": "You are a test."}]}]}`, model))
}

func sameJSON(t *testing.T, got, want []byte) bool {
	t.Helper()
	var g, w any
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("unmarshal %s: %v", got, err)
	}
	if err := json.Unmarshal(want, &w); err != nil {
		t.Fatalf("unmarshal %s: %v", want, err)
	}
	return reflect.DeepEqual(g, w)
}

func create(t *testing.T, s *Store, id string) templates.Record {
	t.Helper()
	rec, err := s.CreateVersion(context.Background(), id, "", spec("m"), nil)
	if err != nil {
		t.Fatalf("CreateVersion(%s): %v", id, err)
	}
	return rec
}

func TestIntegrationCreateGetList(t *testing.T) {
	s := migrated(t)
	ctx := context.Background()

	counts := json.RawMessage(`{"pool":"large","examples":[12,14],"prefix_tokens":9}`)
	for v := 1; v <= 3; v++ {
		var c json.RawMessage
		if v == 2 {
			c = counts
		}
		desc := ""
		if v == 1 {
			desc = "first description"
		}
		rec, err := s.CreateVersion(ctx, "support.triage", desc, spec(fmt.Sprintf("m%d", v)), c)
		if err != nil {
			t.Fatalf("CreateVersion v%d: %v", v, err)
		}
		if rec.ID != "support.triage" || rec.Version != v || rec.Status != templates.StatusDraft {
			t.Fatalf("CreateVersion v%d = %+v", v, rec)
		}
		if rec.CreatedAt.IsZero() || !rec.CreatedAt.Equal(rec.UpdatedAt) {
			t.Fatalf("CreateVersion v%d times: created %s updated %s", v, rec.CreatedAt, rec.UpdatedAt)
		}
	}

	got, err := s.Get(ctx, "support.triage", 2)
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != 2 || got.Status != templates.StatusDraft || !sameJSON(t, got.Spec, spec("m2")) || !sameJSON(t, got.Counts, counts) {
		t.Fatalf("Get v2 = %+v (spec %s, counts %s)", got, got.Spec, got.Counts)
	}
	if got, err := s.Get(ctx, "support.triage", 1); err != nil || got.Counts != nil {
		t.Fatalf("Get v1 counts = %q, %v; want nil (SQL NULL)", got.Counts, err)
	}
	for _, k := range []struct {
		id string
		v  int
	}{{"support.triage", 4}, {"support.triage", 0}, {"missing", 1}} {
		if _, err := s.Get(ctx, k.id, k.v); !errors.Is(err, ErrNotFound) {
			t.Errorf("Get(%s, %d) err = %v, want ErrNotFound", k.id, k.v, err)
		}
	}

	// An empty description keeps the template's; a non-empty one replaces it.
	create(t, s, "alpha")
	if _, err := s.CreateVersion(ctx, "alpha", "alpha desc", spec("m"), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetStatus(ctx, "alpha", 2, templates.StatusActive); err != nil {
		t.Fatal(err)
	}
	ctxT, cancel := context.WithTimeout(ctx, itTimeout)
	defer cancel()
	if _, err := s.pool.Exec(ctxT, `INSERT INTO templates (id) VALUES ('empty-template')`); err != nil {
		t.Fatal(err)
	}

	list, err := s.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 3 || list[0].ID != "alpha" || list[1].ID != "empty-template" || list[2].ID != "support.triage" {
		t.Fatalf("List IDs = %+v", list)
	}
	if list[0].Description != "alpha desc" || list[2].Description != "first description" {
		t.Fatalf("List descriptions = %q, %q", list[0].Description, list[2].Description)
	}
	if list[1].Versions == nil || len(list[1].Versions) != 0 {
		t.Fatalf("template without versions: Versions = %#v, want empty non-nil", list[1].Versions)
	}
	if v := list[0].Versions; len(v) != 2 || v[0].Version != 1 || v[0].Status != templates.StatusDraft ||
		v[1].Version != 2 || v[1].Status != templates.StatusActive || v[1].UpdatedAt.Before(v[1].CreatedAt) {
		t.Fatalf("alpha versions = %+v", v)
	}
	if v := list[2].Versions; len(v) != 3 || v[0].Version != 1 || v[2].Version != 3 {
		t.Fatalf("support.triage versions = %+v", v)
	}
	b, err := json.Marshal(list[0])
	if err != nil {
		t.Fatal(err)
	}
	var keys map[string]json.RawMessage
	_ = json.Unmarshal(b, &keys)
	for _, k := range []string{"id", "description", "created_at", "versions"} {
		if _, ok := keys[k]; !ok {
			t.Errorf("TemplateSummary JSON %s lacks %q", b, k)
		}
	}

	if _, err := s.CreateVersion(ctx, "Not Valid", "", spec("m"), nil); !errors.Is(err, ErrInvalidID) {
		t.Errorf("CreateVersion invalid id err = %v", err)
	}
}

func TestIntegrationConcurrentCreatesGetUniqueVersions(t *testing.T) {
	s := migrated(t)
	ctx := context.Background()
	// Template "seeded" exists; "fresh" is created by the racing calls.
	create(t, s, "seeded")
	const n = 20
	for _, id := range []string{"seeded", "fresh"} {
		var wg sync.WaitGroup
		versions := make([]int, n)
		errs := make([]error, n)
		for i := range n {
			wg.Go(func() {
				rec, err := s.CreateVersion(ctx, id, "", spec("m"), nil)
				versions[i], errs[i] = rec.Version, err
			})
		}
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Fatalf("%s: create #%d: %v", id, i, err)
			}
		}
		sort.Ints(versions)
		first := 1
		if id == "seeded" {
			first = 2
		}
		for i, v := range versions {
			if v != first+i {
				t.Fatalf("%s: versions = %v, want consecutive from %d", id, versions, first)
			}
		}
	}
}

// reach drives a fresh draft to status st along valid transitions.
func reach(t *testing.T, s *Store, id string, v int, st templates.Status) {
	t.Helper()
	var path []templates.Status
	switch st {
	case templates.StatusActive:
		path = []templates.Status{templates.StatusActive}
	case templates.StatusDeprecated:
		path = []templates.Status{templates.StatusActive, templates.StatusDeprecated}
	case templates.StatusRetired:
		path = []templates.Status{templates.StatusRetired}
	}
	for _, to := range path {
		if _, err := s.SetStatus(context.Background(), id, v, to); err != nil {
			t.Fatalf("reach %s: SetStatus %s: %v", st, to, err)
		}
	}
}

func TestIntegrationTransitions(t *testing.T) {
	s := migrated(t)
	ctx := context.Background()
	all := []templates.Status{templates.StatusDraft, templates.StatusActive, templates.StatusDeprecated, templates.StatusRetired, "bogus"}
	valid := 0
	for _, from := range all[:4] {
		for _, to := range all {
			rec := create(t, s, "lifecycle")
			reach(t, s, "lifecycle", rec.Version, from)
			before, err := s.Get(ctx, "lifecycle", rec.Version)
			if err != nil {
				t.Fatal(err)
			}
			got, err := s.SetStatus(ctx, "lifecycle", rec.Version, to)
			if templates.CanTransition(from, to) {
				valid++
				if err != nil {
					t.Errorf("%s -> %s: %v, want ok", from, to, err)
					continue
				}
				if got.Status != to || !got.UpdatedAt.After(before.UpdatedAt) || !sameJSON(t, got.Spec, before.Spec) {
					t.Errorf("%s -> %s: record %+v (before %+v)", from, to, got, before)
				}
				continue
			}
			if !errors.Is(err, ErrInvalidTransition) {
				t.Errorf("%s -> %s: err = %v, want ErrInvalidTransition", from, to, err)
				continue
			}
			after, _ := s.Get(ctx, "lifecycle", rec.Version)
			if after.Status != from || !after.UpdatedAt.Equal(before.UpdatedAt) {
				t.Errorf("%s -> %s refused but record changed: %+v", from, to, after)
			}
		}
	}
	if valid != 6 {
		t.Fatalf("exercised %d valid transitions, want 6 (draft→active, draft→retired, active→deprecated, active→retired, deprecated→active, deprecated→retired)", valid)
	}
	if _, err := s.SetStatus(ctx, "lifecycle", 9999, templates.StatusActive); !errors.Is(err, ErrNotFound) {
		t.Errorf("SetStatus missing version err = %v, want ErrNotFound", err)
	}
}

func TestIntegrationDeleteDraft(t *testing.T) {
	s := migrated(t)
	ctx := context.Background()
	for v := 1; v <= 4; v++ {
		create(t, s, "del")
	}
	reach(t, s, "del", 2, templates.StatusActive)
	reach(t, s, "del", 3, templates.StatusDeprecated)
	reach(t, s, "del", 4, templates.StatusRetired)

	if err := s.DeleteDraft(ctx, "del", 1); err != nil {
		t.Fatalf("DeleteDraft draft: %v", err)
	}
	if _, err := s.Get(ctx, "del", 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get deleted draft err = %v, want ErrNotFound", err)
	}
	if err := s.DeleteDraft(ctx, "del", 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DeleteDraft twice err = %v, want ErrNotFound", err)
	}
	for v := 2; v <= 4; v++ {
		if err := s.DeleteDraft(ctx, "del", v); !errors.Is(err, ErrNotDraft) {
			t.Errorf("DeleteDraft v%d err = %v, want ErrNotDraft", v, err)
		}
		if _, err := s.Get(ctx, "del", v); err != nil {
			t.Errorf("v%d gone after refused delete: %v", v, err)
		}
	}
	if err := s.DeleteDraft(ctx, "nope", 1); !errors.Is(err, ErrNotFound) {
		t.Errorf("DeleteDraft missing template err = %v, want ErrNotFound", err)
	}
	// Versions keep counting up after a delete of the newest draft.
	if rec := create(t, s, "del"); rec.Version != 5 {
		t.Fatalf("next version = %d, want 5", rec.Version)
	}
	if err := s.DeleteDraft(ctx, "del", 5); err != nil {
		t.Fatal(err)
	}
	if rec := create(t, s, "del"); rec.Version != 5 {
		t.Fatalf("version after deleting newest draft = %d, want 5 (max+1)", rec.Version)
	}
}

func TestIntegrationLoadServed(t *testing.T) {
	s := migrated(t)
	ctx := context.Background()
	// b: v1 draft, v2 active, v3 deprecated, v4 retired; a: v1 active.
	for v := 1; v <= 4; v++ {
		create(t, s, "b")
	}
	create(t, s, "a")
	reach(t, s, "b", 2, templates.StatusActive)
	reach(t, s, "b", 3, templates.StatusDeprecated)
	reach(t, s, "b", 4, templates.StatusRetired)
	reach(t, s, "a", 1, templates.StatusActive)
	counts := json.RawMessage(`{"prefix_tokens":4}`)
	if err := s.SetCounts(ctx, "b", 2, counts); err != nil {
		t.Fatal(err)
	}

	type key struct {
		id string
		v  int
		st templates.Status
	}
	keys := func(rs []templates.Record) []key {
		out := make([]key, len(rs))
		for i, r := range rs {
			out[i] = key{r.ID, r.Version, r.Status}
		}
		return out
	}
	all := []key{{"a", 1, templates.StatusActive}, {"b", 2, templates.StatusActive}, {"b", 3, templates.StatusDeprecated}}
	recs, err := s.LoadServed(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if got := keys(recs); !reflect.DeepEqual(got, all) {
		t.Fatalf("LoadServed(10) = %v, want %v", got, all)
	}
	if !sameJSON(t, recs[1].Spec, spec("m")) || !sameJSON(t, recs[1].Counts, counts) || recs[0].Counts != nil {
		t.Fatalf("LoadServed records: %+v", recs)
	}
	for limit, want := range map[int][]key{3: all, 2: all, 1: all[:2], 0: all[:1], -1: all[:1]} {
		recs, err := s.LoadServed(ctx, limit)
		if err != nil {
			t.Fatal(err)
		}
		if got := keys(recs); !reflect.DeepEqual(got, want) {
			t.Errorf("LoadServed(%d) = %v, want %v (limit+1)", limit, got, want)
		}
	}
}

func TestIntegrationStampChanges(t *testing.T) {
	s := migrated(t)
	ctx := context.Background()
	stamp := func() string {
		t.Helper()
		st, err := s.Stamp(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return st
	}
	seen := map[string]string{}
	step := func(name string) {
		t.Helper()
		st := stamp()
		if prev, dup := seen[st]; dup {
			t.Fatalf("stamp after %s = %s, same as after %s", name, st, prev)
		}
		seen[st] = name
		if again := stamp(); again != st {
			t.Fatalf("stamp moved without a write after %s: %s then %s", name, st, again)
		}
	}
	step("migrate")
	create(t, s, "st")
	step("create")
	create(t, s, "st")
	step("second create")
	if _, err := s.SetStatus(ctx, "st", 1, templates.StatusActive); err != nil {
		t.Fatal(err)
	}
	step("set status")
	if err := s.SetCounts(ctx, "st", 1, json.RawMessage(`{"prefix_tokens":7}`)); err != nil {
		t.Fatal(err)
	}
	step("set counts")
	if err := s.SetCounts(ctx, "st", 1, nil); err != nil {
		t.Fatal(err)
	}
	step("clear counts")
	if err := s.DeleteDraft(ctx, "st", 2); err != nil {
		t.Fatal(err)
	}
	step("delete draft")
	if _, err := s.SetStatus(ctx, "st", 1, templates.StatusDraft); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("refused transition err = %v", err)
	}
	if st := stamp(); seen[st] != "delete draft" {
		t.Fatalf("refused transition moved the stamp to %s", st)
	}
	if err := s.SetCounts(ctx, "st", 99, nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("SetCounts missing err = %v, want ErrNotFound", err)
	}
}

// A write waits for the write lock and stamps updated_at after the holder
// released it, so a write that commits later never carries an older time
// than one Stamp has already seen.
func TestIntegrationWritesSerialize(t *testing.T) {
	s := migrated(t)
	ctx := context.Background()
	create(t, s, "serial")

	lctx, cancel := context.WithTimeout(ctx, itTimeout)
	defer cancel()
	holder, err := s.pool.Begin(lctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback(context.Background()) }()
	if _, err := holder.Exec(lctx, `SELECT pg_advisory_xact_lock($1)`, writeLockKey); err != nil {
		t.Fatal(err)
	}

	type result struct {
		rec templates.Record
		err error
	}
	done := make(chan result, 1)
	go func() {
		rec, err := s.SetStatus(ctx, "serial", 1, templates.StatusActive)
		done <- result{rec, err}
	}()
	select {
	case r := <-done:
		t.Fatalf("SetStatus did not wait for the write lock: %+v", r)
	case <-time.After(300 * time.Millisecond):
	}
	var released time.Time
	if err := holder.QueryRow(lctx, `SELECT clock_timestamp()`).Scan(&released); err != nil {
		t.Fatal(err)
	}
	if err := holder.Commit(lctx); err != nil {
		t.Fatal(err)
	}
	r := <-done
	if r.err != nil {
		t.Fatal(r.err)
	}
	if !r.rec.UpdatedAt.After(released) {
		t.Fatalf("updated_at %s not after the lock release %s", r.rec.UpdatedAt, released)
	}
}
