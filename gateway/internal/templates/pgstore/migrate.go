package pgstore

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Migrations are migrations/NNNN_name.sql, applied in version order. A
// migration, once released, is never edited: a schema change is a new file.
//
//go:embed migrations/*.sql
var migrationFS embed.FS

type migration struct {
	version int
	name    string
	sql     string
}

var migrations = mustLoadMigrations(migrationFS)

func mustLoadMigrations(fsys fs.FS) []migration {
	ms, err := loadMigrations(fsys)
	if err != nil {
		panic(err)
	}
	return ms
}

// loadMigrations reads the embedded files, which must have unique positive
// versions. fs.ReadDir sorts by name, and the zero-padded prefix makes that
// version order.
func loadMigrations(fsys fs.FS) ([]migration, error) {
	entries, err := fs.ReadDir(fsys, "migrations")
	if err != nil {
		return nil, fmt.Errorf("pgstore: read migrations: %w", err)
	}
	var out []migration
	for _, e := range entries {
		name := e.Name()
		prefix, _, ok := strings.Cut(name, "_")
		v, err := strconv.Atoi(prefix)
		if !ok || err != nil || v < 1 {
			return nil, fmt.Errorf("pgstore: migration %s: name must be NNNN_name.sql with NNNN >= 1", name)
		}
		if len(out) > 0 && v <= out[len(out)-1].version {
			return nil, fmt.Errorf("pgstore: migration %s: version %d not above %d", name, v, out[len(out)-1].version)
		}
		b, err := fs.ReadFile(fsys, "migrations/"+name)
		if err != nil {
			return nil, fmt.Errorf("pgstore: read migration %s: %w", name, err)
		}
		out = append(out, migration{version: v, name: name, sql: string(b)})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("pgstore: no migrations embedded")
	}
	return out, nil
}

// Migrate applies the migrations not yet recorded in schema_migrations, in
// one transaction under an advisory lock, so gateways starting together
// apply each exactly once. It is idempotent. Versions recorded by a newer
// gateway are left alone.
func (s *Store) Migrate(ctx context.Context) error {
	return s.inTx(ctx, "migrate", func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrateLockKey); err != nil {
			return wrapErr(ctx, "migrate: lock", err)
		}
		if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
			version    int PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
			return wrapErr(ctx, "migrate: schema_migrations", err)
		}
		rows, err := tx.Query(ctx, `SELECT version FROM schema_migrations`)
		if err != nil {
			return wrapErr(ctx, "migrate: applied versions", err)
		}
		applied, err := pgx.CollectRows(rows, pgx.RowTo[int])
		if err != nil {
			return wrapErr(ctx, "migrate: applied versions", err)
		}
		done := make(map[int]bool, len(applied))
		for _, v := range applied {
			done[v] = true
		}
		for _, m := range migrations {
			if done[m.version] {
				continue
			}
			if _, err := tx.Exec(ctx, m.sql); err != nil {
				return wrapErr(ctx, "migrate: "+m.name, err)
			}
			if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, m.version); err != nil {
				return wrapErr(ctx, "migrate: record "+m.name, err)
			}
		}
		return nil
	})
}
