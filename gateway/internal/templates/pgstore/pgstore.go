// Package pgstore is the template registry's Postgres store: embedded schema
// migrations, the admin API's version operations and the snapshot
// refresher's templates.Loader.
//
// Nothing here is on the request path: the refresher and the admin API call
// it out of band, and every operation, transaction included, is bounded by
// Options.QueryTimeout. Open does not connect, so a gateway starts with
// Postgres down; the pool dials on first use. Errors never carry the DSN or
// its password, and spec content is never logged.
//
// Writes from every gateway are serialized by a transaction-scoped advisory
// lock and stamp updated_at with clock_timestamp() while holding it, so
// commit order is timestamp order: every committed write either raises the
// newest update time or changes the version count, and Stamp cannot miss one
// that committed behind a later-stamped write.
package pgstore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates"
)

const (
	defaultConnectTimeout = 2 * time.Second
	defaultQueryTimeout   = 2 * time.Second
	defaultMaxConns       = 4

	// Advisory lock keys, database-wide. migrateLockKey serializes the
	// migrations of gateways starting together; writeLockKey serializes
	// writes (see the package doc).
	migrateLockKey int64 = 0x6c6c6d67_74700001
	writeLockKey   int64 = 0x6c6c6d67_74700002
)

// Errors returned by the store. Callers match them with errors.Is.
var (
	ErrNotFound          = errors.New("pgstore: template version not found")
	ErrInvalidTransition = errors.New("pgstore: invalid status transition")
	ErrNotDraft          = errors.New("pgstore: only a draft version can be deleted")
	ErrInvalidID         = errors.New("pgstore: invalid template id")
)

var (
	// errMalformedDSN replaces the driver's parse error, whose text can
	// echo the connection string and its password.
	errMalformedDSN = errors.New("pgstore: malformed DSN: not a valid libpq connection string or URL")
	errEmptyDSN     = errors.New("pgstore: empty DSN")
)

// Options configures a Store. Zero durations and sizes take the defaults.
type Options struct {
	ConnectTimeout time.Duration // one connection attempt; default 2s
	QueryTimeout   time.Duration // one operation, transaction included; default 2s
	MaxConns       int32         // pool size; default 4
	// SearchPath sets the connections' search_path, for tests that isolate
	// themselves in a schema and for multi-tenant use. Empty keeps the
	// server's default.
	SearchPath string
}

// Store is the Postgres template store, safe for concurrent use.
type Store struct {
	pool         *pgxpool.Pool
	queryTimeout time.Duration
}

var _ templates.Loader = (*Store)(nil)

// Open returns a store for dsn, a libpq connection string or URL. It does
// not connect: only a malformed DSN fails here, and an unreachable server
// fails the first operation instead.
func Open(ctx context.Context, dsn string, o Options) (*Store, error) {
	if dsn == "" {
		return nil, errEmptyDSN
	}
	if o.ConnectTimeout <= 0 {
		o.ConnectTimeout = defaultConnectTimeout
	}
	if o.QueryTimeout <= 0 {
		o.QueryTimeout = defaultQueryTimeout
	}
	if o.MaxConns <= 0 {
		o.MaxConns = defaultMaxConns
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errMalformedDSN
	}
	cfg.MinConns = 0
	cfg.MaxConns = o.MaxConns
	cfg.ConnConfig.ConnectTimeout = o.ConnectTimeout
	if o.SearchPath != "" {
		cfg.ConnConfig.RuntimeParams["search_path"] = o.SearchPath
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pgstore: open pool: %w", err)
	}
	return &Store{pool: pool, queryTimeout: o.QueryTimeout}, nil
}

// Close closes the pool, waiting for connections in use to be released.
func (s *Store) Close() { s.pool.Close() }

// Stamp returns the version count and the newest update time. Any committed
// create, status change, counts update or delete changes it.
func (s *Store) Stamp(ctx context.Context) (string, error) {
	ctx, cancel := s.queryCtx(ctx)
	defer cancel()
	var n int64
	var newest *time.Time
	err := s.pool.QueryRow(ctx, `SELECT count(*), max(updated_at) FROM template_versions`).Scan(&n, &newest)
	if err != nil {
		return "", wrapErr(ctx, "stamp", err)
	}
	var us int64
	if newest != nil {
		us = newest.UnixMicro()
	}
	return strconv.FormatInt(n, 10) + "@" + strconv.FormatInt(us, 10), nil
}

// LoadServed returns the active and deprecated versions ordered by template
// and version, at most limit+1 of them.
func (s *Store) LoadServed(ctx context.Context, limit int) ([]templates.Record, error) {
	ctx, cancel := s.queryCtx(ctx)
	defer cancel()
	rows, err := s.pool.Query(ctx, `SELECT `+recordColumns+` FROM template_versions
		WHERE status IN ('active', 'deprecated')
		ORDER BY template_id, version
		LIMIT $1`, max(limit, 0)+1)
	if err != nil {
		return nil, wrapErr(ctx, "load served", err)
	}
	defer rows.Close()
	var out []templates.Record
	for rows.Next() {
		r, err := scanRecord(rows)
		if err != nil {
			return nil, wrapErr(ctx, "load served", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, wrapErr(ctx, "load served", err)
	}
	return out, nil
}

// recordColumns are the template_versions columns scanRecord reads.
const recordColumns = `template_id, version, status, spec, token_counts, created_at, updated_at`

func scanRecord(row pgx.Row) (templates.Record, error) {
	var r templates.Record
	var status string
	var spec, counts []byte
	if err := row.Scan(&r.ID, &r.Version, &status, &spec, &counts, &r.CreatedAt, &r.UpdatedAt); err != nil {
		return templates.Record{}, err
	}
	r.Status = templates.Status(status)
	r.Spec = spec
	r.Counts = counts
	return r, nil
}

func (s *Store) queryCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.queryTimeout)
}

// inTx runs fn in a transaction under the query timeout and commits when fn
// returns nil. fn wraps its own errors.
func (s *Store) inTx(ctx context.Context, op string, fn func(context.Context, pgx.Tx) error) error {
	ctx, cancel := s.queryCtx(ctx)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return wrapErr(ctx, op, err)
	}
	defer func() {
		// A no-op after Commit. Bounded on its own: ctx may have expired.
		rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), s.queryTimeout)
		defer rcancel()
		_ = tx.Rollback(rctx)
	}()
	if err := fn(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return wrapErr(ctx, op, err)
	}
	return nil
}

// write is inTx holding the write lock.
func (s *Store) write(ctx context.Context, op string, fn func(context.Context, pgx.Tx) error) error {
	return s.inTx(ctx, op, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, writeLockKey); err != nil {
			return wrapErr(ctx, op, err)
		}
		return fn(ctx, tx)
	})
}

// wrapErr adds the operation and, when the context ended, its error, so
// callers can tell a timeout (context.DeadlineExceeded) from a failure.
func wrapErr(ctx context.Context, op string, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil && !errors.Is(err, ctxErr) {
		return fmt.Errorf("pgstore: %s: %w: %w", op, ctxErr, err)
	}
	return fmt.Errorf("pgstore: %s: %w", op, err)
}
