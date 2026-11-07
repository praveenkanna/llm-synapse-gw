package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates"
)

// MaxListRows caps the version rows List reads; a registry holding more is
// listed truncated, in template and version order.
const MaxListRows = 10000

var (
	errInvalidSpec   = errors.New("pgstore: spec is not valid JSON")
	errInvalidCounts = errors.New("pgstore: token counts are not valid JSON")
)

// TemplateSummary is one template with its versions, as List returns it.
type TemplateSummary struct {
	ID          string           `json:"id"`
	Description string           `json:"description"`
	CreatedAt   time.Time        `json:"created_at"`
	Versions    []VersionSummary `json:"versions"`
}

// VersionSummary is one version's lifecycle state, without its spec.
type VersionSummary struct {
	Version   int              `json:"version"`
	Status    templates.Status `json:"status"`
	CreatedAt time.Time        `json:"created_at"`
	UpdatedAt time.Time        `json:"updated_at"`
}

// CreateVersion stores spec as the template's next version, in status
// draft, creating the template on its first version. A non-empty
// description replaces the template's. counts may be nil. Versions count up
// from 1 per template; concurrent creates never share a number. Postgres
// stores spec as jsonb, so the returned Spec is its normalized form, equal
// as JSON but not byte for byte.
func (s *Store) CreateVersion(ctx context.Context, id, description string, spec, counts json.RawMessage) (templates.Record, error) {
	if !templates.ValidID(id) {
		return templates.Record{}, ErrInvalidID
	}
	if !json.Valid(spec) {
		return templates.Record{}, errInvalidSpec
	}
	if len(counts) > 0 && !json.Valid(counts) {
		return templates.Record{}, errInvalidCounts
	}
	var rec templates.Record
	err := s.write(ctx, "create version", func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO templates (id, description) VALUES ($1, $2)
			ON CONFLICT (id) DO UPDATE SET description = EXCLUDED.description
			WHERE EXCLUDED.description <> ''`, id, description); err != nil {
			return wrapErr(ctx, "create version: template", err)
		}
		if _, err := tx.Exec(ctx, `SELECT 1 FROM templates WHERE id = $1 FOR UPDATE`, id); err != nil {
			return wrapErr(ctx, "create version: lock template", err)
		}
		var err error
		rec, err = scanRecord(tx.QueryRow(ctx, `INSERT INTO template_versions
			(template_id, version, status, spec, token_counts, created_at, updated_at)
			SELECT $1::text, n.v, 'draft', $2::jsonb, $3::jsonb, c.ts, c.ts
			FROM (SELECT coalesce(max(version), 0) + 1 AS v FROM template_versions WHERE template_id = $1::text) n,
			     (SELECT clock_timestamp() AS ts) c
			RETURNING `+recordColumns, id, string(spec), countsArg(counts)))
		if err != nil {
			return wrapErr(ctx, "create version", err)
		}
		return nil
	})
	return rec, err
}

// Get returns one version.
func (s *Store) Get(ctx context.Context, id string, version int) (templates.Record, error) {
	ctx, cancel := s.queryCtx(ctx)
	defer cancel()
	rec, err := scanRecord(s.pool.QueryRow(ctx, `SELECT `+recordColumns+` FROM template_versions
		WHERE template_id = $1 AND version = $2`, id, version))
	if errors.Is(err, pgx.ErrNoRows) {
		return templates.Record{}, notFound(id, version)
	}
	if err != nil {
		return templates.Record{}, wrapErr(ctx, "get", err)
	}
	return rec, nil
}

// List returns every template with its versions, ordered by ID and
// version, reading at most MaxListRows rows. A template without versions
// has an empty Versions.
func (s *Store) List(ctx context.Context) ([]TemplateSummary, error) {
	ctx, cancel := s.queryCtx(ctx)
	defer cancel()
	rows, err := s.pool.Query(ctx, `SELECT t.id, t.description, t.created_at,
			v.version, v.status, v.created_at, v.updated_at
		FROM templates t LEFT JOIN template_versions v ON v.template_id = t.id
		ORDER BY t.id, v.version
		LIMIT $1`, MaxListRows)
	if err != nil {
		return nil, wrapErr(ctx, "list", err)
	}
	defer rows.Close()
	var out []TemplateSummary
	for rows.Next() {
		var t TemplateSummary
		var version *int
		var status *string
		var created, updated *time.Time
		if err := rows.Scan(&t.ID, &t.Description, &t.CreatedAt, &version, &status, &created, &updated); err != nil {
			return nil, wrapErr(ctx, "list", err)
		}
		if len(out) == 0 || out[len(out)-1].ID != t.ID {
			t.Versions = []VersionSummary{}
			out = append(out, t)
		}
		if version == nil {
			continue
		}
		last := &out[len(out)-1]
		last.Versions = append(last.Versions, VersionSummary{
			Version:   *version,
			Status:    templates.Status(*status),
			CreatedAt: *created,
			UpdatedAt: *updated,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, wrapErr(ctx, "list", err)
	}
	return out, nil
}

// SetStatus moves a version to status to when templates.CanTransition
// allows it, and returns the updated record.
func (s *Store) SetStatus(ctx context.Context, id string, version int, to templates.Status) (templates.Record, error) {
	var rec templates.Record
	err := s.write(ctx, "set status", func(ctx context.Context, tx pgx.Tx) error {
		from, err := lockVersion(ctx, tx, "set status", id, version)
		if err != nil {
			return err
		}
		if !templates.CanTransition(from, to) {
			return fmt.Errorf("%w: %q to %q", ErrInvalidTransition, from, to)
		}
		rec, err = scanRecord(tx.QueryRow(ctx, `UPDATE template_versions
			SET status = $3, updated_at = clock_timestamp()
			WHERE template_id = $1 AND version = $2
			RETURNING `+recordColumns, id, version, string(to)))
		if err != nil {
			return wrapErr(ctx, "set status", err)
		}
		return nil
	})
	return rec, err
}

// SetCounts replaces a version's token counts; nil clears them.
func (s *Store) SetCounts(ctx context.Context, id string, version int, counts json.RawMessage) error {
	if len(counts) > 0 && !json.Valid(counts) {
		return errInvalidCounts
	}
	return s.write(ctx, "set counts", func(ctx context.Context, tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE template_versions
			SET token_counts = $3::jsonb, updated_at = clock_timestamp()
			WHERE template_id = $1 AND version = $2`, id, version, countsArg(counts))
		if err != nil {
			return wrapErr(ctx, "set counts", err)
		}
		if tag.RowsAffected() == 0 {
			return notFound(id, version)
		}
		return nil
	})
}

// DeleteDraft deletes a version in status draft. The template row stays.
func (s *Store) DeleteDraft(ctx context.Context, id string, version int) error {
	return s.write(ctx, "delete draft", func(ctx context.Context, tx pgx.Tx) error {
		status, err := lockVersion(ctx, tx, "delete draft", id, version)
		if err != nil {
			return err
		}
		if status != templates.StatusDraft {
			return fmt.Errorf("%w: version is %s", ErrNotDraft, status)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM template_versions WHERE template_id = $1 AND version = $2`, id, version); err != nil {
			return wrapErr(ctx, "delete draft", err)
		}
		return nil
	})
}

// lockVersion row-locks a version for the rest of tx and returns its status.
func lockVersion(ctx context.Context, tx pgx.Tx, op, id string, version int) (templates.Status, error) {
	var status string
	err := tx.QueryRow(ctx, `SELECT status FROM template_versions
		WHERE template_id = $1 AND version = $2 FOR UPDATE`, id, version).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", notFound(id, version)
	}
	if err != nil {
		return "", wrapErr(ctx, op, err)
	}
	return templates.Status(status), nil
}

func notFound(id string, version int) error {
	return fmt.Errorf("%w: %q version %d", ErrNotFound, id, version)
}

// countsArg is counts as a query argument: SQL NULL when empty.
func countsArg(counts json.RawMessage) any {
	if len(counts) == 0 {
		return nil
	}
	return string(counts)
}
