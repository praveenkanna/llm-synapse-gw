package templates

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Fail-open reasons of the snapshot refresh (stage
// telemetry.StageTemplateRegistry): a round that failed and left the last
// snapshot serving.
const (
	ReasonPostgresError    = "postgres_error"
	ReasonPostgresTimeout  = "postgres_timeout"
	ReasonSnapshotTooLarge = "snapshot_too_large"
)

// Registry defaults, for zero RegistryOptions fields.
const (
	DefaultRefreshInterval = 5 * time.Second
	DefaultQueryTimeout    = 2 * time.Second
	DefaultMaxVersions     = 4096
)

// maxLoggedSkips bounds the skipped records logged per load.
const maxLoggedSkips = 32

// RegistryOptions configures a Registry. Zero values take the defaults.
type RegistryOptions struct {
	Interval     time.Duration
	QueryTimeout time.Duration // each store call
	MaxVersions  int
	// OnFailOpen is called once per failed round with its reason.
	OnFailOpen func(reason string)
	// OnInvalid is called for each record a load skipped.
	OnInvalid func(id string, version int, err error)
	Logger    *slog.Logger
	Now       func() time.Time
}

// Registry keeps the request path's snapshot fresh, out of band: the
// request path reads Snapshot and never waits on the store.
type Registry struct {
	loader Loader
	o      RegistryOptions
	snap   atomic.Pointer[Snapshot]
	// checked is the UnixNano of the last successful round, or of
	// NewRegistry before one.
	checked atomic.Int64
	kick    chan struct{}

	mu    sync.Mutex // serializes rounds
	stamp string     // of the current snapshot
}

// NewRegistry builds a Registry; Run refreshes it.
func NewRegistry(l Loader, o RegistryOptions) *Registry {
	if o.Interval <= 0 {
		o.Interval = DefaultRefreshInterval
	}
	if o.QueryTimeout <= 0 {
		o.QueryTimeout = DefaultQueryTimeout
	}
	if o.MaxVersions <= 0 {
		o.MaxVersions = DefaultMaxVersions
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	r := &Registry{loader: l, o: o, kick: make(chan struct{}, 1)}
	r.checked.Store(o.Now().UnixNano())
	return r
}

// Snapshot is the current snapshot; nil until the first successful load.
func (r *Registry) Snapshot() *Snapshot { return r.snap.Load() }

// Age is the time since the last successful round, or since NewRegistry
// before one.
func (r *Registry) Age() time.Duration {
	return r.o.Now().Sub(time.Unix(0, r.checked.Load()))
}

// Kick asks Run for a round now, without waiting for it.
func (r *Registry) Kick() {
	select {
	case r.kick <- struct{}{}:
	default:
	}
}

// Run refreshes at once, then every Interval and on Kick, until ctx ends.
func (r *Registry) Run(ctx context.Context) {
	t := time.NewTicker(r.o.Interval)
	defer t.Stop()
	for {
		r.round(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-r.kick:
		}
	}
}

// round is one Refresh that never panics.
func (r *Registry) round(ctx context.Context) {
	defer func() {
		if p := recover(); p != nil {
			r.o.Logger.Warn("template registry refresh panicked", "reason", ReasonPostgresError, "panic", fmt.Sprint(p))
			r.failOpen(ReasonPostgresError)
		}
	}()
	_ = r.Refresh(ctx)
}

// Refresh runs one round: a stamp check, and a full load when the stamp
// changed. On failure the last snapshot keeps serving and OnFailOpen is
// called once, unless ctx itself ended.
func (r *Registry) Refresh(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var stamp string
	err := r.query(ctx, func(qctx context.Context) (err error) {
		stamp, err = r.loader.Stamp(qctx)
		return err
	})
	if err != nil {
		return r.fail(ctx, "stamp", err)
	}
	if r.snap.Load() != nil && stamp == r.stamp {
		r.checked.Store(r.o.Now().UnixNano())
		return nil
	}
	var recs []Record
	err = r.query(ctx, func(qctx context.Context) (err error) {
		recs, err = r.loader.LoadServed(qctx, r.o.MaxVersions)
		return err
	})
	if err != nil {
		return r.fail(ctx, "load", err)
	}
	if len(recs) > r.o.MaxVersions {
		err := fmt.Errorf("templates: load: more than %d served versions", r.o.MaxVersions)
		r.o.Logger.Warn("template registry refresh failed", "reason", ReasonSnapshotTooLarge, "err", err)
		r.failOpen(ReasonSnapshotTooLarge)
		return err
	}

	snap, errs := NewSnapshot(recs, r.o.Now())
	r.snap.Store(snap)
	r.stamp = stamp
	r.checked.Store(r.o.Now().UnixNano())
	r.o.Logger.Info("template snapshot loaded", "versions", snap.Len(), "skipped", len(errs))
	for i, err := range errs {
		var re *RecordError
		if !errors.As(err, &re) {
			continue
		}
		if i < maxLoggedSkips {
			r.o.Logger.Warn("template version skipped", "template", re.ID, "version", re.Version, "err", re.Err)
		}
		if r.o.OnInvalid != nil {
			r.o.OnInvalid(re.ID, re.Version, re.Err)
		}
	}
	return nil
}

// query runs one store call under QueryTimeout. A call that outlived it
// reports context.DeadlineExceeded whatever error the store wrapped.
func (r *Registry) query(ctx context.Context, fn func(context.Context) error) error {
	qctx, cancel := context.WithTimeout(ctx, r.o.QueryTimeout)
	defer cancel()
	err := fn(qctx)
	if err != nil && ctx.Err() == nil && errors.Is(qctx.Err(), context.DeadlineExceeded) &&
		!errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %w", context.DeadlineExceeded, err)
	}
	return err
}

func (r *Registry) fail(ctx context.Context, op string, err error) error {
	err = fmt.Errorf("templates: %s: %w", op, err)
	if ctx.Err() != nil {
		return err // shutting down: not a failed round
	}
	reason := ReasonPostgresError
	if errors.Is(err, context.DeadlineExceeded) {
		reason = ReasonPostgresTimeout
	}
	r.o.Logger.Warn("template registry refresh failed", "reason", reason, "err", err)
	r.failOpen(reason)
	return err
}

func (r *Registry) failOpen(reason string) {
	if r.o.OnFailOpen != nil {
		r.o.OnFailOpen(reason)
	}
}
