// Package dispatch sends a caller's request to one replica of a pool and
// returns when the replica's response headers arrive. It owns every rule
// about the upstream attempts on the way (ADR 0012, ADR 0014,
// .claude/rules/streaming-and-failure.md):
//
//   - the replica comes from the router as a lease: the request holds
//     exactly one at a time, and each lease is released once, on whatever
//     path the request ends, panics included;
//   - a connect failure (nothing reached the replica) is retried once: on
//     another replica of the pool, or on the same one after the
//     connect-retry backoff when the pool has no other. Nothing else is
//     ever retried: a generation must not run twice;
//   - a hard connect failure (refused, unreachable, NXDOMAIN) while the
//     caller is still there is reported to passive health, at most once per
//     request and replica, so a same-replica retry cannot double it;
//   - a streaming attempt runs on a context of its own and holds a
//     first-chunk wait on its replica, so stuck-engine detection can cut it
//     with upstream.ErrReplicaStuck (M1-20); a retry gets a fresh one, so a
//     cut still under way for the failed attempt cannot reach the next;
//   - the request's telemetry follows the lease: the replica it is in
//     flight on, the retry, and each upstream failure (counted and logged,
//     never with prompt or response text).
//
// The caller relays the Exchange's response and closes the Exchange. It
// never handles a lease, a wait or an attempt context.
package dispatch

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

var (
	// ErrNoReplicas is returned when the router has no replica of the pool.
	ErrNoReplicas = errors.New("dispatch: no replicas available")
	// ErrCallerGone is returned when the caller left before a response: during
	// the connect-retry backoff, or while an attempt was waiting for headers.
	// The disconnect is already recorded; nobody is listening for an answer.
	ErrCallerGone = errors.New("dispatch: the caller left before a response")
)

// Error is a request that got no response headers from its pool: the last
// attempt failed and nothing more is retried. Err is the upstream failure
// (see upstream.KindOf); Replica is where it happened. It is already counted
// and logged.
type Error struct {
	Replica discovery.Replica
	Err     error
}

func (e *Error) Error() string { return "dispatch to replica " + e.Replica.Name + ": " + e.Err.Error() }
func (e *Error) Unwrap() error { return e.Err }

// Options tunes a Dispatcher. Zero values take defaults.
type Options struct {
	// Health receives hard connect failures and watches streaming attempts
	// for stuck engines. Nil tracks nothing.
	Health Health
	// Logger receives upstream failure lines (never prompt or response text).
	Logger *slog.Logger
	// BackoffWait waits out the connect-retry backoff before a same-replica
	// retry and reports whether the caller is still there. Nil uses a timer;
	// tests replace it to control when the retry starts.
	BackoffWait func(ctx context.Context, d time.Duration) bool
}

// Dispatcher sends requests to the replicas a router picks. It is safe for
// concurrent use.
type Dispatcher struct {
	router      router.Router
	client      *upstream.Client
	health      Health
	logger      *slog.Logger
	backoff     time.Duration // upstream.connect_retry_backoff
	backoffWait func(ctx context.Context, d time.Duration) bool
}

// New builds a Dispatcher that picks replicas with rtr, sends with client and
// takes the connect-retry backoff from cfg.
func New(rtr router.Router, client *upstream.Client, cfg config.Upstream, opts Options) *Dispatcher {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	if opts.BackoffWait == nil {
		opts.BackoffWait = sleepCtx
	}
	return &Dispatcher{
		router:      rtr,
		client:      client,
		health:      opts.Health,
		logger:      opts.Logger,
		backoff:     cfg.ConnectRetryBackoff,
		backoffWait: opts.BackoffWait,
	}
}

// Send sends req to a replica of pool on behalf of the caller's request r
// and returns once the replica's response headers arrive. req.Stream marks
// a streaming call, the only kind whose attempts are watched for a stuck
// engine: vLLM sends a non-streaming response's headers only after the
// whole generation.
//
// On success the caller owns the Exchange and must Close it. Otherwise the
// error is ErrNoReplicas, ErrCallerGone or an *Error, and everything Send
// acquired is already released.
func (d *Dispatcher) Send(r *http.Request, pool string, req upstream.Request) (ex *Exchange, err error) {
	rec := telemetry.FromContext(r.Context())
	lease, rerr := d.router.Route(r.Context(), r, pool)
	if rerr != nil {
		rec.Route(pool, "")
		return nil, ErrNoReplicas
	}
	// On every exit without an exchange, panics included, what is held then
	// is given back: the response body, then the attempt, then the lease.
	// Separate defers, registered in reverse, so a panic while ending the
	// attempt (in passive health) still releases the lease.
	defer func() {
		if ex == nil {
			lease.Release()
		}
	}()
	rec.Route(pool, lease.Replica.Name)
	a := attempt{cancel: noCancel, wait: NoFirstChunkWait}
	defer func() {
		if ex == nil {
			a.end()
		}
	}()
	var resp *http.Response
	defer func() {
		if ex == nil && resp != nil {
			_ = resp.Body.Close()
		}
	}()
	a = d.startAttempt(r.Context(), lease.Replica, req.Stream)
	resp, err = d.client.Do(a.ctx, lease.Replica, req)
	reported := "" // the replica this request reported a failure on, if any
	if err != nil && upstream.Retryable(err) && r.Context().Err() == nil {
		a.end() // the backoff is not the replica's wait
		reported = d.report(r, lease.Replica, err, reported)
		if !d.retryTarget(r, pool, &lease, err) {
			return nil, ErrCallerGone
		}
		rec.Reroute(lease.Replica.Name, string(upstream.KindOf(err)))
		a = d.startAttempt(r.Context(), lease.Replica, req.Stream)
		resp, err = d.client.Do(a.ctx, lease.Replica, req)
	}
	if err != nil {
		d.report(r, lease.Replica, err, reported)
		return nil, d.failed(r, lease.Replica, err)
	}
	return newExchange(resp, &lease, a), nil
}

// retryTarget picks where to send the one connect retry after err (a
// retryable failure on the replica lease holds): another replica of the
// pool when the router offers one (RouteOther prefers healthy replicas),
// otherwise the same replica after the connect-retry backoff. It counts and
// logs the failed attempt.
//
// On return lease holds the slot for the retry: moving to another replica
// releases the failed replica's slot and takes the new one; a same-replica
// retry keeps its slot through the backoff. It returns false, having
// recorded the disconnect, if the caller goes away during the backoff; no
// retry is sent then and Send still releases lease.
func (d *Dispatcher) retryTarget(r *http.Request, pool string, lease *router.Lease, err error) bool {
	failed := lease.Replica
	if d.moveToOther(r, pool, lease) {
		d.recordError(r, failed, err, "upstream connect failed, retrying on another replica")
		return true
	}
	d.recordError(r, failed, err, "upstream connect failed, retrying on the same replica after a backoff")
	if !d.backoffWait(r.Context(), d.backoff) {
		telemetry.FromContext(r.Context()).ClientDisconnected()
		return false
	}
	return true
}

// moveToOther moves lease to a replica of pool other than the one it
// holds, for the single retry after a connect failure: the failed
// replica's slot is released and lease then holds the new one, so no other
// copy of either lease is left to release again. It returns false, leaving
// lease as it was, when the pool has no other replica to try.
func (d *Dispatcher) moveToOther(r *http.Request, pool string, lease *router.Lease) bool {
	next, err := d.router.RouteOther(r.Context(), r, pool, lease.Replica.Name)
	if err != nil {
		return false
	}
	lease.Release()
	*lease = next
	return true
}

// report passes a passive failure on replica to health tracking, if it
// says the replica is unreachable: a hard connect failure (refused, host or
// network unreachable, NXDOMAIN; see upstream.IsHardConnectFailure) while
// the caller is still there. It is not reported when already names replica:
// one request counts at most once against a replica, so a same-replica
// retry cannot double it. A caller that left, a dial timeout or connect
// budget that ran out (a healthy replica on a saturated host, ADR 0014),
// any other timeout (the replica may just be busy: vLLM queues requests)
// and a failure after the request was sent are never reported, so none of
// them can eject a healthy replica. It returns the name of the replica
// reported to, or already.
func (d *Dispatcher) report(r *http.Request, replica discovery.Replica, err error, already string) string {
	if replica.Name == already {
		return already
	}
	if upstream.IsHardConnectFailure(err) && r.Context().Err() == nil {
		if d.health != nil {
			d.health.RecordTransportFailure(replica, err)
		}
		return replica.Name
	}
	return already
}

// failed accounts for the last attempt's failure on replica and returns
// what Send reports: ErrCallerGone when the attempt was cancelled because
// the caller left, otherwise an *Error, counted and logged.
func (d *Dispatcher) failed(r *http.Request, replica discovery.Replica, err error) error {
	if upstream.KindOf(err) == upstream.KindCanceled && r.Context().Err() != nil {
		telemetry.FromContext(r.Context()).ClientDisconnected()
		return ErrCallerGone
	}
	d.recordError(r, replica, err, "upstream request failed")
	return &Error{Replica: replica, Err: err}
}

// recordError counts, traces and logs an upstream failure.
func (d *Dispatcher) recordError(r *http.Request, replica discovery.Replica, err error, msg string) {
	kind := upstream.KindOf(err)
	telemetry.FromContext(r.Context()).UpstreamError(replica.Name, string(kind))
	d.logger.WarnContext(r.Context(), msg, "pool", replica.Pool, "replica", replica.Name,
		"kind", string(kind), "err", err)
}

// sleepCtx waits d, or until ctx ends. It reports whether d elapsed with
// ctx still live: ctx is checked again after the timer fires, so a caller
// that left at the same moment counts as gone, not as a retry.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-t.C:
		case <-ctx.Done():
			return false
		}
	}
	return ctx.Err() == nil
}
