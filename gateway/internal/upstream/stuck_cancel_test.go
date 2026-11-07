package upstream

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestCancelCauseReplicaStuck: a request cancelled by its caller with
// ErrReplicaStuck fails as KindStuck, never as a client cancellation or a
// timeout, and is not retryable (a generation never runs twice). A plain
// cancellation keeps its kind.
func TestCancelCauseReplicaStuck(t *testing.T) {
	tests := []struct {
		name  string
		cause error
		want  Kind
	}{
		{name: "replica ejected as stuck", cause: ErrReplicaStuck, want: KindStuck},
		{name: "caller left", cause: context.Canceled, want: KindCanceled},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := chunkServer(t, 0, 0, true) // headers at once, then nothing
			c := New(testUpstream(nil))
			defer c.CloseIdleConnections()
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			resp, err := c.Do(ctx, replicaFor(t, srv.URL), Request{Method: http.MethodPost, Path: "/v1/chat/completions", Stream: true})
			if err != nil {
				t.Fatalf("Do: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()
			time.AfterFunc(20*time.Millisecond, func() { cancel(tc.cause) })
			_, err = io.Copy(io.Discard, resp.Body)
			if err == nil || KindOf(err) != tc.want || Retryable(err) {
				t.Fatalf("read err = %v (kind %s, retryable %v), want kind %s, not retryable", err, KindOf(err), Retryable(err), tc.want)
			}
			if tc.want == KindStuck && !errors.Is(err, ErrReplicaStuck) {
				t.Errorf("err %v does not wrap ErrReplicaStuck", err)
			}
		})
	}
}

// TestCancelCauseBeforeHeaders: a replica that never sends its response
// headers; the cancel lands while Do still waits for them. The error is
// KindStuck for ErrReplicaStuck (not a canceled caller, not a response
// header timeout, not retryable) and keeps its kind for a plain cancel.
func TestCancelCauseBeforeHeaders(t *testing.T) {
	tests := []struct {
		name  string
		cause error
		want  Kind
	}{
		{name: "replica ejected as stuck", cause: ErrReplicaStuck, want: KindStuck},
		{name: "caller left", cause: context.Canceled, want: KindCanceled},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			arrived := make(chan struct{}, 1)
			srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				arrived <- struct{}{} // the request reached the replica: Do now waits for headers
				<-r.Context().Done()
			}))
			t.Cleanup(srv.Close)
			c := New(testUpstream(nil)) // stream header timeout 2 s: the cancel comes first
			defer c.CloseIdleConnections()
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			go func() {
				<-arrived
				cancel(tc.cause)
			}()
			resp, err := c.Do(ctx, replicaFor(t, srv.URL), Request{Method: http.MethodPost, Path: "/v1/chat/completions", Stream: true})
			if err == nil {
				_ = resp.Body.Close()
				t.Fatal("Do succeeded against a replica that never answers")
			}
			if KindOf(err) != tc.want || Retryable(err) {
				t.Fatalf("err = %v (kind %s, retryable %v), want kind %s, not retryable", err, KindOf(err), Retryable(err), tc.want)
			}
			var ue *Error
			if !errors.As(err, &ue) || ue.Kind != tc.want {
				t.Fatalf("err = %v, want *Error of kind %s", err, tc.want)
			}
			if got := errors.Is(err, ErrReplicaStuck); got != (tc.want == KindStuck) {
				t.Errorf("errors.Is(err, ErrReplicaStuck) = %v for kind %s", got, tc.want)
			}
			if errors.Is(err, ErrResponseHeaderTimeout) {
				t.Errorf("err %v reports a header timeout", err)
			}
		})
	}
}
