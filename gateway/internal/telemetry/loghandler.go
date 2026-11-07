package telemetry

import (
	"context"
	"log/slog"
)

// NewLogHandler wraps h so that records logged with a request context (for
// example logger.WarnContext(r.Context(), ...)) carry that request's trace_id.
func NewLogHandler(h slog.Handler) slog.Handler {
	return traceHandler{h}
}

type traceHandler struct{ slog.Handler }

func (h traceHandler) Handle(ctx context.Context, rec slog.Record) error {
	if id := FromContext(ctx).TraceID(); id != "" {
		rec = rec.Clone()
		rec.AddAttrs(slog.String("trace_id", id))
	}
	return h.Handler.Handle(ctx, rec)
}

func (h traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return traceHandler{h.Handler.WithAttrs(attrs)}
}

func (h traceHandler) WithGroup(name string) slog.Handler {
	return traceHandler{h.Handler.WithGroup(name)}
}
