package telemetry

import (
	"io"
	"sync"
	"time"
)

const (
	defaultLogBufferBytes = 64 << 10
	defaultLogFlushTick   = 25 * time.Millisecond
)

// BufferedWriter batches structured JSON log lines in memory and flushes them
// in background chunks (or when the 64 KiB buffer fills), keeping stdout pipe
// syscalls off the request completion critical path while preserving strict
// FIFO line ordering and synchronous Flush/Close semantics on shutdown.
type BufferedWriter struct {
	w io.Writer

	writeMu  sync.Mutex // serializes writes to w so swapped buffers stay strictly FIFO
	mu       sync.Mutex // guards buf and closed
	buf      []byte
	flushBuf []byte
	closed   bool

	wake      chan struct{}
	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

// NewBufferedWriter wraps w in a double-buffered asynchronous writer that
// flushes at size/2 watermark, every flushInterval, when full, or on Close.
func NewBufferedWriter(w io.Writer, size int, flushInterval time.Duration) *BufferedWriter {
	if size <= 0 {
		size = defaultLogBufferBytes
	}
	if flushInterval <= 0 {
		flushInterval = defaultLogFlushTick
	}
	bw := &BufferedWriter{
		w:        w,
		buf:      make([]byte, 0, size),
		flushBuf: make([]byte, 0, size),
		wake:     make(chan struct{}, 1),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go bw.loop(flushInterval)
	return bw
}

func (b *BufferedWriter) loop(interval time.Duration) {
	defer close(b.done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-b.wake:
			_ = b.Flush()
		case <-ticker.C:
			_ = b.Flush()
		case <-b.stop:
			_ = b.Flush()
			return
		}
	}
}

// Write appends p to the in-memory buffer and wakes the background flusher at
// half capacity, blocking on I/O only when the buffer is full.
func (b *BufferedWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	b.mu.Lock()
	if !b.closed && len(b.buf)+len(p) <= cap(b.buf) {
		b.buf = append(b.buf, p...)
		shouldWake := len(b.buf) >= cap(b.buf)/2
		b.mu.Unlock()
		if shouldWake {
			select {
			case b.wake <- struct{}{}:
			default:
			}
		}
		return len(p), nil
	}
	b.mu.Unlock()

	// Buffer is full (or writer is closed): serialize under writeMu to preserve
	// strict FIFO order relative to any in-flight flush, then write remaining bytes.
	b.writeMu.Lock()
	defer b.writeMu.Unlock()

	b.mu.Lock()
	if len(b.buf)+len(p) <= cap(b.buf) && !b.closed {
		b.buf = append(b.buf, p...)
		b.mu.Unlock()
		return len(p), nil
	}
	toFlush := b.buf
	b.buf = b.flushBuf[:0]
	b.flushBuf = toFlush
	b.mu.Unlock()

	if len(toFlush) > 0 {
		if _, err := b.w.Write(toFlush); err != nil {
			return 0, err
		}
	}
	return b.w.Write(p)
}

// Flush synchronously writes any buffered bytes to the underlying writer.
func (b *BufferedWriter) Flush() error {
	b.writeMu.Lock()
	defer b.writeMu.Unlock()

	b.mu.Lock()
	if len(b.buf) == 0 {
		b.mu.Unlock()
		return nil
	}
	toFlush := b.buf
	b.buf = b.flushBuf[:0]
	b.flushBuf = toFlush
	b.mu.Unlock()

	_, err := b.w.Write(toFlush)
	return err
}

// Close stops the background flusher and flushes all remaining buffered bytes.
func (b *BufferedWriter) Close() error {
	b.closeOnce.Do(func() {
		b.mu.Lock()
		b.closed = true
		b.mu.Unlock()
		close(b.stop)
		<-b.done
	})
	return b.Flush()
}
