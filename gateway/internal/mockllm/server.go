package mockllm

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

// readHeaderTimeout bounds slow-header clients. No write timeout: streams and
// the hang fault may legitimately stay open for a long time.
const readHeaderTimeout = 10 * time.Second

// Server runs a Mock on a listener that can refuse connections on demand.
type Server struct {
	mock *Mock
	ln   *refusableListener
	srv  *http.Server

	stopped  chan struct{} // closed when Serve returns
	serveErr error         // Serve's result; read only after stopped is closed

	timerMu  sync.Mutex  // guards timer and timerGen; taken before the listener's lock
	timer    *time.Timer // pending end of a RefuseFor window
	timerGen uint64      // bumped whenever the pending window is replaced or cancelled
}

// Start listens on cfg.ListenAddr and serves in the background until Close
// or Shutdown. Use ListenAddr "127.0.0.1:0" in tests.
func Start(cfg Config, logger *slog.Logger) (*Server, error) {
	mock, err := New(cfg)
	if err != nil {
		return nil, err
	}
	ln, err := newRefusableListener(cfg.ListenAddr)
	if err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	s := &Server{
		mock: mock,
		ln:   ln,
		srv: &http.Server{
			Handler:           mock,
			ReadHeaderTimeout: readHeaderTimeout,
			ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
		},
		stopped: make(chan struct{}),
	}
	mock.setRefuser(s.RefuseFor)
	go func() { // exits when srv is closed or shut down
		s.serveErr = s.srv.Serve(ln)
		close(s.stopped)
	}()
	return s, nil
}

// Mock returns the handler, for Stats and captured requests.
func (s *Server) Mock() *Mock { return s.mock }

// Addr returns the bound host:port.
func (s *Server) Addr() string { return s.ln.Addr().String() }

// URL returns the base URL, e.g. http://127.0.0.1:41234.
func (s *Server) URL() string { return "http://" + s.Addr() }

// Done is closed once the server has stopped serving.
func (s *Server) Done() <-chan struct{} { return s.stopped }

// SetRefusing closes (true) or reopens (false) the listening socket. While
// closed, new connections are refused; open connections keep working. It
// cancels any pending RefuseFor window.
func (s *Server) SetRefusing(refuse bool) error {
	s.timerMu.Lock()
	defer s.timerMu.Unlock()
	s.cancelRefuseTimerLocked()
	return s.ln.SetRefusing(refuse)
}

// Refusing reports whether new connections are currently refused.
func (s *Server) Refusing() bool { return s.ln.Refusing() }

// RefuseFor refuses new connections for d, then listens again. A later call
// (or SetRefusing, Shutdown, Close) replaces the window; only the latest one
// ever reopens the socket. Safe for concurrent use.
func (s *Server) RefuseFor(d time.Duration) error {
	s.timerMu.Lock()
	defer s.timerMu.Unlock()
	s.cancelRefuseTimerLocked()
	if err := s.ln.SetRefusing(true); err != nil {
		return err
	}
	gen := s.timerGen
	s.timer = time.AfterFunc(d, func() { s.endRefuseWindow(gen) })
	return nil
}

// endRefuseWindow reopens the socket unless window gen has been superseded.
// The generation check covers a timer that fired while a newer call held
// timerMu: Stop cannot recall a callback that is already running.
func (s *Server) endRefuseWindow(gen uint64) {
	s.timerMu.Lock()
	defer s.timerMu.Unlock()
	if gen != s.timerGen {
		return
	}
	s.timer = nil
	if err := s.ln.SetRefusing(false); err != nil && !errors.Is(err, net.ErrClosed) {
		s.srv.ErrorLog.Printf("mockllm: resume listening: %v", err)
	}
}

// cancelRefuseTimerLocked stops the pending window and invalidates any
// callback already in flight. Caller holds timerMu.
func (s *Server) cancelRefuseTimerLocked() {
	s.timerGen++
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
}

func (s *Server) stopRefuseTimer() {
	s.timerMu.Lock()
	defer s.timerMu.Unlock()
	s.cancelRefuseTimerLocked()
}

// Shutdown stops accepting, waits for open requests until ctx ends, then
// closes whatever is left (hung requests included).
func (s *Server) Shutdown(ctx context.Context) error {
	s.stopRefuseTimer()
	err := s.srv.Shutdown(ctx)
	if err != nil {
		_ = s.srv.Close()
	}
	return s.wait(err)
}

// Close stops the server immediately, cancelling every open request.
func (s *Server) Close() error {
	s.stopRefuseTimer()
	return s.wait(s.srv.Close())
}

func (s *Server) wait(stopErr error) error {
	<-s.stopped
	serveErr := s.serveErr
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}
	if serveErr != nil {
		serveErr = fmt.Errorf("serve %s: %w", s.Addr(), serveErr)
	}
	return errors.Join(stopErr, serveErr)
}
