package mockllm

import (
	"sync"
	"testing"
	"time"
)

// refuseSettle is how long a test waits for a refusal window that must NOT
// reopen the socket; it is several times the window length used below.
const (
	refuseWindow = 20 * time.Millisecond
	refuseSettle = 10 * refuseWindow
)

// TestRefuseForConcurrent races many RefuseFor calls, then pins the socket
// shut with SetRefusing(true). No superseded window may reopen it later.
// Before the fix, an overwritten timer was never stopped and reopened the
// socket behind the caller's back.
func TestRefuseForConcurrent(t *testing.T) {
	srv := startMock(t, nil)
	const callers = 32
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- srv.RefuseFor(refuseWindow)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("RefuseFor: %v", err)
		}
	}
	if !srv.Refusing() {
		t.Fatal("not refusing right after RefuseFor")
	}
	if err := srv.SetRefusing(true); err != nil {
		t.Fatalf("SetRefusing(true): %v", err)
	}
	time.Sleep(refuseSettle)
	if !srv.Refusing() {
		t.Fatal("a superseded RefuseFor window reopened the socket")
	}
}

// TestRefuseForLatestWindowWins checks that a short window replaced by a long
// one does not reopen early, and that the long window still ends on its own.
func TestRefuseForLatestWindowWins(t *testing.T) {
	srv := startMock(t, nil)
	if err := srv.RefuseFor(refuseWindow); err != nil {
		t.Fatalf("RefuseFor short: %v", err)
	}
	if err := srv.RefuseFor(refuseSettle); err != nil {
		t.Fatalf("RefuseFor long: %v", err)
	}
	time.Sleep(3 * refuseWindow)
	if !srv.Refusing() {
		t.Fatal("replaced short window reopened the socket early")
	}
	waitFor(t, "long window to end", func() bool { return !srv.Refusing() })
}

// TestRefuseWindowStaleCallback runs a superseded window's callback directly,
// as if its timer had fired while a newer call held the lock.
func TestRefuseWindowStaleCallback(t *testing.T) {
	srv := startMock(t, nil)
	if err := srv.RefuseFor(time.Hour); err != nil {
		t.Fatalf("RefuseFor: %v", err)
	}
	srv.timerMu.Lock()
	stale := srv.timerGen
	srv.timerMu.Unlock()
	if err := srv.RefuseFor(time.Hour); err != nil {
		t.Fatalf("RefuseFor again: %v", err)
	}
	srv.endRefuseWindow(stale)
	if !srv.Refusing() {
		t.Fatal("stale callback reopened the socket")
	}

	srv.timerMu.Lock()
	current := srv.timerGen
	srv.timerMu.Unlock()
	srv.endRefuseWindow(current)
	if srv.Refusing() {
		t.Fatal("current callback did not reopen the socket")
	}
}
