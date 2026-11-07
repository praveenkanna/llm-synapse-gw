//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/mockllm"
)

// AC-M0-7 at full scale: 1,000 client aborts through nginx and the gateways,
// each cancelling its upstream request at mockllm within 1 s. With the
// gateways' metrics listeners reachable, also AC-M2-4: afterwards every
// admission slot and queue on both gateways is back to 0 within 5 s.
//
// Knobs (environment):
//
//	INTEGRATION_ABORTS         total aborts                        1000
//	INTEGRATION_ABORT_WAVE     clients aborted together per wave   50
//	INTEGRATION_ABORT_RESULTS  write a JSON summary to this path   (off)
//	INTEGRATION_GATEWAY_METRICS_URLS  gateways' /metrics, comma-sep   (off; in-network:
//	                           http://gateway-1:9090/metrics,http://gateway-2:9090/metrics)
//
// A wave must fit the large pool's admission capacity (64 slots in the dev
// stack, gateway.dev.yaml), or its excess queues at the gateway and never
// reaches mockllm.
const (
	envAborts        = "INTEGRATION_ABORTS"
	envAbortWave     = "INTEGRATION_ABORT_WAVE"
	envAbortResults  = "INTEGRATION_ABORT_RESULTS"
	envGatewayMetric = "INTEGRATION_GATEWAY_METRICS_URLS"
	defaultAborts    = 1000
	defaultAbortWave = 50

	// slotsDrainBound is AC-M2-4's bound for admission counters to return
	// to 0.
	slotsDrainBound = 5 * time.Second

	// cancelDeadline is AC-M0-7's bound.
	cancelDeadline = time.Second
	// waveStartTimeout bounds getting a wave's requests running at mockllm.
	waveStartTimeout = 30 * time.Second
	// drainTimeout bounds mockllm releasing a wave after it was cancelled.
	drainTimeout = 5 * time.Second
	// statsPollInterval is how often /mock/stats is read while waiting.
	statsPollInterval = 2 * time.Millisecond

	// midStreamTokens keeps a stream decoding far longer than a wave lasts
	// (about 100 s at mockllm's default 50 tok/s) and stays inside the
	// mock's context room, so it never completes on its own.
	midStreamTokens = 5000
	chatPath        = "/v1/chat/completions"
	abortModel      = "large-model"
)

// abortKind is where in the exchange the client goes away.
type abortKind string

const (
	// beforeHeaders: mockllm never answers (X-Mock-Fault: hang), so the client
	// leaves before any response header. Streaming and non-streaming alternate.
	beforeHeaders abortKind = "before-headers"
	// midStream: the client has read the first SSE bytes, then leaves.
	midStream abortKind = "mid-stream"
)

// waveResult is one wave: n clients of one kind aborted at the same instant.
type waveResult struct {
	Kind    abortKind `json:"kind"`
	Clients int       `json:"clients"`
	// CancelMs is the time from the abort until mockllm counted every
	// request of the wave cancelled; an upper bound (poll granularity) on
	// each request's own cancel latency, since all left at once.
	CancelMs float64 `json:"cancel_ms"`
}

// abortSummary is the JSON written to INTEGRATION_ABORT_RESULTS.
type abortSummary struct {
	Started    time.Time    `json:"started"`
	GatewayURL string       `json:"gateway_url"`
	Aborts     int          `json:"aborts"`
	Cancelled  uint64       `json:"cancelled"`
	Completed  uint64       `json:"completed"`
	Requests   uint64       `json:"requests"`
	DeadlineMs float64      `json:"deadline_ms"`
	MaxMs      float64      `json:"max_cancel_ms"`
	P50Ms      float64      `json:"p50_cancel_ms"`
	ByKind     []kindResult `json:"by_kind"`
	Waves      []waveResult `json:"waves"`
}

type kindResult struct {
	Kind    abortKind `json:"kind"`
	Aborts  int       `json:"aborts"`
	MaxMs   float64   `json:"max_cancel_ms"`
	Within1 bool      `json:"within_deadline"`
}

func TestClientAbortsCancelUpstreamAtScale(t *testing.T) {
	st := newStack(t)
	total := envInt(t, envAborts, defaultAborts)
	waveSize := envInt(t, envAbortWave, defaultAbortWave)
	ctx := context.Background()

	start, err := st.stats(ctx)
	if err != nil {
		t.Fatalf("read /mock/stats: %v", err)
	}
	if start.Running != 0 || start.Waiting != 0 {
		t.Fatalf("mockllm is busy before the test (running %d, waiting %d); run it on an idle stack", start.Running, start.Waiting)
	}
	summary := abortSummary{Started: time.Now().UTC(), GatewayURL: st.gatewayURL, Aborts: total, DeadlineMs: ms(cancelDeadline)}

	kinds := []abortKind{beforeHeaders, midStream}
	for w, sent := 0, 0; sent < total; w++ {
		n := min(waveSize, total-sent)
		kind := kinds[w%len(kinds)]
		res := runAbortWave(t, st, kind, n)
		summary.Waves = append(summary.Waves, res)
		t.Logf("wave %2d %-14s %4d clients: all upstream requests cancelled %.1f ms after the abort", w+1, kind, n, res.CancelMs)
		sent += n
	}

	end, err := st.stats(ctx)
	if err != nil {
		t.Fatalf("read /mock/stats: %v", err)
	}
	summary.Requests = end.Requests - start.Requests
	summary.Cancelled = end.Cancelled - start.Cancelled
	summary.Completed = end.Completed - start.Completed
	summarize(&summary)
	writeSummary(t, summary)

	t.Logf("%d aborts: %d upstream requests, %d cancelled (%.1f%%), %d completed; cancel latency p50 %.1f ms, max %.1f ms (bound %.0f ms)",
		total, summary.Requests, summary.Cancelled, 100*float64(summary.Cancelled)/float64(total), summary.Completed,
		summary.P50Ms, summary.MaxMs, summary.DeadlineMs)
	if summary.Requests != uint64(total) {
		t.Errorf("mockllm received %d generation requests for %d client requests; the path must neither drop nor retry them", summary.Requests, total)
	}
	if summary.Cancelled != uint64(total) || summary.Completed != 0 {
		t.Errorf("cancelled %d and completed %d of %d; every aborted request must be cancelled upstream", summary.Cancelled, summary.Completed, total)
	}
	if d := end.Rejected - start.Rejected; d != 0 {
		t.Errorf("mockllm rejected %d requests", d)
	}
	assertAdmissionDrained(t, st)
}

// runAbortWave starts n clients of one kind, waits until mockllm is running
// all n, aborts them together and measures how long mockllm takes to count
// all n cancelled. It fails the test past cancelDeadline.
func runAbortWave(t *testing.T, st *stack, kind abortKind, n int) waveResult {
	t.Helper()
	bg := context.Background()
	base, err := st.stats(bg)
	if err != nil {
		t.Fatalf("read /mock/stats: %v", err)
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = n
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}

	ctx, abort := context.WithCancel(bg)
	var (
		wg      sync.WaitGroup
		early   atomic.Int64 // responses or stream ends seen before the abort
		failed  atomic.Int64 // requests that errored before the abort
		aborted atomic.Bool
		started = make(chan struct{}, n)
	)
	// On any exit, including t.Fatalf, end every client before returning so
	// none logs after the test.
	defer func() {
		aborted.Store(true)
		abort()
		wg.Wait()
	}()
	for i := range n {
		wg.Go(func() {
			err := abortingClient(ctx, client, st.gatewayURL, kind, i, started)
			if aborted.Load() {
				return
			}
			if err != nil {
				failed.Add(1)
				t.Logf("%s client %d failed before the abort: %v", kind, i, err)
				return
			}
			early.Add(1)
		})
	}

	if kind == midStream {
		waitStarted(t, started, n)
	}
	waitStats(t, st, waveStartTimeout, fmt.Sprintf("%d %s requests running at mockllm", n, kind), func(s mockllm.Stats) (bool, error) {
		if got := s.Requests - base.Requests; got > uint64(n) {
			return false, fmt.Errorf("mockllm received %d requests for %d clients (a retry?)", got, n)
		}
		return s.Running-base.Running == int64(n) && s.Requests-base.Requests == uint64(n), nil
	})
	if e, f := early.Load(), failed.Load(); e+f > 0 {
		t.Fatalf("%s wave: %d clients got a response or stream end and %d failed before the abort", kind, e, f)
	}

	aborted.Store(true)
	abortAt := time.Now()
	abort()
	want := base.Cancelled + uint64(n)
	var got uint64
	for {
		s, err := st.stats(bg)
		if err != nil {
			t.Fatalf("read /mock/stats: %v", err)
		}
		if got = s.Cancelled; got >= want {
			break
		}
		if time.Since(abortAt) > cancelDeadline {
			t.Fatalf("%s wave: %s after the abort only %d of %d upstream requests were cancelled",
				kind, cancelDeadline, got-base.Cancelled, n)
		}
		time.Sleep(statsPollInterval)
	}
	latency := time.Since(abortAt)
	if latency > cancelDeadline {
		t.Fatalf("%s wave: upstream requests cancelled %s after the abort, over the %s bound", kind, latency, cancelDeadline)
	}
	wg.Wait()
	waitStats(t, st, drainTimeout, "mockllm released the wave", func(s mockllm.Stats) (bool, error) {
		return s.Running == base.Running && s.Waiting == base.Waiting, nil
	})
	return waveResult{Kind: kind, Clients: n, CancelMs: ms(latency)}
}

// abortingClient sends one chat completion through the gateway and holds it
// until ctx is cancelled. A mid-stream client signals started after the first
// response bytes. It returns nil if the exchange ended on its own (response
// headers before the abort, or a stream that finished) and the error otherwise;
// after the abort the error is the expected context cancellation.
func abortingClient(ctx context.Context, client *http.Client, base string, kind abortKind, i int, started chan<- struct{}) error {
	stream := kind == midStream || i%2 == 0
	body := `{"model":"` + abortModel + `","messages":[{"role":"user","content":"integration abort ` + strconv.Itoa(i) + `"}],"stream":` + strconv.FormatBool(stream) + `}`
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+chatPath, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if kind == beforeHeaders {
		req.Header.Set(mockllm.HeaderFault, "hang")
	} else {
		req.Header.Set(mockllm.HeaderOutputTokens, strconv.Itoa(midStreamTokens))
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if kind == beforeHeaders {
		return nil // headers arrived: the upstream answered, which hang must not
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	buf := make([]byte, 256)
	if _, err := resp.Body.Read(buf); err != nil {
		return fmt.Errorf("first stream bytes: %w", err)
	}
	started <- struct{}{}
	_, err = io.Copy(io.Discard, resp.Body)
	return err
}

func waitStarted(t *testing.T, started <-chan struct{}, n int) {
	t.Helper()
	deadline := time.After(waveStartTimeout)
	for range n {
		select {
		case <-started:
		case <-deadline:
			t.Fatalf("streams did not start within %s", waveStartTimeout)
		}
	}
}

// waitStats polls /mock/stats until cond holds, cond errors, or timeout.
func waitStats(t *testing.T, st *stack, timeout time.Duration, what string, cond func(mockllm.Stats) (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		s, err := st.stats(context.Background())
		if err != nil {
			t.Fatalf("read /mock/stats: %v", err)
		}
		ok, err := cond(s)
		if err != nil {
			t.Fatalf("waiting for %s: %v", what, err)
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s (stats %+v)", timeout, what, s)
		}
		time.Sleep(statsPollInterval)
	}
}

// summarize fills the latency aggregates from the waves.
func summarize(s *abortSummary) {
	all := make([]float64, 0, len(s.Waves))
	byKind := map[abortKind]*kindResult{}
	var order []abortKind
	for _, w := range s.Waves {
		all = append(all, w.CancelMs)
		k, ok := byKind[w.Kind]
		if !ok {
			k = &kindResult{Kind: w.Kind}
			byKind[w.Kind] = k
			order = append(order, w.Kind)
		}
		k.Aborts += w.Clients
		k.MaxMs = max(k.MaxMs, w.CancelMs)
	}
	for _, kind := range order {
		k := byKind[kind]
		k.Within1 = k.MaxMs <= s.DeadlineMs
		s.ByKind = append(s.ByKind, *k)
	}
	if len(all) == 0 {
		return
	}
	slices.Sort(all)
	s.MaxMs = all[len(all)-1]
	s.P50Ms = all[(len(all)-1)/2]
}

func writeSummary(t *testing.T, s abortSummary) {
	t.Helper()
	path := os.Getenv(envAbortResults)
	if path == "" {
		return
	}
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		t.Fatalf("encode summary: %v", err)
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	t.Logf("summary written to %s", path)
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

// assertAdmissionDrained checks AC-M2-4: within slotsDrainBound every
// gateway in INTEGRATION_GATEWAY_METRICS_URLS reports no admission slot in
// use and nothing queued. Without the variable it does nothing.
func assertAdmissionDrained(t *testing.T, st *stack) {
	t.Helper()
	var urls []string
	for _, u := range strings.Split(os.Getenv(envGatewayMetric), ",") {
		if u = strings.TrimSpace(u); u != "" {
			urls = append(urls, u)
		}
	}
	if len(urls) == 0 {
		t.Logf("%s not set: admission counters not checked", envGatewayMetric)
		return
	}
	start := time.Now()
	for {
		var held float64
		for _, u := range urls {
			v, err := sumMetrics(st, u, "gateway_admission_slots_in_use", "gateway_queue_depth")
			if err != nil {
				t.Fatalf("read %s: %v", u, err)
			}
			held += v
		}
		if held == 0 {
			t.Logf("admission slots and queues at 0 on %d gateways %.0f ms after the last wave", len(urls), ms(time.Since(start)))
			return
		}
		if time.Since(start) > slotsDrainBound {
			t.Fatalf("AC-M2-4: %v slots or queued requests still held %s after the last wave", held, slotsDrainBound)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// sumMetrics sums every sample of the named families on a Prometheus text
// endpoint.
func sumMetrics(st *stack, url string, names ...string) (float64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), statsRequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := st.control.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	var sum float64
	for line := range strings.SplitSeq(string(b), "\n") {
		for _, name := range names {
			if !strings.HasPrefix(line, name+"{") && !strings.HasPrefix(line, name+" ") {
				continue
			}
			f := strings.Fields(line)
			v, err := strconv.ParseFloat(f[len(f)-1], 64)
			if err != nil {
				return 0, fmt.Errorf("%q: %w", line, err)
			}
			sum += v
		}
	}
	return sum, nil
}
