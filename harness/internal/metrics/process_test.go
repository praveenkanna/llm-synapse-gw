package metrics

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseCPUTime(t *testing.T) {
	tests := []struct {
		in   string
		want float64
		bad  bool
	}{
		{in: "0:01.50", want: 1.5},       // macOS mm:ss.ss
		{in: "12:34.00", want: 754},      // macOS
		{in: "01:02:03", want: 3723},     // Linux hh:mm:ss
		{in: "2-00:00:01", want: 172801}, // Linux dd-hh:mm:ss
		{in: "x:01", bad: true},
		{in: "1:2:3:4", bad: true},
		{in: "a-1:00", bad: true},
	}
	for _, tt := range tests {
		got, err := parseCPUTime(tt.in)
		if (err != nil) != tt.bad || (!tt.bad && got != tt.want) {
			t.Errorf("parseCPUTime(%q) = %v, %v; want %v (bad=%v)", tt.in, got, err, tt.want, tt.bad)
		}
	}
}

func TestParsePS(t *testing.T) {
	got, err := parsePS("  20480   0:03.25\n")
	if err != nil || got.RSSBytes != 20<<20 || got.CPUSeconds != 3.25 {
		t.Fatalf("parsePS = %+v, %v", got, err)
	}
	for _, bad := range []string{"", "12", "x 0:01", "12 zz"} {
		if _, err := parsePS(bad); err == nil {
			t.Errorf("parsePS(%q): want error", bad)
		}
	}
}

func TestPIDProbeProc(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "42")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	// comm holds spaces and a parenthesis; utime=250 stime=50 ticks.
	stat := "42 (gate way) x) S 1 42 42 0 -1 4194560 100 0 0 0 250 50 0 0 20 0 8 0 100 1000000 5000\n"
	status := "Name:\tgateway\nVmPeak:\t  90000 kB\nVmRSS:\t   51200 kB\nThreads:\t8\n"
	for name, body := range map[string]string{"stat": stat, "status": status} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	noPS := func(context.Context, int) (string, error) { return "", errors.New("ps must not run") }
	got, err := pidProbe(42, root, noPS)(context.Background())
	if err != nil || got.CPUSeconds != 3 || got.RSSBytes != 50<<20 {
		t.Fatalf("proc probe = %+v, %v", got, err)
	}

	// No /proc entry: falls back to ps.
	ps := func(_ context.Context, pid int) (string, error) { return fmt.Sprintf("1024 0:0%d.00", pid), nil }
	got, err = pidProbe(7, root, ps)(context.Background())
	if err != nil || got.CPUSeconds != 7 || got.RSSBytes != 1<<20 {
		t.Fatalf("ps probe = %+v, %v", got, err)
	}
}

func TestPIDProbeSelf(t *testing.T) {
	got, err := PIDProbe(os.Getpid())(context.Background())
	if err != nil {
		t.Skipf("no /proc or ps here: %v", err)
	}
	if got.RSSBytes == 0 {
		t.Fatalf("own RSS = 0: %+v", got)
	}
}

func TestPromProbe(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			fmt.Fprint(w, "process_resident_memory_bytes 1.048576e+08\nprocess_cpu_seconds_total 12.5\n")
			return
		}
		fmt.Fprint(w, "other 1\n")
	}))
	defer srv.Close()
	got, err := PromProbe(Scraper{}, srv.URL)(context.Background())
	if err != nil || got.RSSBytes != 100<<20 || got.CPUSeconds != 12.5 {
		t.Fatalf("prom probe = %+v, %v", got, err)
	}
	if _, err := PromProbe(Scraper{}, srv.URL+"/other")(context.Background()); err == nil {
		t.Fatal("page without process metrics: want error")
	}
}

func TestSamplerAggregates(t *testing.T) {
	s, err := NewSampler("test", func(context.Context) (ProcSample, error) { return ProcSample{}, nil }, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Unix(1000, 0)
	s.add(ProcSample{RSSBytes: 100 << 20, CPUSeconds: 10}, t0)
	s.add(ProcSample{RSSBytes: 300 << 20, CPUSeconds: 12}, t0.Add(time.Second))   // 2 cores
	s.add(ProcSample{RSSBytes: 200 << 20, CPUSeconds: 13}, t0.Add(2*time.Second)) // 1 core
	// A final sample 1 ms later: its rate (10 cores) is granularity noise, not a peak.
	s.add(ProcSample{RSSBytes: 200 << 20, CPUSeconds: 13.01}, t0.Add(2*time.Second+time.Millisecond))
	st := s.Stats()
	want := ProcessStats{
		Source: "test", Samples: 4, WallSeconds: 2.001, CPUSeconds: 3.01, CPUCoresMean: 3.01 / 2.001, CPUCoresMax: 2,
		RSSStartMB: 100, RSSEndMB: 200, RSSMaxMB: 300, RSSMeanMB: 200,
	}
	if st != want {
		t.Fatalf("stats = %+v\nwant    %+v", st, want)
	}
	if _, err := NewSampler("x", nil, time.Second); err == nil {
		t.Fatal("nil probe: want error")
	}
}

func TestSamplerRun(t *testing.T) {
	var n atomic.Int64
	probe := func(context.Context) (ProcSample, error) {
		i := n.Add(1)
		if i == 2 {
			return ProcSample{}, errors.New("transient")
		}
		return ProcSample{RSSBytes: uint64(i) << 20, CPUSeconds: float64(i)}, nil
	}
	s, _ := NewSampler("run", probe, 5*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.Run(ctx) }()
	for n.Load() < 5 {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	st := s.Stats()
	if st.Errors != 1 || st.LastError != "transient" || st.Samples < 4 || st.RSSStartMB != 1 {
		t.Fatalf("stats = %+v", st)
	}
	if int64(st.Samples+st.Errors) != n.Load() {
		t.Fatalf("samples+errors = %d, probes = %d", st.Samples+st.Errors, n.Load())
	}
}

// The sample taken after Run's context ends must not be bounded by a short
// sampling interval: a probe slower than the interval (a busy host, a 5 ms
// interval in a test) would lose the final sample and its RSS/CPU readings.
func TestSamplerFinalSampleOutlivesShortInterval(t *testing.T) {
	var n atomic.Int64
	probe := func(ctx context.Context) (ProcSample, error) {
		i := n.Add(1)
		select {
		case <-time.After(20 * time.Millisecond): // slower than the 2 ms interval
			return ProcSample{RSSBytes: uint64(i) << 20, CPUSeconds: float64(i)}, nil
		case <-ctx.Done():
			return ProcSample{}, ctx.Err()
		}
	}
	s, _ := NewSampler("final", probe, 2*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.Run(ctx) }()
	for s.Stats().Samples < 1 { // the first sample is in, then stop
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	st := s.Stats()
	// The last probe call is the final sample; it must have been recorded.
	if want := float64(n.Load()); st.RSSEndMB != want {
		t.Fatalf("RSSEndMB = %v, want %v (the final sample was lost): %+v", st.RSSEndMB, want, st)
	}
}
