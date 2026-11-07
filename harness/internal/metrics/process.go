package metrics

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// userHZ is the Linux clock-tick rate for /proc/<pid>/stat CPU times. It is
// 100 on every mainstream kernel/arch (the ABI value, independent of CONFIG_HZ).
const userHZ = 100

// ProcSample is one reading of a process: resident memory and cumulative CPU.
type ProcSample struct {
	RSSBytes   uint64
	CPUSeconds float64
}

// Probe reads a process once.
type Probe func(ctx context.Context) (ProcSample, error)

// PIDProbe reads a local process by PID: from /proc when present (Linux,
// including inside a container sharing the target's PID namespace), else
// with ps (macOS).
func PIDProbe(pid int) Probe {
	return pidProbe(pid, "/proc", runPS)
}

func pidProbe(pid int, procRoot string, ps func(ctx context.Context, pid int) (string, error)) Probe {
	dir := filepath.Join(procRoot, strconv.Itoa(pid))
	if _, err := os.Stat(filepath.Join(dir, "stat")); err == nil {
		return func(context.Context) (ProcSample, error) { return readProc(dir) }
	}
	return func(ctx context.Context) (ProcSample, error) {
		out, err := ps(ctx, pid)
		if err != nil {
			return ProcSample{}, err
		}
		return parsePS(out)
	}
}

func runPS(ctx context.Context, pid int) (string, error) {
	out, err := exec.CommandContext(ctx, "ps", "-o", "rss=,time=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return "", fmt.Errorf("ps -p %d: %w", pid, err)
	}
	return string(out), nil
}

// readProc reads VmRSS from status and utime+stime from stat.
func readProc(dir string) (ProcSample, error) {
	var s ProcSample
	stat, err := os.ReadFile(filepath.Join(dir, "stat"))
	if err != nil {
		return s, err
	}
	// Fields after the command name, which is in parentheses and may hold spaces.
	i := strings.LastIndexByte(string(stat), ')')
	if i < 0 {
		return s, errors.New("proc stat: no command field")
	}
	f := strings.Fields(string(stat[i+1:]))
	if len(f) < 13 { // utime and stime are fields 14 and 15 overall, 12 and 13 here
		return s, errors.New("proc stat: too few fields")
	}
	ut, err1 := strconv.ParseUint(f[11], 10, 64)
	st, err2 := strconv.ParseUint(f[12], 10, 64)
	if err := errors.Join(err1, err2); err != nil {
		return s, fmt.Errorf("proc stat: %w", err)
	}
	s.CPUSeconds = float64(ut+st) / userHZ

	status, err := os.ReadFile(filepath.Join(dir, "status"))
	if err != nil {
		return s, err
	}
	for line := range strings.Lines(string(status)) {
		if rest, ok := strings.CutPrefix(line, "VmRSS:"); ok {
			kb, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimSpace(rest), " kB"), 10, 64)
			if err != nil {
				return s, fmt.Errorf("proc status VmRSS: %w", err)
			}
			s.RSSBytes = kb << 10
			return s, nil
		}
	}
	return s, errors.New("proc status: no VmRSS")
}

// parsePS parses `ps -o rss=,time=`: RSS in KiB, then cumulative CPU time as
// [[dd-]hh:]mm:ss[.ss].
func parsePS(out string) (ProcSample, error) {
	f := strings.Fields(out)
	if len(f) != 2 {
		return ProcSample{}, fmt.Errorf("ps: unexpected output %q (process gone?)", strings.TrimSpace(out))
	}
	kb, err := strconv.ParseUint(f[0], 10, 64)
	if err != nil {
		return ProcSample{}, fmt.Errorf("ps rss: %w", err)
	}
	cpu, err := parseCPUTime(f[1])
	if err != nil {
		return ProcSample{}, err
	}
	return ProcSample{RSSBytes: kb << 10, CPUSeconds: cpu}, nil
}

func parseCPUTime(s string) (float64, error) {
	var days float64
	if d, rest, ok := strings.Cut(s, "-"); ok {
		n, err := strconv.ParseFloat(d, 64)
		if err != nil {
			return 0, fmt.Errorf("ps time %q: %w", s, err)
		}
		days, s = n, rest
	}
	parts := strings.Split(s, ":")
	if len(parts) > 3 {
		return 0, fmt.Errorf("ps time %q: too many fields", s)
	}
	var secs float64
	for _, p := range parts {
		n, err := strconv.ParseFloat(p, 64)
		if err != nil {
			return 0, fmt.Errorf("ps time %q: %w", s, err)
		}
		secs = secs*60 + n
	}
	return days*86400 + secs, nil
}

// Standard process metrics of Prometheus client libraries.
const (
	metricProcRSS = "process_resident_memory_bytes"
	metricProcCPU = "process_cpu_seconds_total"
)

// PromProbe reads process_resident_memory_bytes and process_cpu_seconds_total
// from a /metrics page, for a process in another container or host.
func PromProbe(s Scraper, target string) Probe {
	return func(ctx context.Context) (ProcSample, error) {
		snap, err := s.Scrape(ctx, target)
		if err != nil {
			return ProcSample{}, err
		}
		rss, ok1 := snap.Sum(metricProcRSS)
		cpu, ok2 := snap.Sum(metricProcCPU)
		if !ok1 || !ok2 {
			return ProcSample{}, fmt.Errorf("%s does not export %s and %s", target, metricProcRSS, metricProcCPU)
		}
		return ProcSample{RSSBytes: uint64(rss), CPUSeconds: cpu}, nil
	}
}

// ProcessStats summarises a process over a run. CPU is in cores (1.0 = one
// core busy); the mean is total CPU over wall time, the max is the busiest
// sampling interval of at least half the configured interval.
type ProcessStats struct {
	Source       string  `json:"source"`
	Samples      int     `json:"samples"`
	Errors       int     `json:"errors"`
	LastError    string  `json:"last_error,omitempty"`
	WallSeconds  float64 `json:"wall_seconds"`
	CPUSeconds   float64 `json:"cpu_seconds"`
	CPUCoresMean float64 `json:"cpu_cores_mean"`
	CPUCoresMax  float64 `json:"cpu_cores_max"`
	RSSStartMB   float64 `json:"rss_start_mb"`
	RSSEndMB     float64 `json:"rss_end_mb"`
	RSSMaxMB     float64 `json:"rss_max_mb"`
	RSSMeanMB    float64 `json:"rss_mean_mb"`
}

// Sampler reads a process at a fixed interval and keeps running aggregates
// only, so its memory does not grow with the run length.
type Sampler struct {
	source   string
	probe    Probe
	interval time.Duration

	mu        sync.Mutex
	first     ProcSample
	firstAt   time.Time
	last      ProcSample
	lastAt    time.Time
	rssSum    float64
	stats     ProcessStats
	haveFirst bool
}

// NewSampler returns a sampler for probe; source names it in the output.
func NewSampler(source string, probe Probe, interval time.Duration) (*Sampler, error) {
	if probe == nil || interval <= 0 {
		return nil, errors.New("sampler needs a probe and a positive interval")
	}
	return &Sampler{source: source, probe: probe, interval: interval, stats: ProcessStats{Source: source}}, nil
}

// finalSampleTimeout is the least time the sample taken after Run's context
// ends may take. The sampling interval alone is no bound for it: a short one
// (milliseconds) would lose the final sample on any busy host.
const finalSampleTimeout = time.Second

// Run samples immediately, then every interval until ctx ends, then once more
// (that last sample may take up to max(interval, finalSampleTimeout)).
func (s *Sampler) Run(ctx context.Context) {
	t := time.NewTicker(s.interval)
	defer t.Stop()
	s.sampleOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			final, cancel := context.WithTimeout(context.WithoutCancel(ctx), max(s.interval, finalSampleTimeout))
			s.sampleOnce(final)
			cancel()
			return
		case <-t.C:
			s.sampleOnce(ctx)
		}
	}
}

func (s *Sampler) sampleOnce(ctx context.Context) {
	smp, err := s.probe(ctx)
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.stats.Errors++
		s.stats.LastError = err.Error()
		return
	}
	s.add(smp, now)
}

func (s *Sampler) add(smp ProcSample, now time.Time) {
	st := &s.stats
	mb := float64(smp.RSSBytes) / (1 << 20)
	if !s.haveFirst {
		s.first, s.firstAt, s.haveFirst = smp, now, true
		st.RSSStartMB = mb
	} else if dt := now.Sub(s.lastAt); dt >= s.interval/2 {
		// Rates over shorter gaps (the final sample right after a tick) are
		// dominated by CPU-time granularity (10 ms on Linux), so skip them.
		st.CPUCoresMax = max(st.CPUCoresMax, (smp.CPUSeconds-s.last.CPUSeconds)/dt.Seconds())
	}
	s.last, s.lastAt = smp, now
	st.Samples++
	s.rssSum += mb
	st.RSSMaxMB = max(st.RSSMaxMB, mb)
	st.RSSEndMB = mb
}

// Stats returns the aggregates so far.
func (s *Sampler) Stats() ProcessStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.stats
	if st.Samples > 0 {
		st.RSSMeanMB = s.rssSum / float64(st.Samples)
		st.WallSeconds = s.lastAt.Sub(s.firstAt).Seconds()
		st.CPUSeconds = s.last.CPUSeconds - s.first.CPUSeconds
		if st.WallSeconds > 0 {
			st.CPUCoresMean = st.CPUSeconds / st.WallSeconds
		}
	}
	return st
}
