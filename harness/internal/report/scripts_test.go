package report

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/harness/internal/outcome"
	"github.com/praveenkanna/llm-synapse-gw/harness/internal/runner"
)

// scriptsDir is harness/scripts, from this package's directory.
const scriptsDir = "../../scripts"

// python returns python3, or skips the test when there is none that runs
// (macOS without the command-line tools has a /usr/bin/python3 stub).
func python(t *testing.T) string {
	t.Helper()
	p, err := exec.LookPath("python3")
	if err != nil || exec.Command(p, "-c", "").Run() != nil {
		t.Skip("python3 not installed")
	}
	return p
}

// pyEnv keeps the scripts from writing bytecode into the source tree.
var pyEnv = append(os.Environ(), "PYTHONDONTWRITEBYTECODE=1")

// runScript runs a proof script with args and decodes its JSON output into v.
func runScript(t *testing.T, script string, v any, args ...string) {
	t.Helper()
	cmd := exec.Command(python(t), append([]string{filepath.Join(scriptsDir, script)}, args...)...)
	cmd.Env = pyEnv
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("%s: %v\n%s", script, err, stderr.String())
	}
	if err := json.Unmarshal(out, v); err != nil {
		t.Fatalf("%s output: %v\n%s", script, err, out)
	}
}

// TestScriptsReadTheRecordedOutcomes: the proof scripts count successes and
// windows from the fields runner records (ADR 0021), so they agree with
// report on the same run, and their percentiles per burst window agree with
// Go's nearest rank. Every third request streams without a finish_reason (a
// 200 that is not a success); the run has burst windows.
func TestScriptsReadTheRecordedOutcomes(t *testing.T) {
	python(t)
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		finish := `"stop"`
		if n.Add(1)%3 == 0 {
			finish = "null"
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":\"a\"},\"finish_reason\":%s}]}\n\ndata: [DONE]\n\n", finish)
	}))
	t.Cleanup(srv.Close)
	cfg := runner.Config{Rate: 200, Burst: runner.Burst{Every: 200 * time.Millisecond, Length: 100 * time.Millisecond, Factor: 2},
		MaxRequests: 60, MaxInFlight: 100, Seed: 3}
	src, err := runner.NewSynthetic(runner.SyntheticConfig{Model: "m", Classes: []runner.ClassSpec{{Name: "chat", Weight: 1, Stream: true, MaxTokens: 4, PromptWords: 4}}}, 3)
	if err != nil {
		t.Fatal(err)
	}
	var raw bytes.Buffer
	run, err := runner.New(cfg, src, &runner.Client{HTTP: srv.Client(), BaseURL: srv.URL, Timeout: 5 * time.Second}, &raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	results := filepath.Join(t.TempDir(), FileResults)
	if err := os.WriteFile(results, raw.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	w, err := readWindow(results, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	chat, _ := w.Class("chat")
	ok, total := int(chat.OK), int(chat.Requests)
	// Per burst window, from the records: report does not split by window.
	windowTTFT := map[string][]float64{}
	var inBurst int
	if err := outcome.ReadRecords(bytes.NewReader(raw.Bytes()), 0, func(r outcome.Record) error {
		win := "steady"
		if r.InBurst {
			win = "burst"
			inBurst++
		}
		if r.OK {
			windowTTFT[win] = append(windowTTFT[win], r.TTFTMS)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if ok == total || inBurst == 0 || inBurst == total {
		t.Fatalf("run gave %d of %d ok, %d in bursts; the test needs some of each", ok, total, inBurst)
	}

	type pcts struct{ P50, P95, P99 float64 }
	var cache struct {
		Groups []struct {
			N          int  `json:"n"`
			TTFTMS     pcts `json:"ttft_ms"`
			DurationMS pcts `json:"duration_ms"`
		} `json:"groups"`
		Requests map[string]int `json:"requests"`
	}
	runScript(t, "cache-latency-stats.py", &cache, results, "--json")
	if len(cache.Groups) != 1 || cache.Groups[0].N != ok || cache.Requests["chat"] != total {
		t.Fatalf("cache-latency-stats: groups %+v requests %v; want report's %d ok of %d", cache.Groups, cache.Requests, ok, total)
	}
	same := func(name string, g pcts, want outcome.Stats) {
		t.Helper()
		if g.P50 != want.P50MS || g.P95 != want.P95MS || g.P99 != want.P99MS {
			t.Errorf("%s p50/p95/p99 = %v/%v/%v, want %v/%v/%v", name, g.P50, g.P95, g.P99, want.P50MS, want.P95MS, want.P99MS)
		}
	}
	same("cache-latency-stats TTFT vs report", cache.Groups[0].TTFTMS, chat.TTFT)
	same("cache-latency-stats duration vs report", cache.Groups[0].DurationMS, chat.Duration)

	var windows []struct {
		Window   string  `json:"window"`
		Requests int     `json:"requests"`
		OK       int     `json:"ok"`
		Failed   int     `json:"failed_after_2xx"`
		P50      float64 `json:"ttft_p50_ms"`
		P95      float64 `json:"ttft_p95_ms"`
		P99      float64 `json:"ttft_p99_ms"`
	}
	runScript(t, "burst-window-stats.py", &windows, results, "--warmup", "0", "--json")
	got := map[string]int{}
	var gotOK, gotFailed int
	for _, w := range windows {
		got[w.Window] += w.Requests
		gotOK += w.OK
		gotFailed += w.Failed
		same("burst-window-stats TTFT, "+w.Window, pcts{w.P50, w.P95, w.P99}, outcome.Percentiles(windowTTFT[w.Window]))
	}
	if got["burst"] != inBurst || got["steady"] != total-inBurst || gotOK != ok || gotFailed != total-ok {
		t.Fatalf("burst-window-stats windows %v, ok %d, failed after 2xx %d; want burst %d, steady %d, ok %d, failed %d",
			got, gotOK, gotFailed, inBurst, total-inBurst, ok, total-ok)
	}
}

// TestScriptsRejectOldRecords: a record written before ADR 0021 (no ok
// field) is refused with a one-line message, not silently judged by another
// rule, by every script that reads records.
func TestScriptsRejectOldRecords(t *testing.T) {
	py := python(t)
	dir := t.TempDir()
	old := filepath.Join(dir, FileResults)
	line := []byte(`{"class":"a","status":200,"start_ms":1,"ttft_ms":2,"duration_ms":3}` + "\n")
	if err := os.WriteFile(old, line, 0o644); err != nil {
		t.Fatal(err)
	}
	soak := filepath.Join(dir, "soak")
	if err := os.MkdirAll(filepath.Join(soak, "run", "r1"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(soak, "run", "r1", FileResults), line, 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"cache-latency-stats.py", old},
		{"burst-window-stats.py", old},
		{"m4-soak-report.py", soak, "--soak-seconds", "1", "--replay-bound-exit", "0"},
	} {
		cmd := exec.Command(py, append([]string{filepath.Join(scriptsDir, args[0])}, args[1:]...)...)
		cmd.Env = pyEnv
		out, err := cmd.CombinedOutput()
		if err == nil || !strings.Contains(string(out), "written before ADR 0021") || strings.Contains(string(out), "Traceback") {
			t.Errorf("%s on an old record: err %v, output %s; want a one-line refusal naming ADR 0021", args[0], err, out)
		}
	}
}

// TestBurstWindowsFromTheRun: a burst run's records outside every window
// are "steady", not "all", even when no record after warm-up was in one: the
// run's settings (run.json) say it had bursts.
func TestBurstWindowsFromTheRun(t *testing.T) {
	python(t)
	dir := t.TempDir()
	results := filepath.Join(dir, FileResults)
	rec := `{"class":"chat","start_ms":%d,"status":200,"ok":true,"complete":true,"ttft_ms":1,"first_byte_ms":1,"in_burst":%v,"duration_ms":2}`
	lines := fmt.Sprintf(rec, 1_000, true) + "\n" + fmt.Sprintf(rec, 20_000, false) + "\n"
	if err := os.WriteFile(results, []byte(lines), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ runJSON, want string }{
		{`{"run":{"burst":{"every_ns":60000000000,"length_ns":1000000000,"factor":2}}}`, "steady"},
		{`{"run":{"burst":{"every_ns":0}}}`, "all"},
	} {
		if err := os.WriteFile(filepath.Join(dir, "run.json"), []byte(tc.runJSON), 0o644); err != nil {
			t.Fatal(err)
		}
		var windows []struct {
			Window string `json:"window"`
		}
		runScript(t, "burst-window-stats.py", &windows, results, "--warmup", "10", "--json")
		if len(windows) != 1 || windows[0].Window != tc.want {
			t.Errorf("run.json %s: windows %+v, want one %q", tc.runJSON, windows, tc.want)
		}
	}
}

// TestSoakReportSplitsLateFailures: m4-soak-report counts a request the
// load generator cancelled at the end as cut short whether or not its
// headers had come, and shows a 2xx that failed after its headers in its own
// column (it still fails the verdict).
func TestSoakReportSplitsLateFailures(t *testing.T) {
	py := python(t)
	rec := func(start, status int, err string, ok bool) string {
		return fmt.Sprintf(`{"class":"chat","start_ms":%d,"status":%d,"error":%q,"ok":%v,"complete":%v,"ttft_ms":1,"first_byte_ms":1,"in_burst":false,"duration_ms":2}`,
			start, status, err, ok, ok)
	}
	base := []string{
		rec(1_000, 200, "", true),
		rec(2_000, 503, "http_status", false),
		rec(3_599_000, 200, "canceled", false), // streaming when the load stopped
		rec(3_599_500, 0, "canceled", false),   // not yet answered then
		rec(3_600_000, 200, "", true),
	}
	tests := []struct {
		name, row, verdict string
		extra              []string
	}{
		{name: "cut short at the end", row: "| chat | 3 | 2 | 1 | 0 | 0 |", verdict: "PASS"},
		{name: "failed after its headers", extra: []string{rec(4_000, 200, "stream_error", false)},
			row: "| chat | 4 | 2 | 1 | 1 | 0 |", verdict: "FAIL: chat: 1 responses neither successful"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			write := func(name string, lines ...string) {
				t.Helper()
				p := filepath.Join(dir, name)
				if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(p, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			write(filepath.Join("run", "r1", FileResults), append(base, tc.extra...)...)
			write("samples.jsonl",
				`{"redis":{"used_memory":10,"maxmemory":100,"evicted_keys":0,"keys":1,"expires":1}}`,
				`{"redis":{"used_memory":90,"maxmemory":100,"evicted_keys":5,"keys":1,"expires":1}}`)
			write(filepath.Join("slow", "round-1.json"),
				`{"responses":[{"complete":true,"sha256":"a"},{"complete":true,"sha256":"a"},{"slow":true,"error_event":true}]}`)
			cmd := exec.Command(py, filepath.Join(scriptsDir, "m4-soak-report.py"), dir, "--soak-seconds", "3600", "--replay-bound-exit", "0")
			cmd.Env = pyEnv
			out, _ := cmd.CombinedOutput()
			if !strings.Contains(string(out), tc.row) || !strings.Contains(string(out), "2 requests still running at the end") ||
				!strings.Contains(string(out), "\n"+tc.verdict) {
				t.Fatalf("report:\n%s\nwant row %q, 2 cut short, verdict %q", out, tc.row, tc.verdict)
			}
		})
	}
}

// TestPythonRankMatchesGo: the scripts' one rule of their own, the
// percentile, is the same nearest rank as outcome.Rank.
func TestPythonRankMatchesGo(t *testing.T) {
	py := python(t)
	values := []float64{9, 1, 5, 3, 7, 2, 8, 4, 6, 10, 15, 11}
	qs := []float64{0.01, 0.10, 0.50, 0.90, 0.95, 0.99, 1}
	prog := fmt.Sprintf("import json,sys; sys.path.insert(0, %q); import outcomes; v=%s; print(json.dumps([outcomes.pct(v, q) for q in %s]))",
		scriptsDir, mustJSON(values), mustJSON(qs))
	cmd := exec.Command(py, "-c", prog)
	cmd.Env = pyEnv
	out, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	var got []float64
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatal(err)
	}
	sorted := append([]float64(nil), values...)
	outcome.Percentiles(sorted) // sorts
	for i, q := range qs {
		if want := outcome.Rank(sorted, q); got[i] != want {
			t.Errorf("q=%v: python %v, Go %v", q, got[i], want)
		}
	}
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
