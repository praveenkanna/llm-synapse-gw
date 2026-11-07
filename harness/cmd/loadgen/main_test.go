package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/harness/internal/workload"
)

func fakeServer(t *testing.T) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var auth []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		mu.Lock()
		auth = append(auth, r.Header.Get("Authorization"))
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		for range 3 {
			_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"t\"}}]}\n\n")
			w.(http.Flusher).Flush()
		}
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(srv.Close)
	return srv, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), auth...)
	}
}

func TestRunWritesResults(t *testing.T) {
	srv, auth := fakeServer(t)
	out := t.TempDir()
	t.Setenv(envAPIKey, "k-123")
	var stdout, stderr bytes.Buffer
	err := run([]string{
		"-url", srv.URL + "/", "-rate", "300", "-requests", "30", "-seed", "4", "-out", out,
		"-class", "chat:1:stream:3:2", "-system-words", "2",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run: %v\nstderr: %s", err, stderr.String())
	}
	dirs, _ := os.ReadDir(out)
	if len(dirs) != 1 || !strings.HasSuffix(dirs[0].Name(), "-seed4") {
		t.Fatalf("run dirs = %v", dirs)
	}
	dir := filepath.Join(out, dirs[0].Name())

	f, err := os.Open(filepath.Join(dir, "results.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	lines := 0
	for sc := bufio.NewScanner(f); sc.Scan(); lines++ {
		var rec map[string]any
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil || rec["class"] != "chat" || rec["status"] != 200.0 {
			t.Fatalf("bad record %s (%v)", sc.Text(), err)
		}
	}
	if lines != 30 {
		t.Fatalf("results lines = %d, want 30", lines)
	}

	var sum struct {
		Classes []struct {
			Class    string `json:"class"`
			Requests int    `json:"requests"`
			TTFT     struct {
				P99 float64 `json:"p99_ms"`
			} `json:"ttft"`
		} `json:"classes"`
	}
	b, err := os.ReadFile(filepath.Join(dir, "summary.json"))
	if err != nil || json.Unmarshal(b, &sum) != nil || len(sum.Classes) != 2 || sum.Classes[1].Class != "all" || sum.Classes[1].Requests != 30 {
		t.Fatalf("summary.json = %s (%v)", b, err)
	}

	runJSON, err := os.ReadFile(filepath.Join(dir, "run.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"results.jsonl", "summary.json", "run.json"} {
		b, _ := os.ReadFile(filepath.Join(dir, name))
		if bytes.Contains(b, []byte("k-123")) {
			t.Fatalf("%s contains the API key", name)
		}
	}
	if !bytes.Contains(runJSON, []byte(`"seed": 4`)) {
		t.Fatalf("run.json lacks the seed: %s", runJSON)
	}
	if got := auth(); len(got) != 30 || got[0] != "Bearer k-123" {
		t.Fatalf("auth headers = %v", got)
	}
	if !strings.Contains(stdout.String(), "TTFT p50") {
		t.Fatalf("stdout lacks the summary table:\n%s", stdout.String())
	}
}

func TestRunRejectsBadFlags(t *testing.T) {
	tests := [][]string{
		{"-rate", "1", "-concurrency", "1"},
		{"-duration", "0"},
		{"-rate", "1", "-class", "a:0"},
		{"-rate", "1", "-api", "embeddings"},
		{"-rate", "1", "extra"},
		{"-rate", "1", "-sample-every", "0s"},
	}
	for _, args := range tests {
		t.Run(fmt.Sprint(args), func(t *testing.T) {
			out := t.TempDir()
			if err := run(append(args, "-out", out), io.Discard, io.Discard); err == nil {
				t.Fatal("want error")
			}
			if dirs, _ := os.ReadDir(out); len(dirs) != 0 {
				t.Fatalf("invalid flags still created %v", dirs)
			}
		})
	}
}

func TestRunCapturesMetrics(t *testing.T) {
	var scrapes atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			n := scrapes.Add(1)
			fmt.Fprintf(w, "vllm:prefix_cache_queries_total %d\nvllm:prefix_cache_hits_total %d\n", 100*n, 25*n)
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"t\"}}]}\n\ndata: [DONE]\n\n")
	}))
	defer srv.Close()
	out := t.TempDir()
	args := []string{
		"-url", srv.URL, "-label", "direct", "-scrape", srv.URL, "-watch-pid", strconv.Itoa(os.Getpid()),
		"-sample-every", "10ms", "-concurrency", "2", "-requests", "10", "-out", out,
	}
	if err := run(args, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	dirs, _ := os.ReadDir(out)
	dir := filepath.Join(out, dirs[0].Name())
	var m struct {
		Total struct {
			HitRate float64 `json:"prefix_cache_hit_rate"`
		} `json:"total"`
		Process struct {
			Samples int     `json:"samples"`
			RSSMax  float64 `json:"rss_max_mb"`
		} `json:"process"`
	}
	b, err := os.ReadFile(filepath.Join(dir, "metrics.json"))
	if err != nil || json.Unmarshal(b, &m) != nil {
		t.Fatalf("metrics.json = %s (%v)", b, err)
	}
	if m.Total.HitRate != 0.25 || scrapes.Load() != 2 {
		t.Fatalf("hit rate = %v after %d scrapes", m.Total.HitRate, scrapes.Load())
	}
	if m.Process.Samples == 0 || m.Process.RSSMax <= 0 {
		t.Logf("no process samples on this platform: %s", b)
	}
	var info struct {
		Label string   `json:"label"`
		Args  []string `json:"args"`
	}
	b, _ = os.ReadFile(filepath.Join(dir, "run.json"))
	if json.Unmarshal(b, &info) != nil || info.Label != "direct" || len(info.Args) != len(args) {
		t.Fatalf("run.json = %s", b)
	}
}

func TestRunCaptureFailsFast(t *testing.T) {
	out := t.TempDir()
	err := run([]string{"-url", "http://127.0.0.1:1", "-scrape", "http://127.0.0.1:1", "-rate", "1", "-requests", "1", "-out", out}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "capture") {
		t.Fatalf("err = %v, want capture error", err)
	}
	if dirs, _ := os.ReadDir(out); len(dirs) != 0 {
		t.Fatalf("failed capture still created %v", dirs)
	}
}

const testProfile = `
name: tp
arrival:
  rate: 2
  burst: {every: 30s, length: 5s, factor: 2}
classes:
  - name: chat
    weight: 1
    stream: true
    system_words: 3
    shared_prefixes: 1
    shared_prefix_ratio: 1
    turns: 3
    reply_words: 2
    open_conversations: 2
    prompt_words: 2
    max_tokens: 3
  - name: cls
    model: small
    weight: 0
    stream: true
    triggered_by: {class: chat, count: 1}
    prompt_words: 1
    max_tokens: 1
`

func TestRunWithProfile(t *testing.T) {
	var mu sync.Mutex
	var bodies []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b map[string]any
		_ = json.NewDecoder(r.Body).Decode(&b)
		mu.Lock()
		bodies = append(bodies, b)
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"t\"}}]}\n\ndata: [DONE]\n\n")
	}))
	defer srv.Close()
	dir := t.TempDir()
	path := filepath.Join(dir, "tp.yaml")
	if err := os.WriteFile(path, []byte(testProfile), 0o644); err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	var stderr bytes.Buffer
	args := []string{"-url", srv.URL, "-profile", path, "-rate", "400", "-requests", "40", "-out", out, "-model", "big"}
	if err := run(args, io.Discard, &stderr); err != nil {
		t.Fatalf("run: %v\n%s", err, stderr.String())
	}
	if !strings.Contains(stderr.String(), "profile tp: expected shares outside bursts: chat 50%, cls 50%") {
		t.Errorf("stderr lacks the expected shares:\n%s", stderr.String())
	}
	classes := map[string]int{}
	maxMsgs := 0
	for _, b := range bodies {
		msgs := b["messages"].([]any)
		maxMsgs = max(maxMsgs, len(msgs))
		if b["model"] != "big" {
			t.Fatalf("model = %v, want the -model override", b["model"])
		}
		if len(msgs) == 2 || len(msgs) >= 4 {
			classes["chat"]++
		} else {
			classes["cls"]++
		}
	}
	if len(bodies) != 40 || classes["chat"] != 20 || maxMsgs != 6 {
		t.Fatalf("%d requests, classes %v, longest history %d messages; want 40, 20/20, 6", len(bodies), classes, maxMsgs)
	}
	dirs, _ := os.ReadDir(out)
	var info struct {
		Run struct {
			Rate  float64 `json:"rate"`
			Burst struct {
				Factor float64 `json:"factor"`
			} `json:"burst"`
		} `json:"run"`
		Profile struct {
			Name string `json:"name"`
		} `json:"profile"`
	}
	b, _ := os.ReadFile(filepath.Join(out, dirs[0].Name(), "run.json"))
	if json.Unmarshal(b, &info) != nil || info.Profile.Name != "tp" || info.Run.Rate != 400 || info.Run.Burst.Factor != 2 {
		t.Fatalf("run.json = %s; want profile tp, -rate 400 and the profile's burst", b)
	}
}

func TestApplyProfileDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tp.yaml")
	if err := os.WriteFile(path, []byte(testProfile), 0o644); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name      string
		args      []string
		wantRate  float64
		wantBurst float64
	}{
		{"profile rate and burst", nil, 2, 2},
		{"rate override keeps burst", []string{"-rate", "9"}, 9, 2},
		{"burst override", []string{"-burst-every", "0"}, 2, 0},
		{"closed loop drops burst", []string{"-concurrency", "3"}, 0, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o, err := parseFlags(append([]string{"-profile", path}, tc.args...), io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			p, err := workload.Load(path)
			if err != nil {
				t.Fatal(err)
			}
			o.applyProfile(p)
			burst := o.run.Burst.Factor
			if !o.run.Burst.Enabled() {
				burst = 0
			}
			if o.run.Rate != tc.wantRate || burst != tc.wantBurst {
				t.Fatalf("rate %v burst factor %v, want %v and %v", o.run.Rate, burst, tc.wantRate, tc.wantBurst)
			}
			if _, err := o.newSource(p); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRunProfileRejects(t *testing.T) {
	tests := [][]string{
		{"-profile", "no-such-file.yaml"},
		{"-profile", "../../profiles/chat.yaml", "-class", "a"},
		{"-profile", "../../profiles/chat.yaml", "-system-words", "3"},
		{"-profile", "../../profiles/chat.yaml", "-api", "chat"},
	}
	for _, args := range tests {
		t.Run(fmt.Sprint(args), func(t *testing.T) {
			out := t.TempDir()
			if err := run(append(args, "-out", out), io.Discard, io.Discard); err == nil {
				t.Fatal("want error")
			}
			if dirs, _ := os.ReadDir(out); len(dirs) != 0 {
				t.Fatalf("rejected run still created %v", dirs)
			}
		})
	}
}
