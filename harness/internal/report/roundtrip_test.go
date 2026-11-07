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
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/harness/internal/outcome"
	"github.com/praveenkanna/llm-synapse-gw/harness/internal/runner"
)

// TestRunnerWritesWhatReportReads: a run the load generator wrote is read
// back by report with the same outcomes, so the record has one schema. Odd
// requests stream a whole generation; even ones stream without a
// finish_reason, which ADR 0021 counts as truncated.
func TestRunnerWritesWhatReportReads(t *testing.T) {
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		finish := `"stop"`
		if n.Add(1)%2 == 0 {
			finish = "null"
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"},\"finish_reason\":null}]}\n\n")
		w.(http.Flusher).Flush()
		time.Sleep(20 * time.Millisecond) // the first token comes after the first byte
		fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":\"a\"},\"finish_reason\":null}]}\n\n")
		fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":\"b\"},\"finish_reason\":%s}]}\n\ndata: [DONE]\n\n", finish)
	}))
	t.Cleanup(srv.Close)

	const requests = 6
	cfg := runner.Config{Concurrency: 1, MaxRequests: requests, Seed: 1}
	src, err := runner.NewSynthetic(runner.SyntheticConfig{Model: "m", Classes: []runner.ClassSpec{{Name: "chat", Weight: 1, Stream: true, MaxTokens: 4, PromptWords: 4}}}, 1)
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

	dir := t.TempDir()
	info, _ := json.Marshal(RunInfo{URL: srv.URL, Label: "roundtrip", Run: cfg})
	if err := os.WriteFile(filepath.Join(dir, FileRun), info, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, FileResults), raw.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := Load(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	all := got.Window.All()
	if all.Requests != requests || all.OK != requests/2 || all.Errors != requests/2 {
		t.Fatalf("report read %d requests, %d ok, %d errors; want %d, %d, %d", all.Requests, all.OK, all.Errors, requests, requests/2, requests/2)
	}
	if all.Statuses["error:"+outcome.ErrTruncated] != requests/2 || all.TTFT.Count != requests/2 {
		t.Fatalf("statuses %v, TTFT samples %d; want %d truncated and TTFT over the %d successes", all.Statuses, all.TTFT.Count, requests/2, requests/2)
	}

	// Every line carries the derived facts readers use (ADR 0021).
	var ok, complete int
	err = outcome.ReadRecords(&raw, 0, func(r outcome.Record) error {
		if r.OK != r.Succeeded() || r.Complete != (r.Error == "") {
			t.Errorf("record %d: ok %v complete %v disagree with status %d error %q", r.Seq, r.OK, r.Complete, r.Status, r.Error)
		}
		if r.OK {
			ok++
			if r.FirstByteMS <= 0 || r.TTFTMS-r.FirstByteMS < 10 {
				t.Errorf("record %d: first byte %v ms, TTFT %v ms; want the first token about 20 ms after the first byte", r.Seq, r.FirstByteMS, r.TTFTMS)
			}
		}
		if r.Complete {
			complete++
		}
		return nil
	})
	if err != nil || ok != requests/2 || complete != requests/2 {
		t.Fatalf("read back %d ok, %d complete (err %v); want %d each", ok, complete, err, requests/2)
	}
}
