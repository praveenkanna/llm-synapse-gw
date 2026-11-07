package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeRunDir(t *testing.T, root, label string, ttftMS float64) string {
	t.Helper()
	dir := filepath.Join(root, label)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	runJSON := fmt.Sprintf(`{"url":"http://%s","label":%q,"args":["-label",%q,"-url","http://x y"],
		"run":{"concurrency":4,"duration_ns":2000000000,"seed":9},
		"source":{"classes":[{"name":"interactive","weight":1,"stream":true,"max_tokens":16,"prompt_words":8}]}}`, label, label, label)
	var res strings.Builder
	for i := range 40 {
		fmt.Fprintf(&res, `{"class":"interactive","stream":true,"start_ms":%d,"lag_ms":0.1,"status":200,"ttft_ms":%g,"duration_ms":100,"output_tokens":16}`+"\n", (i/4)*100, ttftMS)
	}
	for name, body := range map[string]string{"run.json": runJSON, "results.jsonl": res.String()} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestReportWritesResultsFile(t *testing.T) {
	root := t.TempDir()
	base, target := writeRunDir(t, root, "direct", 10), writeRunDir(t, root, "gateway", 11)
	tmpl := filepath.Join("..", "..", "..", "docs", "results", "TEMPLATE.md")
	outDir := t.TempDir()
	cmds := filepath.Join(outDir, "cmds.txt")
	if err := os.WriteFile(cmds, []byte("docker compose up\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(outDir, "result.md")
	args := []string{
		"-template", tmpl, "-out", out, "-profile", "chat", "-load", "100%", "-upstream", "mockllm", "-git-sha", "deadbee",
		"-pair", "chat=" + base + "," + target, "-warmup", "100ms", "-commands-file", cmds, "-date", "2026-09-27",
	}
	var stdout bytes.Buffer
	if err := run(args, &stdout, io.Discard); err != nil {
		t.Fatal(err)
	}
	md, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"# 2026-09-27 · M0 · chat · 100%",
		"- Git SHA: deadbee",
		"- Profile file and seed: chat/direct: classes interactive:1:stream:16:8, seed 9",
		"concurrency 4 for 2s",
		"docker compose up\nloadgen -label direct -url 'http://x y'\n",
		"| TTFT p50 interactive (ms) | 10.0 | 11.0 | +1.0 |",
		"| AC-M0-3 | chat | TTFT overhead, class all | **PASS** |",
		mockllmNote,
	} {
		if !bytes.Contains(md, []byte(want)) {
			t.Errorf("results file lacks %q", want)
		}
	}
	if !strings.Contains(stdout.String(), "wrote "+out) {
		t.Errorf("stdout = %q", stdout.String())
	}

	// Existing file: refused without -force, replaced with it.
	if err := run(args, io.Discard, io.Discard); err == nil {
		t.Fatal("overwrite without -force: want error")
	}
	if err := run(append(args, "-force"), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	// Stdout output.
	stdout.Reset()
	if err := run(append(args[2:], "-template", tmpl, "-out", "-"), &stdout, io.Discard); err != nil || !strings.HasPrefix(stdout.String(), "# 2026-09-27") {
		t.Fatalf("stdout output: %v\n%s", err, stdout.String())
	}
}

func TestReportRejectsBadInput(t *testing.T) {
	root := t.TempDir()
	good := writeRunDir(t, root, "a", 1)
	tests := [][]string{
		{"-profile", "p", "-upstream", "u"},                                           // no pair
		{"-pair", "x=" + good + "," + good},                                           // no profile/upstream
		{"-profile", "p", "-upstream", "u", "-pair", "nodirs"},                        // bad pair
		{"-profile", "p", "-upstream", "u", "-pair", "x=" + good + "," + root + "/n"}, // missing target
		{"-profile", "p", "-upstream", "u", "-pair", "x=" + good + "," + good, "-template", root + "/none.md"},
		{"-profile", "p", "-upstream", "u", "-pair", "x=" + good + "," + good, "-warmup", "-1s"},
		{"-profile", "p", "-upstream", "u", "-pair", "x=" + good + "," + good, "stray"},
	}
	for _, args := range tests {
		if err := run(append(args, "-out", "-"), io.Discard, io.Discard); err == nil {
			t.Errorf("run(%v): want error", args)
		}
	}
}

func TestShellJoin(t *testing.T) {
	got := shellJoin([]string{"-url", "http://a", "-class", "a:1", "it's", "", "$x"})
	want := `-url http://a -class a:1 'it'\''s' '' '$x'`
	if got != want {
		t.Fatalf("shellJoin = %s, want %s", got, want)
	}
}
