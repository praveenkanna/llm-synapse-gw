// Command loadgen drives an OpenAI-compatible endpoint (the gateway, vLLM or
// mockllm) with synthetic load and records per-request TTFT, inter-token
// latency, duration and status.
//
// Open loop at a target rate, with periodic bursts:
//
//	loadgen -url http://localhost:8080 -rate 50 -duration 60s \
//	  -burst-every 20s -burst-length 5s -burst-factor 4 \
//	  -class interactive:3:stream:128:64 -class background:1:nostream:512:256
//
// A workload profile (harness/profiles) sets the class mix, lengths, shared
// prefixes, multi-turn history and default arrival rate and bursts; -rate,
// -concurrency, -burst-* and -model override it:
//
//	loadgen -url http://localhost:8080 -profile harness/profiles/mixed.yaml -duration 5m
//
// Closed loop at a target concurrency:
//
//	loadgen -url http://localhost:8080 -concurrency 64 -requests 10000
//
// Same-session baseline: run once against the upstream directly and once
// through the gateway, label each run, and scrape the upstream /metrics and
// sample the gateway process around the gateway run:
//
//	loadgen -label direct -url http://127.0.0.1:8001 -scrape http://127.0.0.1:8001 ...
//	loadgen -label gateway -url http://127.0.0.1:8080 -scrape http://127.0.0.1:8001 \
//	  -watch-pid $(pgrep -x gateway) ...
//
// then compare the two run directories with cmd/report.
//
// Each run writes results.jsonl (one line per request), summary.json
// (p50/p95/p99 per class), run.json (the run's settings) and, when anything
// is captured, metrics.json (upstream counter deltas, process CPU/RSS) to
// <out>/<UTC time>-seed<seed>/, and prints the summary table. The same seed
// yields the same arrival schedule and request sequence. No prompt or
// response text is ever written: prompts are synthetic, and results hold
// measurements only.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/harness/internal/metrics"
	"github.com/praveenkanna/llm-synapse-gw/harness/internal/runner"
	"github.com/praveenkanna/llm-synapse-gw/harness/internal/workload"
)

// envAPIKey names the variable holding the bearer token. There is no flag,
// so the key never appears in process listings or shell history.
const envAPIKey = "LOADGEN_API_KEY"

// defaultModel is sent when neither -model nor the profile names a model.
const defaultModel = "mock-model"

type classFlags []runner.ClassSpec

func (c *classFlags) String() string {
	names := make([]string, len(*c))
	for i, s := range *c {
		names[i] = s.Name
	}
	return strings.Join(names, ",")
}

func (c *classFlags) Set(v string) error {
	spec, err := runner.ParseClassSpec(v)
	if err != nil {
		return err
	}
	*c = append(*c, spec)
	return nil
}

type stringList []string

func (l *stringList) String() string     { return strings.Join(*l, ",") }
func (l *stringList) Set(v string) error { *l = append(*l, v); return nil }

type options struct {
	url     string
	label   string
	profile string          // workload profile file; replaces -class
	set     map[string]bool // flags given on the command line
	timeout time.Duration
	out     string
	run     runner.Config
	source  runner.SyntheticConfig
	capture metrics.CaptureConfig
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(os.Stderr, "loadgen:", err)
		}
		os.Exit(2)
	}
}

func parseFlags(args []string, stderr io.Writer) (options, error) {
	var o options
	var classes classFlags
	var scrape stringList
	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&o.url, "url", "http://localhost:8080", "base URL of the OpenAI-compatible endpoint")
	fs.DurationVar(&o.timeout, "timeout", 5*time.Minute, "per-request timeout")
	fs.StringVar(&o.out, "out", defaultOutDir(), "directory for run output (a per-run subdirectory is created)")
	fs.Float64Var(&o.run.Rate, "rate", 0, "open loop: mean arrivals per second (Poisson)")
	fs.IntVar(&o.run.Concurrency, "concurrency", 0, "closed loop: number of concurrent workers")
	fs.DurationVar(&o.run.Duration, "duration", 30*time.Second, "stop issuing requests after this long (0 = no limit)")
	fs.Int64Var(&o.run.MaxRequests, "requests", 0, "stop after this many requests (0 = no limit)")
	fs.IntVar(&o.run.MaxInFlight, "max-inflight", 10000, "open loop: arrivals beyond this many in flight are dropped and recorded")
	fs.DurationVar(&o.run.Burst.Every, "burst-every", 0, "open loop: burst period (0 = no bursts)")
	fs.DurationVar(&o.run.Burst.Length, "burst-length", 0, "open loop: length of each burst")
	fs.Float64Var(&o.run.Burst.Factor, "burst-factor", 1, "open loop: rate multiplier during a burst")
	fs.Uint64Var(&o.run.Seed, "seed", 1, "seed for arrivals and request content")
	fs.StringVar(&o.source.Model, "model", defaultModel, "model name sent in every request")
	fs.StringVar(&o.source.API, "api", runner.APIChat, "API to drive: chat or completions")
	fs.IntVar(&o.source.SystemWords, "system-words", 256, "words in each class's shared synthetic system prompt (0 = none)")
	fs.StringVar(&o.label, "label", "", "name of the target in reports, e.g. direct or gateway")
	fs.Var(&scrape, "scrape", "upstream /metrics URL (or base URL) to scrape before and after the run (repeatable)")
	fs.IntVar(&o.capture.PID, "watch-pid", 0, "local PID (the gateway) whose CPU and RSS are sampled during the run")
	fs.StringVar(&o.capture.ProcessMetrics, "watch-metrics", "", "/metrics URL exporting process_* metrics, sampled instead of -watch-pid")
	fs.DurationVar(&o.capture.Interval, "sample-every", time.Second, "process sampling interval")
	fs.StringVar(&o.profile, "profile", "", "workload profile YAML (harness/profiles); replaces -class and -system-words")
	fs.Var(&classes, "class", "traffic class name:weight:stream|nostream:max_tokens:prompt_words (repeatable; default interactive:1:stream:128:64)")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if fs.NArg() > 0 {
		return o, fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	o.set = map[string]bool{}
	fs.Visit(func(f *flag.Flag) { o.set[f.Name] = true })
	if o.profile != "" {
		for _, name := range []string{"class", "system-words", "api"} {
			if o.set[name] {
				return o, fmt.Errorf("-%s cannot be combined with -profile; the profile sets it", name)
			}
		}
	}
	if len(classes) == 0 {
		def, _ := runner.ParseClassSpec("interactive")
		classes = classFlags{def}
	}
	o.source.Classes = classes
	o.capture.Upstreams = scrape
	if o.capture.Interval <= 0 {
		return o, errors.New("sample-every must be positive")
	}
	return o, nil
}

// defaultOutDir is harness/out whether loadgen runs from the repository root
// or from harness/ (both are gitignored).
func defaultOutDir() string {
	if _, err := os.Stat(filepath.Join("harness", "go.mod")); err == nil {
		return filepath.Join("harness", "out")
	}
	return "out"
}

func run(args []string, stdout, stderr io.Writer) error {
	o, err := parseFlags(args, stderr)
	if err != nil {
		return err
	}
	var profile *workload.Profile
	if o.profile != "" {
		if profile, err = workload.Load(o.profile); err != nil {
			return err
		}
		o.applyProfile(profile)
	}
	if err := o.run.Validate(); err != nil {
		return err
	}
	src, err := o.newSource(profile)
	if err != nil {
		return err
	}
	conns := o.run.Concurrency + o.run.MaxInFlight
	client := &runner.Client{
		HTTP:    &http.Client{Transport: runner.NewTransport(conns)},
		BaseURL: strings.TrimRight(o.url, "/"),
		APIKey:  os.Getenv(envAPIKey),
		Timeout: o.timeout,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var capture *metrics.Capture
	if o.capture.Enabled() {
		if capture, err = metrics.Start(ctx, o.capture); err != nil {
			return fmt.Errorf("capture: %w", err)
		}
		defer capture.Stop(context.Background()) // ends sampling on early return
	}

	dir, err := makeRunDir(o.out, o.run.Seed)
	if err != nil {
		return err
	}
	info := runInfo{URL: o.url, Label: o.label, Args: args, Timeout: o.timeout.String(), Run: o.run, Source: o.source, Profile: profile}
	if err := writeJSON(filepath.Join(dir, "run.json"), info); err != nil {
		return err
	}
	raw, err := os.Create(filepath.Join(dir, "results.jsonl"))
	if err != nil {
		return fmt.Errorf("create results: %w", err)
	}
	defer raw.Close()

	r, err := runner.New(o.run, src, client, raw)
	if err != nil {
		return err
	}
	if profile != nil {
		fmt.Fprintf(stderr, "loadgen: profile %s: %s\n", profile.Name, formatShares(profile.ExpectedShares()))
	}
	fmt.Fprintf(stderr, "loadgen: running against %s, writing %s\n", o.url, dir)
	sum, runErr := r.Run(ctx)
	errs := []error{runErr, raw.Close(), writeJSON(filepath.Join(dir, "summary.json"), sum)}
	if capture != nil {
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		errs = append(errs, writeJSON(filepath.Join(dir, "metrics.json"), capture.Stop(stopCtx)))
		cancel()
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("write results: %w", err)
	}
	return sum.WriteTable(stdout)
}

// runInfo records the run's settings next to its results. It never holds
// the API key.
type runInfo struct {
	URL     string                 `json:"url"`
	Label   string                 `json:"label,omitempty"`
	Args    []string               `json:"args"` // loadgen's arguments, to reproduce the run
	Timeout string                 `json:"timeout"`
	Run     runner.Config          `json:"run"`
	Source  runner.SyntheticConfig `json:"source"`
	// Profile is the workload profile as loaded, when -profile is used;
	// Source then holds only the model and API.
	Profile *workload.Profile `json:"profile,omitempty"`
}

// applyProfile takes the profile's defaults for everything not set on the
// command line: the API, and in rate mode the rate and burst shape.
func (o *options) applyProfile(p *workload.Profile) {
	o.source.API = p.API
	o.source.Classes = nil
	o.source.SystemWords = 0
	if !o.set["model"] {
		o.source.Model = p.Model
	}
	if !o.set["rate"] && !o.set["concurrency"] {
		o.run.Rate = p.Arrival.Rate
	}
	if o.run.Rate > 0 && !o.set["burst-every"] && !o.set["burst-length"] && !o.set["burst-factor"] {
		b := p.Arrival.Burst
		o.run.Burst = runner.Burst{Every: b.Every, Length: b.Length, Factor: b.Factor}
	}
}

// newSource returns the profile's generator, or the built-in synthetic
// source built from -class.
func (o *options) newSource(p *workload.Profile) (runner.Source, error) {
	if p == nil {
		return runner.NewSynthetic(o.source, o.run.Seed)
	}
	opts := workload.Options{DefaultModel: defaultModel, Burst: o.run.Burst}
	if o.set["model"] {
		opts.Model = o.source.Model
	}
	return workload.NewGenerator(p, opts, o.run.Seed)
}

func formatShares(shares map[string]float64) string {
	names := slices.Sorted(maps.Keys(shares))
	parts := make([]string, len(names))
	for i, n := range names {
		parts[i] = fmt.Sprintf("%s %.0f%%", n, 100*shares[n])
	}
	return "expected shares outside bursts: " + strings.Join(parts, ", ")
}

func makeRunDir(base string, seed uint64) (string, error) {
	if err := os.MkdirAll(base, 0o755); err != nil {
		return "", fmt.Errorf("create output directory: %w", err)
	}
	name := fmt.Sprintf("%s-seed%d", time.Now().UTC().Format("20060102T150405Z"), seed)
	for i := 1; ; i++ {
		dir := filepath.Join(base, name)
		err := os.Mkdir(dir, 0o755)
		if err == nil {
			return dir, nil
		}
		if !errors.Is(err, os.ErrExist) || i >= 100 {
			return "", fmt.Errorf("create run directory: %w", err)
		}
		name = fmt.Sprintf("%s-seed%d-%d", time.Now().UTC().Format("20060102T150405Z"), seed, i)
	}
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("encode %s: %w", filepath.Base(path), err)
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}
	return nil
}
