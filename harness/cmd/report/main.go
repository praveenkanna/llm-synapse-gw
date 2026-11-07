// Command report compares loadgen runs from one session and writes a results
// file from docs/results/TEMPLATE.md.
//
// Each -pair names a baseline run directory (direct to the upstream) and a
// target run directory (through the gateway). The first pair fills the
// template's results table; every pair gets a details section and the
// acceptance checks named by -checks:
//
//	report -profile scale -load "5000 streams" -warmup 15s \
//	  -pair streams=harness/out/<direct-run>,harness/out/<gateway-run> \
//	  -pair nostream=harness/out/<direct-run>,harness/out/<gateway-run> \
//	  -checks AC-M0-3,AC-M0-5,AC-M0-6 -upstream mockllm
//
// Statistics are recomputed from results.jsonl, excluding requests that
// started during -warmup. The output file is never overwritten without -force.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/harness/internal/report"
)

// mockllmNote is required on every results file measured against mockllm.
const mockllmNote = "Upstream is mockllm: these results prove gateway overhead and correctness only, never prefix-cache or GPU behaviour (mockllm reports 0 prefix-cache hits and has no preemption counter)."

type pairFlags []string

func (p *pairFlags) String() string     { return strings.Join(*p, " ") }
func (p *pairFlags) Set(v string) error { *p = append(*p, v); return nil }

type options struct {
	pairs                           pairFlags
	warmup                          time.Duration
	template, out                   string
	force                           bool
	checks                          string
	doc                             report.Doc
	upstream, hardware, pools, conf string
	gitSHA, commandsFile, notesFile string
	args                            []string
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(os.Stderr, "report:", err)
		}
		os.Exit(2)
	}
}

func parseFlags(args []string, stderr io.Writer) (options, error) {
	o := options{args: args}
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Var(&o.pairs, "pair", "name=baseline_dir,target_dir (repeatable; the first fills the results table)")
	fs.DurationVar(&o.warmup, "warmup", 0, "exclude requests that started before this offset")
	fs.StringVar(&o.template, "template", defaultTemplate(), "results template")
	fs.StringVar(&o.out, "out", "", "output file (default <template dir>/<date>-<milestone>-<profile>.md; - for stdout)")
	fs.BoolVar(&o.force, "force", false, "overwrite an existing output file")
	fs.StringVar(&o.checks, "checks", "AC-M0-3,AC-M0-4,AC-M0-5", "acceptance criteria to evaluate (AC-M0-3,4,5,6), comma-separated; empty for none")
	fs.StringVar(&o.doc.Date, "date", time.Now().UTC().Format(time.DateOnly), "date in the title and file name")
	fs.StringVar(&o.doc.Milestone, "milestone", "M0", "milestone")
	fs.StringVar(&o.doc.Profile, "profile", "", "workload profile name (required)")
	fs.StringVar(&o.doc.Load, "load", "n/a", "offered load, e.g. 100% or \"5000 streams\"")
	fs.StringVar(&o.upstream, "upstream", "", "upstream kind and version, e.g. mockllm or \"vLLM v0.30.0\" (required)")
	fs.StringVar(&o.hardware, "hardware", defaultHardware(), "hardware the runs used")
	fs.StringVar(&o.pools, "pools", "", "pools and replicas")
	fs.StringVar(&o.conf, "config-diff", "none", "gateway config difference from defaults")
	fs.StringVar(&o.gitSHA, "git-sha", "", "git SHA under test (default: git rev-parse --short HEAD)")
	fs.StringVar(&o.commandsFile, "commands-file", "", "file with the commands that set up the runs (prepended to the loadgen commands)")
	fs.StringVar(&o.notesFile, "notes-file", "", "file with notes for the Notes section")
	fs.StringVar(&o.doc.Verdict, "verdict", "", "verdict text added after the computed checks")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	switch {
	case fs.NArg() > 0:
		return o, fmt.Errorf("unexpected arguments: %v", fs.Args())
	case len(o.pairs) == 0:
		return o, errors.New("at least one -pair is required")
	case o.doc.Profile == "" || o.upstream == "":
		return o, errors.New("-profile and -upstream are required")
	case o.warmup < 0:
		return o, errors.New("-warmup must not be negative")
	}
	return o, nil
}

func run(args []string, stdout, stderr io.Writer) error {
	o, err := parseFlags(args, stderr)
	if err != nil {
		return err
	}
	tmpl, err := os.ReadFile(o.template)
	if err != nil {
		return fmt.Errorf("template: %w", err)
	}
	for _, p := range o.pairs {
		c, err := loadPair(p, o.warmup)
		if err != nil {
			return err
		}
		o.doc.Comparisons = append(o.doc.Comparisons, c)
	}
	if err := fillDoc(&o); err != nil {
		return err
	}
	md, err := report.Render(string(tmpl), o.doc)
	if err != nil {
		return err
	}
	if o.out == "-" {
		_, err := io.WriteString(stdout, md)
		return err
	}
	out := o.out
	if out == "" {
		out = filepath.Join(filepath.Dir(o.template), fmt.Sprintf("%s-%s-%s.md", o.doc.Date, strings.ToLower(o.doc.Milestone), o.doc.Profile))
	}
	flags := os.O_WRONLY | os.O_CREATE | os.O_EXCL
	if o.force {
		flags = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	}
	f, err := os.OpenFile(out, flags, 0o644)
	if err != nil {
		return fmt.Errorf("output (use -force to overwrite): %w", err)
	}
	if _, err := io.WriteString(f, md); err != nil {
		f.Close()
		return fmt.Errorf("write %s: %w", out, err)
	}
	if err := f.Close(); err != nil {
		return err
	}
	fmt.Fprintln(stdout, "wrote", out)
	return nil
}

func loadPair(spec string, warmup time.Duration) (report.Comparison, error) {
	name, dirs, ok := strings.Cut(spec, "=")
	base, target, ok2 := strings.Cut(dirs, ",")
	if !ok || !ok2 || name == "" || base == "" || target == "" {
		return report.Comparison{}, fmt.Errorf("-pair %q: want name=baseline_dir,target_dir", spec)
	}
	b, err := report.Load(base, warmup)
	if err != nil {
		return report.Comparison{}, fmt.Errorf("baseline %s: %w", base, err)
	}
	t, err := report.Load(target, warmup)
	if err != nil {
		return report.Comparison{}, fmt.Errorf("target %s: %w", target, err)
	}
	return report.Comparison{Name: name, Baseline: b, Target: t}, nil
}

func fillDoc(o *options) error {
	d := &o.doc
	if o.checks != "" {
		for _, c := range strings.Split(o.checks, ",") {
			d.Checks = append(d.Checks, strings.TrimSpace(c))
		}
	}
	sha := o.gitSHA
	if sha == "" {
		sha = gitSHA()
	}
	var profiles, durations, cmds []string
	for _, c := range d.Comparisons {
		for _, r := range []*report.Run{c.Baseline, c.Target} {
			cfg := r.Info.Run
			var classes []string
			for _, cl := range r.Info.Source.Classes {
				mode := "stream"
				if !cl.Stream {
					mode = "nostream"
				}
				classes = append(classes, fmt.Sprintf("%s:%g:%s:%d:%d", cl.Name, cl.Weight, mode, cl.MaxTokens, cl.PromptWords))
			}
			profiles = append(profiles, fmt.Sprintf("%s/%s: classes %s, seed %d", c.Name, r.Name(), strings.Join(classes, " "), cfg.Seed))
			load := fmt.Sprintf("rate %g/s", cfg.Rate)
			if cfg.Concurrency > 0 {
				load = fmt.Sprintf("concurrency %d", cfg.Concurrency)
			}
			durations = append(durations, fmt.Sprintf("%s/%s: %s for %s", c.Name, r.Name(), load, cfg.Duration))
			cmds = append(cmds, "loadgen "+shellJoin(r.Info.Args))
		}
	}
	d.Setup = map[string]string{
		"Git SHA":                           sha,
		"Upstream":                          o.upstream,
		"Hardware":                          o.hardware,
		"Pools and replicas":                o.pools,
		"Gateway config diff from defaults": o.conf,
		"Profile file and seed":             strings.Join(profiles, "; "),
		"Duration and warm-up":              strings.Join(durations, "; ") + fmt.Sprintf("; first %s of each run excluded", o.warmup),
		"Commands":                          "(below)",
	}
	var pre string
	if o.commandsFile != "" {
		b, err := os.ReadFile(o.commandsFile)
		if err != nil {
			return fmt.Errorf("commands file: %w", err)
		}
		pre = strings.TrimRight(string(b), "\n") + "\n"
	}
	d.Commands = pre + strings.Join(cmds, "\n") + "\nreport " + shellJoin(o.args)
	var notes []string
	if strings.Contains(strings.ToLower(o.upstream), "mockllm") {
		notes = append(notes, mockllmNote)
	}
	if o.notesFile != "" {
		b, err := os.ReadFile(o.notesFile)
		if err != nil {
			return fmt.Errorf("notes file: %w", err)
		}
		notes = append(notes, strings.TrimSpace(string(b)))
	}
	d.Notes = strings.Join(notes, "\n\n")
	return nil
}

// shellJoin quotes arguments that a POSIX shell would split or expand.
func shellJoin(args []string) string {
	q := make([]string, len(args))
	for i, a := range args {
		if a != "" && !strings.ContainsAny(a, " \t\n'\"\\$`*?[]{}()<>|&;#~!") {
			q[i] = a
			continue
		}
		q[i] = "'" + strings.ReplaceAll(a, "'", `'\''`) + "'"
	}
	return strings.Join(q, " ")
}

func gitSHA() string {
	out, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return "unknown (not a git checkout)"
	}
	return strings.TrimSpace(string(out))
}

func defaultHardware() string {
	return fmt.Sprintf("%s/%s, %d CPUs (where report ran; override with -hardware)", runtime.GOOS, runtime.GOARCH, runtime.NumCPU())
}

// defaultTemplate finds docs/results/TEMPLATE.md from the repository root or
// from harness/.
func defaultTemplate() string {
	for _, p := range []string{"docs/results/TEMPLATE.md", "../docs/results/TEMPLATE.md"} {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return "docs/results/TEMPLATE.md"
}
