package config

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestPriorityHeaderBoundConstant(t *testing.T) {
	if MaxPriorityHeaderBytes != 256 {
		t.Errorf("MaxPriorityHeaderBytes = %d: a change moves what is read, and the results file", MaxPriorityHeaderBytes)
	}
	if name := strings.Repeat("a", 63); !namePattern.MatchString(name) || MaxPriorityHeaderBytes < len(name) {
		t.Errorf("the longest class name must fit the bound")
	}
}

func TestResolveClassHeaderBound(t *testing.T) {
	p := boundPriority()
	pad := func(n int, class string) string { return class + strings.Repeat(" ", n-len(class)) }
	tests := []struct {
		name          string
		header        string
		authenticated bool
		want          string
	}{
		{name: "at the limit, padded", header: pad(MaxPriorityHeaderBytes, PriorityBackground), authenticated: true, want: PriorityBackground},
		{name: "one over, padded", header: pad(MaxPriorityHeaderBytes+1, PriorityBackground), authenticated: true, want: PriorityUser},
		{name: "over, upper case", header: strings.ToUpper(pad(MaxPriorityHeaderBytes+1, PriorityBackground)), authenticated: true, want: PriorityUser},
		{name: "over, non-ASCII space around it", header: strings.Repeat("\u00a0", MaxPriorityHeaderBytes) + PriorityBackground, authenticated: true, want: PriorityUser},
		{name: "a megabyte", header: strings.Repeat("x", 1<<20), authenticated: true, want: PriorityUser},
		{name: "over cannot lift an anonymous caller", header: pad(MaxPriorityHeaderBytes+1, PriorityInteractive), want: PriorityUser},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := p.ResolveClass(tc.header, "caller", tc.authenticated); got != tc.want {
				t.Fatalf("ResolveClass = %q, want %q", got, tc.want)
			}
		})
	}
}

// An over-long header resolves as absent: the caller's own default applies,
// clamped as ever.
func TestResolveClassOverLongHeaderTakesTheCallerDefault(t *testing.T) {
	p := boundPriority()
	p.Callers = map[string]CallerPolicy{"batch": {Default: PriorityBackground}}
	if got := p.ResolveClass(strings.Repeat("x", MaxPriorityHeaderBytes+1), "batch", true); got != PriorityBackground {
		t.Fatalf("ResolveClass = %q, want the caller's default", got)
	}
}

func resolveCost(p PriorityConfig, header string, runs int) time.Duration {
	best := time.Hour
	for range runs {
		start := time.Now()
		p.ResolveClass(header, "caller", true)
		if d := time.Since(start); d < best {
			best = d
		}
	}
	return best
}

// TestResolveClassCostStopsAtTheBound: each shape is what one step of the
// read walks: non-ASCII space for the trim, non-ASCII letters for the
// lower-casing. The ratio of the two sizes is 256.
func TestResolveClassCostStopsAtTheBound(t *testing.T) {
	const maxRatio = 4
	p := boundPriority()
	for name, unit := range map[string]string{"space": "\u00a0", "letters": "\u00c9", "ascii upper": "AA"} {
		t.Run(name, func(t *testing.T) {
			base := strings.Repeat(unit, 2<<10)
			huge := strings.Repeat(unit, 512<<10)
			var a, b time.Duration
			for range 5 {
				a, b = resolveCost(p, base, 100), resolveCost(p, huge, 100)
				if b <= maxRatio*a {
					return
				}
			}
			t.Fatalf("a resolve took %v at 1 MiB, %v at 4 KiB", b, a)
		})
	}
}

func TestModelNameBoundInConfig(t *testing.T) {
	if MaxModelNameBytes != 256 {
		t.Fatalf("MaxModelNameBytes = %d", MaxModelNameBytes)
	}
	if MaxStuckProbeModelLen != MaxModelNameBytes {
		t.Fatalf("a probe model may be %d bytes, a mapped one %d", MaxStuckProbeModelLen, MaxModelNameBytes)
	}
	for _, tc := range []struct {
		n       int
		wantErr bool
	}{{MaxModelNameBytes, false}, {MaxModelNameBytes + 1, true}} {
		_, err := Parse([]byte(boundConfig(strings.Repeat("m", tc.n), "")), func(string) string { return "" })
		switch {
		case tc.wantErr && (err == nil || !strings.Contains(err.Error(), "models")):
			t.Errorf("%d bytes: err = %v, want a models error", tc.n, err)
		case !tc.wantErr && err != nil:
			t.Errorf("%d bytes: %v", tc.n, err)
		}
	}
}

func TestTokenBoundInConfig(t *testing.T) {
	for _, tc := range []struct {
		n       int
		wantErr bool
	}{{MaxTokenLength, false}, {MaxTokenLength + 1, true}} {
		token := strings.Repeat("t", tc.n)
		_, err := Parse([]byte(boundConfig("m", "auth: {enabled: true, callers: [{name: chat, token: "+token+"}]}")), func(string) string { return "" })
		switch {
		case tc.wantErr && (err == nil || !strings.Contains(err.Error(), "longer than")):
			t.Errorf("%d bytes: err = %v, want a length error", tc.n, err)
		case !tc.wantErr && err != nil:
			t.Errorf("%d bytes: %v", tc.n, err)
		}
	}
}

// boundConfig is a one-pool config mapping model; extra replaces auth off.
func boundConfig(model, extra string) string {
	if extra == "" {
		extra = "auth: {enabled: false}"
	}
	return `
pools:
  - {name: large, replicas: [{name: large-0, url: "http://127.0.0.1:1"}]}
models: {` + model + `: large}
default_pool: large
` + extra + "\n"
}

// boundPriority is the default classes and clamps.
func boundPriority() PriorityConfig {
	return PriorityConfig{
		Classes:           []string{PriorityInteractive, PriorityUser, PriorityBackground},
		DefaultClass:      PriorityUser,
		AnonymousMaxClass: PriorityUser,
		DefaultMaxClass:   PriorityInteractive,
	}
}

// BenchmarkResolveClassHeader times a resolve at and past the limit
// (docs/results). Sizes are literal, so a run before the bound compares
// like for like.
func BenchmarkResolveClassHeader(b *testing.B) {
	p := boundPriority()
	for _, size := range []int{256, 8 << 10, 1 << 20} {
		for name, unit := range map[string]string{"space": "\u00a0", "letters": "\u00c9", "ascii": "xx"} {
			header := strings.Repeat(unit, size/2)
			b.Run(name+"_"+strconv.Itoa(size), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					p.ResolveClass(header, "caller", true)
				}
			})
		}
	}
}
