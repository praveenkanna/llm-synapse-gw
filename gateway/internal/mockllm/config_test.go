package mockllm

import (
	"math/rand/v2"
	"strings"
	"testing"
	"time"
)

func TestDefaultsAreValid(t *testing.T) {
	if err := Defaults().Validate(); err != nil {
		t.Fatalf("Defaults().Validate() = %v", err)
	}
}

func TestFromEnv(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr string
		check   func(t *testing.T, c Config)
	}{
		{
			name: "defaults when unset",
			env:  map[string]string{},
			check: func(t *testing.T, c Config) {
				if c.ListenAddr != DefaultListenAddr || c.TTFT != DefaultTTFT || c.Models[0] != DefaultModel || !c.ControlEnabled || c.VLLMCompat != CompatCurrent {
					t.Fatalf("unexpected defaults: %+v", c)
				}
			},
		},
		{
			name: "all overrides",
			env: map[string]string{
				EnvListenAddr: "127.0.0.1:9000", EnvModels: " a , b ,,", EnvMaxModelLen: "4096",
				EnvTTFT: "120ms", EnvTokensPerSecond: "33.5", EnvOutputTokens: "normal:100:10",
				EnvMaxNumSeqs: "8", EnvFault5xxRate: "0.1", EnvFaultDropRate: "0.2", EnvFaultHangRate: "0.05",
				EnvFaultStallRate: "0.05", EnvErrorStatus: "503", EnvCaptureEntries: "10", EnvCaptureMaxBytes: "100",
				EnvMaxBodyBytes: "2048", EnvControlEnabled: "false", EnvSeed: "42", EnvVLLMCompat: " Legacy ",
			},
			check: func(t *testing.T, c Config) {
				if c.ListenAddr != "127.0.0.1:9000" || strings.Join(c.Models, "|") != "a|b" || c.MaxModelLen != 4096 {
					t.Fatalf("addr/models/len wrong: %+v", c)
				}
				if c.TTFT != 120*time.Millisecond || c.TokensPerSecond != 33.5 || c.OutputTokens != (LengthDist{Kind: DistNormal, A: 100, B: 10}) {
					t.Fatalf("timing/dist wrong: %+v", c)
				}
				want := FaultRates{Error5xx: 0.1, Drop: 0.2, Hang: 0.05, Stall: 0.05}
				if c.MaxNumSeqs != 8 || c.Faults != want || c.ErrorStatus != 503 {
					t.Fatalf("seqs/faults wrong: %+v", c)
				}
				if c.CaptureEntries != 10 || c.CaptureMaxBytes != 100 || c.MaxBodyBytes != 2048 || c.ControlEnabled || c.Seed != 42 {
					t.Fatalf("capture/control/seed wrong: %+v", c)
				}
				if c.VLLMCompat != CompatLegacy {
					t.Fatalf("vllm compat = %q, want legacy", c.VLLMCompat)
				}
			},
		},
		{name: "bad duration", env: map[string]string{EnvTTFT: "fast"}, wantErr: EnvTTFT},
		{name: "bad int", env: map[string]string{EnvMaxNumSeqs: "x"}, wantErr: EnvMaxNumSeqs},
		{name: "bad float", env: map[string]string{EnvFault5xxRate: "x"}, wantErr: EnvFault5xxRate},
		{name: "bad bool", env: map[string]string{EnvControlEnabled: "maybe"}, wantErr: EnvControlEnabled},
		{name: "bad dist", env: map[string]string{EnvOutputTokens: "zipf:1"}, wantErr: EnvOutputTokens},
		{name: "rates over one", env: map[string]string{EnvFault5xxRate: "0.6", EnvFaultDropRate: "0.6"}, wantErr: "sum"},
		{name: "rate out of range", env: map[string]string{EnvFaultHangRate: "1.5"}, wantErr: "hang"},
		{name: "non-5xx status", env: map[string]string{EnvErrorStatus: "404"}, wantErr: "5xx"},
		{name: "negative ttft", env: map[string]string{EnvTTFT: "-1s"}, wantErr: "ttft"},
		{name: "capture too big", env: map[string]string{EnvCaptureEntries: "100000"}, wantErr: "capture entries"},
		{name: "empty models", env: map[string]string{EnvModels: " , "}, wantErr: "model"},
		{name: "zero body limit", env: map[string]string{EnvMaxBodyBytes: "0"}, wantErr: "max body"},
		{
			name: "vllm compat current", env: map[string]string{EnvVLLMCompat: "current"},
			check: func(t *testing.T, c Config) {
				if c.VLLMCompat != CompatCurrent {
					t.Fatalf("vllm compat = %q, want current", c.VLLMCompat)
				}
			},
		},
		{name: "bad vllm compat", env: map[string]string{EnvVLLMCompat: "v0.6"}, wantErr: EnvVLLMCompat},
		{
			name: "scheduling policy default", env: map[string]string{},
			check: func(t *testing.T, c Config) {
				if c.SchedulingPolicy != PolicyFCFS {
					t.Fatalf("scheduling policy = %q, want fcfs", c.SchedulingPolicy)
				}
			},
		},
		{
			name: "scheduling policy priority", env: map[string]string{EnvSchedulingPolicy: " Priority "},
			check: func(t *testing.T, c Config) {
				if c.SchedulingPolicy != PolicyPriority {
					t.Fatalf("scheduling policy = %q, want priority", c.SchedulingPolicy)
				}
			},
		},
		{name: "bad scheduling policy", env: map[string]string{EnvSchedulingPolicy: "lifo"}, wantErr: EnvSchedulingPolicy},
		{
			name: "prefix cache default", env: map[string]string{},
			check: func(t *testing.T, c Config) {
				if c.PrefixCacheBlocks != DefaultPrefixCacheBlocks {
					t.Fatalf("prefix cache blocks = %d, want %d", c.PrefixCacheBlocks, DefaultPrefixCacheBlocks)
				}
			},
		},
		{
			name: "prefix cache off", env: map[string]string{EnvPrefixCacheBlocks: "0"},
			check: func(t *testing.T, c Config) {
				if c.PrefixCacheBlocks != 0 {
					t.Fatalf("prefix cache blocks = %d, want 0", c.PrefixCacheBlocks)
				}
			},
		},
		{
			name: "prefix cache size", env: map[string]string{EnvPrefixCacheBlocks: "1000"},
			check: func(t *testing.T, c Config) {
				if c.PrefixCacheBlocks != 1000 {
					t.Fatalf("prefix cache blocks = %d, want 1000", c.PrefixCacheBlocks)
				}
			},
		},
		{name: "bad prefix cache", env: map[string]string{EnvPrefixCacheBlocks: "lots"}, wantErr: EnvPrefixCacheBlocks},
		{name: "negative prefix cache", env: map[string]string{EnvPrefixCacheBlocks: "-1"}, wantErr: "prefix cache blocks"},
		{name: "prefix cache too big", env: map[string]string{EnvPrefixCacheBlocks: "100000000"}, wantErr: "prefix cache blocks"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, err := FromEnv(func(k string) string { return tc.env[k] })
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("FromEnv error = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("FromEnv: %v", err)
			}
			tc.check(t, c)
		})
	}
}

func TestValidateRejectsUnknownCompat(t *testing.T) {
	c := Defaults()
	c.VLLMCompat = "ancient"
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "vllm compat") {
		t.Fatalf("Validate() = %v, want vllm compat error", err)
	}
	c.VLLMCompat = ""
	if err := c.Validate(); err == nil {
		t.Fatal("Validate() accepted an empty vllm compat")
	}
}

func TestValidateRejectsUnknownSchedulingPolicy(t *testing.T) {
	for _, p := range []SchedulingPolicy{"", "lifo", "FCFS"} {
		c := Defaults()
		c.SchedulingPolicy = p
		if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "scheduling policy") {
			t.Errorf("Validate() with policy %q = %v, want scheduling policy error", p, err)
		}
	}
}

func TestParseLengthDist(t *testing.T) {
	tests := []struct {
		spec    string
		want    LengthDist
		wantErr bool
	}{
		{spec: "fixed:10", want: LengthDist{Kind: DistFixed, A: 10}},
		{spec: "uniform:1:5", want: LengthDist{Kind: DistUniform, A: 1, B: 5}},
		{spec: "NORMAL:50:0", want: LengthDist{Kind: DistNormal, A: 50, B: 0}},
		{spec: "fixed", wantErr: true},
		{spec: "fixed:1:2", wantErr: true},
		{spec: "fixed:0", wantErr: true},
		{spec: "uniform:5:1", wantErr: true},
		{spec: "uniform:0:5", wantErr: true},
		{spec: "normal:10:-1", wantErr: true},
		{spec: "normal:NaN:1", wantErr: true},
		{spec: "fixed:abc", wantErr: true},
		{spec: "poisson:3", wantErr: true},
		{spec: "fixed:100000000", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.spec, func(t *testing.T) {
			got, err := ParseLengthDist(tc.spec)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseLengthDist(%q) = %+v, want error", tc.spec, got)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("ParseLengthDist(%q) = %+v, %v; want %+v", tc.spec, got, err, tc.want)
			}
			if back, err := ParseLengthDist(got.String()); err != nil || back != got {
				t.Fatalf("String round trip %q -> %+v, %v", got.String(), back, err)
			}
		})
	}
}

func TestLengthDistSample(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	tests := []struct {
		dist   LengthDist
		lo, hi int
	}{
		{LengthDist{Kind: DistFixed, A: 7}, 7, 7},
		{LengthDist{Kind: DistUniform, A: 3, B: 9}, 3, 9},
		{LengthDist{Kind: DistNormal, A: 2, B: 50}, 1, maxOutputTokens}, // clamped at 1
	}
	for _, tc := range tests {
		t.Run(tc.dist.String(), func(t *testing.T) {
			seen := map[int]bool{}
			for range 2000 {
				n := tc.dist.Sample(r)
				if n < tc.lo || n > tc.hi {
					t.Fatalf("Sample = %d, want in [%d,%d]", n, tc.lo, tc.hi)
				}
				seen[n] = true
			}
			if tc.dist.Kind == DistUniform && len(seen) != tc.hi-tc.lo+1 {
				t.Fatalf("uniform covered %d values, want %d", len(seen), tc.hi-tc.lo+1)
			}
		})
	}
}
