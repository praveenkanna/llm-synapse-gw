package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
	"time"
)

func TestParseClassSpec(t *testing.T) {
	tests := []struct {
		in      string
		want    ClassSpec
		wantErr bool
	}{
		{in: "chat", want: ClassSpec{Name: "chat", Weight: 1, Stream: true, MaxTokens: 128, PromptWords: 64}},
		{in: "bg:0.5:nostream:512:300", want: ClassSpec{Name: "bg", Weight: 0.5, Stream: false, MaxTokens: 512, PromptWords: 300}},
		{in: "x:2:stream", want: ClassSpec{Name: "x", Weight: 2, Stream: true, MaxTokens: 128, PromptWords: 64}},
		{in: "", wantErr: true},
		{in: "a:0", wantErr: true},
		{in: "a:-1", wantErr: true},
		{in: "a:w", wantErr: true},
		{in: "a:1:maybe", wantErr: true},
		{in: "a:1:stream:0", wantErr: true},
		{in: "a:1:stream:10:0", wantErr: true},
		{in: "a:1:stream:10:5:extra", wantErr: true},
		{in: "a b", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseClassSpec(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error, got %+v", got)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("got %+v, %v; want %+v", got, err, tc.want)
			}
		})
	}
}

func testSynthetic(t *testing.T, api string, seed uint64) *Synthetic {
	t.Helper()
	s, err := NewSynthetic(SyntheticConfig{
		Model: "m", API: api, SystemWords: 16,
		Classes: []ClassSpec{
			{Name: "a", Weight: 3, Stream: true, MaxTokens: 8, PromptWords: 4},
			{Name: "b", Weight: 1, Stream: false, MaxTokens: 16, PromptWords: 2},
		},
	}, seed)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSyntheticDeterministicAndWeighted(t *testing.T) {
	const n = 20000
	x, y, z := testSynthetic(t, APIChat, 42), testSynthetic(t, APIChat, 42), testSynthetic(t, APIChat, 43)
	counts := map[string]int{}
	differs := false
	for range n {
		rx, ry, rz := x.Next(), y.Next(), z.Next()
		if rx.Class != ry.Class || string(rx.Body) != string(ry.Body) {
			t.Fatal("same seed gave different requests")
		}
		differs = differs || string(rx.Body) != string(rz.Body)
		counts[rx.Class]++
	}
	if !differs {
		t.Fatal("different seeds gave identical requests")
	}
	if got := float64(counts["a"]) / n; math.Abs(got-0.75) > 0.02 {
		t.Fatalf("class a share = %.3f, want 0.75 ±0.02", got)
	}
}

func TestSyntheticBodies(t *testing.T) {
	type body struct {
		Model    string `json:"model"`
		Messages []struct {
			Role, Content string
		} `json:"messages"`
		Prompt        string `json:"prompt"`
		MaxTokens     int    `json:"max_tokens"`
		Stream        bool   `json:"stream"`
		StreamOptions *struct {
			IncludeUsage bool `json:"include_usage"`
		} `json:"stream_options"`
	}
	systems := map[string]string{}
	chat := testSynthetic(t, APIChat, 1)
	for range 200 {
		r := chat.Next()
		var b body
		if err := json.Unmarshal(r.Body, &b); err != nil {
			t.Fatal(err)
		}
		if r.Path != pathChat || b.Model != "m" || b.Stream != r.Stream || len(b.Messages) != 2 {
			t.Fatalf("bad chat request %s %s", r.Path, r.Body)
		}
		if r.Stream != (b.StreamOptions != nil && b.StreamOptions.IncludeUsage) {
			t.Fatalf("stream_options.include_usage must be set exactly on streams: %s", r.Body)
		}
		wantMax := map[string]int{"a": 8, "b": 16}[r.Class]
		if b.MaxTokens != wantMax {
			t.Fatalf("max_tokens = %d, want %d", b.MaxTokens, wantMax)
		}
		// One shared system prompt per class: a stable prefix across requests.
		if prev, ok := systems[r.Class]; ok && prev != b.Messages[0].Content {
			t.Fatalf("class %s system prompt changed", r.Class)
		}
		systems[r.Class] = b.Messages[0].Content
	}
	if systems["a"] == systems["b"] {
		t.Fatal("classes share a system prompt")
	}

	comp := testSynthetic(t, APICompletions, 1)
	r := comp.Next()
	var b body
	if err := json.Unmarshal(r.Body, &b); err != nil {
		t.Fatal(err)
	}
	if r.Path != pathCompletions || b.Prompt == "" || len(b.Messages) != 0 {
		t.Fatalf("bad completions request %s %s", r.Path, r.Body)
	}
}

func TestNewSyntheticValidation(t *testing.T) {
	ok := []ClassSpec{{Name: "a", Weight: 1, MaxTokens: 1, PromptWords: 1}}
	tests := []struct {
		name string
		cfg  SyntheticConfig
	}{
		{"no model", SyntheticConfig{Classes: ok}},
		{"no classes", SyntheticConfig{Model: "m"}},
		{"bad api", SyntheticConfig{Model: "m", API: "embeddings", Classes: ok}},
		{"negative system words", SyntheticConfig{Model: "m", SystemWords: -1, Classes: ok}},
		{"duplicate class", SyntheticConfig{Model: "m", Classes: slices.Concat(ok, ok)}},
		{"invalid class", SyntheticConfig{Model: "m", Classes: []ClassSpec{{Name: "a"}}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewSynthetic(tc.cfg, 1); err == nil {
				t.Fatal("want error")
			}
		})
	}
}

// timedSource records the offsets the runner passes to NextAt.
type timedSource struct {
	ats   []time.Duration
	plain int
}

func (s *timedSource) Next() Request {
	s.plain++
	return Request{Class: "plain", Path: pathChat, Body: []byte(`{}`)}
}

func (s *timedSource) NextAt(at time.Duration) Request {
	s.ats = append(s.ats, at)
	return Request{Class: "timed", Path: pathChat, Body: []byte(`{}`)}
}

func TestRunnerUsesTimedSource(t *testing.T) {
	srv := (&fakeUpstream{tokens: 1}).start(t)
	run := func(cfg Config) *timedSource {
		src := &timedSource{}
		var raw bytes.Buffer
		r, err := New(cfg, src, newClient(srv.URL), &raw)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := r.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		return src
	}

	open := run(Config{Rate: 500, MaxRequests: 40, MaxInFlight: 100, Seed: 3})
	if open.plain != 0 || len(open.ats) != 40 || !slices.IsSorted(open.ats) {
		t.Fatalf("open loop: Next called %d times, NextAt %d times (sorted %v)", open.plain, len(open.ats), slices.IsSorted(open.ats))
	}
	// The offsets are the arrival schedule itself.
	arr, _ := NewArrivals(500, Burst{}, rand.New(rand.NewPCG(3, 0x5eed)))
	for i, at := range open.ats {
		if want := arr.Next(); at != want {
			t.Fatalf("arrival %d: NextAt(%v), schedule says %v", i, at, want)
		}
	}

	closed := run(Config{Concurrency: 2, MaxRequests: 10, Seed: 3})
	if closed.plain != 10 || len(closed.ats) != 0 {
		t.Fatalf("closed loop: Next called %d times, NextAt %d times; want Next only", closed.plain, len(closed.ats))
	}
}
