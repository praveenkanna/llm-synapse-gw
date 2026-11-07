package config

import (
	"runtime/debug"
	"testing"
)

func TestParseByteSize(t *testing.T) {
	tests := []struct {
		in      string
		want    ByteSize
		wantErr bool
	}{
		{in: "0", want: 0},
		{in: "1024", want: 1024},
		{in: "512B", want: 512},
		{in: "4KiB", want: 4 << 10},
		{in: "768MiB", want: 768 << 20},
		{in: " 1 GiB ", want: 1 << 30},
		{in: "2TiB", want: 2 << 40},
		{in: "", wantErr: true},
		{in: "MiB", wantErr: true},
		{in: "1.5GiB", wantErr: true},
		{in: "-1MiB", wantErr: true},
		{in: "768MB", wantErr: true},
		{in: "99999999TiB", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseByteSize(tc.in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ParseByteSize(%q) err = %v, wantErr %v", tc.in, err, tc.wantErr)
			}
			if !tc.wantErr && got != tc.want {
				t.Fatalf("ParseByteSize(%q) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

func TestByteSizeString(t *testing.T) {
	tests := map[ByteSize]string{0: "0", 512: "512B", 768 << 20: "768MiB", 1 << 30: "1GiB", 1536: "1536B"}
	for in, want := range tests {
		if got := in.String(); got != want {
			t.Errorf("ByteSize(%d).String() = %q, want %q", int64(in), got, want)
		}
	}
}

func TestRuntimeApply(t *testing.T) {
	prevGC := debug.SetGCPercent(100)
	prevLimit := debug.SetMemoryLimit(-1)
	t.Cleanup(func() {
		debug.SetGCPercent(prevGC)
		debug.SetMemoryLimit(prevLimit)
	})

	t.Run("unset knobs leave the runtime alone", func(t *testing.T) {
		debug.SetGCPercent(123)
		debug.SetMemoryLimit(1 << 40)
		Runtime{}.Apply()
		if got := debug.SetGCPercent(123); got != 123 {
			t.Errorf("GC percent = %d, want untouched 123", got)
		}
		if got := debug.SetMemoryLimit(-1); got != 1<<40 {
			t.Errorf("memory limit = %d, want untouched %d", got, int64(1<<40))
		}
	})
	t.Run("set knobs are applied", func(t *testing.T) {
		Runtime{GCPercent: 400, MemoryLimit: 768 << 20}.Apply()
		if got := debug.SetGCPercent(100); got != 400 {
			t.Errorf("GC percent = %d, want 400", got)
		}
		if got := debug.SetMemoryLimit(-1); got != 768<<20 {
			t.Errorf("memory limit = %d, want %d", got, int64(768<<20))
		}
	})
}
