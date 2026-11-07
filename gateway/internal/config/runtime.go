package config

import (
	"errors"
	"fmt"
	"math"
	"runtime/debug"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"
)

// Runtime tunes the Go garbage collector. Each knob is optional: zero leaves
// Go's default in place, or the GOGC / GOMEMLIMIT environment variable when
// one is set (the Go runtime reads those itself at startup).
//
// At thousands of open streams the gateway's live heap is large and long
// lived, so collections are rare but each one is big. A higher GCPercent
// makes them rarer still at the cost of a bigger heap; MemoryLimit keeps
// that from running the process into its container limit, since near the
// limit the collector runs as often as needed, whatever GCPercent says.
type Runtime struct {
	// GCPercent is GOGC: a collection starts once the heap has grown by this
	// percentage over the live heap the previous one left. 0 = unset.
	GCPercent int `yaml:"gc_percent"`
	// MemoryLimit is GOMEMLIMIT: a soft ceiling on memory the Go runtime
	// holds, e.g. 768MiB. Set it below the container's memory limit. 0 = unset.
	MemoryLimit ByteSize `yaml:"memory_limit"`
}

// Runtime validation limits.
const (
	// MaxGCPercent bounds runtime.gc_percent; beyond it the heap is
	// effectively unbounded without a memory limit.
	MaxGCPercent = 10000
	// MinMemoryLimit is the smallest runtime.memory_limit accepted, so a
	// missing unit ("768" bytes) fails at startup instead of making the
	// collector run continuously.
	MinMemoryLimit ByteSize = 64 << 20
)

// Apply sets the configured collector knobs and leaves unset ones alone. It
// changes process-wide state; call it once at startup.
func (r Runtime) Apply() {
	if r.GCPercent > 0 {
		debug.SetGCPercent(r.GCPercent)
	}
	if r.MemoryLimit > 0 {
		debug.SetMemoryLimit(int64(r.MemoryLimit))
	}
}

func (c *Config) validateRuntime() []error {
	var errs []error
	if p := c.Runtime.GCPercent; p < 0 || p > MaxGCPercent {
		errs = append(errs, fmt.Errorf("runtime.gc_percent: %d out of range [0, %d] (0 leaves Go's default)", p, MaxGCPercent))
	}
	if m := c.Runtime.MemoryLimit; m != 0 && m < MinMemoryLimit {
		errs = append(errs, fmt.Errorf("runtime.memory_limit: %d bytes is below the minimum %s (0 leaves it unset)", m, MinMemoryLimit))
	}
	return errs
}

// ByteSize is a size in bytes, written as an integer with an optional unit
// as in GOMEMLIMIT: B, KiB, MiB, GiB or TiB (e.g. 768MiB).
type ByteSize int64

var byteUnits = []struct {
	suffix string
	mult   int64
}{
	// Longest suffixes first, so "MiB" is not read as "B".
	{"TiB", 1 << 40}, {"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}, {"B", 1},
}

// ParseByteSize parses a size such as "768MiB" or "1073741824".
func ParseByteSize(s string) (ByteSize, error) {
	s = strings.TrimSpace(s)
	num, mult := s, int64(1)
	for _, u := range byteUnits {
		if strings.HasSuffix(s, u.suffix) {
			num, mult = strings.TrimSpace(strings.TrimSuffix(s, u.suffix)), u.mult
			break
		}
	}
	n, err := strconv.ParseInt(num, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid size %q (want e.g. 768MiB, 1GiB)", s)
	}
	if n > math.MaxInt64/mult {
		return 0, fmt.Errorf("size %q is too large", s)
	}
	return ByteSize(n * mult), nil
}

// String formats the size with the largest unit that divides it exactly.
func (b ByteSize) String() string {
	for _, u := range byteUnits {
		if b != 0 && int64(b)%u.mult == 0 {
			return strconv.FormatInt(int64(b)/u.mult, 10) + u.suffix
		}
	}
	return "0"
}

// UnmarshalYAML accepts a size with a unit ("768MiB") or plain bytes.
func (b *ByteSize) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind != yaml.ScalarNode {
		return errors.New("size must be a scalar, e.g. 768MiB")
	}
	v, err := ParseByteSize(n.Value)
	if err != nil {
		return err
	}
	*b = v
	return nil
}
