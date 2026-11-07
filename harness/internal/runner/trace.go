package runner

import (
	"encoding/binary"
	"encoding/hex"
	"math/rand/v2"
)

// headerTraceparent is the W3C Trace Context header. Every request carries a
// fresh one so a harness result can be matched to the gateway's log line.
const headerTraceparent = "traceparent"

// newTraceparent returns a version-00, sampled traceparent with a random
// trace ID and parent ID, plus the trace ID alone. It draws from the global
// (runtime-seeded) generator, not the run's seed: trace IDs must differ
// between runs, and the request sequence must not depend on them.
func newTraceparent() (header, traceID string) {
	var ids [24]byte // 16-byte trace ID, then 8-byte parent ID
	binary.LittleEndian.PutUint64(ids[0:], nonZeroUint64())
	binary.LittleEndian.PutUint64(ids[8:], rand.Uint64())
	binary.LittleEndian.PutUint64(ids[16:], nonZeroUint64())
	var buf [55]byte // "00-" + 32 hex + "-" + 16 hex + "-01"
	copy(buf[:], "00-")
	hex.Encode(buf[3:35], ids[:16])
	buf[35] = '-'
	hex.Encode(buf[36:52], ids[16:])
	copy(buf[52:], "-01")
	return string(buf[:]), string(buf[3:35])
}

// nonZeroUint64 returns a random non-zero value; the W3C spec treats an
// all-zero trace or parent ID as invalid.
func nonZeroUint64() uint64 {
	for {
		if v := rand.Uint64(); v != 0 {
			return v
		}
	}
}
