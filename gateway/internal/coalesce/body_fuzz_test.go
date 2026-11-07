package coalesce

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"testing"
	"unicode/utf8"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex/bodyindextest"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// FuzzTopLevelAgreesWithReference: the coalescing key reads the body's
// top level through the request's index (AD-15) and finds the same names,
// last values and counts as the encoding/json reference on every body
// FingerprintOf reads (valid UTF-8 and JSON). A body with a key written
// with an escape has no key (ADR 0019): that rule is coalescing's own. And
// the key is the one the map-based reader of AD-14 and before gave (cache
// entries keep their keys across the change), computed here from the
// reference's members.
func FuzzTopLevelAgreesWithReference(f *testing.F) {
	bodyindextest.AddSeeds(f, "../../testdata/wire")
	f.Fuzz(func(t *testing.T, body []byte) {
		var ix bodyindex.Index
		readErr := ix.Read(body)
		ref, _, refOK := bodyindextest.Members(body)
		if errors.Is(readErr, bodyindex.ErrTooManyMembers) { // no key past the ceiling (E5)
			if !refOK || len(ref) <= bodyindex.MaxMembers {
				t.Fatalf("past the ceiling, but the reference has %d members (ok %v), body %q", len(ref), refOK, body)
			}
			want := telemetry.ReasonTooManyMembers
			if !utf8.Valid(body) {
				want = telemetry.ReasonUnparseableBody // the UTF-8 rule comes first
			}
			if _, reason := fingerprintOf(body, 1<<20); reason != want {
				t.Fatalf("reason %q past the ceiling, want %q, body %q", reason, want, body)
			}
			return
		}
		if (readErr == nil) != refOK {
			t.Fatalf("read %v, reference ok %v, body %q", readErr, refOK, body)
		}
		got, wantReason := fingerprintOf(body, 1<<20)
		ref0, refReason := legacyFingerprint(body, ref, refOK)
		if wantReason != refReason || got != ref0 {
			t.Fatalf("fingerprint %x (%q), the earlier reader's %x (%q), body %q", got.digest, wantReason, ref0.digest, refReason, body)
		}
		if !refOK || !utf8.Valid(body) {
			return
		}
		var buf [inlineNames]named
		names, reason := topLevel(&ix, buf[:0])
		escaped := slices.ContainsFunc(ref, func(m bodyindextest.Member) bool { return m.Escaped })
		if (reason == telemetry.ReasonUnsupportedKey) != escaped {
			t.Fatalf("reason %q, reference has an escaped key %v, body %q", reason, escaped, body)
		}
		if escaped {
			return
		}
		want, counts := map[string][]byte{}, map[string]int{}
		for _, m := range ref {
			want[m.RawKey] = body[m.Value[0]:m.Value[1]] // the last of a repeated name wins
			counts[m.RawKey]++
		}
		if len(names) != len(want) {
			t.Fatalf("%d names, reference %d, body %q", len(names), len(want), body)
		}
		for i, n := range names {
			if i > 0 && bytes.Compare(names[i-1].key, n.key) >= 0 {
				t.Fatalf("names out of order: %q before %q, body %q", names[i-1].key, n.key, body)
			}
			if v, ok := want[string(n.key)]; !ok || !bytes.Equal(n.value, v) || n.count != counts[string(n.key)] {
				t.Fatalf("name %q = %q x%d, reference %q x%d, body %q", n.key, n.value, n.count, v, counts[string(n.key)], body)
			}
		}
	})
}

// legacyFingerprint is FingerprintOf as it was before the index (a map of
// names, sorted), on the reference's reading of body: the key must not
// change with the reader.
func legacyFingerprint(body []byte, ref []bodyindextest.Member, ok bool) (Fingerprint, string) {
	if !utf8.Valid(body) || !ok {
		return Fingerprint{}, telemetry.ReasonUnparseableBody
	}
	members, counts := map[string][]byte{}, map[string]int{}
	for _, m := range ref {
		if m.Escaped {
			return Fingerprint{}, telemetry.ReasonUnsupportedKey
		}
		members[m.RawKey] = body[m.Value[0]:m.Value[1]]
		counts[m.RawKey]++
	}
	names := make([]string, 0, len(members))
	for name := range members {
		if !ignorable([]byte(name), members[name], counts[name]) {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	h := sha256.New()
	for _, name := range names {
		field := name
		if counts[name] > 1 && (name == "priority" || name == "user") {
			field += "\x00" + strconv.Itoa(counts[name])
		}
		writeField(h, []byte(field))
		var compact bytes.Buffer
		_ = json.Compact(&compact, members[name])
		writeField(h, compact.Bytes())
	}
	var fp Fingerprint
	h.Sum(fp.digest[:0])
	fp.Stream = string(members["stream"]) == "true"
	fp.Deterministic = bodyindex.IsZeroNumber(members["temperature"]) || isInteger(members["seed"])
	return fp, ""
}
