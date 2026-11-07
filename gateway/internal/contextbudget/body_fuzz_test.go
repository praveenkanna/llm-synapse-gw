package contextbudget

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex/bodyindextest"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

// FuzzEachMemberAgreesWithReference: context assembly reads the body's top
// level through the request's index (AD-15) as the encoding/json reference
// does: the last "messages" is the array it assembles, the last of each
// member /tokenize needs is the one it carries, and a body that is not an
// object is unparseable. eachMember, which now walks only nested objects
// (messages, content parts), visits the same members, in order, with the
// same value ranges as the reference on every valid JSON object. An
// escaped key means what it decodes to; any other key is its bytes as
// written (AD-14).
func FuzzEachMemberAgreesWithReference(f *testing.F) {
	bodyindextest.AddSeeds(f, "../../testdata/wire")
	f.Fuzz(func(t *testing.T, body []byte) {
		ref, _, refOK := bodyindextest.Members(body)
		parseAgrees(t, body, ref, refOK)
		if !json.Valid(body) {
			return // nothing reads such a body further
		}
		type member struct {
			key []byte
			v   [2]int
		}
		var got []member
		err := eachMember(body, bodyindex.SkipSpace(body, 0), func(key []byte, v [2]int) error {
			got = append(got, member{key, v})
			return nil
		})
		if (err == nil) != refOK {
			t.Fatalf("err = %v, reference ok %v, body %q", err, refOK, body)
		}
		if !refOK {
			return
		}
		if len(got) != len(ref) {
			t.Fatalf("%d members, reference %d, body %q", len(got), len(ref), body)
		}
		for i, m := range ref {
			key := m.RawKey
			if m.Escaped {
				key = m.Key
			}
			if !bodyindex.KeyIs(got[i].key, key) || got[i].v != m.Value {
				t.Fatalf("member %d = %q %v, reference %q %v, body %q", i, got[i].key, got[i].v, key, m.Value, body)
			}
			for _, name := range []string{"role", "content", "type"} {
				if bodyindex.KeyIs(got[i].key, name) != (key == name) {
					t.Fatalf("member %d reads %q as %q %v, body %q", i, got[i].key, name, key != name, body)
				}
			}
		}
	})
}

// parseAgrees fails t unless Parse, reading body's index, finds the
// messages and the /tokenize members the reference does.
func parseAgrees(t *testing.T, body []byte, ref []bodyindextest.Member, refOK bool) {
	t.Helper()
	q, err := parseBody(body, 0)
	var se *SkipError
	if !refOK {
		if !errors.As(err, &se) || se.Reason != telemetry.ReasonUnparseableBody {
			t.Fatalf("not an object: Parse = %v, want %s, body %q", err, telemetry.ReasonUnparseableBody, body)
		}
		return
	}
	name := func(m bodyindextest.Member) string { // the index's naming rule
		if m.Escaped {
			return m.Key
		}
		return m.RawKey
	}
	last := func(want string) (bodyindextest.Member, bool) {
		var found bodyindextest.Member
		n := 0
		for _, m := range ref {
			if name(m) == want {
				found, n = m, n+1
			}
		}
		return found, n > 0
	}
	for _, m := range unmeasurableMembers {
		if _, ok := last(m); ok {
			if !errors.As(err, &se) || se.Reason != ReasonUnmeasurable {
				t.Fatalf("%q present: Parse = %v, want %s, body %q", m, err, ReasonUnmeasurable, body)
			}
			return
		}
	}
	msgs, ok := last("messages")
	if !ok || body[msgs.Value[0]] != '[' {
		if err == nil {
			t.Fatalf("no messages array, but Parse read one, body %q", body)
		}
		return
	}
	if err != nil {
		return // the messages themselves are refused: nested rules, not the top level
	}
	if q.open != msgs.Value[0] || q.close != msgs.Value[1]-1 {
		t.Fatalf("messages at [%d,%d], reference %v, body %q", q.open, q.close, msgs.Value, body)
	}
	var want [][]byte
	for _, m := range tokenizeMembers {
		if v, ok := last(m); ok {
			want = append(want, fmt.Appendf(nil, "%q:%s", m, body[v.Value[0]:v.Value[1]]))
		}
	}
	if fmt.Sprintf("%q", q.passthrough) != fmt.Sprintf("%q", want) {
		t.Fatalf("passthrough %q, reference %q, body %q", q.passthrough, want, body)
	}
}
