package bodyindex_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex/bodyindextest"
)

// lookedUp are the names the gateway's readers look up.
var lookedUp = []string{"model", "stream", "priority", "max_tokens", "max_completion_tokens", "messages"}

// sameAsReference fails t unless x indexes body as the encoding/json
// reference reads it, Find included.
func sameAsReference(t *testing.T, x *bodyindex.Index, body []byte, ok bool) {
	t.Helper()
	ref, open, refOK := bodyindextest.Members(body)
	if ok != refOK {
		t.Fatalf("ok = %v, reference %v, body %q", ok, refOK, body)
	}
	if !ok {
		if !errors.Is(x.Err(), bodyindex.ErrNotObject) || x.Len() != 0 || !bytes.Equal(x.Body(), body) {
			t.Fatalf("not an object: Err %v, %d members, body kept %v, body %q", x.Err(), x.Len(), bytes.Equal(x.Body(), body), body)
		}
		x.Walk(func(m bodyindex.Member) {
			t.Fatalf("Walk of a body that is not an object visited %+v, body %q", m, body)
		})
		return
	}
	if x.Open() != open || x.Len() != len(ref) {
		t.Fatalf("open %d, %d members; reference %d, %d; body %q", x.Open(), x.Len(), open, len(ref), body)
	}
	if end := bytes.LastIndexByte(body, '}'); x.Close() != end {
		t.Fatalf("close %d, want %d, body %q", x.Close(), end, body)
	}
	kinds := map[string]bodyindex.Kind{"string": bodyindex.String, "number": bodyindex.Number, "object": bodyindex.Object,
		"array": bodyindex.Array, "bool": bodyindex.Bool, "null": bodyindex.Null}
	for i, r := range ref {
		m := x.Member(i)
		name := r.RawKey // an unescaped key is its bytes as written
		if r.Escaped {
			name = r.Key
		}
		if m.Key[0] != r.Start || string(x.RawKey(m)) != r.RawKey || m.Escaped != r.Escaped || x.Name(m) != name ||
			m.Value != r.Value || m.Comma != r.Comma {
			t.Fatalf("member %d = %+v (%q), reference %+v, body %q", i, m, x.Name(m), r, body)
		}
		sameMeaning(t, x, m, name, body)
		if got := kindOf(x.Value(m)); kinds[got] != m.Kind {
			t.Fatalf("member %d kind %d, value decodes as %s, body %q", i, m.Kind, got, body)
		}
	}
	for _, name := range append(lookedUp, names(ref)...) {
		want, n := last(ref, name)
		got, count := x.Find(name)
		if count != n || (n > 0 && (got.Key[0] != want.Start || got.Value != want.Value)) {
			t.Fatalf("Find(%q) = %+v x%d, reference %+v x%d, body %q", name, got, count, want, n, body)
		}
	}
	i := 0 // the walk past the ceiling reads what the index does
	x.Walk(func(m bodyindex.Member) {
		if m != x.Member(i) {
			t.Fatalf("Walk member %d = %+v, index %+v, body %q", i, m, x.Member(i), body)
		}
		i++
	})
	if i != x.Len() {
		t.Fatalf("Walk visited %d members, index %d, body %q", i, x.Len(), body)
	}
}

// names are the body's own member names as Find is asked for them: an
// escaped key's decoded name, an unescaped key's bytes as written (valid
// UTF-8 or not).
func names(ref []bodyindextest.Member) []string {
	var out []string
	for _, m := range ref {
		if m.Escaped {
			out = append(out, m.Key)
		} else {
			out = append(out, m.RawKey)
		}
	}
	return out
}

// last is the reference's last member Find(name) means, by the index's
// rule: an escaped key by its decoded name, an unescaped one by its bytes
// as written (encoding/json would decode invalid UTF-8 to U+FFFD).
func last(ref []bodyindextest.Member, name string) (m bodyindextest.Member, n int) {
	for _, r := range ref {
		k := r.RawKey
		if r.Escaped {
			k = r.Key
		}
		if k == name {
			m, n = r, n+1
		}
	}
	return m, n
}

func kindOf(raw []byte) string {
	var v any
	_ = json.Unmarshal(raw, &v)
	switch v.(type) {
	case string:
		return "string"
	case float64:
		return "number"
	case map[string]any:
		return "object"
	case []any:
		return "array"
	case bool:
		return "bool"
	}
	return "null"
}

// FuzzReadAgreesWithReference: the index reads every body as the
// encoding/json reference does: members in order, names, escapes, key and
// value ranges, commas, value kinds and the last of each name. A body past
// the ceiling is one the reference counts past it, and Walk reads it as
// the reference does.
func FuzzReadAgreesWithReference(f *testing.F) {
	bodyindextest.AddSeeds(f, "../../testdata/wire")
	f.Fuzz(func(t *testing.T, body []byte) {
		var x bodyindex.Index
		err := x.Read(body)
		if !errors.Is(err, bodyindex.ErrTooManyMembers) {
			sameAsReference(t, &x, body, err == nil)
			return
		}
		ref, _, ok := bodyindextest.Members(body)
		if !ok || len(ref) <= bodyindex.MaxMembers {
			t.Fatalf("past the ceiling, but the reference has %d members (ok %v), body %q", len(ref), ok, body)
		}
		n := 0
		x.Walk(func(m bodyindex.Member) {
			if n >= len(ref) || m.Key[0] != ref[n].Start || m.Value != ref[n].Value || m.Comma != ref[n].Comma {
				t.Fatalf("member %d = %+v, reference has %d members", n, m, len(ref))
			}
			n++
		})
		if n != len(ref) {
			t.Fatalf("walked %d members, reference %d", n, len(ref))
		}
	})
}

// FuzzSplicesKeepTheBodyValid: removing any member, inserting one first or
// replacing a value gives valid JSON whose bytes outside the change are
// the old ones, changes only that member, and comes with an index that
// agrees with the reference.
func FuzzSplicesKeepTheBodyValid(f *testing.F) {
	bodyindextest.AddSeeds(f, "../../testdata/wire")
	f.Fuzz(func(t *testing.T, body []byte) {
		var x, out bodyindex.Index
		if x.Read(body) != nil {
			return
		}
		list := func(ix *bodyindex.Index) []string {
			var l []string
			for i := range ix.Len() {
				m := ix.Member(i)
				l = append(l, string(ix.RawKey(m))+"="+string(ix.Value(m)))
			}
			return l
		}
		all := list(&x)
		check := func(what string, b []byte, want []string, keep [2][]byte) {
			t.Helper()
			if !json.Valid(b) {
				t.Fatalf("%s: invalid JSON %q from %q", what, b, body)
			}
			if !bytes.HasPrefix(b, keep[0]) || !bytes.HasSuffix(b, keep[1]) {
				t.Fatalf("%s: bytes outside the change moved: %q from %q", what, b, body)
			}
			sameAsReference(t, &out, b, true)
			if got := list(&out); strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("%s: members %q, want %q, body %q", what, got, want, body)
			}
		}
		// The same splice into the index being spliced, as the request path
		// makes it (out is x): the same body, and an index as fresh.
		inPlace := func(what string, splice func(self *bodyindex.Index) []byte, want []byte) {
			t.Helper()
			var self bodyindex.Index
			_ = self.Read(body)
			if b := splice(&self); !bytes.Equal(b, want) || !bytes.Equal(self.Body(), want) {
				t.Fatalf("%s in place: %q (index holds %q), want %q, body %q", what, b, self.Body(), want, body)
			}
			sameAsReference(t, &self, want, true)
		}
		for i := range x.Len() {
			m := x.Member(i)
			without := append(append([]string{}, all[:i]...), all[i+1:]...)
			// The separator before m goes with it, or the one after it when
			// m is the first.
			from, to := m.Key[0], m.Value[1]
			if m.Comma >= 0 {
				from = m.Comma
			} else if i+1 < x.Len() {
				to = x.Member(i+1).Comma + 1
			}
			removed := x.Remove(m, &out)
			if !bytes.Equal(removed, bodyindex.Remove(body, m)) { // one splice: the Index's is the bytes'
				t.Fatalf("Remove %d: the Index gives %q, the byte splice %q", i, removed, bodyindex.Remove(body, m))
			}
			check("remove", removed, without, [2][]byte{body[:from], body[to:]})
			inPlace("remove", func(self *bodyindex.Index) []byte { return self.Remove(self.Member(i), self) }, removed)
			replaced := append([]string{}, all...)
			replaced[i] = string(x.RawKey(m)) + "=0"
			replacedBody := x.ReplaceValue(m, []byte("0"), &out)
			check("replace", replacedBody, replaced, [2][]byte{body[:m.Value[0]], body[m.Value[1]:]})
			inPlace("replace", func(self *bodyindex.Index) []byte { return self.ReplaceValue(self.Member(i), []byte("0"), self) }, replacedBody)
		}
		inserted := x.InsertFirst([]byte(`"inserted":1`), &out)
		if b := bodyindex.InsertFirst(body, x.Open(), []byte(`"inserted":1`)); !bytes.Equal(inserted, b) {
			t.Fatalf("InsertFirst: the Index gives %q, the byte splice %q", inserted, b)
		}
		check("insert", inserted, append([]string{"inserted=1"}, all...),
			[2][]byte{body[:x.Open()+1], body[x.Open()+1:]})
		inPlace("insert", func(self *bodyindex.Index) []byte { return self.InsertFirst([]byte(`"inserted":1`), self) }, inserted)
		for what, same := range map[string][]byte{ // a member Find did not find
			"remove":  x.Remove(bodyindex.Member{}, &out),
			"replace": x.ReplaceValue(bodyindex.Member{}, []byte("0"), &out),
		} {
			if !bytes.Equal(same, body) || out.Len() != x.Len() {
				t.Fatalf("%s of the zero Member changed the body: %q", what, same)
			}
		}
	})
}

// TestFind: the last of a repeated name is the one read, as vLLM's parser
// does; an escaped spelling counts and can be the last; a name the body
// lacks gives the zero Member, which is safe to ask about.
func TestFind(t *testing.T) {
	var x bodyindex.Index
	if x.Read([]byte(`{"priority":1,"model":"m","pri\u006frity":2,"priority":3,"str\u0065am":true}`)) != nil {
		t.Fatal("not read")
	}
	if m, n := x.Find("priority"); n != 3 || string(x.Value(m)) != "3" {
		t.Fatalf("priority = %s x%d, want the last (3) of 3, the escaped spelling counted", x.Value(m), n)
	}
	if m, n := x.Find("stream"); n != 1 || string(x.Value(m)) != "true" || !m.Escaped || !x.Is(m, "stream") {
		t.Fatalf("stream = %s x%d (escaped %v), want the escaped spelling found", x.Value(m), n, m.Escaped)
	}
	if m, n := x.Find("model"); n != 1 || !x.Is(m, "model") || x.Is(m, "mode") || x.Is(m, "models") {
		t.Fatalf("model = %s x%d, or matched by prefix", x.Value(m), n)
	}
	m, n := x.Find("missing")
	if n != 0 || m.Kind != bodyindex.Invalid || x.Value(m) != nil || x.RawKey(m) != nil || x.Is(m, "") {
		t.Fatalf("missing = %+v x%d, want the zero Member, safe to ask about", m, n)
	}
}

// wide is a body of n members named m0, m1, ...
func wide(n int) []byte {
	var b strings.Builder
	b.WriteByte('{')
	for i := range n {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(`"m` + strconv.Itoa(i) + `":0`)
	}
	b.WriteByte('}')
	return []byte(b.String())
}

// TestMemberCeiling: a body with more than MaxMembers members is not
// indexed, so its memory has a ceiling, and Read says so apart from a body
// that is not an object; one with MaxMembers is indexed. Either way the
// Index keeps the body it was given (a reader still needs its size), and
// Err says why it has no members.
func TestMemberCeiling(t *testing.T) {
	var x bodyindex.Index
	if err := x.Read(wide(bodyindex.MaxMembers)); err != nil || x.Len() != bodyindex.MaxMembers || x.Err() != nil {
		t.Fatalf("a body of MaxMembers members not indexed (%v, %d, %v)", err, x.Len(), x.Err())
	}
	past := wide(bodyindex.MaxMembers + 1)
	if err := x.Read(past); !errors.Is(err, bodyindex.ErrTooManyMembers) || x.Err() != err || x.Len() != 0 ||
		!bytes.Equal(x.Body(), past) || x.Open() != 0 {
		t.Fatalf("a body past MaxMembers: %v (Err %v), %d members, body kept %v, open %d", err, x.Err(), x.Len(), bytes.Equal(x.Body(), past), x.Open())
	}
	if m, n := x.Find("m0"); n != 0 || m.Kind != bodyindex.Invalid {
		t.Fatalf("Find past the ceiling = %+v x%d, want nothing: its members are Walk's", m, n)
	}
	for _, b := range []string{`[]`, `{"a":1} {}`, `{"a":`, ``, ` `} {
		if err := x.Read([]byte(b)); !errors.Is(err, bodyindex.ErrNotObject) || x.Err() != err || string(x.Body()) != b {
			t.Fatalf("Read(%q) = %v (Err %v, body %q), want ErrNotObject and the body kept", b, err, x.Err(), x.Body())
		}
	}
	var never bodyindex.Index
	if err := never.Err(); !errors.Is(err, bodyindex.ErrNotObject) {
		t.Fatalf("an Index never read: Err %v, want ErrNotObject", err)
	}
	// The byte splices past the ceiling need the object's '{', wherever it
	// is: whitespace may come before it.
	spaced := append([]byte(" \n\t"), past...)
	if err := x.Read(spaced); !errors.Is(err, bodyindex.ErrTooManyMembers) || x.Open() != 3 {
		t.Fatalf("a body past MaxMembers after whitespace: %v, open %d, want 3", err, x.Open())
	}
}

// TestReadEveryWidth: around each size the table changes at (the inline
// members, the first table, the ceiling) the index reads a body as the
// reference does, keeps the members it read before its table grew, and
// its splices shift them all. An Index that read a narrower body first
// reads the same.
func TestReadEveryWidth(t *testing.T) {
	var reused bodyindex.Index
	for _, n := range []int{0, 1, 15, 16, 17, 31, 32, 33, 34, 48, 49, 100, bodyindex.MaxMembers - 1, bodyindex.MaxMembers} {
		body := wide(n)
		var x bodyindex.Index
		sameAsReference(t, &x, body, x.Read(body) == nil)
		sameAsReference(t, &reused, body, reused.Read(body) == nil)
		if n == 0 {
			continue
		}
		var out bodyindex.Index
		sameAsReference(t, &out, x.Remove(x.Member(0), &out), true)
		sameAsReference(t, &out, x.InsertFirst([]byte(`"inserted":1`), &out), true)
		if out.Len() != n+1 {
			t.Fatalf("%d members: %d after an insert", n, out.Len())
		}
	}
}

// TestReadAfterAFailedRead: an Index is reused, and a failed Read leaves
// nothing behind: neither why it failed nor what it held.
func TestReadAfterAFailedRead(t *testing.T) {
	for _, failed := range [][]byte{wide(bodyindex.MaxMembers + 1), []byte(`{"a":`), []byte(`[]`)} {
		var x, out bodyindex.Index
		if x.Read(failed) == nil {
			t.Fatalf("Read(%.20q) succeeded", failed)
		}
		x.InsertFirst([]byte(`"p":1`), &out) // refused: out holds what x does
		body := []byte(`{"a":1,"b":2}`)
		if err := x.Read(body); err != nil || x.Err() != nil || x.Len() != 2 {
			t.Fatalf("after %.20q: Read = %v, Err %v, %d members", failed, err, x.Err(), x.Len())
		}
		if err := out.Read(body); err != nil || out.Err() != nil || out.Len() != 2 {
			t.Fatalf("out, after %.20q: Read = %v, Err %v, %d members", failed, err, out.Err(), out.Len())
		}
		if b := x.Remove(x.Member(0), &out); string(b) != `{"b":2}` || out.Err() != nil || out.Len() != 1 {
			t.Fatalf("a splice after %.20q: %q, Err %v, %d members", failed, b, out.Err(), out.Len())
		}
	}
}

// TestWalk: Walk reads every member of a body past the ceiling as Read
// would have, in order; on an indexed body it visits the index's members;
// on a body that is not an object, or no body, it visits none.
func TestWalk(t *testing.T) {
	for _, body := range [][]byte{
		wide(bodyindex.MaxMembers + 5),
		[]byte(` { "` + bodyindextest.Escaped("priority") + `" : 3 , "model":"m",` + string(wide(bodyindex.MaxMembers)[1:])),
		[]byte(`{"a":1, "b" : [2], "c":{"d":"}"}}`),
	} {
		var x bodyindex.Index
		err := x.Read(body)
		ref, _, ok := bodyindextest.Members(body)
		if !ok || (err != nil) != (len(ref) > bodyindex.MaxMembers) {
			t.Fatalf("Read = %v for %d members (reference ok %v)", err, len(ref), ok)
		}
		var got []bodyindex.Member
		x.Walk(func(m bodyindex.Member) { got = append(got, m) })
		if len(got) != len(ref) {
			t.Fatalf("Walk visited %d members, reference %d", len(got), len(ref))
		}
		for i, r := range ref {
			m := got[i]
			if m.Key[0] != r.Start || m.Value != r.Value || m.Comma != r.Comma || m.Escaped != r.Escaped || string(x.RawKey(m)) != r.RawKey {
				t.Fatalf("member %d = %+v, reference %+v", i, m, r)
			}
			if err == nil && m != x.Member(i) {
				t.Fatalf("member %d = %+v, the index has %+v", i, m, x.Member(i))
			}
		}
	}
	for _, body := range []string{`[]`, `{"a":`, ``} {
		var x bodyindex.Index
		_ = x.Read([]byte(body))
		x.Walk(func(m bodyindex.Member) { t.Fatalf("Walk of %q visited %+v", body, m) })
	}
	var never bodyindex.Index
	never.Walk(func(m bodyindex.Member) { t.Fatalf("Walk of no body visited %+v", m) })
}

// TestByteSplicesPastTheCeiling: a body past MaxMembers is edited with the
// byte-level splices, member by member from Walk; the result is what the
// same splice gives on an indexed body, and the Index's own splices refuse
// it (it has no members to shift), returning it unchanged.
func TestByteSplicesPastTheCeiling(t *testing.T) {
	body := append([]byte(`{"priority":5,`), wide(bodyindex.MaxMembers)[1:]...)
	var x, out bodyindex.Index
	if err := x.Read(body); !errors.Is(err, bodyindex.ErrTooManyMembers) {
		t.Fatal(err)
	}
	var prio bodyindex.Member
	x.Walk(func(m bodyindex.Member) {
		if x.Is(m, "priority") {
			prio = m
		}
	})
	stripped := bodyindex.Remove(body, prio)
	if want := append([]byte(`{`), wide(bodyindex.MaxMembers)[1:]...); !bytes.Equal(stripped, want) {
		t.Fatalf("Remove: %.40q..., want %.40q...", stripped, want)
	}
	inserted := bodyindex.InsertFirst(stripped, x.Open(), []byte(`"priority":-1`))
	if want := append([]byte(`{"priority":-1,`), wide(bodyindex.MaxMembers)[1:]...); !bytes.Equal(inserted, want) {
		t.Fatalf("InsertFirst: %.40q..., want %.40q...", inserted, want)
	}
	for what, b := range map[string][]byte{
		"remove":  x.Remove(prio, &out),
		"insert":  x.InsertFirst([]byte(`"p":1`), &out),
		"replace": x.ReplaceValue(prio, []byte("1"), &out),
	} {
		if !bytes.Equal(b, body) || !errors.Is(out.Err(), bodyindex.ErrTooManyMembers) || out.Open() != x.Open() || out.Len() != 0 {
			t.Fatalf("%s of an Index past the ceiling: changed %v, out Err %v open %d, %d members", what, !bytes.Equal(b, body), out.Err(), out.Open(), out.Len())
		}
	}
	if b := bodyindex.Remove(body, bodyindex.Member{}); !bytes.Equal(b, body) {
		t.Fatal("Remove of the zero Member changed the body")
	}
	if b := bodyindex.InsertFirst(body, 0, nil); !bytes.Equal(b, body) {
		t.Fatal("InsertFirst of an empty member changed the body")
	}
	if b := bodyindex.InsertFirst([]byte(` { } `), 1, []byte(`"p":1`)); string(b) != ` {"p":1 } ` {
		t.Fatalf("InsertFirst into an empty object: %q", b)
	}
}

// TestIsZeroNumber pins the gateway's one zero rule: a number that vLLM's
// JSON parser reads as zero is zero, a non-zero literal that underflows
// ("1e-400") included; nothing that is not a number is.
func TestIsZeroNumber(t *testing.T) {
	for raw, want := range map[string]bool{"0": true, "-0": true, "0.0": true, "0e5": true, "0E-2": true, "-0.0e5": true,
		"1e-400": true, "-1e-400": true,
		"1": false, "-3": false, "0.5": false, "10": false, "1e0": false, "5e-324": false, "1e400": false,
		`"0"`: false, "null": false, "false": false, "[]": false, "": false} {
		if got := bodyindex.IsZeroNumber([]byte(raw)); got != want {
			t.Errorf("IsZeroNumber(%q) = %v, want %v", raw, got, want)
		}
	}
}

// TestRefusals: what a caller cannot ask for comes back as nothing, never
// a panic or a stale member: a member past the count (on a reused Index
// too), a splice of an Index that indexed no members, an empty value or
// member.
func TestRefusals(t *testing.T) {
	var x bodyindex.Index
	_ = x.Read([]byte(`{"a":1,"b":2,"c":3}`))
	_ = x.Read([]byte(`{"a":1}`))
	for _, i := range []int{-1, 1, 2, 15, 16} {
		if m := x.Member(i); m.Kind != bodyindex.Invalid {
			t.Fatalf("Member(%d) of one = %+v, want the zero Member", i, m)
		}
	}
	var out bodyindex.Index
	before := string(x.Body())
	if b := x.ReplaceValue(x.Member(0), []byte(" "), &out); string(b) != before || out.Len() != 1 {
		t.Fatalf("an empty value replaced: %q, %d members", b, out.Len())
	}
	if b := x.InsertFirst(nil, &out); string(b) != before || out.Len() != 1 {
		t.Fatalf("an empty member inserted: %q, %d members", b, out.Len())
	}
	for _, body := range []string{`[]`, `{"a":`} {
		_ = x.Read([]byte(body))
		if b := x.InsertFirst([]byte(`"p":1`), &out); string(b) != body || out.Len() != 0 || !errors.Is(out.Err(), bodyindex.ErrNotObject) {
			t.Fatalf("inserted into %q: %q, %d members, Err %v", body, b, out.Len(), out.Err())
		}
	}
	var never bodyindex.Index
	if b := never.InsertFirst([]byte(`"p":1`), &out); b != nil || out.Body() != nil || !errors.Is(out.Err(), bodyindex.ErrNotObject) {
		t.Fatalf("inserted into an Index never read: %q, Err %v", b, out.Err())
	}
}

// TestReplaceValueDropsSurroundingSpace: the replaced member's range and
// kind are what a fresh read of the new body gives.
func TestReplaceValueDropsSurroundingSpace(t *testing.T) {
	var x, out, fresh bodyindex.Index
	_ = x.Read([]byte(`{"a":1,"b":2}`))
	b := x.ReplaceValue(x.Member(0), []byte(" \t[3] \n"), &out)
	if string(b) != `{"a":[3],"b":2}` {
		t.Fatalf("body %q", b)
	}
	if err := fresh.Read(b); err != nil || out.Member(0) != fresh.Member(0) || out.Member(1) != fresh.Member(1) {
		t.Fatalf("index %+v %+v, a fresh read %+v %+v", out.Member(0), out.Member(1), fresh.Member(0), fresh.Member(1))
	}
}

const requestBody = `{"model":"m","messages":[{"role":"user","content":"hi"}],"stream":true,"max_tokens":8,"priority":3}`

//go:noinline
func readAndFind(body []byte) int {
	var x bodyindex.Index // as a request handler declares it
	x.Read(body)
	_, n := x.Find("priority")
	m, _ := x.Find("model")
	return n + len(x.Value(m))
}

//go:noinline
func readAndStrip(body []byte) int {
	var x, out bodyindex.Index
	x.Read(body)
	m, _ := x.Find("priority")
	return len(x.Remove(m, &out))
}

func BenchmarkRead(b *testing.B) {
	for _, size := range []int{1 << 10, 1 << 20} {
		body := []byte(`{"messages":[{"role":"user","content":"` + strings.Repeat("x", size) + `"}],"model":"large","stream":true}`)
		b.Run(strconv.Itoa(size>>10)+"KiB", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(body)))
			for b.Loop() {
				readAndFind(body)
			}
		})
	}
}

func BenchmarkRemove(b *testing.B) {
	body := []byte(`{"priority":3,"messages":[{"role":"user","content":"` + strings.Repeat("x", 1<<20) + `"}],"model":"large"}`)
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	for b.Loop() {
		readAndStrip(body)
	}
}
