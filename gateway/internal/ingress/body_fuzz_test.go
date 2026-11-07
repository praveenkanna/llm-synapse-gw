package ingress

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/bodyindex/bodyindextest"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
)

// FuzzTopLevelFieldsAgreesWithReference: the request path's top-level
// reader, on the request's index (AD-15), finds the same model, stream,
// priority (the last value and the count) and caps as the encoding/json
// reference, on every body (AD-14), and leaves the index on that body.
func FuzzTopLevelFieldsAgreesWithReference(f *testing.F) {
	bodyindextest.AddSeeds(f, "../../testdata/wire")
	f.Fuzz(func(t *testing.T, body []byte) {
		var ix bodyindex.Index
		info, ok := readBody(&ix, body)
		agreesWithReference(t, body, info, ok)
		if !bytes.Equal(ix.Body(), body) {
			t.Fatalf("the index holds %q, not the body %q", ix.Body(), body)
		}
	})
}

// agreesWithReference fails t unless info and ok are what the reference
// reads from body.
func agreesWithReference(t *testing.T, body []byte, info bodyInfo, ok bool) {
	t.Helper()
	members, _, refOK := bodyindextest.Members(body)
	if !refOK {
		if ok {
			t.Fatalf("read a body that is not an object: %+v, body %q", info, body)
		}
		return
	}
	raw := func(name string) []byte {
		m, n := bodyindextest.Last(members, name)
		if n == 0 {
			return nil
		}
		return body[m.Value[0]:m.Value[1]]
	}
	wantStream := string(raw("stream")) == "true"
	var model string
	if m := raw("model"); m != nil && json.Unmarshal(m, &model) != nil {
		if ok || info.stream != wantStream {
			t.Fatalf("a model that is not a string: ok %v, %+v, body %q", ok, info, body)
		}
		return
	}
	tooLong := len(model) > config.MaxModelNameBytes
	if tooLong {
		model = ""
	}
	_, prios := bodyindextest.Last(members, "priority")
	_, maxTok := bodyindextest.Last(members, "max_tokens")
	_, maxComp := bodyindextest.Last(members, "max_completion_tokens")
	if !ok || info.model != model || info.modelTooLong != tooLong || info.stream != wantStream || info.priorities != prios ||
		!bytes.Equal(info.priority, raw("priority")) || info.hasCap != (maxTok+maxComp > 0) {
		t.Fatalf("read %+v (ok %v); reference model %q stream %v priority %q x%d caps %d, body %q",
			info, ok, model, wantStream, raw("priority"), prios, maxTok+maxComp, body)
	}
}
