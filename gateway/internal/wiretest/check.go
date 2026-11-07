package wiretest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/ingress"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/mockllm"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

// The comparisons below are shared by the in-process tests beside this file
// and the dev-stack run in gateway/integration, so both prove the same thing.

// Headers that legitimately differ between the direct and gateway paths.
const (
	hdrAcceptEncoding = "Accept-Encoding"
	hdrAuthorization  = "Authorization"
	hdrXAccel         = "X-Accel-Buffering"
	hdrDate           = "Date"
)

// TraceparentRE matches a version-00 W3C traceparent; groups are the trace
// ID and the parent (span) ID.
var TraceparentRE = regexp.MustCompile(`^00-([0-9a-f]{32})-([0-9a-f]{16})-[0-9a-f]{2}$`)

// Recorder is a replica's request capture: mockllm in process, or its
// /mock/requests endpoint on a running stack.
type Recorder interface {
	ResetRequests() error
	Requests() ([]mockllm.RecordedRequest, error)
}

// Exchange is one call as the caller saw it and as mockllm received it.
type Exchange struct {
	Resp     *http.Response
	Body     []byte
	Upstream *mockllm.RecordedRequest // nil for routes mockllm does not capture
}

// Sender makes golden calls with a client that never adds Accept-Encoding
// or decompresses, so headers on the wire are exactly the golden's.
type Sender struct {
	Client *http.Client
	// Recorder is the capture of the replica both paths reach.
	Recorder Recorder
	// Model, when set, replaces the golden's served model ("mock-model") in
	// the body, for a stack that serves another name. The rewritten bytes
	// are then what both paths send and what the replica must receive.
	Model string
	// Header is added to every call (e.g. mockllm control headers).
	Header http.Header
}

// GoldenModel is the served model name the golden bodies use.
const GoldenModel = "mock-model"

// Prepare returns g as this sender sends it: with the model renamed if the
// sender asks for it. Everything else in the body stays byte for byte.
func (s Sender) Prepare(g Golden) Golden {
	if s.Model == "" || s.Model == GoldenModel || g.Body == nil {
		return g
	}
	g.Body = bytes.ReplaceAll(g.Body, []byte(`"`+GoldenModel+`"`), []byte(`"`+s.Model+`"`))
	return g
}

// Send makes g's call against baseURL and collects what the replica
// received. g must already be prepared (see Prepare).
func (s Sender) Send(t testing.TB, baseURL string, g Golden) Exchange {
	t.Helper()
	if err := s.Recorder.ResetRequests(); err != nil {
		t.Fatalf("reset mockllm capture: %v", err)
	}
	var body io.Reader = http.NoBody
	if g.Body != nil {
		body = bytes.NewReader(g.Body)
	}
	req, err := http.NewRequest(g.Method, baseURL+g.Path, body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header = g.HTTPHeader()
	for k, v := range s.Header {
		req.Header[k] = slices.Clone(v)
	}
	resp, err := s.Client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", g.Method, baseURL+g.Path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	ex := Exchange{Resp: resp, Body: got}
	recs, err := s.Recorder.Requests()
	if err != nil {
		t.Fatalf("read mockllm capture: %v", err)
	}
	recs = slices.DeleteFunc(recs, func(r mockllm.RecordedRequest) bool {
		return r.Header.Get("User-Agent") == upstream.ProbeUserAgent // the gateway's own probe, not the call
	})
	switch {
	case g.Method == http.MethodGet:
		// mockllm captures generation requests only.
	case len(recs) != 1:
		t.Fatalf("mockllm captured %d requests, want 1", len(recs))
	default:
		ex.Upstream = &recs[0]
	}
	return ex
}

// Expect names what the path through the gateway may add over a direct call.
type Expect struct {
	// UpstreamAuth is how many Authorization values the replica must get
	// through the gateway: 1 when the gateway has an upstream key, 0 when it
	// has none. The caller's own credential never goes upstream.
	UpstreamAuth int
	// ProxyRequestHeaders are request headers a proxy in front of the
	// gateway adds (nginx: X-Real-Ip, X-Forwarded-For, X-Forwarded-Proto).
	// They must reach the replica through the gateway; values are not
	// compared.
	ProxyRequestHeaders []string
	// ProxyResponseHeaders are response headers such a proxy adds (nginx:
	// Server, Connection). They must be on the gateway path's response.
	ProxyResponseHeaders []string
	// ProxyConsumesXAccel is set when a proxy in front acts on the gateway's
	// X-Accel-Buffering: no and does not pass it on (nginx does this), so
	// the caller must not see it.
	ProxyConsumesXAccel bool
}

// Compare asserts AC-M0-1 and AC-M0-2 for one golden sent directly and
// through the gateway.
func Compare(t testing.TB, g Golden, direct, viaGW Exchange, e Expect) {
	t.Helper()
	if g.Method == http.MethodPost {
		AssertBodyForwarded(t, g, direct, viaGW)
		AssertUpstreamHeaders(t, g, direct.Upstream.Header, viaGW.Upstream.Header, e)
	}
	AssertResponse(t, g, direct, viaGW, e)
}

// AssertBodyForwarded checks AC-M0-1: the replica receives the caller's
// bytes, exactly.
func AssertBodyForwarded(t testing.TB, g Golden, direct, viaGW Exchange) {
	t.Helper()
	for path, ex := range map[string]Exchange{"direct": direct, "gateway": viaGW} {
		rec := ex.Upstream
		if rec.Truncated || rec.BodyBytes != len(g.Body) {
			t.Fatalf("%s: mockllm captured %d of %d bytes (truncated=%v), golden has %d",
				path, len(rec.Body), rec.BodyBytes, rec.Truncated, len(g.Body))
		}
		if !bytes.Equal(rec.Body, g.Body) {
			t.Errorf("%s: upstream body differs from golden\n got: %q\nwant: %q", path, rec.Body, g.Body)
		}
		if rec.Method != g.Method || rec.Path != g.Path || rec.Query != "" {
			t.Errorf("%s: upstream request line %s %s?%s, want %s %s", path, rec.Method, rec.Path, rec.Query, g.Method, g.Path)
		}
	}
}

// AssertUpstreamHeaders checks that upstream headers match a direct call
// except for the intended differences: Accept-Encoding: identity on streams,
// the gateway's Traceparent, its own credential, and what a proxy in front
// added.
func AssertUpstreamHeaders(t testing.TB, g Golden, direct, viaGW http.Header, e Expect) {
	t.Helper()
	want := direct.Clone()
	got := viaGW.Clone()

	callerTP := want.Get(telemetry.HeaderTraceparent)
	gwTP := got.Get(telemetry.HeaderTraceparent)
	AssertTraceparent(t, callerTP, gwTP)
	want.Del(telemetry.HeaderTraceparent)
	got.Del(telemetry.HeaderTraceparent)

	if g.Stream {
		if v := got.Values(hdrAcceptEncoding); len(v) != 1 || v[0] != "identity" {
			t.Errorf("streaming request: upstream Accept-Encoding = %q, want [identity]", v)
		}
		want.Set(hdrAcceptEncoding, "identity")
	}
	if n := len(got.Values(hdrAuthorization)); n != e.UpstreamAuth {
		t.Errorf("upstream Authorization values = %d, want %d (the gateway's own key, never the caller's)", n, e.UpstreamAuth)
	}
	want.Del(hdrAuthorization)
	got.Del(hdrAuthorization)
	dropAdded(t, "upstream request", want, got, e.ProxyRequestHeaders)
	assertHeadersEqual(t, "upstream request", want, got)
}

// AssertTraceparent checks that the gateway sent a valid traceparent and,
// when the caller sent one, continued its trace under a new gateway span.
func AssertTraceparent(t testing.TB, caller, gateway string) {
	t.Helper()
	m := TraceparentRE.FindStringSubmatch(gateway)
	if m == nil {
		t.Fatalf("gateway sent traceparent %q, want a valid W3C value", gateway)
	}
	if caller == "" {
		return
	}
	cm := TraceparentRE.FindStringSubmatch(caller)
	if cm == nil {
		t.Fatalf("golden traceparent %q is invalid", caller)
	}
	if m[1] != cm[1] {
		t.Errorf("trace ID %s not continued upstream (got %s)", cm[1], m[1])
	}
	if m[2] == cm[2] {
		t.Errorf("gateway reused the caller's span ID %s instead of its own", cm[2])
	}
}

// AssertResponse checks AC-M0-2: status, headers, framing and (normalised)
// body match the direct call; streams gain only X-Accel-Buffering: no (unless
// a proxy in front consumes it), plus whatever that proxy adds.
func AssertResponse(t testing.TB, g Golden, direct, viaGW Exchange, e Expect) {
	t.Helper()
	if viaGW.Resp.StatusCode != direct.Resp.StatusCode {
		t.Fatalf("status = %d, direct = %d; body %s", viaGW.Resp.StatusCode, direct.Resp.StatusCode, viaGW.Body)
	}
	if g.Name != "chat_unknown_model" && direct.Resp.StatusCode != http.StatusOK {
		t.Fatalf("direct call failed with %d: %s", direct.Resp.StatusCode, direct.Body)
	}

	want := direct.Resp.Header.Clone()
	got := viaGW.Resp.Header.Clone()
	want.Del(hdrDate)
	got.Del(hdrDate)
	if g.Stream && direct.Resp.StatusCode == http.StatusOK && !e.ProxyConsumesXAccel {
		if v := got.Get(hdrXAccel); v != "no" {
			t.Errorf("streamed response X-Accel-Buffering = %q, want no", v)
		}
		want.Set(hdrXAccel, "no")
	}
	dropAdded(t, "response", want, got, e.ProxyResponseHeaders)
	assertHeadersEqual(t, "response", want, got)

	if !slices.Equal(viaGW.Resp.TransferEncoding, direct.Resp.TransferEncoding) ||
		viaGW.Resp.ContentLength != direct.Resp.ContentLength {
		t.Errorf("framing: transfer-encoding %v length %d, direct %v length %d",
			viaGW.Resp.TransferEncoding, viaGW.Resp.ContentLength, direct.Resp.TransferEncoding, direct.Resp.ContentLength)
	}

	if !bytes.Equal(Normalize(viaGW.Body), Normalize(direct.Body)) {
		t.Errorf("normalised body differs from direct\n got: %s\nwant: %s", viaGW.Body, direct.Body)
	}
	if direct.Resp.StatusCode == http.StatusOK {
		assertSchema(t, g, viaGW)
	}
}

// dropAdded asserts each header a proxy adds is on the gateway path and not
// on the direct one, then removes it from both for the exact comparison.
func dropAdded(t testing.TB, what string, want, got http.Header, added []string) {
	t.Helper()
	for _, k := range added {
		if len(got.Values(k)) == 0 {
			t.Errorf("%s: header %s the proxy adds is missing on the gateway path", what, k)
		}
		if v := want.Values(k); len(v) != 0 {
			t.Errorf("%s: header %s = %q on the direct path too; it is not a proxy addition", what, k, v)
		}
		want.Del(k)
		got.Del(k)
	}
}

// assertSchema checks the response against the OpenAI shape the case asked
// for: SSE framing for streams, one JSON document otherwise, with the right
// object type and number of choices.
func assertSchema(t testing.TB, g Golden, ex Exchange) {
	t.Helper()
	ct := ex.Resp.Header.Get("Content-Type")
	if g.Stream {
		if !strings.HasPrefix(ct, "text/event-stream") {
			t.Fatalf("streamed Content-Type = %q", ct)
		}
		events, err := SSEEvents(ex.Body)
		if err != nil {
			t.Fatalf("SSE framing: %v\n%s", err, ex.Body)
		}
		for i, ev := range events {
			assertObject(t, fmt.Sprintf("event %d", i), ev, streamObject(g.Path))
		}
		return
	}
	if !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("Content-Type = %q", ct)
	}
	if g.Path == ingress.PathModels {
		assertObject(t, "models", ex.Body, "list")
		return
	}
	var doc struct {
		Choices []json.RawMessage `json:"choices"`
		Usage   *json.RawMessage  `json:"usage"`
	}
	assertObject(t, "response", ex.Body, responseObject(g.Path))
	if err := json.Unmarshal(ex.Body, &doc); err != nil {
		t.Fatal(err)
	}
	if n := wantChoices(t, g); len(doc.Choices) != n || doc.Usage == nil {
		t.Errorf("response has %d choices (usage present: %v), want %d", len(doc.Choices), doc.Usage != nil, n)
	}
}

func assertObject(t testing.TB, what string, doc []byte, want string) {
	t.Helper()
	var head struct {
		Object string `json:"object"`
	}
	if err := json.Unmarshal(doc, &head); err != nil {
		t.Fatalf("%s is not JSON: %v", what, err)
	}
	if head.Object != want {
		t.Errorf("%s object = %q, want %q", what, head.Object, want)
	}
}

func responseObject(path string) string {
	if path == ingress.PathCompletions {
		return "text_completion"
	}
	return "chat.completion"
}

func streamObject(path string) string {
	if path == ingress.PathCompletions {
		return "text_completion"
	}
	return "chat.completion.chunk"
}

// wantChoices is the golden's "n" (default 1).
func wantChoices(t testing.TB, g Golden) int {
	t.Helper()
	var b struct {
		N *int `json:"n"`
	}
	if err := json.Unmarshal(g.Body, &b); err != nil {
		t.Fatal(err)
	}
	if b.N == nil {
		return 1
	}
	return *b.N
}

func assertHeadersEqual(t testing.TB, what string, want, got http.Header) {
	t.Helper()
	keys := slices.Sorted(maps.Keys(want))
	for k := range got {
		if _, ok := want[k]; !ok {
			keys = append(keys, k)
		}
	}
	for _, k := range keys {
		if !slices.Equal(want[k], got[k]) {
			t.Errorf("%s header %s = %q, want %q", what, k, got[k], want[k])
		}
	}
}
