package wiretest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/config"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/discovery"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/dispatch"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/ingress"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/mockllm"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry/telemetrytest"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/upstream"
)

const (
	goldenDir = "../../testdata/wire"

	servedModel = GoldenModel
	// outputTokens fixes mockllm's output length so both paths get the same
	// text. It travels as a mockllm control header, on both paths alike.
	outputTokens = "5"
	// upstreamKey is the gateway's own replica credential. mockllm redacts
	// Authorization values, so tests see only that one was sent.
	upstreamKey = "sk-gateway-upstream-golden"
)

// inProcess is what differs on the in-process path: the gateway has an
// upstream key and nothing sits in front of it.
var inProcess = Expect{UpstreamAuth: 1}

// rig is a mockllm replica and a gateway in front of it, wired as
// cmd/gateway wires them.
type rig struct {
	mock    *mockllm.Server
	gateway *httptest.Server
	tel     *telemetry.Telemetry // the gateway's request telemetry
	sender  Sender
}

// counted is what the gateway's telemetry counted of metric.
func (r *rig) counted(t testing.TB, metric string, labels ...string) float64 {
	t.Helper()
	return telemetrytest.Sum(t, r.tel.Metrics(), metric, labels...)
}

func newRig(t *testing.T) *rig {
	t.Helper()
	mcfg := mockllm.Defaults()
	mcfg.ListenAddr = "127.0.0.1:0"
	mcfg.Models = []string{servedModel}
	mcfg.TTFT = 0
	mcfg.TokensPerSecond = 0
	mcfg.Seed = 7
	mock, err := mockllm.Start(mcfg, nil)
	if err != nil {
		t.Fatalf("mockllm.Start: %v", err)
	}
	t.Cleanup(func() { _ = mock.Close() })

	cfg, err := config.Parse([]byte(fmt.Sprintf(`
upstream:
  api_key: %s
pools:
  - name: large
    replicas:
      - {name: large-0, url: %s}
models:
  %s: large
default_pool: large
auth: {enabled: false}
`, upstreamKey, mock.URL(), servedModel)), func(string) string { return "" })
	if err != nil {
		t.Fatalf("config.Parse: %v", err)
	}
	disc, err := discovery.NewStatic(cfg.Pools)
	if err != nil {
		t.Fatal(err)
	}
	up := upstream.New(cfg.Upstream)
	t.Cleanup(up.CloseIdleConnections)
	tel := telemetry.New(telemetry.Options{})
	h := ingress.New(cfg, disc, up, dispatch.New(router.NewRoundRobin(disc, cfg.PoolNames()), up, cfg.Upstream, dispatch.Options{}), ingress.Options{})
	mux := http.NewServeMux()
	h.Register(mux)
	gw := httptest.NewServer(tel.Middleware(mux))
	t.Cleanup(gw.Close)

	// The caller's client never adds Accept-Encoding or decompresses: headers
	// on the wire are exactly the golden's.
	tr := &http.Transport{DisableCompression: true}
	t.Cleanup(tr.CloseIdleConnections)
	return &rig{mock: mock, gateway: gw, tel: tel, sender: Sender{
		Client:   &http.Client{Transport: tr},
		Recorder: MockRecorder{Mock: mock.Mock()},
		Header:   http.Header{mockllm.HeaderOutputTokens: {outputTokens}},
	}}
}

// TestWireGoldens sends every golden directly and through the gateway and
// compares what the replica received and what the caller got back.
func TestWireGoldens(t *testing.T) {
	goldens, err := Load(goldenDir)
	if err != nil {
		t.Fatal(err)
	}
	r := newRig(t)
	for _, g := range goldens {
		t.Run(g.Name, func(t *testing.T) {
			direct := r.sender.Send(t, r.mock.URL(), g)
			before := r.counted(t, telemetry.MetricUpstreamErrors)
			viaGW := r.sender.Send(t, r.gateway.URL, g)
			after := r.counted(t, telemetry.MetricUpstreamErrors)

			Compare(t, g, direct, viaGW, inProcess)
			if d := after - before; d != 0 {
				t.Errorf("gateway counted %v upstream errors: transparent forwarding never fails upstream, before or during the response", d)
			}
		})
	}
}

// The unknown-model golden fails open: it reaches the default pool unchanged
// and the replica's own 404 comes back.
func TestUnknownModelFailsOpenToDefaultPool(t *testing.T) {
	goldens, err := Load(goldenDir)
	if err != nil {
		t.Fatal(err)
	}
	i := slices.IndexFunc(goldens, func(g Golden) bool { return g.Name == "chat_unknown_model" })
	if i < 0 {
		t.Fatal("golden chat_unknown_model missing")
	}
	r := newRig(t)
	ex := r.sender.Send(t, r.gateway.URL, goldens[i])
	if ex.Resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want the replica's 404", ex.Resp.StatusCode)
	}
	if got := r.counted(t, telemetry.MetricFailOpen, `stage="`+telemetry.StagePoolResolve+`"`, `reason="`+ingress.ReasonUnknownModel+`"`); got != 1 {
		t.Errorf("unknown-model fail-opens = %v, want 1", got)
	}
	if !bytes.Equal(ex.Upstream.Body, goldens[i].Body) {
		t.Error("unknown-model body was not forwarded unchanged")
	}
}

// The golden set covers every request shape the done criteria name, and each
// case's stream flag agrees with its body.
func TestGoldenCoverage(t *testing.T) {
	goldens, err := Load(goldenDir)
	if err != nil {
		t.Fatal(err)
	}
	covered := map[string]bool{}
	for _, g := range goldens {
		if g.Body == nil {
			continue
		}
		var b map[string]json.RawMessage
		if err := json.Unmarshal(g.Body, &b); err != nil {
			t.Fatalf("%s: body is not a JSON object: %v", g.Name, err)
		}
		stream := string(b["stream"]) == "true"
		if stream != g.Stream {
			t.Errorf("%s: manifest stream=%v but body stream=%s", g.Name, g.Stream, b["stream"])
		}
		mark := func(feature string, ok bool) {
			if ok {
				covered[feature] = true
			}
		}
		mark("stream", stream)
		mark("non-stream", !stream)
		mark("tools", b["tools"] != nil)
		mark("tool_choice", b["tool_choice"] != nil)
		mark("response_format json_schema", bytes.Contains(b["response_format"], []byte(`"json_schema"`)))
		mark("guided decoding", b["guided_json"] != nil && b["guided_regex"] != nil && b["guided_choice"] != nil && b["guided_grammar"] != nil)
		mark("logprobs", b["logprobs"] != nil)
		if n, err := strconv.Atoi(string(b["n"])); err == nil && n > 1 {
			covered["n>1"] = true
		}
		mark("stream without Accept-Encoding", stream && g.HTTPHeader().Get(hdrAcceptEncoding) == "")
		mark("stream with Accept-Encoding", stream && g.HTTPHeader().Get(hdrAcceptEncoding) != "")
		mark("caller traceparent", g.HTTPHeader().Get(telemetry.HeaderTraceparent) != "")
		mark("completions endpoint", g.Path == ingress.PathCompletions)
		mark("CRLF and escapes", bytes.Contains(g.Body, []byte("\r\n")) && bytes.Contains(g.Body, []byte(`\u`)))
	}
	for _, f := range []string{
		"stream", "non-stream", "tools", "tool_choice", "response_format json_schema", "guided decoding",
		"logprobs", "n>1", "stream without Accept-Encoding", "stream with Accept-Encoding",
		"caller traceparent", "completions endpoint", "CRLF and escapes",
	} {
		if !covered[f] {
			t.Errorf("no golden covers %s", f)
		}
	}
}
