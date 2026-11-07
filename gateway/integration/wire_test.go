//go:build integration

package integration

import (
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/mockllm"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/wiretest"
)

// AC-M0-1 and AC-M0-2 on the dev stack: the M0-07 goldens, sent byte for byte
// straight to mockllm and through nginx and the gateways, with the same
// assertions as the in-process test in internal/wiretest.
//
// Knobs (environment):
//
//	INTEGRATION_WIRE_GOLDENS  golden directory                 ../testdata/wire
//	INTEGRATION_WIRE_MODEL    model the large pool serves      large-model
//	INTEGRATION_UPSTREAM_AUTH Authorization values the replica
//	                          gets through the gateway (1 when
//	                          GATEWAY_UPSTREAM_API_KEY is set)  0
const (
	envWireGoldens      = "INTEGRATION_WIRE_GOLDENS"
	envWireModel        = "INTEGRATION_WIRE_MODEL"
	envUpstreamAuth     = "INTEGRATION_UPSTREAM_AUTH"
	defaultWireGoldens  = "../testdata/wire"
	defaultWireModel    = "large-model"
	defaultUpstreamAuth = 0

	// wireOutputTokens fixes mockllm's output length so both paths get the
	// same text; it travels as a control header on both paths alike.
	wireOutputTokens = "5"
	// wireCallTimeout bounds one golden call (a few tokens at the dev
	// stack's 50 ms TTFT and 50 tok/s).
	wireCallTimeout = 30 * time.Second
)

// What nginx (deploy/compose/nginx/nginx.conf) adds on the gateway path.
var (
	// proxy_set_header X-Real-IP, X-Forwarded-For, X-Forwarded-Proto.
	nginxRequestHeaders = []string{"X-Real-Ip", "X-Forwarded-For", "X-Forwarded-Proto"}
	// Server (server_tokens off still names nginx) and keep-alive.
	nginxResponseHeaders = []string{"Server", "Connection"}
)

// TestWireGoldensThroughStack runs every golden directly against the large
// pool's mockllm and through the stack's entry point, and compares what the
// replica received and what the caller got back.
func TestWireGoldensThroughStack(t *testing.T) {
	s := newStack(t)
	goldens, err := wiretest.Load(envOr(envWireGoldens, defaultWireGoldens))
	if err != nil {
		t.Fatalf("load goldens (set %s when running outside gateway/integration): %v", envWireGoldens, err)
	}
	direct := s.mockURLs[0] // the large pool's replica: model and default pool
	tr := &http.Transport{DisableCompression: true}
	t.Cleanup(tr.CloseIdleConnections)
	sender := wiretest.Sender{
		Client:   &http.Client{Transport: tr, Timeout: wireCallTimeout},
		Recorder: wiretest.HTTPRecorder{Client: s.control, BaseURL: direct},
		Model:    envOr(envWireModel, defaultWireModel),
		Header:   http.Header{mockllm.HeaderOutputTokens: {wireOutputTokens}},
	}
	expect := wiretest.Expect{
		UpstreamAuth:         upstreamAuth(t),
		ProxyRequestHeaders:  nginxRequestHeaders,
		ProxyResponseHeaders: nginxResponseHeaders,
		// nginx honours the gateway's X-Accel-Buffering: no and strips it.
		ProxyConsumesXAccel: true,
	}
	for _, g := range goldens {
		g = sender.Prepare(g)
		t.Run(g.Name, func(t *testing.T) {
			viaDirect := sender.Send(t, direct, g)
			viaGW := sender.Send(t, s.gatewayURL, g)
			wiretest.Compare(t, g, viaDirect, viaGW, expect)
		})
	}
}

// upstreamAuth reads INTEGRATION_UPSTREAM_AUTH: 0 or 1.
func upstreamAuth(t *testing.T) int {
	t.Helper()
	v := strings.TrimSpace(envOr(envUpstreamAuth, strconv.Itoa(defaultUpstreamAuth)))
	n, err := strconv.Atoi(v)
	if err != nil || (n != 0 && n != 1) {
		t.Fatalf("%s=%q must be 0 or 1", envUpstreamAuth, v)
	}
	return n
}
