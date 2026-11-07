//go:build integration

package integration

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/mockllm"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/wiretest"
)

// Trace-ID continuity on the dev stack: the trace ID a caller (the harness)
// sends in traceparent is the one the replica receives and the one on the
// gateway's per-request log line, so harness results.jsonl trace_id joins
// with gateway logs.
//
// The gateway logs go to its containers' stdout; the test reads them with
// the host's docker CLI (`docker logs`), so it runs from the host, not from
// run-in-network.sh (where it skips). Knobs (environment):
//
//	INTEGRATION_COMPOSE_PROJECT   compose project name           llm-synapse-gw
//	INTEGRATION_GATEWAY_SERVICES  gateway services, comma-sep    gateway-1,gateway-2
const (
	envComposeProject      = "INTEGRATION_COMPOSE_PROJECT"
	envGatewayServices     = "INTEGRATION_GATEWAY_SERVICES"
	defaultComposeProject  = "llm-synapse-gw"
	defaultGatewayServices = "gateway-1,gateway-2"

	// logWait bounds the per-request line reaching `docker logs`.
	logWait = 10 * time.Second
	// logPollInterval is how often the logs are read while waiting.
	logPollInterval = 200 * time.Millisecond
	// logSinceSlack starts the log read before the request, covering clock
	// skew between the host and the Docker VM.
	logSinceSlack = 30 * time.Second
	// dockerCmdTimeout bounds one docker CLI call.
	dockerCmdTimeout = 10 * time.Second

	traceChatBody = `{"model":"large-model","stream":true,"max_tokens":8,"messages":[{"role":"user","content":"trace join"}]}`
)

// requestLine is the part of the gateway's per-request log line the join
// relies on (internal/telemetry middleware).
type requestLine struct {
	Msg          string `json:"msg"`
	TraceID      string `json:"trace_id"`
	SpanID       string `json:"span_id"`
	ParentSpanID string `json:"parent_span_id"`
	Path         string `json:"path"`
	Pool         string `json:"pool"`
	Status       int    `json:"status"`
}

func TestTraceIDJoinsUpstreamAndGatewayLog(t *testing.T) {
	s := newStack(t)
	logs := newGatewayLogs(t)
	rec := wiretest.HTTPRecorder{Client: s.control, BaseURL: s.mockURLs[0]}
	client := &http.Client{Timeout: wireCallTimeout}

	t.Run("caller traceparent is continued", func(t *testing.T) {
		traceID, parentID := randomHex(t, 16), randomHex(t, 8)
		callerTP := "00-" + traceID + "-" + parentID + "-01"
		since := time.Now().Add(-logSinceSlack)
		upTP := sendTraced(t, client, rec, s.gatewayURL, callerTP)

		wiretest.AssertTraceparent(t, callerTP, upTP)
		up := wiretest.TraceparentRE.FindStringSubmatch(upTP)
		if !strings.HasSuffix(upTP, "-01") {
			t.Errorf("upstream traceparent %s dropped the caller's sampled flag", upTP)
		}
		line := logs.requestLine(t, since, traceID)
		if line.SpanID != up[2] {
			t.Errorf("log span_id = %s, want the parent sent upstream %s", line.SpanID, up[2])
		}
		if line.ParentSpanID != parentID {
			t.Errorf("log parent_span_id = %q, want the caller's %s", line.ParentSpanID, parentID)
		}
		assertRequestLine(t, line)
	})

	t.Run("gateway starts a trace when the caller sends none", func(t *testing.T) {
		since := time.Now().Add(-logSinceSlack)
		upTP := sendTraced(t, client, rec, s.gatewayURL, "")
		up := wiretest.TraceparentRE.FindStringSubmatch(upTP)
		if up == nil {
			t.Fatalf("upstream traceparent %q is not a valid W3C value", upTP)
		}
		line := logs.requestLine(t, since, up[1])
		if line.SpanID != up[2] || line.ParentSpanID != "" {
			t.Errorf("log span_id/parent_span_id = %s/%q, want %s and none", line.SpanID, line.ParentSpanID, up[2])
		}
		assertRequestLine(t, line)
	})
}

// sendTraced streams one chat completion through the gateway, with
// traceparent tp when non-empty, and returns the traceparent mockllm got.
func sendTraced(t *testing.T, client *http.Client, rec wiretest.Recorder, gatewayURL, tp string) string {
	t.Helper()
	if err := rec.ResetRequests(); err != nil {
		t.Fatalf("reset mockllm capture: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, gatewayURL+chatPath, strings.NewReader(traceChatBody))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(mockllm.HeaderOutputTokens, wireOutputTokens)
	if tp != "" {
		req.Header.Set(telemetry.HeaderTraceparent, tp)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", chatPath, err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("POST %s: status %d, err %v: %s", chatPath, resp.StatusCode, err, body)
	}
	if _, err := wiretest.SSEEvents(body); err != nil {
		t.Fatalf("SSE framing: %v", err)
	}
	recs, err := rec.Requests()
	if err != nil {
		t.Fatalf("read mockllm capture: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("mockllm captured %d requests, want 1 (is other traffic running?)", len(recs))
	}
	if !bytes.Equal(recs[0].Body, []byte(traceChatBody)) {
		t.Errorf("upstream body differs from what was sent")
	}
	vals := recs[0].Header.Values(telemetry.HeaderTraceparent)
	if len(vals) != 1 {
		t.Fatalf("upstream traceparent values = %q, want exactly one", vals)
	}
	return vals[0]
}

func assertRequestLine(t *testing.T, line requestLine) {
	t.Helper()
	if line.Path != chatPath || line.Pool != "large" || line.Status != http.StatusOK {
		t.Errorf("log line path/pool/status = %s/%s/%d, want %s/large/200", line.Path, line.Pool, line.Status, chatPath)
	}
}

// gatewayLogs reads the gateway containers' stdout through the docker CLI.
type gatewayLogs struct {
	containers []string // container IDs, one per gateway service
}

// newGatewayLogs finds the gateway containers of the compose project. It
// skips the test when there is no docker CLI (run-in-network.sh's container).
func newGatewayLogs(t *testing.T) *gatewayLogs {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("no docker CLI to read gateway logs; run this test from the host: " +
			"cd gateway && go test -tags integration -count=1 -run TestTraceIDJoinsUpstreamAndGatewayLog ./integration/")
	}
	project := envOr(envComposeProject, defaultComposeProject)
	g := &gatewayLogs{}
	for _, svc := range strings.Split(envOr(envGatewayServices, defaultGatewayServices), ",") {
		if svc = strings.TrimSpace(svc); svc == "" {
			continue
		}
		out, err := docker(t, "ps", "-q",
			"--filter", "label=com.docker.compose.project="+project,
			"--filter", "label=com.docker.compose.service="+svc)
		ids := strings.Fields(string(out))
		if err != nil || len(ids) != 1 {
			t.Fatalf("gateway service %s of compose project %s: %d running containers (err %v); set %s",
				svc, project, len(ids), err, envComposeProject)
		}
		g.containers = append(g.containers, ids[0])
	}
	if len(g.containers) == 0 {
		t.Fatalf("%s names no gateway service", envGatewayServices)
	}
	return g
}

// requestLine waits for the one per-request log line carrying traceID on
// any gateway and returns it.
func (g *gatewayLogs) requestLine(t *testing.T, since time.Time, traceID string) requestLine {
	t.Helper()
	deadline := time.Now().Add(logWait)
	for {
		found, err := g.find(t, since, traceID)
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case len(found) == 1:
			return found[0]
		case len(found) > 1:
			t.Fatalf("%d per-request log lines carry trace_id %s, want 1", len(found), traceID)
		case time.Now().After(deadline):
			t.Fatalf("no gateway per-request log line with trace_id %s within %s", traceID, logWait)
		}
		time.Sleep(logPollInterval)
	}
}

func (g *gatewayLogs) find(t *testing.T, since time.Time, traceID string) ([]requestLine, error) {
	t.Helper()
	var found []requestLine
	for _, id := range g.containers {
		out, err := docker(t, "logs", "--since", since.UTC().Format(time.RFC3339Nano), id)
		if err != nil {
			return nil, fmt.Errorf("docker logs %s: %w", id, err)
		}
		sc := bufio.NewScanner(bytes.NewReader(out))
		sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
		for sc.Scan() {
			if !bytes.Contains(sc.Bytes(), []byte(traceID)) {
				continue
			}
			var line requestLine
			if json.Unmarshal(sc.Bytes(), &line) != nil || line.Msg != telemetry.LogMessage || line.TraceID != traceID {
				continue
			}
			found = append(found, line)
		}
		if err := sc.Err(); err != nil {
			return nil, fmt.Errorf("scan docker logs %s: %w", id, err)
		}
	}
	return found, nil
}

// docker runs the docker CLI and returns its stdout (gateway logs go to the
// containers' stdout).
func docker(t *testing.T, args ...string) ([]byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), dockerCmdTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return nil, fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return out, err
}

// randomHex returns 2n lowercase hex characters, never all zeros.
func randomHex(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	for {
		if _, err := rand.Read(b); err != nil {
			t.Fatal(err)
		}
		if s := hex.EncodeToString(b); strings.Trim(s, "0") != "" {
			return s
		}
	}
}
