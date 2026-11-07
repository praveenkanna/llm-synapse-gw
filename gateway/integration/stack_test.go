//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/mockllm"
)

// Environment variables and their defaults (see doc.go).
const (
	envGatewayURL       = "INTEGRATION_GATEWAY_URL"
	envMockURLs         = "INTEGRATION_MOCKLLM_URLS"
	defaultGatewayURL   = "http://127.0.0.1:8080"
	defaultMockURLs     = "http://127.0.0.1:8001,http://127.0.0.1:8002"
	stackProbeTimeout   = 5 * time.Second
	statsRequestTimeout = 2 * time.Second
)

// stack is the running Compose dev stack as seen from the host.
type stack struct {
	gatewayURL string
	mockURLs   []string
	control    *http.Client // for /mock/stats and health probes, never for test traffic
}

// newStack reads the stack addresses from the environment and fails the test
// with the start command if the entry point or a mockllm replica is down.
func newStack(t *testing.T) *stack {
	t.Helper()
	s := &stack{
		gatewayURL: strings.TrimRight(envOr(envGatewayURL, defaultGatewayURL), "/"),
		control:    &http.Client{Timeout: statsRequestTimeout},
	}
	for _, u := range strings.Split(envOr(envMockURLs, defaultMockURLs), ",") {
		if u = strings.TrimRight(strings.TrimSpace(u), "/"); u != "" {
			s.mockURLs = append(s.mockURLs, u)
		}
	}
	if len(s.mockURLs) == 0 {
		t.Fatalf("%s names no mockllm replica", envMockURLs)
	}
	ctx, cancel := context.WithTimeout(context.Background(), stackProbeTimeout)
	defer cancel()
	for _, probe := range append([]string{s.gatewayURL + "/livez"}, s.healthURLs()...) {
		if err := s.get(ctx, probe, nil); err != nil {
			t.Fatalf("dev stack not reachable (%v); start it with `make up-dev` or set %s / %s", err, envGatewayURL, envMockURLs)
		}
	}
	return s
}

func (s *stack) healthURLs() []string {
	out := make([]string, len(s.mockURLs))
	for i, u := range s.mockURLs {
		out[i] = u + "/health"
	}
	return out
}

// stats sums /mock/stats over every mockllm replica.
func (s *stack) stats(ctx context.Context) (mockllm.Stats, error) {
	var sum mockllm.Stats
	for _, u := range s.mockURLs {
		var one mockllm.Stats
		if err := s.get(ctx, u+"/mock/stats", &one); err != nil {
			return mockllm.Stats{}, err
		}
		sum.Requests += one.Requests
		sum.Completed += one.Completed
		sum.Cancelled += one.Cancelled
		sum.Rejected += one.Rejected
		sum.Running += one.Running
		sum.Waiting += one.Waiting
	}
	return sum, nil
}

// get fetches url and, if into is non-nil, decodes a JSON body into it.
func (s *stack) get(ctx context.Context, url string, into any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := s.control.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}
	if into == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(into)
}

func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// envInt reads a positive integer knob, failing the test on a bad value.
func envInt(t *testing.T, key string, def int) int {
	t.Helper()
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		t.Fatalf("%s=%q must be a positive integer", key, v)
	}
	return n
}
