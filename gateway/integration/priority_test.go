//go:build integration

package integration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/mockllm"
)

// TestPriorityOnlyOnFlaggedPools checks AC-M2-6 on the dev stack: the large
// pool (priority_scheduling: true, mockllm with the priority policy)
// receives each class's mapped vLLM priority spliced into an otherwise
// unchanged body, and the small pool (unflagged, fcfs mockllm, which
// rejects a non-zero priority with 400 as vLLM does) never receives a
// non-zero priority, even when the caller sends one.
func TestPriorityOnlyOnFlaggedPools(t *testing.T) {
	st := newStack(t)
	if len(st.mockURLs) < 2 {
		t.Skip("needs the large and small mockllm replicas")
	}
	large, small := st.mockURLs[0], st.mockURLs[1]
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	waitPriorityVerified(ctx, t, st, large)

	before := map[string]mockllm.Stats{large: replicaStats(ctx, t, st, large), small: replicaStats(ctx, t, st, small)}
	client := &http.Client{Timeout: 30 * time.Second}
	marker := fmt.Sprintf("prio-it-%d", time.Now().UnixNano())
	const n = 10
	for i := range n {
		// Background to the flagged pool: the gateway adds "priority":10.
		body := fmt.Sprintf(`{"model":"large-model","messages":[{"role":"user","content":"%s-bg-%d"}],"max_tokens":1}`, marker, i)
		post(ctx, t, client, st.gatewayURL, body, "background", http.StatusOK)
		// A caller's own priority to the unflagged pool: cut by the gateway,
		// or the fcfs replica would answer 400.
		body = fmt.Sprintf(`{"model":"small-model","priority":5,"messages":[{"role":"user","content":"%s-sm-%d"}],"max_tokens":1}`, marker, i)
		post(ctx, t, client, st.gatewayURL, body, "user", http.StatusOK)
	}

	afterLarge, afterSmall := replicaStats(ctx, t, st, large), replicaStats(ctx, t, st, small)
	if d := afterSmall.PriorityRequests - before[small].PriorityRequests; d != 0 {
		t.Errorf("unflagged pool received %d requests with a non-zero priority, want 0", d)
	}
	if d := afterSmall.Rejected - before[small].Rejected; d != 0 {
		t.Errorf("unflagged pool rejected %d requests", d)
	}
	if d := afterLarge.PriorityRequests - before[large].PriorityRequests; d < n {
		t.Errorf("flagged pool received %d requests with a priority, want >= %d", d, n)
	}

	var recs []mockllm.RecordedRequest
	if err := st.get(ctx, large+"/mock/requests", &recs); err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, r := range recs {
		if !bytes.Contains(r.Body, []byte(marker+"-bg-")) {
			continue
		}
		seen++
		if !bytes.HasPrefix(r.Body, []byte(`{"priority":10,"model":"large-model","messages":[{"role":"user","content":"`+marker)) {
			t.Errorf("flagged pool body = %s", r.Body)
		}
	}
	if seen == 0 {
		t.Error("no background request found in the large replica's capture")
	}
}

func waitPriorityVerified(ctx context.Context, t *testing.T, st *stack, large string) {
	t.Helper()
	for {
		s := replicaStats(ctx, t, st, large)
		if s.PriorityRequests > 0 { // the gateways' probes reached the replica
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("no priority probe reached the large replica; is priority_scheduling on in gateway.dev.yaml?")
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func replicaStats(ctx context.Context, t *testing.T, st *stack, url string) mockllm.Stats {
	t.Helper()
	var s mockllm.Stats
	if err := st.get(ctx, url+"/mock/stats", &s); err != nil {
		t.Fatal(err)
	}
	return s
}

func post(ctx context.Context, t *testing.T, client *http.Client, gatewayURL, body, class string, want int) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, gatewayURL+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Priority", class)
	req.Header.Set(mockllm.HeaderOutputTokens, "1")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != want {
		t.Fatalf("status %d, want %d: %s", resp.StatusCode, want, b)
	}
}
