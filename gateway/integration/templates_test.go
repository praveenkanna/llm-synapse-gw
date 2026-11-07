//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/templates/pgstore"
)

// envPostgresDSN reaches the stack's Postgres (published on localhost);
// unset, the tests that need the registry skip.
const envPostgresDSN = "INTEGRATION_POSTGRES_DSN"

const fallbackSpec = `{"model":"large-model","variables":[{"name":"order"}],
 "messages":[{"role":"system","segments":[{"kind":"static","text":"You are the order-status assistant. Answer in one sentence."}]},
             {"role":"user","segments":[{"kind":"volatile","text":"Where is order {{order}}?"}]}]}`

func fallbackBody(order string, stream bool) []byte {
	b := fmt.Sprintf(`{"model":"large-model","messages":[{"role":"system","content":"You are the order-status assistant. Answer in one sentence."},{"role":"user","content":"Where is order %s?"}]`, order)
	if stream {
		b += `,"stream":true,"stream_options":{"include_usage":true}`
	}
	return []byte(b + `}`)
}

// registerActive stores and activates a version of spec under a fresh
// template ID, retiring it when the test ends. It skips without a DSN.
func registerActive(t *testing.T, spec string) (id string, version int) {
	t.Helper()
	dsn := os.Getenv(envPostgresDSN)
	if dsn == "" {
		t.Skipf("%s not set", envPostgresDSN)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store, err := pgstore.Open(ctx, dsn, pgstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	// The gateways migrate the schema at startup; wait for it.
	for {
		if _, err := store.Stamp(ctx); err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("template store not migrated: %v", ctx.Err())
		}
		time.Sleep(200 * time.Millisecond)
	}
	id = fmt.Sprintf("it-fallback-%08x", rand.Uint32())
	rec, err := store.CreateVersion(ctx, id, "integration test", json.RawMessage(spec), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SetStatus(ctx, id, rec.Version, templates.StatusActive); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = store.SetStatus(ctx, id, rec.Version, templates.StatusRetired)
	})
	return id, rec.Version
}

// promptTokens posts body with the template headers and returns the
// response's usage.prompt_tokens.
func promptTokens(t *testing.T, url string, body []byte, id string, version int, stream bool) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(templates.HeaderTemplateID, id)
	req.Header.Set(templates.HeaderTemplateVersion, strconv.Itoa(version))
	req.Header.Set("X-Mock-Output-Tokens", "3")
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST %s: status %d: %s", url, resp.StatusCode, raw)
	}
	var usage struct {
		Usage *struct {
			PromptTokens int `json:"prompt_tokens"`
		} `json:"usage"`
	}
	if !stream {
		if err := json.Unmarshal(raw, &usage); err != nil || usage.Usage == nil {
			t.Fatalf("POST %s: no usage in %s", url, raw)
		}
		return usage.Usage.PromptTokens
	}
	for _, line := range strings.Split(string(raw), "\n") {
		data, ok := strings.CutPrefix(line, "data: ")
		if !ok || data == "[DONE]" {
			continue
		}
		if json.Unmarshal([]byte(data), &usage) == nil && usage.Usage != nil {
			return usage.Usage.PromptTokens
		}
	}
	t.Fatalf("POST %s: stream carried no usage", url)
	return 0
}

// AC-M3-5 (ADR 0006): a Level 2 call is an ordinary chat completion with
// two extra headers, so with the gateway down a caller pointed straight at
// a replica keeps working, and gets the prompt token count it gets through
// the gateway.
func TestLevel2CallWorksDirectToReplica(t *testing.T) {
	s := newStack(t)
	id, version := registerActive(t, fallbackSpec)
	for _, stream := range []bool{false, true} {
		body := fallbackBody(strconv.Itoa(rand.IntN(1_000_000)), stream)
		direct := promptTokens(t, s.mockURLs[0], body, id, version, stream)
		via := promptTokens(t, s.gatewayURL, body, id, version, stream)
		if direct <= 0 || direct != via {
			t.Fatalf("stream=%v: prompt tokens direct %d, through the gateway %d; want equal", stream, direct, via)
		}
	}
}

// Admin API on the Compose network: INTEGRATION_ADMIN_URL (a gateway's
// admin port, e.g. http://gateway-1:8081) and INTEGRATION_ADMIN_TOKEN. The
// port is not published on the host, so these tests skip outside the network.
const (
	envAdminURL   = "INTEGRATION_ADMIN_URL"
	envAdminToken = "INTEGRATION_ADMIN_TOKEN"
	harnessSpecs  = "../../harness/profiles/templates/"
)

type adminAPI struct {
	t     *testing.T
	url   string
	token string
}

func newAdminAPI(t *testing.T) *adminAPI {
	t.Helper()
	url, token := os.Getenv(envAdminURL), os.Getenv(envAdminToken)
	if url == "" || token == "" {
		t.Skipf("%s and %s not set", envAdminURL, envAdminToken)
	}
	return &adminAPI{t: t, url: strings.TrimRight(url, "/"), token: token}
}

// do sends one admin request and returns the status and body.
func (a *adminAPI) do(method, path string, body []byte) (int, []byte) {
	a.t.Helper()
	req, err := http.NewRequest(method, a.url+"/admin/v1"+path, bytes.NewReader(body))
	if err != nil {
		a.t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+a.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		a.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

func readSpec(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(harnessSpecs + name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// register creates and activates a version of the compiled harness spec
// under a fresh template ID and returns it with the registration response.
func (a *adminAPI) register() (id string, version int, counts templates.Counts) {
	a.t.Helper()
	id = fmt.Sprintf("it-compiled-%08x", rand.Uint32())
	code, raw := a.do(http.MethodPost, "/templates/"+id+"/versions", readSpec(a.t, "m3-prompt-compiled.json"))
	if code != http.StatusCreated {
		a.t.Fatalf("register: status %d: %s", code, raw)
	}
	var created struct {
		Version int              `json:"version"`
		Counts  templates.Counts `json:"counts"`
	}
	if err := json.Unmarshal(raw, &created); err != nil {
		a.t.Fatal(err)
	}
	path := fmt.Sprintf("/templates/%s/versions/%d", id, created.Version)
	if code, raw := a.do(http.MethodPost, path+"/status", []byte(`{"status":"active"}`)); code != http.StatusOK {
		a.t.Fatalf("activate: status %d: %s", code, raw)
	}
	a.t.Cleanup(func() { a.do(http.MethodPost, path+"/status", []byte(`{"status":"retired"}`)) })
	return id, created.Version, created.Counts
}

// AC-M3-2: the misordered prompt is rejected at registration, with the
// offending segment named; the reordered one registers with token counts.
func TestAdminRejectsMisorderedAndCountsCompiled(t *testing.T) {
	a := newAdminAPI(t)
	code, raw := a.do(http.MethodPost, "/templates/it-misordered/versions", readSpec(t, "m3-prompt-misordered.json"))
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("misordered spec: status %d, want 422: %s", code, raw)
	}
	var rej struct {
		Error struct {
			Errors []struct{ Path, Message string } `json:"errors"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &rej); err != nil || len(rej.Error.Errors) == 0 || rej.Error.Errors[0].Path == "" {
		t.Fatalf("422 does not name a segment: %s", raw)
	}
	t.Logf("misordered spec rejected at %s: %s", rej.Error.Errors[0].Path, rej.Error.Errors[0].Message)

	_, _, counts := a.register()
	if counts.Error != "" || len(counts.Examples) != 2 || counts.PrefixTokens <= 0 || counts.PrefixTokens >= counts.Examples[0] {
		t.Fatalf("registration counts = %+v", counts)
	}
	t.Logf("compiled spec: examples %v tokens, %d shared prefix tokens", counts.Examples, counts.PrefixTokens)
}

// AC-M3-1: for 100 random variable maps, the count the admin API gives
// equals the prompt_tokens the replica reports for the rendered request
// sent through the gateway.
func TestAdminTokenCountsMatchUsage(t *testing.T) {
	s := newStack(t)
	a := newAdminAPI(t)
	id, version, _ := a.register()
	path := fmt.Sprintf("/templates/%s/versions/%d", id, version)

	const alphabet = "abc xyz\n\"\\é漢😀{}"
	runes := []rune(alphabet)
	randText := func() string {
		r := make([]rune, 1+rand.IntN(300))
		for i := range r {
			r[i] = runes[rand.IntN(len(runes))]
		}
		return string(r)
	}
	for i := 0; i < 100; i++ {
		vars, _ := json.Marshal(map[string]any{"variables": map[string]string{
			"request_id": randText(), "current_time": randText(), "user_name": randText(), "question": randText(),
		}})
		code, raw := a.do(http.MethodPost, path+"/tokenize", vars)
		if code != http.StatusOK {
			t.Fatalf("sample %d: tokenize status %d: %s", i, code, raw)
		}
		var tok struct {
			PromptTokens int `json:"prompt_tokens"`
		}
		_ = json.Unmarshal(raw, &tok)
		code, raw = a.do(http.MethodPost, path+"/render", vars)
		if code != http.StatusOK {
			t.Fatalf("sample %d: render status %d: %s", i, code, raw)
		}
		var rendered struct {
			Messages json.RawMessage `json:"messages"`
		}
		_ = json.Unmarshal(raw, &rendered)
		body := []byte(`{"model":"large-model","max_tokens":1,"messages":` + string(rendered.Messages) + `}`)
		if got := promptTokens(t, s.gatewayURL, body, id, version, false); got != tok.PromptTokens {
			t.Fatalf("sample %d: admin counted %d tokens, the replica reported %d", i, tok.PromptTokens, got)
		}
	}
}

// Requests naming a template registered through the admin API are matched
// by every gateway once its snapshot refreshes: the per-template request
// counter reaches the number sent, with no fail-open for the template.
func TestAdminRegisteredTemplateIsMatched(t *testing.T) {
	s := newStack(t)
	a := newAdminAPI(t)
	var metricURLs []string
	for _, u := range strings.Split(os.Getenv(envGatewayMetric), ",") {
		if u = strings.TrimSpace(u); u != "" {
			metricURLs = append(metricURLs, u)
		}
	}
	if len(metricURLs) == 0 {
		t.Skipf("%s not set", envGatewayMetric)
	}
	id, version, _ := a.register()
	path := fmt.Sprintf("/templates/%s/versions/%d", id, version)
	vars := []byte(`{"variables":{"request_id":"r-1","current_time":"2026-10-20T00:00:00Z","user_name":"Avery","question":"Where is my order?"}}`)
	code, raw := a.do(http.MethodPost, path+"/render", vars)
	if code != http.StatusOK {
		t.Fatalf("render: status %d: %s", code, raw)
	}
	var rendered struct {
		Messages json.RawMessage `json:"messages"`
	}
	_ = json.Unmarshal(raw, &rendered)
	body := []byte(`{"model":"large-model","max_tokens":4,"messages":` + string(rendered.Messages) + `}`)

	// Both gateways refresh within templates.refresh_interval; wait for them.
	time.Sleep(7 * time.Second)
	const n = 20
	for i := 0; i < n; i++ {
		promptTokens(t, s.gatewayURL, body, id, version, false)
	}
	var matched float64
	for _, u := range metricURLs {
		v, err := templateRequests(s, u, id)
		if err != nil {
			t.Fatal(err)
		}
		matched += v
	}
	if matched != n {
		t.Fatalf("gateways counted %v requests for %s, sent %d", matched, id, n)
	}
}

// templateRequests sums gateway_template_requests_total for one template.
func templateRequests(s *stack, url, id string) (float64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), statsRequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := s.control.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	var sum float64
	needle := `template="` + id + `"`
	for line := range strings.SplitSeq(string(b), "\n") {
		if !strings.HasPrefix(line, "gateway_template_requests_total{") || !strings.Contains(line, needle) {
			continue
		}
		f := strings.Fields(line)
		v, err := strconv.ParseFloat(f[len(f)-1], 64)
		if err != nil {
			return 0, err
		}
		sum += v
	}
	return sum, nil
}
