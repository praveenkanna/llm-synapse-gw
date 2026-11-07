package wiretest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNormalize(t *testing.T) {
	tests := []struct {
		name, in, want string
	}{
		{
			name: "chat id and created",
			in:   `{"id":"chatcmpl-0a1b","object":"chat.completion","created":1727400000,"model":"m"}`,
			want: `{"id":"XXXXXXXXXXXXX","object":"chat.completion","created":0000000000,"model":"m"}`,
		},
		{
			name: "completion id in every SSE event",
			in:   "data: {\"id\":\"cmpl-ff\",\"created\":1}\n\ndata: {\"id\":\"cmpl-ff\",\"created\":1}\n\n",
			want: "data: {\"id\":\"XXXXXXX\",\"created\":0}\n\ndata: {\"id\":\"XXXXXXX\",\"created\":0}\n\n",
		},
		{
			name: "other ids and text untouched",
			in:   `{"id":"call_0","content":"created: 5","tool_call_id":"chatcmpl-1"}`,
			want: `{"id":"call_0","content":"created: 5","tool_call_id":"chatcmpl-1"}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := string(Normalize([]byte(tc.in)))
			if got != tc.want {
				t.Errorf("Normalize = %s\nwant       %s", got, tc.want)
			}
			if len(got) != len(tc.in) {
				t.Errorf("length changed: %d -> %d", len(tc.in), len(got))
			}
		})
	}
}

func TestSSEEvents(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		events  int
		wantErr string
	}{
		{name: "two events", in: "data: {\"a\":1}\n\ndata: {\"b\":2}\n\ndata: [DONE]\n\n", events: 2},
		{name: "missing done", in: "data: {\"a\":1}\n\n", wantErr: "[DONE]"},
		{name: "no trailing blank line", in: "data: {\"a\":1}\n\ndata: [DONE]\n", wantErr: "[DONE]"},
		{name: "only done", in: "data: [DONE]\n\n", wantErr: "no data events"},
		{name: "early done", in: "data: [DONE]\n\ndata: [DONE]\n\n", wantErr: "before the end"},
		{name: "not a data line", in: "event: x\n\ndata: [DONE]\n\n", wantErr: "not a data line"},
		{name: "single newline between events", in: "data: {\"a\":1}\ndata: {\"b\":2}\n\ndata: [DONE]\n\n", wantErr: "not a JSON object"},
		{name: "payload not object", in: "data: [1]\n\ndata: [DONE]\n\n", wantErr: "not a JSON object"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			events, err := SSEEvents([]byte(tc.in))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(events) != tc.events {
				t.Errorf("got %d events, want %d", len(events), tc.events)
			}
		})
	}
}

func TestLoadRejectsBadManifests(t *testing.T) {
	tests := []struct {
		name, manifest, wantErr string
	}{
		{"not json", `{`, "parse manifest"},
		{"no cases", `{"cases":[]}`, "no cases"},
		{"no name", `{"cases":[{"method":"GET","path":"/v1/models"}]}`, "without a name"},
		{"duplicate", `{"cases":[{"name":"a","method":"GET","path":"/x"},{"name":"a","method":"GET","path":"/x"}]}`, "duplicate"},
		{"bad method", `{"cases":[{"name":"a","method":"PUT","path":"/x"}]}`, "method"},
		{"bad path", `{"cases":[{"name":"a","method":"GET","path":"x"}]}`, "path"},
		{"post without body", `{"cases":[{"name":"a","method":"POST","path":"/x"}]}`, "needs a body"},
		{"missing body file", `{"cases":[{"name":"a","method":"POST","path":"/x","body":"nope.json"}]}`, "read body"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, ManifestFile), []byte(tc.manifest), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := Load(dir)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Load err = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

// Bodies load exactly as stored, including CRLF and trailing whitespace.
func TestLoadKeepsBodyBytes(t *testing.T) {
	dir := t.TempDir()
	body := "{\r\n\t\"model\" : \"m\"\r\n}  \r\n"
	if err := os.WriteFile(filepath.Join(dir, "a.body.json"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest := `{"cases":[{"name":"a","method":"POST","path":"/v1/completions","body":"a.body.json","header":{"content-type":"application/json"}}]}`
	if err := os.WriteFile(filepath.Join(dir, ManifestFile), []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}
	gs, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if string(gs[0].Body) != body {
		t.Errorf("body = %q, want %q", gs[0].Body, body)
	}
	if got := gs[0].HTTPHeader().Get("Content-Type"); got != "application/json" {
		t.Errorf("header not canonicalised: %v", gs[0].HTTPHeader())
	}
}
