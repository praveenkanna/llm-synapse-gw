package wiretest

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/mockllm"
)

// maxCaptureResponseBytes bounds a /mock/requests answer read by HTTPRecorder
// (mockllm keeps at most 64 entries of 256 KiB by default, base64 in JSON).
const maxCaptureResponseBytes = 64 << 20

// MockRecorder reads the capture of an in-process mockllm.
type MockRecorder struct{ Mock *mockllm.Mock }

// ResetRequests clears the capture.
func (r MockRecorder) ResetRequests() error {
	r.Mock.ResetRequests()
	return nil
}

// Requests returns the captured requests, oldest first.
func (r MockRecorder) Requests() ([]mockllm.RecordedRequest, error) {
	return r.Mock.Requests(), nil
}

// HTTPRecorder reads a running mockllm's capture through its control
// endpoints (GET and DELETE /mock/requests).
type HTTPRecorder struct {
	Client  *http.Client
	BaseURL string // e.g. http://mockllm:8000
}

// ResetRequests clears the capture.
func (r HTTPRecorder) ResetRequests() error {
	_, err := r.do(http.MethodDelete, http.StatusNoContent)
	return err
}

// Requests returns the captured requests, oldest first.
func (r HTTPRecorder) Requests() ([]mockllm.RecordedRequest, error) {
	body, err := r.do(http.MethodGet, http.StatusOK)
	if err != nil {
		return nil, err
	}
	var out []mockllm.RecordedRequest
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("decode /mock/requests: %w", err)
	}
	return out, nil
}

func (r HTTPRecorder) do(method string, want int) ([]byte, error) {
	url := strings.TrimRight(r.BaseURL, "/") + "/mock/requests"
	req, err := http.NewRequest(method, url, http.NoBody)
	if err != nil {
		return nil, err
	}
	resp, err := r.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxCaptureResponseBytes))
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", method, url, err)
	}
	if resp.StatusCode != want {
		return nil, fmt.Errorf("%s %s: status %d (is MOCKLLM_CONTROL_ENABLED on?)", method, url, resp.StatusCode)
	}
	return body, nil
}
