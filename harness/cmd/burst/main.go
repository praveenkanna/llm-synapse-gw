// Command burst sends N identical requests at once and reports, per
// response, its status, size, completeness and SHA-256, plus how many
// distinct responses there were. It is the client side of the coalescing
// proof (AC-M4-1): N identical concurrent requests should reach the replica
// once and all get the same complete bytes.
//
//	burst -url http://nginx:80 -n 50 -body-file body.json \
//	  -H 'X-Template-Id: faq' -H 'X-Template-Version: 1' -out run.json
//
// With -lead-delay, one request goes first and the other N-1 follow after
// the delay (all at once). -slow-readers K makes the last K requests read
// at -slow-rate bytes/s through a small receive buffer, so a coalescing
// follower falls behind its leader (the replay-bound eviction path).
// Response bodies are hashed and dropped: no response text is written.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/praveenkanna/llm-synapse-gw/harness/internal/outcome"
)

// maxBody bounds how much of one response is read. Bodies are hashed as they
// arrive, not held; the outcome rules hold at most their own ceiling of a
// non-streaming body.
const maxBody = 64 << 20

type headerFlags []string

func (h *headerFlags) String() string { return strings.Join(*h, ", ") }
func (h *headerFlags) Set(v string) error {
	name, _, ok := strings.Cut(v, ":")
	if !ok || strings.TrimSpace(name) == "" {
		return fmt.Errorf("header %q: want 'Name: value'", v)
	}
	*h = append(*h, v)
	return nil
}

type options struct {
	url       string
	path      string
	n         int
	bodyFile  string
	headers   headerFlags
	leadDelay time.Duration
	timeout   time.Duration
	out       string
	slow      int // the last slow requests read slowly
	slowRate  int // bytes/s for slow readers
}

// response is one request's outcome (ADR 0021). It holds no response text.
type response struct {
	Index      int     `json:"index"`
	Slow       bool    `json:"slow,omitempty"`
	Status     int     `json:"status"`
	Err        string  `json:"error,omitempty"` // an outcome error kind
	OK         bool    `json:"ok"`
	Cache      string  `json:"cache,omitempty"` // X-Gateway-Cache
	Stream     bool    `json:"stream"`
	Bytes      int     `json:"bytes"`
	SHA256     string  `json:"sha256,omitempty"`
	Complete   bool    `json:"complete"`
	ErrorEvent bool    `json:"error_event,omitempty"` // a stream carried an SSE error event
	TTFTMS     float64 `json:"ttft_ms"`
	FirstMS    float64 `json:"first_byte_ms"`
	DurationMS float64 `json:"duration_ms"`
}

type report struct {
	URL       string         `json:"url"`
	N         int            `json:"n"`
	LeadDelay string         `json:"lead_delay"`
	BodySHA   string         `json:"body_sha256"`
	Headers   []string       `json:"headers"`
	OK        int            `json:"ok"`
	Complete  int            `json:"complete"`
	Distinct  int            `json:"distinct_responses"`
	ByHash    map[string]int `json:"by_hash"`
	Responses []response     `json:"responses"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(os.Stderr, "burst:", err)
		}
		os.Exit(2)
	}
}

func run(args []string, stdout io.Writer) error {
	var o options
	fs := flag.NewFlagSet("burst", flag.ContinueOnError)
	fs.StringVar(&o.url, "url", "http://localhost:8080", "base URL")
	fs.StringVar(&o.path, "path", "/v1/chat/completions", "request path")
	fs.IntVar(&o.n, "n", 50, "identical requests to send")
	fs.StringVar(&o.bodyFile, "body-file", "", "request body (JSON)")
	fs.Var(&o.headers, "H", "request header 'Name: value' (repeatable)")
	fs.DurationVar(&o.leadDelay, "lead-delay", 0, "send one request first and the rest after this delay (0 = all at once)")
	fs.DurationVar(&o.timeout, "timeout", 2*time.Minute, "per-request timeout")
	fs.StringVar(&o.out, "out", "", "write the JSON report here")
	fs.IntVar(&o.slow, "slow-readers", 0, "the last K requests read slowly")
	fs.IntVar(&o.slowRate, "slow-rate", 2048, "read rate of slow readers, bytes/s")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if o.n < 1 || o.bodyFile == "" {
		return errors.New("need -n >= 1 and -body-file")
	}
	if o.slow < 0 || o.slow > o.n || o.slowRate < 1 {
		return errors.New("need 0 <= -slow-readers <= -n and -slow-rate >= 1")
	}
	body, err := os.ReadFile(o.bodyFile)
	if err != nil {
		return err
	}
	rep := send(context.Background(), o, body)
	if o.out != "" {
		b, _ := json.MarshalIndent(rep, "", "  ")
		if err := os.WriteFile(o.out, append(b, '\n'), 0o644); err != nil {
			return err
		}
	}
	fmt.Fprintf(stdout, "sent %d: ok %d, complete %d, distinct responses %d\n", rep.N, rep.OK, rep.Complete, rep.Distinct)
	for _, h := range slices.Sorted(maps.Keys(rep.ByHash)) {
		fmt.Fprintf(stdout, "  %s  x%d\n", h, rep.ByHash[h])
	}
	return nil
}

func send(ctx context.Context, o options, body []byte) report {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = o.n
	tr.DisableCompression = true
	client := &http.Client{Transport: tr}
	slowTr := tr.Clone()
	slowTr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		c, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, addr)
		if tc, ok := c.(*net.TCPConn); ok {
			_ = tc.SetReadBuffer(4096)
		}
		return c, err
	}
	slowClient := &http.Client{Transport: slowTr}
	sum := sha256.Sum256(body)
	rep := report{URL: o.url, N: o.n, LeadDelay: o.leadDelay.String(), BodySHA: hex.EncodeToString(sum[:]),
		Headers: o.headers, ByHash: map[string]int{}, Responses: make([]response, o.n)}

	start := make(chan struct{})
	rest := make(chan struct{})
	var wg sync.WaitGroup
	for i := range o.n {
		wg.Go(func() {
			<-start
			if i > 0 && o.leadDelay > 0 {
				<-rest
			}
			if i >= o.n-o.slow {
				rep.Responses[i] = do(ctx, slowClient, o, body, i, o.slowRate)
			} else {
				rep.Responses[i] = do(ctx, client, o, body, i, 0)
			}
		})
	}
	close(start)
	if o.leadDelay > 0 {
		time.Sleep(o.leadDelay)
	}
	close(rest)
	wg.Wait()

	for _, r := range rep.Responses {
		if r.OK {
			rep.OK++
		}
		if r.Complete {
			rep.Complete++
		}
		if r.SHA256 != "" {
			rep.ByHash[r.SHA256]++
		}
	}
	rep.Distinct = len(rep.ByHash)
	return rep
}

// do sends one request and reads its whole response, at rate bytes/s when
// rate > 0, within the per-request timeout. The bytes are hashed (to count
// distinct responses) and the response is measured with the outcome rules.
func do(ctx context.Context, client *http.Client, o options, body []byte, i, rate int) response {
	res := response{Index: i, Slow: rate > 0}
	ctx, cancel := outcome.WithRequestTimeout(ctx, o.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(o.url, "/")+o.path, strings.NewReader(string(body)))
	if err != nil {
		res.Err = outcome.ErrRequest
		return res
	}
	req.Header.Set("Content-Type", "application/json")
	for _, h := range o.headers {
		name, value, _ := strings.Cut(h, ":")
		req.Header.Set(strings.TrimSpace(name), strings.TrimSpace(value))
	}
	t0 := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		res.Err = outcome.SendErr(ctx, err)
		res.DurationMS = outcome.MS(time.Since(t0))
		return res
	}
	defer resp.Body.Close()
	res.Status = resp.StatusCode
	res.Cache = resp.Header.Get("X-Gateway-Cache")
	res.Stream = strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream")
	success := resp.StatusCode >= 200 && resp.StatusCode < 300
	measured := outcome.NewBody(res.Stream, t0, nil)

	h := sha256.New()
	buf := make([]byte, 32<<10)
	if rate > 0 {
		buf = make([]byte, max(1, min(len(buf), rate/10))) // ten reads a second
	}
	readErr := ""
	for {
		if rate > 0 && res.Bytes > 0 {
			time.Sleep(time.Duration(float64(len(buf)) / float64(rate) * float64(time.Second)))
		}
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			now := time.Now()
			if res.Bytes == 0 {
				res.FirstMS = outcome.MS(now.Sub(t0))
			}
			res.Bytes += n
			h.Write(buf[:n])
			if success {
				measured.Write(buf[:n], now)
			}
		}
		if rerr != nil {
			if rerr != io.EOF {
				readErr = outcome.ReadErr(ctx)
			}
			break
		}
		if res.Bytes > maxBody {
			readErr = outcome.ErrBadResponse
			break
		}
	}
	end := time.Now()
	res.DurationMS = outcome.MS(end.Sub(t0))
	res.SHA256 = hex.EncodeToString(h.Sum(nil))
	if !success {
		res.Err = outcome.ErrHTTPStatus
		return res
	}
	m := measured.End(end, readErr)
	res.Err, res.Complete, res.ErrorEvent = m.Err, m.Complete, m.ErrorEvent
	res.TTFTMS = outcome.MS(m.TTFT)
	res.OK = outcome.OK(res.Status, res.Err)
	return res
}
