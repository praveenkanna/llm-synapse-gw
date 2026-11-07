package affinity_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/router/affinity"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
)

const headerTestBody = `{"model":"m","messages":[{"role":"user","content":"hello"}]}`

// sized returns a header value of exactly n bytes.
func sized(n int) string { return strings.Repeat("s", n) }

// TestMaxSessionHeaderBytes pins the owner's limit (2026-10-14): moving it
// moves what AC-M1-5 was measured at.
func TestMaxSessionHeaderBytes(t *testing.T) {
	if affinity.MaxSessionHeaderBytes != 256 {
		t.Fatalf("MaxSessionHeaderBytes = %d, want 256", affinity.MaxSessionHeaderBytes)
	}
}

// TestHeaderTier covers which header gives the key, around the limit and
// with both headers sent. An over-long header counts as absent, so the next
// tier decides, and HeaderTooLong says one was ignored.
func TestHeaderTier(t *testing.T) {
	limit := affinity.MaxSessionHeaderBytes
	bodyKey := affinity.Extract(nil, []byte(headerTestBody)).Key
	if bodyKey == "" {
		t.Fatal("the test body has no key")
	}
	long, longer := sized(limit+1), sized(1<<20)
	for _, tc := range []struct {
		name           string
		session, convo string
		body           string
		want           affinity.Result
	}{
		{name: "session 1 byte", session: "s", body: headerTestBody, want: affinity.Result{Key: "s", Tier: affinity.TierHeader}},
		{name: "session one under", session: sized(limit - 1), body: headerTestBody, want: affinity.Result{Key: sized(limit - 1), Tier: affinity.TierHeader}},
		{name: "session at the limit", session: sized(limit), body: headerTestBody, want: affinity.Result{Key: sized(limit), Tier: affinity.TierHeader}},
		{name: "session one over", session: long, body: headerTestBody, want: affinity.Result{Key: bodyKey, Tier: affinity.TierPrefixHash, HeaderTooLong: true}},
		{name: "session 1 MiB", session: longer, body: headerTestBody, want: affinity.Result{Key: bodyKey, Tier: affinity.TierPrefixHash, HeaderTooLong: true}},
		{name: "conversation at the limit", convo: sized(limit), body: headerTestBody, want: affinity.Result{Key: sized(limit), Tier: affinity.TierHeader}},
		{name: "conversation one over", convo: long, body: headerTestBody, want: affinity.Result{Key: bodyKey, Tier: affinity.TierPrefixHash, HeaderTooLong: true}},
		{name: "both over", session: longer, convo: long, body: headerTestBody, want: affinity.Result{Key: bodyKey, Tier: affinity.TierPrefixHash, HeaderTooLong: true}},
		{name: "session over, conversation usable", session: long, convo: "c", body: headerTestBody, want: affinity.Result{Key: "c", Tier: affinity.TierHeader, HeaderTooLong: true}},
		{name: "session usable, conversation not read", session: "s", convo: long, body: headerTestBody, want: affinity.Result{Key: "s", Tier: affinity.TierHeader}},
		{name: "over and no body", session: long, want: affinity.Result{Tier: affinity.TierNone, Reason: affinity.ReasonNoKey, HeaderTooLong: true}},
		{name: "over and a bad body", session: long, body: "not json", want: affinity.Result{Tier: affinity.TierNone, Reason: telemetry.ReasonUnparseableBody, HeaderTooLong: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := http.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
			if err != nil {
				t.Fatal(err)
			}
			if tc.session != "" {
				r.Header.Set(affinity.HeaderSessionID, tc.session)
			}
			if tc.convo != "" {
				r.Header.Set(affinity.HeaderConversationID, tc.convo)
			}
			got := affinity.Extract(r, []byte(tc.body))
			if got != tc.want {
				t.Fatalf("Extract = {key %d bytes, %s, %q, too long %v}, want {key %d bytes, %s, %q, too long %v}",
					len(got.Key), got.Tier, got.Reason, got.HeaderTooLong, len(tc.want.Key), tc.want.Tier, tc.want.Reason, tc.want.HeaderTooLong)
			}
		})
	}
}

// TestHeaderTierRepeatedHeader: only a header's first value is read, so
// repeating a header adds no cost and a long later value counts for nothing.
func TestHeaderTierRepeatedHeader(t *testing.T) {
	long := sized(affinity.MaxSessionHeaderBytes + 1)
	body := []byte(headerTestBody)
	for _, tc := range []struct {
		name        string
		values      []string
		wantKey     string
		wantTooLong bool
	}{
		{name: "short first", values: []string{"s", long}, wantKey: "s"},
		{name: "long first", values: []string{long, "s"}, wantKey: affinity.Extract(nil, body).Key, wantTooLong: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &http.Request{Header: http.Header{affinity.HeaderSessionID: tc.values}}
			got := affinity.Extract(r, body)
			if got.Key != tc.wantKey || got.HeaderTooLong != tc.wantTooLong {
				t.Fatalf("Extract = %d-byte key, too long %v; want key %q, too long %v", len(got.Key), got.HeaderTooLong, tc.wantKey, tc.wantTooLong)
			}
		})
	}
}

// TestHeaderTierAllocatesNothing: reading a header, within the limit or over
// it, allocates nothing.
func TestHeaderTierAllocatesNothing(t *testing.T) {
	for _, n := range []int{64, affinity.MaxSessionHeaderBytes, affinity.MaxSessionHeaderBytes + 1, 1 << 20} {
		r := &http.Request{Header: http.Header{}}
		r.Header.Set(affinity.HeaderSessionID, sized(n))
		if got := testing.AllocsPerRun(100, func() { affinity.Extract(r, nil) }); got != 0 {
			t.Errorf("%d-byte header: %v allocations", n, got)
		}
	}
}
