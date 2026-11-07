package telemetry

import (
	"strings"
	"testing"
)

// TestRetainReplicasDropsRemovedReplicaSeries: after a reload removes r1 and
// moves r2 to another pool, their per-replica series go away; r0's stay, and
// an in-flight series above zero survives until its requests finish.
func TestRetainReplicasDropsRemovedReplicaSeries(t *testing.T) {
	m := NewMetrics()
	for _, r := range []string{"r0", "r1", "r2"} {
		m.SetReplicaInfo("large", r, "0.6.3")
		m.upstreamErrors.with(r, "connect").inc()
		m.inflight.with("large", r, ClassDefault).add(1)
		m.inflight.with("large", r, ClassDefault).add(-1)
	}
	held := m.inflight.with("large", "r1", "batch") // a request still in flight on r1
	held.add(1)
	m.upstreamErrors.with("", "connect").inc() // no replica: never pruned

	m.RetainReplicas(map[string]string{"r0": "large", "r2": "small"})

	body := scrape(t, m)
	for _, gone := range []string{
		`gateway_replica_info{pool="large",replica="r1"`,
		`gateway_replica_info{pool="large",replica="r2"`,
		`gateway_upstream_errors_total{replica="r1"`,
		`gateway_inflight{pool="large",replica="r1",class="default"}`,
		`gateway_inflight{pool="large",replica="r2",class="default"}`,
	} {
		if strings.Contains(body, gone) {
			t.Errorf("series %s still exported after its replica was removed:\n%s", gone, body)
		}
	}
	for _, kept := range []string{
		`gateway_replica_info{pool="large",replica="r0",vllm_version="0.6.3"} 1`,
		`gateway_upstream_errors_total{replica="r0",kind="connect"} 1`,
		`gateway_upstream_errors_total{replica="r2",kind="connect"} 1`, // no pool label: r2 still exists
		`gateway_upstream_errors_total{replica="",kind="connect"} 1`,
		`gateway_inflight{pool="large",replica="r0",class="default"} 0`,
		`gateway_inflight{pool="large",replica="r1",class="batch"} 1`,
	} {
		if !strings.Contains(body, kept) {
			t.Errorf("missing %s in:\n%s", kept, body)
		}
	}
	if len(m.replicaVersions) != 1 {
		t.Errorf("replica versions remembered = %d, want 1 (r0)", len(m.replicaVersions))
	}

	// The held request finishes; the next retain drops its series.
	held.add(-1)
	m.RetainReplicas(map[string]string{"r0": "large", "r2": "small"})
	if body := scrape(t, m); strings.Contains(body, `replica="r1"`) {
		t.Errorf("r1 series left after its last request finished:\n%s", body)
	}
	if got := m.inflight.seriesCount(); got != 1 {
		t.Errorf("inflight series = %d, want 1 (r0)", got)
	}

	// A re-added replica gets fresh series again.
	m.SetReplicaInfo("large", "r1", "0.7.0")
	if body := scrape(t, m); !strings.Contains(body, `gateway_replica_info{pool="large",replica="r1",vllm_version="0.7.0"} 1`) {
		t.Errorf("re-added replica not exported:\n%s", body)
	}
}

func TestRetainReplicasNilMetrics(t *testing.T) {
	var m *Metrics
	m.RetainReplicas(nil) // no panic
}
