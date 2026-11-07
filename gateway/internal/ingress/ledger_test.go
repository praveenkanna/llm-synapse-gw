package ingress

import (
	"testing"

	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry"
	"github.com/praveenkanna/llm-synapse-gw/gateway/internal/telemetry/telemetrytest"
)

// outcomes is what a gateway's request telemetry counted, the one ledger of
// what the handler and the dispatcher did (AD-12), under the names of the
// counters it replaced. Retries, upstream errors, disconnects and fail-opens
// are counted as they happen; relay errors and rejections are request
// outcomes, counted when the handler returns, which can trail the client.
type outcomes struct {
	UpstreamErrors    float64 // upstream failures, before or during the response
	ConnectRetries    float64
	RelayErrors       float64 // responses failed after their status was sent
	ClientDisconnects float64
	UnknownModel      float64 // fail open to the default pool: model not mapped
	UnparseableBody   float64 // fail open to the default pool: model not readable
	AdmissionRejected float64 // answered 429 or 503: admission's refusals in a fixture whose pools have replicas that answer 200
}

func countOutcomes(t testing.TB, m *telemetry.Metrics) outcomes {
	t.Helper()
	sum := func(metric string, labels ...string) float64 { return telemetrytest.Sum(t, m, metric, labels...) }
	status := func(s string) float64 { return sum(telemetry.MetricRequests, `status="`+s+`"`) }
	return outcomes{
		UpstreamErrors:    sum(telemetry.MetricUpstreamErrors),
		ConnectRetries:    sum(telemetry.MetricRetries),
		RelayErrors:       status(telemetry.OutcomeStreamError) + status(telemetry.OutcomeAborted),
		ClientDisconnects: sum(telemetry.MetricClientDisconnects),
		UnknownModel:      sum(telemetry.MetricFailOpen, `stage="`+telemetry.StagePoolResolve+`"`, `reason="`+ReasonUnknownModel+`"`),
		UnparseableBody:   sum(telemetry.MetricFailOpen, `stage="`+telemetry.StagePoolResolve+`"`, `reason="`+telemetry.ReasonUnparseableBody+`"`),
		AdmissionRejected: status("429") + status("503"),
	}
}

// outcomes is what g's telemetry counted.
func (g *poolGateway) outcomes(t testing.TB) outcomes {
	t.Helper()
	return countOutcomes(t, g.tel.Metrics())
}
