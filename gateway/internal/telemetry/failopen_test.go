package telemetry

import "testing"

// TestFailOpenVocabulary pins every stage and shared reason to its metric
// label value: renaming one changes gateway_failopen_total's series, which
// dashboards and alerts read.
func TestFailOpenVocabulary(t *testing.T) {
	for got, want := range map[string]string{
		StagePoolResolve:      "pool_resolve",
		StageAffinity:         "affinity",
		StageAffinityHeader:   "affinity_header",
		StageCompile:          "compile",
		StageCoalesce:         "coalesce",
		StageCache:            "cache",
		StageContext:          "context",
		StageUpstreamPriority: "upstream_priority",
		StageStuckEngine:      "stuck_engine",
		StageTemplateRegistry: "template_registry",
		StageAdmissionRedis:   "admission_redis",
		StageUpstreamDNS:      "upstream_dns",
		StageLoadview:         "loadview",
		StageStuckProbe:       "stuck_probe",

		ReasonUnparseableBody:  "unparseable_body",
		ReasonBodyTooLarge:     "body_too_large",
		ReasonTooManyMembers:   "too_many_members",
		ReasonNoModel:          "no_model",
		ReasonProbeRejected:    "probe_rejected",
		ReasonRedisError:       "redis_error",
		ReasonNotDeterministic: "not_deterministic",
		ReasonUnsupportedKey:   "unsupported_key",
		ReasonFlightsFull:      "flights_full",
	} {
		if got != want {
			t.Errorf("fail-open name %q, want %q", got, want)
		}
	}
}
