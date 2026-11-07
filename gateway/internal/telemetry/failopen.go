package telemetry

// Fail-open stages, for gateway_failopen_total{stage,reason}. Every stage
// name is defined here once.
//
// A request-scoped stage (one skipped for a request on its handler
// goroutine) is recorded with Request.FailOpen, which counts the metric and
// puts "stage:reason" on the request's log line. A process-scoped stage (a
// background refresh, a probe, a dial) is counted with Metrics.FailOpen,
// once per cycle, never once per request.
const (
	// Request-scoped.
	StagePoolResolve      = "pool_resolve"      // model not readable, too long or not mapped: default pool
	StageAffinity         = "affinity"          // no affinity key or no healthy ring: round-robin
	StageAffinityHeader   = "affinity_header"   // a session header ignored: the next key tier decides
	StageCompile          = "compile"           // template not matched: forwarded unchanged
	StageMaxTokens        = "max_tokens"        // gateway cap not set: the prompt may leave less room than the cap
	StageCoalesce         = "coalesce"          // not shared: the request generates alone
	StageCache            = "cache"             // response cache not used
	StageContext          = "context"           // budgeted context not assembled
	StageUpstreamPriority = "upstream_priority" // vLLM priority not set (also the background probe)
	StageStuckEngine      = "stuck_engine"      // a streaming attempt not watched for a stuck engine

	// Process-scoped.
	StageTemplateRegistry = "template_registry" // registry snapshot not refreshed
	StageAdmissionRedis   = "admission_redis"   // admission shares not synced through Redis
	StageUpstreamDNS      = "upstream_dns"      // replica address lookup failed
	StageLoadview         = "loadview"          // replica load not scraped
	StageStuckProbe       = "stuck_probe"       // stuck-engine generation probe not run
)

// Fail-open reasons that more than one stage uses. A reason only one stage
// uses is defined, once, by the package that records it.
const (
	ReasonUnparseableBody = "unparseable_body" // the request body is not the JSON the stage reads
	ReasonBodyTooLarge    = "body_too_large"   // the body is over the stage's ceiling
	ReasonTooManyMembers  = "too_many_members" // the body has more top-level members than bodyindex.MaxMembers (coalescing, cache, context)
	ReasonNoModel         = "no_model"         // no model to send a probe with
	ReasonProbeRejected   = "probe_rejected"   // the replica refused the probe
	ReasonRedisError      = "redis_error"      // Redis refused, dropped or broke the protocol
	ReasonNearModelLen    = "near_model_len"   // the prompt could leave less room than the cap (max_tokens stage)

	// Coalescing and the response cache: the request is recorded under
	// whichever of the two stages it was going to.
	ReasonNotDeterministic = "not_deterministic" // sampling settings make the response vary
	ReasonUnsupportedKey   = "unsupported_key"   // the body has a key the fingerprint cannot read safely
	ReasonFlightsFull      = "flights_full"      // the flight table is at its ceiling
)
