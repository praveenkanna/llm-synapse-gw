// Package integration holds end-to-end tests that run against the Docker
// Compose dev stack (nginx, two gateways, mockllm), not in-process fakes.
//
// Every test file carries the `integration` build tag, so plain
// `go test ./...` skips them. Run them with the stack up:
//
//	make up-dev
//	gateway/integration/run-in-network.sh                     # any host
//	cd gateway && go test -tags integration -count=1 -v ./integration/   # Linux host
//
// run-in-network.sh runs the test binary in a container on the stack's
// network, along with the Redis integration tests of internal/redis and
// internal/arbitrator. It is the only way to reach the admin API, the
// gateways' metrics and Redis, so CI runs it after the host run. Use it on
// macOS too: the host port forwarding of the Docker VM (observed with
// OrbStack) delays some client disconnects by seconds, which would fail the
// abort test for reasons outside the gateway.
//
// The tests:
//
//	TestClientAbortsCancelUpstreamAtScale  AC-M0-7, 1,000 aborts (in-network on macOS); AC-M2-4,
//	                                       admission counters back to 0 (in-network)
//	TestPriorityOnlyOnFlaggedPools         AC-M2-6, vLLM priority only on the flagged pool
//	TestLevel2CallWorksDirectToReplica     AC-M3-5, a Level 2 call (rendered messages + template headers) gets the
//	                                       same prompt_tokens straight from a replica and through the gateway;
//	                                       needs INTEGRATION_POSTGRES_DSN (published on 127.0.0.1:5433)
//	TestAdmin*                             AC-M3-1/2 and template matching through the admin API; need
//	                                       INTEGRATION_ADMIN_URL (http://gateway-1:8081, in-network only),
//	                                       INTEGRATION_ADMIN_TOKEN and INTEGRATION_GATEWAY_METRICS_URLS;
//	                                       run-in-network.sh sets all three from the stack and gateway.env
//	TestWireGoldensThroughStack            AC-M0-1/2, the M0-07 goldens through nginx (host or in-network)
//	TestTraceIDJoinsUpstreamAndGatewayLog  caller trace ID reaches mockllm and the gateway's
//	                                       per-request log line; needs the host's docker CLI to
//	                                       read gateway logs, so run it from the host (it skips
//	                                       in-network):
//
//	cd gateway && go test -tags integration -count=1 -run 'TestWire|TestTrace' ./integration/
//
// The tests read these environment variables (defaults match
// deploy/compose/docker-compose.yml):
//
//	INTEGRATION_GATEWAY_URL      entry point (nginx)                      http://127.0.0.1:8080
//	INTEGRATION_MOCKLLM_URLS     mockllm replicas, comma-sep; the first   http://127.0.0.1:8001,http://127.0.0.1:8002
//	                             serves the large (default) pool
//	INTEGRATION_COMPOSE_PROJECT  compose project, for gateway logs        llm-synapse-gw
//
// and the per-test knobs documented in each test file.
//
// The tests read mockllm's /mock/stats counters and /mock/requests capture
// and compare deltas, so no other traffic may run against the stack at the
// same time.
package integration
