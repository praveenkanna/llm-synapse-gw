# syntax=docker/dockerfile:1.7
#
# Compose overrides the frontend image with the BUILDKIT_SYNTAX build arg so it
# is pulled through IMAGE_REGISTRY; keep that tag in step with the line above.
#
# Images for the gateway module. Build context: gateway/.
#
#   docker build -f gateway/Dockerfile --target gateway -t llm-synapse-gw gateway
#   docker build -f gateway/Dockerfile --target mockllm -t llm-mockllm gateway
#
# Both final stages are distroless, run as the nonroot user (65532) and carry a
# tiny static HTTP probe (/usr/local/bin/httpcheck) for Compose healthchecks,
# since distroless has no shell, curl or wget.
#
# CGO is off today. When the tokenizer binding lands (M3, ADR 0008) the gateway
# stage needs glibc: build with CGO_ENABLED=1 and switch its base to
# gcr.io/distroless/cc-debian12:nonroot (as spikes/tokenizer/Dockerfile does).

# Base image sources. Defaults pull straight from Docker Hub and gcr.io. To pull
# through a proxy, set REGISTRY to its prefix with a trailing slash (e.g.
# registry.example.com/) and point DISTROLESS_IMAGE at a proxied copy.
ARG REGISTRY=""
ARG GO_VERSION=1.26.8
ARG DISTROLESS_IMAGE=gcr.io/distroless/static-debian12:nonroot

FROM --platform=$BUILDPLATFORM ${REGISTRY}golang:${GO_VERSION}-bookworm AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-trimpath

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

# httpcheck: exits 0 when GET <url> answers 2xx within 2s, 1 otherwise.
COPY <<'EOF' /probe/main.go
package main

import (
	"fmt"
	"net/http"
	"os"
	"time"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: httpcheck URL")
		os.Exit(2)
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	_ = resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		fmt.Fprintln(os.Stderr, "status", resp.StatusCode)
		os.Exit(1)
	}
}
EOF

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOOS=$TARGETOS GOARCH=$TARGETARCH go build -ldflags='-s -w' -o /out/ ./cmd/gateway ./cmd/mockllm \
 && cd /probe && GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -ldflags='-s -w' -o /out/httpcheck main.go

# Mock vLLM replica for the dev profile (ADR 0010). Listens on :8000 like vLLM.
FROM ${DISTROLESS_IMAGE} AS mockllm
COPY --from=build /out/mockllm /usr/local/bin/mockllm
COPY --from=build /out/httpcheck /usr/local/bin/httpcheck
USER nonroot:nonroot
EXPOSE 8000
ENTRYPOINT ["/usr/local/bin/mockllm"]

# The gateway (default target). Request path on :8080, metrics on :9090.
FROM ${DISTROLESS_IMAGE} AS gateway
COPY --from=build /out/gateway /usr/local/bin/gateway
COPY --from=build /out/httpcheck /usr/local/bin/httpcheck
USER nonroot:nonroot
EXPOSE 8080 9090
ENTRYPOINT ["/usr/local/bin/gateway"]
