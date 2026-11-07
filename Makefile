# LLM gateway: build, test and lint both Go modules (gateway/, harness/).
# Mirrors the CI `go` job in .github/workflows/ci.yml.

MODULES     := gateway harness
GO          ?= go
STATICCHECK ?= honnef.co/go/tools/cmd/staticcheck@v0.8.2
BIN_DIR     := bin
COMPOSE     := docker compose -f deploy/compose/docker-compose.yml
NGINX_URL   ?= http://127.0.0.1:8080

.DEFAULT_GOAL := help

.PHONY: help all build test test-race lint fmt fmt-check vet staticcheck run clean \
        compose-env compose-config up-dev up-gpu down smoke

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-12s %s\n", $$1, $$2}'

all: lint build test ## Lint, build and test every module

build: ## Compile every package; gateway commands land in gateway/bin/ (gitignored)
	@set -e; for m in $(MODULES); do echo "==> build $$m"; (cd $$m && $(GO) build -o /dev/null ./...); done
	cd gateway && $(GO) build -o $(BIN_DIR)/ ./cmd/...

test: ## Run tests with the race detector, then the allocation tests without it
	@set -e; for m in $(MODULES); do echo "==> test $$m"; (cd $$m && $(GO) test -race -count=1 ./... && $(GO) test -count=1 -run Alloc ./...); done

test-race: test

lint: fmt-check vet staticcheck ## gofmt check, go vet and staticcheck

fmt: ## Format all Go code in place
	@set -e; for m in $(MODULES); do (cd $$m && gofmt -w .); done

fmt-check: ## Fail if any Go file is not gofmt-formatted
	@set -e; for m in $(MODULES); do \
		out=$$(cd $$m && gofmt -l .); \
		if [ -n "$$out" ]; then echo "Not gofmt-formatted in $$m:"; echo "$$out"; exit 1; fi; \
	done

vet: ## go vet every module
	@set -e; for m in $(MODULES); do echo "==> vet $$m"; (cd $$m && $(GO) vet ./...); done

staticcheck: ## staticcheck every module (fetched via go run)
	@set -e; for m in $(MODULES); do echo "==> staticcheck $$m"; (cd $$m && $(GO) run $(STATICCHECK) ./...); done

run: ## Run the gateway on 127.0.0.1:8080 (metrics :9090) with gateway.example.yaml and auth off; its replicas are Compose hostnames, so point GATEWAY_CONFIG (path relative to gateway/) at your own replicas or run inside the dev stack
	cd gateway && GATEWAY_CONFIG=$${GATEWAY_CONFIG:-../deploy/compose/config/gateway.example.yaml} \
		GATEWAY_AUTH_ENABLED=$${GATEWAY_AUTH_ENABLED:-false} \
		GATEWAY_LISTEN_ADDR=$${GATEWAY_LISTEN_ADDR:-127.0.0.1:8080} \
		GATEWAY_METRICS_LISTEN_ADDR=$${GATEWAY_METRICS_LISTEN_ADDR:-127.0.0.1:9090} \
		$(GO) run ./cmd/gateway

clean: ## Remove build output
	rm -rf gateway/$(BIN_DIR) harness/$(BIN_DIR)

# ---- Docker Compose stack (deploy/compose) ----

compose-env: ## Create deploy/compose/*.env from the committed examples (never overwrites)
	@for f in deploy/compose/*.env.example; do t=$${f%.example}; \
		if [ ! -f "$$t" ]; then cp "$$f" "$$t"; echo "created $$t"; fi; done
	@scripts/compose-secrets.sh deploy/compose

compose-config: compose-env ## Validate the compose file for both profiles
	$(COMPOSE) --profile dev config -q
	$(COMPOSE) --profile gpu config -q

up-dev: compose-env ## Build and start the dev stack (mockllm, no GPU); waits until healthy
	$(COMPOSE) --profile dev up -d --build --wait

up-gpu: compose-env ## Start the gpu stack (vLLM); set tokens in deploy/compose/gateway.env first
	GATEWAY_CONFIG_FILE=gateway.gpu.yaml $(COMPOSE) --profile gpu up -d --build --wait

down: ## Stop the stack (both profiles); named volumes are kept
	$(COMPOSE) --profile dev --profile gpu down

smoke: ## Stream one chat completion through nginx (TOKEN=... for the gpu profile)
	curl -sN --fail-with-body --max-time 30 $(NGINX_URL)/v1/chat/completions \
		-H 'Content-Type: application/json' $(if $(TOKEN),-H 'Authorization: Bearer $(TOKEN)') \
		-d '{"model":"large-model","stream":true,"max_tokens":16,"messages":[{"role":"user","content":"hello"}]}'
