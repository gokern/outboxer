# outboxer — local development tooling.
#
# The tests need a real Postgres and will FAIL, not skip, without one: a suite
# that goes green by skipping is a suite that reports coverage it does not have.
# POSTGRES_URL must point at a database this package may own outright — it drops
# and recreates the outbox table on every run.
#
# `make db` starts a throwaway Postgres. CI runs the suite against every
# supported major, so override DB_IMAGE and DB_PORT to reproduce one leg of that
# matrix: make db DB_IMAGE=postgres:14-alpine DB_PORT=15444

.DEFAULT_GOAL := help

DB_NAME      ?= outboxer_test
DB_PORT      ?= 15433
DB_IMAGE     ?= postgres:17-alpine
DB_CONTAINER ?= outboxer-test-pg

POSTGRES_URL ?= postgres://postgres:postgres@127.0.0.1:$(DB_PORT)/$(DB_NAME)?sslmode=disable

# Shuffled on purpose. Every test owns its table, but they share one database,
# and a test that leaves a trigger or a function behind changes what the next
# one measures. A fixed order hides that until the day somebody adds a test.
#
# Every test also calls t.Parallel, which is where most of the wall clock went:
# the slowest tests are waiting out leases and shutdown bounds rather than
# working, and waiting overlaps. -parallel defaults to GOMAXPROCS; raising it
# well past that is bounded by the server's connection limit, not the CPU.
GO_TEST ?= go test -race -shuffle=on

.PHONY: help
help: ## List the targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-18s\033[0m %s\n", $$1, $$2}'

.PHONY: db
db: ## Start a throwaway Postgres for the tests
	docker run -d --rm --name $(DB_CONTAINER) \
		-p $(DB_PORT):5432 \
		-e POSTGRES_USER=postgres \
		-e POSTGRES_PASSWORD=postgres \
		-e POSTGRES_DB=$(DB_NAME) \
		$(DB_IMAGE)
	@until docker exec $(DB_CONTAINER) pg_isready -q -U postgres; do sleep 0.2; done
	@echo "POSTGRES_URL=$(POSTGRES_URL)"

.PHONY: db-stop
db-stop: ## Stop the throwaway Postgres
	docker stop $(DB_CONTAINER)

.PHONY: test
test: ## Run the tests with the race detector
	POSTGRES_URL='$(POSTGRES_URL)' $(GO_TEST) ./...

.PHONY: test-pgbouncer
test-pgbouncer: ## Run the pooler tests against a real PgBouncer (needs PGBOUNCER_URL and DIRECT_URL)
	POSTGRES_URL='$(POSTGRES_URL)' PGBOUNCER_URL='$(PGBOUNCER_URL)' DIRECT_URL='$(DIRECT_URL)' \
		go test -race -count=1 -run 'Pooler' -v ./...

.PHONY: test-cover
test-cover: ## Run the tests and report total coverage
	POSTGRES_URL='$(POSTGRES_URL)' $(GO_TEST) -covermode=atomic -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

.PHONY: lint
lint: ## Run golangci-lint
	golangci-lint run ./...

.PHONY: fmt
fmt: ## Apply the formatters configured in .golangci.yaml
	golangci-lint fmt ./...

.PHONY: vuln
vuln: ## Scan dependencies for known vulnerabilities
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

.PHONY: check
check: lint test vuln ## Everything CI runs before merge
