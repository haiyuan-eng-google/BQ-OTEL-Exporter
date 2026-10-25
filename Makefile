GOTEST ?= go test
GOCMD  ?= go
OCB_VERSION ?= v0.163.0
COLLECTOR_DIR ?= otelcol-bq-dist
COLLECTOR_BIN ?= $(COLLECTOR_DIR)/otelcol-bq

.PHONY: all
all: fmt vet lint test

.PHONY: fmt
fmt:
	$(GOCMD) fmt ./...

.PHONY: vet
vet:
	$(GOCMD) vet ./...

.PHONY: lint
lint:
	golangci-lint run ./...
	golangci-lint run --build-tags=integration ./...

.PHONY: test
test:
	$(GOTEST) -race -cover -count=1 ./...

# Live-BigQuery integration tests. Requires BQ_PROJECT and BQ_DATASET plus
# application default credentials.
.PHONY: integration-test
integration-test:
	$(GOTEST) -tags=integration -race ./...

# Build and check the repository's custom Collector distribution. The stock
# otelcol-contrib binary does not contain this exporter.
.PHONY: collector-build
collector-build:
	$(GOCMD) run go.opentelemetry.io/collector/cmd/builder@$(OCB_VERSION) --config example/otelcol-builder.yaml

.PHONY: collector-components
collector-components: collector-build
	$(COLLECTOR_BIN) components | grep -q 'bigquery'

.PHONY: collector-validate
collector-validate: collector-build
	BQ_PROJECT=my-project BQ_DATASET=otel BQ_LOCATION=US \
		$(COLLECTOR_BIN) validate --config example/otel-collector-config.yml

.PHONY: collector-check
collector-check: collector-components collector-validate

.PHONY: collector-image
collector-image:
	docker build --tag bq-otel-collector:0.1.0-dev --file example/Dockerfile .

# Regenerate the files mdatagen derives from metadata.yaml, and fail when
# metadata.yaml no longer satisfies the schema of the pinned mdatagen release.
#
# Three constraints shape the recipe.
#
# * The mdatagen module carries replace directives, so `go run …@version` and
#   `go install …@version` both refuse to build it. Adding it to this module with
#   `go get -tool` would pull its whole dependency tree into go.mod. Instead it
#   is built from a throwaway module pinned to the same Collector release as the
#   builder.
# * mdatagen names the Go package of the test files it writes after the
#   directory that holds metadata.yaml, so it fails in a directory whose name is
#   not a valid Go identifier, such as a checkout called BQ-OTEL-Exporter. The
#   inputs are therefore staged in a scratch directory named after the package.
# * The exporter's self-telemetry is written by hand in
#   internal/metadata/telemetry.go, so what mdatagen generates from the
#   telemetry section is validated and then discarded. Only generated_status.go
#   is copied back, under the repository's license header.
#   generated_package_test.go is deliberately not regenerated: it carries goleak
#   options that mdatagen cannot express (see goleak_options_unit_test.go).
.PHONY: mdatagen
mdatagen:
	@tmp="$$(mktemp -d)" && trap 'rm -rf "$$tmp"' EXIT && \
		( cd "$$tmp" && \
		  $(GOCMD) mod init mdatagen-tool 2>/dev/null && \
		  $(GOCMD) get -tool go.opentelemetry.io/collector/cmd/mdatagen@$(OCB_VERSION) && \
		  $(GOCMD) build -o "$$tmp/mdatagen" go.opentelemetry.io/collector/cmd/mdatagen ) && \
		stage="$$tmp/bigqueryexporter" && mkdir "$$stage" && \
		cp go.mod doc.go metadata.yaml "$$stage" && \
		( cd "$$stage" && "$$tmp/mdatagen" metadata.yaml ) && \
		{ printf '%s\n' '// Copyright 2026 Google LLC' '// SPDX-License-Identifier: Apache-2.0' ''; \
		  cat "$$stage/internal/metadata/generated_status.go"; } > internal/metadata/generated_status.go

.PHONY: tidy
tidy:
	$(GOCMD) mod tidy

# Reports known vulnerabilities reachable from this module's code. Install with
# `go install golang.org/x/vuln/cmd/govulncheck@latest`.
.PHONY: vuln
vuln:
	govulncheck ./...

# Runs every benchmark once per package, with allocation counts. Unit tests are
# skipped.
.PHONY: bench
bench:
	$(GOTEST) -run '^$$' -bench . -benchmem ./...

# Compares benchmarks with benchstat. Given two saved `go test -bench` outputs,
# it compares just those:
#
#   make bench-compare BASE=old.txt HEAD=new.txt
#
# Otherwise it runs the benchmarks in BENCH_PKGS BENCH_COUNT times each at
# BASE_REF, checked out in a temporary worktree, and in the working tree, and
# compares the two runs. On a branch of several commits, set BASE_REF to the
# branch point, for example BASE_REF=origin/main. Set BENCHSTAT to an installed
# benchstat binary to skip `go run`.
BASE_REF    ?= HEAD~1
BENCH_PKGS  ?= ./...
BENCH_COUNT ?= 6
BENCH_TIME  ?= 100ms
BENCHSTAT   ?= $(GOCMD) run golang.org/x/perf/cmd/benchstat@latest
.PHONY: bench-compare
bench-compare:
	@set -e; \
	if [ -n "$(BASE)$(HEAD)" ]; then \
		if [ -z "$(BASE)" ] || [ -z "$(HEAD)" ]; then \
			echo "bench-compare: set both BASE and HEAD, or neither" >&2; \
			exit 2; \
		fi; \
		$(BENCHSTAT) base="$(BASE)" head="$(HEAD)"; \
		exit 0; \
	fi; \
	tmp=$$(mktemp -d); \
	trap 'git worktree remove --force "$$tmp/base" 2>/dev/null; rm -rf "$$tmp"' EXIT; \
	trap 'exit 130' HUP INT TERM; \
	git worktree add --detach --quiet "$$tmp/base" "$(BASE_REF)"; \
	echo "==> $(BASE_REF)"; \
	if ! (cd "$$tmp/base" && $(GOTEST) -run '^$$' -bench . -benchmem -count $(BENCH_COUNT) -benchtime $(BENCH_TIME) $(BENCH_PKGS)) > "$$tmp/base.txt" 2>&1; then \
		cat "$$tmp/base.txt" >&2; \
		exit 1; \
	fi; \
	echo "==> working tree"; \
	if ! $(GOTEST) -run '^$$' -bench . -benchmem -count $(BENCH_COUNT) -benchtime $(BENCH_TIME) $(BENCH_PKGS) > "$$tmp/head.txt" 2>&1; then \
		cat "$$tmp/head.txt" >&2; \
		exit 1; \
	fi; \
	$(BENCHSTAT) base="$$tmp/base.txt" head="$$tmp/head.txt"

# Runs each fuzz target for FUZZTIME. Go fuzzes one target per invocation, so
# the targets of every package are discovered with `go test -list`. Packages
# without fuzz targets are skipped. A new target needs no change here, but CI
# fuzzes only the targets listed in .github/workflows/bench-fuzz.yml, which
# TestBenchFuzzWorkflowListsEveryFuzzTarget keeps complete.
FUZZTIME ?= 30s
.PHONY: fuzz
fuzz:
	@set -e; \
	for pkg in $$($(GOCMD) list ./...); do \
		for target in $$($(GOTEST) -list '^Fuzz' $$pkg | awk '/^Fuzz/ { print $$1 }'); do \
			echo "==> $$pkg $$target"; \
			$(GOTEST) -run '^$$' -fuzz "^$$target$$" -fuzztime $(FUZZTIME) $$pkg; \
		done; \
	done

# Runs the synthetic OpenTelemetry GenAI & MCP load testbench across stream
# pool sizes 1, 2, 4, 8, and 16 against the in-process bqfake Storage Write API
# server (or live BigQuery when BENCH_MODE=live and BQ_PROJECT is set).
# BENCH_DATASET is intentionally separate from BQ_DATASET so an ambient
# BQ_DATASET is never passed to -ephemeral-dataset teardown.
BENCH_MODE     ?= bqfake
BENCH_DURATION ?= 1s
BENCH_WORKERS  ?= 8
BENCH_DATASET  ?=
.PHONY: bench-envelope
bench-envelope:
	$(GOCMD) run ./cmd/bq-otel-testbench \
		-mode=$(BENCH_MODE) \
		-signals=traces,logs,metrics \
		-stream-pool-sweep=1,2,4,8,16 \
		-duration=$(BENCH_DURATION) \
		-batch-size=500 \
		-workers=$(BENCH_WORKERS) \
		-project="$(BQ_PROJECT)" \
		-dataset="$(BENCH_DATASET)" \
		-ephemeral-dataset=$(if $(and $(filter live,$(BENCH_MODE)),$(BQ_PROJECT),$(if $(BENCH_DATASET),,true)),true,false) \
		-format=markdown

