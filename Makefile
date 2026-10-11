GOTEST ?= go test
GOCMD  ?= go
OCB_VERSION ?= v0.158.0
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

# Regenerate the files mdatagen derives from metadata.yaml.
#
# The mdatagen module carries replace directives, so `go run …@version` and
# `go install …@version` both refuse to build it. Adding it to this module with
# `go get -tool` would pull its whole dependency tree into go.mod. Instead it
# is built from a throwaway module pinned to the same Collector release as the
# builder, then run from the repository root.
.PHONY: mdatagen
mdatagen:
	@tmp="$$(mktemp -d)" && trap 'rm -rf "$$tmp"' EXIT && \
		( cd "$$tmp" && \
		  $(GOCMD) mod init mdatagen-tool 2>/dev/null && \
		  $(GOCMD) get -tool go.opentelemetry.io/collector/cmd/mdatagen@$(OCB_VERSION) && \
		  $(GOCMD) build -o "$$tmp/mdatagen" go.opentelemetry.io/collector/cmd/mdatagen ) && \
		"$$tmp/mdatagen" metadata.yaml

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

# Runs each fuzz target for FUZZTIME. Go fuzzes one target per invocation, so
# the targets of every package are discovered with `go test -list`. Packages
# without fuzz targets are skipped.
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
