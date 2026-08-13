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

.PHONY: test
test:
	$(GOTEST) -race -cover ./...

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

.PHONY: mdatagen
mdatagen:
	$(GOCMD) generate ./...
