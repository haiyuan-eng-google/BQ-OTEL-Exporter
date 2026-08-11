GOTEST ?= go test
GOCMD  ?= go

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

.PHONY: mdatagen
mdatagen:
	$(GOCMD) generate ./...
