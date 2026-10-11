# Multi-arch builder for the BigQuery OTel Exporter custom Collector.
#
# Buildx sets TARGETOS and TARGETARCH automatically. The builder stage runs
# OCB to produce the collector binary, then copies it into a distroless
# nonroot runtime image.

ARG TARGETOS=linux
ARG TARGETARCH=amd64

FROM --platform=${TARGETOS}/${TARGETARCH} golang:1.26.9-bookworm@sha256:d9c68c2c51161e12fd77e4c6320687c9cd86e1af1e3ad6e6cd63ff970641453c AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN GOTOOLCHAIN=local go run go.opentelemetry.io/collector/cmd/builder@v0.158.0 \
    --config example/otelcol-builder.yaml

FROM --platform=${TARGETOS}/${TARGETARCH} gcr.io/distroless/base-debian12:nonroot@sha256:b12529fbbd0bb15eea8905f69d83148679e0b4d7d434c8808100792029b1caae

# OCI standard image labels. GoReleaser overrides these at build time.
LABEL org.opencontainers.image.title="otelcol-bq"
LABEL org.opencontainers.image.description="OpenTelemetry Collector with the BigQuery exporter"
LABEL org.opencontainers.image.source="https://github.com/haiyuan-eng-google/BQ-OTEL-Exporter"
LABEL org.opencontainers.image.licenses="Apache-2.0"

COPY --from=builder /src/otelcol-bq-dist/otelcol-bq /otelcol-bq

EXPOSE 4317 4318 13133

# Health check: the health_check extension serves / on port 13133.
# In Docker Compose, use:
#   healthcheck:
#     test: ["CMD", "/otelcol-bq", "--help"]
#     interval: 30s
#     timeout: 5s
#     retries: 3
# The distroless image has no shell or curl, so the health check must use
# an external HTTP probe (e.g. docker-compose healthcheck with wget/curl
# in a sidecar, or Kubernetes livenessProbe on :13133).

ENTRYPOINT ["/otelcol-bq"]
CMD ["--config=/etc/otel/config.yml"]
