# Multi-arch builder for the BigQuery OTel Exporter custom Collector.
#
# Buildx sets TARGETOS and TARGETARCH automatically. The builder stage runs
# OCB to produce the collector binary and compiles the static /healthcheck
# probe binary, then copies both into a distroless nonroot runtime image.

ARG TARGETOS=linux
ARG TARGETARCH=amd64

FROM --platform=${TARGETOS}/${TARGETARCH} golang:1.26.9-bookworm@sha256:d9c68c2c51161e12fd77e4c6320687c9cd86e1af1e3ad6e6cd63ff970641453c AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN GOTOOLCHAIN=local go run go.opentelemetry.io/collector/cmd/builder@v0.158.0 \
    --config example/otelcol-builder.yaml
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" \
    -o /out/healthcheck ./cmd/healthcheck

FROM --platform=${TARGETOS}/${TARGETARCH} gcr.io/distroless/base-debian12:nonroot@sha256:7f0c72cd138b442ae0deeb69c08b1acf5525439ba251a49ad93c320a061567e5

# OCI standard image labels. GoReleaser overrides these at build time.
LABEL org.opencontainers.image.title="otelcol-bq"
LABEL org.opencontainers.image.description="OpenTelemetry Collector with the BigQuery exporter"
LABEL org.opencontainers.image.source="https://github.com/haiyuan-eng-google/BQ-OTEL-Exporter"
LABEL org.opencontainers.image.licenses="Apache-2.0"

COPY --from=builder /src/otelcol-bq-dist/otelcol-bq /otelcol-bq
COPY --from=builder /out/healthcheck /healthcheck
COPY example/otel-collector-config.yml /etc/otel/config.yml

EXPOSE 4317 4318 13133

# The distroless runtime has no shell, so the probe is the bundled static
# /healthcheck binary in exec form; it exits non-zero unless the health_check
# extension on :13133 answers 2xx.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD ["/healthcheck"]

ENTRYPOINT ["/otelcol-bq"]
CMD ["--config=/etc/otel/config.yml"]
