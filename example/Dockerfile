FROM golang:1.26.5-bookworm@sha256:53eeac89074db483fdf0ab3be1df32bf6e47562263d2d0d6baa7f26acb4957dd AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN GOTOOLCHAIN=local go run go.opentelemetry.io/collector/cmd/builder@v0.158.0 \
    --config example/otelcol-builder.yaml

FROM gcr.io/distroless/base-debian12:nonroot@sha256:b12529fbbd0bb15eea8905f69d83148679e0b4d7d434c8808100792029b1caae

COPY --from=builder /src/otelcol-bq-dist/otelcol-bq /otelcol-bq

EXPOSE 4317 4318
ENTRYPOINT ["/otelcol-bq"]
CMD ["--config=/etc/otel/config.yml"]
