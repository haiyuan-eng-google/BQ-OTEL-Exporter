# Contributing

## Provenance

This component is an **independent implementation**. It is not a fork or
adaptation of any existing OTLP-to-BigQuery exporter.

Contributors must not copy code from third-party exporters into this repository.
If a third-party implementation is consulted for behavioral reference, say so in
the pull request description, name the source, and describe what was observed
rather than what was copied. Reuse of Apache-2.0 licensed code is possible but
must be raised in the PR first so attribution and license headers are handled
correctly.

## Layout

The repository follows the OpenTelemetry Collector contrib component layout, so
that donating it upstream is a move rather than a rewrite:

```
config.go            exporter configuration and validation
factory.go           component factory and defaults
exporter_traces.go   traces pipeline
exporter_logs.go     logs pipeline
metadata.yaml        component metadata consumed by mdatagen
internal/bigquery    Storage Write API client wrapper and error classification
internal/schema      versioned table DDL and canonical dedup SQL
internal/metadata    generated component status
example/             runnable collector configuration
testdata/            configuration fixtures
```

## Development

```bash
make fmt vet test        # the standard loop
make integration-test    # live BigQuery; needs BQ_PROJECT, BQ_DATASET and ADC
```

CI runs build, vet, and unit tests on every pull request. Integration tests
against live BigQuery are not run on PRs from forks.

## Schema changes

The BigQuery table layout is a **versioned contract** that downstream queries
and dbt models pin against.

- Adding a nullable column is a minor version bump.
- Renaming, retyping, or removing a column is a major version bump and needs a
  migration note.
- Every schema change updates `internal/schema` and the README table together.
