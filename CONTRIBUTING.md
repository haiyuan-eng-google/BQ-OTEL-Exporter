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
internal/metadata    generated component status and self-telemetry instruments
docs/                schema-rationale.md (why the schema and write path look as
                     they do) and framework compatibility notes
example/             runnable collector configuration
testdata/            configuration fixtures
```

## Development

You need the Go toolchain named in `go.mod` and two tools that CI also runs:

- [golangci-lint](https://golangci-lint.run/welcome/install/) v2, configured by
  `.golangci.yml`;
- [govulncheck](https://pkg.go.dev/golang.org/x/vuln/cmd/govulncheck), installed
  with `go install golang.org/x/vuln/cmd/govulncheck@latest`.

```bash
make fmt vet lint test   # the standard loop
make vuln                # govulncheck: known vulnerabilities reachable from this code
make tidy                # go mod tidy; CI fails if go.mod or go.sum would change
make bench               # every benchmark once, with allocation counts
make bench-compare       # benchstat: BASE_REF (default HEAD~1) vs the working tree
make fuzz                # every Fuzz target for FUZZTIME (default 30s) each
make integration-test    # live BigQuery; needs BQ_PROJECT, BQ_DATASET and ADC
make mdatagen            # regenerate the files derived from metadata.yaml
```

CI runs gofmt, build, vet (with and without the `integration` build tag),
`go mod tidy -diff`, `make mdatagen` followed by a clean-diff check,
golangci-lint, govulncheck and the unit tests on every pull request. The unit
tests run twice: once normally, and once with
`GOOGLE_API_USE_CLIENT_CERTIFICATE=true` to prove that they do not depend on the
credential and mTLS environment of the machine they run on.

No CI job runs the live-BigQuery integration tests, because they need real
credentials and create billable resources. If a change touches the write path or
the auto-create logic, run `make integration-test` against a scratch project and
say so in the pull request.

`make mdatagen` builds the `mdatagen` release that matches the Collector version
pinned in the Makefile, validates `metadata.yaml` against its schema, and
regenerates `internal/metadata/generated_status.go`. mdatagen also derives
telemetry code from the `telemetry` section; that output is validated and then
discarded, because `internal/metadata/telemetry.go` is hand-written.
`generated_package_test.go` is not regenerated either: it carries goleak options
that mdatagen cannot express. CI runs the target and fails on any resulting diff,
so keep the `telemetry` metrics and attributes in ASCII order and give every
metric a `stability`.

New Go files start with the same two-line header as the rest of the repository:

```go
// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0
```

## Schema changes

The BigQuery table layout is a **versioned contract** that downstream queries
and dbt models pin against.

- Adding a nullable column is a minor version bump.
- Renaming, retyping, or removing a column is a major version bump and needs a
  migration note.
- Every schema change updates `internal/schema` and the README table together.
- A change to the reasoning behind the layout, deduplication or delivery
  semantics is recorded in [`docs/schema-rationale.md`](docs/schema-rationale.md).
  Code comments point to that page, not to any document outside the repository.

## Security and conduct

Report suspected vulnerabilities privately, as described in
[SECURITY.md](SECURITY.md). Everyone taking part in the project is expected to
follow the [Code of Conduct](CODE_OF_CONDUCT.md).
