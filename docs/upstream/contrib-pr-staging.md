# Contrib donation: staged pull requests 1 and 2

Status: draft. Nothing on this page has been filed upstream. Filing, finding a
sponsor, and presenting to the Collector SIG are human steps, tracked in
[outreach-and-governance.md](outreach-and-governance.md).

This page stages the first two pull requests that donate this exporter to
[opentelemetry-collector-contrib](https://github.com/open-telemetry/opentelemetry-collector-contrib)
as `exporter/bigqueryexporter`. The split follows the donation steps in
contrib's `docs/new-components.md` (lines 187-225):

| Pull request | Content | Stability | Staged in |
| --- | --- | --- | --- |
| 1 | Structure: README, configuration, factory, and metadata. Creating an exporter returns an error. | development | this page |
| 2 | Implementation: the create functions delegate to this library at `v0.1.0`. | development | this page |
| 3 | Alpha promotion, `contrib` in `status.distributions`, and the `cmd/otelcontribcol` builder config. | alpha | [contrib-alpha-and-releases.md](contrib-alpha-and-releases.md) |

The donation issue must be accepted before pull request 1 is opened. It is
drafted in [new-component-issue-draft.md](new-component-issue-draft.md).

## Pinned references

The contrib files cited in `docs/upstream` are at commit
[`87eb65f5e96d9d70315ee401515c330c105d2a63`](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/87eb65f5e96d9d70315ee401515c330c105d2a63),
unless a citation says otherwise. `Makefile:663` is line 663 of the root
`Makefile` at that commit. Line numbers and generated lists drift, so re-pin
before filing.

## Before pull request 1 is opened

1. The library tag `v0.1.0` exists. Every staged `go.mod` requires
   `github.com/haiyuan-eng-google/BQ-OTEL-Exporter v0.1.0`, so no contrib gate
   can resolve the module until the tag is pushed.
2. The [go directive decision](#the-go-directive-decision) is made before
   that tag.
3. The donation issue is accepted and has a sponsor. Its number replaces
   `<donation-issue>` in the changelog entries; `chloggen validate` rejects
   the placeholder.
4. Confirmed GitHub handles replace `<sponsor>` and `<codeowner-3>` in
   `metadata.yaml`. `contribcheck.PendingCodeowners` lists the placeholders
   that remain.
5. The collector module versions in the staged `go.mod` files are aligned with
   the checkout (see [Risks](#risks)).

## Design of the wrapper

- **The configuration is a type alias.** `type Config = bqotel.Config` keeps
  every key, default, and validation rule in one place, the library. In an
  author-reported scratch run (2026-10-25, outside CI), a local struct that
  embedded the library configuration with `mapstructure:",squash"` failed the
  test that mdatagen v0.163.0 generates, with
  `exporter default config must define sending_queue`.
- **metadata.yaml declares the sending queue default.**
  `sending_queue.support: has_overrides` with `num_consumers: 8` and batching
  off repeats the library default: `createDefaultConfig` in this repository's
  `factory.go` sets `queue.NumConsumers = defaultMaxInflightRequests`, which
  is 8. The wrapper's `createDefaultConfig` assigns
  `metadata.NewDefaultSendingQueueConfig()`. In an author-reported scratch run
  (2026-10-25, outside CI), the generated `TestComponentDefaultQueueBatchSender`
  failed with the queue-batch feature gate enabled until it did.
- **Pull request 1 refuses to create exporters.** Every create function
  returns `errNotImplemented`, so a pipeline that names the exporter fails at
  startup instead of accepting telemetry and dropping it.
  `tests.skip_shutdown: true` keeps mdatagen from generating
  `TestComponentLifecycle`, whose subtests create each exporter and require no
  error. Pull request 2 removes the setting.
- **The generated start test is skipped.** `tests.skip_lifecycle: true`,
  because `Start` needs Google Cloud credentials and a reachable BigQuery API.
- **goleak ignores the opencensus stats worker**,
  `go.opencensus.io/stats/view.(*worker).start`. The metadata.yaml of
  `exporter/googlecloudexporter` carries the same entry. In an author-reported
  scratch run (2026-10-25, outside CI), the generated package test failed goleak
  without it.
- **`//go:generate make mdatagen` is in `doc.go`**, as in the `doc.go` example
  in `docs/new-components.md` (lines 174-183). `check-stability-mod`
  (Makefile.Common:310) looks for that string in a component's Go files, and
  the `mdatagen` target runs `$(MDATAGEN) $(MDATAGEN_METADATA_YAML)`
  (Makefile.Common:294-295, with `MDATAGEN_METADATA_YAML?= metadata.yaml` at
  line 291).

## The go directive decision

The staged `go.mod` files declare `go 1.26.0`. That is the go directive of the
contrib root `go.mod` and of `exporter/googlecloudexporter/go.mod`. The gotidy
recipe passes the root directive to `go mod tidy -compat` (Makefile:169, with
`GO_COMPAT_VERSION` at Makefile.Common:20).

This library's `go.mod` declares `go 1.26.5`. In an author-reported scratch
run (2026-10-25, outside CI), `go mod tidy` on each staged tree, with a local
`replace` to this repository, raised the wrapper's go line from 1.26.0 to
1.26.5 and changed no direct requirement. In this repository's build list, no
module except the library declares a go directive above 1.26.0
(`go list -m -f '{{.GoVersion}}' all`, Go 1.27.3).
Before tagging `v0.1.0`, choose one of these:

- Lower the library's go directive to 1.26.0, so that gotidy leaves the
  staged `go.mod` unchanged.
- Keep 1.26.5, commit the tidied `go.mod`, and change `ContribGoVersion` in
  `internal/contribcheck`. Reviewers may ask why one module requires a newer
  Go than the rest of contrib.

## How the staged files are checked

[`internal/contribcheck`](../../internal/contribcheck) reads the staged files
from this page and from
[contrib-alpha-and-releases.md](contrib-alpha-and-releases.md). A staged file
is a marker line directly followed by a fenced block that holds the whole
file. A README, which contains fenced blocks itself, is staged in a
four-backtick fence. The marker looks like this:

```text
<!-- contribcheck:file pr=1 path=exporter/bigqueryexporter/doc.go -->
```

`go test ./internal/contribcheck/` rebuilds the component directory that each
pull request leaves behind, where a later pull request replaces files of an
earlier one, and checks that:

- the ten files of pull request 1 exist, and each pull request stages exactly
  the files listed on these pages;
- each Go file that is not generated starts with the two-line OpenTelemetry
  license header and a blank line, and each one that is not a test has an
  import comment;
- the package root exports one function, `NewFactory() exporter.Factory`, and
  `doc.go` holds the only `//go:generate` directive;
- `go.mod` declares the contrib module path and `go 1.26.0`, requires the
  library at `v0.1.0`, and has no `toolchain` or `replace` line;
- the Makefile is `include ../../Makefile.Common` and the README starts with
  the mdatagen status section;
- `metadata.yaml` has the expected stability and distributions, three or more
  codeowners, the sending queue overrides, and the test settings above;
- pull request 3 changes `metadata.yaml` only in `status.stability` and
  `status.distributions`;
- every `exporters.bigquery` example in the staged READMEs decodes into this
  library's configuration and passes `confmap.Validate`;
- the gate table below matches `contribcheck.Gates`.

With `CONTRIBCHECK_MDATAGEN` set to an mdatagen binary, the test also runs it
on each tree. It checks that mdatagen leaves each staged README unchanged,
writes the expected stability levels, and that the checks above still pass on
the generated files.

These checks are preconditions of the contrib gates. A precondition that holds
here is not a gate that passed: the gates run only in a contrib checkout.

### Files the pull requests carry but this page does not stage

In an author-reported scratch run (2026-10-25, outside CI), mdatagen v0.163.0
wrote `documentation.md`, `generated_component_test.go`,
`generated_package_test.go`, `internal/metadata/generated_status.go`, and the
README status section. The gotidy recipe deletes and rewrites `go.sum`. Commit
what the tools write in the checkout instead of copying generated files from
here.

## Pull request 1: component structure

Suggested title: `[exporter/bigquery] Add the BigQuery exporter skeleton`

Description draft:

> Adds the structure of the BigQuery exporter, accepted in
> #&lt;donation-issue&gt;: the configuration, the factory, and the metadata at
> `development` stability. The configuration is the configuration of the
> [BQ-OTEL-Exporter](https://github.com/haiyuan-eng-google/BQ-OTEL-Exporter)
> library, which holds the implementation. In this pull request, creating an
> exporter returns an error, so a pipeline that names it fails at startup.
> The next pull request adds the export path.

### Files

<!-- contribcheck:file pr=1 path=exporter/bigqueryexporter/doc.go -->
```go
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate make mdatagen

// Package bigqueryexporter exports OTLP traces, logs, and metrics to BigQuery
// tables through the BigQuery Storage Write API.
package bigqueryexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/bigqueryexporter"
```

<!-- contribcheck:file pr=1 path=exporter/bigqueryexporter/metadata.yaml -->
```yaml
type: bigquery
display_name: BigQuery Exporter

status:
  class: exporter
  stability:
    development: [traces, logs, metrics]
  distributions: []
  codeowners:
    active: [haiyuan-eng-google, <sponsor>, <codeowner-3>]

sending_queue:
  support: has_overrides
  rationale: >-
    Consumer concurrency matches the in-flight request window of the BigQuery
    Storage Write API client; more consumers only add export attempts that
    wait on that window. Batching stays off, which is also the wrapped
    library's default while the pkg.exporterhelper.queueBatchEnabled feature
    gate is disabled.
  overrides:
    enabled: true
    num_consumers: 8
    batch:
      enabled: false

tests:
  config:
    project: my-project
    dataset: otel
  # Start needs Google Cloud credentials and a reachable BigQuery API.
  skip_lifecycle: true
  # The export path is not part of this component version, so every create
  # function returns an error on purpose.
  skip_shutdown: true
  goleak:
    ignore:
      top:
        # The BigQuery Storage Write API client links opencensus, whose stats
        # view worker starts at package init and runs for the process lifetime.
        - "go.opencensus.io/stats/view.(*worker).start"
```

<!-- contribcheck:file pr=1 path=exporter/bigqueryexporter/Makefile -->
```makefile
include ../../Makefile.Common
```

<!-- contribcheck:file pr=1 path=exporter/bigqueryexporter/go.mod -->
```text
module github.com/open-telemetry/opentelemetry-collector-contrib/exporter/bigqueryexporter

go 1.26.0

require (
	github.com/haiyuan-eng-google/BQ-OTEL-Exporter v0.1.0
	github.com/stretchr/testify v1.12.1
	go.opentelemetry.io/collector/component v1.69.0
	go.opentelemetry.io/collector/component/componenttest v0.163.0
	go.opentelemetry.io/collector/config/configoptional v1.69.0
	go.opentelemetry.io/collector/confmap v1.69.0
	go.opentelemetry.io/collector/exporter v1.69.0
	go.opentelemetry.io/collector/exporter/exporterhelper v0.163.0
	go.opentelemetry.io/collector/exporter/exportertest v0.163.0
	go.opentelemetry.io/collector/featuregate v1.69.0
	go.uber.org/goleak v1.3.0
)
```

<!-- contribcheck:file pr=1 path=exporter/bigqueryexporter/config.go -->
```go
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/bigqueryexporter"

import (
	bqotel "github.com/haiyuan-eng-google/BQ-OTEL-Exporter"
)

// Config defines the configuration of the BigQuery exporter.
//
// It is the configuration type of the library this component wraps, so every
// key, default, and validation rule is defined once, in that library. That
// includes the standard exporterhelper settings: timeout, sending_queue, and
// retry_on_failure.
type Config = bqotel.Config
```

<!-- contribcheck:file pr=1 path=exporter/bigqueryexporter/config_test.go -->
```go
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/confmaptest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/bigqueryexporter/internal/metadata"
)

func TestLoadConfig(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	tests := []struct {
		id          component.ID
		expected    component.Config
		errContains string
	}{
		{
			id: component.NewID(metadata.Type),
			expected: func() component.Config {
				cfg := createDefaultConfig().(*Config)
				cfg.Project = "my-project"
				cfg.Dataset = "otel"
				return cfg
			}(),
		},
		{
			id: component.NewIDWithName(metadata.Type, "customname"),
			expected: func() component.Config {
				cfg := createDefaultConfig().(*Config)
				cfg.Project = "my-project"
				cfg.Dataset = "otel_custom"
				cfg.TimeoutSettings.Timeout = 20 * time.Second
				cfg.Traces.Table = "spans"
				cfg.Logs.Table = "logs"
				cfg.Metrics.Table = "metrics"
				return cfg
			}(),
		},
		{
			id:          component.NewIDWithName(metadata.Type, "missing_dataset"),
			errContains: "dataset must be specified",
		},
	}

	for _, tt := range tests {
		t.Run(tt.id.String(), func(t *testing.T) {
			cfg := createDefaultConfig()
			sub, err := cm.Sub(tt.id.String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(cfg))

			if tt.errContains != "" {
				assert.ErrorContains(t, confmap.Validate(cfg), tt.errContains)
				return
			}
			require.NoError(t, confmap.Validate(cfg))
			assert.Equal(t, tt.expected, cfg)
		})
	}
}

// Project and dataset have no safe default, so the default configuration must
// fail validation instead of guessing a destination.
func TestDefaultConfigRequiresDestination(t *testing.T) {
	err := confmap.Validate(createDefaultConfig())
	assert.ErrorContains(t, err, "project must be specified")
	assert.ErrorContains(t, err, "dataset must be specified")
}
```

<!-- contribcheck:file pr=1 path=exporter/bigqueryexporter/factory.go -->
```go
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/bigqueryexporter"

import (
	"context"
	"errors"

	bqotel "github.com/haiyuan-eng-google/BQ-OTEL-Exporter"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/bigqueryexporter/internal/metadata"
)

// errNotImplemented is what every create function returns until the export
// path is added. Refusing to build an exporter is deliberate: a skeleton that
// accepted telemetry and discarded it would lose data without telling anyone.
var errNotImplemented = errors.New("the bigquery exporter cannot export yet: this component version has no export path")

// NewFactory creates a factory for the BigQuery exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithTraces(createTracesExporter, metadata.TracesStability),
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
		exporter.WithMetrics(createMetricsExporter, metadata.MetricsStability),
	)
}

// createDefaultConfig returns the wrapped library's defaults with the sending
// queue default that metadata.yaml declares.
func createDefaultConfig() component.Config {
	cfg := bqotel.NewFactory().CreateDefaultConfig().(*Config)
	cfg.QueueSettings = metadata.NewDefaultSendingQueueConfig()
	return cfg
}

func createTracesExporter(context.Context, exporter.Settings, component.Config) (exporter.Traces, error) {
	return nil, errNotImplemented
}

func createLogsExporter(context.Context, exporter.Settings, component.Config) (exporter.Logs, error) {
	return nil, errNotImplemented
}

func createMetricsExporter(context.Context, exporter.Settings, component.Config) (exporter.Metrics, error) {
	return nil, errNotImplemented
}
```

<!-- contribcheck:file pr=1 path=exporter/bigqueryexporter/factory_test.go -->
```go
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/bigqueryexporter/internal/metadata"
)

func TestCreateDefaultConfig(t *testing.T) {
	cfg := NewFactory().CreateDefaultConfig()
	require.NotNil(t, cfg)
	assert.NoError(t, componenttest.CheckConfigStruct(cfg))
	assert.Equal(t, metadata.NewDefaultSendingQueueConfig(), cfg.(*Config).QueueSettings)
}

func TestCreateExportersIsRefused(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.Project = "my-project"
	cfg.Dataset = "otel"
	set := exportertest.NewNopSettings(metadata.Type)

	_, err := factory.CreateTraces(t.Context(), set, cfg)
	assert.ErrorIs(t, err, errNotImplemented)
	_, err = factory.CreateLogs(t.Context(), set, cfg)
	assert.ErrorIs(t, err, errNotImplemented)
	_, err = factory.CreateMetrics(t.Context(), set, cfg)
	assert.ErrorIs(t, err, errNotImplemented)
}
```

<!-- contribcheck:file pr=1 path=exporter/bigqueryexporter/README.md -->
````markdown
<!-- status autogenerated section -->
# BigQuery Exporter
| Status        |           |
| ------------- |-----------|
| Stability     | [development]: traces, logs, metrics   |
| Distributions | [] |
| Issues        | [![Open issues](https://img.shields.io/github/issues-search/open-telemetry/opentelemetry-collector-contrib?query=is%3Aissue%20is%3Aopen%20label%3Aexporter%2Fbigquery%20&label=open&color=orange&logo=opentelemetry)](https://github.com/open-telemetry/opentelemetry-collector-contrib/issues?q=is%3Aopen+is%3Aissue+label%3Aexporter%2Fbigquery) [![Closed issues](https://img.shields.io/github/issues-search/open-telemetry/opentelemetry-collector-contrib?query=is%3Aissue%20is%3Aclosed%20label%3Aexporter%2Fbigquery%20&label=closed&color=blue&logo=opentelemetry)](https://github.com/open-telemetry/opentelemetry-collector-contrib/issues?q=is%3Aclosed+is%3Aissue+label%3Aexporter%2Fbigquery) |
| Code coverage | [![codecov](https://codecov.io/github/open-telemetry/opentelemetry-collector-contrib/graph/main/badge.svg?component=exporter_bigquery)](https://app.codecov.io/gh/open-telemetry/opentelemetry-collector-contrib/tree/main/?components%5B0%5D=exporter_bigquery&displayType=list) |
| [Code Owners](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/CONTRIBUTING.md#becoming-a-code-owner)    | [@haiyuan-eng-google](https://www.github.com/haiyuan-eng-google), [@<sponsor>](https://www.github.com/<sponsor>), [@<codeowner-3>](https://www.github.com/<codeowner-3>) |

[development]: https://github.com/open-telemetry/opentelemetry-collector/blob/main/docs/component-stability.md#development
<!-- end autogenerated section -->

The BigQuery exporter writes OTLP traces, logs, and metrics to BigQuery tables
through the [BigQuery Storage Write API](https://cloud.google.com/bigquery/docs/write-api).
Each signal goes to its own table with a documented, versioned schema, so the
data can be queried with SQL without a Cloud Logging sink, a Pub/Sub and
Dataflow pipeline, or batch file loads in between.

This component wraps the
[github.com/haiyuan-eng-google/BQ-OTEL-Exporter](https://github.com/haiyuan-eng-google/BQ-OTEL-Exporter)
library, which holds the implementation, the table schema contract, and their
tests. The configuration is the library's configuration.

> This version of the component registers the factory and the configuration
> only. Creating an exporter returns an error, so a pipeline that uses it fails
> at startup instead of discarding telemetry.

## Configuration

`project` and `dataset` are required; there is no default destination.

```yaml
exporters:
  bigquery:
    project: my-project
    dataset: otel
```

Each signal has its own table. The names default to `otel_spans`, `otel_logs`,
and `otel_metrics`:

```yaml
exporters:
  bigquery:
    project: my-project
    dataset: otel
    traces:
      table: otel_spans
    logs:
      table: otel_logs
    metrics:
      table: otel_metrics
```

The exporter also accepts the standard `timeout`, `sending_queue`, and
`retry_on_failure` settings. The sending queue defaults to 8 consumers, the
in-flight request window of the Storage Write API client, with batching off.

The full option reference covers credentials, dataset and table creation,
write tuning, structural limits, quarantine, payload offload to Cloud Storage,
and redaction. It is in the
[library README](https://github.com/haiyuan-eng-google/BQ-OTEL-Exporter/blob/v0.1.0/README.md#configuration).

## Delivery semantics

Delivery is at least once. The exporter appends to the Storage Write API
default stream, so a retried append can store the same record twice. The
library documents the read-time deduplication views for that case.
````

<!-- contribcheck:file pr=1 path=exporter/bigqueryexporter/testdata/config.yaml -->
```yaml
bigquery:
  project: my-project
  dataset: otel
bigquery/customname:
  project: my-project
  dataset: otel_custom
  timeout: 20s
  traces:
    table: spans
  logs:
    table: logs
  metrics:
    table: metrics
bigquery/missing_dataset:
  project: my-project
```

### Repository files outside the component

`docs/new-components.md` line 144 says to add the component to
`versions.yaml`. It belongs in the `contrib-base` module set
(versions.yaml:11), between lines 47 and 48:

```diff
       - github.com/open-telemetry/opentelemetry-collector-contrib/exporter/azuremonitorexporter
+      - github.com/open-telemetry/opentelemetry-collector-contrib/exporter/bigqueryexporter
       - github.com/open-telemetry/opentelemetry-collector-contrib/exporter/bmchelixexporter
```

The generator gates in the table below write other repository files, such as
`.github/component_labels.txt` (Makefile:127) and the CODEOWNERS and issue
template lists in `.github` (Makefile:443). Commit what they write.

### Changelog entry

`make chlog-new` (Makefile:479-480) creates `.chloggen/<branch>.yaml` from the
template. The entry, without the template comments:

```yaml
change_type: new_component
component: exporter/bigquery
note: Add the skeleton of the BigQuery exporter, which will write traces, logs, and metrics to BigQuery through the Storage Write API.
issues: [<donation-issue>]
subtext: |
  This version registers the factory and the configuration only. Creating an
  exporter returns an error, so a pipeline that uses it fails at startup.
change_logs: [user]
```

In an author-reported scratch run (2026-10-25, outside CI), chloggen v0.30.0
`validate` rejected this entry with the pinned `.chloggen/config.yaml`:
`exporter/bigquery is not a valid 'component'`. It passed once
`exporter/bigquery` was added to the `components` list of that file and
`<donation-issue>` was replaced by a number. The list is regenerated by
`make generate-chloggen-components` (Makefile:474-475:
`$(GITHUBGEN) $(GITHUBGEN_ARGS) chloggen-components`). Whether `make gengithub`
also refreshes it was not checked.

### Gates

`docs/new-components.md` (lines 195-211) lists the commands to run from the
repository root before submitting the first pull request. The first fifteen
rows are the gates that the v0.1.0 milestone issue names; the last row is in
the contrib list but not in the issue. "Static precondition verified locally"
means `contribcheck` checks a static precondition of the gate on the staged
trees during `go test ./internal/contribcheck/`. It does not mean the upstream
tool or gate ran in CI (where `CONTRIBCHECK_MDATAGEN` and upstream tools are
unset). Any "In an author-reported scratch run (2026-10-25, not run in CI)"
note in the last column records a one-off workstation check outside this
repository and is not re-verified by `go test ./...`. No gate was run in a
full contrib checkout.

| Gate | Recipe source | Recipe excerpt | Status | Local coverage, or why it needs a checkout |
| --- | --- | --- | --- | --- |
| `make checkdoc` | `Makefile:663` | `$(CHECKFILE) --project-path $(CURDIR) --component-rel-path $(COMP_REL_PATH) --module-name $(MOD_NAME) --file-name "README.md"` | static precondition verified locally | `README.md` exists and starts with the status section. `COMP_REL_PATH` is `cmd/otelcontribcol/components.go` (Makefile:12), and the comment above the target (Makefile:660) says it checks the READMEs of the default components of that collector. The component joins it in pull request 3. In an author-reported scratch run (2026-10-25, not run in CI), checkfile v0.30.0 exited 0 on all three trees with a hand-written `components.go` that imports the component, and panicked when `README.md` was deleted from the pull request 3 tree. |
| `make checkmetadata` | `Makefile:668` | `$(CHECKFILE) --project-path $(CURDIR) --component-rel-path $(COMP_REL_PATH) --module-name $(MOD_NAME) --file-name "metadata.yaml"` | static precondition verified locally | `metadata.yaml` exists and decodes strictly into the keys `contribcheck` models, with the checks listed above. Same `components.go` caveat as checkdoc (comment at Makefile:665). In an author-reported scratch run (2026-10-25, not run in CI), checkfile v0.30.0 exited 0 on all three trees and mdatagen v0.163.0 accepted each `metadata.yaml`. |
| `make checkapi` | `Makefile:672` | `$(CHECKAPI) -folder . -config .checkapi.yaml` | static precondition verified locally | The package root exports only `NewFactory() exporter.Factory`, the exporter entry of `.checkapi.yaml:61-64`. In an author-reported scratch run (2026-10-25, not run in CI), checkapi v0.30.0 exited 0 on all three trees; on the pull request 3 tree it reported an added exported function and a `NewFactory` that returns `any`. With `component_api_strict: true` it failed (see Risks). |
| `make goporto` | `Makefile:261` | `$(PORTO) -w --include-internal --skip-dirs "^cmd$$" ./` | static precondition verified locally | Each non-test Go file that is not generated has `package <name> // import "<module path>"`. In an author-reported scratch run (2026-10-25, not run in CI), porto v0.7.0 changed nothing on all three trees under a root `go.mod`, as in a checkout, and `porto -l` listed a file of the pull request 3 tree whose import comment was removed. |
| `make crosslink` | `Makefile:767` | `$(CROSSLINK) --root=$(shell pwd) --prune` | requires upstream checkout | It runs on the repository root. `docs/new-components.md` line 143 says it adds a `replace` directive to the `go.mod` of every intra-repository dependant. The staged `go.mod` files carry no `replace`; commit what crosslink writes. |
| `make tidylist` | `Makefile:157` | `$(CROSSLINK) tidylist` | requires upstream checkout | It runs in `internal/tidylist` with `--validate`, `--allow-circular allow-circular.txt`, two `--skip` flags, and `tidylist.txt` (Makefile:155-162). Both files are outside the component. |
| `make gotidy` | `Makefile:169` | `$(GOCMD) mod tidy -compat=$(GO_COMPAT_VERSION)` | static precondition verified locally | `go.mod` declares the module path and `go 1.26.0`, requires the library at `v0.1.0`, and has no `replace` and no `toolchain` line; the recipe runs `go get toolchain@none` (Makefile:170). In an author-reported scratch run (2026-10-25, not run in CI), `go mod tidy` with a local `replace` kept every direct requirement and raised the go line to 1.26.5 (see the go directive decision). The recipe loops over `internal/tidylist/tidylist.txt`, so the module must be listed there first. |
| `make genotelcontribcol` | `Makefile:501` | `$(BUILDER) --skip-compilation --config cmd/otelcontribcol/builder-config-replaced.yaml` | requires upstream checkout | It runs `./internal/buildscripts/ocb-add-replaces.sh otelcontribcol` first (Makefile:499) and reads the builder config of `cmd/otelcontribcol`. The component enters that config in pull request 3 (`docs/new-components.md` line 219). |
| `make generate` | `Makefile.Common:320` | `$(MAKE) for-all CMD="$(GOCMD) generate ./..."` | static precondition verified locally | `doc.go` holds the only `//go:generate make mdatagen`. In a component directory the target runs `go generate ./...`, `fmt`, and `gci` (Makefile.Common:327-329). In an author-reported scratch run (2026-10-25, not run in CI), `mdatagen metadata.yaml` (v0.163.0) exited 0 on all three trees and left each staged README unchanged; `go vet` and `go test -race` then passed with 100% statement coverage of the component package. |
| `make multimod-verify` | `Makefile:751` | `$(MULTIMOD) verify` | requires upstream checkout | It runs on the repository root, whose `versions.yaml` pull request 1 changes (diff above). |
| `make generate-gh-issue-templates` | `Makefile:790` | `$(GITHUBGEN) issue-templates` | requires upstream checkout | `docs/new-components.md` line 167 says it adds the component to the dropdown list in the issue templates, which are in `.github`. |
| `make gengithub` | `Makefile:446` | `$(GITHUBGEN) $(GITHUBGEN_ARGS)` | requires upstream checkout | `GITHUBGEN_ARGS ?= -skipgithub` (Makefile:441). The comment above the target (Makefile:443) says it updates CODEOWNERS and the issue template component lists in `.github`. |
| `make genlabels` | `Makefile:127` | `@echo "# This file is auto-generated. Do not edit manually." > .github/component_labels.txt` | requires upstream checkout | It writes `.github/component_labels.txt`. |
| `make gencodecov` | `Makefile:455` | `cd $(SRC_ROOT)/cmd/codecovgen && go run . --base-prefix github.com/open-telemetry/opentelemetry-collector-contrib` | requires upstream checkout | It runs `cmd/codecovgen` with `--dir $(SRC_ROOT)`, the repository root. |
| `make addlicense` | `Makefile.Common:124` | `ADDLICENSE_CMD := $(ADDLICENSE) -s=only -y "" -c "The OpenTelemetry Authors"` | static precondition verified locally | Each Go file that is not generated starts with `// Copyright The OpenTelemetry Authors`, `// SPDX-License-Identifier: Apache-2.0`, and a blank line. The target (Makefile.Common:244-254) applies `ADDLICENSE_CMD` to the `*.go` and `*.sh` files that `ALL_SRC_AND_SHELL_CMD` lists (Makefile.Common:127). In an author-reported scratch run (2026-10-25, not run in CI), addlicense v1.2.0 changed nothing on all three trees, `-check` passed, and it rewrote the `config.go` of the pull request 3 tree after its header was removed. |
| `make genoteltestbedcol` | `Makefile:519` | `$(BUILDER) --skip-compilation --config cmd/oteltestbedcol/builder-config-replaced.yaml` | requires upstream checkout | It reads the builder config of `cmd/oteltestbedcol`. Not in the milestone issue; `docs/new-components.md` line 204 lists it. |

## Pull request 2: implementation

Suggested title: `[exporter/bigquery] Export through the BQ-OTEL-Exporter library`

Description draft:

> Adds the export path of the BigQuery exporter (#&lt;donation-issue&gt;). The
> create functions delegate to the factory of the
> [BQ-OTEL-Exporter](https://github.com/haiyuan-eng-google/BQ-OTEL-Exporter)
> library at `v0.1.0`, which owns the Storage Write API client, the table
> schemas, and the timeout, queue, and retry settings, following the pattern
> of `exporter/googlecloudexporter`. Creating an exporter needs no
> credentials; `Start` does, so the generated start test stays skipped.

Pull request 2 replaces five files and keeps the other five of pull request 1.

### Files

<!-- contribcheck:file pr=2 path=exporter/bigqueryexporter/factory.go -->
```go
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/bigqueryexporter"

import (
	"context"

	bqotel "github.com/haiyuan-eng-google/BQ-OTEL-Exporter"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/bigqueryexporter/internal/metadata"
)

// NewFactory creates a factory for the BigQuery exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithTraces(createTracesExporter, metadata.TracesStability),
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
		exporter.WithMetrics(createMetricsExporter, metadata.MetricsStability),
	)
}

// createDefaultConfig returns the wrapped library's defaults with the sending
// queue default that metadata.yaml declares.
func createDefaultConfig() component.Config {
	cfg := bqotel.NewFactory().CreateDefaultConfig().(*Config)
	cfg.QueueSettings = metadata.NewDefaultSendingQueueConfig()
	return cfg
}

// The create functions delegate to the wrapped library's factory, which owns
// the export path: the Storage Write API client, the table schema, and the
// exporterhelper timeout, queue, and retry settings carried by cfg. Both
// factories use the component type "bigquery", so the settings pass the
// library factory's component type check unchanged.

func createTracesExporter(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Traces, error) {
	return bqotel.NewFactory().CreateTraces(ctx, set, cfg)
}

func createLogsExporter(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Logs, error) {
	return bqotel.NewFactory().CreateLogs(ctx, set, cfg)
}

func createMetricsExporter(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Metrics, error) {
	return bqotel.NewFactory().CreateMetrics(ctx, set, cfg)
}
```

<!-- contribcheck:file pr=2 path=exporter/bigqueryexporter/factory_test.go -->
```go
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/bigqueryexporter/internal/metadata"
)

func TestCreateDefaultConfig(t *testing.T) {
	cfg := NewFactory().CreateDefaultConfig()
	require.NotNil(t, cfg)
	assert.NoError(t, componenttest.CheckConfigStruct(cfg))
	assert.Equal(t, metadata.NewDefaultSendingQueueConfig(), cfg.(*Config).QueueSettings)
}

// Creating an exporter needs neither credentials nor the network, and an
// exporter that was created but never started shuts down cleanly.
func TestCreateExporters(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.Project = "my-project"
	cfg.Dataset = "otel"
	set := exportertest.NewNopSettings(metadata.Type)

	traces, err := factory.CreateTraces(t.Context(), set, cfg)
	require.NoError(t, err)
	assert.NoError(t, traces.Shutdown(t.Context()))

	logs, err := factory.CreateLogs(t.Context(), set, cfg)
	require.NoError(t, err)
	assert.NoError(t, logs.Shutdown(t.Context()))

	metrics, err := factory.CreateMetrics(t.Context(), set, cfg)
	require.NoError(t, err)
	assert.NoError(t, metrics.Shutdown(t.Context()))
}
```

`tests.skip_shutdown` is removed, so mdatagen generates the
create-and-shut-down test:

<!-- contribcheck:file pr=2 path=exporter/bigqueryexporter/metadata.yaml -->
```yaml
type: bigquery
display_name: BigQuery Exporter

status:
  class: exporter
  stability:
    development: [traces, logs, metrics]
  distributions: []
  codeowners:
    active: [haiyuan-eng-google, <sponsor>, <codeowner-3>]

sending_queue:
  support: has_overrides
  rationale: >-
    Consumer concurrency matches the in-flight request window of the BigQuery
    Storage Write API client; more consumers only add export attempts that
    wait on that window. Batching stays off, which is also the wrapped
    library's default while the pkg.exporterhelper.queueBatchEnabled feature
    gate is disabled.
  overrides:
    enabled: true
    num_consumers: 8
    batch:
      enabled: false

tests:
  config:
    project: my-project
    dataset: otel
  # Start needs Google Cloud credentials and a reachable BigQuery API.
  skip_lifecycle: true
  goleak:
    ignore:
      top:
        # The BigQuery Storage Write API client links opencensus, whose stats
        # view worker starts at package init and runs for the process lifetime.
        - "go.opencensus.io/stats/view.(*worker).start"
```

The README replaces the note that creating an exporter fails:

<!-- contribcheck:file pr=2 path=exporter/bigqueryexporter/README.md -->
````markdown
<!-- status autogenerated section -->
# BigQuery Exporter
| Status        |           |
| ------------- |-----------|
| Stability     | [development]: traces, logs, metrics   |
| Distributions | [] |
| Issues        | [![Open issues](https://img.shields.io/github/issues-search/open-telemetry/opentelemetry-collector-contrib?query=is%3Aissue%20is%3Aopen%20label%3Aexporter%2Fbigquery%20&label=open&color=orange&logo=opentelemetry)](https://github.com/open-telemetry/opentelemetry-collector-contrib/issues?q=is%3Aopen+is%3Aissue+label%3Aexporter%2Fbigquery) [![Closed issues](https://img.shields.io/github/issues-search/open-telemetry/opentelemetry-collector-contrib?query=is%3Aissue%20is%3Aclosed%20label%3Aexporter%2Fbigquery%20&label=closed&color=blue&logo=opentelemetry)](https://github.com/open-telemetry/opentelemetry-collector-contrib/issues?q=is%3Aclosed+is%3Aissue+label%3Aexporter%2Fbigquery) |
| Code coverage | [![codecov](https://codecov.io/github/open-telemetry/opentelemetry-collector-contrib/graph/main/badge.svg?component=exporter_bigquery)](https://app.codecov.io/gh/open-telemetry/opentelemetry-collector-contrib/tree/main/?components%5B0%5D=exporter_bigquery&displayType=list) |
| [Code Owners](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/CONTRIBUTING.md#becoming-a-code-owner)    | [@haiyuan-eng-google](https://www.github.com/haiyuan-eng-google), [@<sponsor>](https://www.github.com/<sponsor>), [@<codeowner-3>](https://www.github.com/<codeowner-3>) |

[development]: https://github.com/open-telemetry/opentelemetry-collector/blob/main/docs/component-stability.md#development
<!-- end autogenerated section -->

The BigQuery exporter writes OTLP traces, logs, and metrics to BigQuery tables
through the [BigQuery Storage Write API](https://cloud.google.com/bigquery/docs/write-api).
Each signal goes to its own table with a documented, versioned schema, so the
data can be queried with SQL without a Cloud Logging sink, a Pub/Sub and
Dataflow pipeline, or batch file loads in between.

This component wraps the
[github.com/haiyuan-eng-google/BQ-OTEL-Exporter](https://github.com/haiyuan-eng-google/BQ-OTEL-Exporter)
library, which holds the implementation, the table schema contract, and their
tests. The configuration is the library's configuration.

> This component is in development and is not part of any collector
> distribution yet. To try it, build a collector that includes it with the
> [OpenTelemetry Collector Builder](https://opentelemetry.io/docs/collector/custom-collector/).

## Configuration

`project` and `dataset` are required; there is no default destination.

```yaml
exporters:
  bigquery:
    project: my-project
    dataset: otel
```

Each signal has its own table. The names default to `otel_spans`, `otel_logs`,
and `otel_metrics`:

```yaml
exporters:
  bigquery:
    project: my-project
    dataset: otel
    traces:
      table: otel_spans
    logs:
      table: otel_logs
    metrics:
      table: otel_metrics
```

The exporter also accepts the standard `timeout`, `sending_queue`, and
`retry_on_failure` settings. The sending queue defaults to 8 consumers, the
in-flight request window of the Storage Write API client, with batching off.

The full option reference covers credentials, dataset and table creation,
write tuning, structural limits, quarantine, payload offload to Cloud Storage,
and redaction. It is in the
[library README](https://github.com/haiyuan-eng-google/BQ-OTEL-Exporter/blob/v0.1.0/README.md#configuration).

## Delivery semantics

Delivery is at least once. The exporter appends to the Storage Write API
default stream, so a retried append can store the same record twice. The
library documents the read-time deduplication views for that case.
````

`go.opentelemetry.io/collector/pdata` is added because the generated
`TestComponentLifecycle` imports it:

<!-- contribcheck:file pr=2 path=exporter/bigqueryexporter/go.mod -->
```text
module github.com/open-telemetry/opentelemetry-collector-contrib/exporter/bigqueryexporter

go 1.26.0

require (
	github.com/haiyuan-eng-google/BQ-OTEL-Exporter v0.1.0
	github.com/stretchr/testify v1.12.1
	go.opentelemetry.io/collector/component v1.69.0
	go.opentelemetry.io/collector/component/componenttest v0.163.0
	go.opentelemetry.io/collector/config/configoptional v1.69.0
	go.opentelemetry.io/collector/confmap v1.69.0
	go.opentelemetry.io/collector/exporter v1.69.0
	go.opentelemetry.io/collector/exporter/exporterhelper v0.163.0
	go.opentelemetry.io/collector/exporter/exportertest v0.163.0
	go.opentelemetry.io/collector/featuregate v1.69.0
	go.opentelemetry.io/collector/pdata v1.69.0
	go.uber.org/goleak v1.3.0
)
```

### Changelog entry

```yaml
change_type: enhancement
component: exporter/bigquery
note: Export traces, logs, and metrics to BigQuery through the Storage Write API.
issues: [<donation-issue>]
subtext: |
  The exporter wraps the github.com/haiyuan-eng-google/BQ-OTEL-Exporter v0.1.0
  library, which defines the configuration, the table schemas, and the export path.
change_logs: [user]
```

### Gates

The same sixteen gates apply, with the same status. Rerun all of them: pull
request 2 changes `go.mod` and the generated tests.

## Risks

- **checkapi passes only because the component API check is not strict.**
  `.checkapi.yaml` sets `component_api: true` (line 98). In an author-reported
  scratch run (2026-10-25, outside CI), checkapi v0.30.0 with
  `component_api_strict: true` failed with `cannot find the config struct`: the
  type alias is not a struct of the component package. Reviewers may ask for a
  local struct, but a local struct that embeds the library configuration with
  `squash` failed the generated sending queue test (see the design notes).
- **Collector module versions.** The staged `go.mod` files require the
  collector modules at the versions this library uses (v1.69.0 and v0.163.0).
  Contrib modules at the pinned commit require pseudo-versions, for example
  `go.opentelemetry.io/collector/exporter v1.69.1-0.20261022201525-42ee5d39b18f`
  (`exporter/googlecloudexporter/go.mod` line 13). Align with the checkout at
  filing time. The library then builds against versions that this repository
  does not test.
- **Generator version.** Only mdatagen v0.163.0 was run. Contrib's tools
  module pins `go.opentelemetry.io/collector/cmd/mdatagen
  v0.163.1-0.20261022201525-42ee5d39b18f` (`internal/tools/go.mod` line 295),
  whose output was not checked.
- **Linter version.** gci and gofumpt were checked with golangci-lint v2.14.0,
  this repository's version. Contrib's tools module pins v2.13.2
  (`internal/tools/go.mod` line 136).
- **Self-telemetry scope.** The library records its own metrics under its own
  instrumentation scope (`ScopeName` in this repository's
  `internal/metadata/generated_status.go`), not under the scope of the contrib
  component.
- **Library defaults.** `metadata.yaml` fixes 8 consumers and batching off. If
  a later library version changes its defaults, the wrapper keeps these values
  until `metadata.yaml` changes too.
- **Breaking changes.** `docs/new-components.md` (lines 114-117) says
  codeowners must update or review the component when unstable Collector
  interfaces change, or the component is excluded from the default builds.
  Each such change can need a library release followed by a contrib update.

## How this staging was verified

In a standalone checkout of this repository, `go test ./internal/contribcheck/...`
verifies the markdown structure, fenced-file layout, `metadata.yaml` schema, and
upstream checklist invariants; `TestMdatagen` skips unless `CONTRIBCHECK_MDATAGEN`
points to an `mdatagen` binary, and the fenced wrapper files in this document are
staged templates rather than Go packages compiled by the root module.

The additional checks below are **author-reported** observations from a one-off
scratch directory outside the repository on 2026-10-25 (not reproduced by a
standalone `go test ./...` run in this repository and not results of the
upstream contrib CI gates):

- **Trees.** For each pull request, the staged files were overlaid in order,
  then `mdatagen metadata.yaml` (v0.163.0) ran, a `replace` pointed the
  library at this repository, and `go mod tidy`, `go vet ./...`, and
  `go test -race -cover ./...` ran with Go 1.27.3. Every step passed for all
  three trees. Statement coverage of the component package was 100%; mdatagen
  left every staged README unchanged.
- **Tests that ran.** Pull request 1: `TestLoadConfig`,
  `TestDefaultConfigRequiresDestination`, `TestCreateDefaultConfig`,
  `TestCreateExportersIsRefused`, and the generated factory, config, and
  queue tests. Pull requests 2 and 3: `TestCreateExporters` instead of
  `TestCreateExportersIsRefused`, plus the generated `TestComponentLifecycle`
  shutdown subtests for logs, metrics, and traces.
- **Upstream tools.** On a contrib-like root that held one tree, a root
  `go.mod`, and the pinned `.checkapi.yaml` and `.golangci.yml`: checkapi
  v0.30.0, porto v0.7.0, addlicense v1.2.0 (both modes), checkfile v0.30.0
  with a hand-written `cmd/otelcontribcol/components.go`, and golangci-lint
  v2.14.0 `fmt --diff` and `run` exited 0 or changed nothing on all three
  trees. The tools ran on the generated tree, whose `go.mod` had the
  `replace` and the tidied go line.
- **Negative probes.** On the pull request 3 tree, each mutation made its
  tool report a problem: an added exported function and a `NewFactory` that
  returns `any` (checkapi), a removed import comment (`porto -l`), a removed
  license header (addlicense rewrote the file), a deleted README (checkfile
  panicked), and merged import groups (`golangci-lint fmt --diff`).
- **Not run:** crosslink, tidylist, multimod, githubgen, the collector
  builder, codecovgen, and every make target in a contrib checkout.
