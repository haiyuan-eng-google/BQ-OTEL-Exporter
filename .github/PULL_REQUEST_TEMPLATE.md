## What this changes

<!-- A short description of the change and the problem it solves. Link the
issue it addresses, if there is one. -->

## How it was tested

<!-- Commands you ran and what they showed. Unit tests are expected for any
behavior change. If you ran the live-BigQuery tests, say which. -->

## Checklist

- [ ] `make fmt vet lint test` passes locally.
- [ ] A change to the table layout, a configuration option or a metric updates the code, `README.md` and `metadata.yaml` together, and `docs/schema-rationale.md` when the reasoning changes.
- [ ] A schema change follows the versioning rules in `CONTRIBUTING.md`.
- [ ] No credentials, real telemetry or table data appear in the code, tests, fixtures or logs.
- [ ] New Go files carry the `Copyright` and `SPDX-License-Identifier` header.
- [ ] If any third-party implementation was consulted for behavior, it is named above, as `CONTRIBUTING.md` requires.
