// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

// Package bigqueryexporter exports OTLP traces and logs to BigQuery relational
// tables through the Storage Write API.
//
// The exporter has one job: reliable transport with a stable, documented
// schema. It does not aggregate, sample or interpret telemetry.
package bigqueryexporter // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter"
