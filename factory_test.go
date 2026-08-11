// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/exporter/exportertest"
)

func TestNewFactory(t *testing.T) {
	f := NewFactory()
	if got := f.Type().String(); got != "bigquery" {
		t.Fatalf("factory type = %q, want %q", got, "bigquery")
	}
}

func TestCreateExporters(t *testing.T) {
	cfg := validConfig()
	f := NewFactory()
	set := exportertest.NewNopSettings(f.Type())

	if _, err := f.CreateTraces(context.Background(), set, cfg); err != nil {
		t.Fatalf("CreateTraces: %v", err)
	}
	if _, err := f.CreateLogs(context.Background(), set, cfg); err != nil {
		t.Fatalf("CreateLogs: %v", err)
	}
}

// Metrics are out of scope for v1 (N1). Asserting the absence keeps the
// decision honest: if someone wires metrics up, this test makes them
// revisit the decision record rather than slip it in.
func TestMetricsNotSupported(t *testing.T) {
	f := NewFactory()
	set := exportertest.NewNopSettings(f.Type())
	if _, err := f.CreateMetrics(context.Background(), set, validConfig()); err == nil {
		t.Fatal("expected metrics creation to fail while N1 stands")
	}
}
