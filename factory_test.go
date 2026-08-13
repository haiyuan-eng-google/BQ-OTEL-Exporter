// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/exporter/exportertest"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/transform"
)

func TestNewFactory(t *testing.T) {
	if got := NewFactory().Type().String(); got != "bigquery" {
		t.Fatalf("factory type = %q, want %q", got, "bigquery")
	}
}

func TestCreateExporters(t *testing.T) {
	f := NewFactory()
	set := exportertest.NewNopSettings(f.Type())

	if _, err := f.CreateTraces(context.Background(), set, validConfig()); err != nil {
		t.Fatalf("CreateTraces: %v", err)
	}
	if _, err := f.CreateLogs(context.Background(), set, validConfig()); err != nil {
		t.Fatalf("CreateLogs: %v", err)
	}
}

func TestDefaultQueueConcurrencyFitsManagedWriterWindow(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	queue := cfg.QueueSettings.Get()
	if queue == nil {
		t.Fatal("default sending queue is disabled")
	}
	if queue.NumConsumers != cfg.Write.MaxInflightRequests {
		t.Fatalf("queue consumers = %d, managedwriter request slots = %d; defaults must not oversubscribe the stream",
			queue.NumConsumers, cfg.Write.MaxInflightRequests)
	}
}

// Metrics are out of scope for v1. Asserting the absence keeps the decision
// honest: if someone wires metrics up, this test makes them revisit the
// decision record rather than slip it in.
func TestMetricsNotSupported(t *testing.T) {
	f := NewFactory()
	set := exportertest.NewNopSettings(f.Type())
	if _, err := f.CreateMetrics(context.Background(), set, validConfig()); err == nil {
		t.Fatal("expected metrics creation to fail while metrics remain out of scope")
	}
}

// rowPositionToRecordIndex is what lets a retry payload be rebuilt from the
// original batch: row positions and record positions diverge as soon as
// anything is rejected.
func TestRowPositionToRecordIndex(t *testing.T) {
	tests := []struct {
		name     string
		rowCount int
		rejected []int
		want     []int
	}{
		{"nothing rejected", 3, nil, []int{0, 1, 2}},
		{"first rejected", 2, []int{0}, []int{1, 2}},
		{"middle rejected", 3, []int{1}, []int{0, 2, 3}},
		{"several rejected", 2, []int{0, 2}, []int{1, 3}},
		{"no rows survive", 0, []int{0}, []int{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var rejects []transform.Rejection
			for _, i := range tc.rejected {
				rejects = append(rejects, transform.Rejection{Index: i})
			}
			got := rowPositionToRecordIndex(tc.rowCount, rejects)
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got %v, want %v", got, tc.want)
				}
			}
		})
	}
}
