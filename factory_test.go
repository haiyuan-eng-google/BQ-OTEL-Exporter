// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"context"
	"errors"
	"slices"
	"testing"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
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
	if _, err := f.CreateMetrics(context.Background(), set, validConfig()); err != nil {
		t.Fatalf("CreateMetrics: %v", err)
	}
}

func TestExportedCreateExporterConstructors(t *testing.T) {
	var (
		_ exporter.CreateTracesFunc  = CreateTracesExporter
		_ exporter.CreateLogsFunc    = CreateLogsExporter
		_ exporter.CreateMetricsFunc = CreateMetricsExporter
	)

	set := exportertest.NewNopSettings(NewFactory().Type())
	ctx := context.Background()

	if cfg, ok := CreateDefaultConfig().(*Config); !ok || cfg == nil {
		t.Fatalf("CreateDefaultConfig() = %#v, want non-nil *Config", cfg)
	}

	if exp, err := CreateTracesExporter(ctx, set, validConfig()); err != nil || exp == nil {
		t.Fatalf("CreateTracesExporter(validConfig) = (%v, %v), want non-nil exporter", exp, err)
	}
	if exp, err := CreateLogsExporter(ctx, set, validConfig()); err != nil || exp == nil {
		t.Fatalf("CreateLogsExporter(validConfig) = (%v, %v), want non-nil exporter", exp, err)
	}
	if exp, err := CreateMetricsExporter(ctx, set, validConfig()); err != nil || exp == nil {
		t.Fatalf("CreateMetricsExporter(validConfig) = (%v, %v), want non-nil exporter", exp, err)
	}

	type wrongConfig struct {
		component.Config
	}
	var typedNil *Config
	for _, tc := range []struct {
		name string
		cfg  component.Config
	}{
		{"nil interface", nil},
		{"typed nil *Config", typedNil},
		{"wrong component.Config type", &wrongConfig{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := CreateTracesExporter(ctx, set, tc.cfg); !errors.Is(err, errUnexpectedConfigType) {
				t.Errorf("CreateTracesExporter(%s) error = %v, want errUnexpectedConfigType", tc.name, err)
			}
			if _, err := CreateLogsExporter(ctx, set, tc.cfg); !errors.Is(err, errUnexpectedConfigType) {
				t.Errorf("CreateLogsExporter(%s) error = %v, want errUnexpectedConfigType", tc.name, err)
			}
			if _, err := CreateMetricsExporter(ctx, set, tc.cfg); !errors.Is(err, errUnexpectedConfigType) {
				t.Errorf("CreateMetricsExporter(%s) error = %v, want errUnexpectedConfigType", tc.name, err)
			}
		})
	}

	if _, err := newTracesExporter(set.TelemetrySettings, nil); !errors.Is(err, errUnexpectedConfigType) {
		t.Errorf("newTracesExporter(nil) error = %v, want errUnexpectedConfigType", err)
	}
	if _, err := newLogsExporter(set.TelemetrySettings, nil); !errors.Is(err, errUnexpectedConfigType) {
		t.Errorf("newLogsExporter(nil) error = %v, want errUnexpectedConfigType", err)
	}
	if _, err := newMetricsExporter(set.TelemetrySettings, nil); !errors.Is(err, errUnexpectedConfigType) {
		t.Errorf("newMetricsExporter(nil) error = %v, want errUnexpectedConfigType", err)
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

func TestDefaultSignalAndQuarantineConfig(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	if cfg.Metrics.Table != "otel_metrics" {
		t.Fatalf("default metrics.table = %q, want otel_metrics", cfg.Metrics.Table)
	}
	if cfg.Quarantine.PartitionExpirationDays != 30 {
		t.Fatalf("default quarantine.partition_expiration_days = %d, want 30", cfg.Quarantine.PartitionExpirationDays)
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

// validRecordIndices names the records a retry payload may carry: everything
// sent that the service did not reject.
func TestValidRecordIndices(t *testing.T) {
	tests := []struct {
		name        string
		recordIndex []int
		rejected    []int
		want        []int
	}{
		{"nothing rejected", []int{0, 1, 3}, nil, []int{0, 1, 3}},
		{"one rejected", []int{0, 1, 3}, []int{1}, []int{0, 3}},
		{"every record rejected", []int{0, 1}, []int{0, 1}, []int{}},
		{"a record that was not sent changes nothing", []int{0, 2}, []int{1}, []int{0, 2}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var rowErrors []transform.QuarantineEntry
			for _, i := range tc.rejected {
				rowErrors = append(rowErrors, transform.QuarantineEntry{Rejection: transform.Rejection{Index: i}})
			}
			if got := validRecordIndices(tc.recordIndex, rowErrors); !slices.Equal(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}
