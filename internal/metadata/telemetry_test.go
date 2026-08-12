// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package metadata

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/metric/noop"
)

func TestNewTelemetry(t *testing.T) {
	tel, err := NewTelemetry(noop.NewMeterProvider())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	tel.RecordAcknowledged(ctx, 5)
	tel.RecordRejected(ctx, "row_error", 2)
	tel.RecordRetry(ctx, "unavailable")
	tel.RecordUncertainAckReplay(ctx)
}

// A nil Telemetry must be inert rather than panic: a metrics failure should
// never take down the export path.
func TestNilTelemetryIsSafe(t *testing.T) {
	var tel *Telemetry
	ctx := context.Background()
	tel.RecordAcknowledged(ctx, 1)
	tel.RecordRejected(ctx, "x", 1)
	tel.RecordRetry(ctx, "y")
	tel.RecordUncertainAckReplay(ctx)
}

// The instrument names in code must match the ones metadata.yaml documents.
// A mismatch means an operator builds a dashboard on a metric that is never
// emitted, and nothing fails loudly.
func TestMetricNamesMatchMetadataYAML(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "metadata.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	doc := string(raw)

	for _, name := range []string{
		nameAcknowledgedRows,
		nameRejectedRows,
		nameRetries,
		nameUncertainAckReplays,
	} {
		if !strings.Contains(doc, name+":") {
			t.Errorf("metric %q is emitted in code but not declared in metadata.yaml", name)
		}
	}
}
