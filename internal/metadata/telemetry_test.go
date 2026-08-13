// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package metadata

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestTelemetryRecordsEveryInstrument(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown meter provider: %v", err)
		}
	})

	tel, err := NewTelemetry(provider)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	tel.RecordAcknowledged(ctx, 5)
	tel.RecordRejected(ctx, "row_error", 2)
	tel.RecordRetry(ctx, "unavailable")
	tel.RecordUncertainAckReplay(ctx)
	tel.RecordInflightRequests(ctx, 3)
	tel.RecordInflightRequests(ctx, -1)
	tel.RecordUnresolvedResults(ctx, 2)
	tel.RecordUnresolvedResults(ctx, -1)
	tel.RecordAppendResultWait(ctx, 1500*time.Millisecond)
	tel.RecordTimeoutAfterDispatch(ctx)
	tel.RecordStreamRetirement(ctx)

	var resourceMetrics metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &resourceMetrics); err != nil {
		t.Fatal(err)
	}
	metrics := metricsByName(resourceMetrics)
	if len(metrics) != 9 {
		t.Fatalf("collected %d metrics, want 9: %v", len(metrics), metricNames(metrics))
	}

	assertIntSum(t, metrics[nameAcknowledgedRows], 5, nil)
	assertIntSum(t, metrics[nameRejectedRows], 2, []attribute.KeyValue{attribute.String(AttrReason, "row_error")})
	assertIntSum(t, metrics[nameRetries], 1, []attribute.KeyValue{attribute.String(AttrClass, "unavailable")})
	assertIntSum(t, metrics[nameUncertainAckReplays], 1, nil)
	assertIntSum(t, metrics[nameInflightRequests], 2, nil)
	assertIntSum(t, metrics[nameUnresolvedResults], 1, nil)
	assertIntSum(t, metrics[nameTimeoutsAfterDispatch], 1, nil)
	assertIntSum(t, metrics[nameStreamRetirements], 1, nil)

	histogram, ok := metrics[nameAppendResultWait].Data.(metricdata.Histogram[float64])
	if !ok || len(histogram.DataPoints) != 1 {
		t.Fatalf("append-result wait data = %#v, want one float64 histogram point", metrics[nameAppendResultWait].Data)
	}
	point := histogram.DataPoints[0]
	if point.Count != 1 || point.Sum != 1.5 || point.Attributes.Len() != 0 {
		t.Fatalf("append-result wait point = %#v, want count=1 sum=1.5 and no attributes", point)
	}
	wantBounds := []float64{0.001, 0.01, 0.1, 1, 5, 30, 70, 120}
	if !reflect.DeepEqual(point.Bounds, wantBounds) {
		t.Fatalf("append-result wait bounds = %v, want %v", point.Bounds, wantBounds)
	}
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
	tel.RecordInflightRequests(ctx, 1)
	tel.RecordUnresolvedResults(ctx, 1)
	tel.RecordAppendResultWait(ctx, time.Millisecond)
	tel.RecordTimeoutAfterDispatch(ctx)
	tel.RecordStreamRetirement(ctx)
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
		nameInflightRequests,
		nameUnresolvedResults,
		nameAppendResultWait,
		nameTimeoutsAfterDispatch,
		nameStreamRetirements,
	} {
		if !strings.Contains(doc, name+":") {
			t.Errorf("metric %q is emitted in code but not declared in metadata.yaml", name)
		}
	}
}

func metricsByName(resourceMetrics metricdata.ResourceMetrics) map[string]metricdata.Metrics {
	out := make(map[string]metricdata.Metrics)
	for _, scope := range resourceMetrics.ScopeMetrics {
		for _, metric := range scope.Metrics {
			out[metric.Name] = metric
		}
	}
	return out
}

func metricNames(metrics map[string]metricdata.Metrics) []string {
	names := make([]string, 0, len(metrics))
	for name := range metrics {
		names = append(names, name)
	}
	return names
}

func assertIntSum(t *testing.T, metric metricdata.Metrics, want int64, attrs []attribute.KeyValue) {
	t.Helper()
	sum, ok := metric.Data.(metricdata.Sum[int64])
	if !ok || len(sum.DataPoints) != 1 {
		t.Fatalf("%s data = %#v, want one int64 sum point", metric.Name, metric.Data)
	}
	point := sum.DataPoints[0]
	if point.Value != want {
		t.Errorf("%s value = %d, want %d", metric.Name, point.Value, want)
	}
	wantAttrs := attribute.NewSet(attrs...)
	if !point.Attributes.Equals(&wantAttrs) {
		t.Errorf("%s attributes = %v, want %v", metric.Name, point.Attributes.ToSlice(), attrs)
	}
}
