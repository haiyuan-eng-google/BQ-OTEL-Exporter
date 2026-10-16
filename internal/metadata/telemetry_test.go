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
	tel.RecordQuarantined(ctx, "row_error", 3)
	tel.RecordQuarantineFailed(ctx, "serialization", 1)
	tel.RecordAppendBytes(ctx, 4096)
	tel.RecordOffloadedPayloads(ctx, 2)
	tel.RecordOffloadedBytes(ctx, 8192)
	tel.RecordOffloadDedupHit(ctx, "spans", 3)
	tel.RecordOffloadDedupHit(ctx, "spans", 0) // ignored
	tel.RecordOffloadError(ctx, "unavailable", 1)
	tel.RecordRedactedValues(ctx, "spans", "key", 4)
	tel.RecordRedactedValues(ctx, "spans", "value", 2)
	tel.RecordRedactedValues(ctx, "spans", "key", 0) // ignored
	tel.RecordBackpressureLevel(ctx, "spans", "otel_spans", 0.5)
	tel.RecordEffectiveRequestBytes(ctx, "spans", "otel_spans", 4194304)
	tel.RecordEffectiveRequestBytes(ctx, "spans", "otel_spans", 0) // ignored
	tel.RecordBackpressureEvent(ctx, "spans", "otel_spans", "throttle")

	var resourceMetrics metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &resourceMetrics); err != nil {
		t.Fatal(err)
	}
	metrics := metricsByName(resourceMetrics)
	if len(metrics) != 20 {
		t.Fatalf("collected %d metrics, want 20: %v", len(metrics), metricNames(metrics))
	}

	assertIntSum(t, metrics[nameAcknowledgedRows], 5, nil)
	assertIntSum(t, metrics[nameRejectedRows], 2, []attribute.KeyValue{attribute.String(AttrReason, "row_error")})
	assertIntSum(t, metrics[nameRetries], 1, []attribute.KeyValue{attribute.String(AttrClass, "unavailable")})
	assertIntSum(t, metrics[nameUncertainAckReplays], 1, nil)
	assertIntSum(t, metrics[nameInflightRequests], 2, nil)
	assertIntSum(t, metrics[nameUnresolvedResults], 1, nil)
	assertIntSum(t, metrics[nameTimeoutsAfterDispatch], 1, nil)
	assertIntSum(t, metrics[nameStreamRetirements], 1, nil)
	assertIntSum(t, metrics[nameQuarantinedRows], 3, []attribute.KeyValue{attribute.String(AttrReason, "row_error")})
	assertIntSum(t, metrics[nameQuarantineFailedRows], 1, []attribute.KeyValue{attribute.String(AttrReason, "serialization")})
	assertIntSum(t, metrics[nameAppendBytes], 4096, nil)
	assertIntSum(t, metrics[nameOffloadedPayloads], 2, nil)
	assertIntSum(t, metrics[nameOffloadedBytes], 8192, nil)
	assertIntSum(t, metrics[nameOffloadDedupHits], 3, []attribute.KeyValue{attribute.String(AttrSignal, "spans")})
	assertIntSum(t, metrics[nameOffloadErrors], 1, []attribute.KeyValue{attribute.String(AttrClass, "unavailable")})
	assertFloatGauge(t, metrics[nameBackpressureLevel], 0.5, []attribute.KeyValue{
		attribute.String(AttrSignal, "spans"),
		attribute.String(AttrTable, "otel_spans"),
	})
	assertIntGauge(t, metrics[nameEffectiveRequestBytes], 4194304, []attribute.KeyValue{
		attribute.String(AttrSignal, "spans"),
		attribute.String(AttrTable, "otel_spans"),
	})
	assertIntSum(t, metrics[nameBackpressureEvents], 1, []attribute.KeyValue{
		attribute.String(AttrSignal, "spans"),
		attribute.String(AttrTable, "otel_spans"),
		attribute.String(AttrTrigger, "throttle"),
	})
	redacted, ok := metrics[nameRedactedValues].Data.(metricdata.Sum[int64])
	if !ok || len(redacted.DataPoints) != 2 || !redacted.IsMonotonic {
		t.Fatalf("redacted_values data = %#v, want a monotonic sum with one point per match type", metrics[nameRedactedValues].Data)
	}
	for _, dp := range redacted.DataPoints {
		mt, _ := dp.Attributes.Value(AttrMatchType)
		sig, _ := dp.Attributes.Value(AttrSignal)
		if sig.AsString() != "spans" {
			t.Errorf("redacted_values signal = %q, want spans", sig.AsString())
		}
		switch mt.AsString() {
		case "key":
			if dp.Value != 4 {
				t.Errorf("redacted_values{match_type=key} = %d, want 4", dp.Value)
			}
		case "value":
			if dp.Value != 2 {
				t.Errorf("redacted_values{match_type=value} = %d, want 2", dp.Value)
			}
		default:
			t.Errorf("unexpected match_type %q", mt.AsString())
		}
	}

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
func TestNilTelemetryIsSafe(_ *testing.T) {
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
	tel.RecordQuarantined(ctx, "x", 1)
	tel.RecordQuarantineFailed(ctx, "x", 1)
	tel.RecordAppendBytes(ctx, 1)
	tel.RecordOffloadedPayloads(ctx, 1)
	tel.RecordOffloadedBytes(ctx, 1)
	tel.RecordOffloadDedupHit(ctx, "spans", 1)
	tel.RecordOffloadError(ctx, "y", 1)
	tel.RecordRedactedValues(ctx, "spans", "key", 1)
	tel.RecordBackpressureLevel(ctx, "spans", "otel_spans", 0.5)
	tel.RecordEffectiveRequestBytes(ctx, "spans", "otel_spans", 1024)
	tel.RecordBackpressureEvent(ctx, "spans", "otel_spans", "throttle")
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
		nameQuarantinedRows,
		nameQuarantineFailedRows,
		nameAppendBytes,
		nameOffloadedPayloads,
		nameOffloadedBytes,
		nameOffloadDedupHits,
		nameOffloadErrors,
		nameRedactedValues,
		nameBackpressureLevel,
		nameEffectiveRequestBytes,
		nameBackpressureEvents,
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

func assertIntGauge(t *testing.T, metric metricdata.Metrics, want int64, attrs []attribute.KeyValue) {
	t.Helper()
	gauge, ok := metric.Data.(metricdata.Gauge[int64])
	if !ok || len(gauge.DataPoints) != 1 {
		t.Fatalf("%s data = %#v, want one int64 gauge point", metric.Name, metric.Data)
	}
	point := gauge.DataPoints[0]
	if point.Value != want {
		t.Errorf("%s value = %d, want %d", metric.Name, point.Value, want)
	}
	wantAttrs := attribute.NewSet(attrs...)
	if !point.Attributes.Equals(&wantAttrs) {
		t.Errorf("%s attributes = %v, want %v", metric.Name, point.Attributes.ToSlice(), attrs)
	}
}

func assertFloatGauge(t *testing.T, metric metricdata.Metrics, want float64, attrs []attribute.KeyValue) {
	t.Helper()
	gauge, ok := metric.Data.(metricdata.Gauge[float64])
	if !ok || len(gauge.DataPoints) != 1 {
		t.Fatalf("%s data = %#v, want one float64 gauge point", metric.Name, metric.Data)
	}
	point := gauge.DataPoints[0]
	if point.Value != want {
		t.Errorf("%s value = %v, want %v", metric.Name, point.Value, want)
	}
	wantAttrs := attribute.NewSet(attrs...)
	if !point.Attributes.Equals(&wantAttrs) {
		t.Errorf("%s attributes = %v, want %v", metric.Name, point.Attributes.ToSlice(), attrs)
	}
}
