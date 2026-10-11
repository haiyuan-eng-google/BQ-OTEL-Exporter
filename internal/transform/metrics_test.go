// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package transform

import (
	"math"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
)

func TestMetricRowsAllFiveTypes(t *testing.T) {
	fixed := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	opts := Options{
		Limits: DefaultLimits(),
		Now:    func() time.Time { return fixed },
	}

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "genai-worker")
	rm.SetSchemaUrl("https://opentelemetry.io/schemas/1.40.0")
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("otel.genai")
	sm.Scope().SetVersion("1.0.0")
	sm.SetSchemaUrl("https://opentelemetry.io/schemas/1.40.0")

	// 1. Gauge (double + exemplar)
	mGauge := sm.Metrics().AppendEmpty()
	mGauge.SetName("gen_ai.Server.active_requests")
	mGauge.SetDescription("Active GenAI requests")
	mGauge.SetUnit("{request}")
	mGauge.Metadata().PutStr("prometheus.type", "gauge")
	gdp := mGauge.SetEmptyGauge().DataPoints().AppendEmpty()
	gdp.SetTimestamp(pcommon.Timestamp(1_700_000_001_000_000_000))
	gdp.SetDoubleValue(12.5)
	gdp.Attributes().PutStr("gen_ai.provider.name", "gcp.vertex_ai")
	ex := gdp.Exemplars().AppendEmpty()
	ex.SetTimestamp(pcommon.Timestamp(1_700_000_001_000_000_000))
	ex.SetDoubleValue(12.5)
	ex.SetTraceID(pcommon.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	ex.SetSpanID(pcommon.SpanID{1, 2, 3, 4, 5, 6, 7, 8})
	ex.FilteredAttributes().PutStr("region", "us-central1")

	// 2. Sum (int64, cumulative, monotonic)
	mSum := sm.Metrics().AppendEmpty()
	mSum.SetName("gen_ai.client.token.usage")
	mSum.SetUnit("{token}")
	sum := mSum.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	sum.SetIsMonotonic(true)
	sdp := sum.DataPoints().AppendEmpty()
	sdp.SetStartTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000))
	sdp.SetTimestamp(pcommon.Timestamp(1_700_000_002_000_000_000))
	sdp.SetIntValue(4096)
	sdp.Attributes().PutStr("gen_ai.token.type", "input")

	// 3. Histogram
	mHist := sm.Metrics().AppendEmpty()
	mHist.SetName("gen_ai.client.operation.duration")
	mHist.SetUnit("s")
	hist := mHist.SetEmptyHistogram()
	hist.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	hdp := hist.DataPoints().AppendEmpty()
	hdp.SetStartTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000))
	hdp.SetTimestamp(pcommon.Timestamp(1_700_000_003_000_000_000))
	hdp.SetCount(10)
	hdp.SetSum(4.5)
	hdp.SetMin(0.1)
	hdp.SetMax(1.2)
	hdp.ExplicitBounds().FromRaw([]float64{0.25, 0.5, 1.0})
	hdp.BucketCounts().FromRaw([]uint64{3, 4, 2, 1})

	// 4. ExponentialHistogram
	mExp := sm.Metrics().AppendEmpty()
	mExp.SetName("gen_ai.Server.time_to_first_token")
	mExp.SetUnit("s")
	exp := mExp.SetEmptyExponentialHistogram()
	exp.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	edp := exp.DataPoints().AppendEmpty()
	edp.SetTimestamp(pcommon.Timestamp(1_700_000_004_000_000_000))
	edp.SetCount(5)
	edp.SetSum(1.8)
	edp.SetScale(3)
	edp.SetZeroCount(1)
	edp.SetZeroThreshold(0.001)
	edp.Positive().SetOffset(2)
	edp.Positive().BucketCounts().FromRaw([]uint64{2, 2})

	// 5. Summary
	mSumy := sm.Metrics().AppendEmpty()
	mSumy.SetName("rpc.client.duration")
	sumyDP := mSumy.SetEmptySummary().DataPoints().AppendEmpty()
	sumyDP.SetTimestamp(pcommon.Timestamp(1_700_000_005_000_000_000))
	sumyDP.SetCount(100)
	sumyDP.SetSum(55.0)
	qv := sumyDP.QuantileValues().AppendEmpty()
	qv.SetQuantile(0.95)
	qv.SetValue(1.1)

	rows, rejects := MetricRows(md, opts)
	if len(rejects) != 0 {
		t.Fatalf("unexpected rejections: %+v", rejects)
	}
	if len(rows) != 5 {
		t.Fatalf("len(rows) = %d, want 5", len(rows))
	}

	// Verify Gauge row
	if rows[0]["metric_type"] != MetricTypeGauge || rows[0]["value_type"] != ScalarValueTypeDouble || rows[0]["value_double"] != 12.5 {
		t.Errorf("gauge row = %+v", rows[0])
	}
	if rows[0]["metric_metadata"] != `{"prometheus.type":"gauge"}` {
		t.Errorf("gauge metric_metadata = %v", rows[0]["metric_metadata"])
	}
	exRows, ok := rows[0]["exemplars"].([]Row)
	if !ok || len(exRows) != 1 || exRows[0]["trace_id"] != "0102030405060708090a0b0c0d0e0f10" {
		t.Errorf("gauge exemplars = %+v", rows[0]["exemplars"])
	}

	// Verify Sum row
	if rows[1]["metric_type"] != MetricTypeSum || rows[1]["value_type"] != ScalarValueTypeInt64 || rows[1]["value_int64"] != int64(4096) {
		t.Errorf("sum row = %+v", rows[1])
	}
	if rows[1]["aggregation_temporality"] != TemporalityCumulative || rows[1]["is_monotonic"] != true {
		t.Errorf("sum temporality/monotonic = %+v", rows[1])
	}

	// Verify Histogram row
	if rows[2]["metric_type"] != MetricTypeHistogram || rows[2]["count"] != int64(10) || rows[2]["sum"] != 4.5 {
		t.Errorf("histogram row = %+v", rows[2])
	}
	if bounds := rows[2]["explicit_bounds"].([]float64); len(bounds) != 3 {
		t.Errorf("explicit_bounds = %v", bounds)
	}
	if buckets := rows[2]["bucket_counts"].([]int64); len(buckets) != 4 {
		t.Errorf("bucket_counts = %v", buckets)
	}

	// Verify ExponentialHistogram row
	if rows[3]["metric_type"] != MetricTypeExponentialHistogram || rows[3]["exp_scale"] != int64(3) || rows[3]["exp_zero_count"] != int64(1) {
		t.Errorf("exp histogram row = %+v", rows[3])
	}
	pos, ok := rows[3]["exp_positive"].(Row)
	if !ok || pos["offset"] != int64(2) {
		t.Errorf("exp_positive = %+v", rows[3]["exp_positive"])
	}

	// Verify Summary row
	if rows[4]["metric_type"] != MetricTypeSummary || rows[4]["count"] != int64(100) {
		t.Errorf("summary row = %+v", rows[4])
	}
	qRows, ok := rows[4]["summary_quantiles"].([]Row)
	if !ok || len(qRows) != 1 || qRows[0]["quantile"] != 0.95 || qRows[0]["value"] != 1.1 {
		t.Errorf("summary_quantiles = %+v", rows[4]["summary_quantiles"])
	}

	for i, r := range rows {
		fp, ok := r["point_fingerprint"].(string)
		if !ok || !strings.HasPrefix(fp, schema.FingerprintPrefix) {
			t.Errorf("row[%d] point_fingerprint = %q, want fp1:*", i, fp)
		}
		if r["schema_version"] != schema.Version {
			t.Errorf("row[%d] schema_version = %v, want %q", i, r["schema_version"], schema.Version)
		}
	}
}

func TestMetricRejectionsAndQuarantine(t *testing.T) {
	fixed := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	opts := Options{
		Limits:           DefaultLimits(),
		ExporterInstance: "collector-1",
		MaxRowBytes:      1 << 20,
		Now:              func() time.Time { return fixed },
	}

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "svc")
	sm := rm.ScopeMetrics().AppendEmpty()

	// idx 0: empty metric name -> INVALID_ID
	m0 := sm.Metrics().AppendEmpty()
	dp0 := m0.SetEmptyGauge().DataPoints().AppendEmpty()
	dp0.SetTimestamp(1_700_000_000_000_000_000)
	dp0.SetDoubleValue(1.0)

	// idx 1: missing timestamp -> MISSING_TIMESTAMP
	m1 := sm.Metrics().AppendEmpty()
	m1.SetName("m.missing_ts")
	dp1 := m1.SetEmptyGauge().DataPoints().AppendEmpty()
	dp1.SetDoubleValue(1.0)

	// idx 2: overflowing timestamp -> TIMESTAMP_OVERFLOW
	m2 := sm.Metrics().AppendEmpty()
	m2.SetName("m.overflow_ts")
	dp2 := m2.SetEmptyGauge().DataPoints().AppendEmpty()
	dp2.SetTimestamp(pcommon.Timestamp(1 << 63))
	dp2.SetDoubleValue(1.0)

	// idx 3: NaN double without NoRecordedValue flag -> SERIALIZATION
	m3 := sm.Metrics().AppendEmpty()
	m3.SetName("m.nan")
	dp3 := m3.SetEmptyGauge().DataPoints().AppendEmpty()
	dp3.SetTimestamp(1_700_000_000_000_000_000)
	dp3.SetDoubleValue(math.NaN())

	// idx 4: valid point
	m4 := sm.Metrics().AppendEmpty()
	m4.SetName("m.ok")
	dp4 := m4.SetEmptyGauge().DataPoints().AppendEmpty()
	dp4.SetTimestamp(1_700_000_000_000_000_000)
	dp4.SetIntValue(7)

	rows, rejects := MetricRows(md, opts)
	if len(rows) != 1 {
		t.Fatalf("len(rows) = %d, want 1", len(rows))
	}
	if len(rejects) != 4 {
		t.Fatalf("len(rejects) = %d, want 4: %+v", len(rejects), rejects)
	}

	var entries []QuarantineEntry
	for _, r := range rejects {
		entries = append(entries, QuarantineEntry{Rejection: r})
	}
	qRows := MetricQuarantineRows(md, entries, opts)
	if len(qRows) != 4 {
		t.Fatalf("len(qRows) = %d, want 4", len(qRows))
	}
	wantCodes := []string{"INVALID_ID", "MISSING_TIMESTAMP", "TIMESTAMP_OVERFLOW", "SERIALIZATION"}
	for i, want := range wantCodes {
		if qRows[i]["signal"] != schema.SignalMetrics {
			t.Errorf("qRows[%d].signal = %v, want %q", i, qRows[i]["signal"], schema.SignalMetrics)
		}
		if qRows[i]["error_code"] != want {
			t.Errorf("qRows[%d].error_code = %v, want %q", i, qRows[i]["error_code"], want)
		}
	}

	selected := SelectMetrics(md, []int{4})
	if selected.DataPointCount() != 1 {
		t.Fatalf("selected DataPointCount = %d, want 1", selected.DataPointCount())
	}
	gotName := selected.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Name()
	if gotName != "m.ok" {
		t.Fatalf("selected metric name = %q, want m.ok", gotName)
	}
}
