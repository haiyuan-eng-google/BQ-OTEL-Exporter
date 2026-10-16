// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
	"go.yaml.in/yaml/v3"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/testfakes/bqfake"
)

type sdkManifest struct {
	SchemaVersion string             `yaml:"schema_version"`
	Provenance    string             `yaml:"provenance"`
	Frameworks    []sdkManifestEntry `yaml:"frameworks"`
	Scenarios     []sdkScenarioEntry `yaml:"scenarios"`
}

type sdkManifestEntry struct {
	ID               string   `yaml:"id"`
	Version          string   `yaml:"version"`
	CompanionVersion string   `yaml:"companion_version"`
	Tier             string   `yaml:"tier"`
	Signals          []string `yaml:"signals"`
	GenAISystem      string   `yaml:"gen_ai_system"`
	TracesFile       string   `yaml:"traces_file"`
	LogsFile         string   `yaml:"logs_file"`
	MetricsFile      string   `yaml:"metrics_file"`
	// LogsOutsideGenAIView marks a framework whose log events carry no
	// gen_ai.* attributes, so v_gen_ai_logs does not select them and they are
	// queried from otel_logs instead.
	LogsOutsideGenAIView bool `yaml:"logs_outside_gen_ai_view"`
}

type sdkScenarioEntry struct {
	ID             string   `yaml:"id"`
	Kind           string   `yaml:"kind"`
	Provenance     string   `yaml:"provenance"`
	SemconvVersion string   `yaml:"semconv_version"`
	Signals        []string `yaml:"signals"`
	GenAISystem    string   `yaml:"gen_ai_system"`
	TracesFile     string   `yaml:"traces_file"`
	LogsFile       string   `yaml:"logs_file"`
	MetricsFile    string   `yaml:"metrics_file"`
}

func loadSDKManifest(t *testing.T) sdkManifest {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "otlp", "manifest.yaml"))
	if err != nil {
		t.Fatalf("read testdata/otlp/manifest.yaml: %v", err)
	}
	var m sdkManifest
	if err := yaml.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal manifest.yaml: %v", err)
	}
	if m.SchemaVersion != schema.Version {
		t.Fatalf("manifest schema_version = %q, want %q", m.SchemaVersion, schema.Version)
	}
	// The fixtures are hand-written after the documented evidence, not
	// captures, and the manifest must not claim otherwise.
	if m.Provenance != "synthetic" {
		t.Fatalf("manifest provenance = %q, want %q", m.Provenance, "synthetic")
	}
	if len(m.Frameworks) != 7 {
		t.Fatalf("manifest frameworks count = %d, want 7", len(m.Frameworks))
	}
	if len(m.Scenarios) != 2 {
		t.Fatalf("manifest scenarios count = %d, want 2", len(m.Scenarios))
	}
	for _, sc := range m.Scenarios {
		if sc.ID == "" {
			t.Fatalf("scenario entry has empty id")
		}
		if sc.Kind != "scenario" {
			t.Fatalf("scenario %s kind = %q, want %q", sc.ID, sc.Kind, "scenario")
		}
		if sc.Provenance != "synthetic" {
			t.Fatalf("scenario %s provenance = %q, want %q", sc.ID, sc.Provenance, "synthetic")
		}
		if sc.SemconvVersion == "" {
			t.Fatalf("scenario %s semconv_version is empty", sc.ID)
		}
		if len(sc.Signals) == 0 {
			t.Fatalf("scenario %s signals list is empty", sc.ID)
		}
	}
	return m
}

func TestSDKManifestDocVersionParity(t *testing.T) {
	m := loadSDKManifest(t)
	docBytes, err := os.ReadFile(filepath.Join("docs", "agent-frameworks.md"))
	if err != nil {
		t.Fatalf("read docs/agent-frameworks.md: %v", err)
	}
	doc := string(docBytes)

	for _, fw := range m.Frameworks {
		if !strings.Contains(doc, fw.Version) {
			t.Errorf("framework %s version %q not found in docs/agent-frameworks.md", fw.ID, fw.Version)
		}
		if fw.CompanionVersion != "" && !strings.Contains(doc, fw.CompanionVersion) {
			t.Errorf("framework %s companion_version %q not found in docs/agent-frameworks.md", fw.ID, fw.CompanionVersion)
		}
	}
	for _, sc := range m.Scenarios {
		if !strings.Contains(doc, sc.ID) {
			t.Errorf("scenario %s id not found in docs/agent-frameworks.md", sc.ID)
		}
		if !strings.Contains(doc, sc.SemconvVersion) {
			t.Errorf("scenario %s semconv_version %q not found in docs/agent-frameworks.md", sc.ID, sc.SemconvVersion)
		}
	}
}

// The GenAI client metrics of every framework are two delta histograms, one
// point to the call: token usage (an input point and an output point) and
// operation duration. A fixture that drifted from that shape, such as a bucket
// count that no longer matches its value, would still load and would make the
// end-to-end checks below meaningless, so the shape is pinned on its own.
func TestSDKMetricsFixtureShapes(t *testing.T) {
	m := loadSDKManifest(t)
	var unmarshaler pmetric.JSONUnmarshaler
	for _, fw := range m.Frameworks {
		t.Run(fw.ID, func(t *testing.T) {
			if !slices.Contains(fw.Signals, "metrics") || fw.MetricsFile == "" {
				t.Fatalf("framework %s must list the metrics signal and a metrics_file", fw.ID)
			}
			path := filepath.Join("testdata", "otlp", fw.MetricsFile)
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			md, err := unmarshaler.UnmarshalMetrics(raw)
			if err != nil {
				t.Fatalf("UnmarshalMetrics(%s): %v", path, err)
			}
			if md.ResourceMetrics().Len() != 1 || md.ResourceMetrics().At(0).ScopeMetrics().Len() != 1 {
				t.Fatalf("%s must hold one resource with one scope", path)
			}
			metrics := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
			if metrics.Len() != 2 {
				t.Fatalf("%s holds %d metrics, want 2", path, metrics.Len())
			}

			wantUnit := map[string]string{
				"gen_ai.client.token.usage":        "{token}",
				"gen_ai.client.operation.duration": "s",
			}
			seen := make(map[string]int)
			for _, mt := range metrics.All() {
				unit, known := wantUnit[mt.Name()]
				if !known {
					t.Fatalf("unexpected metric %q in %s", mt.Name(), path)
				}
				if mt.Unit() != unit {
					t.Errorf("%s unit = %q, want %q", mt.Name(), mt.Unit(), unit)
				}
				if mt.Type() != pmetric.MetricTypeHistogram {
					t.Fatalf("%s type = %v, want a histogram", mt.Name(), mt.Type())
				}
				if got := mt.Histogram().AggregationTemporality(); got != pmetric.AggregationTemporalityDelta {
					t.Errorf("%s temporality = %v, want delta", mt.Name(), got)
				}
				for _, dp := range mt.Histogram().DataPoints().All() {
					requireOneCallHistogramPoint(t, mt.Name(), dp)
					key := mt.Name()
					if tokenType, ok := dp.Attributes().Get("gen_ai.token.type"); ok {
						key += "/" + tokenType.Str()
					}
					seen[key]++
				}
			}
			for _, key := range []string{
				"gen_ai.client.token.usage/input",
				"gen_ai.client.token.usage/output",
				"gen_ai.client.operation.duration",
			} {
				if seen[key] != 1 {
					t.Errorf("%s holds %d %s points, want 1", path, seen[key], key)
				}
			}
			if len(seen) != 3 {
				t.Errorf("%s holds points of kinds %v, want exactly the three above", path, seen)
			}
		})
	}
}

// requireOneCallHistogramPoint checks a histogram point that records exactly
// one call: its single observation is the sum, the minimum and the maximum,
// it falls in exactly one bucket, that bucket is the one the explicit bounds
// assign it, and one exemplar carries the same value, a trace and a span ID,
// and a timestamp inside the point's interval.
func requireOneCallHistogramPoint(t *testing.T, name string, dp pmetric.HistogramDataPoint) {
	t.Helper()
	if dp.Count() != 1 {
		t.Errorf("%s count = %d, want 1", name, dp.Count())
	}
	if !dp.HasSum() || !dp.HasMin() || !dp.HasMax() || dp.Sum() != dp.Min() || dp.Sum() != dp.Max() {
		t.Errorf("%s sum, min and max = %v, %v, %v, want one value", name, dp.Sum(), dp.Min(), dp.Max())
	}
	bounds := dp.ExplicitBounds().AsRaw()
	counts := dp.BucketCounts().AsRaw()
	if len(counts) != len(bounds)+1 {
		t.Fatalf("%s holds %d bucket counts for %d bounds, want one more than the bounds", name, len(counts), len(bounds))
	}
	for i := 1; i < len(bounds); i++ {
		if bounds[i] <= bounds[i-1] {
			t.Fatalf("%s explicit bounds are not strictly increasing: %v", name, bounds)
		}
	}
	// A value belongs to the first bucket whose upper bound it does not exceed.
	wantBucket := sort.SearchFloat64s(bounds, dp.Sum())
	for i, c := range counts {
		want := uint64(0)
		if i == wantBucket {
			want = 1
		}
		if c != want {
			t.Errorf("%s bucket %d holds %d, want %d for the value %v", name, i, c, want, dp.Sum())
		}
	}
	if dp.Exemplars().Len() != 1 {
		t.Fatalf("%s holds %d exemplars, want 1", name, dp.Exemplars().Len())
	}
	ex := dp.Exemplars().At(0)
	if ex.ValueType() != pmetric.ExemplarValueTypeDouble || ex.DoubleValue() != dp.Sum() {
		t.Errorf("%s exemplar value = %v, want the point's value %v", name, ex.DoubleValue(), dp.Sum())
	}
	if ex.TraceID().IsEmpty() || ex.SpanID().IsEmpty() {
		t.Errorf("%s exemplar must carry a trace ID and a span ID", name)
	}
	if ex.Timestamp() < dp.StartTimestamp() || ex.Timestamp() > dp.Timestamp() {
		t.Errorf("%s exemplar timestamp %v lies outside the point's interval [%v, %v]",
			name, ex.Timestamp(), dp.StartTimestamp(), dp.Timestamp())
	}
}

// metricPointCount counts the data points of one metric, whatever its type.
func metricPointCount(m pmetric.Metric) int {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		return m.Gauge().DataPoints().Len()
	case pmetric.MetricTypeSum:
		return m.Sum().DataPoints().Len()
	case pmetric.MetricTypeHistogram:
		return m.Histogram().DataPoints().Len()
	case pmetric.MetricTypeExponentialHistogram:
		return m.ExponentialHistogram().DataPoints().Len()
	case pmetric.MetricTypeSummary:
		return m.Summary().DataPoints().Len()
	default:
		return 0
	}
}

// decodeAttributes decodes the plain-JSON attributes column of a row.
func decodeAttributes(t *testing.T, row map[string]any) map[string]any {
	t.Helper()
	raw, _ := row["attributes"].(string)
	var attrs map[string]any
	if err := json.Unmarshal([]byte(raw), &attrs); err != nil {
		t.Fatalf("unmarshal attributes JSON %q: %v", raw, err)
	}
	return attrs
}

// spanTokenCount reads the token count of the given type ("input" or
// "output") off a span's attributes: the GenAI attribute, or the
// OpenInference one that CrewAI's instrumentation emits instead. The
// attributes column writes an int attribute as a decimal string and a double
// as a JSON number.
func spanTokenCount(attrs map[string]any, tokenType string) (float64, bool) {
	keys := map[string][]string{
		"input":  {"gen_ai.usage.input_tokens", "llm.token_count.prompt"},
		"output": {"gen_ai.usage.output_tokens", "llm.token_count.completion"},
	}[tokenType]
	for _, key := range keys {
		switch v := attrs[key].(type) {
		case float64:
			return v, true
		case string:
			if n, err := strconv.ParseFloat(v, 64); err == nil {
				return n, true
			}
		}
	}
	return 0, false
}

func TestAllSevenAgentFrameworkGoldenFixturesEndToEnd(t *testing.T) {
	m := loadSDKManifest(t)
	srv := bqfake.NewServer(t)
	wireFakeHooks(t, srv)

	cfg := validConfig()
	cfg.Project = "sdk-golden-proj"
	cfg.Dataset = "otel_sdk_golden"
	cfg.AutoCreate.Dataset = true
	cfg.AutoCreate.Tables = true
	cfg.AutoCreate.CompanionViews = true

	settings := component.TelemetrySettings{
		Logger:        zap.NewNop(),
		MeterProvider: noop.NewMeterProvider(),
	}
	tracesExp, err := newTracesExporter(settings, cfg)
	if err != nil {
		t.Fatalf("newTracesExporter: %v", err)
	}
	logsExp, err := newLogsExporter(settings, cfg)
	if err != nil {
		t.Fatalf("newLogsExporter: %v", err)
	}
	metricsExp, err := newMetricsExporter(settings, cfg)
	if err != nil {
		t.Fatalf("newMetricsExporter: %v", err)
	}

	ctx := context.Background()
	if err := tracesExp.start(ctx, nil); err != nil {
		t.Fatalf("tracesExp.start: %v", err)
	}
	t.Cleanup(func() { _ = tracesExp.shutdown(context.Background()) })
	if err := logsExp.start(ctx, nil); err != nil {
		t.Fatalf("logsExp.start: %v", err)
	}
	t.Cleanup(func() { _ = logsExp.shutdown(context.Background()) })
	if err := metricsExp.start(ctx, nil); err != nil {
		t.Fatalf("metricsExp.start: %v", err)
	}
	t.Cleanup(func() { _ = metricsExp.shutdown(context.Background()) })

	var traceUnmarshaler ptrace.JSONUnmarshaler
	var logUnmarshaler plog.JSONUnmarshaler
	var metricUnmarshaler pmetric.JSONUnmarshaler

	totalExpectedSpans := 0
	totalExpectedLogs := 0
	totalExpectedPoints := 0
	// Every fixture uses a service.name of its own, so the rows each one
	// produced can be counted separately.
	wantSpansByService := make(map[string]int)
	wantLogsByService := make(map[string]int)
	wantPointsByService := make(map[string]int)
	logsOutsideView := make(map[string]bool)

	for _, fw := range m.Frameworks {
		t.Run(fw.ID, func(t *testing.T) {
			if slices.Contains(fw.Signals, "traces") && fw.TracesFile != "" {
				tracesPath := filepath.Join("testdata", "otlp", fw.TracesFile)
				tracesRaw, err := os.ReadFile(tracesPath)
				if err != nil {
					t.Fatalf("read %s: %v", tracesPath, err)
				}
				td, err := traceUnmarshaler.UnmarshalTraces(tracesRaw)
				if err != nil {
					t.Fatalf("UnmarshalTraces(%s): %v", tracesPath, err)
				}
				totalExpectedSpans += td.SpanCount()
				for _, rs := range td.ResourceSpans().All() {
					svc, _ := rs.Resource().Attributes().Get("service.name")
					for _, ss := range rs.ScopeSpans().All() {
						wantSpansByService[svc.Str()] += ss.Spans().Len()
					}
				}

				if err := tracesExp.pushTraceData(ctx, td); err != nil {
					t.Fatalf("pushTraceData(%s): %v", fw.ID, err)
				}
			}

			if slices.Contains(fw.Signals, "logs") && fw.LogsFile != "" {
				logsPath := filepath.Join("testdata", "otlp", fw.LogsFile)
				logsRaw, err := os.ReadFile(logsPath)
				if err != nil {
					t.Fatalf("read %s: %v", logsPath, err)
				}
				ld, err := logUnmarshaler.UnmarshalLogs(logsRaw)
				if err != nil {
					t.Fatalf("UnmarshalLogs(%s): %v", logsPath, err)
				}
				totalExpectedLogs += ld.LogRecordCount()
				for _, rl := range ld.ResourceLogs().All() {
					svc, _ := rl.Resource().Attributes().Get("service.name")
					for _, sl := range rl.ScopeLogs().All() {
						wantLogsByService[svc.Str()] += sl.LogRecords().Len()
					}
					if fw.LogsOutsideGenAIView {
						logsOutsideView[svc.Str()] = true
					}
				}

				if err := logsExp.pushLogData(ctx, ld); err != nil {
					t.Fatalf("pushLogData(%s): %v", fw.ID, err)
				}
			}

			if slices.Contains(fw.Signals, "metrics") && fw.MetricsFile != "" {
				metricsPath := filepath.Join("testdata", "otlp", fw.MetricsFile)
				metricsRaw, err := os.ReadFile(metricsPath)
				if err != nil {
					t.Fatalf("read %s: %v", metricsPath, err)
				}
				md, err := metricUnmarshaler.UnmarshalMetrics(metricsRaw)
				if err != nil {
					t.Fatalf("UnmarshalMetrics(%s): %v", metricsPath, err)
				}
				totalExpectedPoints += md.DataPointCount()
				for _, rm := range md.ResourceMetrics().All() {
					svc, _ := rm.Resource().Attributes().Get("service.name")
					for _, sm := range rm.ScopeMetrics().All() {
						for _, mt := range sm.Metrics().All() {
							wantPointsByService[svc.Str()] += metricPointCount(mt)
						}
					}
				}

				if err := metricsExp.pushMetricsData(ctx, md); err != nil {
					t.Fatalf("pushMetricsData(%s): %v", fw.ID, err)
				}
			}
		})
	}

	spanRows := srv.Rows("otel_spans")
	logRows := srv.Rows("otel_logs")
	metricRows := srv.Rows("otel_metrics")
	if len(spanRows) != totalExpectedSpans {
		t.Fatalf("expected exactly %d span rows across all frameworks, got %d", totalExpectedSpans, len(spanRows))
	}
	if len(logRows) != totalExpectedLogs {
		t.Fatalf("expected exactly %d log rows across all frameworks, got %d", totalExpectedLogs, len(logRows))
	}
	if len(metricRows) != totalExpectedPoints {
		t.Fatalf("expected exactly %d metric rows across all frameworks, got %d", totalExpectedPoints, len(metricRows))
	}
	for _, sig := range []struct {
		name string
		rows []map[string]any
		want map[string]int
	}{
		{"span", spanRows, wantSpansByService},
		{"log", logRows, wantLogsByService},
		{"metric", metricRows, wantPointsByService},
	} {
		got := make(map[string]int)
		for _, r := range sig.rows {
			svc, _ := r["service_name"].(string)
			got[svc]++
		}
		for svc, n := range sig.want {
			if got[svc] != n {
				t.Errorf("%s rows for fixture service %q = %d, want %d", sig.name, svc, got[svc], n)
			}
		}
		for svc, n := range got {
			if _, ok := sig.want[svc]; !ok {
				t.Errorf("%d %s rows for service %q, which no fixture sends", n, sig.name, svc)
			}
		}
	}

	// Index decoded spans by (service_name, trace_id, span_id), which is how an
	// exemplar names the span it was recorded under.
	type joinKey struct {
		serviceName string
		traceID     string
		spanID      string
	}
	type spanFixture struct {
		row   map[string]any
		attrs map[string]any
	}
	spansByKey := make(map[joinKey]spanFixture)
	systemsSeenInSpans := make(map[string]bool)

	for _, r := range spanRows {
		if r["schema_version"] != schema.Version {
			t.Fatalf("span schema_version = %v, want %q", r["schema_version"], schema.Version)
		}
		attrsStr, _ := r["attributes"].(string)
		var attrs map[string]any
		if err := json.Unmarshal([]byte(attrsStr), &attrs); err != nil {
			t.Fatalf("unmarshal span attributes JSON: %v", err)
		}

		svc, _ := r["service_name"].(string)
		tid, _ := r["trace_id"].(string)
		sid, _ := r["span_id"].(string)
		spansByKey[joinKey{serviceName: svc, traceID: tid, spanID: sid}] = spanFixture{row: r, attrs: attrs}

		opName, _ := attrs["gen_ai.operation.name"].(string)
		sysName, _ := attrs["gen_ai.system"].(string)
		if sysName == "" {
			sysName, _ = attrs["gen_ai.provider.name"].(string)
		}
		invID, _ := attrs["gcp.vertex.agent.invocation_id"].(string)

		if sysName != "" {
			systemsSeenInSpans[sysName] = true
		}

		if svc == "crewai-analyst-crew" {
			// CrewAI is instrumented via OpenInference (openinference-instrumentation-crewai),
			// which emits openinference.span.kind and crew_tasks rather than gen_ai.system.
			if attrs["openinference.span.kind"] != "AGENT" {
				t.Fatalf("CrewAI span missing openinference.span.kind=AGENT: %v", attrs)
			}
			if attrs["crew_tasks"] == nil {
				t.Fatalf("CrewAI span missing crew_tasks: %v", attrs)
			}
		} else if opName == "" && sysName == "" && invID == "" {
			// All other supported agent frameworks emit standard OTel GenAI or Vertex conventions
			// and pass the v_gen_ai_spans WHERE filter.
			t.Fatalf("span row %v failed v_gen_ai_spans WHERE filter", r["span_id"])
		}

		if sysName == "gcp.vertex.agent" {
			if invID == "" || attrs["gcp.vertex.agent.session_id"] == nil || attrs["gcp.vertex.agent.event_id"] == nil {
				t.Fatalf("ADK span missing gcp.vertex.agent.{invocation_id,session_id,event_id}: %v", attrs)
			}
		}
	}

	for _, fw := range m.Frameworks {
		if fw.GenAISystem != "" && !systemsSeenInSpans[fw.GenAISystem] {
			t.Errorf("framework %s (gen_ai.system=%q) was not observed in v_gen_ai_spans rows", fw.ID, fw.GenAISystem)
		}
	}

	systemsSeenInLogs := make(map[string]bool)
	claudeCostEventVerified := false

	for _, l := range logRows {
		if l["schema_version"] != schema.Version {
			t.Fatalf("log schema_version = %v, want %q", l["schema_version"], schema.Version)
		}
		lAttrsStr, _ := l["attributes"].(string)
		var lAttrs map[string]any
		if err := json.Unmarshal([]byte(lAttrsStr), &lAttrs); err != nil {
			t.Fatalf("unmarshal log attributes JSON: %v", err)
		}

		// Note: transform.LogRows promotes attributes["event.name"] into the event_name
		// column when LogRecord.EventName is empty, while v_gen_ai_logs ALSO applies
		// COALESCE(NULLIF(l.event_name, ''), JSON_VALUE(l.attributes, '$."event.name"')).
		colEventName, _ := l["event_name"].(string)
		attrEventName, _ := lAttrs["event.name"].(string)
		promotedEventName := colEventName
		if promotedEventName == "" {
			promotedEventName = attrEventName
		}

		// Provider identity arrives as gen_ai.system or gen_ai.provider.name
		// depending on the instrumentation generation; the view accepts either.
		lSys, _ := lAttrs["gen_ai.system"].(string)
		if lSys == "" {
			lSys, _ = lAttrs["gen_ai.provider.name"].(string)
		}
		lInv, _ := lAttrs["gcp.vertex.agent.invocation_id"].(string)
		inView := strings.HasPrefix(promotedEventName, "gen_ai.") || lSys != "" || lInv != ""

		svc, _ := l["service_name"].(string)
		if logsOutsideView[svc] {
			// Claude Code's cost event is claude_code.api_request with
			// event.name "api_request" and no gen_ai.* attribute, so it lands
			// in otel_logs, joined to its span, but v_gen_ai_logs does not
			// select it.
			if inView {
				t.Errorf("log row for %q unexpectedly passes the v_gen_ai_logs WHERE predicate: event_name=%q attrs=%v", svc, promotedEventName, lAttrs)
			}
			if colEventName == "api_request" && attrEventName == "api_request" && lAttrs["cost_usd"] != nil {
				claudeCostEventVerified = true
			}
		} else if !inView {
			t.Fatalf("log row failed v_gen_ai_logs WHERE predicate: event_name=%q attrs=%v", promotedEventName, lAttrs)
		}
		if lSys != "" {
			systemsSeenInLogs[lSys] = true
		}

		// v_gen_ai_logs left-joins the log dedup view to the span dedup view
		// on trace_id, span_id, a NULL-safe service_name, and a span start
		// within the bounded window around the record's timestamp.
		tid, _ := l["trace_id"].(string)
		sid, _ := l["span_id"].(string)
		joined := false
		for _, s := range spanRows {
			if logJoinsSpan(l, s) {
				joined = true
				break
			}
		}
		if !joined {
			t.Errorf("log row (service=%q, trace=%q, span=%q) did not join to any span in otel_spans_dedup", svc, tid, sid)
		}
	}

	if !claudeCostEventVerified {
		t.Fatalf("expected the anthropic_agent_sdk log fixture to land its api_request cost event in otel_logs with event_name promoted from event.name")
	}
	for _, fw := range m.Frameworks {
		if slices.Contains(fw.Signals, "logs") && fw.GenAISystem != "" && !fw.LogsOutsideGenAIView {
			if !systemsSeenInLogs[fw.GenAISystem] {
				t.Errorf("framework %s (gen_ai.system=%q) was not observed in v_gen_ai_logs rows", fw.ID, fw.GenAISystem)
			}
		}
	}

	// Every metrics.json describes the one span its exemplars link to: the token
	// sums and the duration equal that span's, and the provider, operation and
	// request model the span names are the ones the metric names. A span that
	// carries no gen_ai.* attributes, such as CrewAI's, is held to the token sums
	// and the duration alone.
	pointsByKind := make(map[string]int)
	for _, r := range metricRows {
		if r["schema_version"] != schema.Version {
			t.Fatalf("metric schema_version = %v, want %q", r["schema_version"], schema.Version)
		}
		svc, _ := r["service_name"].(string)
		name, _ := r["metric_name"].(string)
		if !sqlLike(genAIMetricNamePattern, name) {
			t.Errorf("metric %q of %q fails the v_gen_ai_metrics WHERE predicate", name, svc)
		}
		mAttrs := decodeAttributes(t, r)
		exemplars, _ := r["exemplars"].([]map[string]any)
		if len(exemplars) != 1 {
			t.Errorf("%s point of %q carries %d exemplars, want 1", name, svc, len(exemplars))
			continue
		}
		tid, _ := exemplars[0]["trace_id"].(string)
		sid, _ := exemplars[0]["span_id"].(string)
		sp, ok := spansByKey[joinKey{serviceName: svc, traceID: tid, spanID: sid}]
		if !ok {
			t.Errorf("exemplar of a %s point of %q links trace %q span %q, which is not a span row of that service", name, svc, tid, sid)
			continue
		}

		sum, _ := r["sum"].(float64)
		switch name {
		case "gen_ai.client.token.usage":
			tokenType, _ := mAttrs["gen_ai.token.type"].(string)
			want, found := spanTokenCount(sp.attrs, tokenType)
			if !found {
				t.Errorf("%s %q point of %q: its span reports no token count of that type", name, tokenType, svc)
			} else if sum != want {
				t.Errorf("%s %q point of %q sums to %v, but its span reports %v", name, tokenType, svc, sum, want)
			}
			pointsByKind[svc+"/"+name+"/"+tokenType]++
		case "gen_ai.client.operation.duration":
			ns, _ := sp.row["duration_nano"].(int64)
			if math.Abs(sum-float64(ns)/1e9) > 1e-9 {
				t.Errorf("%s point of %q sums to %vs, but its span lasted %vs", name, svc, sum, float64(ns)/1e9)
			}
			pointsByKind[svc+"/"+name]++
		default:
			t.Errorf("unexpected metric %q of %q", name, svc)
		}

		for _, key := range []string{"gen_ai.operation.name", "gen_ai.system", "gen_ai.provider.name", "gen_ai.request.model"} {
			want, onSpan := sp.attrs[key]
			if !onSpan {
				continue
			}
			if got := mAttrs[key]; got != want {
				t.Errorf("%s point of %q has %s = %v, but its span has %v", name, svc, key, got, want)
			}
		}
	}
	for svc := range wantPointsByService {
		for _, kind := range []string{
			"gen_ai.client.token.usage/input",
			"gen_ai.client.token.usage/output",
			"gen_ai.client.operation.duration",
		} {
			key := svc + "/" + kind
			if pointsByKind[key] != 1 {
				t.Errorf("service %q has %d %s points, want 1", svc, pointsByKind[key], kind)
			}
		}
	}
}

// spanReasoningCount reads reasoning tokens off a span's attributes using the
// v1.40+ key first and the earlier key second.
func spanReasoningCount(attrs map[string]any) (float64, bool) {
	for _, key := range []string{"gen_ai.usage.reasoning.output_tokens", "gen_ai.usage.reasoning_tokens"} {
		switch v := attrs[key].(type) {
		case float64:
			return v, true
		case string:
			if n, err := strconv.ParseFloat(v, 64); err == nil {
				return n, true
			}
		}
	}
	return 0, false
}

func TestScenarioMetricsFixtureShapes(t *testing.T) {
	m := loadSDKManifest(t)
	var unmarshaler pmetric.JSONUnmarshaler
	wantUnit := map[string]string{
		"gen_ai.client.token.usage":                             "{token}",
		"gen_ai.client.operation.duration":                      "s",
		"gen_ai.client.inference.usage.input_tokens":            "{token}",
		"gen_ai.client.inference.usage.output_tokens":           "{token}",
		"gen_ai.client.inference.usage.reasoning.output_tokens": "{token}",
	}

	for _, sc := range m.Scenarios {
		if !slices.Contains(sc.Signals, "metrics") {
			continue
		}
		t.Run(sc.ID, func(t *testing.T) {
			if sc.MetricsFile == "" {
				t.Fatalf("scenario %s lists metrics signal without metrics_file", sc.ID)
			}
			path := filepath.Join("testdata", "otlp", sc.MetricsFile)
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			md, err := unmarshaler.UnmarshalMetrics(raw)
			if err != nil {
				t.Fatalf("UnmarshalMetrics(%s): %v", path, err)
			}
			if md.ResourceMetrics().Len() != 1 || md.ResourceMetrics().At(0).ScopeMetrics().Len() != 1 {
				t.Fatalf("%s must hold one resource with one scope", path)
			}
			metrics := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
			if metrics.Len() != 5 {
				t.Fatalf("%s holds %d metrics, want 5", path, metrics.Len())
			}

			seen := make(map[string]int)
			for _, mt := range metrics.All() {
				unit, known := wantUnit[mt.Name()]
				if !known {
					t.Fatalf("unexpected metric %q in %s", mt.Name(), path)
				}
				if mt.Unit() != unit {
					t.Errorf("%s unit = %q, want %q", mt.Name(), mt.Unit(), unit)
				}
				if mt.Type() != pmetric.MetricTypeHistogram {
					t.Fatalf("%s type = %v, want a histogram", mt.Name(), mt.Type())
				}
				if got := mt.Histogram().AggregationTemporality(); got != pmetric.AggregationTemporalityDelta {
					t.Errorf("%s temporality = %v, want delta", mt.Name(), got)
				}
				for _, dp := range mt.Histogram().DataPoints().All() {
					requireOneCallHistogramPoint(t, mt.Name(), dp)
					key := mt.Name()
					if tokenType, ok := dp.Attributes().Get("gen_ai.token.type"); ok {
						key += "/" + tokenType.Str()
					}
					seen[key]++
				}
			}

			for _, key := range []string{
				"gen_ai.client.token.usage/input",
				"gen_ai.client.token.usage/output",
				"gen_ai.client.operation.duration",
				"gen_ai.client.inference.usage.input_tokens",
				"gen_ai.client.inference.usage.output_tokens",
				"gen_ai.client.inference.usage.reasoning.output_tokens",
			} {
				if seen[key] != 2 {
					t.Errorf("%s holds %d %s points, want 2", path, seen[key], key)
				}
			}
			if len(seen) != 6 {
				t.Errorf("%s holds points of kinds %v, want exactly 6 kinds", path, seen)
			}
		})
	}
}

func TestScenarioGoldenFixturesEndToEnd(t *testing.T) {
	m := loadSDKManifest(t)
	srv := bqfake.NewServer(t)
	wireFakeHooks(t, srv)

	cfg := validConfig()
	cfg.Project = "sdk-scenario-proj"
	cfg.Dataset = "otel_sdk_scenarios"
	cfg.AutoCreate.Dataset = true
	cfg.AutoCreate.Tables = true
	cfg.AutoCreate.CompanionViews = true
	cfg.Quarantine.Enabled = true
	cfg.Quarantine.Table = "otel_quarantine"

	settings := component.TelemetrySettings{
		Logger:        zap.NewNop(),
		MeterProvider: noop.NewMeterProvider(),
	}
	tracesExp, err := newTracesExporter(settings, cfg)
	if err != nil {
		t.Fatalf("newTracesExporter: %v", err)
	}
	logsExp, err := newLogsExporter(settings, cfg)
	if err != nil {
		t.Fatalf("newLogsExporter: %v", err)
	}
	metricsExp, err := newMetricsExporter(settings, cfg)
	if err != nil {
		t.Fatalf("newMetricsExporter: %v", err)
	}

	ctx := context.Background()
	if err := tracesExp.start(ctx, nil); err != nil {
		t.Fatalf("tracesExp.start: %v", err)
	}
	t.Cleanup(func() { _ = tracesExp.shutdown(context.Background()) })
	if err := logsExp.start(ctx, nil); err != nil {
		t.Fatalf("logsExp.start: %v", err)
	}
	t.Cleanup(func() { _ = logsExp.shutdown(context.Background()) })
	if err := metricsExp.start(ctx, nil); err != nil {
		t.Fatalf("metricsExp.start: %v", err)
	}
	t.Cleanup(func() { _ = metricsExp.shutdown(context.Background()) })

	var traceUnmarshaler ptrace.JSONUnmarshaler
	var logUnmarshaler plog.JSONUnmarshaler
	var metricUnmarshaler pmetric.JSONUnmarshaler

	totalExpectedSpans := 0
	totalExpectedLogs := 0
	totalExpectedPoints := 0
	wantSpansByService := make(map[string]int)
	wantLogsByService := make(map[string]int)
	wantPointsByService := make(map[string]int)

	for _, sc := range m.Scenarios {
		t.Run(sc.ID, func(t *testing.T) {
			if slices.Contains(sc.Signals, "traces") && sc.TracesFile != "" {
				tracesPath := filepath.Join("testdata", "otlp", sc.TracesFile)
				tracesRaw, err := os.ReadFile(tracesPath)
				if err != nil {
					t.Fatalf("read %s: %v", tracesPath, err)
				}
				td, err := traceUnmarshaler.UnmarshalTraces(tracesRaw)
				if err != nil {
					t.Fatalf("UnmarshalTraces(%s): %v", tracesPath, err)
				}
				totalExpectedSpans += td.SpanCount()
				for _, rs := range td.ResourceSpans().All() {
					svc, _ := rs.Resource().Attributes().Get("service.name")
					for _, ss := range rs.ScopeSpans().All() {
						wantSpansByService[svc.Str()] += ss.Spans().Len()
					}
				}
				if err := tracesExp.pushTraceData(ctx, td); err != nil {
					t.Fatalf("pushTraceData(%s): %v", sc.ID, err)
				}
			}

			if slices.Contains(sc.Signals, "logs") && sc.LogsFile != "" {
				logsPath := filepath.Join("testdata", "otlp", sc.LogsFile)
				logsRaw, err := os.ReadFile(logsPath)
				if err != nil {
					t.Fatalf("read %s: %v", logsPath, err)
				}
				ld, err := logUnmarshaler.UnmarshalLogs(logsRaw)
				if err != nil {
					t.Fatalf("UnmarshalLogs(%s): %v", logsPath, err)
				}
				totalExpectedLogs += ld.LogRecordCount()
				for _, rl := range ld.ResourceLogs().All() {
					svc, _ := rl.Resource().Attributes().Get("service.name")
					for _, sl := range rl.ScopeLogs().All() {
						wantLogsByService[svc.Str()] += sl.LogRecords().Len()
					}
				}
				if err := logsExp.pushLogData(ctx, ld); err != nil {
					t.Fatalf("pushLogData(%s): %v", sc.ID, err)
				}
			}

			if slices.Contains(sc.Signals, "metrics") && sc.MetricsFile != "" {
				metricsPath := filepath.Join("testdata", "otlp", sc.MetricsFile)
				metricsRaw, err := os.ReadFile(metricsPath)
				if err != nil {
					t.Fatalf("read %s: %v", metricsPath, err)
				}
				md, err := metricUnmarshaler.UnmarshalMetrics(metricsRaw)
				if err != nil {
					t.Fatalf("UnmarshalMetrics(%s): %v", metricsPath, err)
				}
				totalExpectedPoints += md.DataPointCount()
				for _, rm := range md.ResourceMetrics().All() {
					svc, _ := rm.Resource().Attributes().Get("service.name")
					for _, sm := range rm.ScopeMetrics().All() {
						for _, mt := range sm.Metrics().All() {
							wantPointsByService[svc.Str()] += metricPointCount(mt)
						}
					}
				}
				if err := metricsExp.pushMetricsData(ctx, md); err != nil {
					t.Fatalf("pushMetricsData(%s): %v", sc.ID, err)
				}
			}
		})
	}

	if qRows := srv.Rows("otel_quarantine"); len(qRows) != 0 {
		t.Fatalf("otel_quarantine rows = %d, want 0: %v", len(qRows), qRows)
	}

	spanRows := srv.Rows("otel_spans")
	logRows := srv.Rows("otel_logs")
	metricRows := srv.Rows("otel_metrics")
	if len(spanRows) != totalExpectedSpans {
		t.Fatalf("expected %d span rows across scenarios, got %d", totalExpectedSpans, len(spanRows))
	}
	if len(logRows) != totalExpectedLogs {
		t.Fatalf("expected %d log rows across scenarios, got %d", totalExpectedLogs, len(logRows))
	}
	if len(metricRows) != totalExpectedPoints {
		t.Fatalf("expected %d metric rows across scenarios, got %d", totalExpectedPoints, len(metricRows))
	}

	for _, sig := range []struct {
		name string
		rows []map[string]any
		want map[string]int
	}{
		{"span", spanRows, wantSpansByService},
		{"log", logRows, wantLogsByService},
		{"metric", metricRows, wantPointsByService},
	} {
		got := make(map[string]int)
		for _, r := range sig.rows {
			svc, _ := r["service_name"].(string)
			got[svc]++
		}
		for svc, n := range sig.want {
			if got[svc] != n {
				t.Errorf("%s rows for scenario service %q = %d, want %d", sig.name, svc, got[svc], n)
			}
		}
	}

	type joinKey struct {
		serviceName string
		traceID     string
		spanID      string
	}
	type spanFixture struct {
		row   map[string]any
		attrs map[string]any
	}
	spansByKey := make(map[joinKey]spanFixture)
	for _, r := range spanRows {
		if r["schema_version"] != schema.Version {
			t.Fatalf("span schema_version = %v, want %q", r["schema_version"], schema.Version)
		}
		attrs := decodeAttributes(t, r)
		svc, _ := r["service_name"].(string)
		tid, _ := r["trace_id"].(string)
		sid, _ := r["span_id"].(string)
		spansByKey[joinKey{serviceName: svc, traceID: tid, spanID: sid}] = spanFixture{row: r, attrs: attrs}
	}

	for _, l := range logRows {
		if l["schema_version"] != schema.Version {
			t.Fatalf("log schema_version = %v, want %q", l["schema_version"], schema.Version)
		}
		svc, _ := l["service_name"].(string)
		tid, _ := l["trace_id"].(string)
		sid, _ := l["span_id"].(string)
		joined := false
		for _, s := range spanRows {
			if logJoinsSpan(l, s) {
				joined = true
				break
			}
		}
		if !joined {
			t.Errorf("scenario log row (service=%q, trace=%q, span=%q) did not join to any span", svc, tid, sid)
		}
	}

	for _, r := range metricRows {
		if r["schema_version"] != schema.Version {
			t.Fatalf("metric schema_version = %v, want %q", r["schema_version"], schema.Version)
		}
		svc, _ := r["service_name"].(string)
		name, _ := r["metric_name"].(string)
		if !sqlLike(genAIMetricNamePattern, name) {
			t.Errorf("metric %q of %q fails the v_gen_ai_metrics WHERE predicate", name, svc)
		}
		mAttrs := decodeAttributes(t, r)
		exemplars, _ := r["exemplars"].([]map[string]any)
		if len(exemplars) != 1 {
			t.Errorf("%s point of %q carries %d exemplars, want 1", name, svc, len(exemplars))
			continue
		}
		tid, _ := exemplars[0]["trace_id"].(string)
		sid, _ := exemplars[0]["span_id"].(string)
		sp, ok := spansByKey[joinKey{serviceName: svc, traceID: tid, spanID: sid}]
		if !ok {
			t.Errorf("exemplar of %s point of %q links trace %q span %q, not found in otel_spans", name, svc, tid, sid)
			continue
		}

		sum, _ := r["sum"].(float64)
		switch name {
		case "gen_ai.client.token.usage":
			tokenType, _ := mAttrs["gen_ai.token.type"].(string)
			want, found := spanTokenCount(sp.attrs, tokenType)
			if !found || sum != want {
				t.Errorf("%s %q point sum = %v, want span token count %v (found=%v)", name, tokenType, sum, want, found)
			}
		case "gen_ai.client.inference.usage.input_tokens":
			want, found := spanTokenCount(sp.attrs, "input")
			if !found || sum != want {
				t.Errorf("%s point sum = %v, want span input token count %v (found=%v)", name, sum, want, found)
			}
		case "gen_ai.client.inference.usage.output_tokens":
			want, found := spanTokenCount(sp.attrs, "output")
			if !found || sum != want {
				t.Errorf("%s point sum = %v, want span output token count %v (found=%v)", name, sum, want, found)
			}
		case "gen_ai.client.inference.usage.reasoning.output_tokens":
			want, found := spanReasoningCount(sp.attrs)
			if !found || sum != want {
				t.Errorf("%s point sum = %v, want span reasoning token count %v (found=%v)", name, sum, want, found)
			}
		case "gen_ai.client.operation.duration":
			ns, _ := sp.row["duration_nano"].(int64)
			if math.Abs(sum-float64(ns)/1e9) > 1e-9 {
				t.Errorf("%s point sums to %vs, but its span lasted %vs", name, sum, float64(ns)/1e9)
			}
		default:
			t.Errorf("unexpected metric %q of %q", name, svc)
		}

		for _, key := range []string{"gen_ai.operation.name", "gen_ai.system", "gen_ai.agent.name", "gen_ai.request.model", "gen_ai.response.model"} {
			want, onSpan := sp.attrs[key]
			if !onSpan {
				continue
			}
			if got, onMetric := mAttrs[key]; onMetric && got != want {
				t.Errorf("%s point of %q has %s = %v, but its span has %v", name, svc, key, got, want)
			}
		}
	}
}
