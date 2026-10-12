// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
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
	// LogsOutsideGenAIView marks a framework whose log events carry no
	// gen_ai.* attributes, so v_gen_ai_logs does not select them and they are
	// queried from otel_logs instead.
	LogsOutsideGenAIView bool `yaml:"logs_outside_gen_ai_view"`
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

	ctx := context.Background()
	if err := tracesExp.start(ctx, nil); err != nil {
		t.Fatalf("tracesExp.start: %v", err)
	}
	t.Cleanup(func() { _ = tracesExp.shutdown(context.Background()) })
	if err := logsExp.start(ctx, nil); err != nil {
		t.Fatalf("logsExp.start: %v", err)
	}
	t.Cleanup(func() { _ = logsExp.shutdown(context.Background()) })

	var traceUnmarshaler ptrace.JSONUnmarshaler
	var logUnmarshaler plog.JSONUnmarshaler

	totalExpectedSpans := 0
	totalExpectedLogs := 0
	// Every fixture uses a service.name of its own, so the rows each one
	// produced can be counted separately.
	wantSpansByService := make(map[string]int)
	wantLogsByService := make(map[string]int)
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
		})
	}

	spanRows := srv.Rows("otel_spans")
	logRows := srv.Rows("otel_logs")
	if len(spanRows) != totalExpectedSpans {
		t.Fatalf("expected exactly %d span rows across all frameworks, got %d", totalExpectedSpans, len(spanRows))
	}
	if len(logRows) != totalExpectedLogs {
		t.Fatalf("expected exactly %d log rows across all frameworks, got %d", totalExpectedLogs, len(logRows))
	}
	for name, rows := range map[string][]map[string]any{"span": spanRows, "log": logRows} {
		want := wantSpansByService
		if name == "log" {
			want = wantLogsByService
		}
		got := make(map[string]int)
		for _, r := range rows {
			svc, _ := r["service_name"].(string)
			got[svc]++
		}
		for svc, n := range want {
			if got[svc] != n {
				t.Errorf("%s rows for fixture service %q = %d, want %d", name, svc, got[svc], n)
			}
		}
		for svc, n := range got {
			if _, ok := want[svc]; !ok {
				t.Errorf("%d %s rows for service %q, which no fixture sends", n, name, svc)
			}
		}
	}

	// Index decoded spans by (service_name, trace_id, span_id) to simulate the
	// v_gen_ai_logs LEFT JOIN v_gen_ai_spans USING (service_name, trace_id, span_id).
	type joinKey struct {
		serviceName string
		traceID     string
		spanID      string
	}
	spansByKey := make(map[joinKey]map[string]any)
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
		spansByKey[joinKey{serviceName: svc, traceID: tid, spanID: sid}] = attrs

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

		tid, _ := l["trace_id"].(string)
		sid, _ := l["span_id"].(string)
		if _, ok := spansByKey[joinKey{serviceName: svc, traceID: tid, spanID: sid}]; !ok {
			t.Errorf("log row (service=%q, trace=%q, span=%q) did not join to any span in v_gen_ai_spans", svc, tid, sid)
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
}
