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
	VerifiedDate  string             `yaml:"verified_date"`
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
	anthropicFallbackVerified := false

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

		lSys, _ := lAttrs["gen_ai.system"].(string)
		lInv, _ := lAttrs["gcp.vertex.agent.invocation_id"].(string)

		if !strings.HasPrefix(promotedEventName, "gen_ai.") && lSys == "" && lInv == "" {
			t.Fatalf("log row failed v_gen_ai_logs WHERE predicate: event_name=%q attrs=%v", promotedEventName, lAttrs)
		}
		if lSys != "" {
			systemsSeenInLogs[lSys] = true
		}

		if lSys == "anthropic" && promotedEventName == "gen_ai.cost" && attrEventName == "gen_ai.cost" {
			anthropicFallbackVerified = true
		}

		svc, _ := l["service_name"].(string)
		tid, _ := l["trace_id"].(string)
		sid, _ := l["span_id"].(string)
		if _, ok := spansByKey[joinKey{serviceName: svc, traceID: tid, spanID: sid}]; !ok {
			t.Errorf("log row (service=%q, trace=%q, span=%q) did not join to any span in v_gen_ai_spans", svc, tid, sid)
		}
	}

	if !anthropicFallbackVerified {
		t.Fatalf("expected anthropic_agent_sdk log fixture to exercise event.name -> gen_ai.cost promotion")
	}
	for _, fw := range m.Frameworks {
		if slices.Contains(fw.Signals, "logs") && fw.GenAISystem != "" {
			if !systemsSeenInLogs[fw.GenAISystem] {
				t.Errorf("framework %s (gen_ai.system=%q) was not observed in v_gen_ai_logs rows", fw.ID, fw.GenAISystem)
			}
		}
	}
}
