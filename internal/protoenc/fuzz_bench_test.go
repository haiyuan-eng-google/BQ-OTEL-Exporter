// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package protoenc

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/transform"
)

func buildDynamicDescriptor(t testing.TB, dp *descriptorpb.DescriptorProto) protoreflect.MessageDescriptor {
	t.Helper()
	fdp := &descriptorpb.FileDescriptorProto{
		Syntax:      proto.String("proto2"),
		Name:        proto.String("fuzz_row.proto"),
		MessageType: []*descriptorpb.DescriptorProto{dp},
	}
	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		t.Fatalf("protodesc.NewFile: %v", err)
	}
	return fd.Messages().Get(0)
}

func FuzzProtoEncRoundTrip(f *testing.F) {
	f.Add("0102030405060708090a0b0c0d0e0f10", "0102030405060708", "invoke_agent", int64(42000), int64(1700000000123456), `{"gen_ai.system":"gcp.vertex.agent"}`)
	f.Add("ffffffffffffffffffffffffffffffff", "0000000000000000", "", int64(0), int64(0), `{}`)
	f.Add("abcdef0123456789abcdef0123456789", "1234567890abcdef", "call_llm", int64(999999999), int64(1800000000000000), `{"k":"v","num":123}`)

	enc, err := New(schema.SpansTableSchema())
	if err != nil {
		f.Fatalf("New(SpansTableSchema): %v", err)
	}
	md := buildDynamicDescriptor(f, enc.Descriptor())

	f.Fuzz(func(t *testing.T, traceID, spanID, name string, durationNano, startMicros int64, attrsJSON string) {
		endMicros := startMicros + durationNano/1000
		startNano := startMicros * 1000
		endNano := startNano + durationNano

		row := transform.Row{
			"start_time":               startMicros,
			"start_time_unix_nano":     startNano,
			"end_time":                 endMicros,
			"end_time_unix_nano":       endNano,
			"receive_time":             startMicros,
			"receive_time_unix_nano":   startNano,
			"duration_nano":            durationNano,
			"trace_id":                 traceID,
			"span_id":                  spanID,
			"parent_span_id":           "",
			"trace_state":              "",
			"flags":                    int64(1),
			"name":                     name,
			"kind":                     int64(3),
			"kind_name":                "SPAN_KIND_CLIENT",
			"status":                   transform.Row{"code": int64(1), "code_name": "STATUS_CODE_OK", "message": ""},
			"attributes":               attrsJSON,
			"attributes_schema":        `{}`,
			"dropped_attributes_count": int64(0),
			"service_name":             "fuzz-agent",
			"resource":                 transform.Row{"attributes": `{}`, "attributes_schema": `{}`, "dropped_attributes_count": int64(0)},
			"resource_schema_url":      "",
			"instrumentation_scope":    transform.Row{"name": "fuzz", "version": "1.0", "attributes": `{}`, "attributes_schema": `{}`, "dropped_attributes_count": int64(0)},
			"scope_schema_url":         "",
			"events":                   []transform.Row{},
			"dropped_events_count":     int64(0),
			"links":                    []transform.Row{},
			"dropped_links_count":      int64(0),
			"payload_refs":             []transform.Row{},
			"schema_version":           schema.Version,
		}

		wire, err := enc.Encode(row)
		if err != nil {
			t.Fatalf("enc.Encode failed: %v", err)
		}

		msg := dynamicpb.NewMessage(md)
		if err := proto.Unmarshal(wire, msg); err != nil {
			t.Fatalf("dynamicpb Unmarshal failed: %v", err)
		}

		gotTraceID := msg.Get(md.Fields().ByName("trace_id")).String()
		if gotTraceID != traceID {
			t.Fatalf("round-trip trace_id mismatch: got %q, want %q", gotTraceID, traceID)
		}
		gotName := msg.Get(md.Fields().ByName("name")).String()
		if gotName != name {
			t.Fatalf("round-trip name mismatch: got %q, want %q", gotName, name)
		}
		gotDuration := msg.Get(md.Fields().ByName("duration_nano")).Int()
		if gotDuration != durationNano {
			t.Fatalf("round-trip duration_nano mismatch: got %d, want %d", gotDuration, durationNano)
		}
		gotStartMicros := msg.Get(md.Fields().ByName("start_time")).Int()
		if gotStartMicros != startMicros {
			t.Fatalf("round-trip start_time mismatch: got %d, want %d", gotStartMicros, startMicros)
		}
	})
}

func BenchmarkProtoEncEncode(b *testing.B) {
	enc, err := New(schema.SpansTableSchema())
	if err != nil {
		b.Fatalf("New(SpansTableSchema): %v", err)
	}
	now := time.Unix(1700000000, 0).UTC()
	nowMicros := now.UnixMicro()
	endMicros := now.Add(250 * time.Millisecond).UnixMicro()
	row := transform.Row{
		"start_time":               nowMicros,
		"start_time_unix_nano":     now.UnixNano(),
		"end_time":                 endMicros,
		"end_time_unix_nano":       now.Add(250 * time.Millisecond).UnixNano(),
		"receive_time":             nowMicros,
		"receive_time_unix_nano":   now.UnixNano(),
		"duration_nano":            int64(250_000_000),
		"trace_id":                 "0102030405060708090a0b0c0d0e0f10",
		"span_id":                  "0102030405060708",
		"parent_span_id":           "0000000000000001",
		"trace_state":              "",
		"flags":                    int64(1),
		"name":                     "invoke_agent",
		"kind":                     int64(3),
		"kind_name":                "SPAN_KIND_CLIENT",
		"status":                   transform.Row{"code": int64(1), "code_name": "STATUS_CODE_OK", "message": ""},
		"attributes":               `{"gen_ai.system":"gcp.vertex.agent","gen_ai.request.model":"gemini-2.5-pro"}`,
		"attributes_schema":        `{"gen_ai.system":{"type":"string"},"gen_ai.request.model":{"type":"string"}}`,
		"dropped_attributes_count": int64(0),
		"service_name":             "agent",
		"resource":                 transform.Row{"attributes": `{"service.name":"agent"}`, "attributes_schema": `{"service.name":{"type":"string"}}`, "dropped_attributes_count": int64(0)},
		"resource_schema_url":      "",
		"instrumentation_scope":    transform.Row{"name": "genai", "version": "0.1.0", "attributes": `{}`, "attributes_schema": `{}`, "dropped_attributes_count": int64(0)},
		"scope_schema_url":         "",
		"events": []transform.Row{
			{
				"time":                     nowMicros,
				"time_unix_nano":           now.UnixNano(),
				"name":                     "gen_ai.content.prompt",
				"attributes":               `{"gen_ai.prompt":"hello"}`,
				"attributes_schema":        `{"gen_ai.prompt":{"type":"string"}}`,
				"dropped_attributes_count": int64(0),
			},
		},
		"dropped_events_count": int64(0),
		"links":                []transform.Row{},
		"dropped_links_count":  int64(0),
		"payload_refs": []transform.Row{
			{
				"attribute_path": "attributes.gen_ai.prompt",
				"part_index":     int64(0),
				"mime_type":      "application/json",
				"storage_mode":   "GCS_OFFLOAD",
				"byte_size":      int64(4096),
				"sha256":         "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				"object_ref": transform.Row{
					"uri":        "gs://bucket/obj",
					"version":    "1",
					"authorizer": "projects/p/locations/us/connections/c",
					"details":    `{}`,
				},
			},
		},
		"schema_version": schema.Version,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wire, err := enc.Encode(row)
		if err != nil || len(wire) == 0 {
			b.Fatalf("Encode failed: %v", err)
		}
	}
}
