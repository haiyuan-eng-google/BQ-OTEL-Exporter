// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package protoenc

import (
	"testing"
	"time"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/transform"
)

func opts() transform.Options {
	fixed := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	return transform.Options{
		Limits: transform.DefaultLimits(),
		Now:    func() time.Time { return fixed },
	}
}

// The full chain: pdata to rows to protobuf bytes, against the real §7.1
// schema. If the transform emits a column the schema does not declare, or a Go
// type the descriptor does not accept, this is where it surfaces.
func TestEncodeSpanRow(t *testing.T) {
	enc, err := New(schema.SpansTableSchema())
	if err != nil {
		t.Fatal(err)
	}

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "checkout")
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("GET /cart")
	span.SetTraceID(pcommon.TraceID{1, 2, 3})
	span.SetSpanID(pcommon.SpanID{4, 5, 6})
	span.SetStartTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000))
	span.SetEndTimestamp(pcommon.Timestamp(1_700_000_001_000_000_000))
	span.Attributes().PutInt("http.status_code", 200)
	ev := span.Events().AppendEmpty()
	ev.SetName("cache.miss")
	ev.SetTimestamp(pcommon.Timestamp(1_700_000_000_500_000_000))

	rows, rejects := transform.SpanRows(td, opts())
	if len(rejects) != 0 {
		t.Fatalf("unexpected rejections: %+v", rejects)
	}

	b, err := enc.Encode(rows[0])
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(b) == 0 {
		t.Fatal("encoded row is empty")
	}

	// Decode against the same descriptor and confirm the values survived.
	msg := dynamicpb.NewMessage(enc.md)
	if err := proto.Unmarshal(b, msg); err != nil {
		t.Fatalf("round trip failed: %v", err)
	}
	if got := stringField(t, msg, "name"); got != "GET /cart" {
		t.Errorf("name = %q", got)
	}
	if got := stringField(t, msg, "service_name"); got != "checkout" {
		t.Errorf("service_name = %q", got)
	}
	if got := stringField(t, msg, "schema_version"); got != schema.Version {
		t.Errorf("schema_version = %q, want %q", got, schema.Version)
	}

	// Repeated STRUCT columns must survive as nested messages, not JSON.
	fd := enc.md.Fields().ByName("events")
	if !fd.IsList() {
		t.Fatal("events must be a repeated field")
	}
	if n := msg.Get(fd).List().Len(); n != 1 {
		t.Fatalf("events length = %d, want 1", n)
	}
}

func TestEncodeLogRow(t *testing.T) {
	enc, err := New(schema.LogsTableSchema())
	if err != nil {
		t.Fatal(err)
	}

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "checkout")
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000))
	lr.Body().SetStr("disk full")

	rows, rejects := transform.LogRows(ld, opts())
	if len(rejects) != 0 {
		t.Fatalf("unexpected rejections: %+v", rejects)
	}
	b, err := enc.Encode(rows[0])
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	msg := dynamicpb.NewMessage(enc.md)
	if err := proto.Unmarshal(b, msg); err != nil {
		t.Fatal(err)
	}
	if got := stringField(t, msg, "body"); got != `{"string_value":"disk full"}` {
		t.Errorf("body = %q", got)
	}
}

// A column the table does not have must fail loudly. Dropping it silently
// would turn a schema drift bug into missing data nobody notices.
func TestEncodeRejectsUnknownColumn(t *testing.T) {
	enc, err := New(schema.LogsTableSchema())
	if err != nil {
		t.Fatal(err)
	}
	_, err = enc.Encode(transform.Row{"not_a_column": "x"})
	if err == nil {
		t.Fatal("expected an error for an unknown column")
	}
}

func TestSchemaMismatch(t *testing.T) {
	want := schema.LogsTableSchema()

	if got := SchemaMismatch(want, want); len(got) != 0 {
		t.Errorf("identical schemas reported a mismatch: %v", got)
	}

	// Additive evolution: an extra column in the table is fine, since a writer
	// pinned to an older contract just leaves it NULL.
	extra := schema.LogsTableSchema()
	extra.Fields = append(extra.Fields, &storagepb.TableFieldSchema{
		Name: "future_column", Type: storagepb.TableFieldSchema_STRING,
		Mode: storagepb.TableFieldSchema_NULLABLE,
	})
	if got := SchemaMismatch(want, extra); len(got) != 0 {
		t.Errorf("an extra table column must not be a mismatch, got %v", got)
	}

	// A missing column is a real mismatch: rows would target a shape the
	// table cannot accept.
	short := &storagepb.TableSchema{Fields: want.GetFields()[:3]}
	if got := SchemaMismatch(want, short); len(got) == 0 {
		t.Error("missing columns must be reported")
	}
}

func TestDescriptorIsSelfContained(t *testing.T) {
	enc, err := New(schema.SpansTableSchema())
	if err != nil {
		t.Fatal(err)
	}
	d := enc.Descriptor()
	if d == nil {
		t.Fatal("descriptor is nil")
	}
	// Nested types must be inlined: the service has no access to our registry,
	// so a descriptor that merely references them would be unusable.
	if len(d.GetNestedType()) == 0 {
		t.Error("expected the events and links struct types to be inlined")
	}
}

func stringField(t *testing.T, msg protoreflect.Message, name string) string {
	t.Helper()
	fd := msg.Descriptor().Fields().ByName(protoreflect.Name(name))
	if fd == nil {
		t.Fatalf("field %q not found", name)
	}
	return msg.Get(fd).String()
}
