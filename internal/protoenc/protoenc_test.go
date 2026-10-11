// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package protoenc

import (
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
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

// The full chain: pdata to rows to protobuf bytes, against the real span table
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

	// The singular STRUCT columns and INT64 nanos must reach the service
	// intact, so check them through the descriptor the write stream gets.
	norm := decodeNormalized(t, enc, b)
	for path, want := range map[string]string{
		"kind_name":                        "SPAN_KIND_UNSPECIFIED",
		"status.code_name":                 "STATUS_CODE_UNSET",
		"resource.attributes":              `{"service.name":"checkout"}`,
		"resource.attributes_schema":       `{"service.name":"string"}`,
		"instrumentation_scope.attributes": "{}",
	} {
		if got := get(t, norm, path).String(); got != want {
			t.Errorf("%s = %q, want %q", path, got, want)
		}
	}
	if got := get(t, norm, "end_time_unix_nano").Int(); got != 1_700_000_001_000_000_000 {
		t.Errorf("end_time_unix_nano = %d, want the exact nanos", got)
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
	if got := stringField(t, msg, "body"); got != `"disk full"` {
		t.Errorf("body = %q", got)
	}

	// The singular STRUCT columns, identity columns and INT64 nanos must
	// reach the service intact, so check them through the descriptor the
	// write stream gets.
	norm := decodeNormalized(t, enc, b)
	for path, want := range map[string]string{
		"resource.attributes":        `{"service.name":"checkout"}`,
		"resource.attributes_schema": `{"service.name":"string"}`,
		"log_name":                   "checkout",
		"schema_version":             schema.Version,
	} {
		if got := get(t, norm, path).String(); got != want {
			t.Errorf("%s = %q, want %q", path, got, want)
		}
	}
	if got := get(t, norm, "time_unix_nano").Int(); got != 1_700_000_000_000_000_000 {
		t.Errorf("time_unix_nano = %d, want the exact nanos", got)
	}
	if got := get(t, norm, "insert_id").String(); got != rows[0]["record_fingerprint"] {
		t.Errorf("insert_id = %q, want the record fingerprint %q", got, rows[0]["record_fingerprint"])
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

// Shorthand for the table schemas in the nested STRUCT tests.
const (
	nullable = storagepb.TableFieldSchema_NULLABLE
	required = storagepb.TableFieldSchema_REQUIRED
	repeated = storagepb.TableFieldSchema_REPEATED

	tString    = storagepb.TableFieldSchema_STRING
	tInt64     = storagepb.TableFieldSchema_INT64
	tJSON      = storagepb.TableFieldSchema_JSON
	tTimestamp = storagepb.TableFieldSchema_TIMESTAMP
	tStruct    = storagepb.TableFieldSchema_STRUCT
)

func col(name string, typ storagepb.TableFieldSchema_Type, mode storagepb.TableFieldSchema_Mode, fields ...*storagepb.TableFieldSchema) *storagepb.TableFieldSchema {
	return &storagepb.TableFieldSchema{Name: name, Type: typ, Mode: mode, Fields: fields}
}

// attrCols appends the attribute columns that every STRUCT in the nested
// schema carries, so sibling STRUCTs share these sub-field names.
func attrCols(lead ...*storagepb.TableFieldSchema) []*storagepb.TableFieldSchema {
	return append(lead,
		col("attributes", tJSON, nullable),
		col("attributes_schema", tJSON, nullable),
		col("dropped_attributes_count", tInt64, nullable),
	)
}

// nestedSchema has the nested shape of a span table with STRUCT columns:
// singular STRUCTs whose sub-fields share names, repeated STRUCTs, and a
// singular STRUCT inside a repeated one.
func nestedSchema() *storagepb.TableSchema {
	return &storagepb.TableSchema{Fields: []*storagepb.TableFieldSchema{
		col("span_id", tString, required),
		col("status", tStruct, nullable,
			col("code", tInt64, nullable),
			col("code_name", tString, nullable),
			col("message", tString, nullable)),
		col("resource", tStruct, nullable, attrCols()...),
		col("instrumentation_scope", tStruct, nullable, attrCols(
			col("name", tString, nullable),
			col("version", tString, nullable))...),
		col("events", tStruct, repeated, attrCols(
			col("time", tTimestamp, nullable),
			col("time_unix_nano", tInt64, nullable),
			col("name", tString, nullable))...),
		col("links", tStruct, repeated, attrCols(
			col("trace_id", tString, nullable),
			col("span_id", tString, nullable),
			col("trace_state", tString, nullable),
			col("flags", tInt64, nullable))...),
		col("payload_refs", tStruct, repeated,
			col("attribute_path", tString, nullable),
			col("part_index", tInt64, nullable),
			col("mime_type", tString, nullable),
			col("storage_mode", tString, nullable),
			col("byte_size", tInt64, nullable),
			col("sha256", tString, nullable),
			col("object_ref", tStruct, nullable,
				col("uri", tString, nullable),
				col("version", tString, nullable),
				col("authorizer", tString, nullable),
				col("details", tJSON, nullable))),
	}}
}

func newNestedEncoder(t *testing.T) *Encoder {
	t.Helper()
	enc, err := New(nestedSchema())
	if err != nil {
		t.Fatal(err)
	}
	return enc
}

// Singular STRUCT columns encode as nested messages with every sub-field in
// place, and sub-fields the row leaves out stay NULL.
func TestEncodeSingularStruct(t *testing.T) {
	enc := newNestedEncoder(t)
	b, err := enc.Encode(transform.Row{
		"span_id": "0102030405060708",
		"status":  transform.Row{"code": int64(2), "code_name": "STATUS_CODE_ERROR", "message": "upstream timeout"},
		"resource": transform.Row{
			"attributes":               `{"service.name":"checkout"}`,
			"attributes_schema":        `{"service.name":"string"}`,
			"dropped_attributes_count": int64(0),
		},
		"instrumentation_scope": transform.Row{"name": "otel-go", "version": "1.2.3"},
	})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	msg := decode(t, enc, b)

	for path, want := range map[string]any{
		"status.code":                       int64(2),
		"status.code_name":                  "STATUS_CODE_ERROR",
		"status.message":                    "upstream timeout",
		"resource.attributes":               `{"service.name":"checkout"}`,
		"resource.attributes_schema":        `{"service.name":"string"}`,
		"resource.dropped_attributes_count": int64(0),
		"instrumentation_scope.name":        "otel-go",
		"instrumentation_scope.version":     "1.2.3",
	} {
		if got := get(t, msg, path).Interface(); got != want {
			t.Errorf("%s = %v (%T), want %v (%T)", path, got, got, want, want)
		}
	}
	if scope := get(t, msg, "instrumentation_scope").Message(); has(t, scope, "attributes") {
		t.Error("instrumentation_scope.attributes is set, but the row left it out")
	}
}

// A nil STRUCT value, typed or untyped, leaves the field unset, which BigQuery
// stores as a NULL STRUCT.
func TestEncodeNullStruct(t *testing.T) {
	enc := newNestedEncoder(t)
	for name, val := range map[string]any{
		"untyped nil":   nil,
		"nil Row":       transform.Row(nil),
		"nil plain map": map[string]any(nil),
	} {
		t.Run(name, func(t *testing.T) {
			b, err := enc.Encode(transform.Row{"span_id": "0102030405060708", "status": val})
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if has(t, decode(t, enc, b), "status") {
				t.Error("status is set, want it left NULL")
			}
		})
	}
}

// An empty but non-nil row is a present STRUCT whose sub-fields are all NULL,
// which BigQuery distinguishes from a NULL STRUCT.
func TestEncodeEmptyStructIsPresent(t *testing.T) {
	enc := newNestedEncoder(t)
	b, err := enc.Encode(transform.Row{"span_id": "0102030405060708", "status": transform.Row{}})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	status := get(t, decode(t, enc, b), "status").Message()
	for _, name := range []string{"code", "code_name", "message"} {
		if has(t, status, name) {
			t.Errorf("status.%s is set, want NULL", name)
		}
	}
}

// A STRUCT built as a plain map is accepted just like a transform.Row.
func TestEncodeStructAcceptsPlainMap(t *testing.T) {
	enc := newNestedEncoder(t)
	b, err := enc.Encode(transform.Row{
		"span_id": "0102030405060708",
		"status":  map[string]any{"code": int64(1)},
	})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if got := get(t, decode(t, enc, b), "status.code").Int(); got != 1 {
		t.Errorf("status.code = %d, want 1", got)
	}
}

// A STRUCT value of the wrong shape fails the row, and the error names the
// column path down to the offending field.
func TestEncodeStructErrors(t *testing.T) {
	enc := newNestedEncoder(t)
	tests := []struct {
		name string
		row  transform.Row
		want []string // substrings the error must contain
	}{
		{"scalar for struct", transform.Row{"status": "ERROR"},
			[]string{`column "status"`, "got string"}},
		{"list for singular struct", transform.Row{"status": []transform.Row{{"code": int64(1)}}},
			[]string{`column "status"`, "got []transform.Row"}},
		{"unknown sub-column", transform.Row{"status": transform.Row{"bogus": "x"}},
			[]string{`column "status"`, `column "bogus" is not present`}},
		{"sub-column type mismatch", transform.Row{"status": transform.Row{"code": "two"}},
			[]string{`column "status"`, `column "code"`, "expected int64"}},
		{"inside repeated struct", transform.Row{"payload_refs": []transform.Row{{"object_ref": "not a struct"}}},
			[]string{`column "payload_refs"[0]`, `column "object_ref"`, "got string"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.row["span_id"] = "0102030405060708"
			_, err := enc.Encode(tc.row)
			if err == nil {
				t.Fatal("Encode succeeded, want an error")
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not contain %q", err, want)
				}
			}
		})
	}
}

// A singular STRUCT inside a repeated STRUCT, as payload_refs[].object_ref
// is, can be present on one element and NULL on the next.
func TestEncodeStructInsideRepeatedStruct(t *testing.T) {
	enc := newNestedEncoder(t)
	b, err := enc.Encode(transform.Row{
		"span_id": "0102030405060708",
		"payload_refs": []transform.Row{
			{
				"attribute_path": "gen_ai.input.messages",
				"part_index":     int64(0),
				"mime_type":      "image/png",
				"storage_mode":   "OBJECT_REF",
				"byte_size":      int64(2048),
				"sha256":         "9f86d081884c7d65",
				"object_ref": transform.Row{
					"uri":        "gs://bucket/object.png",
					"version":    "1700000000000000",
					"authorizer": "project.us.connection",
					"details":    `{"content_type":"image/png"}`,
				},
			},
			{"attribute_path": "gen_ai.output.messages", "part_index": int64(1), "object_ref": nil},
		},
	})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	refs := decode(t, enc, b).Get(enc.md.Fields().ByName("payload_refs")).List()
	if refs.Len() != 2 {
		t.Fatalf("payload_refs length = %d, want 2", refs.Len())
	}
	first, second := refs.Get(0).Message(), refs.Get(1).Message()
	for path, want := range map[string]any{
		"byte_size":             int64(2048),
		"object_ref.uri":        "gs://bucket/object.png",
		"object_ref.version":    "1700000000000000",
		"object_ref.authorizer": "project.us.connection",
		"object_ref.details":    `{"content_type":"image/png"}`,
	} {
		if got := get(t, first, path).Interface(); got != want {
			t.Errorf("payload_refs[0].%s = %v, want %v", path, got, want)
		}
	}
	if got := get(t, second, "attribute_path").String(); got != "gen_ai.output.messages" {
		t.Errorf("payload_refs[1].attribute_path = %q", got)
	}
	if has(t, second, "object_ref") {
		t.Error("payload_refs[1].object_ref is set, want NULL")
	}
}

// Sibling STRUCTs share the sub-field names attributes, attributes_schema and
// dropped_attributes_count. Decoded against the normalized descriptor that the
// write stream receives, every value must land in its own STRUCT, which holds
// only if each nested message type keeps a distinct name.
func TestEncodeSiblingStructsThroughNormalizedDescriptor(t *testing.T) {
	enc := newNestedEncoder(t)
	b, err := enc.Encode(transform.Row{
		"span_id":               "0102030405060708",
		"resource":              transform.Row{"attributes": `{"from":"resource"}`, "dropped_attributes_count": int64(1)},
		"instrumentation_scope": transform.Row{"name": "otel-go", "attributes": `{"from":"scope"}`, "dropped_attributes_count": int64(2)},
		"events":                []transform.Row{{"name": "retry", "attributes": `{"from":"event"}`, "dropped_attributes_count": int64(3)}},
		"links":                 []transform.Row{{"span_id": "1112131415161718", "attributes": `{"from":"link"}`, "dropped_attributes_count": int64(4)}},
		"payload_refs":          []transform.Row{{"object_ref": transform.Row{"uri": "gs://bucket/o", "details": `{"from":"object_ref"}`}}},
	})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	msg := decodeNormalized(t, enc, b)
	only := func(name string) protoreflect.Message {
		t.Helper()
		l := msg.Get(msg.Descriptor().Fields().ByName(protoreflect.Name(name))).List()
		if l.Len() != 1 {
			t.Fatalf("%s length = %d, want 1", name, l.Len())
		}
		return l.Get(0).Message()
	}
	for _, c := range []struct {
		where string
		msg   protoreflect.Message
		path  string
		want  any
	}{
		{"row", msg, "resource.attributes", `{"from":"resource"}`},
		{"row", msg, "resource.dropped_attributes_count", int64(1)},
		{"row", msg, "instrumentation_scope.attributes", `{"from":"scope"}`},
		{"row", msg, "instrumentation_scope.dropped_attributes_count", int64(2)},
		{"events[0]", only("events"), "attributes", `{"from":"event"}`},
		{"events[0]", only("events"), "dropped_attributes_count", int64(3)},
		{"links[0]", only("links"), "attributes", `{"from":"link"}`},
		{"links[0]", only("links"), "dropped_attributes_count", int64(4)},
		{"payload_refs[0]", only("payload_refs"), "object_ref.details", `{"from":"object_ref"}`},
	} {
		if got := get(t, c.msg, c.path).Interface(); got != c.want {
			t.Errorf("%s: %s = %v, want %v", c.where, c.path, got, c.want)
		}
	}

	seen := make(map[string]bool)
	for _, nt := range enc.Descriptor().GetNestedType() {
		if seen[nt.GetName()] {
			t.Errorf("nested type %q is defined more than once", nt.GetName())
		}
		seen[nt.GetName()] = true
	}
}

// Sibling STRUCTs that each hold a STRUCT sub-field of the same name but a
// different shape need distinct message types. Naming nested types without
// their parent would give both the same type and encode one with the other's
// layout.
func TestDescriptorScopesNestedStructNames(t *testing.T) {
	enc, err := New(&storagepb.TableSchema{Fields: []*storagepb.TableFieldSchema{
		col("resource", tStruct, nullable,
			col("meta", tStruct, nullable, col("origin", tString, nullable))),
		col("instrumentation_scope", tStruct, nullable,
			col("meta", tStruct, nullable, col("depth", tInt64, nullable))),
	}})
	if err != nil {
		t.Fatal(err)
	}
	b, err := enc.Encode(transform.Row{
		"resource":              transform.Row{"meta": transform.Row{"origin": "sdk"}},
		"instrumentation_scope": transform.Row{"meta": transform.Row{"depth": int64(7)}},
	})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	msg := decodeNormalized(t, enc, b)
	if got := get(t, msg, "resource.meta.origin").String(); got != "sdk" {
		t.Errorf("resource.meta.origin = %q, want sdk", got)
	}
	if got := get(t, msg, "instrumentation_scope.meta.depth").Int(); got != 7 {
		t.Errorf("instrumentation_scope.meta.depth = %d, want 7", got)
	}
	resMeta := get(t, msg, "resource.meta").Message().Descriptor().FullName()
	scopeMeta := get(t, msg, "instrumentation_scope.meta").Message().Descriptor().FullName()
	if resMeta == scopeMeta {
		t.Errorf("both meta sub-fields use message type %s", resMeta)
	}
}

// decode decodes b against the encoder's own descriptor.
func decode(t *testing.T, enc *Encoder, b []byte) protoreflect.Message {
	t.Helper()
	msg := dynamicpb.NewMessage(enc.md)
	if err := proto.Unmarshal(b, msg); err != nil {
		t.Fatalf("round trip failed: %v", err)
	}
	return msg
}

// decodeNormalized decodes b against the normalized descriptor the encoder
// hands to the write stream, which is the only view of the bytes the service
// has.
func decodeNormalized(t *testing.T, enc *Encoder, b []byte) protoreflect.Message {
	t.Helper()
	fd, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:        proto.String("normalized.proto"),
		Syntax:      proto.String("proto2"),
		MessageType: []*descriptorpb.DescriptorProto{enc.Descriptor()},
	}, nil)
	if err != nil {
		t.Fatalf("normalized descriptor does not form a valid file: %v", err)
	}
	msg := dynamicpb.NewMessage(fd.Messages().Get(0))
	if err := proto.Unmarshal(b, msg); err != nil {
		t.Fatalf("decoding against the normalized descriptor: %v", err)
	}
	return msg
}

// get returns the value at a dotted path of singular fields, failing the test
// if a step does not exist or is unset.
func get(t *testing.T, msg protoreflect.Message, path string) protoreflect.Value {
	t.Helper()
	names := strings.Split(path, ".")
	for _, name := range names[:len(names)-1] {
		if !has(t, msg, name) {
			t.Fatalf("%s: %q is unset", path, name)
		}
		msg = msg.Get(msg.Descriptor().Fields().ByName(protoreflect.Name(name))).Message()
	}
	last := names[len(names)-1]
	if !has(t, msg, last) {
		t.Fatalf("%s: %q is unset", path, last)
	}
	return msg.Get(msg.Descriptor().Fields().ByName(protoreflect.Name(last)))
}

// has reports whether the named field is set on msg.
func has(t *testing.T, msg protoreflect.Message, name string) bool {
	t.Helper()
	fd := msg.Descriptor().Fields().ByName(protoreflect.Name(name))
	if fd == nil {
		t.Fatalf("%s has no field %q", msg.Descriptor().FullName(), name)
	}
	return msg.Has(fd)
}

// Quarantine rows must encode against the published quarantine schema, with
// every absent optional column left NULL rather than empty.
func TestEncodeQuarantineRow(t *testing.T) {
	enc, err := New(schema.QuarantineTableSchema())
	if err != nil {
		t.Fatal(err)
	}
	o := opts()
	o.ExporterInstance = "collector-7"
	o.MaxRowBytes = 1 << 20

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "checkout")
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("GET /cart")
	span.SetSpanID(pcommon.SpanID{1, 2, 3, 4, 5, 6, 7, 8})
	span.SetStartTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000))
	span.SetEndTimestamp(pcommon.Timestamp(1_700_000_001_000_000_000))
	span.Attributes().PutInt("http.status_code", 200)

	_, rejects := transform.SpanRows(td, o)
	if len(rejects) != 1 {
		t.Fatalf("rejections = %+v, want the span without a trace ID", rejects)
	}
	rows := transform.SpanQuarantineRows(td, []transform.QuarantineEntry{{Rejection: rejects[0]}}, o)
	if len(rows) != 1 {
		t.Fatalf("got %d quarantine rows, want 1", len(rows))
	}
	b, err := enc.Encode(rows[0])
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	norm := decodeNormalized(t, enc, b)
	for path, want := range map[string]string{
		"signal":            "spans",
		"error_code":        "INVALID_TRACE_ID",
		"error_message":     "invalid_id: trace_id",
		"service_name":      "checkout",
		"span_id":           "0102030405060708",
		"raw_attributes":    `{"http.status_code":"200"}`,
		"exporter_instance": "collector-7",
		"schema_version":    schema.Version,
	} {
		if got := get(t, norm, path).String(); got != want {
			t.Errorf("%s = %q, want %q", path, got, want)
		}
	}
	wantIngest := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC).UnixMicro()
	if got := get(t, norm, "ingest_time").Int(); got != wantIngest {
		t.Errorf("ingest_time = %d, want %d", got, wantIngest)
	}
	if got := get(t, norm, "raw_payload").String(); !strings.Contains(got, `"spanId":"0102030405060708"`) {
		t.Errorf("raw_payload = %q, want the span as OTLP/JSON", got)
	}
	for _, name := range []string{"trace_id", "payload_ref"} {
		if has(t, norm, name) {
			t.Errorf("%s is set, want NULL", name)
		}
	}

	ld := plog.NewLogs()
	lr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.Timestamp(1 << 63))
	lr.Body().SetStr("disk full")
	_, logRejects := transform.LogRows(ld, o)
	if len(logRejects) != 1 {
		t.Fatalf("rejections = %+v, want the record with an overflowing time", logRejects)
	}
	entries := []transform.QuarantineEntry{{Rejection: logRejects[0], Detail: "seen twice"}}
	logRows := transform.LogQuarantineRows(ld, entries, o)
	if len(logRows) != 1 {
		t.Fatalf("got %d quarantine rows, want 1", len(logRows))
	}
	lb, err := enc.Encode(logRows[0])
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	logNorm := decodeNormalized(t, enc, lb)
	for path, want := range map[string]string{
		"signal":         "logs",
		"error_code":     "TIMESTAMP_OVERFLOW",
		"error_message":  "timestamp_overflow: time_unix_nano: seen twice",
		"raw_attributes": "{}",
		"schema_version": schema.Version,
	} {
		if got := get(t, logNorm, path).String(); got != want {
			t.Errorf("log %s = %q, want %q", path, got, want)
		}
	}
	if got := get(t, logNorm, "raw_payload").String(); !strings.Contains(got, `"disk full"`) {
		t.Errorf("log raw_payload = %q, want the record as OTLP/JSON", got)
	}
	for _, name := range []string{"service_name", "trace_id", "span_id", "payload_ref"} {
		if has(t, logNorm, name) {
			t.Errorf("log %s is set, want NULL", name)
		}
	}
}

type stubOffloader struct {
	refs []transform.PayloadRef
}

func (s stubOffloader) OffloadSpan(_ ptrace.Span, _ pcommon.Resource, _ pcommon.InstrumentationScope) []transform.PayloadRef {
	return s.refs
}

func (s stubOffloader) OffloadLog(_ plog.LogRecord, _ pcommon.Resource, _ pcommon.InstrumentationScope) []transform.PayloadRef {
	return s.refs
}

func TestEncodeSpanAndLogRowsWithPayloadRefs(t *testing.T) {
	ref := transform.PayloadRef{
		AttributePath: "attributes.gen_ai.prompt",
		PartIndex:     0,
		MIMEType:      "application/json",
		StorageMode:   "gcs_object_ref",
		ByteSize:      16384,
		SHA256:        strings.Repeat("c", 64),
		ObjectRef: transform.ObjectRef{
			URI:        "gs://bucket/offload/prompt-1.json",
			Version:    "101",
			Authorizer: "projects/my-proj/locations/us/connections/gcs",
			Details:    `{"gcs_metadata":{"content_type":"application/json"}}`,
		},
	}
	o := opts()
	o.PayloadOffloader = stubOffloader{refs: []transform.PayloadRef{ref}}

	td := ptrace.NewTraces()
	span := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetTraceID(pcommon.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	span.SetSpanID(pcommon.SpanID{1, 2, 3, 4, 5, 6, 7, 8})
	span.SetName("chat")
	span.SetStartTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000))
	span.SetEndTimestamp(pcommon.Timestamp(1_700_000_001_000_000_000))

	spanRows, rejects := transform.SpanRows(td, o)
	if len(rejects) != 0 || len(spanRows) != 1 {
		t.Fatalf("SpanRows = (%d, %v), want (1, nil)", len(spanRows), rejects)
	}
	spanEnc, err := New(schema.SpansTableSchema())
	if err != nil {
		t.Fatal(err)
	}
	sb, err := spanEnc.Encode(spanRows[0])
	if err != nil {
		t.Fatalf("spanEnc.Encode: %v", err)
	}
	spanNorm := decodeNormalized(t, spanEnc, sb)
	spanRefList := spanNorm.Get(spanNorm.Descriptor().Fields().ByName("payload_refs")).List()
	if spanRefList.Len() != 1 {
		t.Fatalf("span payload_refs len = %d, want 1", spanRefList.Len())
	}
	spanRefMsg := spanRefList.Get(0).Message()
	if got := get(t, spanRefMsg, "attribute_path").String(); got != "attributes.gen_ai.prompt" {
		t.Errorf("span payload_refs[0].attribute_path = %q", got)
	}
	if got := get(t, spanRefMsg, "object_ref.uri").String(); got != "gs://bucket/offload/prompt-1.json" {
		t.Errorf("span payload_refs[0].object_ref.uri = %q", got)
	}
	if got := get(t, spanRefMsg, "byte_size").Int(); got != 16384 {
		t.Errorf("span payload_refs[0].byte_size = %d, want 16384", got)
	}

	ld := plog.NewLogs()
	lr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000))
	lr.Body().SetStr("ok")
	logRows, logRejects := transform.LogRows(ld, o)
	if len(logRejects) != 0 || len(logRows) != 1 {
		t.Fatalf("LogRows = (%d, %v), want (1, nil)", len(logRows), logRejects)
	}
	logEnc, err := New(schema.LogsTableSchema())
	if err != nil {
		t.Fatal(err)
	}
	lb, err := logEnc.Encode(logRows[0])
	if err != nil {
		t.Fatalf("logEnc.Encode: %v", err)
	}
	logNorm := decodeNormalized(t, logEnc, lb)
	logRefList := logNorm.Get(logNorm.Descriptor().Fields().ByName("payload_refs")).List()
	if logRefList.Len() != 1 {
		t.Fatalf("log payload_refs len = %d, want 1", logRefList.Len())
	}
	if got := get(t, logRefList.Get(0).Message(), "object_ref.authorizer").String(); got != "projects/my-proj/locations/us/connections/gcs" {
		t.Errorf("log payload_refs[0].object_ref.authorizer = %q", got)
	}

	qo := opts()
	qo.QuarantinePayloadOffloader = func(_ string, _ []byte) transform.ObjectRef {
		return ref.ObjectRef
	}
	qRows := transform.SpanQuarantineRows(td, []transform.QuarantineEntry{{Rejection: transform.Rejection{Index: 0, Reason: transform.ReasonRowBytes, Limit: 10, Actual: 20}}}, qo)
	qEnc, err := New(schema.QuarantineTableSchema())
	if err != nil {
		t.Fatal(err)
	}
	qb, err := qEnc.Encode(qRows[0])
	if err != nil {
		t.Fatalf("qEnc.Encode: %v", err)
	}
	qNorm := decodeNormalized(t, qEnc, qb)
	if has(t, qNorm, "raw_payload") {
		t.Errorf("quarantine raw_payload is set when offloaded, want NULL")
	}
	if got := get(t, qNorm, "payload_ref.uri").String(); got != "gs://bucket/offload/prompt-1.json" {
		t.Errorf("quarantine payload_ref.uri = %q", got)
	}
}
