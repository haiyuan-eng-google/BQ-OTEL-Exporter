// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bqfake

import (
	"context"
	"sync"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/protoenc"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/transform"
)

func TestServerDecodeSpanLogAndQuarantineRows(t *testing.T) {
	srv := NewServer(t)
	ctx := context.Background()

	client, err := managedwriter.NewClient(ctx, "test-project", srv.WriteClientOptions()...)
	if err != nil {
		t.Fatalf("managedwriter.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	spanEnc, err := protoenc.New(schema.SpansTableSchema())
	if err != nil {
		t.Fatalf("protoenc.New(SpansTableSchema): %v", err)
	}
	stream, err := client.NewManagedStream(ctx,
		managedwriter.WithDestinationTable("projects/test-project/datasets/otel/tables/otel_spans"),
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithSchemaDescriptor(spanEnc.Descriptor()),
	)
	if err != nil {
		t.Fatalf("NewManagedStream: %v", err)
	}
	t.Cleanup(func() { _ = stream.Close() })

	start := time.Date(2026, 8, 12, 14, 35, 10, 0, time.UTC)
	end := start.Add(25 * time.Millisecond)
	row := transform.Row{
		"start_time":             start.UnixMicro(),
		"start_time_unix_nano":   start.UnixNano(),
		"end_time":               end.UnixMicro(),
		"end_time_unix_nano":     end.UnixNano(),
		"receive_time":           end.UnixMicro(),
		"receive_time_unix_nano": end.UnixNano(),
		"duration_nano":          int64(25 * time.Millisecond),
		"trace_id":               "0102030405060708090a0b0c0d0e0f10",
		"span_id":                "0102030405060708",
		"name":                   "invoke_agent",
		"kind":                   int64(2),
		"kind_name":              "SPAN_KIND_SERVER",
		"service_name":           "adk-service",
		"attributes":             `{"gen_ai.system":"gcp.vertex.agent"}`,
		"attributes_schema":      `{"gen_ai.system":"string"}`,
		"schema_version":         schema.Version,
	}
	encoded, err := spanEnc.Encode(row)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	res, err := stream.AppendRows(ctx, [][]byte{encoded})
	if err != nil {
		t.Fatalf("AppendRows: %v", err)
	}
	if _, err := res.GetResult(ctx); err != nil {
		t.Fatalf("GetResult: %v", err)
	}

	gotRows := srv.Rows("otel_spans")
	if len(gotRows) != 1 {
		t.Fatalf("got %d rows, want 1", len(gotRows))
	}
	got := gotRows[0]
	if got["trace_id"] != "0102030405060708090a0b0c0d0e0f10" {
		t.Errorf("trace_id = %v, want 0102030405060708090a0b0c0d0e0f10", got["trace_id"])
	}
	if got["name"] != "invoke_agent" {
		t.Errorf("name = %v, want invoke_agent", got["name"])
	}
	hour, ok := StartTimeHour(got)
	if !ok || !hour.Equal(time.Date(2026, 8, 12, 14, 0, 0, 0, time.UTC)) {
		t.Errorf("StartTimeHour = (%v, %v), want 2026-08-12T14:00:00Z", hour, ok)
	}
}

func TestServerFaultInjectionModes(t *testing.T) {
	srv := NewServer(t)
	ctx := context.Background()

	client, err := managedwriter.NewClient(ctx, "test-project", srv.WriteClientOptions()...)
	if err != nil {
		t.Fatalf("managedwriter.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	qEnc, err := protoenc.New(schema.QuarantineTableSchema())
	if err != nil {
		t.Fatalf("protoenc.New: %v", err)
	}
	stream, err := client.NewManagedStream(ctx,
		managedwriter.WithDestinationTable("projects/test-project/datasets/otel/tables/otel_quarantine"),
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithSchemaDescriptor(qEnc.Descriptor()),
	)
	if err != nil {
		t.Fatalf("NewManagedStream: %v", err)
	}
	t.Cleanup(func() { _ = stream.Close() })

	encoded, err := qEnc.Encode(transform.Row{
		"ingest_time":    time.Now().UnixMicro(),
		"signal":         schema.SignalSpans,
		"error_code":     "INVALID_SPAN_ID",
		"error_message":  "invalid_id: span_id",
		"schema_version": schema.Version,
	})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	srv.EnqueueFault(
		Fault{
			Mode:  FaultRowErrors,
			Table: "otel_quarantine",
			RowErrors: []*storagepb.RowError{
				{Index: 0, Code: storagepb.RowError_FIELDS_ERROR, Message: "bad field"},
			},
		},
		Fault{Mode: FaultUnavailable, Table: "otel_quarantine"},
		Fault{Mode: FaultResourceExhausted, Table: "otel_quarantine"},
		Fault{Mode: FaultDelayedAck, Table: "otel_quarantine", Delay: 10 * time.Millisecond},
	)

	// 1. RowErrors fault: GetResult returns error, FullResponse carries RowErrors, 0 rows stored.
	res1, err := stream.AppendRows(ctx, [][]byte{encoded})
	if err != nil {
		t.Fatalf("AppendRows #1: %v", err)
	}
	if _, err := res1.GetResult(ctx); err == nil {
		t.Fatal("expected RowErrors fault to return an error")
	}
	full1, err := res1.FullResponse(ctx)
	if status.Code(err) != codes.InvalidArgument || len(full1.GetRowErrors()) != 1 {
		t.Fatalf("FullResponse RowErrors = %v (err=%v), want 1 row error with InvalidArgument", full1.GetRowErrors(), err)
	}
	if srv.RowCount("otel_quarantine") != 0 {
		t.Fatalf("RowCount after RowErrors = %d, want 0", srv.RowCount("otel_quarantine"))
	}

	// 2. Unavailable fault.
	res2, err := stream.AppendRows(ctx, [][]byte{encoded})
	if err != nil {
		t.Fatalf("AppendRows #2: %v", err)
	}
	if _, err := res2.GetResult(ctx); status.Code(err) != codes.Unavailable {
		t.Fatalf("GetResult #2 status = %v, want Unavailable", status.Code(err))
	}

	// 3. ResourceExhausted fault.
	res3, err := stream.AppendRows(ctx, [][]byte{encoded})
	if err != nil {
		t.Fatalf("AppendRows #3: %v", err)
	}
	if _, err := res3.GetResult(ctx); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("GetResult #3 status = %v, want ResourceExhausted", status.Code(err))
	}

	// 4. DelayedAck fault succeeds after delay and stores the row.
	res4, err := stream.AppendRows(ctx, [][]byte{encoded})
	if err != nil {
		t.Fatalf("AppendRows #4: %v", err)
	}
	if _, err := res4.GetResult(ctx); err != nil {
		t.Fatalf("GetResult #4: %v", err)
	}
	if srv.RowCount("otel_quarantine") != 1 {
		t.Fatalf("RowCount after DelayedAck = %d, want 1", srv.RowCount("otel_quarantine"))
	}
}

func TestRESTMetadataEndpoints(t *testing.T) {
	srv := NewServer(t)
	ctx := context.Background()

	httpClient, httpBase := srv.NewHTTPClient()
	t.Cleanup(func() { httpBase.CloseIdleConnections() })

	client, err := bq.NewClient(ctx, "test-project", option.WithHTTPClient(httpClient))
	if err != nil {
		t.Fatalf("bq.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	ds := client.Dataset("otel_ds")
	if _, err := ds.Metadata(ctx); err == nil {
		t.Fatal("expected 404 before dataset creation")
	}
	if err := ds.Create(ctx, &bq.DatasetMetadata{Location: "US"}); err != nil {
		t.Fatalf("Dataset.Create: %v", err)
	}
	md, err := ds.Metadata(ctx)
	if err != nil || md.Location != "US" {
		t.Fatalf("Dataset.Metadata = (%#v, %v), want Location US", md, err)
	}

	tblMD, viewQuery, err := schema.DestinationMetadata(schema.SpansTableSchema(), "test-project", "otel_ds", "otel_spans")
	if err != nil {
		t.Fatalf("DestinationMetadata: %v", err)
	}
	if err := ds.Table("otel_spans").Create(ctx, tblMD); err != nil {
		t.Fatalf("Table.Create: %v", err)
	}
	gotTbl, err := ds.Table("otel_spans").Metadata(ctx)
	if err != nil || len(gotTbl.Schema) != len(schema.SpansTableSchema().GetFields()) {
		t.Fatalf("Table.Metadata fields = %d (err=%v), want %d", len(gotTbl.Schema), err, len(schema.SpansTableSchema().GetFields()))
	}

	if err := ds.Table("otel_spans_dedup").Create(ctx, &bq.TableMetadata{ViewQuery: viewQuery, UseLegacySQL: false}); err != nil {
		t.Fatalf("View.Create: %v", err)
	}
	gotView, err := ds.Table("otel_spans_dedup").Metadata(ctx)
	if err != nil || gotView.ViewQuery != viewQuery {
		t.Fatalf("View.Metadata query = %q (err=%v), want %q", gotView.ViewQuery, err, viewQuery)
	}
}

func TestAppendRowsDecodesWithWriterDescriptorAndValidatesAgainstTableSchema(t *testing.T) {
	srv := NewServer(t)
	ctx := context.Background()

	if err := srv.RegisterTableSchema("pair_table", &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "first", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_REQUIRED},
			{Name: "second", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
		},
	}); err != nil {
		t.Fatalf("RegisterTableSchema: %v", err)
	}

	conn, err := grpc.NewClient(srv.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	writeClient := storagepb.NewBigQueryWriteClient(conn)

	const streamName = "projects/test-project/datasets/otel/tables/pair_table/streams/_default"

	// 1. Reordered field numbers in the writer descriptor must decode by the
	// writer's field numbers rather than swapping values by table column order.
	reorderedDP := &descriptorpb.DescriptorProto{
		Name: proto.String("ReorderedRow"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{
				Name:   proto.String("second"),
				Number: proto.Int32(1),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
			},
			{
				Name:   proto.String("first"),
				Number: proto.Int32(2),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_REQUIRED.Enum(),
				Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
			},
		},
	}
	fileDesc, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:        proto.String("reordered.proto"),
		Syntax:      proto.String("proto2"),
		MessageType: []*descriptorpb.DescriptorProto{reorderedDP},
	}, nil)
	if err != nil {
		t.Fatalf("protodesc.NewFile: %v", err)
	}
	msg := dynamicpb.NewMessage(fileDesc.Messages().ByName("ReorderedRow"))
	msg.Set(msg.Descriptor().Fields().ByName("first"), protoreflect.ValueOfString("alpha"))
	msg.Set(msg.Descriptor().Fields().ByName("second"), protoreflect.ValueOfString("beta"))
	raw, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}

	stream, err := writeClient.AppendRows(ctx)
	if err != nil {
		t.Fatalf("AppendRows: %v", err)
	}
	if err := stream.Send(&storagepb.AppendRowsRequest{
		WriteStream: streamName,
		Rows: &storagepb.AppendRowsRequest_ProtoRows{
			ProtoRows: &storagepb.AppendRowsRequest_ProtoData{
				WriterSchema: &storagepb.ProtoSchema{ProtoDescriptor: reorderedDP},
				Rows:         &storagepb.ProtoRows{SerializedRows: [][]byte{raw}},
			},
		},
	}); err != nil {
		t.Fatalf("Send reordered row: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv reordered row: %v", err)
	}
	if resp.GetError() != nil {
		t.Fatalf("unexpected error for reordered writer descriptor: %v", resp.GetError())
	}
	rows := srv.Rows("pair_table")
	if len(rows) != 1 || rows[0]["first"] != "alpha" || rows[0]["second"] != "beta" {
		t.Fatalf("decoded rows = %#v, want first=alpha and second=beta", rows)
	}

	// 2. Unknown column, incompatible type, missing required column, and
	// missing/invalid writer descriptor must be rejected with InvalidArgument.
	for _, tc := range []struct {
		name string
		dp   *descriptorpb.DescriptorProto
	}{
		{
			name: "unknown column",
			dp: &descriptorpb.DescriptorProto{
				Name: proto.String("UnknownColRow"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:   proto.String("first"),
						Number: proto.Int32(1),
						Label:  descriptorpb.FieldDescriptorProto_LABEL_REQUIRED.Enum(),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					},
					{
						Name:   proto.String("unknown_column"),
						Number: proto.Int32(2),
						Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					},
				},
			},
		},
		{
			name: "mismatched field type",
			dp: &descriptorpb.DescriptorProto{
				Name: proto.String("BadTypeRow"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:   proto.String("first"),
						Number: proto.Int32(1),
						Label:  descriptorpb.FieldDescriptorProto_LABEL_REQUIRED.Enum(),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
					},
				},
			},
		},
		{
			name: "missing required column",
			dp: &descriptorpb.DescriptorProto{
				Name: proto.String("MissingReqRow"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:   proto.String("second"),
						Number: proto.Int32(1),
						Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					},
				},
			},
		},
		{
			name: "missing writer descriptor",
			dp:   nil,
		},
		{
			name: "empty writer descriptor",
			dp:   &descriptorpb.DescriptorProto{Name: proto.String("")},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, err := writeClient.AppendRows(ctx)
			if err != nil {
				t.Fatalf("AppendRows: %v", err)
			}
			var ws *storagepb.ProtoSchema
			if tc.dp != nil {
				ws = &storagepb.ProtoSchema{ProtoDescriptor: tc.dp}
			}
			if err := st.Send(&storagepb.AppendRowsRequest{
				WriteStream: streamName,
				Rows: &storagepb.AppendRowsRequest_ProtoRows{
					ProtoRows: &storagepb.AppendRowsRequest_ProtoData{
						WriterSchema: ws,
						Rows:         &storagepb.ProtoRows{SerializedRows: [][]byte{raw}},
					},
				},
			}); err != nil {
				t.Fatalf("Send: %v", err)
			}
			r, err := st.Recv()
			if err != nil {
				t.Fatalf("Recv: %v", err)
			}
			if r.GetError() == nil || codes.Code(r.GetError().GetCode()) != codes.InvalidArgument {
				t.Fatalf("response error = %v, want InvalidArgument", r.GetError())
			}
		})
	}
}

func TestConcurrentTableGetAndPatch(t *testing.T) {
	srv := NewServer(t)
	ctx := context.Background()

	httpClient, httpBase := srv.NewHTTPClient()
	t.Cleanup(func() { httpBase.CloseIdleConnections() })

	client, err := bq.NewClient(ctx, "test-project", option.WithHTTPClient(httpClient))
	if err != nil {
		t.Fatalf("bq.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	ds := client.Dataset("race_ds")
	if err := ds.Create(ctx, &bq.DatasetMetadata{Location: "US"}); err != nil {
		t.Fatalf("Dataset.Create: %v", err)
	}
	tblMD, _, err := schema.DestinationMetadata(schema.SpansTableSchema(), "test-project", "race_ds", "otel_spans")
	if err != nil {
		t.Fatalf("DestinationMetadata: %v", err)
	}
	tbl := ds.Table("otel_spans")
	if err := tbl.Create(ctx, tblMD); err != nil {
		t.Fatalf("Table.Create: %v", err)
	}

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for range 25 {
				if _, err := tbl.Metadata(ctx); err != nil {
					t.Errorf("Table.Metadata: %v", err)
					return
				}
				if got, ok := srv.Table("test-project", "race_ds", "otel_spans"); !ok || got == nil {
					t.Errorf("srv.Table returned (%v, %v)", got, ok)
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			for range 25 {
				if _, err := tbl.Update(ctx, bq.TableMetadataToUpdate{
					Schema: tblMD.Schema,
				}, ""); err != nil {
					t.Errorf("Table.Update: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}
