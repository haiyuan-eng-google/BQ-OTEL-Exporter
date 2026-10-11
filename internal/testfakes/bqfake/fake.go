// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

// Package bqfake provides an in-process BigQuery Storage Write API gRPC server
// and a BigQuery REST v2 metadata HTTP server for hermetic end-to-end tests.
//
// Incoming ProtoRows are decoded with dynamicpb against the destination table's
// storagepb.TableSchema (or the stream's WriterSchema descriptor) into plain
// Go maps so tests can assert exact column values offline without live Google
// Cloud credentials.
package bqfake // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/testfakes/bqfake"

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	bigqueryapi "google.golang.org/api/bigquery/v2"
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

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
)

// FaultMode selects a simulated Storage Write API failure or delay.
type FaultMode int

const (
	// FaultNone acknowledges the request normally and records decoded rows.
	FaultNone FaultMode = iota
	// FaultRowErrors rejects the request with per-row errors and appends no rows.
	FaultRowErrors
	// FaultStreamClosed aborts the bidirectional AppendRows stream to exercise
	// stream retirement and reconnection.
	FaultStreamClosed
	// FaultUnavailable returns an in-band Unavailable response status.
	FaultUnavailable
	// FaultResourceExhausted returns an in-band ResourceExhausted response status.
	FaultResourceExhausted
	// FaultDelayedAck sleeps for Delay before acknowledging the request.
	FaultDelayedAck
	// FaultPermissionDenied returns an in-band PermissionDenied response status.
	FaultPermissionDenied
	// FaultInvalidArgument returns an in-band InvalidArgument response status.
	FaultInvalidArgument
)

// Fault describes a single programmed fault response on AppendRows.
type Fault struct {
	Mode      FaultMode
	Table     string
	RowErrors []*storagepb.RowError
	Delay     time.Duration
	Message   string
}

// Server hosts both an in-process gRPC BigQueryWriteServer and an httptest
// BigQuery REST v2 metadata server.
type Server struct {
	storagepb.UnimplementedBigQueryWriteServer

	lis      net.Listener
	grpcSrv  *grpc.Server
	restSrv  *httptest.Server
	servedCh chan struct{}

	mu          sync.Mutex
	schemas     map[string]*storagepb.TableSchema
	descriptors map[string]protoreflect.MessageDescriptor
	rows        map[string][]map[string]any
	faults      []Fault

	connections int
	requests    int
	ackedRows   int

	datasets map[string]*bigqueryapi.Dataset
	tables   map[string]map[string]*bigqueryapi.Table
}

// NewServer starts a new in-process Server and registers cleanup on t.
func NewServer(t testing.TB) *Server {
	t.Helper()
	srv, err := New()
	if err != nil {
		t.Fatalf("bqfake.New: %v", err)
	}
	t.Cleanup(func() {
		_ = srv.Close()
	})
	return srv
}

// New starts a new in-process gRPC and REST fake server.
func New() (*Server, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listening on loopback: %w", err)
	}
	s := &Server{
		lis:         lis,
		grpcSrv:     grpc.NewServer(),
		servedCh:    make(chan struct{}),
		schemas:     make(map[string]*storagepb.TableSchema),
		descriptors: make(map[string]protoreflect.MessageDescriptor),
		rows:        make(map[string][]map[string]any),
		datasets:    make(map[string]*bigqueryapi.Dataset),
		tables:      make(map[string]map[string]*bigqueryapi.Table),
	}

	if err := s.RegisterTableSchema("otel_spans", schema.SpansTableSchema()); err != nil {
		_ = lis.Close()
		return nil, err
	}
	if err := s.RegisterTableSchema("otel_logs", schema.LogsTableSchema()); err != nil {
		_ = lis.Close()
		return nil, err
	}
	if err := s.RegisterTableSchema("otel_quarantine", schema.QuarantineTableSchema()); err != nil {
		_ = lis.Close()
		return nil, err
	}

	storagepb.RegisterBigQueryWriteServer(s.grpcSrv, s)
	go func() {
		defer close(s.servedCh)
		_ = s.grpcSrv.Serve(lis)
	}()

	s.restSrv = httptest.NewServer(http.HandlerFunc(s.handleREST))
	return s, nil
}

// Close shuts down the REST and gRPC servers.
func (s *Server) Close() error {
	if s.restSrv != nil {
		s.restSrv.Close()
	}
	if s.grpcSrv != nil {
		s.grpcSrv.GracefulStop()
		<-s.servedCh
	}
	return nil
}

// Addr returns the loopback host:port of the gRPC Storage Write server.
func (s *Server) Addr() string {
	return s.lis.Addr().String()
}

// RESTURL returns the base URL of the REST v2 metadata server.
func (s *Server) RESTURL() string {
	return s.restSrv.URL
}

// WriteClientOptions returns client options that connect managedwriter to the
// fake gRPC server without TLS or ambient credentials.
func (s *Server) WriteClientOptions() []option.ClientOption {
	return []option.ClientOption{
		option.WithEndpoint(s.Addr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	}
}

// rewriteTransport redirects outgoing BigQuery REST requests to the local
// httptest.Server while preserving path and query parameters.
type rewriteTransport struct {
	base   http.RoundTripper
	target *url.URL
}

func (rt *rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	cloned := req.Clone(req.Context())
	cloned.URL.Scheme = rt.target.Scheme
	cloned.URL.Host = rt.target.Host
	cloned.Host = rt.target.Host
	return rt.base.RoundTrip(cloned)
}

// NewHTTPClient returns an HTTP client and underlying transport that route all
// BigQuery REST v2 requests to this Server's httptest.Server without network
// access or credentials.
func (s *Server) NewHTTPClient() (*http.Client, *http.Transport) {
	target, _ := url.Parse(s.restSrv.URL)
	base := &http.Transport{}
	client := &http.Client{
		Transport: &rewriteTransport{
			base:   base,
			target: target,
		},
	}
	return client, base
}

// RegisterTableSchema registers a storagepb.TableSchema for decoding ProtoRows
// sent to the named table.
func (s *Server) RegisterTableSchema(table string, ts *storagepb.TableSchema) error {
	md, err := descriptorFromTableSchema(ts)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.schemas[table] = ts
	s.descriptors[table] = md
	return nil
}

// EnqueueFault appends one or more faults to the server's fault queue.
func (s *Server) EnqueueFault(faults ...Fault) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.faults = append(s.faults, faults...)
}

// Counts returns connection, request, and acknowledged-row totals.
func (s *Server) Counts() (connections, requests, ackedRows int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connections, s.requests, s.ackedRows
}

// Rows returns a deep copy of all decoded rows appended to table.
func (s *Server) Rows(table string) []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	src := s.rows[table]
	out := make([]map[string]any, len(src))
	for i, r := range src {
		out[i] = cloneMap(r)
	}
	return out
}

// RowCount returns the number of rows appended to table.
func (s *Server) RowCount(table string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.rows[table])
}

// ResetRows clears all stored rows and resets request counters.
func (s *Server) ResetRows() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = make(map[string][]map[string]any)
	s.requests = 0
	s.ackedRows = 0
}

// Dataset returns the REST v2 Dataset metadata if created.
func (s *Server) Dataset(project, dataset string) (*bigqueryapi.Dataset, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ds, ok := s.datasets[project+"."+dataset]
	return ds, ok
}

// Table returns the REST v2 Table metadata if created.
func (s *Server) Table(project, dataset, table string) (*bigqueryapi.Table, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	byTable, ok := s.tables[project+"."+dataset]
	if !ok {
		return nil, false
	}
	tbl, ok := byTable[table]
	return tbl, ok
}

// SeedDataset pre-populates a dataset in the REST v2 server.
func (s *Server) SeedDataset(project, dataset, location string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := project + "." + dataset
	s.datasets[key] = &bigqueryapi.Dataset{
		DatasetReference: &bigqueryapi.DatasetReference{
			ProjectId: project,
			DatasetId: dataset,
		},
		Location: location,
		Etag:     "etag-" + key,
	}
}

// SeedTable pre-populates a table in the REST v2 server and registers its
// storagepb.TableSchema for ProtoRows decoding.
func (s *Server) SeedTable(project, dataset, table string, ts *storagepb.TableSchema) error {
	if err := s.RegisterTableSchema(table, ts); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := project + "." + dataset
	if _, ok := s.datasets[key]; !ok {
		s.datasets[key] = &bigqueryapi.Dataset{
			DatasetReference: &bigqueryapi.DatasetReference{
				ProjectId: project,
				DatasetId: dataset,
			},
			Location: "US",
			Etag:     "etag-" + key,
		}
	}
	if s.tables[key] == nil {
		s.tables[key] = make(map[string]*bigqueryapi.Table)
	}
	s.tables[key][table] = &bigqueryapi.Table{
		TableReference: &bigqueryapi.TableReference{
			ProjectId: project,
			DatasetId: dataset,
			TableId:   table,
		},
		Type:   "TABLE",
		Schema: toRESTTableSchema(ts),
		Etag:   "etag-" + table,
	}
	return nil
}

// GetWriteStream implements storagepb.BigQueryWriteServer.
func (s *Server) GetWriteStream(
	_ context.Context, req *storagepb.GetWriteStreamRequest,
) (*storagepb.WriteStream, error) {
	return &storagepb.WriteStream{
		Name:     req.GetName(),
		Type:     storagepb.WriteStream_COMMITTED,
		Location: "US",
	}, nil
}

// CreateWriteStream implements storagepb.BigQueryWriteServer.
func (s *Server) CreateWriteStream(
	_ context.Context, req *storagepb.CreateWriteStreamRequest,
) (*storagepb.WriteStream, error) {
	stream := req.GetWriteStream()
	name := req.GetParent() + "/streams/_default"
	if stream != nil && stream.GetName() != "" {
		name = stream.GetName()
	}
	return &storagepb.WriteStream{
		Name:     name,
		Type:     storagepb.WriteStream_COMMITTED,
		Location: "US",
	}, nil
}

// AppendRows implements storagepb.BigQueryWriteServer.
func (s *Server) AppendRows(srv storagepb.BigQueryWrite_AppendRowsServer) error {
	s.mu.Lock()
	s.connections++
	s.mu.Unlock()

	var streamTable string
	var streamDesc protoreflect.MessageDescriptor

	for {
		req, err := srv.Recv()
		if err != nil {
			return nil
		}
		if ws := req.GetWriteStream(); ws != "" {
			if tbl := parseTableFromStream(ws); tbl != "" {
				streamTable = tbl
			}
		}
		if protoDesc := req.GetProtoRows().GetWriterSchema().GetProtoDescriptor(); protoDesc != nil {
			if md, derr := descriptorFromProto(protoDesc); derr == nil {
				streamDesc = md
			}
		}

		s.mu.Lock()
		s.requests++
		md := s.descriptors[streamTable]
		if md == nil {
			md = streamDesc
		}
		fault := s.popMatchingFaultLocked(streamTable)
		s.mu.Unlock()

		switch fault.Mode {
		case FaultStreamClosed:
			msg := fault.Message
			if msg == "" {
				msg = "stream closed by fake fault injection"
			}
			return status.Error(codes.Aborted, msg)

		case FaultUnavailable:
			msg := fault.Message
			if msg == "" {
				msg = "service unavailable"
			}
			if err := srv.Send(errorResponse(codes.Unavailable, msg, nil)); err != nil {
				return nil
			}
			continue

		case FaultResourceExhausted:
			msg := fault.Message
			if msg == "" {
				msg = "rate limit exceeded: Quota exceeded"
			}
			if err := srv.Send(errorResponse(codes.ResourceExhausted, msg, nil)); err != nil {
				return nil
			}
			continue

		case FaultPermissionDenied:
			msg := fault.Message
			if msg == "" {
				msg = "permission denied on destination table"
			}
			if err := srv.Send(errorResponse(codes.PermissionDenied, msg, nil)); err != nil {
				return nil
			}
			continue

		case FaultInvalidArgument:
			msg := fault.Message
			if msg == "" {
				msg = "invalid argument"
			}
			if err := srv.Send(errorResponse(codes.InvalidArgument, msg, nil)); err != nil {
				return nil
			}
			continue

		case FaultRowErrors:
			msg := fault.Message
			if msg == "" {
				msg = "row errors in append request"
			}
			if err := srv.Send(errorResponse(codes.InvalidArgument, msg, fault.RowErrors)); err != nil {
				return nil
			}
			continue

		case FaultDelayedAck:
			if fault.Delay > 0 {
				timer := time.NewTimer(fault.Delay)
				select {
				case <-srv.Context().Done():
					timer.Stop()
					return nil
				case <-timer.C:
				}
			}
		}

		serialized := req.GetProtoRows().GetRows().GetSerializedRows()
		decodedBatch := make([]map[string]any, 0, len(serialized))
		if md != nil {
			for i, raw := range serialized {
				row, derr := DecodeProtoRow(md, raw)
				if derr != nil {
					rowErr := &storagepb.RowError{
						Index:   int64(i),
						Code:    storagepb.RowError_FIELDS_ERROR,
						Message: derr.Error(),
					}
					_ = srv.Send(errorResponse(codes.InvalidArgument, "decode failure", []*storagepb.RowError{rowErr}))
					decodedBatch = nil
					break
				}
				decodedBatch = append(decodedBatch, row)
			}
			if decodedBatch == nil && len(serialized) > 0 {
				continue
			}
		}

		s.mu.Lock()
		s.ackedRows += len(serialized)
		if streamTable != "" && len(decodedBatch) > 0 {
			s.rows[streamTable] = append(s.rows[streamTable], decodedBatch...)
		}
		s.mu.Unlock()

		resp := &storagepb.AppendRowsResponse{
			Response: &storagepb.AppendRowsResponse_AppendResult_{
				AppendResult: &storagepb.AppendRowsResponse_AppendResult{},
			},
		}
		if err := srv.Send(resp); err != nil {
			return nil
		}
	}
}

func (s *Server) popMatchingFaultLocked(table string) Fault {
	for i, f := range s.faults {
		if f.Table == "" || f.Table == table {
			s.faults = append(s.faults[:i], s.faults[i+1:]...)
			return f
		}
	}
	return Fault{Mode: FaultNone}
}

func errorResponse(code codes.Code, msg string, rowErrs []*storagepb.RowError) *storagepb.AppendRowsResponse {
	return &storagepb.AppendRowsResponse{
		Response: &storagepb.AppendRowsResponse_Error{
			Error: status.New(code, msg).Proto(),
		},
		RowErrors: rowErrs,
	}
}

// parseTableFromStream extracts "{table}" from
// "projects/{p}/datasets/{d}/tables/{table}/streams/{s}".
func parseTableFromStream(streamName string) string {
	parts := strings.Split(streamName, "/")
	for i := 0; i+1 < len(parts); i++ {
		if parts[i] == "tables" {
			return parts[i+1]
		}
	}
	return ""
}

func descriptorFromTableSchema(ts *storagepb.TableSchema) (protoreflect.MessageDescriptor, error) {
	desc, err := adapt.StorageSchemaToProto2Descriptor(ts, "root")
	if err != nil {
		return nil, fmt.Errorf("deriving proto2 descriptor: %w", err)
	}
	md, ok := desc.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("unexpected descriptor type %T", desc)
	}
	return md, nil
}

func descriptorFromProto(dp *descriptorpb.DescriptorProto) (protoreflect.MessageDescriptor, error) {
	name := dp.GetName()
	if name == "" {
		name = "Row"
	}
	fdp := &descriptorpb.FileDescriptorProto{
		Name:        proto.String("stream_row.proto"),
		Syntax:      proto.String("proto2"),
		MessageType: []*descriptorpb.DescriptorProto{dp},
	}
	fileDesc, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		return nil, err
	}
	md := fileDesc.Messages().ByName(protoreflect.Name(name))
	if md == nil {
		return nil, fmt.Errorf("message %q not found in descriptor", name)
	}
	return md, nil
}

// DecodeRowWithSchema unmarshals a serialized ProtoRows row against ts into a
// nested map[string]any keyed by column name.
func DecodeRowWithSchema(ts *storagepb.TableSchema, raw []byte) (map[string]any, error) {
	md, err := descriptorFromTableSchema(ts)
	if err != nil {
		return nil, err
	}
	return DecodeProtoRow(md, raw)
}

// DecodeProtoRow unmarshals a serialized protobuf message against md into a
// nested map[string]any keyed by field name.
func DecodeProtoRow(md protoreflect.MessageDescriptor, raw []byte) (map[string]any, error) {
	msg := dynamicpb.NewMessage(md)
	if err := proto.Unmarshal(raw, msg); err != nil {
		return nil, fmt.Errorf("unmarshaling proto row: %w", err)
	}
	return messageToMap(msg), nil
}

func messageToMap(msg protoreflect.Message) map[string]any {
	out := make(map[string]any)
	msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		name := string(fd.Name())
		if fd.IsList() {
			list := v.List()
			if fd.Kind() == protoreflect.MessageKind {
				items := make([]map[string]any, list.Len())
				for i := 0; i < list.Len(); i++ {
					items[i] = messageToMap(list.Get(i).Message())
				}
				out[name] = items
			} else {
				items := make([]any, list.Len())
				for i := 0; i < list.Len(); i++ {
					items[i] = scalarToGo(fd, list.Get(i))
				}
				out[name] = items
			}
			return true
		}
		if fd.Kind() == protoreflect.MessageKind {
			out[name] = messageToMap(v.Message())
			return true
		}
		out[name] = scalarToGo(fd, v)
		return true
	})
	return out
}

func scalarToGo(fd protoreflect.FieldDescriptor, v protoreflect.Value) any {
	switch fd.Kind() {
	case protoreflect.StringKind:
		return v.String()
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind,
		protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return v.Int()
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind,
		protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return int64(v.Uint())
	case protoreflect.DoubleKind, protoreflect.FloatKind:
		return v.Float()
	case protoreflect.BoolKind:
		return v.Bool()
	case protoreflect.BytesKind:
		return append([]byte(nil), v.Bytes()...)
	default:
		return v.Interface()
	}
}

// StartTimeHour extracts the microsecond start_time column from a decoded span
// row and returns its UTC hour truncation (`TIMESTAMP_TRUNC(start_time, HOUR)`).
func StartTimeHour(row map[string]any) (time.Time, bool) {
	us, ok := row["start_time"].(int64)
	if !ok {
		return time.Time{}, false
	}
	return time.UnixMicro(us).UTC().Truncate(time.Hour), true
}

func cloneMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		switch tv := v.(type) {
		case map[string]any:
			out[k] = cloneMap(tv)
		case []map[string]any:
			cp := make([]map[string]any, len(tv))
			for i, elem := range tv {
				cp[i] = cloneMap(elem)
			}
			out[k] = cp
		case []byte:
			out[k] = append([]byte(nil), tv...)
		default:
			out[k] = v
		}
	}
	return out
}

// handleREST implements the subset of the BigQuery REST v2 API used by
// cloud.google.com/go/bigquery for dataset/table/view metadata and creation.
func (s *Server) handleREST(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/bigquery/v2/")
	parts := strings.Split(strings.Trim(path, "/"), "/")

	// Routes:
	// 1. projects/{p}/datasets
	// 2. projects/{p}/datasets/{d}
	// 3. projects/{p}/datasets/{d}/tables
	// 4. projects/{p}/datasets/{d}/tables/{t}
	if len(parts) < 3 || parts[0] != "projects" || parts[2] != "datasets" {
		writeRESTError(w, http.StatusNotFound, "unsupported path: "+r.URL.Path)
		return
	}
	project := parts[1]

	if len(parts) == 3 {
		if r.Method != http.MethodPost {
			writeRESTError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var ds bigqueryapi.Dataset
		if err := decodeJSONBody(r.Body, &ds); err != nil {
			writeRESTError(w, http.StatusBadRequest, err.Error())
			return
		}
		datasetID := ""
		if ds.DatasetReference != nil {
			datasetID = ds.DatasetReference.DatasetId
		}
		if datasetID == "" {
			writeRESTError(w, http.StatusBadRequest, "missing datasetId")
			return
		}
		key := project + "." + datasetID
		s.mu.Lock()
		if _, exists := s.datasets[key]; exists {
			s.mu.Unlock()
			writeRESTError(w, http.StatusConflict, "Already Exists: Dataset "+key)
			return
		}
		ds.DatasetReference = &bigqueryapi.DatasetReference{
			ProjectId: project,
			DatasetId: datasetID,
		}
		if ds.Etag == "" {
			ds.Etag = "etag-" + key
		}
		s.datasets[key] = &ds
		s.mu.Unlock()
		writeRESTJSON(w, http.StatusOK, &ds)
		return
	}

	datasetID := parts[3]
	dsKey := project + "." + datasetID

	if len(parts) == 4 {
		if r.Method != http.MethodGet {
			writeRESTError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		s.mu.Lock()
		ds, ok := s.datasets[dsKey]
		s.mu.Unlock()
		if !ok {
			writeRESTError(w, http.StatusNotFound, "Not found: Dataset "+dsKey)
			return
		}
		writeRESTJSON(w, http.StatusOK, ds)
		return
	}

	if parts[4] != "tables" {
		writeRESTError(w, http.StatusNotFound, "unsupported resource under dataset: "+parts[4])
		return
	}

	if len(parts) == 5 {
		if r.Method != http.MethodPost {
			writeRESTError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var tbl bigqueryapi.Table
		if err := decodeJSONBody(r.Body, &tbl); err != nil {
			writeRESTError(w, http.StatusBadRequest, err.Error())
			return
		}
		tableID := ""
		if tbl.TableReference != nil {
			tableID = tbl.TableReference.TableId
		}
		if tableID == "" {
			writeRESTError(w, http.StatusBadRequest, "missing tableId")
			return
		}
		s.mu.Lock()
		if _, ok := s.datasets[dsKey]; !ok {
			s.mu.Unlock()
			writeRESTError(w, http.StatusNotFound, "Not found: Dataset "+dsKey)
			return
		}
		if s.tables[dsKey] == nil {
			s.tables[dsKey] = make(map[string]*bigqueryapi.Table)
		}
		if _, exists := s.tables[dsKey][tableID]; exists {
			s.mu.Unlock()
			writeRESTError(w, http.StatusConflict, "Already Exists: Table "+dsKey+"."+tableID)
			return
		}
		tbl.TableReference = &bigqueryapi.TableReference{
			ProjectId: project,
			DatasetId: datasetID,
			TableId:   tableID,
		}
		if tbl.View != nil {
			tbl.Type = "VIEW"
		} else if tbl.Type == "" {
			tbl.Type = "TABLE"
		}
		if tbl.Etag == "" {
			tbl.Etag = "etag-" + tableID
		}
		s.tables[dsKey][tableID] = &tbl
		var storageSchema *storagepb.TableSchema
		if tbl.Schema != nil && len(tbl.Schema.Fields) > 0 {
			storageSchema = fromRESTTableSchema(tbl.Schema)
		}
		s.mu.Unlock()

		if storageSchema != nil {
			_ = s.RegisterTableSchema(tableID, storageSchema)
		}
		writeRESTJSON(w, http.StatusOK, &tbl)
		return
	}

	if len(parts) == 6 {
		tableID := parts[5]
		switch r.Method {
		case http.MethodGet:
			s.mu.Lock()
			byTable := s.tables[dsKey]
			var tbl *bigqueryapi.Table
			if byTable != nil {
				tbl = byTable[tableID]
			}
			s.mu.Unlock()
			if tbl == nil {
				writeRESTError(w, http.StatusNotFound, "Not found: Table "+dsKey+"."+tableID)
				return
			}
			writeRESTJSON(w, http.StatusOK, tbl)
			return

		case http.MethodPatch, http.MethodPut:
			var patch bigqueryapi.Table
			if err := decodeJSONBody(r.Body, &patch); err != nil {
				writeRESTError(w, http.StatusBadRequest, err.Error())
				return
			}
			s.mu.Lock()
			byTable := s.tables[dsKey]
			var existing *bigqueryapi.Table
			if byTable != nil {
				existing = byTable[tableID]
			}
			if existing == nil {
				s.mu.Unlock()
				writeRESTError(w, http.StatusNotFound, "Not found: Table "+dsKey+"."+tableID)
				return
			}
			if patch.Schema != nil {
				existing.Schema = patch.Schema
			}
			if patch.View != nil {
				existing.View = patch.View
				existing.Type = "VIEW"
			}
			if patch.TimePartitioning != nil {
				existing.TimePartitioning = patch.TimePartitioning
			}
			if patch.Clustering != nil {
				existing.Clustering = patch.Clustering
			}
			var storageSchema *storagepb.TableSchema
			if existing.Schema != nil && len(existing.Schema.Fields) > 0 {
				storageSchema = fromRESTTableSchema(existing.Schema)
			}
			s.mu.Unlock()
			if storageSchema != nil {
				_ = s.RegisterTableSchema(tableID, storageSchema)
			}
			writeRESTJSON(w, http.StatusOK, existing)
			return

		default:
			writeRESTError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
	}

	writeRESTError(w, http.StatusNotFound, "unsupported endpoint: "+r.URL.Path)
}

func decodeJSONBody(r io.Reader, dst any) error {
	defer func() { _, _ = io.Copy(io.Discard, r) }()
	return json.NewDecoder(r).Decode(dst)
}

func writeRESTJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeRESTError(w http.ResponseWriter, code int, msg string) {
	type errBody struct {
		Error struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	var body errBody
	body.Error.Code = code
	body.Error.Message = msg
	writeRESTJSON(w, code, &body)
}

var restToStorageType = map[string]storagepb.TableFieldSchema_Type{
	"STRING":     storagepb.TableFieldSchema_STRING,
	"INTEGER":    storagepb.TableFieldSchema_INT64,
	"INT64":      storagepb.TableFieldSchema_INT64,
	"FLOAT":      storagepb.TableFieldSchema_DOUBLE,
	"FLOAT64":    storagepb.TableFieldSchema_DOUBLE,
	"NUMERIC":    storagepb.TableFieldSchema_NUMERIC,
	"BIGNUMERIC": storagepb.TableFieldSchema_BIGNUMERIC,
	"BOOLEAN":    storagepb.TableFieldSchema_BOOL,
	"BOOL":       storagepb.TableFieldSchema_BOOL,
	"TIMESTAMP":  storagepb.TableFieldSchema_TIMESTAMP,
	"DATE":       storagepb.TableFieldSchema_DATE,
	"TIME":       storagepb.TableFieldSchema_TIME,
	"DATETIME":   storagepb.TableFieldSchema_DATETIME,
	"GEOGRAPHY":  storagepb.TableFieldSchema_GEOGRAPHY,
	"BYTES":      storagepb.TableFieldSchema_BYTES,
	"INTERVAL":   storagepb.TableFieldSchema_INTERVAL,
	"JSON":       storagepb.TableFieldSchema_JSON,
	"RECORD":     storagepb.TableFieldSchema_STRUCT,
	"STRUCT":     storagepb.TableFieldSchema_STRUCT,
	"RANGE":      storagepb.TableFieldSchema_RANGE,
}

var storageToRESTType = func() map[storagepb.TableFieldSchema_Type]string {
	m := make(map[storagepb.TableFieldSchema_Type]string, len(restToStorageType))
	for k, v := range maps.All(restToStorageType) {
		if _, exists := m[v]; !exists {
			m[v] = k
		}
	}
	m[storagepb.TableFieldSchema_INT64] = "INTEGER"
	m[storagepb.TableFieldSchema_DOUBLE] = "FLOAT"
	m[storagepb.TableFieldSchema_BOOL] = "BOOLEAN"
	m[storagepb.TableFieldSchema_STRUCT] = "RECORD"
	return m
}()

func fromRESTTableSchema(ts *bigqueryapi.TableSchema) *storagepb.TableSchema {
	if ts == nil {
		return nil
	}
	return &storagepb.TableSchema{
		Fields: fromRESTFields(ts.Fields),
	}
}

func fromRESTFields(fields []*bigqueryapi.TableFieldSchema) []*storagepb.TableFieldSchema {
	out := make([]*storagepb.TableFieldSchema, 0, len(fields))
	for _, f := range fields {
		if f == nil {
			continue
		}
		typ, ok := restToStorageType[strings.ToUpper(f.Type)]
		if !ok {
			typ = storagepb.TableFieldSchema_STRING
		}
		mode := storagepb.TableFieldSchema_NULLABLE
		switch strings.ToUpper(f.Mode) {
		case "REQUIRED":
			mode = storagepb.TableFieldSchema_REQUIRED
		case "REPEATED":
			mode = storagepb.TableFieldSchema_REPEATED
		}
		out = append(out, &storagepb.TableFieldSchema{
			Name:        f.Name,
			Description: f.Description,
			Type:        typ,
			Mode:        mode,
			Fields:      fromRESTFields(f.Fields),
		})
	}
	return out
}

func toRESTTableSchema(ts *storagepb.TableSchema) *bigqueryapi.TableSchema {
	if ts == nil {
		return nil
	}
	return &bigqueryapi.TableSchema{
		Fields: toRESTFields(ts.GetFields()),
	}
}

func toRESTFields(fields []*storagepb.TableFieldSchema) []*bigqueryapi.TableFieldSchema {
	out := make([]*bigqueryapi.TableFieldSchema, 0, len(fields))
	for _, f := range fields {
		if f == nil {
			continue
		}
		mode := "NULLABLE"
		switch f.GetMode() {
		case storagepb.TableFieldSchema_REQUIRED:
			mode = "REQUIRED"
		case storagepb.TableFieldSchema_REPEATED:
			mode = "REPEATED"
		}
		out = append(out, &bigqueryapi.TableFieldSchema{
			Name:        f.GetName(),
			Description: f.GetDescription(),
			Type:        storageToRESTType[f.GetType()],
			Mode:        mode,
			Fields:      toRESTFields(f.GetFields()),
		})
	}
	return out
}
