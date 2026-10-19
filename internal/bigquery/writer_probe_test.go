// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigquery

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestWriterStreamPoolRecoversAfterMultiplexedByteBudgetTimeout(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	backend := &storageWriteFake{mode: fakeAck}
	server := grpc.NewServer()
	storagepb.RegisterBigQueryWriteServer(server, backend)
	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = server.Serve(lis)
	}()
	t.Cleanup(func() {
		server.Stop()
		<-served
	})

	client := newMultiplexedClientUnderTest(t, lis, 100, 1500)
	w := NewWriter(context.Background(), client, WriterOptions{
		Project: "p", Dataset: "d", Table: "t",
		Descriptor:          testRowDescriptor(),
		MaxRequestBytes:     1200,
		MaxInflightRequests: 100,
		MaxInflightBytes:    1500,
		StreamPoolSize:      2,
		TraceID:             "writer-test",
	})
	t.Cleanup(func() { _ = w.Close() })

	// Warm both slots so both ManagedStreams attach to the same multiConn.
	for i := range 2 {
		out := appendWithin(w, time.Second, [][]byte{{0x0a, 0x01, byte('a' + i)}})
		if out.Err != nil || out.AcknowledgedRows != 1 {
			t.Fatalf("warmup append %d = %+v, want 1 acknowledged row", i, out)
		}
	}

	row1000 := make([]byte, 1000)
	row1000[0] = 0x0a
	row1000[1] = 0xe4
	row1000[2] = 0x07

	backend.setMode(fakeNoACK)
	timedOut := appendWithin(w, 120*time.Millisecond, [][]byte{row1000})
	if !errors.Is(timedOut.Err, context.DeadlineExceeded) {
		t.Fatalf("timed-out append = %+v, want context.DeadlineExceeded", timedOut)
	}

	backend.setMode(fakeAck)
	for i := range 4 {
		out := appendWithin(w, time.Second, [][]byte{row1000})
		if out.Err != nil || out.AcknowledgedRows != 1 {
			t.Fatalf("retry append %d after multiplexed timeout = %+v, want 1 acknowledged row and nil error", i, out)
		}
	}

	w.mu.Lock()
	inflight := w.inflightBytes
	w.mu.Unlock()
	if inflight != 0 {
		t.Fatalf("writer inflightBytes after recovery = %d, want 0", inflight)
	}
}

type wireByteTrackingServer struct {
	storagepb.UnimplementedBigQueryWriteServer

	hold      time.Duration
	curBytes  atomic.Int64
	peakBytes atomic.Int64
	curReqs   atomic.Int64
	peakReqs  atomic.Int64
}

func (s *wireByteTrackingServer) GetWriteStream(
	_ context.Context, req *storagepb.GetWriteStreamRequest,
) (*storagepb.WriteStream, error) {
	return &storagepb.WriteStream{Name: req.GetName(), Location: "us"}, nil
}

func (s *wireByteTrackingServer) AppendRows(srv storagepb.BigQueryWrite_AppendRowsServer) error {
	for {
		req, err := srv.Recv()
		if err != nil {
			return nil
		}
		wireSize := int64(proto.Size(req))
		cb := s.curBytes.Add(wireSize)
		for {
			old := s.peakBytes.Load()
			if cb <= old || s.peakBytes.CompareAndSwap(old, cb) {
				break
			}
		}
		cr := s.curReqs.Add(1)
		for {
			old := s.peakReqs.Load()
			if cr <= old || s.peakReqs.CompareAndSwap(old, cr) {
				break
			}
		}

		time.Sleep(s.hold)

		s.curBytes.Add(-wireSize)
		s.curReqs.Add(-1)

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

func TestWriterStreamPoolActualServerWireBytesStayWithinCap(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	backend := &wireByteTrackingServer{hold: 50 * time.Millisecond}
	server := grpc.NewServer()
	storagepb.RegisterBigQueryWriteServer(server, backend)
	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = server.Serve(lis)
	}()
	t.Cleanup(func() {
		server.Stop()
		<-served
	})

	conn, err := grpc.NewClient("passthrough:///wire-byte-cap",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	lifetimeCtx, cancelLifetime := context.WithCancel(context.Background())
	client, err := managedwriter.NewClient(lifetimeCtx, "p",
		option.WithGRPCConn(conn),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Close()
		cancelLifetime()
		_ = conn.Close()
	})

	longFieldDesc := &descriptorpb.DescriptorProto{
		Name: proto.String("Row"),
		Field: []*descriptorpb.FieldDescriptorProto{{
			Name:   proto.String(strings.Repeat("x", 300)),
			Number: proto.Int32(1),
			Type:   descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum(),
			Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		}},
	}

	w := NewWriter(lifetimeCtx, client, WriterOptions{
		Project: "p", Dataset: "d", Table: "t",
		Descriptor:          longFieldDesc,
		MaxRequestBytes:     1500,
		MaxInflightRequests: 8,
		MaxInflightBytes:    2400,
		StreamPoolSize:      2,
		TraceID:             "writer-test",
	})
	t.Cleanup(func() { _ = w.Close() })

	row1000 := make([]byte, 1000)
	row1000[0] = 0x0a
	row1000[1] = 0xe4
	row1000[2] = 0x07

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out := appendWithin(w, 2*time.Second, [][]byte{row1000})
			if out.Err != nil || out.AcknowledgedRows != 1 {
				errs <- fmt.Errorf("Append outcome = %+v, want 1 acknowledged row", out)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	if got := backend.peakBytes.Load(); got > 2400 {
		t.Fatalf("peak unacknowledged server wire bytes = %d, exceeded MaxInflightBytes 2400", got)
	}
	if got := backend.peakReqs.Load(); got != 1 {
		t.Fatalf("peak concurrent unacknowledged requests = %d, want 1", got)
	}

	// Also verify unclipped exclusive flight when one sub-request's conservative
	// wire reservation exceeds MaxInflightBytes.
	wTight := NewWriter(lifetimeCtx, client, WriterOptions{
		Project: "p", Dataset: "d", Table: "t",
		Descriptor:          longFieldDesc,
		MaxRequestBytes:     1500,
		MaxInflightRequests: 8,
		MaxInflightBytes:    1200,
		StreamPoolSize:      2,
		TraceID:             "writer-test",
	})
	t.Cleanup(func() { _ = wTight.Close() })

	out := appendWithin(wTight, 2*time.Second, [][]byte{row1000})
	if out.Err != nil || out.AcknowledgedRows != 1 {
		t.Fatalf("exclusive-flight append = %+v, want 1 acknowledged row", out)
	}
	wTight.mu.Lock()
	inflight := wTight.inflightBytes
	wTight.mu.Unlock()
	if inflight != 0 {
		t.Fatalf("wTight inflightBytes after exclusive flight = %d, want 0", inflight)
	}
}
