// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigquery

import (
	"context"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"google.golang.org/api/option"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// The tests in this file drive the real managedwriter client through Writer,
// against an in-process Storage Write service. The stream seam used elsewhere
// cannot show how managedwriter itself retains contexts, fails requests when a
// connection is torn down, or reports a closed stream; issue #3 was a wedge
// in exactly that layer.

// fakeMode selects how storageWriteFake answers AppendRows requests.
type fakeMode int

const (
	// fakeAck acknowledges every request.
	fakeAck fakeMode = iota
	// fakeNoACK wedges the connection: requests keep arriving but none is
	// ever answered, until the client tears the connection down.
	fakeNoACK
	// fakeDeny rejects every request with PermissionDenied.
	fakeDeny
	// fakeFailOnce answers the next request with Unavailable, then
	// acknowledges.
	fakeFailOnce
	// fakeReject rejects a request with InvalidArgument and no row errors:
	// a permanent verdict on the whole request.
	fakeReject
	// fakeDeadline answers a request with the service's own
	// DeadlineExceeded, which leaves its outcome uncertain.
	fakeDeadline
)

// storageWriteFake is an in-process BigQuery Storage Write service with a
// switchable backend behavior.
type storageWriteFake struct {
	storagepb.UnimplementedBigQueryWriteServer

	mu          sync.Mutex
	mode        fakeMode
	plan        []fakeMode
	connections int
	requests    int
	ackedRows   int
}

func (f *storageWriteFake) setMode(m fakeMode) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mode = m
}

// setPlan answers the next requests with the given modes, in order, before
// the current mode applies again.
func (f *storageWriteFake) setPlan(modes ...fakeMode) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.plan = modes
}

func (f *storageWriteFake) counts() (connections, requests, ackedRows int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connections, f.requests, f.ackedRows
}

func (f *storageWriteFake) GetWriteStream(
	_ context.Context, req *storagepb.GetWriteStreamRequest,
) (*storagepb.WriteStream, error) {
	return &storagepb.WriteStream{Name: req.GetName(), Location: "us"}, nil
}

func (f *storageWriteFake) AppendRows(srv storagepb.BigQueryWrite_AppendRowsServer) error {
	f.mu.Lock()
	f.connections++
	f.mu.Unlock()
	for {
		req, err := srv.Recv()
		if err != nil {
			return nil
		}
		f.mu.Lock()
		f.requests++
		mode := f.mode
		if len(f.plan) > 0 {
			mode, f.plan = f.plan[0], f.plan[1:]
		} else if mode == fakeFailOnce {
			f.mode = fakeAck
		}
		if mode == fakeAck {
			f.ackedRows += len(req.GetProtoRows().GetRows().GetSerializedRows())
		}
		f.mu.Unlock()

		var resp *storagepb.AppendRowsResponse
		switch mode {
		case fakeNoACK:
			for {
				if _, err := srv.Recv(); err != nil {
					return nil
				}
				f.mu.Lock()
				f.requests++
				f.mu.Unlock()
			}
		case fakeDeny:
			resp = errorResponse(codes.PermissionDenied, "table access denied")
		case fakeFailOnce:
			resp = errorResponse(codes.Unavailable, "backend unavailable")
		case fakeReject:
			resp = errorResponse(codes.InvalidArgument, "request rejected")
		case fakeDeadline:
			resp = errorResponse(codes.DeadlineExceeded, "append deadline exceeded")
		default:
			resp = &storagepb.AppendRowsResponse{
				Response: &storagepb.AppendRowsResponse_AppendResult_{
					AppendResult: &storagepb.AppendRowsResponse_AppendResult{},
				},
			}
		}
		if err := srv.Send(resp); err != nil {
			return nil
		}
	}
}

func errorResponse(code codes.Code, msg string) *storagepb.AppendRowsResponse {
	return &storagepb.AppendRowsResponse{
		Response: &storagepb.AppendRowsResponse_Error{
			Error: &statuspb.Status{Code: int32(code), Message: msg},
		},
	}
}

// newManagedWriterUnderTest wires Writer to a real managedwriter client that
// talks to a fresh storageWriteFake, the same way the exporter does: one
// lifetime context owns both the client and every stream generation.
func newManagedWriterUnderTest(
	t *testing.T, maxInflight, maxRequestBytes int,
) (*Writer, *storageWriteFake, *fakeObserver) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	backend := &storageWriteFake{}
	server := grpc.NewServer()
	storagepb.RegisterBigQueryWriteServer(server, backend)
	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = server.Serve(lis)
	}()
	conn, err := grpc.NewClient("passthrough:///storage-write-fake",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	lifetimeCtx, cancelLifetime := context.WithCancel(context.Background())
	client, err := managedwriter.NewClient(lifetimeCtx, "p",
		option.WithGRPCConn(conn), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	observer := &fakeObserver{}
	w := NewWriter(lifetimeCtx, client, WriterOptions{
		Project: "p", Dataset: "d", Table: "t",
		Descriptor: &descriptorpb.DescriptorProto{
			Name: proto.String("Row"),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name:   proto.String("v"),
				Number: proto.Int32(1),
				Type:   descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum(),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			}},
		},
		MaxRequestBytes:     maxRequestBytes,
		MaxInflightRequests: maxInflight,
		MaxInflightBytes:    64 << 20,
		TraceID:             "writer-test",
		Observer:            observer,
	})
	// Mirrors signalExporter.shutdown: writer, then client, then lifetime.
	t.Cleanup(func() {
		_ = w.Close()
		_ = client.Close()
		cancelLifetime()
		_ = conn.Close()
		server.Stop()
		<-served
	})
	return w, backend, observer
}

// appendWithin runs one exporterhelper-shaped attempt: its context carries the
// attempt timeout and is canceled as soon as the attempt returns.
func appendWithin(w *Writer, timeout time.Duration, rows [][]byte) AppendOutcome {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return w.Append(ctx, rows)
}

func rowsOf(n int) [][]byte {
	rows := make([][]byte, n)
	for i := range rows {
		rows[i] = []byte{0x0a, 0x01, byte('a' + i%26)}
	}
	return rows
}

func waitForBackendRequests(t *testing.T, f *storageWriteFake, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, requests, _ := f.counts(); requests >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	_, requests, _ := f.counts()
	t.Fatalf("backend received %d requests, want %d", requests, want)
}

func waitForZeroGauges(t *testing.T, o *fakeObserver) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if inflight, unresolved, _, _, _ := o.snapshot(); inflight == 0 && unresolved == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	inflight, unresolved, _, _, _ := o.snapshot()
	t.Fatalf("inflight=%d unresolved=%d, want both gauges back at zero", inflight, unresolved)
}

// Before the fix, the stream retained the first attempt's context: once that
// attempt returned, every later append failed with a bare "context canceled"
// until the process restarted.
func TestManagedStreamOutlivesAttemptThatCreatedIt(t *testing.T) {
	w, backend, _ := newManagedWriterUnderTest(t, 8, 1<<20)

	for i := 1; i <= 6; i++ {
		if out := appendWithin(w, 5*time.Second, rowsOf(1)); out.Err != nil {
			t.Fatalf("append %d failed: %v (error_code=%s)", i, out.Err, out.Verdict.Label)
		}
	}
	if connections, _, acked := backend.counts(); connections != 1 || acked != 6 {
		t.Fatalf("connections=%d acked=%d, want all six appends on the first generation", connections, acked)
	}
}

// Run 1 of the live evidence: two appends failed fast with PermissionDenied,
// and every append after the table was fixed still failed until restart.
func TestFastRejectionsDoNotWedgeRestoredBackend(t *testing.T) {
	w, backend, _ := newManagedWriterUnderTest(t, 8, 1<<20)

	backend.setMode(fakeDeny)
	for i := 1; i <= 2; i++ {
		out := appendWithin(w, 5*time.Second, rowsOf(1))
		if out.Verdict.Class != Permanent || out.Verdict.Label != LabelPermission {
			t.Fatalf("rejected append %d = %s/%s (%v), want permanent/%s",
				i, out.Verdict.Class, out.Verdict.Label, out.Err, LabelPermission)
		}
	}

	backend.setMode(fakeAck)
	for i := 1; i <= 3; i++ {
		if out := appendWithin(w, 5*time.Second, rowsOf(1)); out.Err != nil {
			t.Fatalf("append %d after the backend recovered failed: %v (error_code=%s)",
				i, out.Err, out.Verdict.Label)
		}
	}
}

// The historical shipped relationship: ten queue senders share an eight-slot
// flow-control window on a connection that never acknowledges. Every sender
// must return at its deadline, only the window may reach the connection, and
// the next append must recover on a fresh generation without a restart.
func TestTenSendersRecoverFromWedgedEightSlotConnection(t *testing.T) {
	w, backend, observer := newManagedWriterUnderTest(t, 8, 1<<20)
	if out := appendWithin(w, 5*time.Second, rowsOf(1)); out.Err != nil {
		t.Fatalf("warm-up append failed: %v", out.Err)
	}

	backend.setMode(fakeNoACK)
	const senders = 10
	outcomes := make(chan AppendOutcome, senders)
	for range senders {
		go func() { outcomes <- appendWithin(w, 300*time.Millisecond, rowsOf(1)) }()
	}
	uncertain, preDispatch := 0, 0
	for range senders {
		select {
		case out := <-outcomes:
			switch {
			case out.Err == nil:
				t.Fatal("an append on a connection that never acknowledges reported success")
			case out.Verdict.Class == Uncertain && out.Verdict.Label == LabelUncertainAck:
				uncertain++
			case out.Verdict.Class == Retryable:
				preDispatch++
			default:
				t.Fatalf("unexpected verdict %s/%s: %v", out.Verdict.Class, out.Verdict.Label, out.Err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("a sender did not return after its attempt deadline")
		}
	}
	if uncertain != 8 || preDispatch != 2 {
		t.Fatalf("uncertain=%d pre-dispatch=%d, want 8 dispatched and 2 held back by the window",
			uncertain, preDispatch)
	}
	waitForBackendRequests(t, backend, 9)
	if _, requests, _ := backend.counts(); requests != 9 {
		t.Fatalf("backend received %d requests, want the warm-up plus the 8-request window", requests)
	}

	backend.setMode(fakeAck)
	if out := appendWithin(w, 5*time.Second, rowsOf(1)); out.Err != nil {
		t.Fatalf("append after the backend recovered failed: %v (error_code=%s)", out.Err, out.Verdict.Label)
	}
	if connections, _, _ := backend.counts(); connections != 2 {
		t.Fatalf("connections=%d, want the wedged generation replaced exactly once", connections)
	}
	waitForZeroGauges(t, observer)
	// The first sender to expire retires the generation. Each other
	// dispatched sender either expires too or loses its connection to that
	// retirement first; both leave it uncertain, but only an expiry counts as
	// a timeout after dispatch.
	if _, _, _, timeouts, retirements := observer.snapshot(); timeouts < 1 || timeouts > 8 || retirements != 1 {
		t.Fatalf("timeouts_after_dispatch=%d retirements=%d, want 1..8 and exactly 1", timeouts, retirements)
	}
}

// A split request whose first result fails must still wait for every other
// result it dispatched, so none leaves the exporter's accounting.
func TestSplitFailureDrainsEveryManagedStreamResult(t *testing.T) {
	w, backend, observer := newManagedWriterUnderTest(t, 8, 1)

	backend.setMode(fakeFailOnce)
	out := appendWithin(w, 5*time.Second, rowsOf(4))
	if out.Verdict.Label != LabelUnavailable || out.AcknowledgedRows != 3 {
		t.Fatalf("outcome = %s acknowledged=%d (%v), want first failure %s and 3 acknowledged rows",
			out.Verdict.Label, out.AcknowledgedRows, out.Err, LabelUnavailable)
	}
	if inflight, unresolved, waits, _, _ := observer.snapshot(); inflight != 0 || unresolved != 0 || waits != 4 {
		t.Fatalf("after Append returned: inflight=%d unresolved=%d waits=%d, want 0/0/4",
			inflight, unresolved, waits)
	}
}

// A split batch takes its verdict from the request that still owes the most,
// not from whichever failure is drained first: rows that may already be
// applied (uncertain) outrank rows that certainly were not (retryable), which
// outrank rows that never can be (permanent). The exporter drops every row of
// a permanent batch, so a permanent first request must not decide the batch.
// The diagnostic error stays the first failure.
func TestSplitVerdictFollowsDeliveryPrecedenceOnManagedStream(t *testing.T) {
	for _, tc := range []struct {
		name      string
		plan      []fakeMode
		class     Class
		label     string
		firstCode codes.Code
	}{
		{"rejected then unacknowledged", []fakeMode{fakeReject, fakeNoACK},
			Uncertain, LabelUncertainAck, codes.InvalidArgument},
		{"rejected then service deadline", []fakeMode{fakeReject, fakeDeadline},
			Uncertain, LabelUncertainAck, codes.InvalidArgument},
		{"rejected then unavailable", []fakeMode{fakeReject, fakeFailOnce},
			Retryable, LabelUnavailable, codes.InvalidArgument},
		{"service deadline then rejected", []fakeMode{fakeDeadline, fakeReject},
			Uncertain, LabelUncertainAck, codes.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, backend, _ := newManagedWriterUnderTest(t, 8, 1)
			backend.setPlan(tc.plan...)
			observer := &dispatchObserver{}
			w.opts.Observer = observer

			out := appendWithin(w, 300*time.Millisecond, rowsOf(2))
			if n := observer.dispatches.Load(); n != 2 {
				t.Fatalf("dispatched %d requests, want both halves of the split", n)
			}
			if out.Verdict.Class != tc.class || out.Verdict.Label != tc.label {
				t.Fatalf("batch verdict = %s/%s (%v), want %s/%s",
					out.Verdict.Class, out.Verdict.Label, out.Err, tc.class, tc.label)
			}
			if got := status.Code(out.Err); got != tc.firstCode {
				t.Fatalf("diagnostic error code = %s, want the first failure's %s", got, tc.firstCode)
			}
		})
	}
}

// dispatchObserver counts the requests Writer handed to managedwriter, and can
// act at that moment. Whether the service then read a request can race a
// retirement that tears the connection down, so the client-side count is the
// deterministic one.
type dispatchObserver struct {
	fakeObserver
	dispatches atomic.Int32
	onDispatch func()
}

func (o *dispatchObserver) RecordInflightRequests(ctx context.Context, delta int64) {
	o.fakeObserver.RecordInflightRequests(ctx, delta)
	if delta > 0 {
		o.dispatches.Add(1)
		if o.onDispatch != nil {
			o.onDispatch()
		}
	}
}

// When the attempt ends between the requests of a split batch, the unsent
// request is a certain retry but the sent one may already be applied: the
// batch is an uncertain replay, not a routine cancellation.
func TestPartialSplitDispatchIsUncertainOnManagedStream(t *testing.T) {
	w, backend, _ := newManagedWriterUnderTest(t, 8, 1)
	backend.setMode(fakeNoACK)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// End the attempt the moment its first request is handed over.
	observer := &dispatchObserver{onDispatch: cancel}
	w.opts.Observer = observer

	out := w.Append(ctx, rowsOf(2))
	if n := observer.dispatches.Load(); n != 1 {
		t.Fatalf("dispatched %d requests, want only the first before the attempt ended", n)
	}
	if _, requests, _ := backend.counts(); requests > 1 {
		t.Fatalf("backend received %d requests, want at most the first", requests)
	}
	if out.Verdict.Class != Uncertain || out.Verdict.Label != LabelUncertainAck {
		t.Fatalf("partially dispatched batch = %s/%s (%v), want uncertain/%s",
			out.Verdict.Class, out.Verdict.Label, out.Err, LabelUncertainAck)
	}
}

// With a one-slot window the second request of a split can only be sent after
// the first is drained. A permanent rejection of the first must not stop the
// second from being sent; a batch dropped as permanent would otherwise lose
// rows nobody ever tried to deliver.
func TestPermanentRejectionDoesNotStopSplitDispatchOnManagedStream(t *testing.T) {
	w, backend, _ := newManagedWriterUnderTest(t, 1, 1)
	backend.setPlan(fakeReject)

	out := appendWithin(w, 5*time.Second, rowsOf(2))
	if _, requests, acked := backend.counts(); requests != 2 || acked != 1 {
		t.Fatalf("backend received %d requests and acknowledged %d rows, want both requests sent and the second acknowledged",
			requests, acked)
	}
	if out.AcknowledgedRows != 1 || out.Verdict.Class != Permanent || out.Verdict.Label != LabelInvalidArgument {
		t.Fatalf("outcome = %s/%s acknowledged=%d (%v), want permanent/%s with the second request acknowledged",
			out.Verdict.Class, out.Verdict.Label, out.AcknowledgedRows, out.Err, LabelInvalidArgument)
	}
}

// Retiring a generation closes the connection under every request still in
// flight on it. Those requests reached the service and may have been applied,
// so their replay is an uncertain-acknowledgement replay, not a certain retry.
func TestRetiredGenerationOutcomesAreUncertain(t *testing.T) {
	w, backend, observer := newManagedWriterUnderTest(t, 8, 1<<20)
	if out := appendWithin(w, 5*time.Second, rowsOf(1)); out.Err != nil {
		t.Fatalf("warm-up append failed: %v", out.Err)
	}

	backend.setMode(fakeNoACK)
	sibling := make(chan AppendOutcome, 1)
	go func() { sibling <- appendWithin(w, 10*time.Second, rowsOf(1)) }()
	waitForBackendRequests(t, backend, 2)

	// This attempt expires after dispatch and retires the shared generation.
	if out := appendWithin(w, 150*time.Millisecond, rowsOf(1)); out.Verdict.Label != LabelUncertainAck {
		t.Fatalf("expired attempt = %s/%s (%v), want uncertain_ack", out.Verdict.Class, out.Verdict.Label, out.Err)
	}
	select {
	case out := <-sibling:
		if out.Verdict.Class != Uncertain || out.Verdict.Label != LabelUncertainAck {
			t.Fatalf("sibling on the retired generation = %s/%s (%v), want uncertain/%s",
				out.Verdict.Class, out.Verdict.Label, out.Err, LabelUncertainAck)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("sibling append did not finish after its generation was retired")
	}
	// Only the expired attempt timed out; the sibling's attempt was still live.
	if _, _, _, timeouts, retirements := observer.snapshot(); timeouts != 1 || retirements != 1 {
		t.Fatalf("timeouts_after_dispatch=%d retirements=%d, want 1 and 1", timeouts, retirements)
	}

	backend.setMode(fakeAck)
	if out := appendWithin(w, 5*time.Second, rowsOf(1)); out.Err != nil {
		t.Fatalf("append after the backend recovered failed: %v", out.Err)
	}
}

// Closing the writer tears down the connection under an append that is still
// waiting for its acknowledgement. managedwriter reports that as a gRPC
// Canceled status, which must be labeled as shutdown rather than as a routine
// runtime cancellation.
func TestShutdownLabelsAppendInFlightOnManagedStream(t *testing.T) {
	w, backend, _ := newManagedWriterUnderTest(t, 8, 1<<20)
	if out := appendWithin(w, 5*time.Second, rowsOf(1)); out.Err != nil {
		t.Fatalf("warm-up append failed: %v", out.Err)
	}

	backend.setMode(fakeNoACK)
	inflight := make(chan AppendOutcome, 1)
	go func() { inflight <- appendWithin(w, 10*time.Second, rowsOf(1)) }()
	waitForBackendRequests(t, backend, 2)

	closed := make(chan error, 1)
	go func() { closed <- w.Close() }()
	select {
	case out := <-inflight:
		if out.Verdict.Class != Retryable || out.Verdict.Label != LabelShutdown {
			t.Fatalf("append in flight at shutdown = %s/%s (%v), want retryable/%s",
				out.Verdict.Class, out.Verdict.Label, out.Err, LabelShutdown)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("append in flight did not finish after the writer closed")
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return")
	}
}

// managedwriter reports io.EOF as the terminal state of a stream closed on
// purpose. A clean shutdown after successful appends must not surface it as a
// failure.
func TestCleanCloseAfterManagedStreamAppendReportsNoError(t *testing.T) {
	w, _, _ := newManagedWriterUnderTest(t, 8, 1<<20)
	if out := appendWithin(w, 5*time.Second, rowsOf(1)); out.Err != nil {
		t.Fatalf("append failed: %v", out.Err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("clean Close returned %v, want nil", err)
	}
}

// guardedStream records any Close of a generation that is still the writer's
// current stream. Only retired or discarded generations may be closed.
type guardedStream struct {
	stream
	w          *Writer
	violations *atomic.Int32
}

func (g *guardedStream) Close() error {
	g.w.mu.Lock()
	current := g.w.ms != nil && g.w.ms.stream == stream(g)
	g.w.mu.Unlock()
	if current {
		g.violations.Add(1)
	}
	return g.stream.Close()
}

// Concurrent appends with short deadlines, a backend flipping between healthy,
// wedged and failing, repeated retirement and replacement, and a shutdown in
// the middle of traffic. The writer must never close its current generation,
// never deadlock, never classify its own lifecycle as a permanent rejection
// (which would drop the batch), and must recover once the backend does.
func TestConcurrentAppendRetirementAndShutdownOnManagedStream(t *testing.T) {
	w, backend, observer := newManagedWriterUnderTest(t, 8, 1<<20)
	var violations atomic.Int32
	realFactory := w.factory
	w.factory = func(ctx context.Context, opts ...managedwriter.WriterOption) (stream, error) {
		s, err := realFactory(ctx, opts...)
		if err != nil {
			return nil, err
		}
		return &guardedStream{stream: s, w: w, violations: &violations}, nil
	}

	var permanent atomic.Int32
	stop := make(chan struct{})
	var workers sync.WaitGroup
	for i := range 8 {
		workers.Add(1)
		go func(seed int64) {
			defer workers.Done()
			r := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-stop:
					return
				default:
				}
				timeout := time.Duration(5+r.Intn(60)) * time.Millisecond
				if out := appendWithin(w, timeout, rowsOf(1+r.Intn(3))); out.Verdict.Class == Permanent && out.Err != nil {
					permanent.Add(1)
				}
			}
		}(int64(i))
	}

	r := rand.New(rand.NewSource(99))
	modes := []fakeMode{fakeAck, fakeNoACK, fakeAck, fakeFailOnce}
	for range 25 {
		backend.setMode(modes[r.Intn(len(modes))])
		time.Sleep(time.Duration(20+r.Intn(30)) * time.Millisecond)
	}
	backend.setMode(fakeAck)
	recovered := false
	for i := 0; i < 50 && !recovered; i++ {
		recovered = appendWithin(w, time.Second, rowsOf(1)).Err == nil
	}
	if !recovered {
		t.Error("no append succeeded after the backend recovered")
	}

	closed := make(chan error, 1)
	go func() { closed <- w.Close() }()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close deadlocked with concurrent appends")
	}
	close(stop)
	stopped := make(chan struct{})
	go func() { workers.Wait(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("appends did not return after Close")
	}

	if n := violations.Load(); n != 0 {
		t.Fatalf("the current stream generation was closed %d times", n)
	}
	if n := permanent.Load(); n != 0 {
		t.Fatalf("%d appends were classified permanent although the backend never rejected one", n)
	}
	if _, _, _, _, retirements := observer.snapshot(); retirements == 0 {
		t.Fatal("no generation was retired; the storm did not exercise recovery")
	}
	waitForZeroGauges(t, observer)
}
