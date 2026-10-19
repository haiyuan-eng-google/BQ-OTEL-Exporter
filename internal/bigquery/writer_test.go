// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigquery

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSplitRequests(t *testing.T) {
	row := func(n int) []byte { return make([]byte, n) }

	t.Run("groups rows under the threshold", func(t *testing.T) {
		got := SplitRequests([][]byte{row(400), row(400), row(400)}, 1000)
		if len(got) != 2 {
			t.Fatalf("got %d requests, want 2", len(got))
		}
		if len(got[0]) != 2 || len(got[1]) != 1 {
			t.Fatalf("unexpected grouping: %d then %d", len(got[0]), len(got[1]))
		}
	})

	t.Run("no request exceeds the threshold", func(t *testing.T) {
		rows := make([][]byte, 50)
		for i := range rows {
			rows[i] = row(300)
		}
		for _, req := range SplitRequests(rows, 1000) {
			total := 0
			for _, r := range req {
				total += len(r)
			}
			// A single oversized row is the documented exception; none here.
			if total > 1000 && len(req) > 1 {
				t.Fatalf("request of %d bytes exceeds the 1000-byte threshold", total)
			}
		}
	})

	// Oversized rows must not vanish. The caller rejects them earlier with a
	// count; dropping one here would lose data with no record of it.
	t.Run("an oversized row still goes out alone", func(t *testing.T) {
		got := SplitRequests([][]byte{row(5000)}, 1000)
		if len(got) != 1 || len(got[0]) != 1 {
			t.Fatalf("oversized row was not preserved: %v", got)
		}
	})

	t.Run("every row survives the split", func(t *testing.T) {
		rows := make([][]byte, 37)
		for i := range rows {
			rows[i] = row(100)
		}
		count := 0
		for _, req := range SplitRequests(rows, 250) {
			count += len(req)
		}
		if count != len(rows) {
			t.Fatalf("split produced %d rows from %d", count, len(rows))
		}
	})

	t.Run("accounts for protobuf wire framing and request envelope reserve", func(t *testing.T) {
		// Two 480-byte rows sum to 960 raw bytes (<= 1000), but with a 64-byte
		// envelope reserve and 3 bytes of ProtoRows tag+varint framing per row,
		// their wire size is 64 + 483 + 483 = 1030 > 1000, so they must split.
		got := SplitRequests([][]byte{row(480), row(480)}, 1000)
		if len(got) != 2 {
			t.Fatalf("got %d requests, want 2 when wire framing exceeds budget", len(got))
		}
	})
}

// --- fakes ---

var closedReadyCh = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

type fakeResult struct {
	err  error
	resp *storagepb.AppendRowsResponse
}

func (f fakeResult) Ready() <-chan struct{}                   { return closedReadyCh }
func (f fakeResult) GetResult(context.Context) (int64, error) { return 0, f.err }
func (f fakeResult) FullResponse(context.Context) (*storagepb.AppendRowsResponse, error) {
	return f.resp, nil
}

type fakeStream struct {
	results  []appendResult
	dispatch error
	calls    int
	closed   int
}

type fakeObserver struct {
	mu sync.Mutex

	inflight           int64
	unresolved         int64
	waits              int
	timeouts           int
	retirements        int
	backpressureEvents []string
	backpressureLevel  float64
	effectiveBytes     int64
}

func (o *fakeObserver) RecordInflightRequests(_ context.Context, delta int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.inflight += delta
}

func (o *fakeObserver) RecordUnresolvedResults(_ context.Context, delta int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.unresolved += delta
}

func (o *fakeObserver) RecordAppendResultWait(_ context.Context, _ time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.waits++
}

func (o *fakeObserver) RecordTimeoutAfterDispatch(context.Context) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.timeouts++
}

func (o *fakeObserver) RecordStreamRetirement(context.Context) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.retirements++
}

func (o *fakeObserver) RecordBackpressureLevel(_ context.Context, _, _ string, level float64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.backpressureLevel = level
}

func (o *fakeObserver) RecordEffectiveRequestBytes(_ context.Context, _, _ string, bytes int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.effectiveBytes = bytes
}

func (o *fakeObserver) RecordBackpressureEvent(_ context.Context, _, _, trigger string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.backpressureEvents = append(o.backpressureEvents, trigger)
}

func (o *fakeObserver) snapshot() (inflight, unresolved int64, waits, timeouts, retirements int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.inflight, o.unresolved, o.waits, o.timeouts, o.retirements
}

func (f *fakeStream) AppendRows(_ context.Context, _ [][]byte, _ ...managedwriter.AppendOption) (appendResult, error) {
	if f.dispatch != nil {
		return nil, f.dispatch
	}
	r := f.results[min(f.calls, len(f.results)-1)]
	f.calls++
	return r, nil
}

func (f *fakeStream) Close() error { f.closed++; return nil }

func newTestWriter(s *fakeStream) *Writer {
	return &Writer{
		opts: WriterOptions{Project: "p", Dataset: "d", Table: "t", MaxRequestBytes: 1000},
		factory: func(context.Context, ...managedwriter.WriterOption) (stream, error) {
			return s, nil
		},
	}
}

type controlledResult struct {
	started chan struct{}
	ready   chan struct{}
	once    sync.Once

	mu       sync.Mutex
	getCalls int
	err      error
}

func newControlledResult() *controlledResult {
	return &controlledResult{
		started: make(chan struct{}),
		ready:   make(chan struct{}),
	}
}

func (r *controlledResult) Ready() <-chan struct{} {
	return r.ready
}

func (r *controlledResult) GetResult(ctx context.Context) (int64, error) {
	r.mu.Lock()
	r.getCalls++
	r.mu.Unlock()
	r.once.Do(func() { close(r.started) })
	select {
	case <-r.ready:
		r.mu.Lock()
		defer r.mu.Unlock()
		return 0, r.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (r *controlledResult) FullResponse(context.Context) (*storagepb.AppendRowsResponse, error) {
	return nil, nil
}

func (r *controlledResult) calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.getCalls
}

func (r *controlledResult) complete(err error) {
	r.mu.Lock()
	r.err = err
	r.mu.Unlock()
	close(r.ready)
}

func (r *controlledResult) waitForCalls(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if r.calls() >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("GetResult called %d times, want at least %d", r.calls(), want)
}

type scriptedStream struct {
	mu sync.Mutex

	results        []appendResult
	dispatchErrors []error
	calls          int
	closed         int
}

func (s *scriptedStream) AppendRows(
	_ context.Context, _ [][]byte, _ ...managedwriter.AppendOption,
) (appendResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.calls
	s.calls++
	if i < len(s.dispatchErrors) && s.dispatchErrors[i] != nil {
		return nil, s.dispatchErrors[i]
	}
	if i >= len(s.results) {
		return nil, errors.New("unexpected append")
	}
	return s.results[i], nil
}

func (s *scriptedStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed++
	return nil
}

func (s *scriptedStream) closeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func newScriptedWriter(streams ...stream) (*Writer, *[]context.Context) {
	createdWith := []context.Context{}
	next := 0
	w := &Writer{
		opts: WriterOptions{
			Project:             "p",
			Dataset:             "d",
			Table:               "t",
			MaxRequestBytes:     1,
			MaxInflightRequests: 8,
		},
		factory: func(ctx context.Context, _ ...managedwriter.WriterOption) (stream, error) {
			createdWith = append(createdWith, ctx)
			if next >= len(streams) {
				return nil, errors.New("unexpected stream creation")
			}
			s := streams[next]
			next++
			return s, nil
		},
	}
	return w, &createdWith
}

type blockingFactory struct {
	mu sync.Mutex

	calls    int
	started  chan struct{}
	release  chan struct{}
	canceled chan struct{}
	stream   stream
	once     sync.Once
}

func newBlockingFactory(s stream) *blockingFactory {
	return &blockingFactory{
		started:  make(chan struct{}),
		release:  make(chan struct{}),
		canceled: make(chan struct{}),
		stream:   s,
	}
}

func (f *blockingFactory) create(ctx context.Context, _ ...managedwriter.WriterOption) (stream, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	f.once.Do(func() { close(f.started) })
	select {
	case <-ctx.Done():
		close(f.canceled)
		return nil, ctx.Err()
	case <-f.release:
		return f.stream, nil
	}
}

func (f *blockingFactory) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

type stickyNoACKStream struct {
	result *controlledResult

	mu     sync.Mutex
	closed int
}

func newStickyNoACKStream() *stickyNoACKStream {
	return &stickyNoACKStream{result: newControlledResult()}
}

func (s *stickyNoACKStream) AppendRows(
	context.Context, [][]byte, ...managedwriter.AppendOption,
) (appendResult, error) {
	return s.result, nil
}

func (s *stickyNoACKStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed++
	return nil
}

type flowWindowResult struct {
	once    sync.Once
	release func()
}

func (r *flowWindowResult) Ready() <-chan struct{} {
	return closedReadyCh
}

func (r *flowWindowResult) GetResult(context.Context) (int64, error) {
	r.once.Do(r.release)
	return 0, nil
}

func (r *flowWindowResult) FullResponse(context.Context) (*storagepb.AppendRowsResponse, error) {
	return nil, nil
}

type flowWindowStream struct {
	slots     chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
}

func newFlowWindowStream(size int) *flowWindowStream {
	return &flowWindowStream{slots: make(chan struct{}, size), closed: make(chan struct{})}
}

func (s *flowWindowStream) AppendRows(
	ctx context.Context, _ [][]byte, _ ...managedwriter.AppendOption,
) (appendResult, error) {
	select {
	case s.slots <- struct{}{}:
		return &flowWindowResult{release: func() { <-s.slots }}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.closed:
		return nil, context.Canceled
	}
}

func (s *flowWindowStream) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}

type concurrentFlowWindowStream struct {
	slots  chan struct{}
	closed chan struct{}

	mu         sync.Mutex
	firstByRow map[byte]bool
	firstsSeen chan struct{}
	firstsOnce sync.Once
	closeOnce  sync.Once
}

func newConcurrentFlowWindowStream(size int) *concurrentFlowWindowStream {
	return &concurrentFlowWindowStream{
		slots:      make(chan struct{}, size),
		closed:     make(chan struct{}),
		firstByRow: make(map[byte]bool),
		firstsSeen: make(chan struct{}),
	}
}

func (s *concurrentFlowWindowStream) AppendRows(
	ctx context.Context, rows [][]byte, _ ...managedwriter.AppendOption,
) (appendResult, error) {
	select {
	case s.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.closed:
		return nil, context.Canceled
	}

	rowID := rows[0][0]
	s.mu.Lock()
	first := !s.firstByRow[rowID]
	s.firstByRow[rowID] = true
	if len(s.firstByRow) == 2 {
		s.firstsOnce.Do(func() { close(s.firstsSeen) })
	}
	s.mu.Unlock()
	if first {
		select {
		case <-s.firstsSeen:
		case <-ctx.Done():
			<-s.slots
			return nil, ctx.Err()
		case <-s.closed:
			<-s.slots
			return nil, context.Canceled
		}
	}
	return &flowWindowResult{release: func() { <-s.slots }}, nil
}

func (s *concurrentFlowWindowStream) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}

type closeCompletesStream struct {
	result *controlledResult
	once   sync.Once
}

func (s *closeCompletesStream) AppendRows(
	context.Context, [][]byte, ...managedwriter.AppendOption,
) (appendResult, error) {
	return s.result, nil
}

func (s *closeCompletesStream) Close() error {
	s.once.Do(func() { s.result.complete(context.Canceled) })
	return nil
}

type lifecycleBoundCloseStream struct {
	ctx context.Context
}

func (s *lifecycleBoundCloseStream) AppendRows(
	context.Context, [][]byte, ...managedwriter.AppendOption,
) (appendResult, error) {
	return fakeResult{}, nil
}

func (s *lifecycleBoundCloseStream) Close() error {
	select {
	case <-s.ctx.Done():
		return nil
	case <-time.After(100 * time.Millisecond):
		return errors.New("stream lifecycle was not canceled before Close")
	}
}

func TestAppendSuccess(t *testing.T) {
	s := &fakeStream{results: []appendResult{fakeResult{}}}
	out := newTestWriter(s).Append(context.Background(), [][]byte{{1}, {2}, {3}})

	if out.Err != nil {
		t.Fatalf("unexpected error: %v", out.Err)
	}
	if out.AcknowledgedRows != 3 {
		t.Fatalf("acknowledged %d rows, want 3", out.AcknowledgedRows)
	}
}

func TestAppendStopsWaitingForStreamCreationAndCloseCancelsConstructor(t *testing.T) {
	factory := newBlockingFactory(&scriptedStream{results: []appendResult{fakeResult{}}})
	w := &Writer{
		opts:        WriterOptions{Project: "p", Dataset: "d", Table: "t", MaxRequestBytes: 1},
		lifetimeCtx: context.Background(),
		factory:     factory.create,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	appendDone := make(chan AppendOutcome, 1)
	go func() { appendDone <- w.Append(ctx, [][]byte{{1}}) }()
	<-factory.started

	var out AppendOutcome
	select {
	case out = <-appendDone:
	case <-time.After(250 * time.Millisecond):
		close(factory.release)
		<-appendDone
		_ = w.Close()
		t.Fatal("Append did not stop waiting when its attempt context expired")
	}
	if !errors.Is(out.Err, context.DeadlineExceeded) {
		t.Fatalf("append error = %v, want deadline exceeded", out.Err)
	}
	if out.Verdict.Class != Retryable || out.Verdict.Label == LabelUncertainAck {
		t.Fatalf("pre-dispatch verdict = %s/%s, want retryable and not uncertain",
			out.Verdict.Class, out.Verdict.Label)
	}

	closed := make(chan error, 1)
	go func() { closed <- w.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel and join the stream constructor")
	}
	select {
	case <-factory.canceled:
	default:
		t.Fatal("stream constructor context was not canceled by Close")
	}
}

func TestCloseJoinsAndDiscardsLateConstructedStream(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	late := &scriptedStream{}
	w := &Writer{
		opts: WriterOptions{Project: "p", Dataset: "d", Table: "t", MaxRequestBytes: 1},
		factory: func(context.Context, ...managedwriter.WriterOption) (stream, error) {
			close(started)
			<-release
			return late, nil
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	appendDone := make(chan AppendOutcome, 1)
	go func() { appendDone <- w.Append(ctx, [][]byte{{1}}) }()
	<-started
	if out := <-appendDone; !errors.Is(out.Err, context.DeadlineExceeded) {
		t.Fatalf("append error = %v, want deadline exceeded", out.Err)
	}

	firstClose := make(chan error, 1)
	secondClose := make(chan error, 1)
	go func() { firstClose <- w.Close() }()
	go func() { secondClose <- w.Close() }()
	select {
	case <-firstClose:
		t.Fatal("Close returned before the in-progress constructor terminated")
	case <-secondClose:
		t.Fatal("concurrent Close returned before the first Close completed")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	for i, result := range []<-chan error{firstClose, secondClose} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("Close %d returned %v", i+1, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("Close %d did not join the constructor", i+1)
		}
	}
	if got := late.closeCount(); got != 1 {
		t.Fatalf("late constructed stream closed %d times, want once", got)
	}
}

func TestConcurrentAppendsShareStreamCreation(t *testing.T) {
	s := &scriptedStream{results: []appendResult{fakeResult{}}}
	factory := newBlockingFactory(s)
	w := &Writer{
		opts:        WriterOptions{Project: "p", Dataset: "d", Table: "t", MaxRequestBytes: 1},
		lifetimeCtx: context.Background(),
		factory:     factory.create,
	}
	defer func() { _ = w.Close() }()

	firstDone := make(chan AppendOutcome, 1)
	go func() { firstDone <- w.Append(context.Background(), [][]byte{{1}}) }()
	<-factory.started

	waitCtx, cancelWait := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelWait()
	waiterDone := make(chan AppendOutcome, 1)
	go func() { waiterDone <- w.Append(waitCtx, [][]byte{{2}}) }()
	select {
	case out := <-waiterDone:
		if !errors.Is(out.Err, context.DeadlineExceeded) {
			t.Fatalf("waiting append error = %v, want deadline exceeded", out.Err)
		}
	case <-time.After(250 * time.Millisecond):
		close(factory.release)
		<-firstDone
		<-waiterDone
		t.Fatal("concurrent waiter ignored its attempt deadline")
	}
	if got := factory.callCount(); got != 1 {
		t.Fatalf("stream factory called %d times while creation was shared, want 1", got)
	}

	close(factory.release)
	if out := <-firstDone; out.Err != nil {
		t.Fatalf("first append failed after shared creation: %v", out.Err)
	}
	if got := factory.callCount(); got != 1 {
		t.Fatalf("stream factory called %d times, want 1", got)
	}
}

func TestRetiredGenerationWindowBoundsRepeatedNoACK(t *testing.T) {
	var mu sync.Mutex
	var streams []*stickyNoACKStream
	w := &Writer{
		opts:        WriterOptions{Project: "p", Dataset: "d", Table: "t", MaxRequestBytes: 1},
		lifetimeCtx: context.Background(),
		factory: func(context.Context, ...managedwriter.WriterOption) (stream, error) {
			s := newStickyNoACKStream()
			mu.Lock()
			streams = append(streams, s)
			mu.Unlock()
			return s, nil
		},
	}

	appendAndCancel := func() AppendOutcome {
		mu.Lock()
		before := len(streams)
		mu.Unlock()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan AppendOutcome, 1)
		go func() { done <- w.Append(ctx, [][]byte{{1}}) }()
		for {
			mu.Lock()
			var latest *stickyNoACKStream
			if len(streams) > before {
				latest = streams[len(streams)-1]
			}
			mu.Unlock()
			if latest != nil {
				select {
				case <-latest.result.started:
					cancel()
					return <-done
				default:
				}
			}
			time.Sleep(time.Millisecond)
		}
	}

	for range 2 {
		if out := appendAndCancel(); out.Verdict.Class != Uncertain {
			t.Fatalf("canceled append class = %s, want uncertain", out.Verdict.Class)
		}
	}

	thirdCtx, cancelThird := context.WithTimeout(context.Background(), 30*time.Millisecond)
	third := w.Append(thirdCtx, [][]byte{{3}})
	cancelThird()
	if !errors.Is(third.Err, context.DeadlineExceeded) {
		t.Fatalf("third append error = %v, want deadline while generation window is full", third.Err)
	}
	if third.Verdict.Class != Retryable || third.Verdict.Label == LabelUncertainAck {
		t.Fatalf("pre-dispatch verdict = %s/%s, want retryable and not uncertain",
			third.Verdict.Class, third.Verdict.Label)
	}

	mu.Lock()
	created := len(streams)
	owned := append([]*stickyNoACKStream(nil), streams...)
	mu.Unlock()
	for _, s := range owned {
		s.result.complete(nil)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if created != 2 {
		t.Fatalf("created %d unresolved stream generations, want hard bound 2", created)
	}
}

func TestAppendDrainsAtConfiguredFlowWindow(t *testing.T) {
	s := newFlowWindowStream(2)
	w := &Writer{
		opts: WriterOptions{
			Project: "p", Dataset: "d", Table: "t", MaxRequestBytes: 1, MaxInflightRequests: 2,
		},
		factory: func(context.Context, ...managedwriter.WriterOption) (stream, error) { return s, nil },
	}

	done := make(chan AppendOutcome, 1)
	go func() { done <- w.Append(context.Background(), [][]byte{{1}, {2}, {3}, {4}, {5}}) }()
	select {
	case out := <-done:
		if out.Err != nil || out.AcknowledgedRows != 5 {
			t.Fatalf("append outcome = %+v, want all five rows acknowledged", out)
		}
	case <-time.After(time.Second):
		_ = w.Close()
		<-done
		t.Fatal("Append self-deadlocked after filling its managedwriter flow window")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentAppendsShareGenerationFlowWindow(t *testing.T) {
	s := newConcurrentFlowWindowStream(2)
	w := &Writer{
		opts: WriterOptions{
			Project: "p", Dataset: "d", Table: "t", MaxRequestBytes: 1, MaxInflightRequests: 2,
		},
		factory: func(context.Context, ...managedwriter.WriterOption) (stream, error) { return s, nil },
	}

	done := make(chan AppendOutcome, 2)
	go func() { done <- w.Append(context.Background(), [][]byte{{1}, {1}}) }()
	go func() { done <- w.Append(context.Background(), [][]byte{{2}, {2}}) }()
	for i := 0; i < 2; i++ {
		select {
		case out := <-done:
			if out.Err != nil || out.AcknowledgedRows != 2 {
				t.Fatalf("append outcome = %+v, want two acknowledged rows", out)
			}
		case <-time.After(time.Second):
			_ = w.Close()
			t.Fatal("concurrent Appends deadlocked on the stream-wide flow window")
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAppendWaitingForGenerationSlotTimesOutBeforeDispatch(t *testing.T) {
	pending := newControlledResult()
	s := &scriptedStream{results: []appendResult{pending, fakeResult{}}}
	w, createdWith := newScriptedWriter(s)
	w.opts.MaxInflightRequests = 1

	firstDone := make(chan AppendOutcome, 1)
	go func() { firstDone <- w.Append(context.Background(), [][]byte{{1}}) }()
	<-pending.started

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	waiting := w.Append(ctx, [][]byte{{2}})
	if !errors.Is(waiting.Err, context.DeadlineExceeded) {
		t.Fatalf("waiting append error = %v, want deadline exceeded", waiting.Err)
	}
	if waiting.Verdict.Class != Retryable || waiting.Verdict.Label == LabelUncertainAck {
		t.Fatalf("pre-dispatch verdict = %s/%s, want retryable and not uncertain",
			waiting.Verdict.Class, waiting.Verdict.Label)
	}
	if s.calls != 1 {
		t.Fatalf("AppendRows calls = %d, want timed-out request not dispatched", s.calls)
	}

	pending.complete(nil)
	if out := <-firstDone; out.Err != nil {
		t.Fatalf("first append failed: %v", out.Err)
	}
	if out := w.Append(context.Background(), [][]byte{{3}}); out.Err != nil {
		t.Fatalf("append after slot release failed: %v", out.Err)
	}
	if len(*createdWith) != 1 {
		t.Fatalf("created %d streams, want pre-dispatch timeout to keep the generation", len(*createdWith))
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCloseOverlappingActiveAppend(t *testing.T) {
	result := newControlledResult()
	s := &closeCompletesStream{result: result}
	w := &Writer{
		opts:    WriterOptions{Project: "p", Dataset: "d", Table: "t", MaxRequestBytes: 1},
		factory: func(context.Context, ...managedwriter.WriterOption) (stream, error) { return s, nil },
	}

	appendDone := make(chan AppendOutcome, 1)
	go func() { appendDone <- w.Append(context.Background(), [][]byte{{1}}) }()
	<-result.started
	closeDone := make(chan error, 1)
	go func() { closeDone <- w.Close() }()

	select {
	case <-appendDone:
	case <-time.After(time.Second):
		t.Fatal("active Append did not finish after Close retired its stream")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close deadlocked with an active Append")
	}
	if out := w.Append(context.Background(), [][]byte{{2}}); out.Err == nil {
		t.Fatal("append after Close unexpectedly succeeded")
	}
}

// A caller that reaches the writer after shutdown began gets its batch handed
// back to exporterhelper, labeled as shutdown. A permanent verdict would drop
// telemetry the persistent queue could otherwise keep.
func TestAppendAfterCloseIsShutdownNotPermanent(t *testing.T) {
	w := newTestWriter(&fakeStream{results: []appendResult{fakeResult{}}})
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	out := w.Append(context.Background(), [][]byte{{1}})
	if out.Verdict.Class != Retryable || out.Verdict.Label != LabelShutdown {
		t.Fatalf("append after Close = %s/%s (%v), want retryable/%s",
			out.Verdict.Class, out.Verdict.Label, out.Err, LabelShutdown)
	}
}

func TestCloseCancelsOwnedContextBeforeClosingStream(t *testing.T) {
	w := &Writer{
		opts: WriterOptions{Project: "p", Dataset: "d", Table: "t", MaxRequestBytes: 1},
		factory: func(ctx context.Context, _ ...managedwriter.WriterOption) (stream, error) {
			return &lifecycleBoundCloseStream{ctx: ctx}, nil
		},
	}
	if out := w.Append(context.Background(), [][]byte{{1}}); out.Err != nil {
		t.Fatalf("creating the lifecycle-bound stream: %v", out.Err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// A managed stream retains the context used to create it. An exporterhelper
// attempt context is canceled as soon as that attempt returns, so retaining it
// here would poison every later append on the same stream.
func TestStreamFactoryDoesNotRetainAttemptContext(t *testing.T) {
	s := &scriptedStream{results: []appendResult{fakeResult{}}}
	w, createdWith := newScriptedWriter(s)

	attemptCtx, cancelAttempt := context.WithCancel(context.Background())
	out := w.Append(attemptCtx, [][]byte{{1}})
	if out.Err != nil {
		t.Fatalf("append failed: %v", out.Err)
	}
	cancelAttempt()

	if len(*createdWith) != 1 {
		t.Fatalf("created %d streams, want 1", len(*createdWith))
	}
	select {
	case <-(*createdWith)[0].Done():
		t.Fatal("stream factory retained the canceled append-attempt context")
	default:
	}
}

// Every result already handed back by AppendRows must be observed even when
// an earlier split fails. Returning early leaves managedwriter flow-control
// capacity occupied by results the exporter never accounts for.
func TestAppendDrainsEveryDispatchedResultAfterResultFailure(t *testing.T) {
	failed := newControlledResult()
	succeeded := newControlledResult()
	failed.complete(status.Error(codes.Unavailable, "down"))
	succeeded.complete(nil)
	s := &scriptedStream{results: []appendResult{failed, succeeded}}
	w, _ := newScriptedWriter(s)
	w.opts.MaxInflightRequests = 2

	out := w.Append(context.Background(), [][]byte{{1}, {2}})
	if out.Verdict.Class != Retryable {
		t.Fatalf("class = %s, want retryable", out.Verdict.Class)
	}
	if failed.calls() != 1 || succeeded.calls() != 1 {
		t.Fatalf("GetResult calls = failed:%d succeeded:%d, want 1 each",
			failed.calls(), succeeded.calls())
	}
}

func TestAppendStopsDispatchingAfterWindowedResultFailure(t *testing.T) {
	failed := newControlledResult()
	succeeded := newControlledResult()
	failed.complete(status.Error(codes.Unavailable, "down"))
	succeeded.complete(nil)
	s := &scriptedStream{results: []appendResult{failed, succeeded, fakeResult{}}}
	w, _ := newScriptedWriter(s)
	w.opts.MaxInflightRequests = 2

	out := w.Append(context.Background(), [][]byte{{1}, {2}, {3}})
	if out.Verdict.Class != Retryable {
		t.Fatalf("class = %s, want retryable", out.Verdict.Class)
	}
	if s.calls != 2 {
		t.Fatalf("AppendRows calls = %d, want the two requests dispatched before the failure", s.calls)
	}
	if failed.calls() != 1 || succeeded.calls() != 1 {
		t.Fatalf("GetResult calls = failed:%d succeeded:%d, want every dispatched result once",
			failed.calls(), succeeded.calls())
	}
}

// A later dispatch can fail after earlier split requests have already been
// accepted. Those earlier append results still need to be drained.
func TestAppendDrainsDispatchedResultsAfterLaterDispatchFailure(t *testing.T) {
	dispatched := newControlledResult()
	dispatched.complete(nil)
	s := &scriptedStream{
		results:        []appendResult{dispatched, nil},
		dispatchErrors: []error{nil, status.Error(codes.Unavailable, "down")},
	}
	w, _ := newScriptedWriter(s)

	out := w.Append(context.Background(), [][]byte{{1}, {2}})
	if out.Verdict.Class != Retryable {
		t.Fatalf("class = %s, want retryable", out.Verdict.Class)
	}
	if dispatched.calls() != 1 {
		t.Fatalf("first dispatched result was observed %d times, want 1", dispatched.calls())
	}
}

func TestAppendPreservesFirstFailureWhileDrainingWindow(t *testing.T) {
	firstFailure := newControlledResult()
	firstFailure.complete(status.Error(codes.Unavailable, "first result failed"))
	s := &scriptedStream{results: []appendResult{
		firstFailure,
		fakeResult{err: status.Error(codes.InvalidArgument, "second result failed")},
	}}
	w, _ := newScriptedWriter(s)
	w.opts.MaxInflightRequests = 2

	out := w.Append(context.Background(), [][]byte{{1}, {2}})
	if got := status.Code(out.Err); got != codes.Unavailable {
		t.Fatalf("error code = %s, want first transport failure %s", got, codes.Unavailable)
	}
}

// Once AppendRows returns a result, cancellation makes the acknowledgement
// ambiguous. The affected stream generation must be retired before another
// exporterhelper attempt can reuse it.
func TestAppendRetiresGenerationWhenAttemptEndsAfterDispatch(t *testing.T) {
	for name, makeAttempt := range map[string]func() (context.Context, func()){
		"canceled": func() (context.Context, func()) {
			ctx, cancel := context.WithCancel(context.Background())
			return ctx, cancel
		},
		"deadline": func() (context.Context, func()) {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			return ctx, cancel
		},
	} {
		t.Run(name, func(t *testing.T) {
			pending := newControlledResult()
			first := &scriptedStream{results: []appendResult{pending}}
			second := &scriptedStream{results: []appendResult{fakeResult{}}}
			w, createdWith := newScriptedWriter(first, second)
			defer func() { _ = w.Close() }()

			ctx, stop := makeAttempt()
			done := make(chan AppendOutcome, 1)
			go func() { done <- w.Append(ctx, [][]byte{{1}}) }()
			<-pending.started
			stop()
			out := <-done

			if out.Verdict.Class != Uncertain || out.Verdict.Label != LabelUncertainAck {
				t.Fatalf("verdict = %s/%s, want uncertain/%s",
					out.Verdict.Class, out.Verdict.Label, LabelUncertainAck)
			}
			if first.closeCount() != 1 {
				t.Fatalf("retired stream was closed %d times, want 1", first.closeCount())
			}

			if next := w.Append(context.Background(), [][]byte{{2}}); next.Err != nil {
				t.Fatalf("next attempt failed: %v", next.Err)
			}
			if len(*createdWith) != 2 {
				t.Fatalf("created %d streams, want a fresh generation", len(*createdWith))
			}
		})
	}
}

// Two append attempts can still be awaiting results from the same generation.
// A late failure from that retired generation must never close the replacement
// stream another attempt has already created.
func TestLateOldGenerationFailureCannotRetireReplacement(t *testing.T) {
	late := newControlledResult()
	retireFirst := newControlledResult()
	retireFirst.complete(status.Error(codes.FailedPrecondition, "finalized"))
	old := &scriptedStream{results: []appendResult{late, retireFirst}}
	replacement := &scriptedStream{results: []appendResult{fakeResult{}}}
	w, _ := newScriptedWriter(old, replacement)

	lateDone := make(chan AppendOutcome, 1)
	go func() {
		lateDone <- w.Append(context.Background(), [][]byte{{1}})
	}()
	<-late.started

	if out := w.Append(context.Background(), [][]byte{{2}}); !out.Verdict.StreamRecreate {
		t.Fatal("first failure should retire the old generation")
	}
	if out := w.Append(context.Background(), [][]byte{{3}}); out.Err != nil {
		t.Fatalf("replacement append failed: %v", out.Err)
	}

	late.complete(status.Error(codes.FailedPrecondition, "finalized"))
	select {
	case <-lateDone:
	case <-time.After(time.Second):
		t.Fatal("late append did not finish")
	}
	if replacement.closeCount() != 0 {
		t.Fatal("late failure from the old generation closed its replacement")
	}
}

// retiredMidDispatchStream holds the first AppendRows until released and then
// fails it the way managedwriter fails a call whose stream was closed during
// the call: with a plain error from its own writer bookkeeping. Later calls
// return a result that retires the generation.
type retiredMidDispatchStream struct {
	entered chan struct{}
	release chan struct{}

	mu    sync.Mutex
	calls int
}

func (s *retiredMidDispatchStream) AppendRows(
	context.Context, [][]byte, ...managedwriter.AppendOption,
) (appendResult, error) {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.mu.Unlock()
	if call == 1 {
		close(s.entered)
		<-s.release
		return nil, errors.New(`writer "w" unknown`)
	}
	return fakeResult{err: status.Error(codes.FailedPrecondition, "finalized")}, nil
}

func (s *retiredMidDispatchStream) Close() error { return nil }

// A dispatch that fails because another attempt retired its generation during
// the call failed on this writer's own teardown, not on a service verdict. The
// request may already be on the wire, so it is uncertain; a permanent verdict
// would drop the batch.
func TestDispatchFailureOnRetiredGenerationIsUncertain(t *testing.T) {
	s := &retiredMidDispatchStream{entered: make(chan struct{}), release: make(chan struct{})}
	w, _ := newScriptedWriter(s)

	dispatching := make(chan AppendOutcome, 1)
	go func() { dispatching <- w.Append(context.Background(), [][]byte{{1}}) }()
	<-s.entered
	if out := w.Append(context.Background(), [][]byte{{2}}); !out.Verdict.StreamRecreate {
		t.Fatal("second append should retire the shared generation")
	}
	close(s.release)

	out := <-dispatching
	if out.Verdict.Class != Uncertain || out.Verdict.Label != LabelUncertainAck {
		t.Fatalf("dispatch failed by a concurrent retirement = %s/%s, want uncertain/%s",
			out.Verdict.Class, out.Verdict.Label, LabelUncertainAck)
	}
}

// orphanedResultStream reproduces a race in managedwriter's AppendRows: when
// the call's context ends, or the stream closes, while the call is under way,
// AppendRows can still report success and hand back an AppendResult for a
// request it never sent. Nothing ever resolves that result.
type orphanedResultStream struct{}

func (orphanedResultStream) AppendRows(
	ctx context.Context, _ [][]byte, _ ...managedwriter.AppendOption,
) (appendResult, error) {
	<-ctx.Done()
	return newControlledResult(), nil
}

func (orphanedResultStream) Close() error { return nil }

// Tracking such a result pins its retired generation forever. After two
// attempts hit the race, the two-generation bound would leave no room for a
// replacement and every later append would fail until restart.
func TestOrphanedAppendResultsCannotWedgeWriter(t *testing.T) {
	healthy := &scriptedStream{results: []appendResult{fakeResult{}}}
	w, createdWith := newScriptedWriter(orphanedResultStream{}, orphanedResultStream{}, healthy)
	defer func() { _ = w.Close() }()

	for i := 1; i <= 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		out := w.Append(ctx, [][]byte{{byte(i)}})
		cancel()
		if out.Verdict.Class != Uncertain || out.Verdict.Label != LabelUncertainAck {
			t.Fatalf("append %d racing its deadline = %s/%s (%v), want uncertain/%s",
				i, out.Verdict.Class, out.Verdict.Label, out.Err, LabelUncertainAck)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if out := w.Append(ctx, [][]byte{{3}}); out.Err != nil {
		t.Fatalf("append after two orphaned results failed: %v (error_code=%s)", out.Err, out.Verdict.Label)
	}
	if len(*createdWith) != 3 {
		t.Fatalf("created %d stream generations, want a third after the two retired ones", len(*createdWith))
	}
}

// closedUnderDispatchStream holds the first AppendRows until released while a
// sibling's call retires the generation, then reports success for a request
// it never sent: the stream-closure half of the managedwriter race above.
type closedUnderDispatchStream struct {
	entered chan struct{}
	release chan struct{}

	mu    sync.Mutex
	calls int
}

func (s *closedUnderDispatchStream) AppendRows(
	context.Context, [][]byte, ...managedwriter.AppendOption,
) (appendResult, error) {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.mu.Unlock()
	if call == 1 {
		close(s.entered)
		<-s.release
		return newControlledResult(), nil
	}
	return fakeResult{err: status.Error(codes.FailedPrecondition, "finalized")}, nil
}

func (s *closedUnderDispatchStream) Close() error { return nil }

// A success reported after the generation was retired under the call, with
// the attempt still live, is ambiguous too. Tracking it would pin the retired
// generation exactly as an expired attempt's orphan does.
func TestRetiredDispatchSuccessCannotWedgeWriter(t *testing.T) {
	newStream := func() *closedUnderDispatchStream {
		return &closedUnderDispatchStream{entered: make(chan struct{}), release: make(chan struct{})}
	}
	first, second := newStream(), newStream()
	w, _ := newScriptedWriter(first, second, &scriptedStream{results: []appendResult{fakeResult{}}})
	observer := &fakeObserver{}
	w.opts.Observer = observer
	defer func() { _ = w.Close() }()

	for i, s := range []*closedUnderDispatchStream{first, second} {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		done := make(chan AppendOutcome, 1)
		go func() { done <- w.Append(ctx, [][]byte{{1}}) }()
		<-s.entered
		if out := w.Append(context.Background(), [][]byte{{2}}); !out.Verdict.StreamRecreate {
			cancel()
			t.Fatalf("round %d: the sibling should retire the generation", i+1)
		}
		close(s.release)
		out := <-done
		cancel()
		if out.Verdict.Class != Uncertain || out.Verdict.Label != LabelUncertainAck {
			t.Fatalf("round %d: success after retirement = %s/%s, want uncertain/%s",
				i+1, out.Verdict.Class, out.Verdict.Label, LabelUncertainAck)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if out := w.Append(ctx, [][]byte{{3}}); out.Err != nil {
		t.Fatalf("append after two closed-under-dispatch successes failed: %v", out.Err)
	}
	if inflight, unresolved, _, _, _ := observer.snapshot(); inflight != 0 || unresolved != 0 {
		t.Fatalf("inflight=%d unresolved=%d, want 0/0", inflight, unresolved)
	}
}

// GetResult returning the canceled attempt context does not mean the
// managedwriter result itself is terminal. The writer must keep a bounded
// lifecycle owner for that result until it resolves or the writer shuts down.
func TestCanceledAttemptTracksResultToTerminalState(t *testing.T) {
	pending := newControlledResult()
	first := &scriptedStream{results: []appendResult{pending}}
	w, _ := newScriptedWriter(first)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan AppendOutcome, 1)
	go func() { done <- w.Append(ctx, [][]byte{{1}}) }()
	<-pending.started
	cancel()
	out := <-done
	if out.Verdict.Class != Uncertain {
		t.Fatalf("class = %s, want uncertain", out.Verdict.Class)
	}

	// One call belongs to the canceled attempt; the second is the retained
	// lifecycle owner. Without it the pending result is abandoned.
	pending.waitForCalls(t, 2)
	pending.complete(nil)
	if err := w.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
}

func TestCanceledAttemptTelemetryReturnsToZero(t *testing.T) {
	pending := newControlledResult()
	first := &scriptedStream{results: []appendResult{pending}}
	observer := &fakeObserver{}
	w, _ := newScriptedWriter(first)
	w.opts.Observer = observer

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan AppendOutcome, 1)
	go func() { done <- w.Append(ctx, [][]byte{{1}}) }()
	<-pending.started
	cancel()
	out := <-done
	if out.Verdict.Class != Uncertain {
		t.Fatalf("class = %s, want uncertain", out.Verdict.Class)
	}
	pending.waitForCalls(t, 2)

	inflight, unresolved, _, timeouts, retirements := observer.snapshot()
	if inflight != 1 || unresolved != 1 || timeouts != 1 || retirements != 1 {
		t.Fatalf("during drain: inflight=%d unresolved=%d timeouts=%d retirements=%d, want 1 each",
			inflight, unresolved, timeouts, retirements)
	}

	pending.complete(nil)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	inflight, unresolved, waits, _, _ := observer.snapshot()
	if inflight != 0 || unresolved != 0 || waits != 1 {
		t.Fatalf("after drain: inflight=%d unresolved=%d waits=%d, want 0/0/1",
			inflight, unresolved, waits)
	}
}

// This matches the historical shipped relationship: ten queue consumers and
// eight managedwriter flow-control slots. All ten attempts must return after a
// no-ACK timeout, and a later append must recover on a fresh generation.
func TestTenConcurrentAttemptsRecoverFromEightSlotNoACK(t *testing.T) {
	stuck := newBoundedNoACKStream(8)
	replacement := &scriptedStream{results: []appendResult{fakeResult{}}}
	w, _ := newScriptedWriter(stuck, replacement)

	const attempts = 10
	// The attempts end together, as a shared deadline would. Ending them one
	// at a time would let an attempt scheduled late start after the stuck
	// generation retired, with a live context, and consume the replacement's
	// only scripted result.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan AppendOutcome, attempts)
	for range attempts {
		go func() { done <- w.Append(ctx, [][]byte{{1}}) }()
	}
	stuck.waitForCalls(t, 8)
	cancel()
	for range attempts {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("concurrent append did not return after cancellation")
		}
	}

	if out := w.Append(context.Background(), [][]byte{{2}}); out.Err != nil {
		t.Fatalf("append after no-ACK generation failed: %v", out.Err)
	}
	if stuck.closeCount() != 1 {
		t.Fatalf("stuck generation closed %d times, want once", stuck.closeCount())
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

type boundedNoACKStream struct {
	mu      sync.Mutex
	calls   int
	closed  int
	slots   chan struct{}
	results []*controlledResult
}

func newBoundedNoACKStream(slots int) *boundedNoACKStream {
	return &boundedNoACKStream{slots: make(chan struct{}, slots)}
}

func (s *boundedNoACKStream) AppendRows(
	ctx context.Context, _ [][]byte, _ ...managedwriter.AppendOption,
) (appendResult, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	select {
	case s.slots <- struct{}{}:
		result := newControlledResult()
		s.mu.Lock()
		s.results = append(s.results, result)
		s.mu.Unlock()
		return result, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *boundedNoACKStream) Close() error {
	s.mu.Lock()
	s.closed++
	results := append([]*controlledResult(nil), s.results...)
	s.mu.Unlock()
	for _, result := range results {
		result.complete(context.Canceled)
	}
	return nil
}

func (s *boundedNoACKStream) closeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *boundedNoACKStream) waitForCalls(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		calls := s.calls
		s.mu.Unlock()
		if calls == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("AppendRows calls did not reach %d", want)
}

func TestCanceledManagedStreamIsDistinguishedFromShutdown(t *testing.T) {
	lifetimeCtx, cancelLifetime := context.WithCancel(context.Background())
	w := &Writer{lifetimeCtx: lifetimeCtx}

	req := requestSlice{rows: [][]byte{{1}}}
	v, ended := w.classifyAppendFailure(context.Background(), context.Canceled, req)
	if ended || v.Label != LabelCanceled {
		t.Fatalf("runtime cancellation = %s ended=%v, want %s/false", v.Label, ended, LabelCanceled)
	}

	cancelLifetime()
	v, ended = w.classifyAppendFailure(context.Background(), context.Canceled, req)
	if ended || v.Label != LabelShutdown {
		t.Fatalf("shutdown cancellation = %s ended=%v, want %s/false", v.Label, ended, LabelShutdown)
	}
}

// A backend may never resolve an abandoned result. Shutdown must cancel and
// join the lifecycle waiter rather than leak it indefinitely.
func TestCloseJoinsUnresolvedResultOwner(t *testing.T) {
	pending := newControlledResult()
	first := &scriptedStream{results: []appendResult{pending}}
	w, _ := newScriptedWriter(first)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan AppendOutcome, 1)
	go func() { done <- w.Append(ctx, [][]byte{{1}}) }()
	<-pending.started
	cancel()
	<-done
	pending.waitForCalls(t, 2)

	closed := make(chan error, 1)
	go func() { closed <- w.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel and join the unresolved result owner")
	}
}

// Acknowledged rows must only be counted after the service confirms them.
func TestAppendCountsNothingOnFailure(t *testing.T) {
	s := &fakeStream{results: []appendResult{fakeResult{err: status.Error(codes.Unavailable, "down")}}}
	out := newTestWriter(s).Append(context.Background(), [][]byte{{1}, {2}})

	if out.AcknowledgedRows != 0 {
		t.Fatalf("acknowledged %d rows on a failed append, want 0", out.AcknowledgedRows)
	}
	if out.Verdict.Class != Retryable {
		t.Fatalf("class = %s, want retryable", out.Verdict.Class)
	}
}

// Row errors are reported per row, with indices translated into the caller's
// batch coordinates rather than the request's.
func TestAppendReportsRowErrors(t *testing.T) {
	resp := &storagepb.AppendRowsResponse{
		RowErrors: []*storagepb.RowError{
			{Index: 1, Code: storagepb.RowError_FIELDS_ERROR, Message: "bad field body"},
		},
	}
	s := &fakeStream{results: []appendResult{fakeResult{err: errors.New("row errors"), resp: resp}}}

	out := newTestWriter(s).Append(context.Background(), [][]byte{{1}, {2}, {3}})
	if len(out.RowErrors) != 1 {
		t.Fatalf("got %d row errors, want 1", len(out.RowErrors))
	}
	if out.RowErrors[0].Index != 1 {
		t.Fatalf("row index = %d, want 1", out.RowErrors[0].Index)
	}
}

// An invalid stream must be torn down so the next append rebuilds it, rather
// than retrying forever against a stream the service has finalized.
func TestAppendRecreatesStreamOnInvalidStream(t *testing.T) {
	s := &fakeStream{results: []appendResult{fakeResult{err: status.Error(codes.FailedPrecondition, "finalized")}}}
	w := newTestWriter(s)

	out := w.Append(context.Background(), [][]byte{{1}})
	if !out.Verdict.StreamRecreate {
		t.Fatal("expected the verdict to call for a stream recreate")
	}
	if s.closed == 0 {
		t.Fatal("the stream should have been closed so the next append rebuilds it")
	}
	if w.hasCurrentStream() {
		t.Fatal("writer should have dropped its stream reference")
	}
}

func TestAppendEmptyIsNoop(t *testing.T) {
	s := &fakeStream{}
	out := newTestWriter(s).Append(context.Background(), nil)
	if out.Err != nil || out.AcknowledgedRows != 0 || s.calls != 0 {
		t.Fatalf("empty append should do nothing, got %+v after %d calls", out, s.calls)
	}
}

func TestWriterOptionsRefs(t *testing.T) {
	o := WriterOptions{Project: "p", Dataset: "d", Table: "t"}
	if got, want := o.TableRef(), "projects/p/datasets/d/tables/t"; got != want {
		t.Errorf("TableRef() = %q, want %q", got, want)
	}
	if got, want := o.TableID(), "p.d.t"; got != want {
		t.Errorf("TableID() = %q, want %q", got, want)
	}
}

// Retry ownership is a contract, not a default. If write retries were ever
// enabled here, managedwriter and exporterhelper would both replay and the
// at-most-N+1 duplicate bound would stop holding.
func TestWriteRetriesAreExplicitlyDisabled(t *testing.T) {
	w := newTestWriter(&fakeStream{})
	w.opts.Descriptor = nil
	managedStream := &managedwriter.ManagedStream{}
	opts := w.writerOptions()
	if len(opts) == 0 {
		t.Fatal("expected writer options")
	}
	// The retry-ownership option is deliberately last in writerOptions. Apply
	// it in isolation because the other options require a fully constructed
	// ManagedStream and are unrelated to this contract.
	opts[len(opts)-1](managedStream)

	// WriterOption intentionally exposes behavior rather than state. The
	// dependency is pinned, so inspecting its configured retry strategy here
	// gives us a direct regression assertion instead of counting opaque
	// options: EnableWriteRetries(true) makes this field non-nil.
	retry := reflect.ValueOf(managedStream).Elem().FieldByName("retry")
	if !retry.IsValid() {
		t.Fatal("managedwriter retry field changed; re-verify the pinned dependency's retry contract")
	}
	if !retry.IsNil() {
		t.Fatal("managedwriter write retries are enabled; exporterhelper must be the sole retry owner")
	}
}

func TestAppendTracksAckedRangesAcrossSplitRequests(t *testing.T) {
	first := newControlledResult()
	second := newControlledResult()
	first.complete(nil)
	second.complete(status.Error(codes.Unavailable, "second sub-request failed"))

	s := &scriptedStream{results: []appendResult{first, second}}
	w, _ := newScriptedWriter(s)
	w.opts.MaxRequestBytes = 200
	w.opts.MaxInflightRequests = 2

	row := make([]byte, 60)
	out := w.Append(context.Background(), [][]byte{row, row, row, row})
	if out.AcknowledgedRows != 2 {
		t.Fatalf("acknowledged %d rows, want 2 from first sub-request", out.AcknowledgedRows)
	}
	if len(out.AckedRanges) != 1 || out.AckedRanges[0] != [2]int{0, 2} {
		t.Fatalf("AckedRanges = %v, want [[0 2]]", out.AckedRanges)
	}
	for i, want := range []bool{true, true, false, false} {
		if got := out.IsAcked(i); got != want {
			t.Errorf("IsAcked(%d) = %v, want %v", i, got, want)
		}
	}
}

func TestAppendStreamRecreateOutranksPermanentAndStopsDispatch(t *testing.T) {
	t.Run("StreamRecreate outranks plain Permanent across concurrent sub-requests", func(t *testing.T) {
		first := newControlledResult()
		second := newControlledResult()
		first.complete(status.Error(codes.InvalidArgument, "malformed request"))
		second.complete(status.Error(codes.FailedPrecondition, "stream is finalized"))

		s := &scriptedStream{results: []appendResult{first, second}}
		w, _ := newScriptedWriter(s)
		w.opts.MaxRequestBytes = 100
		w.opts.MaxInflightRequests = 2

		row := make([]byte, 60)
		out := w.Append(context.Background(), [][]byte{row, row})
		if !out.Verdict.StreamRecreate || out.Verdict.Label != LabelInvalidStream {
			t.Fatalf("out.Verdict = %+v, want StreamRecreate=true with label=%s", out.Verdict, LabelInvalidStream)
		}
	})

	t.Run("StreamRecreate stops subsequent sub-request dispatch and preserves verdict label", func(t *testing.T) {
		first := newControlledResult()
		second := newControlledResult()
		first.complete(status.Error(codes.InvalidArgument, "Schema mismatch for field foo"))
		second.complete(nil)

		s := &scriptedStream{results: []appendResult{first, second}}
		w, _ := newScriptedWriter(s)
		w.opts.MaxRequestBytes = 100
		w.opts.MaxInflightRequests = 1

		row := make([]byte, 60)
		out := w.Append(context.Background(), [][]byte{row, row})
		if s.calls != 1 {
			t.Fatalf("AppendRows calls = %d, want 1 (replayOwed stops second sub-request dispatch)", s.calls)
		}
		if !out.Verdict.StreamRecreate || out.Verdict.Label != LabelSchemaMismatch {
			t.Fatalf("out.Verdict = %+v, want StreamRecreate=true and label=%s preserved without errStreamRetired overwrite",
				out.Verdict, LabelSchemaMismatch)
		}
	})
}

func TestWriterAdaptiveBackpressureThrottleAndRecovery(t *testing.T) {
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	bp := NewBackpressureController(BackpressureOptions{
		MinRequestBytes:     200,
		MaxRequestBytes:     800,
		MinInflightRequests: 1,
		MaxInflightRequests: 4,
		DecreaseFactor:      0.5,
		RecoveryInterval:    10 * time.Second,
		LatencyThreshold:    2 * time.Second,
		Now:                 func() time.Time { return now },
	})
	obs := &fakeObserver{}

	quotaErr := status.Error(codes.ResourceExhausted, throughputQuotaPrefix+" for project x")
	s := &fakeStream{
		results: []appendResult{
			fakeResult{err: quotaErr},
			fakeResult{},
			fakeResult{},
		},
	}
	w := &Writer{
		opts: WriterOptions{
			Project:             "p",
			Dataset:             "d",
			Table:               "otel_spans",
			Signal:              "spans",
			MaxRequestBytes:     800,
			MaxInflightRequests: 4,
			Observer:            obs,
			Backpressure:        bp,
		},
		factory: func(context.Context, ...managedwriter.WriterOption) (stream, error) {
			return s, nil
		},
	}
	defer func() { _ = w.Close() }()

	// 4 rows of 120 bytes each -> fits in one 800-byte request initially.
	row := make([]byte, 120)
	first := w.Append(context.Background(), [][]byte{row, row, row, row})
	if first.Err == nil || first.Verdict.Class != Retryable || first.Verdict.Label != LabelQuota {
		t.Fatalf("first Append outcome = %+v, want retryable quota_exhausted", first)
	}
	if s.calls != 1 {
		t.Fatalf("AppendRows calls on 1st attempt = %d, want 1 (no internal retry)", s.calls)
	}
	if got := bp.EffectiveRequestBytes(); got != 400 {
		t.Fatalf("EffectiveRequestBytes() after throttle = %d, want 400", got)
	}
	if got := bp.EffectiveInflightRequests(); got != 2 {
		t.Fatalf("EffectiveInflightRequests() after throttle = %d, want 2", got)
	}

	// Second attempt (simulating exporterhelper retry) splits the same 4 rows
	// under the 400-byte effective limit into 2 sub-requests of 2 rows each.
	now = now.Add(11 * time.Second)
	second := w.Append(context.Background(), [][]byte{row, row, row, row})
	if second.Err != nil || second.AcknowledgedRows != 4 {
		t.Fatalf("second Append outcome = %+v, want 4 acknowledged rows", second)
	}
	if s.calls != 3 {
		t.Fatalf("total AppendRows calls = %d, want 3 (1 failed + 2 split sub-requests)", s.calls)
	}
	// Clean ack after RecoveryInterval stepped effective bytes up from 400 to 460 (+10% of 600 span).
	if got := bp.EffectiveRequestBytes(); got != 460 {
		t.Fatalf("EffectiveRequestBytes() after recovery = %d, want 460", got)
	}

	obs.mu.Lock()
	events := append([]string(nil), obs.backpressureEvents...)
	obs.mu.Unlock()
	if !reflect.DeepEqual(events, []string{"throttle", "recovery"}) {
		t.Fatalf("backpressure events = %v, want [throttle recovery]", events)
	}
}

func TestWriterAdaptiveBackpressureOversizeMultiRowRetryable(t *testing.T) {
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	bp := NewBackpressureController(BackpressureOptions{
		MinRequestBytes:     200,
		MaxRequestBytes:     800,
		MinInflightRequests: 1,
		MaxInflightRequests: 4,
		DecreaseFactor:      0.5,
		RecoveryInterval:    10 * time.Second,
		Now:                 func() time.Time { return now },
	})
	oversizeErr := status.Error(codes.ResourceExhausted, "Request payload size exceeds the limit: 10485760 bytes.")
	s := &fakeStream{
		results: []appendResult{
			fakeResult{err: oversizeErr},
			fakeResult{err: oversizeErr},
			fakeResult{err: oversizeErr},
			fakeResult{err: oversizeErr},
		},
	}
	w := &Writer{
		opts: WriterOptions{
			Project:             "p",
			Dataset:             "d",
			Table:               "otel_spans",
			Signal:              "spans",
			MaxRequestBytes:     800,
			MaxInflightRequests: 4,
			Backpressure:        bp,
		},
		factory: func(context.Context, ...managedwriter.WriterOption) (stream, error) {
			return s, nil
		},
	}
	defer func() { _ = w.Close() }()

	row := make([]byte, 50)
	multiOut := w.Append(context.Background(), [][]byte{row, row})
	if multiOut.Verdict.Class != Retryable || multiOut.Verdict.Label != LabelOversizeRequest {
		t.Fatalf("multi-row oversize verdict above floor = %+v, want Retryable/oversize_request", multiOut.Verdict)
	}
	if got := bp.EffectiveRequestBytes(); got != 400 {
		t.Fatalf("EffectiveRequestBytes() after 1st multi-row oversize = %d, want 400", got)
	}

	// Step down to the floor (200 bytes).
	now = now.Add(300 * time.Millisecond)
	secondOut := w.Append(context.Background(), [][]byte{row, row})
	if secondOut.Verdict.Class != Retryable || secondOut.Verdict.Label != LabelOversizeRequest {
		t.Fatalf("2nd multi-row oversize verdict above floor = %+v, want Retryable/oversize_request", secondOut.Verdict)
	}
	if got := bp.EffectiveRequestBytes(); got != 200 {
		t.Fatalf("EffectiveRequestBytes() after 2nd multi-row oversize = %d, want 200 (floor)", got)
	}

	// Once at MinRequestBytes (200), a multi-row oversize error cannot split any
	// smaller and must stay permanent so retries do not spin without progress.
	now = now.Add(300 * time.Millisecond)
	atFloorOut := w.Append(context.Background(), [][]byte{row, row})
	if atFloorOut.Verdict.Class != Permanent || atFloorOut.Verdict.Label != LabelQuota {
		t.Fatalf("multi-row oversize verdict at floor = %+v, want Permanent/quota_exhausted", atFloorOut.Verdict)
	}

	singleOut := w.Append(context.Background(), [][]byte{row})
	if singleOut.Verdict.Class != Permanent || singleOut.Verdict.Label != LabelQuota {
		t.Fatalf("single-row oversize verdict = %+v, want Permanent/quota_exhausted", singleOut.Verdict)
	}
}

type virtualClockStream struct {
	mu          sync.Mutex
	now         time.Time
	ackLatency  time.Duration
	pendingAcks []*virtualClockAck
}

type virtualClockAck struct {
	readyAt time.Time
	ready   chan struct{}
	closed  bool
}

func (a *virtualClockAck) Ready() <-chan struct{} { return a.ready }

func (a *virtualClockAck) GetResult(context.Context) (int64, error) {
	return 0, nil
}

func (a *virtualClockAck) FullResponse(context.Context) (*storagepb.AppendRowsResponse, error) {
	return nil, nil
}

func newVirtualClockStream(start time.Time, ackLatency time.Duration) *virtualClockStream {
	return &virtualClockStream{now: start, ackLatency: ackLatency}
}

func (s *virtualClockStream) currentTime() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.now
}

func (s *virtualClockStream) advanceLocked(d time.Duration) {
	s.now = s.now.Add(d)
	for _, a := range s.pendingAcks {
		if !a.closed && !s.now.Before(a.readyAt) {
			a.closed = true
			close(a.ready)
		}
	}
}

func (s *virtualClockStream) AppendRows(
	_ context.Context, _ [][]byte, _ ...managedwriter.AppendOption,
) (appendResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ack := &virtualClockAck{
		readyAt: s.now.Add(s.ackLatency),
		ready:   make(chan struct{}),
	}
	s.pendingAcks = append(s.pendingAcks, ack)
	// Simulate 300ms of time passing per dispatch (or ackLatency when larger)
	// so earlier in-flight requests become ready while later ones are still
	// being dispatched.
	s.advanceLocked(s.ackLatency)
	return ack, nil
}

func (s *virtualClockStream) Close() error { return nil }

func TestWriterAckAgeExcludesSubsequentDispatchAndDrainDelay(t *testing.T) {
	t.Run("eight 300ms acks under 2.4s total dispatch stay healthy", func(t *testing.T) {
		start := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
		s := newVirtualClockStream(start, 300*time.Millisecond)
		bp := NewBackpressureController(BackpressureOptions{
			MinRequestBytes:     50,
			MaxRequestBytes:     200,
			MinInflightRequests: 1,
			MaxInflightRequests: 8,
			DecreaseFactor:      0.5,
			RecoveryInterval:    10 * time.Second,
			LatencyThreshold:    2 * time.Second,
			Now:                 s.currentTime,
		})
		obs := &fakeObserver{}
		w := &Writer{
			opts: WriterOptions{
				Project:             "p",
				Dataset:             "d",
				Table:               "otel_spans",
				Signal:              "spans",
				MaxRequestBytes:     200,
				MaxInflightRequests: 8,
				Observer:            obs,
				Backpressure:        bp,
			},
			factory: func(context.Context, ...managedwriter.WriterOption) (stream, error) {
				return s, nil
			},
		}
		defer func() { _ = w.Close() }()

		row := make([]byte, 120)
		rows := make([][]byte, 8)
		for i := range rows {
			rows[i] = row
		}
		out := w.Append(context.Background(), rows)
		if out.Err != nil || out.AcknowledgedRows != 8 {
			t.Fatalf("Append outcome = %+v, want 8 acknowledged rows without error", out)
		}
		if got := bp.EffectiveRequestBytes(); got != 200 {
			t.Fatalf("EffectiveRequestBytes() = %d, want 200 (healthy 300ms acks must not trigger latency step-down)", got)
		}
		if got := bp.EffectiveInflightRequests(); got != 8 {
			t.Fatalf("EffectiveInflightRequests() = %d, want 8", got)
		}
		obs.mu.Lock()
		events := append([]string(nil), obs.backpressureEvents...)
		obs.mu.Unlock()
		if len(events) != 0 {
			t.Fatalf("backpressure events = %v, want none", events)
		}
	})

	t.Run("genuinely slow 2.5s ack triggers latency step-down", func(t *testing.T) {
		start := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
		s := newVirtualClockStream(start, 2500*time.Millisecond)
		bp := NewBackpressureController(BackpressureOptions{
			MinRequestBytes:     50,
			MaxRequestBytes:     200,
			MinInflightRequests: 1,
			MaxInflightRequests: 8,
			DecreaseFactor:      0.5,
			RecoveryInterval:    10 * time.Second,
			LatencyThreshold:    2 * time.Second,
			Now:                 s.currentTime,
		})
		obs := &fakeObserver{}
		w := &Writer{
			opts: WriterOptions{
				Project:             "p",
				Dataset:             "d",
				Table:               "otel_spans",
				Signal:              "spans",
				MaxRequestBytes:     200,
				MaxInflightRequests: 8,
				Observer:            obs,
				Backpressure:        bp,
			},
			factory: func(context.Context, ...managedwriter.WriterOption) (stream, error) {
				return s, nil
			},
		}
		defer func() { _ = w.Close() }()

		out := w.Append(context.Background(), [][]byte{make([]byte, 120)})
		if out.Err != nil || out.AcknowledgedRows != 1 {
			t.Fatalf("Append outcome = %+v, want 1 acknowledged row", out)
		}
		if got := bp.EffectiveRequestBytes(); got != 100 {
			t.Fatalf("EffectiveRequestBytes() after 2.5s ack = %d, want 100", got)
		}
		if got := bp.EffectiveInflightRequests(); got != 4 {
			t.Fatalf("EffectiveInflightRequests() after 2.5s ack = %d, want 4", got)
		}
		obs.mu.Lock()
		events := append([]string(nil), obs.backpressureEvents...)
		obs.mu.Unlock()
		if !reflect.DeepEqual(events, []string{"latency"}) {
			t.Fatalf("backpressure events = %v, want [latency]", events)
		}
	})
}

type countingLatencyStream struct {
	ackLatency      time.Duration
	inflight        atomic.Int64
	peakOutstanding atomic.Int64
	closed          atomic.Int64
}

func (s *countingLatencyStream) AppendRows(
	_ context.Context, _ [][]byte, _ ...managedwriter.AppendOption,
) (appendResult, error) {
	cur := s.inflight.Add(1)
	for {
		peak := s.peakOutstanding.Load()
		if cur <= peak || s.peakOutstanding.CompareAndSwap(peak, cur) {
			break
		}
	}
	res := newControlledResult()
	time.AfterFunc(s.ackLatency, func() {
		s.inflight.Add(-1)
		res.complete(nil)
	})
	return res, nil
}

func (s *countingLatencyStream) Close() error {
	s.closed.Add(1)
	return nil
}

func TestWriterEffectiveInflightEnforcedAtFloor(t *testing.T) {
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	bp := NewBackpressureController(BackpressureOptions{
		MinRequestBytes:     100,
		MaxRequestBytes:     400,
		MinInflightRequests: 1,
		MaxInflightRequests: 4,
		DecreaseFactor:      0.5,
		RecoveryInterval:    10 * time.Second,
		Now:                 func() time.Time { return now },
	})
	// Step the controller down to its floor (1 in-flight).
	bp.OnThrottle()
	now = now.Add(300 * time.Millisecond)
	bp.OnThrottle()
	if got := bp.EffectiveInflightRequests(); got != 1 {
		t.Fatalf("EffectiveInflightRequests() = %d, want 1 at floor", got)
	}

	s := &countingLatencyStream{ackLatency: 2 * time.Millisecond}
	w := &Writer{
		opts: WriterOptions{
			Project:             "p",
			Dataset:             "d",
			Table:               "otel_spans",
			Signal:              "spans",
			MaxRequestBytes:     400,
			MaxInflightRequests: 4,
			Backpressure:        bp,
		},
		factory: func(context.Context, ...managedwriter.WriterOption) (stream, error) {
			return s, nil
		},
	}
	defer func() { _ = w.Close() }()

	row := make([]byte, 80)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out := w.Append(context.Background(), [][]byte{row, row, row, row})
			if out.Err != nil || out.AcknowledgedRows != 4 {
				t.Errorf("Append outcome = %+v, want 4 acknowledged rows", out)
			}
		}()
	}
	wg.Wait()

	if got := s.peakOutstanding.Load(); got != 1 {
		t.Fatalf("peak outstanding AppendRows = %d, want 1 at floor", got)
	}
}

type windowHoldStream struct {
	mu      sync.Mutex
	calls   int
	twoSeen chan struct{}
	results []*controlledResult
}

func newWindowHoldStream() *windowHoldStream {
	return &windowHoldStream{twoSeen: make(chan struct{})}
}

func (s *windowHoldStream) AppendRows(
	context.Context, [][]byte, ...managedwriter.AppendOption,
) (appendResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	res := newControlledResult()
	s.results = append(s.results, res)
	if s.calls == 2 {
		close(s.twoSeen)
	}
	return res, nil
}

func (s *windowHoldStream) Close() error {
	s.mu.Lock()
	results := append([]*controlledResult(nil), s.results...)
	s.mu.Unlock()
	for _, r := range results {
		r.complete(context.Canceled)
	}
	return nil
}

func TestWriterAdaptiveWindowCloseWakesWaiters(t *testing.T) {
	bp := NewBackpressureController(BackpressureOptions{
		MinRequestBytes:     100,
		MaxRequestBytes:     400,
		MinInflightRequests: 1,
		MaxInflightRequests: 4,
		DecreaseFactor:      0.5,
		RecoveryInterval:    10 * time.Second,
	})
	// Reduce window from 4 to 2.
	bp.OnThrottle()
	if got := bp.EffectiveInflightRequests(); got != 2 {
		t.Fatalf("EffectiveInflightRequests() = %d, want 2", got)
	}

	s := newWindowHoldStream()
	w := &Writer{
		opts: WriterOptions{
			Project:             "p",
			Dataset:             "d",
			Table:               "otel_spans",
			Signal:              "spans",
			MaxRequestBytes:     400,
			MaxInflightRequests: 4,
			Backpressure:        bp,
		},
		factory: func(context.Context, ...managedwriter.WriterOption) (stream, error) {
			return s, nil
		},
	}

	const callers = 12
	outcomes := make(chan AppendOutcome, callers)
	for range callers {
		go func() {
			outcomes <- w.Append(context.Background(), [][]byte{{1}})
		}()
	}

	select {
	case <-s.twoSeen:
	case <-time.After(time.Second):
		t.Fatal("expected 2 calls to enter AppendRows under reduced window")
	}
	time.Sleep(15 * time.Millisecond)

	s.mu.Lock()
	dispatchedBeforeClose := s.calls
	s.mu.Unlock()
	if dispatchedBeforeClose != 2 {
		t.Fatalf("AppendRows calls before Close = %d, want 2 (effective window)", dispatchedBeforeClose)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close() failed: %v", err)
	}

	for range callers {
		out := <-outcomes
		if out.AcknowledgedRows != 0 {
			t.Fatalf("AcknowledgedRows = %d, want 0", out.AcknowledgedRows)
		}
		if out.Verdict.Class != Retryable {
			t.Fatalf("Verdict = %+v, want Retryable", out.Verdict)
		}
	}
}

type attemptExpiryVirtualResult struct {
	mu            sync.Mutex
	calls         int
	elapsed       time.Duration
	advanceClock  func(time.Duration)
	cancelAttempt context.CancelFunc
}

func (r *attemptExpiryVirtualResult) Ready() <-chan struct{} { return nil }

func (r *attemptExpiryVirtualResult) GetResult(ctx context.Context) (int64, error) {
	r.mu.Lock()
	r.calls++
	call := r.calls
	r.mu.Unlock()
	if call == 1 {
		r.advanceClock(r.elapsed)
		r.cancelAttempt()
		return 0, ctx.Err()
	}
	return 0, nil
}

func (r *attemptExpiryVirtualResult) FullResponse(context.Context) (*storagepb.AppendRowsResponse, error) {
	return nil, nil
}

type attemptExpiryVirtualStream struct {
	mu            sync.Mutex
	now           time.Time
	elapsed       time.Duration
	cancelAttempt context.CancelFunc
}

func (s *attemptExpiryVirtualStream) currentTime() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.now
}

func (s *attemptExpiryVirtualStream) advance(d time.Duration) {
	s.mu.Lock()
	s.now = s.now.Add(d)
	s.mu.Unlock()
}

func (s *attemptExpiryVirtualStream) AppendRows(
	_ context.Context, _ [][]byte, _ ...managedwriter.AppendOption,
) (appendResult, error) {
	return &attemptExpiryVirtualResult{
		elapsed:       s.elapsed,
		advanceClock:  s.advance,
		cancelAttempt: s.cancelAttempt,
	}, nil
}

func (s *attemptExpiryVirtualStream) Close() error { return nil }

func TestWriterAttemptEndedFeedbackUsesSlowAckNotThrottle(t *testing.T) {
	for _, tc := range []struct {
		name         string
		elapsed      time.Duration
		wantBytes    int
		wantInflight int
		wantEvents   []string
	}{
		{
			name:         "fast attempt expiry below latency threshold does not throttle",
			elapsed:      100 * time.Millisecond,
			wantBytes:    200,
			wantInflight: 8,
			wantEvents:   nil,
		},
		{
			name:         "slow attempt expiry above latency threshold steps down via latency",
			elapsed:      2500 * time.Millisecond,
			wantBytes:    100,
			wantInflight: 4,
			wantEvents:   []string{"latency"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
			attemptCtx, cancelAttempt := context.WithCancel(context.Background())
			defer cancelAttempt()

			s := &attemptExpiryVirtualStream{
				now:           start,
				elapsed:       tc.elapsed,
				cancelAttempt: cancelAttempt,
			}
			bp := NewBackpressureController(BackpressureOptions{
				MinRequestBytes:     50,
				MaxRequestBytes:     200,
				MinInflightRequests: 1,
				MaxInflightRequests: 8,
				DecreaseFactor:      0.5,
				RecoveryInterval:    10 * time.Second,
				LatencyThreshold:    2 * time.Second,
				Now:                 s.currentTime,
			})
			obs := &fakeObserver{}
			w := &Writer{
				opts: WriterOptions{
					Project:             "p",
					Dataset:             "d",
					Table:               "otel_spans",
					Signal:              "spans",
					MaxRequestBytes:     200,
					MaxInflightRequests: 8,
					Observer:            obs,
					Backpressure:        bp,
				},
				factory: func(context.Context, ...managedwriter.WriterOption) (stream, error) {
					return s, nil
				},
			}
			defer func() { _ = w.Close() }()

			out := w.Append(attemptCtx, [][]byte{make([]byte, 120)})
			if out.Verdict.Class != Uncertain || out.Verdict.Label != LabelUncertainAck {
				t.Fatalf("Verdict = %+v, want Uncertain/%s", out.Verdict, LabelUncertainAck)
			}
			if got := bp.EffectiveRequestBytes(); got != tc.wantBytes {
				t.Fatalf("EffectiveRequestBytes() = %d, want %d", got, tc.wantBytes)
			}
			if got := bp.EffectiveInflightRequests(); got != tc.wantInflight {
				t.Fatalf("EffectiveInflightRequests() = %d, want %d", got, tc.wantInflight)
			}
			obs.mu.Lock()
			events := append([]string(nil), obs.backpressureEvents...)
			obs.mu.Unlock()
			if !reflect.DeepEqual(events, tc.wantEvents) {
				t.Fatalf("backpressure events = %v, want %v", events, tc.wantEvents)
			}
		})
	}
}

func TestWriterFloorRecoveryUnderConcurrentLoad(t *testing.T) {
	stepToFloor := func(bp *BackpressureController, advance func(time.Duration)) {
		t.Helper()
		for range 4 {
			bp.OnThrottle()
			advance(300 * time.Millisecond)
		}
		if got := bp.EffectiveInflightRequests(); got != 1 {
			t.Fatalf("EffectiveInflightRequests() = %d, want 1 at floor", got)
		}
	}

	t.Run("virtual-clock stale slow ack does not abandon multi-sub-request batch", func(t *testing.T) {
		start := time.Now().Add(time.Hour)
		s := newVirtualClockStream(start, 20*time.Millisecond)
		bp := NewBackpressureController(BackpressureOptions{
			MinRequestBytes:     200,
			MaxRequestBytes:     1600,
			MinInflightRequests: 1,
			MaxInflightRequests: 8,
			DecreaseFactor:      0.5,
			RecoveryInterval:    time.Second,
			LatencyThreshold:    500 * time.Millisecond,
			Now:                 s.currentTime,
		})
		stepToFloor(bp, s.advanceLocked)
		bp.OnSlowAck(800 * time.Millisecond)

		obs := &fakeObserver{}
		w := &Writer{
			opts: WriterOptions{
				Project:             "p",
				Dataset:             "d",
				Table:               "otel_spans",
				Signal:              "spans",
				MaxRequestBytes:     1600,
				MaxInflightRequests: 8,
				Observer:            obs,
				Backpressure:        bp,
			},
			factory: func(context.Context, ...managedwriter.WriterOption) (stream, error) {
				return s, nil
			},
		}
		defer func() { _ = w.Close() }()

		row := make([]byte, 120)
		rows := [][]byte{row, row, row}
		attemptStart := s.currentTime()
		deadline := attemptStart.Add(time.Second)
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		defer cancel()

		out := w.Append(ctx, rows)
		if out.Err != nil {
			t.Fatalf("Append outcome error = %v (verdict %+v), want nil", out.Err, out.Verdict)
		}
		if out.AcknowledgedRows != 3 {
			t.Fatalf("AcknowledgedRows = %d, want 3", out.AcknowledgedRows)
		}
		if elapsed := s.currentTime().Sub(attemptStart); elapsed != 60*time.Millisecond {
			t.Fatalf("virtual elapsed = %v, want 60ms", elapsed)
		}
		if remaining := deadline.Sub(s.currentTime()); remaining != 940*time.Millisecond {
			t.Fatalf("remaining deadline = %v, want 940ms", remaining)
		}
		if _, _, _, _, retirements := obs.snapshot(); retirements != 0 {
			t.Fatalf("stream retirements = %d, want 0", retirements)
		}
	})

	t.Run("default-threshold 3.4s slow ack delivers multi-request batch on 5s deadline", func(t *testing.T) {
		bp := NewBackpressureController(BackpressureOptions{
			MinRequestBytes:     256,
			MaxRequestBytes:     1024,
			MinInflightRequests: 1,
			MaxInflightRequests: 8,
			DecreaseFactor:      0.5,
			RecoveryInterval:    10 * time.Second,
			LatencyThreshold:    2 * time.Second,
		})
		bp.OnSlowAck(3400 * time.Millisecond)
		if got := bp.EffectiveRequestBytes(); got != 512 {
			t.Fatalf("EffectiveRequestBytes() after 3.4s ack = %d, want 512", got)
		}
		if got := bp.EffectiveInflightRequests(); got != 4 {
			t.Fatalf("EffectiveInflightRequests() after 3.4s ack = %d, want 4", got)
		}

		var dispatches atomic.Int32
		s := &funcStream{
			appendFn: func(_ context.Context, _ [][]byte) (appendResult, error) {
				dispatches.Add(1)
				return fakeResult{}, nil
			},
		}
		w := &Writer{
			opts: WriterOptions{
				Project:             "p",
				Dataset:             "d",
				Table:               "otel_spans",
				Signal:              "spans",
				MaxRequestBytes:     1024,
				MaxInflightRequests: 8,
				Backpressure:        bp,
			},
			factory: func(context.Context, ...managedwriter.WriterOption) (stream, error) {
				return s, nil
			},
		}
		defer func() { _ = w.Close() }()

		row := make([]byte, 200)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		out := w.Append(ctx, [][]byte{row, row, row, row})
		if out.Err != nil {
			t.Fatalf("Append outcome error = %v (verdict %+v), want nil", out.Err, out.Verdict)
		}
		if out.AcknowledgedRows != 4 {
			t.Fatalf("AcknowledgedRows = %d, want 4", out.AcknowledgedRows)
		}
		if got := dispatches.Load(); got != 2 {
			t.Fatalf("dispatched sub-requests = %d, want 2", got)
		}
	})

	t.Run("reduced-window caller waits for slot after 3.4s slow ack and delivers", func(t *testing.T) {
		bp := NewBackpressureController(BackpressureOptions{
			MinRequestBytes:     256,
			MaxRequestBytes:     1024,
			MinInflightRequests: 1,
			MaxInflightRequests: 8,
			DecreaseFactor:      0.5,
			RecoveryInterval:    time.Minute,
			LatencyThreshold:    2 * time.Second,
		})
		bp.effectiveInflight.Store(1)
		bp.OnSlowAck(3400 * time.Millisecond)

		slotTaken := make(chan struct{})
		var calls atomic.Int32
		s := &funcStream{
			appendFn: func(_ context.Context, _ [][]byte) (appendResult, error) {
				if calls.Add(1) == 1 {
					close(slotTaken)
					return newDelayedAppendResult(100*time.Millisecond, nil), nil
				}
				return fakeResult{}, nil
			},
		}
		w := &Writer{
			opts: WriterOptions{
				Project:             "p",
				Dataset:             "d",
				Table:               "otel_spans",
				Signal:              "spans",
				MaxRequestBytes:     1024,
				MaxInflightRequests: 8,
				Backpressure:        bp,
			},
			factory: func(context.Context, ...managedwriter.WriterOption) (stream, error) {
				return s, nil
			},
		}
		defer func() { _ = w.Close() }()

		done1 := make(chan AppendOutcome, 1)
		go func() {
			done1 <- w.Append(context.Background(), [][]byte{make([]byte, 64)})
		}()
		<-slotTaken

		start := time.Now()
		ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel2()
		out2 := w.Append(ctx2, [][]byte{make([]byte, 64)})
		waited := time.Since(start)

		if out2.Err != nil || out2.AcknowledgedRows != 1 {
			t.Fatalf("second caller outcome = %+v, want 1 acknowledged row without error", out2)
		}
		if waited < 50*time.Millisecond {
			t.Fatalf("second caller waited %v, want ~100ms while first caller held slot", waited)
		}
		if out1 := <-done1; out1.Err != nil || out1.AcknowledgedRows != 1 {
			t.Fatalf("first caller outcome = %+v, want 1 acknowledged row", out1)
		}
	})

	t.Run("pre-dispatch slot wait context expiry returns retryable without stream retirement", func(t *testing.T) {
		bp := NewBackpressureController(BackpressureOptions{
			MinRequestBytes:     256,
			MaxRequestBytes:     1024,
			MinInflightRequests: 1,
			MaxInflightRequests: 8,
			DecreaseFactor:      0.5,
			RecoveryInterval:    time.Minute,
			LatencyThreshold:    2 * time.Second,
		})
		bp.effectiveInflight.Store(1)

		slotTaken := make(chan struct{})
		releaseSlot := make(chan struct{})
		var calls atomic.Int32
		obs := &fakeObserver{}
		s := &funcStream{
			appendFn: func(_ context.Context, _ [][]byte) (appendResult, error) {
				if calls.Add(1) == 1 {
					close(slotTaken)
					return &blockingAppendResult{release: releaseSlot}, nil
				}
				return fakeResult{}, nil
			},
		}
		w := &Writer{
			opts: WriterOptions{
				Project:             "p",
				Dataset:             "d",
				Table:               "otel_spans",
				Signal:              "spans",
				MaxRequestBytes:     1024,
				MaxInflightRequests: 8,
				Observer:            obs,
				Backpressure:        bp,
			},
			factory: func(context.Context, ...managedwriter.WriterOption) (stream, error) {
				return s, nil
			},
		}
		defer func() { _ = w.Close() }()
		// Registered after the Close defer so it runs first: when an assertion
		// below fails, the first request is released before Close waits for
		// it, and the test reports the failure instead of hanging.
		releaseOnce := sync.OnceFunc(func() { close(releaseSlot) })
		defer releaseOnce()

		done1 := make(chan AppendOutcome, 1)
		go func() {
			done1 <- w.Append(context.Background(), [][]byte{make([]byte, 64)})
		}()
		<-slotTaken

		ctx2, cancel2 := context.WithTimeout(context.Background(), 25*time.Millisecond)
		defer cancel2()
		done2 := make(chan AppendOutcome, 1)
		go func() {
			done2 <- w.Append(ctx2, [][]byte{make([]byte, 64)})
		}()
		var out2 AppendOutcome
		select {
		case out2 = <-done2:
		case <-time.After(5 * time.Second):
			t.Fatal("slot waiter did not return after its context expired")
		}

		if out2.Verdict.Class != Retryable || out2.Verdict.Label != LabelCanceled {
			t.Fatalf("expired slot-waiter verdict = %+v, want Retryable/%s", out2.Verdict, LabelCanceled)
		}
		if !errors.Is(out2.Err, context.DeadlineExceeded) {
			t.Fatalf("expired slot-waiter err = %v, want context.DeadlineExceeded", out2.Err)
		}
		if out2.Verdict.Class == Uncertain || out2.Verdict.Label == LabelUncertainAck {
			t.Fatalf("expired slot-waiter must not get uncertain verdict: %+v", out2.Verdict)
		}
		if _, _, _, _, retirements := obs.snapshot(); retirements != 0 {
			t.Fatalf("stream retirements = %d, want 0", retirements)
		}

		releaseOnce()
		select {
		case out1 := <-done1:
			if out1.Err != nil || out1.AcknowledgedRows != 1 {
				t.Fatalf("first caller outcome = %+v, want 1 acknowledged row", out1)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("first caller did not finish after its slot was released")
		}
	})

	t.Run("exporterhelper-style concurrent consumers recover from floor to ceiling", func(t *testing.T) {
		bp := NewBackpressureController(BackpressureOptions{
			MinRequestBytes:     200,
			MaxRequestBytes:     1600,
			MinInflightRequests: 1,
			MaxInflightRequests: 8,
			DecreaseFactor:      0.5,
			RecoveryInterval:    15 * time.Millisecond,
			LatencyThreshold:    2 * time.Second,
		})
		bp.effectiveBytes.Store(200)
		bp.effectiveInflight.Store(1)

		s := &countingLatencyStream{ackLatency: 2 * time.Millisecond}
		obs := &fakeObserver{}
		w := &Writer{
			opts: WriterOptions{
				Project:             "p",
				Dataset:             "d",
				Table:               "otel_spans",
				Signal:              "spans",
				MaxRequestBytes:     1600,
				MaxInflightRequests: 8,
				Observer:            obs,
				Backpressure:        bp,
			},
			factory: func(context.Context, ...managedwriter.WriterOption) (stream, error) {
				return s, nil
			},
		}
		defer func() { _ = w.Close() }()

		ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()

		var uncertainCount atomic.Int64
		var wg sync.WaitGroup
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				row := make([]byte, 120)
				pending := [][]byte{row, row, row, row}
				backoff := 3 * time.Millisecond
				for ctx.Err() == nil {
					if bp.EffectiveInflightRequests() == 8 && bp.EffectiveRequestBytes() == 1600 {
						return
					}
					attemptCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
					out := w.Append(attemptCtx, pending)
					cancel()
					if out.Verdict.Class == Uncertain || out.Verdict.Label == LabelUncertainAck {
						uncertainCount.Add(1)
						return
					}
					if out.Err == nil {
						pending = [][]byte{row, row, row, row}
						backoff = 3 * time.Millisecond
						continue
					}
					var remaining [][]byte
					for i := range pending {
						if !out.IsAcked(i) {
							remaining = append(remaining, pending[i])
						}
					}
					if len(remaining) > 0 {
						pending = remaining
					}
					select {
					case <-ctx.Done():
						return
					case <-time.After(backoff):
					}
					if backoff < 12*time.Millisecond {
						backoff = backoff * 3 / 2
					}
				}
			}()
		}
		wg.Wait()

		if got := uncertainCount.Load(); got != 0 {
			t.Fatalf("uncertain verdicts during floor recovery = %d, want 0", got)
		}
		if _, _, _, _, retirements := obs.snapshot(); retirements != 0 {
			t.Fatalf("stream retirements during floor recovery = %d, want 0", retirements)
		}
		if got := bp.EffectiveInflightRequests(); got != 8 {
			t.Fatalf("final EffectiveInflightRequests() = %d, want 8 (ceiling)", got)
		}
		if got := bp.EffectiveRequestBytes(); got != 1600 {
			t.Fatalf("final EffectiveRequestBytes() = %d, want 1600 (ceiling)", got)
		}
	})
}

func TestWriterOversizeInflightRemainsRetryableAfterSiblingReachesFloor(t *testing.T) {
	bp := NewBackpressureController(BackpressureOptions{
		MinRequestBytes:     256,
		MaxRequestBytes:     1024,
		MinInflightRequests: 1,
		MaxInflightRequests: 8,
		DecreaseFactor:      0.5,
		RecoveryInterval:    time.Minute,
		LatencyThreshold:    time.Minute,
	})

	req1Sent := make(chan struct{})
	releaseReq1 := make(chan struct{})
	oversizeErr := status.Error(codes.ResourceExhausted, "Request payload size exceeds the limit")

	var callNum atomic.Int32
	s := &funcStream{
		appendFn: func(_ context.Context, _ [][]byte) (appendResult, error) {
			if callNum.Add(1) == 1 {
				close(req1Sent)
				return &blockingAppendResult{
					release: releaseReq1,
					err:     oversizeErr,
				}, nil
			}
			return fakeResult{err: oversizeErr}, nil
		},
	}

	w := &Writer{
		opts: WriterOptions{
			Project:             "p",
			Dataset:             "d",
			Table:               "otel_spans",
			Signal:              "spans",
			MaxRequestBytes:     1024,
			MaxInflightRequests: 8,
			Backpressure:        bp,
		},
		factory: func(context.Context, ...managedwriter.WriterOption) (stream, error) {
			return s, nil
		},
	}
	defer func() { _ = w.Close() }()
	releaseOnce := sync.OnceFunc(func() { close(releaseReq1) })
	defer releaseOnce()

	row := make([]byte, 200)
	done := make(chan AppendOutcome, 1)
	go func() {
		done <- w.Append(context.Background(), [][]byte{row, row, row})
	}()

	select {
	case <-req1Sent:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Call 1 to be sent")
	}
	// Sibling throttles drive the controller down to the 256-byte floor while
	// Call 1's 600-byte request (split under the old 1024-byte limit) is still
	// in flight.
	bp.effectiveBytes.Store(256)
	bp.effectiveInflight.Store(1)
	releaseOnce()

	var out AppendOutcome
	select {
	case out = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Call 1 to finish after release")
	}
	if out.Verdict.Class != Retryable || out.Verdict.Label != LabelOversizeRequest {
		t.Fatalf("inflight oversize sub-request verdict = %+v, want Retryable/%s", out.Verdict, LabelOversizeRequest)
	}
	if out.Verdict.Owner != OwnerExporterHelper {
		t.Fatalf("inflight oversize sub-request owner = %v, want %v", out.Verdict.Owner, OwnerExporterHelper)
	}

	// A multi-row sub-request sent after the floor is reached cannot be split
	// any smaller (splitLimitBytes == floor == 256), so it remains Permanent.
	outAtFloor := w.Append(context.Background(), [][]byte{make([]byte, 90), make([]byte, 90)})
	if outAtFloor.Verdict.Class != Permanent || outAtFloor.Verdict.Label != LabelQuota {
		t.Fatalf("already-at-floor sub-request verdict = %+v, want Permanent/%s", outAtFloor.Verdict, LabelQuota)
	}
}

// A multi-row sub-request whose AppendRows call fails synchronously with an
// oversize ResourceExhausted error, after a sibling has driven the controller
// to its byte floor, is classified with the split limit it was formed under,
// as on the asynchronous result path: it can still be split smaller, so it is
// retryable rather than a permanent quota failure.
func TestWriterSyncAppendRowsOversizeUsesFrozenSplitLimit(t *testing.T) {
	bp := NewBackpressureController(BackpressureOptions{
		MinRequestBytes:     256,
		MaxRequestBytes:     1024,
		MinInflightRequests: 1,
		MaxInflightRequests: 8,
		DecreaseFactor:      0.5,
		RecoveryInterval:    time.Minute,
		LatencyThreshold:    time.Minute,
	})
	oversizeErr := status.Error(codes.ResourceExhausted, "Request payload size exceeds the limit")

	var calls atomic.Int32
	s := &funcStream{
		appendFn: func(_ context.Context, _ [][]byte) (appendResult, error) {
			calls.Add(1)
			// Sibling throttles reach the 256-byte floor after this request
			// was split under the 1024-byte limit and before AppendRows
			// reports the oversize error.
			bp.effectiveBytes.Store(256)
			bp.effectiveInflight.Store(1)
			return nil, oversizeErr
		},
	}
	w := &Writer{
		opts: WriterOptions{
			Project:             "p",
			Dataset:             "d",
			Table:               "otel_spans",
			Signal:              "spans",
			MaxRequestBytes:     1024,
			MaxInflightRequests: 8,
			Backpressure:        bp,
		},
		factory: func(context.Context, ...managedwriter.WriterOption) (stream, error) {
			return s, nil
		},
	}
	defer func() { _ = w.Close() }()

	row := make([]byte, 200)
	out := w.Append(context.Background(), [][]byte{row, row, row})
	if got := calls.Load(); got != 1 {
		t.Fatalf("AppendRows calls = %d, want 1 (three rows fit one request under the 1024-byte limit)", got)
	}
	if out.Verdict.Class != Retryable || out.Verdict.Label != LabelOversizeRequest {
		t.Fatalf("synchronous oversize sub-request verdict = %+v, want Retryable/%s", out.Verdict, LabelOversizeRequest)
	}
	if out.Verdict.Owner != OwnerExporterHelper {
		t.Fatalf("synchronous oversize sub-request owner = %v, want %v", out.Verdict.Owner, OwnerExporterHelper)
	}

	// A multi-row sub-request formed at the floor cannot be split any smaller
	// (splitLimitBytes == floor == 256), so the same synchronous error stays
	// Permanent.
	outAtFloor := w.Append(context.Background(), [][]byte{make([]byte, 90), make([]byte, 90)})
	if got := calls.Load(); got != 2 {
		t.Fatalf("AppendRows calls = %d, want 2", got)
	}
	if outAtFloor.Verdict.Class != Permanent || outAtFloor.Verdict.Label != LabelQuota {
		t.Fatalf("already-at-floor synchronous sub-request verdict = %+v, want Permanent/%s", outAtFloor.Verdict, LabelQuota)
	}
}

func TestWriterRecoveryWakesBlockedSlotWaiter(t *testing.T) {
	var clockMu sync.Mutex
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	bp := NewBackpressureController(BackpressureOptions{
		MinRequestBytes:     256,
		MaxRequestBytes:     1024,
		MinInflightRequests: 1,
		MaxInflightRequests: 4,
		DecreaseFactor:      0.5,
		RecoveryInterval:    50 * time.Millisecond,
		LatencyThreshold:    time.Second,
		Now: func() time.Time {
			clockMu.Lock()
			defer clockMu.Unlock()
			return now
		},
	})
	bp.effectiveBytes.Store(1024)
	bp.effectiveInflight.Store(1)

	req1Sent := make(chan struct{})
	releaseReq1 := make(chan struct{})
	req2Dispatched := make(chan struct{})
	var calls atomic.Int32

	s := &funcStream{
		appendFn: func(_ context.Context, _ [][]byte) (appendResult, error) {
			switch calls.Add(1) {
			case 1:
				close(req1Sent)
				return &blockingAppendResult{release: releaseReq1}, nil
			case 2:
				close(req2Dispatched)
				return fakeResult{}, nil
			default:
				return fakeResult{}, nil
			}
		},
	}

	w := &Writer{
		opts: WriterOptions{
			Project:             "p",
			Dataset:             "d",
			Table:               "otel_spans",
			Signal:              "spans",
			MaxRequestBytes:     1024,
			MaxInflightRequests: 4,
			Backpressure:        bp,
		},
		factory: func(context.Context, ...managedwriter.WriterOption) (stream, error) {
			return s, nil
		},
	}
	defer func() { _ = w.Close() }()
	// Registered after the Close defer so it runs first: when an assertion
	// below fails, Request 1 is released before Close waits for it, and the
	// test reports the failure instead of hanging.
	releaseOnce := sync.OnceFunc(func() { close(releaseReq1) })
	defer releaseOnce()

	done1 := make(chan AppendOutcome, 1)
	go func() {
		done1 <- w.Append(context.Background(), [][]byte{make([]byte, 64)})
	}()
	<-req1Sent

	done2 := make(chan AppendOutcome, 1)
	go func() {
		done2 <- w.Append(context.Background(), [][]byte{make([]byte, 64)})
	}()

	w.mu.Lock()
	slot0 := w.slots[0]
	w.mu.Unlock()

	// Wait until Goroutine 2 is registered as a slot waiter while Request 1 holds slot 1/1.
	waitDeadline := time.Now().Add(time.Second)
	for {
		slot0.mu.Lock()
		gen := slot0.ms
		waiting := gen != nil && gen.slotSignal != nil
		slot0.mu.Unlock()
		if waiting {
			break
		}
		if time.Now().After(waitDeadline) {
			t.Fatal("timed out waiting for second caller to block on slotSignal")
		}
		time.Sleep(time.Millisecond)
	}

	clockMu.Lock()
	now = now.Add(100 * time.Millisecond)
	clockMu.Unlock()

	slot0.mu.Lock()
	gen := slot0.ms
	slot0.mu.Unlock()
	w.recordCleanOrSlowAck(5*time.Millisecond, gen)
	if got := bp.EffectiveInflightRequests(); got != 2 {
		t.Fatalf("EffectiveInflightRequests() after clean ack = %d, want 2", got)
	}

	// Goroutine 2 must wake up and dispatch Request 2 BEFORE Request 1 finishes.
	select {
	case <-req2Dispatched:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("blocked slot waiter was not woken when window stepped up from 1 to 2")
	}

	releaseOnce()
	for i, done := range []chan AppendOutcome{done1, done2} {
		select {
		case out := <-done:
			if out.Err != nil {
				t.Fatalf("Request %d outcome error = %v, want nil", i+1, out.Err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("Request %d did not finish after Request 1 was released", i+1)
		}
	}
}

// A clean acknowledgement that grows the window must wake callers waiting for
// a slot on the current generation, even when the acknowledged request belongs
// to an older generation that was retired and is still draining.
func TestWriterRecoveryOnRetiredGenerationWakesCurrentGenerationWaiter(t *testing.T) {
	var clockMu sync.Mutex
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	bp := NewBackpressureController(BackpressureOptions{
		MinRequestBytes:     256,
		MaxRequestBytes:     1024,
		MinInflightRequests: 1,
		MaxInflightRequests: 4,
		DecreaseFactor:      0.5,
		RecoveryInterval:    50 * time.Millisecond,
		LatencyThreshold:    time.Second,
		Now: func() time.Time {
			clockMu.Lock()
			defer clockMu.Unlock()
			return now
		},
	})
	bp.effectiveInflight.Store(1)

	req1Sent := make(chan struct{})
	releaseReq1 := make(chan struct{})
	req2Sent := make(chan struct{})
	releaseReq2 := make(chan struct{})
	req3Dispatched := make(chan struct{})
	var calls atomic.Int32
	s := &funcStream{
		appendFn: func(_ context.Context, _ [][]byte) (appendResult, error) {
			switch calls.Add(1) {
			case 1:
				close(req1Sent)
				return &blockingAppendResult{release: releaseReq1}, nil
			case 2:
				close(req2Sent)
				return &blockingAppendResult{release: releaseReq2}, nil
			case 3:
				close(req3Dispatched)
				return fakeResult{}, nil
			default:
				return fakeResult{}, nil
			}
		},
	}
	w := &Writer{
		opts: WriterOptions{
			Project:             "p",
			Dataset:             "d",
			Table:               "otel_spans",
			Signal:              "spans",
			MaxRequestBytes:     1024,
			MaxInflightRequests: 4,
			Backpressure:        bp,
		},
		factory: func(context.Context, ...managedwriter.WriterOption) (stream, error) {
			return s, nil
		},
	}
	defer func() { _ = w.Close() }()
	// Registered after the Close defer so they run first: when an assertion
	// below fails, both blocked requests are released before Close waits for
	// them, and the test reports the failure instead of hanging.
	releaseOnce1 := sync.OnceFunc(func() { close(releaseReq1) })
	defer releaseOnce1()
	releaseOnce2 := sync.OnceFunc(func() { close(releaseReq2) })
	defer releaseOnce2()

	waitClosed := func(ch <-chan struct{}, what string) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %s", what)
		}
	}
	appendAsync := func() chan AppendOutcome {
		done := make(chan AppendOutcome, 1)
		go func() {
			done <- w.Append(context.Background(), [][]byte{make([]byte, 64)})
		}()
		return done
	}

	// Request 1 holds the only slot of generation A, which is then retired
	// while the request is still in flight, as a sibling's failure would.
	done1 := appendAsync()
	waitClosed(req1Sent, "Request 1 to be sent")
	w.mu.Lock()
	slot0 := w.slots[0]
	w.mu.Unlock()
	var genA *streamGeneration
	waitInflight := time.Now().Add(time.Second)
	for {
		slot0.mu.Lock()
		genA = slot0.ms
		registered := genA != nil && genA.pending == 1
		slot0.mu.Unlock()
		if registered {
			break
		}
		if time.Now().After(waitInflight) {
			t.Fatal("timed out waiting for Request 1 result registration on generation A")
		}
		time.Sleep(time.Millisecond)
	}
	w.retireStream(slot0, genA)

	// Request 2 opens generation B and holds its only slot, so Request 3
	// waits for a slot on B.
	done2 := appendAsync()
	waitClosed(req2Sent, "Request 2 to be sent")
	done3 := appendAsync()
	waitDeadline := time.Now().Add(5 * time.Second)
	for {
		slot0.mu.Lock()
		genB := slot0.ms
		waiting := genB != nil && genB != genA && genB.slotSignal != nil
		slot0.mu.Unlock()
		if waiting {
			break
		}
		if time.Now().After(waitDeadline) {
			t.Fatal("timed out waiting for Request 3 to block on generation B's slot")
		}
		time.Sleep(time.Millisecond)
	}

	clockMu.Lock()
	now = now.Add(100 * time.Millisecond)
	clockMu.Unlock()

	// Request 1's clean acknowledgement on retired generation A steps the
	// window up from 1 to 2, which must wake Request 3 on generation B before
	// Request 2 releases B's slot.
	releaseOnce1()
	select {
	case <-req3Dispatched:
	case <-time.After(2 * time.Second):
		t.Fatal("slot waiter on the current generation was not woken when an acknowledgement on a retired generation grew the window from 1 to 2")
	}
	if got := bp.EffectiveInflightRequests(); got != 2 {
		t.Fatalf("EffectiveInflightRequests() after clean ack = %d, want 2", got)
	}

	releaseOnce2()
	for i, done := range []chan AppendOutcome{done1, done2, done3} {
		select {
		case out := <-done:
			if out.Err != nil {
				t.Fatalf("Request %d outcome error = %v, want nil", i+1, out.Err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("Request %d did not finish", i+1)
		}
	}
}

func TestWriterAsyncReadyWatcherExcludesHeadOfLineDrainDelay(t *testing.T) {
	bp := NewBackpressureController(BackpressureOptions{
		MinRequestBytes:     256,
		MaxRequestBytes:     256,
		MinInflightRequests: 1,
		MaxInflightRequests: 4,
		DecreaseFactor:      0.5,
		RecoveryInterval:    time.Minute,
		LatencyThreshold:    100 * time.Millisecond,
	})

	obs := &fakeObserver{}
	var calls atomic.Int32
	s := &funcStream{
		appendFn: func(_ context.Context, _ [][]byte) (appendResult, error) {
			switch calls.Add(1) {
			case 1:
				// Sub-request 0 takes 220ms and fails with a non-throttle error
				// (LabelInternal), holding drainOne at the head of the line.
				return newDelayedAppendResult(220*time.Millisecond, status.Error(codes.Internal, "transient internal failure")), nil
			default:
				// Sub-request 1 finishes in 10ms (< 100ms LatencyThreshold) while
				// drainOne is still blocked on Sub-request 0.
				return newDelayedAppendResult(10*time.Millisecond, nil), nil
			}
		},
	}

	w := &Writer{
		opts: WriterOptions{
			Project:             "p",
			Dataset:             "d",
			Table:               "otel_spans",
			Signal:              "spans",
			MaxRequestBytes:     256,
			MaxInflightRequests: 4,
			Observer:            obs,
			Backpressure:        bp,
		},
		factory: func(context.Context, ...managedwriter.WriterOption) (stream, error) {
			return s, nil
		},
	}
	defer func() { _ = w.Close() }()

	row := make([]byte, 200)
	out := w.Append(context.Background(), [][]byte{row, row})
	if out.AcknowledgedRows != 1 {
		t.Fatalf("AcknowledgedRows = %d, want 1 (second sub-request acked)", out.AcknowledgedRows)
	}
	if got := bp.EffectiveInflightRequests(); got != 4 {
		t.Fatalf("EffectiveInflightRequests() = %d, want 4 (Sub-request 1 ready at 10ms must not count 220ms head-of-line delay)", got)
	}
	obs.mu.Lock()
	events := append([]string(nil), obs.backpressureEvents...)
	obs.mu.Unlock()
	if len(events) != 0 {
		t.Fatalf("backpressure events = %v, want none", events)
	}
}

func TestWriterSiblingTeardownDoesNotThrottleController(t *testing.T) {
	bp := NewBackpressureController(BackpressureOptions{
		MinRequestBytes:     256,
		MaxRequestBytes:     1024,
		MinInflightRequests: 1,
		MaxInflightRequests: 4,
		DecreaseFactor:      0.5,
		RecoveryInterval:    time.Minute,
		LatencyThreshold:    time.Minute,
	})

	obs := &fakeObserver{}
	bothDispatched := make(chan struct{})
	streamRetired := make(chan struct{})
	var calls atomic.Int32
	var closeOnce sync.Once

	s := &funcStream{
		appendFn: func(_ context.Context, _ [][]byte) (appendResult, error) {
			n := calls.Add(1)
			if n == 2 {
				close(bothDispatched)
			}
			return &callbackAppendResult{
				getFn: func(context.Context) (int64, error) {
					<-bothDispatched
					if n == 1 {
						// Non-throttle stream failure retires the generation.
						return 0, status.Error(codes.FailedPrecondition, "stream is closed")
					}
					// Sibling waits until Call 1 retires the stream, then sees
					// local stream teardown (codes.Canceled -> siblingTeardown).
					<-streamRetired
					return 0, status.Error(codes.Canceled, "canceled by stream teardown")
				},
			}, nil
		},
		closeFn: func() error {
			closeOnce.Do(func() { close(streamRetired) })
			return nil
		},
	}

	w := &Writer{
		opts: WriterOptions{
			Project:             "p",
			Dataset:             "d",
			Table:               "otel_spans",
			Signal:              "spans",
			MaxRequestBytes:     1024,
			MaxInflightRequests: 4,
			Observer:            obs,
			Backpressure:        bp,
		},
		factory: func(context.Context, ...managedwriter.WriterOption) (stream, error) {
			return s, nil
		},
	}
	defer func() { _ = w.Close() }()

	outcomes := make(chan AppendOutcome, 2)
	for range 2 {
		go func() {
			outcomes <- w.Append(context.Background(), [][]byte{make([]byte, 64)})
		}()
	}

	var sawUncertain bool
	for range 2 {
		out := <-outcomes
		if out.Verdict.Label == LabelUncertainAck {
			sawUncertain = true
		}
	}
	if !sawUncertain {
		t.Fatal("expected sibling teardown call to receive LabelUncertainAck")
	}
	if got := bp.EffectiveInflightRequests(); got != 4 {
		t.Fatalf("EffectiveInflightRequests() = %d, want 4 (sibling teardown must not throttle controller)", got)
	}
	if got := bp.EffectiveRequestBytes(); got != 1024 {
		t.Fatalf("EffectiveRequestBytes() = %d, want 1024", got)
	}
	obs.mu.Lock()
	events := append([]string(nil), obs.backpressureEvents...)
	obs.mu.Unlock()
	if len(events) != 0 {
		t.Fatalf("backpressure events = %v, want none", events)
	}
}

type funcStream struct {
	appendFn func(context.Context, [][]byte) (appendResult, error)
	closeFn  func() error
}

func (f *funcStream) AppendRows(ctx context.Context, rows [][]byte, _ ...managedwriter.AppendOption) (appendResult, error) {
	return f.appendFn(ctx, rows)
}

func (f *funcStream) Close() error {
	if f.closeFn != nil {
		return f.closeFn()
	}
	return nil
}

type blockingAppendResult struct {
	release <-chan struct{}
	err     error
}

func (r *blockingAppendResult) Ready() <-chan struct{} {
	return r.release
}

func (r *blockingAppendResult) GetResult(ctx context.Context) (int64, error) {
	select {
	case <-r.release:
		return 0, r.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (r *blockingAppendResult) FullResponse(ctx context.Context) (*storagepb.AppendRowsResponse, error) {
	_, err := r.GetResult(ctx)
	return nil, err
}

type callbackAppendResult struct {
	ready <-chan struct{}
	getFn func(context.Context) (int64, error)
}

func (r *callbackAppendResult) Ready() <-chan struct{} {
	if r.ready != nil {
		return r.ready
	}
	ch := make(chan struct{})
	close(ch)
	return ch
}

func (r *callbackAppendResult) GetResult(ctx context.Context) (int64, error) {
	return r.getFn(ctx)
}

func (r *callbackAppendResult) FullResponse(ctx context.Context) (*storagepb.AppendRowsResponse, error) {
	_, err := r.GetResult(ctx)
	return nil, err
}

func newDelayedAppendResult(delay time.Duration, err error) appendResult {
	ready := make(chan struct{})
	go func() {
		time.Sleep(delay)
		close(ready)
	}()
	return &blockingAppendResult{release: ready, err: err}
}

type poolCountingStream struct {
	calls  atomic.Int64
	closed atomic.Int64
}

func (s *poolCountingStream) AppendRows(
	context.Context, [][]byte, ...managedwriter.AppendOption,
) (appendResult, error) {
	s.calls.Add(1)
	return fakeResult{}, nil
}

func (s *poolCountingStream) Close() error {
	s.closed.Add(1)
	return nil
}

func TestWriterStreamPoolOpensNStreamsAndBalancesLoad(t *testing.T) {
	const poolSize = 4
	var (
		mu      sync.Mutex
		streams []*poolCountingStream
	)
	w := &Writer{
		opts: WriterOptions{
			Project:             "p",
			Dataset:             "d",
			Table:               "t",
			MaxRequestBytes:     150,
			MaxInflightRequests: 4,
			MaxInflightBytes:    64 * 1024,
			StreamPoolSize:      poolSize,
		},
		lifetimeCtx: context.Background(),
		factory: func(context.Context, ...managedwriter.WriterOption) (stream, error) {
			s := &poolCountingStream{}
			mu.Lock()
			streams = append(streams, s)
			mu.Unlock()
			return s, nil
		},
	}

	row := make([]byte, 60)
	rows := make([][]byte, 16)
	for i := range rows {
		rows[i] = row
	}
	out := w.Append(context.Background(), rows)
	if out.Err != nil || out.AcknowledgedRows != 16 {
		t.Fatalf("Append outcome = %+v, want 16 acknowledged rows and nil error", out)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	mu.Lock()
	created := append([]*poolCountingStream(nil), streams...)
	mu.Unlock()
	if len(created) != poolSize {
		t.Fatalf("created %d streams, want %d", len(created), poolSize)
	}
	var total int64
	for i, s := range created {
		calls := s.calls.Load()
		if calls != 4 {
			t.Errorf("slot %d AppendRows calls = %d, want 4", i, calls)
		}
		if s.closed.Load() != 1 {
			t.Errorf("slot %d Close calls = %d, want 1", i, s.closed.Load())
		}
		total += calls
	}
	if total != 16 {
		t.Fatalf("total AppendRows calls = %d, want 16", total)
	}
}

// INV-P2-2: retiring a stream generation on one slot must never close or
// disturb an in-flight append on another slot in the pool.
func TestWriterStreamPoolPerSlotRetirementIsolation(t *testing.T) {
	slot0Pending := newControlledResult()
	slot0Initial := &scriptedStream{results: []appendResult{slot0Pending}}
	slot0Replacement := &scriptedStream{results: []appendResult{fakeResult{}}}

	slot1Inflight := newControlledResult()
	slot1Initial := &scriptedStream{results: []appendResult{slot1Inflight, fakeResult{}}}

	var (
		mu      sync.Mutex
		created []stream
		nextIdx int
	)
	script := []stream{slot0Initial, slot1Initial, slot0Replacement}
	obs := &fakeObserver{}
	w := &Writer{
		opts: WriterOptions{
			Project:             "p",
			Dataset:             "d",
			Table:               "t",
			MaxRequestBytes:     1024,
			MaxInflightRequests: 2,
			MaxInflightBytes:    64 * 1024,
			StreamPoolSize:      2,
			Observer:            obs,
		},
		lifetimeCtx: context.Background(),
		factory: func(context.Context, ...managedwriter.WriterOption) (stream, error) {
			mu.Lock()
			defer mu.Unlock()
			s := script[nextIdx]
			nextIdx++
			created = append(created, s)
			return s, nil
		},
	}
	defer func() { _ = w.Close() }()
	// Guard both blocked results with sync.OnceFunc deferred after w.Close so
	// they run first on early test failure and prevent w.Close from hanging.
	completeSlot0 := sync.OnceFunc(func() {
		slot0Pending.complete(status.Error(codes.FailedPrecondition, "stream is finalized"))
	})
	defer completeSlot0()
	completeSlot1 := sync.OnceFunc(func() {
		slot1Inflight.complete(nil)
	})
	defer completeSlot1()

	// Hold slot 0's append in flight until slot 1 also has an in-flight append,
	// then fail slot 0 with FailedPrecondition so slot 0 retires while slot 1
	// is mid-append.
	slot0Done := make(chan AppendOutcome, 1)
	go func() {
		slot0Done <- w.Append(context.Background(), [][]byte{{1}})
	}()
	select {
	case <-slot0Pending.started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for slot 0 append to start")
	}

	slot1Done := make(chan AppendOutcome, 1)
	go func() {
		slot1Done <- w.Append(context.Background(), [][]byte{{2}})
	}()
	select {
	case <-slot1Inflight.started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for slot 1 append to start")
	}

	// Now fail slot 0 with FailedPrecondition ("stream is finalized").
	completeSlot0()
	var out0 AppendOutcome
	select {
	case out0 = <-slot0Done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for slot 0 append outcome")
	}
	if !out0.Verdict.StreamRecreate || out0.Verdict.Label != LabelInvalidStream {
		t.Fatalf("slot 0 verdict = %+v, want StreamRecreate=true with label=%s", out0.Verdict, LabelInvalidStream)
	}
	if slot0Initial.closeCount() != 1 {
		t.Fatalf("slot 0 initial stream close count = %d, want 1", slot0Initial.closeCount())
	}
	if slot1Initial.closeCount() != 0 {
		t.Fatalf("slot 1 initial stream close count = %d while slot 0 retired, want 0 (INV-P2-2)", slot1Initial.closeCount())
	}

	// Complete slot 1's in-flight request cleanly and verify AckedRanges.
	completeSlot1()
	var out1 AppendOutcome
	select {
	case out1 = <-slot1Done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for slot 1 append outcome")
	}
	if out1.Err != nil || out1.AcknowledgedRows != 1 {
		t.Fatalf("slot 1 outcome = %+v, want 1 acknowledged row and nil error", out1)
	}
	if len(out1.AckedRanges) != 1 || out1.AckedRanges[0] != ([2]int{0, 1}) {
		t.Fatalf("slot 1 AckedRanges = %v, want [[0 1]]", out1.AckedRanges)
	}

	// Subsequent appends open slot 0's replacement stream and succeed on both slots.
	for i := range 2 {
		if out := w.Append(context.Background(), [][]byte{{byte(i + 3)}}); out.Err != nil {
			t.Fatalf("post-retirement append %d failed: %v", i, out.Err)
		}
	}
	mu.Lock()
	totalCreated := len(created)
	mu.Unlock()
	if totalCreated != 3 {
		t.Fatalf("created %d streams total, want 3 (slot 0 initial, slot 1 initial, slot 0 replacement)", totalCreated)
	}
	if slot1Initial.closeCount() != 0 {
		t.Fatalf("slot 1 initial stream close count before Writer.Close = %d, want 0", slot1Initial.closeCount())
	}
	if _, _, _, _, retirements := obs.snapshot(); retirements != 1 {
		t.Fatalf("stream retirements = %d, want 1", retirements)
	}
}

func TestWriterStreamPoolSelectsSlotWithOpenWindowOverLowerPendingFullSlot(t *testing.T) {
	bp := NewBackpressureController(BackpressureOptions{
		MinRequestBytes:     256,
		MaxRequestBytes:     1024,
		MinInflightRequests: 1,
		MaxInflightRequests: 2,
		DecreaseFactor:      0.5,
		RecoveryInterval:    time.Minute,
		LatencyThreshold:    time.Minute,
	})

	slot0Sent := make(chan struct{})
	releaseSlot0 := make(chan struct{})
	slot0Stream := &funcStream{
		appendFn: func(_ context.Context, _ [][]byte) (appendResult, error) {
			close(slot0Sent)
			return &blockingAppendResult{release: releaseSlot0}, nil
		},
	}

	slot1Sent1 := make(chan struct{})
	slot1Sent2 := make(chan struct{})
	releaseSlot1A := make(chan struct{})
	var slot1ACalls atomic.Int32
	slot1GenAStream := &funcStream{
		appendFn: func(_ context.Context, _ [][]byte) (appendResult, error) {
			switch slot1ACalls.Add(1) {
			case 1:
				close(slot1Sent1)
			case 2:
				close(slot1Sent2)
			}
			return &blockingAppendResult{release: releaseSlot1A}, nil
		},
	}

	var slot1BCalls atomic.Int32
	slot1GenBStream := &funcStream{
		appendFn: func(_ context.Context, _ [][]byte) (appendResult, error) {
			slot1BCalls.Add(1)
			return fakeResult{}, nil
		},
	}

	var (
		mu      sync.Mutex
		nextIdx int
	)
	script := []stream{slot0Stream, slot1GenAStream, slot1GenBStream}
	w := &Writer{
		opts: WriterOptions{
			Project:             "p",
			Dataset:             "d",
			Table:               "t",
			MaxRequestBytes:     1024,
			MaxInflightRequests: 2,
			MaxInflightBytes:    64 * 1024,
			StreamPoolSize:      2,
			Backpressure:        bp,
		},
		lifetimeCtx: context.Background(),
		factory: func(context.Context, ...managedwriter.WriterOption) (stream, error) {
			mu.Lock()
			defer mu.Unlock()
			s := script[nextIdx]
			nextIdx++
			return s, nil
		},
	}
	defer func() { _ = w.Close() }()
	release0Once := sync.OnceFunc(func() { close(releaseSlot0) })
	defer release0Once()
	release1AOnce := sync.OnceFunc(func() { close(releaseSlot1A) })
	defer release1AOnce()

	waitSent := func(ch <-chan struct{}, what string) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for %s", what)
		}
	}
	startAppend := func() chan AppendOutcome {
		done := make(chan AppendOutcome, 1)
		go func() {
			done <- w.Append(context.Background(), [][]byte{make([]byte, 32)})
		}()
		return done
	}

	// Caller 1 takes slot 0 (1/2 in flight on slot 0).
	done1 := startAppend()
	waitSent(slot0Sent, "slot 0 request")

	// Caller 2 takes slot 1 (1/2 in flight on slot 1 generation A).
	done2 := startAppend()
	waitSent(slot1Sent1, "slot 1 generation A request 1")

	// Caller 3 also goes to slot 1 (2/2 in flight on slot 1 generation A).
	w.nextSlot.Store(1)
	done3 := startAppend()
	waitSent(slot1Sent2, "slot 1 generation A request 2")

	// Retire slot 1 generation A while both of its requests remain draining,
	// then shrink the effective per-slot window to 1. Slot 0 now has lower
	// pending (1) but its live generation is full (dispatched == 1), whereas
	// slot 1 has higher pending (2 draining on retired genA) but open window
	// capacity for a new generation (activeGenerations == 1 < 2).
	w.mu.Lock()
	slot0 := w.slots[0]
	slot1 := w.slots[1]
	w.mu.Unlock()
	var genA *streamGeneration
	waitInflight := time.Now().Add(time.Second)
	for {
		slot0.mu.Lock()
		s0Ready := slot0.ms != nil && slot0.ms.pending == 1
		slot0.mu.Unlock()
		slot1.mu.Lock()
		genA = slot1.ms
		s1Ready := genA != nil && genA.pending == 2
		slot1.mu.Unlock()
		if s0Ready && s1Ready {
			break
		}
		if time.Now().After(waitInflight) {
			t.Fatal("timed out waiting for slot 0 and slot 1 results to register")
		}
		time.Sleep(time.Millisecond)
	}
	w.retireStream(slot1, genA)

	bp.effectiveInflight.Store(1)
	w.nextSlot.Store(0) // Start scan at slot 0 so lower pending alone would pick slot 0.

	if got := slot0.pending.Load(); got != 1 {
		t.Fatalf("slot 0 pending = %d, want 1", got)
	}
	if got := slot1.pending.Load(); got != 2 {
		t.Fatalf("slot 1 pending = %d, want 2", got)
	}
	if slotHasOpenWindow(slot0, 1) {
		t.Fatal("slot 0 should not have an open window at effectiveInflight=1")
	}
	if !slotHasOpenWindow(slot1, 1) {
		t.Fatal("slot 1 should have an open window for its replacement generation")
	}

	// Caller 4 must select slot 1 (open window) and finish immediately without
	// blocking on slot 0's full window.
	done4 := startAppend()
	select {
	case out4 := <-done4:
		if out4.Err != nil || out4.AcknowledgedRows != 1 {
			t.Fatalf("caller 4 outcome = %+v, want 1 acknowledged row and nil error", out4)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("caller 4 blocked on full slot 0 instead of selecting slot 1 with an open window")
	}
	if got := slot1BCalls.Load(); got != 1 {
		t.Fatalf("slot 1 generation B AppendRows calls = %d, want 1", got)
	}

	release0Once()
	release1AOnce()
	for i, done := range []chan AppendOutcome{done1, done2, done3} {
		select {
		case out := <-done:
			if out.Err != nil {
				t.Fatalf("caller %d outcome error = %v, want nil", i+1, out.Err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("caller %d did not finish after release", i+1)
		}
	}
}

type byteTrackingResult struct {
	ready   chan struct{}
	release func()
	once    sync.Once
}

func (r *byteTrackingResult) Ready() <-chan struct{} { return r.ready }

func (r *byteTrackingResult) GetResult(ctx context.Context) (int64, error) {
	select {
	case <-r.ready:
		r.once.Do(r.release)
		return 0, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (r *byteTrackingResult) FullResponse(context.Context) (*storagepb.AppendRowsResponse, error) {
	return nil, nil
}

type byteTrackingStream struct {
	inflight  *atomic.Int64
	peak      *atomic.Int64
	hold      time.Duration
	wireBytes func([][]byte) int
}

func (s *byteTrackingStream) AppendRows(
	_ context.Context, req [][]byte, _ ...managedwriter.AppendOption,
) (appendResult, error) {
	n := int64(s.wireBytes(req))
	cur := s.inflight.Add(n)
	for {
		old := s.peak.Load()
		if cur <= old || s.peak.CompareAndSwap(old, cur) {
			break
		}
	}
	ready := make(chan struct{})
	res := &byteTrackingResult{
		ready:   ready,
		release: func() { s.inflight.Add(-n) },
	}
	time.AfterFunc(s.hold, func() { close(ready) })
	return res, nil
}

func (s *byteTrackingStream) Close() error { return nil }

func TestWriterStreamPoolEnforcesPoolWideMaxInflightBytes(t *testing.T) {
	// A single 84-byte row has rowWireBytes = 86, plus 64-byte base framing
	// and 55-byte table stream path ("projects/p/datasets/d/tables/t/streams/_default")
	// = 205 wire bytes per sub-request. Setting MaxInflightBytes = 410 allows
	// at most 2 sub-requests in flight across all 4 slots simultaneously.
	const (
		poolSize         = 4
		maxInflightBytes = 410
		rowSize          = 84
	)
	var (
		inflight atomic.Int64
		peak     atomic.Int64
	)
	w := &Writer{
		opts: WriterOptions{
			Project:             "p",
			Dataset:             "d",
			Table:               "t",
			MaxRequestBytes:     200,
			MaxInflightRequests: 8,
			MaxInflightBytes:    maxInflightBytes,
			StreamPoolSize:      poolSize,
		},
		lifetimeCtx: context.Background(),
	}
	w.factory = func(context.Context, ...managedwriter.WriterOption) (stream, error) {
		return &byteTrackingStream{
			inflight:  &inflight,
			peak:      &peak,
			hold:      5 * time.Millisecond,
			wireBytes: w.requestWireBytes,
		}, nil
	}
	defer func() { _ = w.Close() }()

	const callers = 8
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			row := make([]byte, rowSize)
			out := w.Append(context.Background(), [][]byte{row, row})
			if out.Err != nil || out.AcknowledgedRows != 2 {
				errs <- fmt.Errorf("Append outcome = %+v, want 2 rows and nil error", out)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if got := peak.Load(); got > maxInflightBytes {
		t.Fatalf("peak concurrent wire bytes across pool = %d, exceeded MaxInflightBytes %d", got, maxInflightBytes)
	}
	if got := peak.Load(); got < 205 {
		t.Fatalf("peak concurrent wire bytes = %d, want at least one 205-byte request", got)
	}
	if got := inflight.Load(); got != 0 {
		t.Fatalf("final in-flight wire bytes = %d, want 0", got)
	}
}

func TestWriterStreamPoolSizeOneMatchesSingleStream(t *testing.T) {
	firstFailed := newControlledResult()
	firstFailed.complete(status.Error(codes.FailedPrecondition, "stream is finalized"))
	first := &scriptedStream{results: []appendResult{fakeResult{}, firstFailed}}
	second := &scriptedStream{results: []appendResult{fakeResult{}}}
	w, createdWith := newScriptedWriter(first, second)
	w.opts.StreamPoolSize = 1
	w.opts.MaxRequestBytes = 150
	w.opts.MaxInflightBytes = 1024
	defer func() { _ = w.Close() }()

	row := make([]byte, 60)
	if out := w.Append(context.Background(), [][]byte{row}); out.Err != nil || out.AcknowledgedRows != 1 {
		t.Fatalf("first append = %+v, want 1 acknowledged row", out)
	}
	if out := w.Append(context.Background(), [][]byte{row}); !out.Verdict.StreamRecreate {
		t.Fatalf("second append verdict = %+v, want StreamRecreate=true", out.Verdict)
	}
	if out := w.Append(context.Background(), [][]byte{row}); out.Err != nil || out.AcknowledgedRows != 1 {
		t.Fatalf("third append = %+v, want recovery on replacement stream", out)
	}
	if len(*createdWith) != 2 {
		t.Fatalf("created %d streams, want 2 (initial + replacement)", len(*createdWith))
	}
}

func TestWriterStreamPoolDrainsInflightInsteadOfBlockingOnExhaustedSlotGenerations(t *testing.T) {
	var created atomic.Int32
	w := &Writer{
		opts: WriterOptions{
			Project:             "p",
			Dataset:             "d",
			Table:               "t",
			MaxRequestBytes:     200, // one 100-byte row per request
			MaxInflightRequests: 1,   // per-slot window
			MaxInflightBytes:    221, // pool-wide budget == one request (102 + 64 + 55)
			StreamPoolSize:      2,
		},
		lifetimeCtx: context.Background(),
	}
	w.factory = func(context.Context, ...managedwriter.WriterOption) (stream, error) {
		created.Add(1)
		return &funcStream{
			appendFn: func(context.Context, [][]byte) (appendResult, error) {
				return fakeResult{}, nil
			},
		}, nil
	}
	defer func() { _ = w.Close() }()

	slots, err := w.beginAppend()
	if err != nil {
		t.Fatal(err)
	}
	w.appendWG.Done()
	slotB := slots[1]
	ctx := context.Background()

	// Pin both generation slots on slot B with retired generations held by
	// simulated concurrent callers so slot B has ms == nil and activeGenerations == 2.
	genB1, err := w.ensureSlotStream(ctx, slotB)
	if err != nil {
		t.Fatal(err)
	}
	w.retireStream(slotB, genB1)
	defer w.releaseGenerationUser(slotB, genB1)

	genB2, err := w.ensureSlotStream(ctx, slotB)
	if err != nil {
		t.Fatal(err)
	}
	w.retireStream(slotB, genB2)
	defer w.releaseGenerationUser(slotB, genB2)

	slotB.mu.Lock()
	bExhausted := slotB.ms == nil && slotB.creation == nil && slotB.activeGenerations == maxActiveStreamGenerations
	slotB.mu.Unlock()
	if !bExhausted {
		t.Fatal("slot B did not reach exhausted generation state")
	}

	row := make([]byte, 100)
	// Caller O sends two 166-byte requests. Request 1 goes to slot A; Request 2
	// sees slot B exhausted while Request 1 is still in flight, drains Request 1,
	// frees slot A's window and the 166-byte pool budget, and finishes on slot A
	// without blocking on slot B's generationSignal.
	w.nextSlot.Store(0)
	ctxO, cancelO := context.WithTimeout(ctx, time.Second)
	defer cancelO()
	outO := w.Append(ctxO, [][]byte{row, row})
	if outO.Err != nil || outO.AcknowledgedRows != 2 {
		t.Fatalf("caller O outcome = %+v, want 2 acknowledged rows and nil error while slot B is exhausted", outO)
	}

	ctxU, cancelU := context.WithTimeout(ctx, time.Second)
	defer cancelU()
	outU := w.Append(ctxU, [][]byte{row})
	if outU.Err != nil || outU.AcknowledgedRows != 1 {
		t.Fatalf("caller U outcome = %+v, want 1 acknowledged row and nil error", outU)
	}
}

func TestWriterStreamPoolReleasesRetiredCachedGenerationBeforeBlockingWait(t *testing.T) {
	rA1 := newControlledResult()
	rB1 := newControlledResult()
	var streamsMade atomic.Int32
	w := &Writer{
		opts: WriterOptions{
			Project:             "p",
			Dataset:             "d",
			Table:               "t",
			MaxRequestBytes:     200,
			MaxInflightRequests: 1,
			MaxInflightBytes:    1 << 20,
			StreamPoolSize:      2,
		},
		lifetimeCtx: context.Background(),
	}
	w.factory = func(context.Context, ...managedwriter.WriterOption) (stream, error) {
		switch streamsMade.Add(1) {
		case 1:
			// Slot A initial stream: held by Caller A1 so slot A's window stays full.
			return &funcStream{
				appendFn: func(context.Context, [][]byte) (appendResult, error) {
					return rA1, nil
				},
			}, nil
		case 2:
			// Slot B generation 1: first request blocks on rB1.
			return &funcStream{
				appendFn: func(context.Context, [][]byte) (appendResult, error) {
					return rB1, nil
				},
			}, nil
		default:
			return &funcStream{
				appendFn: func(context.Context, [][]byte) (appendResult, error) {
					return fakeResult{}, nil
				},
			}, nil
		}
	}
	defer func() { _ = w.Close() }()
	completeA1 := sync.OnceFunc(func() { rA1.complete(nil) })
	defer completeA1()
	completeB1 := sync.OnceFunc(func() { rB1.complete(nil) })
	defer completeB1()

	slots, err := w.beginAppend()
	if err != nil {
		t.Fatal(err)
	}
	w.appendWG.Done()
	slotB := slots[1]
	ctx := context.Background()

	// Caller A1 occupies slot A's single window slot.
	w.nextSlot.Store(0)
	doneA1 := make(chan AppendOutcome, 1)
	go func() {
		doneA1 <- w.Append(ctx, [][]byte{make([]byte, 100)})
	}()
	rA1.waitForCalls(t, 1)

	// Caller P sends 2 requests starting at slot B: Request 1 dispatches on
	// slot B (genB1) and blocks in drainOne(rB1) when Request 2 finds both
	// slots' windows full.
	w.nextSlot.Store(1)
	doneP := make(chan AppendOutcome, 1)
	go func() {
		row := make([]byte, 100)
		doneP <- w.Append(ctx, [][]byte{row, row})
	}()
	rB1.waitForCalls(t, 1)

	slotB.mu.Lock()
	genB1 := slotB.ms
	slotB.mu.Unlock()
	if genB1 == nil {
		t.Fatal("slot B generation 1 was not created")
	}

	// Retire genB1, then open and retire genB2 with a pinned user ref.
	// Slot B now has activeGenerations == 2: genB1 is held by Caller P's
	// cached slotGens[1] + rB1, and genB2 is held by this test.
	w.retireStream(slotB, genB1)
	genB2, err := w.ensureSlotStream(ctx, slotB)
	if err != nil {
		t.Fatal(err)
	}
	w.retireStream(slotB, genB2)
	defer w.releaseGenerationUser(slotB, genB2)

	// Complete rB1: Caller P finishes draining Request 1, releases its retired
	// cached slotGens[1] (genB1) before its next slot selection/wait, freeing
	// one generation slot on slot B so Request 2 opens genB3 and completes
	// while slot A is still blocked by Caller A1.
	completeB1()
	select {
	case outP := <-doneP:
		if outP.Err != nil || outP.AcknowledgedRows != 2 {
			t.Fatalf("caller P outcome = %+v, want 2 acknowledged rows and nil error", outP)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("caller P did not release retired cached slotGens[1] before blocking on full slot A")
	}

	completeA1()
	select {
	case outA1 := <-doneA1:
		if outA1.Err != nil || outA1.AcknowledgedRows != 1 {
			t.Fatalf("caller A1 outcome = %+v, want 1 acknowledged row", outA1)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("caller A1 did not finish")
	}
}

func TestWriterStreamPoolRefreshesRetiredCachedGenerationOnHealthyReplacement(t *testing.T) {
	rA1 := newControlledResult()
	rB1 := newControlledResult()
	var streamsMade atomic.Int32
	w := &Writer{
		opts: WriterOptions{
			Project:             "p",
			Dataset:             "d",
			Table:               "t",
			MaxRequestBytes:     200, // one 100-byte row per request
			MaxInflightRequests: 1,
			MaxInflightBytes:    1 << 20,
			StreamPoolSize:      2,
		},
		lifetimeCtx: context.Background(),
	}
	w.factory = func(context.Context, ...managedwriter.WriterOption) (stream, error) {
		switch streamsMade.Add(1) {
		case 1:
			// Slot A initial stream: held by Caller 1A so slot A's window stays full.
			return &funcStream{
				appendFn: func(context.Context, [][]byte) (appendResult, error) {
					return rA1, nil
				},
			}, nil
		case 2:
			// Slot B generation 1: held by Caller 1B so slot B's window stays full.
			return &funcStream{
				appendFn: func(context.Context, [][]byte) (appendResult, error) {
					return rB1, nil
				},
			}, nil
		default:
			// Healthy replacement stream on Slot B.
			return &funcStream{
				appendFn: func(context.Context, [][]byte) (appendResult, error) {
					return fakeResult{}, nil
				},
			}, nil
		}
	}
	defer func() { _ = w.Close() }()
	completeA1 := sync.OnceFunc(func() { rA1.complete(nil) })
	defer completeA1()
	completeB1 := sync.OnceFunc(func() { rB1.complete(nil) })
	defer completeB1()

	slots, err := w.beginAppend()
	if err != nil {
		t.Fatal(err)
	}
	w.appendWG.Done()
	slotB := slots[1]
	ctx := context.Background()
	row := make([]byte, 100)

	// Fill slot A and slot B so both slots' 1-request windows are full.
	w.nextSlot.Store(0)
	done1A := make(chan AppendOutcome, 1)
	go func() { done1A <- w.Append(ctx, [][]byte{row}) }()
	rA1.waitForCalls(t, 1)

	w.nextSlot.Store(1)
	done1B := make(chan AppendOutcome, 1)
	go func() { done1B <- w.Append(ctx, [][]byte{row}) }()
	rB1.waitForCalls(t, 1)

	slotB.mu.Lock()
	genB1 := slotB.ms
	slotB.mu.Unlock()
	if genB1 == nil {
		t.Fatal("slot B has no generation")
	}

	// Start Caller 2 pointing at Slot B. With len(inflight) == 0 and both
	// windows full, Caller 2 caches slotGens[1] = genB1 and parks inside
	// waitForDispatchSlot(ctx, slotB, genB1, reqBytes).
	w.nextSlot.Store(1)
	done2 := make(chan AppendOutcome, 1)
	go func() { done2 <- w.Append(ctx, [][]byte{row, row}) }()

	deadline := time.Now().Add(2 * time.Second)
	for {
		slotB.mu.Lock()
		waiting := genB1.slotSignal != nil
		slotB.mu.Unlock()
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Caller 2 did not block in waitForDispatchSlot on genB1")
		}
		time.Sleep(time.Millisecond)
	}

	// Retire genB1 while Caller 2 is parked inside waitForDispatchSlot on it.
	// Retiring genB1 wakes waitForDispatchSlot with errStreamRetired; Caller 2
	// must hit the pre-dispatch errors.Is(acquireErr, errStreamRetired) retry
	// branch, release its retired cached slotGens[1], open healthy replacement
	// genB2 on Slot B, and finish both rows while Slot A is still blocked.
	w.retireStream(slotB, genB1)

	select {
	case out2 := <-done2:
		if out2.Err != nil || out2.AcknowledgedRows != 2 {
			t.Fatalf("Caller 2 outcome = %+v, want 2 acknowledged rows and nil error", out2)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Caller 2 did not recover from errStreamRetired on healthy replacement")
	}
	if got := streamsMade.Load(); got != 3 {
		t.Fatalf("streamsMade = %d, want 3 (slot A + slot B initial + slot B replacement)", got)
	}

	completeA1()
	completeB1()
	if out1A := <-done1A; out1A.Err != nil || out1A.AcknowledgedRows != 1 {
		t.Fatalf("Caller 1A outcome = %+v, want 1 acknowledged row", out1A)
	}
	if out1B := <-done1B; out1B.Err != nil || out1B.AcknowledgedRows != 1 {
		t.Fatalf("Caller 1B outcome = %+v, want 1 acknowledged row", out1B)
	}
}

func TestWriterStreamPoolBackpressureRecoveryWakesOtherSlotWaiter(t *testing.T) {
	var (
		clockMu sync.Mutex
		now     = time.Date(2026, 8, 12, 16, 0, 0, 0, time.UTC)
	)
	nowFn := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		clockMu.Lock()
		now = now.Add(d)
		clockMu.Unlock()
	}

	bp := NewBackpressureController(BackpressureOptions{
		MinRequestBytes:     100,
		MaxRequestBytes:     200,
		MinInflightRequests: 1,
		MaxInflightRequests: 2,
		DecreaseFactor:      0.5,
		RecoveryInterval:    10 * time.Millisecond,
		Now:                 nowFn,
	})
	if !bp.OnThrottle() || bp.EffectiveInflightRequests() != 1 {
		t.Fatalf("bp.EffectiveInflightRequests() = %d after OnThrottle, want 1", bp.EffectiveInflightRequests())
	}

	rA1 := newControlledResult()
	rB1 := newControlledResult()
	var streamsMade atomic.Int32
	w := &Writer{
		opts: WriterOptions{
			Project:             "p",
			Dataset:             "d",
			Table:               "t",
			MaxRequestBytes:     200,
			MaxInflightRequests: 2,
			MaxInflightBytes:    1 << 20,
			StreamPoolSize:      2,
			Backpressure:        bp,
		},
		lifetimeCtx: context.Background(),
	}
	w.factory = func(context.Context, ...managedwriter.WriterOption) (stream, error) {
		switch streamsMade.Add(1) {
		case 1:
			return &funcStream{
				appendFn: func(context.Context, [][]byte) (appendResult, error) {
					return rA1, nil
				},
			}, nil
		default:
			var slotBCalls atomic.Int32
			return &funcStream{
				appendFn: func(context.Context, [][]byte) (appendResult, error) {
					if slotBCalls.Add(1) == 1 {
						return rB1, nil
					}
					return fakeResult{}, nil
				},
			}, nil
		}
	}
	defer func() { _ = w.Close() }()
	completeA1 := sync.OnceFunc(func() { rA1.complete(nil) })
	defer completeA1()
	completeB1 := sync.OnceFunc(func() { rB1.complete(nil) })
	defer completeB1()

	slots, err := w.beginAppend()
	if err != nil {
		t.Fatal(err)
	}
	w.appendWG.Done()
	slotB := slots[1]
	ctx := context.Background()
	row := make([]byte, 50)

	// Fill Slot 0 (rA1) and Slot 1 (rB1) under the reduced effective window of 1.
	w.nextSlot.Store(0)
	doneA1 := make(chan AppendOutcome, 1)
	go func() { doneA1 <- w.Append(ctx, [][]byte{row}) }()
	rA1.waitForCalls(t, 1)

	w.nextSlot.Store(1)
	doneB1 := make(chan AppendOutcome, 1)
	go func() { doneB1 <- w.Append(ctx, [][]byte{row}) }()
	rB1.waitForCalls(t, 1)

	slotB.mu.Lock()
	genB1 := slotB.ms
	slotB.mu.Unlock()
	if genB1 == nil {
		t.Fatal("slot B has no generation")
	}

	// Caller W targets Slot 1 and blocks in waitForDispatchSlot because
	// EffectiveMaxInflightRequests() == 1 on both slots.
	w.nextSlot.Store(1)
	doneW := make(chan AppendOutcome, 1)
	go func() { doneW <- w.Append(ctx, [][]byte{row}) }()

	deadline := time.Now().Add(2 * time.Second)
	for {
		slotB.mu.Lock()
		waiting := genB1.slotSignal != nil
		slotB.mu.Unlock()
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Caller W did not block in waitForDispatchSlot on Slot 1")
		}
		time.Sleep(time.Millisecond)
	}

	// Advance past RecoveryInterval and complete rA1 on Slot 0 cleanly while rB1
	// on Slot 1 remains pending. The clean ack on Slot 0 raises
	// EffectiveInflightRequests() to 2 and must wake Caller W on Slot 1.
	advance(20 * time.Millisecond)
	completeA1()

	select {
	case outW := <-doneW:
		if outW.Err != nil || outW.AcknowledgedRows != 1 {
			t.Fatalf("Caller W outcome = %+v, want 1 acknowledged row", outW)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Caller W on Slot 1 was not woken by backpressure recovery on Slot 0")
	}

	completeB1()
	if outA1 := <-doneA1; outA1.Err != nil || outA1.AcknowledgedRows != 1 {
		t.Fatalf("Caller A1 outcome = %+v, want 1 acknowledged row", outA1)
	}
	if outB1 := <-doneB1; outB1.Err != nil || outB1.AcknowledgedRows != 1 {
		t.Fatalf("Caller B1 outcome = %+v, want 1 acknowledged row", outB1)
	}
}

func TestWriterStreamPoolReleasesBytesOnSyncAppendRowsError(t *testing.T) {
	var calls atomic.Int32
	w := &Writer{
		opts: WriterOptions{
			Project:             "p",
			Dataset:             "d",
			Table:               "t",
			MaxRequestBytes:     250,
			MaxInflightRequests: 2,
			MaxInflightBytes:    221, // exact wire bytes for one 100-byte row
			StreamPoolSize:      2,
		},
		lifetimeCtx: context.Background(),
	}
	w.factory = func(context.Context, ...managedwriter.WriterOption) (stream, error) {
		return &funcStream{
			appendFn: func(context.Context, [][]byte) (appendResult, error) {
				if calls.Add(1) == 1 {
					return nil, status.Error(codes.Unavailable, "sync append failure")
				}
				return fakeResult{}, nil
			},
		}, nil
	}
	defer func() { _ = w.Close() }()

	row := make([]byte, 100)
	first := w.Append(context.Background(), [][]byte{row})
	if status.Code(first.Err) != codes.Unavailable {
		t.Fatalf("first Append error = %v, want Unavailable", first.Err)
	}

	w.mu.Lock()
	inflight := w.inflightBytes
	w.mu.Unlock()
	if inflight != 0 {
		t.Fatalf("inflightBytes after sync AppendRows error = %d, want 0", inflight)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	second := w.Append(ctx, [][]byte{row})
	if second.Err != nil || second.AcknowledgedRows != 1 {
		t.Fatalf("second Append after sync error = %+v, want 1 acknowledged row and nil error", second)
	}
}

func TestWriterStreamPoolSyncCancellationKeepsNativeBytesReservedUntilTeardown(t *testing.T) {
	for _, tc := range []struct {
		name            string
		returnNilErrRes bool
	}{
		{name: "sync_error", returnNilErrRes: false},
		{name: "ambiguous_res_with_canceled_ctx", returnNilErrRes: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			closeEntered := make(chan struct{})
			allowClose := make(chan struct{})
			var (
				streamsMade     atomic.Int32
				slot1Dispatched atomic.Int32
			)

			w := &Writer{
				opts: WriterOptions{
					Project:             "p",
					Dataset:             "d",
					Table:               "t",
					MaxRequestBytes:     250,
					MaxInflightRequests: 2,
					MaxInflightBytes:    221, // exact wire bytes for one 100-byte row
					StreamPoolSize:      2,
				},
				lifetimeCtx: context.Background(),
			}
			w.factory = func(context.Context, ...managedwriter.WriterOption) (stream, error) {
				if streamsMade.Add(1) == 1 {
					var closeOnce sync.Once
					return &funcStream{
						appendFn: func(_ context.Context, _ [][]byte) (appendResult, error) {
							if tc.returnNilErrRes {
								return newControlledResult(), nil
							}
							return nil, context.Canceled
						},
						closeFn: func() error {
							closeOnce.Do(func() { close(closeEntered) })
							<-allowClose
							return nil
						},
					}, nil
				}
				return &funcStream{
					appendFn: func(context.Context, [][]byte) (appendResult, error) {
						slot1Dispatched.Add(1)
						return fakeResult{}, nil
					},
				}, nil
			}
			defer func() { _ = w.Close() }()

			// Warm both slots so slot 1 already has a live stream generation before
			// slot 0's synchronous cancellation races teardown.
			slots, err := w.beginAppend()
			if err != nil {
				t.Fatal(err)
			}
			w.appendWG.Done()
			for i, s := range slots {
				gen, serr := w.ensureSlotStream(context.Background(), s)
				if serr != nil {
					t.Fatalf("warming slot %d: %v", i, serr)
				}
				w.releaseGenerationUser(s, gen)
			}

			row := make([]byte, 100)
			ctx0, cancel0 := context.WithCancel(context.Background())
			cancel0()
			// Create a child context that is still live during pre-dispatch acquisition
			// and canceled inside AppendRows.
			ctx0Live, cancel0Live := context.WithCancel(context.Background())
			defer cancel0Live()
			slots[0].mu.Lock()
			slot0Stream := slots[0].ms.stream.(*funcStream)
			origAppend := slot0Stream.appendFn
			slot0Stream.appendFn = func(_ context.Context, data [][]byte) (appendResult, error) {
				cancel0Live()
				return origAppend(ctx0, data)
			}
			slots[0].mu.Unlock()

			w.nextSlot.Store(0)
			done0 := make(chan AppendOutcome, 1)
			go func() {
				done0 <- w.Append(ctx0Live, [][]byte{row})
			}()

			select {
			case <-closeEntered:
			case <-time.After(2 * time.Second):
				t.Fatal("slot 0 Close() was not entered during synchronous failure retirement")
			}

			// While slot 0's stream Close() is blocked in teardown, the 221-byte
			// reservation must remain held in w.inflightBytes.
			w.mu.Lock()
			heldBytes := w.inflightBytes
			w.mu.Unlock()
			if heldBytes != 221 {
				close(allowClose)
				t.Fatalf("inflightBytes during slot 0 Close() teardown = %d, want 221", heldBytes)
			}

			// Launch a concurrent append on slot 1; it must wait on the pool-wide
			// byte gate while slot 0's teardown is still in progress.
			w.nextSlot.Store(1)
			done1 := make(chan AppendOutcome, 1)
			go func() {
				ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel1()
				done1 <- w.Append(ctx1, [][]byte{row})
			}()

			deadline := time.Now().Add(2 * time.Second)
			for {
				w.mu.Lock()
				waitingOnBytes := w.byteSignal != nil
				w.mu.Unlock()
				if waitingOnBytes {
					break
				}
				if time.Now().After(deadline) {
					close(allowClose)
					t.Fatal("slot 1 did not block on w.byteSignal while slot 0 teardown was in progress")
				}
				time.Sleep(time.Millisecond)
			}
			if got := slot1Dispatched.Load(); got != 0 {
				close(allowClose)
				t.Fatalf("slot 1 dispatched %d requests before slot 0 Close() completed, want 0", got)
			}

			close(allowClose)

			select {
			case out0 := <-done0:
				if !errors.Is(out0.Err, context.Canceled) {
					t.Fatalf("slot 0 outcome = %+v, want context.Canceled", out0)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("slot 0 Append did not return after Close() completed")
			}

			select {
			case out1 := <-done1:
				if out1.Err != nil || out1.AcknowledgedRows != 1 {
					t.Fatalf("slot 1 outcome = %+v, want 1 acknowledged row and nil error", out1)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("slot 1 Append did not complete after slot 0 released bytes")
			}

			w.mu.Lock()
			finalBytes := w.inflightBytes
			w.mu.Unlock()
			if finalBytes != 0 {
				t.Fatalf("final inflightBytes = %d, want 0", finalBytes)
			}
		})
	}
}

func TestWriterStreamPoolSimplexDelayedResultOwnerDoesNotRetireSiblings(t *testing.T) {
	const poolSize = 4
	results := make([]*controlledResult, poolSize)
	var (
		completeOnce [poolSize]sync.Once
		closed       [poolSize]atomic.Int32
	)
	for i := range poolSize {
		results[i] = newControlledResult()
	}
	completeResult := func(i int, err error) {
		completeOnce[i].Do(func() {
			results[i].complete(err)
		})
	}

	obs := &fakeObserver{}
	var streamsMade atomic.Int32
	w := &Writer{
		opts: WriterOptions{
			Project:             "p",
			Dataset:             "d",
			Table:               "t",
			MaxRequestBytes:     250,
			MaxInflightRequests: 2,
			MaxInflightBytes:    1 << 20,
			StreamPoolSize:      poolSize,
			Observer:            obs,
		},
		lifetimeCtx: context.Background(),
	}
	w.factory = func(context.Context, ...managedwriter.WriterOption) (stream, error) {
		idx := int(streamsMade.Add(1) - 1)
		if idx >= poolSize {
			return &funcStream{
				appendFn: func(context.Context, [][]byte) (appendResult, error) {
					return fakeResult{}, nil
				},
			}, nil
		}
		return &funcStream{
			appendFn: func(context.Context, [][]byte) (appendResult, error) {
				return results[idx], nil
			},
			closeFn: func() error {
				closed[idx].Add(1)
				// Simulate a result-owner goroutine that takes 20ms (> 5ms) after
				// stream Close() to observe the closed connection and resolve.
				go func(slotIdx int) {
					time.Sleep(20 * time.Millisecond)
					completeResult(slotIdx, status.Error(codes.Canceled, "simplex stream closed"))
				}(idx)
				return nil
			},
		}, nil
	}
	defer func() { _ = w.Close() }()

	slots, err := w.beginAppend()
	if err != nil {
		t.Fatal(err)
	}
	w.appendWG.Done()
	for i, s := range slots {
		gen, serr := w.ensureSlotStream(context.Background(), s)
		if serr != nil {
			t.Fatalf("warming slot %d: %v", i, serr)
		}
		w.releaseGenerationUser(s, gen)
	}

	row := make([]byte, 60)
	siblingDone := make([]chan AppendOutcome, poolSize)
	for i := 1; i < poolSize; i++ {
		siblingDone[i] = make(chan AppendOutcome, 1)
		w.nextSlot.Store(uint64(i))
		go func(slotIdx int) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			siblingDone[slotIdx] <- w.Append(ctx, [][]byte{row})
		}(i)
		results[i].waitForCalls(t, 1)
	}

	// Slot 0 times out after 30ms; its Close() takes 20ms to resolve results[0].
	// Because this is a non-multiplexed pool, slots 1..3 must not be retired.
	w.nextSlot.Store(0)
	ctx0, cancel0 := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel0()
	out0 := w.Append(ctx0, [][]byte{row})
	if !errors.Is(out0.Err, context.DeadlineExceeded) {
		t.Fatalf("slot 0 outcome = %+v, want context.DeadlineExceeded", out0)
	}

	if got := closed[0].Load(); got != 1 {
		t.Fatalf("slot 0 close count = %d, want 1", got)
	}
	for i := 1; i < poolSize; i++ {
		if got := closed[i].Load(); got != 0 {
			t.Fatalf("healthy sibling slot %d close count = %d, want 0", i, got)
		}
		completeResult(i, nil)
		select {
		case out := <-siblingDone[i]:
			if out.Err != nil || out.AcknowledgedRows != 1 {
				t.Fatalf("sibling slot %d outcome = %+v, want 1 acknowledged row and nil error", i, out)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("sibling slot %d did not complete", i)
		}
	}
	obs.mu.Lock()
	retirements := obs.retirements
	obs.mu.Unlock()
	if retirements != 1 {
		t.Fatalf("stream retirements = %d, want 1 (only the timed-out slot)", retirements)
	}
}

func TestWriterStreamPoolOverlapRetirementKeepsReservation(t *testing.T) {
	for _, tc := range []struct {
		name           string
		cancelSendCtx  bool
		waitSendReturn bool
	}{
		{name: "canceled_append_returns_while_teardown_held", cancelSendCtx: true, waitSendReturn: true},
		{name: "non_canceled_append_joins_teardown", cancelSendCtx: false, waitSendReturn: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			appendEntered := make(chan struct{})
			releaseAppend := make(chan struct{})
			closeEntered := make(chan struct{})
			allowClose := make(chan struct{})

			var (
				streamsMade     atomic.Int32
				nativeBytes     atomic.Int64
				peakNativeBytes atomic.Int64
				slot1Dispatched atomic.Int32
			)
			recordNative := func(delta int64) {
				cur := nativeBytes.Add(delta)
				for {
					prev := peakNativeBytes.Load()
					if cur <= prev || peakNativeBytes.CompareAndSwap(prev, cur) {
						break
					}
				}
			}

			w := &Writer{
				opts: WriterOptions{
					Project:             "p",
					Dataset:             "d",
					Table:               "t",
					MaxRequestBytes:     200,
					MaxInflightRequests: 2,
					MaxInflightBytes:    221, // exact wire bytes for one 100-byte row
					StreamPoolSize:      2,
				},
				lifetimeCtx: context.Background(),
			}
			w.factory = func(context.Context, ...managedwriter.WriterOption) (stream, error) {
				if streamsMade.Add(1) == 1 {
					var (
						appendOnce sync.Once
						closeOnce  sync.Once
					)
					return &funcStream{
						appendFn: func(ctx context.Context, _ [][]byte) (appendResult, error) {
							recordNative(221)
							appendOnce.Do(func() { close(appendEntered) })
							<-releaseAppend
							if tc.cancelSendCtx {
								return nil, ctx.Err()
							}
							return nil, status.Error(codes.Canceled, "stream closing")
						},
						closeFn: func() error {
							closeOnce.Do(func() { close(closeEntered) })
							<-allowClose
							recordNative(-221)
							return nil
						},
					}, nil
				}
				return &funcStream{
					appendFn: func(context.Context, [][]byte) (appendResult, error) {
						slot1Dispatched.Add(1)
						recordNative(221)
						recordNative(-221)
						return fakeResult{}, nil
					},
				}, nil
			}
			defer func() { _ = w.Close() }()

			slots, err := w.beginAppend()
			if err != nil {
				t.Fatal(err)
			}
			w.appendWG.Done()
			for i, s := range slots {
				gen, serr := w.ensureSlotStream(context.Background(), s)
				if serr != nil {
					t.Fatalf("warming slot %d: %v", i, serr)
				}
				w.releaseGenerationUser(s, gen)
			}

			slots[0].mu.Lock()
			gen0 := slots[0].ms
			slots[0].mu.Unlock()

			row := make([]byte, 100)
			sendCtx, cancelSend := context.WithCancel(context.Background())
			defer cancelSend()

			w.nextSlot.Store(0)
			sendDone := make(chan AppendOutcome, 1)
			go func() {
				sendDone <- w.Append(sendCtx, [][]byte{row})
			}()

			select {
			case <-appendEntered:
			case <-time.After(2 * time.Second):
				t.Fatal("slot 0 AppendRows was not entered")
			}

			// Start a concurrent retirement on slot 0 while the send is still
			// inside AppendRows, and wait until gen0.Close() is entered and held.
			retireDone := make(chan struct{})
			go func() {
				defer close(retireDone)
				w.retireStream(slots[0], gen0)
			}()

			select {
			case <-closeEntered:
			case <-time.After(2 * time.Second):
				t.Fatal("concurrent retirement did not enter Close()")
			}

			// Now fail the in-flight AppendRows while Close() is still held.
			if tc.cancelSendCtx {
				cancelSend()
			}
			close(releaseAppend)

			if tc.waitSendReturn {
				select {
				case out0 := <-sendDone:
					if !errors.Is(out0.Err, context.Canceled) {
						close(allowClose)
						t.Fatalf("canceled slot 0 outcome = %+v, want context.Canceled", out0)
					}
				case <-time.After(2 * time.Second):
					close(allowClose)
					t.Fatal("canceled Append did not return while teardown was held")
				}
			}

			// Even after the overlapping AppendRows has returned while Close()
			// is still held, the 221-byte reservation must remain held until
			// Close() finishes so a sibling slot cannot raise native residency
			// above MaxInflightBytes (221).
			w.mu.Lock()
			heldBytes := w.inflightBytes
			w.mu.Unlock()
			if heldBytes != 221 {
				close(allowClose)
				t.Fatalf("inflightBytes while overlapping Close() is held = %d, want 221", heldBytes)
			}

			w.nextSlot.Store(1)
			slot1Done := make(chan AppendOutcome, 1)
			go func() {
				ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel1()
				slot1Done <- w.Append(ctx1, [][]byte{row})
			}()

			deadline := time.Now().Add(2 * time.Second)
			for {
				w.mu.Lock()
				waitingOnBytes := w.byteSignal != nil
				w.mu.Unlock()
				if waitingOnBytes {
					break
				}
				if time.Now().After(deadline) {
					close(allowClose)
					t.Fatal("slot 1 did not block on w.byteSignal while overlapping Close() was held")
				}
				time.Sleep(time.Millisecond)
			}
			if got := slot1Dispatched.Load(); got != 0 {
				close(allowClose)
				t.Fatalf("slot 1 dispatched %d requests while Close() was held, want 0", got)
			}
			if peak := peakNativeBytes.Load(); peak > 221 {
				close(allowClose)
				t.Fatalf("peak modeled native residency while Close() was held = %d, want <= 221", peak)
			}

			// Release Close(); deferred bytes must now be refunded and wake slot 1.
			close(allowClose)
			<-retireDone

			if !tc.waitSendReturn {
				select {
				case out0 := <-sendDone:
					if out0.Err == nil {
						t.Fatalf("overlapping slot 0 outcome = %+v, want non-nil error", out0)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("overlapping slot 0 Append did not finish after Close() completed")
				}
			}

			select {
			case out1 := <-slot1Done:
				if out1.Err != nil || out1.AcknowledgedRows != 1 {
					t.Fatalf("slot 1 outcome = %+v, want 1 acknowledged row and nil error", out1)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("slot 1 Append did not complete after overlapping Close() finished")
			}

			if peak := peakNativeBytes.Load(); peak > 221 {
				t.Fatalf("peak modeled native residency = %d, want <= 221", peak)
			}
			w.mu.Lock()
			finalBytes := w.inflightBytes
			w.mu.Unlock()
			if finalBytes != 0 {
				t.Fatalf("final inflightBytes = %d, want 0", finalBytes)
			}
		})
	}
}
