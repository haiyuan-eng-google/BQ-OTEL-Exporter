// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigquery

import (
	"context"
	"errors"
	"reflect"
	"sync"
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
}

// --- fakes ---

type fakeResult struct {
	err  error
	resp *storagepb.AppendRowsResponse
}

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

	inflight    int64
	unresolved  int64
	waits       int
	timeouts    int
	retirements int
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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

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

	v, ended := w.classifyAppendFailure(context.Background(), context.Canceled)
	if ended || v.Label != LabelCanceled {
		t.Fatalf("runtime cancellation = %s ended=%v, want %s/false", v.Label, ended, LabelCanceled)
	}

	cancelLifetime()
	v, ended = w.classifyAppendFailure(context.Background(), context.Canceled)
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
	_ = <-done
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
	if w.ms != nil {
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
