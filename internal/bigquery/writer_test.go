// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigquery

import (
	"context"
	"errors"
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
	recreations int
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

func (o *fakeObserver) RecordStreamRecreation(context.Context) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.recreations++
}

func (o *fakeObserver) snapshot() (inflight, unresolved int64, waits, timeouts, recreations int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.inflight, o.unresolved, o.waits, o.timeouts, o.recreations
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
			Project:         "p",
			Dataset:         "d",
			Table:           "t",
			MaxRequestBytes: 1,
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

	out := w.Append(context.Background(), [][]byte{{1}, {2}})
	if out.Verdict.Class != Retryable {
		t.Fatalf("class = %s, want retryable", out.Verdict.Class)
	}
	if failed.calls() != 1 || succeeded.calls() != 1 {
		t.Fatalf("GetResult calls = failed:%d succeeded:%d, want 1 each",
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

// Once AppendRows returns a result, cancellation makes the acknowledgement
// ambiguous. The affected stream generation must be retired before another
// exporterhelper attempt can reuse it.
func TestAppendRetiresGenerationWhenAttemptEndsAfterDispatch(t *testing.T) {
	for name, cancel := range map[string]func() (context.Context, context.CancelFunc){
		"canceled": func() (context.Context, context.CancelFunc) {
			return context.WithCancel(context.Background())
		},
		"deadline": func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), time.Nanosecond)
		},
	} {
		t.Run(name, func(t *testing.T) {
			pending := newControlledResult()
			first := &scriptedStream{results: []appendResult{pending}}
			second := &scriptedStream{results: []appendResult{fakeResult{}}}
			w, createdWith := newScriptedWriter(first, second)
			defer func() { _ = w.Close() }()

			ctx, stop := cancel()
			stop()
			out := w.Append(ctx, [][]byte{{1}})

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

// GetResult returning the canceled attempt context does not mean the
// managedwriter result itself is terminal. The writer must keep a bounded
// lifecycle owner for that result until it resolves or the writer shuts down.
func TestCanceledAttemptTracksResultToTerminalState(t *testing.T) {
	pending := newControlledResult()
	first := &scriptedStream{results: []appendResult{pending}}
	w, _ := newScriptedWriter(first)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	out := w.Append(ctx, [][]byte{{1}})
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
	cancel()
	out := w.Append(ctx, [][]byte{{1}})
	if out.Verdict.Class != Uncertain {
		t.Fatalf("class = %s, want uncertain", out.Verdict.Class)
	}
	pending.waitForCalls(t, 2)

	inflight, unresolved, _, timeouts, recreations := observer.snapshot()
	if inflight != 1 || unresolved != 1 || timeouts != 1 || recreations != 1 {
		t.Fatalf("during drain: inflight=%d unresolved=%d timeouts=%d recreations=%d, want 1 each",
			inflight, unresolved, timeouts, recreations)
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
	cancels := make([]context.CancelFunc, attempts)
	done := make(chan AppendOutcome, attempts)
	for i := range attempts {
		ctx, cancel := context.WithCancel(context.Background())
		cancels[i] = cancel
		go func() { done <- w.Append(ctx, [][]byte{{1}}) }()
	}
	stuck.waitForCalls(t, attempts)
	for _, cancel := range cancels {
		cancel()
	}
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
	cancel()
	_ = w.Append(ctx, [][]byte{{1}})
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
	opts := w.writerOptions()
	if len(opts) == 0 {
		t.Fatal("expected writer options")
	}
	// The option set is opaque, so assert on the count as a change detector
	// alongside the explicit EnableWriteRetries(false) call in writerOptions.
	if len(opts) != 7 {
		t.Fatalf("writer option count changed to %d; confirm EnableWriteRetries(false) is still set", len(opts))
	}
}
