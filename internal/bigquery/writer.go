// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

// Package bigquery wraps the Storage Write API managedwriter client with the
// stream management, request sizing and error classification the exporter
// needs (§8.1–§8.3).
package bigquery // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/bigquery"

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/descriptorpb"
)

// WriterOptions configures one destination stream.
type WriterOptions struct {
	Project string
	Dataset string
	Table   string

	// Descriptor describes the row message, derived from the destination
	// table's schema.
	Descriptor *descriptorpb.DescriptorProto

	// MaxRequestBytes is our own headroom threshold beneath the API's
	// per-request limit. Requests are split to stay under it.
	MaxRequestBytes int

	// MaxInflightRequests and MaxInflightBytes bound asynchronous appends.
	// managedwriter enforces the native limits; the exporter mirrors the
	// request-count window so concurrent calls drain results before they can
	// collectively block inside AppendRows.
	MaxInflightRequests int
	MaxInflightBytes    int

	// TraceID identifies this client to the Write API backend, which makes
	// producer-side attribution possible when the service team is debugging
	// on our behalf.
	TraceID string

	// Observer receives bounded, content-free stream lifecycle telemetry.
	Observer Observer
}

// Observer is the delivery-critical stream telemetry surface. The exporter
// implementation records only counts and durations; no payload-derived value
// crosses this boundary.
type Observer interface {
	RecordInflightRequests(context.Context, int64)
	RecordUnresolvedResults(context.Context, int64)
	RecordAppendResultWait(context.Context, time.Duration)
	RecordTimeoutAfterDispatch(context.Context)
	RecordStreamRetirement(context.Context)
}

// TableRef renders the fully qualified destination table path.
func (o WriterOptions) TableRef() string {
	return managedwriter.TableParentFromParts(o.Project, o.Dataset, o.Table)
}

// TableID renders the BigQuery-style project.dataset.table identifier used in
// diagnostics.
func (o WriterOptions) TableID() string {
	return fmt.Sprintf("%s.%s.%s", o.Project, o.Dataset, o.Table)
}

// streamFactory creates a managed stream. Swapped out in tests.
type streamFactory func(ctx context.Context, opts ...managedwriter.WriterOption) (stream, error)

// stream is the slice of ManagedStream the writer depends on.
type stream interface {
	AppendRows(ctx context.Context, data [][]byte, opts ...managedwriter.AppendOption) (appendResult, error)
	Close() error
}

// appendResult is the slice of AppendResult the writer depends on.
type appendResult interface {
	GetResult(ctx context.Context) (int64, error)
	FullResponse(ctx context.Context) (*storagepb.AppendRowsResponse, error)
}

// Writer owns one default-stream ManagedStream per destination table.
type Writer struct {
	opts        WriterOptions
	factory     streamFactory
	lifetimeCtx context.Context
	ownedCtx    context.Context
	ownedCancel context.CancelFunc

	mu                sync.Mutex
	ms                *streamGeneration
	creation          *streamCreation
	generationSignal  chan struct{}
	activeGenerations int
	closed            bool
	closeDone         chan struct{}
	closeErr          error

	appendWG      sync.WaitGroup
	constructorWG sync.WaitGroup
	drainWG       sync.WaitGroup
}

const maxActiveStreamGenerations = 2

var (
	errStreamRetired = errors.New("write stream generation is retired")
	errWriterClosed  = errors.New("writer is closed")
)

type streamCreation struct {
	done chan struct{}
	err  error
}

// streamGeneration binds an append result to the exact stream that dispatched
// it. Pointer identity is the generation token: a late failure can retire this
// generation only while it is still the writer's current stream.
type streamGeneration struct {
	stream
	users         int
	pending       int
	retired       bool
	slotHeld      bool
	dispatchSlots chan struct{}
	retiredDone   chan struct{}
}

// NewWriter builds a writer whose managed streams retain lifetimeCtx for their
// entire component lifetime.
func NewWriter(
	lifetimeCtx context.Context, client *managedwriter.Client, opts WriterOptions,
) *Writer {
	return &Writer{
		opts:        opts,
		lifetimeCtx: lifetimeCtx,
		factory: func(ctx context.Context, wopts ...managedwriter.WriterOption) (stream, error) {
			ms, err := client.NewManagedStream(ctx, wopts...)
			if err != nil {
				return nil, err
			}
			return realStream{ms}, nil
		},
	}
}

// writerOptions renders the managedwriter configuration for this destination.
func (w *Writer) writerOptions() []managedwriter.WriterOption {
	return []managedwriter.WriterOption{
		// The default stream is offset-free: at-least-once, no connection or
		// offset bookkeeping.
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithDestinationTable(w.opts.TableRef()),
		managedwriter.WithSchemaDescriptor(w.opts.Descriptor),
		managedwriter.WithMaxInflightRequests(w.opts.MaxInflightRequests),
		managedwriter.WithMaxInflightBytes(w.opts.MaxInflightBytes),
		managedwriter.WithTraceID(w.opts.TraceID),

		// Set explicitly even though write retries are already off by
		// default: retry ownership is a contract and must not depend on a
		// library default. managedwriter owns connection recovery only;
		// exporterhelper owns append replay. Two retriers would multiply
		// attempts and break the duplicate bound.
		managedwriter.EnableWriteRetries(false),
	}
}

// ensureStream returns the live stream generation, creating it on first use.
//
// managedwriter retains the stream-construction context. It must therefore be
// the component-owned lifetime context, never an exporterhelper attempt
// context that is canceled as soon as one Consume call returns.
func (w *Writer) ensureStream(ctx context.Context) (*streamGeneration, error) {
	for {
		w.mu.Lock()
		if w.closed {
			w.mu.Unlock()
			return nil, errWriterClosed
		}
		if err := ctx.Err(); err != nil {
			w.mu.Unlock()
			return nil, err
		}
		if w.ms != nil {
			gen := w.ms
			gen.users++
			w.mu.Unlock()
			return gen, nil
		}

		if w.creation == nil && w.activeGenerations < maxActiveStreamGenerations {
			w.startStreamCreationLocked()
		}
		if creation := w.creation; creation != nil {
			done := creation.done
			w.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-done:
				if creation.err != nil {
					return nil, fmt.Errorf("creating write stream for %s: %w", w.opts.TableID(), creation.err)
				}
				continue
			}
		}

		signal := w.generationSignalLocked()
		w.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-signal:
		}
	}
}

func (w *Writer) startStreamCreationLocked() {
	creation := &streamCreation{done: make(chan struct{})}
	w.creation = creation
	w.activeGenerations++
	ctx := w.ownedContextLocked()
	w.constructorWG.Add(1)
	go func() {
		defer w.constructorWG.Done()
		ms, err := w.factory(ctx, w.writerOptions()...)
		var discard stream

		w.mu.Lock()
		switch {
		case err != nil:
			creation.err = err
			w.activeGenerations--
		case w.closed || ctx.Err() != nil:
			discard = ms
			creation.err = errWriterClosed
			if ctxErr := ctx.Err(); ctxErr != nil {
				creation.err = ctxErr
			}
			w.activeGenerations--
		default:
			window := w.opts.MaxInflightRequests
			if window <= 0 {
				window = 1
			}
			w.ms = &streamGeneration{
				stream:        ms,
				slotHeld:      true,
				dispatchSlots: make(chan struct{}, window),
				retiredDone:   make(chan struct{}),
			}
		}
		if w.creation == creation {
			w.creation = nil
		}
		close(creation.done)
		w.notifyGenerationChangeLocked()
		w.mu.Unlock()

		if discard != nil {
			_ = discard.Close()
		}
	}()
}

func (w *Writer) ownedContextLocked() context.Context {
	if w.ownedCtx == nil {
		parent := w.lifetimeCtx
		if parent == nil {
			parent = context.Background()
		}
		w.ownedCtx, w.ownedCancel = context.WithCancel(parent)
	}
	return w.ownedCtx
}

func (w *Writer) generationSignalLocked() <-chan struct{} {
	if w.generationSignal == nil {
		w.generationSignal = make(chan struct{})
	}
	return w.generationSignal
}

func (w *Writer) notifyGenerationChangeLocked() {
	if w.generationSignal != nil {
		close(w.generationSignal)
		w.generationSignal = nil
	}
}

// beginAppend prevents shutdown from starting a Wait while a new append can
// still register work. The WaitGroup Add happens under the same lock that
// closes the writer, so Close's Wait cannot race an Add from a new call.
func (w *Writer) beginAppend() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errWriterClosed
	}
	w.ownedContextLocked()
	w.appendWG.Add(1)
	return nil
}

// trackResult keeps terminal ownership after an attempt context expires.
// managedwriter's GetResult only reports that caller cancellation; it does not
// make the underlying AppendResult terminal. This waiter lives until the
// result resolves or Writer.Close cancels the component lifecycle context.
func (w *Writer) trackResult(gen *streamGeneration, res appendResult, started time.Time) {
	w.drainWG.Add(1)
	w.mu.Lock()
	ctx := w.ownedCtx
	w.mu.Unlock()
	w.observeUnresolved(1)
	go func() {
		defer func() {
			w.observeInflight(-1)
			w.observeUnresolved(-1)
			w.observeResultWait(started)
			w.releaseResult(gen)
			w.drainWG.Done()
		}()
		_, _ = res.GetResult(ctx)
	}()
}

// retireStream tears down gen only if it is still current. Another append may
// already have retired gen and installed a replacement; a late result from the
// old generation must not close that replacement.
func (w *Writer) retireStream(gen *streamGeneration) {
	w.mu.Lock()
	if w.ms != gen {
		w.mu.Unlock()
		return
	}
	w.ms = nil
	w.markGenerationRetiredLocked(gen)
	w.releaseGenerationSlotLocked(gen)
	w.notifyGenerationChangeLocked()
	w.mu.Unlock()
	_ = gen.Close()
	if w.opts.Observer != nil {
		w.opts.Observer.RecordStreamRetirement(context.Background())
	}
}

func (w *Writer) markGenerationRetiredLocked(gen *streamGeneration) {
	if gen.retired {
		return
	}
	gen.retired = true
	close(gen.retiredDone)
}

func (w *Writer) tryAcquireDispatchSlot(
	ctx context.Context, gen *streamGeneration,
) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	select {
	case <-gen.retiredDone:
		return false, errStreamRetired
	default:
	}
	select {
	case gen.dispatchSlots <- struct{}{}:
		if err := ctx.Err(); err != nil {
			w.releaseDispatchSlot(gen)
			return false, err
		}
		select {
		case <-gen.retiredDone:
			w.releaseDispatchSlot(gen)
			return false, errStreamRetired
		default:
			return true, nil
		}
	default:
		return false, nil
	}
}

func (w *Writer) waitForDispatchSlot(ctx context.Context, gen *streamGeneration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case gen.dispatchSlots <- struct{}{}:
		if err := ctx.Err(); err != nil {
			w.releaseDispatchSlot(gen)
			return err
		}
		select {
		case <-gen.retiredDone:
			w.releaseDispatchSlot(gen)
			return errStreamRetired
		default:
			return nil
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-gen.retiredDone:
		return errStreamRetired
	}
}

func (w *Writer) releaseDispatchSlot(gen *streamGeneration) {
	<-gen.dispatchSlots
}

func (w *Writer) registerResult(gen *streamGeneration) {
	w.mu.Lock()
	gen.pending++
	w.mu.Unlock()
}

func (w *Writer) releaseResult(gen *streamGeneration) {
	w.mu.Lock()
	gen.pending--
	w.releaseGenerationSlotLocked(gen)
	w.mu.Unlock()
	w.releaseDispatchSlot(gen)
}

func (w *Writer) releaseGenerationUser(gen *streamGeneration) {
	w.mu.Lock()
	gen.users--
	w.releaseGenerationSlotLocked(gen)
	w.mu.Unlock()
}

func (w *Writer) releaseGenerationSlotLocked(gen *streamGeneration) {
	if !gen.slotHeld || !gen.retired || gen.users != 0 || gen.pending != 0 {
		return
	}
	gen.slotHeld = false
	w.activeGenerations--
	w.notifyGenerationChangeLocked()
}

func (w *Writer) observeInflight(delta int64) {
	if w.opts.Observer != nil {
		w.opts.Observer.RecordInflightRequests(context.Background(), delta)
	}
}

func (w *Writer) observeUnresolved(delta int64) {
	if w.opts.Observer != nil {
		w.opts.Observer.RecordUnresolvedResults(context.Background(), delta)
	}
}

func (w *Writer) observeResultWait(started time.Time) {
	if w.opts.Observer != nil {
		w.opts.Observer.RecordAppendResultWait(context.Background(), time.Since(started))
	}
}

func (w *Writer) observeTimeoutAfterDispatch() {
	if w.opts.Observer != nil {
		w.opts.Observer.RecordTimeoutAfterDispatch(context.Background())
	}
}

func (w *Writer) finishResult(gen *streamGeneration, started time.Time) {
	w.observeInflight(-1)
	w.observeResultWait(started)
	w.releaseResult(gen)
}

func (w *Writer) classifyAndRetire(
	ctx context.Context, err error, gen *streamGeneration,
) (Verdict, bool) {
	verdict, attemptEnded := w.classifyAppendFailure(ctx, err)
	if !attemptEnded && verdict.Label != LabelShutdown && isTeardown(err) && w.isRetired(gen) {
		// Another attempt retired this generation and closed the connection
		// under this request. It may already have reached the service, so the
		// outcome is as ambiguous as an expired attempt's and its replay can
		// duplicate.
		verdict = Verdict{Uncertain, OwnerExporterHelper, LabelUncertainAck, false}
	}
	if verdict.StreamRecreate || attemptEnded || verdict.Class == Uncertain {
		w.retireStream(gen)
	}
	if attemptEnded {
		w.observeTimeoutAfterDispatch()
	}
	return verdict, attemptEnded
}

// deliveryRank orders verdict classes by what a batch still owes its rows:
// uncertain rows may already be applied and are replayed with duplicate
// accounting, retryable rows certainly were not and are replayed, and only
// permanently rejected rows may be dropped.
func deliveryRank(c Class) int {
	switch c {
	case Uncertain:
		return 2
	case Retryable:
		return 1
	default:
		return 0
	}
}

func (w *Writer) isRetired(gen *streamGeneration) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return gen.retired
}

func classifyPreDispatchFailure(err error) Verdict {
	switch {
	case errors.Is(err, errWriterClosed):
		// Nothing was sent. The batch goes back to exporterhelper: a permanent
		// verdict would drop telemetry the persistent queue could keep.
		return Verdict{Retryable, OwnerExporterHelper, LabelShutdown, false}
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return Verdict{Retryable, OwnerExporterHelper, LabelCanceled, false}
	case errors.Is(err, errStreamRetired):
		return Verdict{Retryable, OwnerExporterHelper, LabelInvalidStream, false}
	default:
		return Classify(err)
	}
}

// RowError identifies a row the service rejected within an append.
type RowError struct {
	// Index is the row's position within the request that carried it.
	Index int64
	// Code is the service's row-error code, a bounded enum.
	Code string
	// Message is the service's description. It can echo field names but not
	// values; callers must not log it verbatim without sanitizing.
	Message string
}

// AppendOutcome reports what happened to one Append call.
type AppendOutcome struct {
	// AcknowledgedRows counts rows the service confirmed.
	AcknowledgedRows int
	// RowErrors lists rows the service rejected, indexed within the batch
	// passed to Append.
	RowErrors []RowError
	// Verdict classifies the batch's transport-level failures, if any. With
	// several requests it is the failure that still owes its rows the most:
	// uncertain over retryable over permanent.
	Verdict Verdict
	// Err is the first transport-level error, kept for diagnosis.
	Err error
}

// Append splits rows into requests under the size threshold and sends them,
// then waits for every acknowledgement.
//
// Requests are dispatched up to the configured flow-control window before a
// result is awaited. Rows that do not fit any request are reported as row
// errors rather than failing the batch.
func (w *Writer) Append(ctx context.Context, rows [][]byte) AppendOutcome {
	if len(rows) == 0 {
		return AppendOutcome{}
	}
	if err := w.beginAppend(); err != nil {
		return AppendOutcome{Verdict: classifyPreDispatchFailure(err), Err: err}
	}
	defer w.appendWG.Done()

	gen, err := w.ensureStream(ctx)
	if err != nil {
		return AppendOutcome{Verdict: classifyPreDispatchFailure(err), Err: err}
	}
	defer w.releaseGenerationUser(gen)

	requests := SplitRequests(rows, w.opts.MaxRequestBytes)

	type pending struct {
		res        appendResult
		start      int
		count      int
		dispatched time.Time
	}
	inflight := make([]pending, 0, len(requests))
	out := AppendOutcome{}
	// fail folds one request's failure into the batch. Err keeps the first
	// failure for diagnosis, but the verdict follows delivery precedence
	// across every request: the exporter drops all rows of a permanent batch,
	// so one request's permanent rejection must not decide for rows another
	// request still owes.
	fail := func(v Verdict, err error) {
		if out.Err == nil {
			out.Err = err
			out.Verdict = v
			return
		}
		if deliveryRank(v.Class) > deliveryRank(out.Verdict.Class) {
			out.Verdict = v
		}
	}
	// Once the batch must be replayed anyway, dispatching more of it would
	// only send those rows twice. Until then, a permanent rejection of one
	// request does not keep the others from being sent.
	replayOwed := func() bool {
		return out.Err != nil && out.Verdict.Class != Permanent
	}
	drainOne := func(p pending) {
		if _, rerr := p.res.GetResult(ctx); rerr != nil {
			// Row errors arrive alongside the error; the whole request is not
			// appended when any row in it fails.
			rowErrs := extractRowErrors(ctx, p.res, p.start)
			if len(rowErrs) > 0 {
				w.finishResult(gen, p.dispatched)
				out.RowErrors = append(out.RowErrors, rowErrs...)
				return
			}
			v, attemptEnded := w.classifyAndRetire(ctx, rerr, gen)
			if attemptEnded {
				w.trackResult(gen, p.res, p.dispatched)
			} else {
				w.finishResult(gen, p.dispatched)
			}
			// Keep draining all other results already dispatched by this call.
			fail(v, rerr)
			return
		}
		w.finishResult(gen, p.dispatched)
		out.AcknowledgedRows += p.count
	}

	offset := 0
dispatch:
	for _, req := range requests {
		start := offset
		offset += len(req)
		for {
			if replayOwed() {
				break dispatch
			}
			acquired, acquireErr := w.tryAcquireDispatchSlot(ctx, gen)
			if acquireErr != nil {
				fail(classifyPreDispatchFailure(acquireErr), acquireErr)
				break dispatch
			}
			if acquired {
				break
			}
			if len(inflight) > 0 {
				drainOne(inflight[0])
				inflight = inflight[1:]
				continue
			}
			if acquireErr := w.waitForDispatchSlot(ctx, gen); acquireErr != nil {
				fail(classifyPreDispatchFailure(acquireErr), acquireErr)
				break dispatch
			}
			break
		}
		dispatched := time.Now()
		res, aerr := gen.AppendRows(ctx, req)
		if aerr == nil {
			// managedwriter's AppendRows can report success for a request it
			// never sent when the call races the end of the attempt or the
			// stream's closure, and nothing ever resolves that AppendResult.
			// Tracking it would pin this generation's window slot forever, so
			// such a success is as ambiguous as the attempt error itself.
			if err := ctx.Err(); err != nil {
				aerr = err
			} else if w.isRetired(gen) {
				aerr = errStreamRetired
			}
		}
		if aerr != nil {
			w.releaseDispatchSlot(gen)
			// AppendRows can return the attempt-context error after handing a
			// request to the bidirectional stream. Its acknowledgement is then
			// ambiguous, and this generation cannot safely be reused.
			v, _ := w.classifyAndRetire(ctx, aerr, gen)
			fail(v, aerr)
			continue
		}
		w.registerResult(gen)
		w.observeInflight(1)
		inflight = append(inflight, pending{
			res: res, start: start, count: len(req), dispatched: dispatched,
		})
	}

	for _, p := range inflight {
		drainOne(p)
	}
	return out
}

// classifyAppendFailure distinguishes an ordinary server failure from the
// attempt context ending after AppendRows was invoked. Cancellation at that
// point has the same delivery ambiguity as a deadline: the server may have
// applied the rows even though the caller did not observe the acknowledgement.
// Once the writer's lifecycle has ended, a teardown failure is shutdown however
// managedwriter reports it.
func (w *Writer) classifyAppendFailure(ctx context.Context, err error) (Verdict, bool) {
	ctxErr := ctx.Err()
	if ctxErr != nil && errors.Is(err, ctxErr) {
		return Verdict{Uncertain, OwnerExporterHelper, LabelUncertainAck, false}, true
	}
	w.mu.Lock()
	ownedCtx := w.ownedCtx
	if ownedCtx == nil {
		ownedCtx = w.lifetimeCtx
	}
	w.mu.Unlock()
	if ownedCtx != nil && ownedCtx.Err() != nil && isTeardown(err) {
		return Verdict{Retryable, OwnerExporterHelper, LabelShutdown, false}, false
	}
	return Classify(err), false
}

// isTeardown reports whether err is how managedwriter fails a request whose
// stream or connection was closed under it, rather than a status the service
// returned: a cancellation, as a context error or a gRPC status; the io.EOF it
// records for a closed stream; or a plain error from its writer bookkeeping.
func isTeardown(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
		return true
	}
	s, ok := status.FromError(err)
	return !ok || s.Code() == codes.Canceled
}

// extractRowErrors pulls per-row failures out of a response, translating
// request-local indices into batch-local ones.
func extractRowErrors(ctx context.Context, res appendResult, base int) []RowError {
	resp, err := res.FullResponse(ctx)
	if err != nil || resp == nil {
		return nil
	}
	var out []RowError
	for _, re := range resp.GetRowErrors() {
		out = append(out, RowError{
			Index:   int64(base) + re.GetIndex(),
			Code:    re.GetCode().String(),
			Message: re.GetMessage(),
		})
	}
	return out
}

// SplitRequests groups serialized rows into requests that stay under the size
// threshold.
//
// The threshold is our own headroom below the API's per-request limit, which
// absorbs the protobuf framing overhead the caller cannot see. A row larger
// than the threshold on its own is emitted as a single-row request: the caller
// is responsible for rejecting oversized rows before this point, and silently
// dropping one here would lose data without a count.
func SplitRequests(rows [][]byte, maxBytes int) [][][]byte {
	if maxBytes <= 0 {
		return [][][]byte{rows}
	}
	var (
		out     [][][]byte
		current [][]byte
		size    int
	)
	for _, row := range rows {
		n := len(row)
		if len(current) > 0 && size+n > maxBytes {
			out = append(out, current)
			current, size = nil, 0
		}
		current = append(current, row)
		size += n
	}
	if len(current) > 0 {
		out = append(out, current)
	}
	return out
}

// Close shuts the stream down. In-flight appends are drained by the caller's
// context deadline before this is called.
func (w *Writer) Close() error {
	w.mu.Lock()
	if w.closed {
		done := w.closeDone
		w.mu.Unlock()
		if done != nil {
			<-done
		}
		w.mu.Lock()
		err := w.closeErr
		w.mu.Unlock()
		return err
	}
	w.closed = true
	w.closeDone = make(chan struct{})
	gen := w.ms
	w.ms = nil
	if gen != nil {
		w.markGenerationRetiredLocked(gen)
		w.releaseGenerationSlotLocked(gen)
	}
	cancel := w.ownedCancel
	w.notifyGenerationChangeLocked()
	w.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	var err error
	if gen != nil {
		err = gen.Close()
	}
	w.constructorWG.Wait()
	// All Append calls must finish registering any asynchronous result owner
	// before cancellation and Wait; this ordering avoids Add/Wait races.
	w.appendWG.Wait()
	w.drainWG.Wait()
	w.mu.Lock()
	w.closeErr = err
	close(w.closeDone)
	w.mu.Unlock()
	return err
}

// realStream adapts *managedwriter.ManagedStream to the stream interface.
type realStream struct{ ms *managedwriter.ManagedStream }

func (r realStream) AppendRows(ctx context.Context, data [][]byte, opts ...managedwriter.AppendOption) (appendResult, error) {
	return r.ms.AppendRows(ctx, data, opts...)
}

// Close reports a stream's expected terminal state as success. managedwriter
// records io.EOF for a stream closed on purpose, and the cancellation of the
// lifecycle context Writer.Close cancels just before closing the stream; a
// clean shutdown is not a failure.
func (r realStream) Close() error {
	err := r.ms.Close()
	if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
