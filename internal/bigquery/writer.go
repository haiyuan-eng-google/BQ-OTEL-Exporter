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
	"sync"
	"time"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
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
	// Flow control is managedwriter's native limits rather than a bespoke
	// window; the exporter only maps acknowledgements back to batches.
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
	RecordStreamRecreation(context.Context)
}

// TableRef renders the fully qualified destination table path.
func (o WriterOptions) TableRef() string {
	return fmt.Sprintf("projects/%s/datasets/%s/tables/%s", o.Project, o.Dataset, o.Table)
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
	drainCtx    context.Context
	drainCancel context.CancelFunc

	mu     sync.Mutex
	ms     *streamGeneration
	closed bool

	appendWG sync.WaitGroup
	drainWG  sync.WaitGroup
}

// streamGeneration binds an append result to the exact stream that dispatched
// it. Pointer identity is the generation token: a late failure can retire this
// generation only while it is still the writer's current stream.
type streamGeneration struct {
	stream
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
func (w *Writer) ensureStream() (*streamGeneration, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil, errors.New("writer is closed")
	}
	if w.ms != nil {
		return w.ms, nil
	}
	ctx := w.lifetimeCtx
	if ctx == nil {
		ctx = context.Background()
	}
	ms, err := w.factory(ctx, w.writerOptions()...)
	if err != nil {
		return nil, fmt.Errorf("creating write stream for %s: %w", w.opts.TableID(), err)
	}
	gen := &streamGeneration{stream: ms}
	w.ms = gen
	return gen, nil
}

// beginAppend prevents shutdown from starting a Wait while a new append can
// still register work. The WaitGroup Add happens under the same lock that
// closes the writer, so Close's Wait cannot race an Add from a new call.
func (w *Writer) beginAppend() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("writer is closed")
	}
	if w.drainCtx == nil {
		ctx := w.lifetimeCtx
		if ctx == nil {
			ctx = context.Background()
		}
		w.drainCtx, w.drainCancel = context.WithCancel(ctx)
	}
	w.appendWG.Add(1)
	return nil
}

// trackResult keeps terminal ownership after an attempt context expires.
// managedwriter's GetResult only reports that caller cancellation; it does not
// make the underlying AppendResult terminal. This waiter lives until the
// result resolves or Writer.Close cancels the bounded drain context.
func (w *Writer) trackResult(res appendResult, started time.Time) {
	w.drainWG.Add(1)
	ctx := w.drainCtx
	w.observeUnresolved(1)
	go func() {
		defer func() {
			w.observeInflight(-1)
			w.observeUnresolved(-1)
			w.observeResultWait(started)
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
	w.mu.Unlock()
	_ = gen.Close()
	if w.opts.Observer != nil {
		w.opts.Observer.RecordStreamRecreation(context.Background())
	}
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
	// Verdict classifies a transport-level failure, if any.
	Verdict Verdict
	// Err is the transport-level error, if any.
	Err error
}

// Append splits rows into requests under the size threshold and sends them,
// then waits for every acknowledgement.
//
// Requests are dispatched before any result is awaited: waiting inline would
// serialize the path and defeat the point of an asynchronous API. Rows that do
// not fit any request are reported as row errors rather than failing the
// batch.
func (w *Writer) Append(ctx context.Context, rows [][]byte) AppendOutcome {
	if len(rows) == 0 {
		return AppendOutcome{}
	}
	if err := w.beginAppend(); err != nil {
		return AppendOutcome{Verdict: Classify(err), Err: err}
	}
	defer w.appendWG.Done()

	gen, err := w.ensureStream()
	if err != nil {
		return AppendOutcome{Verdict: Classify(err), Err: err}
	}

	requests := SplitRequests(rows, w.opts.MaxRequestBytes)

	type pending struct {
		res        appendResult
		start      int
		count      int
		dispatched time.Time
	}
	inflight := make([]pending, 0, len(requests))
	out := AppendOutcome{}

	offset := 0
	for _, req := range requests {
		dispatched := time.Now()
		res, aerr := gen.AppendRows(ctx, req)
		if aerr != nil {
			// AppendRows can return the attempt-context error after handing a
			// request to the bidirectional stream. Its acknowledgement is then
			// ambiguous, and this generation cannot safely be reused.
			v, attemptEnded := w.classifyAppendFailure(ctx, aerr)
			if v.StreamRecreate || attemptEnded || v.Class == Uncertain {
				w.retireStream(gen)
			}
			if attemptEnded {
				w.observeTimeoutAfterDispatch()
			}
			out.Verdict = v
			out.Err = aerr
			break
		}
		w.observeInflight(1)
		inflight = append(inflight, pending{
			res: res, start: offset, count: len(req), dispatched: dispatched,
		})
		offset += len(req)
	}

	for _, p := range inflight {
		if _, rerr := p.res.GetResult(ctx); rerr != nil {
			// Row errors arrive alongside the error; the whole request is not
			// appended when any row in it fails.
			rowErrs := extractRowErrors(ctx, p.res, p.start)
			if len(rowErrs) > 0 {
				w.observeInflight(-1)
				w.observeResultWait(p.dispatched)
				out.RowErrors = append(out.RowErrors, rowErrs...)
				continue
			}
			v, attemptEnded := w.classifyAppendFailure(ctx, rerr)
			if v.StreamRecreate || attemptEnded || v.Class == Uncertain {
				w.retireStream(gen)
			}
			if attemptEnded {
				w.observeTimeoutAfterDispatch()
				w.trackResult(p.res, p.dispatched)
			} else {
				w.observeInflight(-1)
				w.observeResultWait(p.dispatched)
			}
			// Preserve the first transport failure as the batch verdict, but
			// keep draining all other results already dispatched by this call.
			if out.Err == nil {
				out.Verdict = v
				out.Err = rerr
			}
			continue
		}
		w.observeInflight(-1)
		w.observeResultWait(p.dispatched)
		out.AcknowledgedRows += p.count
	}
	return out
}

// classifyAttemptFailure distinguishes an ordinary server failure from the
// attempt context ending after AppendRows was invoked. Cancellation at that
// point has the same delivery ambiguity as a deadline: the server may have
// applied the rows even though the caller did not observe the acknowledgement.
func (w *Writer) classifyAppendFailure(ctx context.Context, err error) (Verdict, bool) {
	ctxErr := ctx.Err()
	if ctxErr != nil && errors.Is(err, ctxErr) {
		return Verdict{Uncertain, OwnerExporterHelper, LabelUncertainAck, false}, true
	}
	if w.lifetimeCtx != nil && w.lifetimeCtx.Err() != nil && errors.Is(err, context.Canceled) {
		return Verdict{Retryable, OwnerExporterHelper, LabelShutdown, false}, false
	}
	return Classify(err), false
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
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	gen := w.ms
	w.ms = nil
	drainCancel := w.drainCancel
	w.drainCancel = nil
	w.mu.Unlock()
	var err error
	if gen != nil {
		err = gen.Close()
	}
	// All Append calls must finish registering any asynchronous result owner
	// before cancellation and Wait; this ordering avoids Add/Wait races.
	w.appendWG.Wait()
	if drainCancel != nil {
		drainCancel()
	}
	w.drainWG.Wait()
	return err
}

// realStream adapts *managedwriter.ManagedStream to the stream interface.
type realStream struct{ ms *managedwriter.ManagedStream }

func (r realStream) AppendRows(ctx context.Context, data [][]byte, opts ...managedwriter.AppendOption) (appendResult, error) {
	return r.ms.AppendRows(ctx, data, opts...)
}

func (r realStream) Close() error { return r.ms.Close() }
