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
	opts    WriterOptions
	factory streamFactory

	mu     sync.Mutex
	ms     stream
	closed bool
}

// NewWriter builds a writer over an existing managedwriter client.
func NewWriter(client *managedwriter.Client, opts WriterOptions) *Writer {
	return &Writer{
		opts: opts,
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

// ensureStream returns the live stream, creating it on first use.
func (w *Writer) ensureStream(ctx context.Context) (stream, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil, errors.New("writer is closed")
	}
	if w.ms != nil {
		return w.ms, nil
	}
	ms, err := w.factory(ctx, w.writerOptions()...)
	if err != nil {
		return nil, fmt.Errorf("creating write stream for %s: %w", w.opts.TableID(), err)
	}
	w.ms = ms
	return ms, nil
}

// recreateStream tears down the current stream so the next append rebuilds it.
func (w *Writer) recreateStream() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.ms != nil {
		_ = w.ms.Close()
		w.ms = nil
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

	ms, err := w.ensureStream(ctx)
	if err != nil {
		return AppendOutcome{Verdict: Classify(err), Err: err}
	}

	requests := SplitRequests(rows, w.opts.MaxRequestBytes)

	type pending struct {
		res   appendResult
		start int
		count int
	}
	inflight := make([]pending, 0, len(requests))

	offset := 0
	for _, req := range requests {
		res, aerr := ms.AppendRows(ctx, req)
		if aerr != nil {
			// A dispatch failure is transport-level: nothing in this request
			// was appended.
			v := Classify(aerr)
			if v.StreamRecreate {
				w.recreateStream()
			}
			return AppendOutcome{Verdict: v, Err: aerr}
		}
		inflight = append(inflight, pending{res: res, start: offset, count: len(req)})
		offset += len(req)
	}

	out := AppendOutcome{}
	for _, p := range inflight {
		if _, rerr := p.res.GetResult(ctx); rerr != nil {
			// Row errors arrive alongside the error; the whole request is not
			// appended when any row in it fails.
			rowErrs := extractRowErrors(ctx, p.res, p.start)
			if len(rowErrs) > 0 {
				out.RowErrors = append(out.RowErrors, rowErrs...)
				continue
			}
			v := Classify(rerr)
			if v.StreamRecreate {
				w.recreateStream()
			}
			out.Verdict = v
			out.Err = rerr
			return out
		}
		out.AcknowledgedRows += p.count
	}
	return out
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
	defer w.mu.Unlock()
	w.closed = true
	if w.ms == nil {
		return nil
	}
	err := w.ms.Close()
	w.ms = nil
	return err
}

// realStream adapts *managedwriter.ManagedStream to the stream interface.
type realStream struct{ ms *managedwriter.ManagedStream }

func (r realStream) AppendRows(ctx context.Context, data [][]byte, opts ...managedwriter.AppendOption) (appendResult, error) {
	return r.ms.AppendRows(ctx, data, opts...)
}

func (r realStream) Close() error { return r.ms.Close() }
