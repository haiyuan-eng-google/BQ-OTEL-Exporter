// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

// Package bigquery wraps the Storage Write API managedwriter client with the
// stream management, request sizing and error classification the exporter
// needs. docs/schema-rationale.md describes the delivery guarantees these
// pieces uphold.
package bigquery // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/bigquery"

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/types/descriptorpb"
)

// WriterOptions configures one destination stream.
type WriterOptions struct {
	Project string
	Dataset string
	Table   string
	Signal  string

	// Descriptor describes the row message, derived from the destination
	// table's schema.
	Descriptor *descriptorpb.DescriptorProto

	// MaxRequestBytes is our own headroom threshold beneath the API's
	// per-request limit. Requests are split to stay under it.
	MaxRequestBytes int

	// MaxInflightRequests bounds asynchronous appends per stream slot.
	// MaxInflightBytes bounds the total memory held by in-flight appends
	// across the entire stream pool.
	// managedwriter enforces the per-stream limits; the exporter mirrors the
	// per-slot request-count window and enforces the pool-wide byte budget so
	// concurrent calls drain results before they can collectively block inside
	// AppendRows.
	MaxInflightRequests int
	MaxInflightBytes    int

	// StreamPoolSize sets the number of default-stream ManagedStream slots
	// opened per destination table (1..16). Values <= 1 open a single stream.
	StreamPoolSize int

	// TraceID identifies this client to the Write API backend, which makes
	// producer-side attribution possible when the service team is debugging
	// on our behalf.
	TraceID string

	// Observer receives bounded, content-free stream lifecycle telemetry.
	Observer Observer

	// Backpressure optionally adjusts the effective per-request byte split
	// threshold and in-flight dispatch concurrency using AIMD control.
	Backpressure *BackpressureController
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
	RecordBackpressureLevel(ctx context.Context, signal, table string, level float64)
	RecordEffectiveRequestBytes(ctx context.Context, signal, table string, bytes int64)
	RecordBackpressureEvent(ctx context.Context, signal, table, trigger string)
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
	Ready() <-chan struct{}
	GetResult(ctx context.Context) (int64, error)
	FullResponse(ctx context.Context) (*storagepb.AppendRowsResponse, error)
}

// Writer owns a pool of default-stream ManagedStream slots per destination table.
type Writer struct {
	opts        WriterOptions
	factory     streamFactory
	lifetimeCtx context.Context
	ownedCtx    context.Context
	ownedCancel context.CancelFunc

	// mu guards writer-wide lifecycle (closed, closeDone, closeErr,
	// ownedCtx, ownedCancel, slots) and the pool-wide in-flight byte budget
	// (inflightBytes, byteSignal). It is never held at the same time as any
	// slot.mu.
	mu            sync.Mutex
	slots         []*streamSlot
	nextSlot      atomic.Uint64
	inflightBytes int
	byteSignal    chan struct{}
	closed        bool
	closeDone     chan struct{}
	closeErr      error

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

// streamSlot owns one independent default-stream lifecycle and flow window.
type streamSlot struct {
	index    int
	ownedCtx context.Context
	pending  atomic.Int64

	// mu guards this slot's stream lifecycle (ms, creation, generationSignal,
	// activeGenerations, closed) and every streamGeneration created on it.
	// It is never held at the same time as Writer.mu.
	mu                sync.Mutex
	ms                *streamGeneration
	creation          *streamCreation
	generationSignal  chan struct{}
	activeGenerations int
	closed            bool
}

// streamGeneration binds an append result to the exact stream that dispatched
// it. Pointer identity is the generation token: a late failure can retire this
// generation only while it is still its slot's current stream.
type streamGeneration struct {
	stream
	slot        *streamSlot
	users       int
	pending     int
	dispatched  int
	slotSignal  chan struct{}
	retired     bool
	slotHeld    bool
	retiredDone chan struct{}
}

// NewWriter builds a writer whose managed streams retain lifetimeCtx for their
// entire component lifetime.
func NewWriter(
	lifetimeCtx context.Context, client *managedwriter.Client, opts WriterOptions,
) *Writer {
	w := &Writer{
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
	w.ensureSlotsLocked()
	return w
}

func (w *Writer) ensureSlotsLocked() []*streamSlot {
	if len(w.slots) == 0 {
		n := max(1, w.opts.StreamPoolSize)
		w.slots = make([]*streamSlot, n)
		for i := range n {
			w.slots[i] = &streamSlot{
				index:  i,
				closed: w.closed,
			}
		}
	}
	return w.slots
}

func (w *Writer) perSlotMaxInflightBytes() int {
	if w.opts.MaxInflightBytes <= 0 || w.opts.StreamPoolSize <= 1 {
		return w.opts.MaxInflightBytes
	}
	perSlot := w.opts.MaxInflightBytes / w.opts.StreamPoolSize
	if perSlot < w.opts.MaxRequestBytes {
		perSlot = w.opts.MaxRequestBytes
	}
	return perSlot
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
		managedwriter.WithMaxInflightBytes(w.perSlotMaxInflightBytes()),
		managedwriter.WithTraceID(w.opts.TraceID),

		// Set explicitly even though write retries are already off by
		// default: retry ownership is a contract and must not depend on a
		// library default. managedwriter owns connection recovery only;
		// exporterhelper owns append replay. Two retriers would multiply
		// attempts and break the duplicate bound.
		managedwriter.EnableWriteRetries(false),
	}
}

func slotHasOpenWindow(slot *streamSlot, effectiveInflight int) bool {
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.closed {
		return false
	}
	if slot.ms == nil {
		return slot.activeGenerations < maxActiveStreamGenerations
	}
	return !slot.ms.retired && slot.ms.dispatched < effectiveInflight
}

func (w *Writer) selectSlot(slots []*streamSlot) *streamSlot {
	if len(slots) == 1 {
		return slots[0]
	}
	effectiveInflight := w.EffectiveMaxInflightRequests()
	start := int((w.nextSlot.Add(1) - 1) % uint64(len(slots)))
	best := slots[start]
	bestOpen := slotHasOpenWindow(best, effectiveInflight)
	bestPending := best.pending.Load()
	if bestOpen && bestPending == 0 {
		return best
	}
	for i := 1; i < len(slots); i++ {
		cand := slots[(start+i)%len(slots)]
		candOpen := slotHasOpenWindow(cand, effectiveInflight)
		candPending := cand.pending.Load()
		if (candOpen && !bestOpen) || (candOpen == bestOpen && candPending < bestPending) {
			best = cand
			bestOpen = candOpen
			bestPending = candPending
			if bestOpen && bestPending == 0 {
				break
			}
		}
	}
	return best
}

// ensureSlotStream returns the live stream generation for slot, creating it on
// first use.
//
// managedwriter retains the stream-construction context. It must therefore be
// the component-owned lifetime context, never an exporterhelper attempt
// context that is canceled as soon as one Consume call returns.
func (w *Writer) ensureSlotStream(
	ctx context.Context, slot *streamSlot,
) (*streamGeneration, error) {
	gen, _, err := w.acquireSlotStream(ctx, slot, true)
	return gen, err
}

// acquireSlotStream returns the live stream generation for slot, starting
// creation when a generation slot is free. When waitForGenerationSlot is false
// and all generation slots on slot are occupied by draining retired streams,
// it returns (nil, false, nil) instead of blocking on generationSignal so a
// caller with in-flight work can drain first.
func (w *Writer) acquireSlotStream(
	ctx context.Context, slot *streamSlot, waitForGenerationSlot bool,
) (*streamGeneration, bool, error) {
	for {
		slot.mu.Lock()
		if slot.closed {
			slot.mu.Unlock()
			return nil, false, errWriterClosed
		}
		if err := ctx.Err(); err != nil {
			slot.mu.Unlock()
			return nil, false, err
		}
		if slot.ms != nil {
			gen := slot.ms
			gen.users++
			slot.mu.Unlock()
			return gen, true, nil
		}

		if slot.creation == nil && slot.activeGenerations < maxActiveStreamGenerations {
			w.startSlotStreamCreationLocked(slot)
		}
		if creation := slot.creation; creation != nil {
			done := creation.done
			slot.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, false, ctx.Err()
			case <-done:
				if creation.err != nil {
					return nil, false, fmt.Errorf("creating write stream for %s: %w", w.opts.TableID(), creation.err)
				}
				continue
			}
		}

		if !waitForGenerationSlot {
			slot.mu.Unlock()
			return nil, false, nil
		}

		signal := slot.generationSignalLocked()
		slot.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-signal:
		}
	}
}

func (w *Writer) startSlotStreamCreationLocked(slot *streamSlot) {
	creation := &streamCreation{done: make(chan struct{})}
	slot.creation = creation
	slot.activeGenerations++
	ownedCtx := slot.ownedCtx
	w.constructorWG.Add(1)
	go func() {
		defer w.constructorWG.Done()
		ms, err := w.factory(ownedCtx, w.writerOptions()...)
		var discard stream

		slot.mu.Lock()
		switch {
		case err != nil:
			creation.err = err
			slot.activeGenerations--
		case slot.closed || ownedCtx.Err() != nil:
			discard = ms
			creation.err = errWriterClosed
			if ctxErr := ownedCtx.Err(); ctxErr != nil {
				creation.err = ctxErr
			}
			slot.activeGenerations--
		default:
			slot.ms = &streamGeneration{
				stream:      ms,
				slot:        slot,
				slotHeld:    true,
				retiredDone: make(chan struct{}),
			}
		}
		if slot.creation == creation {
			slot.creation = nil
		}
		close(creation.done)
		slot.notifyGenerationChangeLocked()
		slot.mu.Unlock()

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

func (s *streamSlot) generationSignalLocked() <-chan struct{} {
	if s.generationSignal == nil {
		s.generationSignal = make(chan struct{})
	}
	return s.generationSignal
}

func (s *streamSlot) notifyGenerationChangeLocked() {
	if s.generationSignal != nil {
		close(s.generationSignal)
		s.generationSignal = nil
	}
}

// beginAppend prevents shutdown from starting a Wait while a new append can
// still register work. The WaitGroup Add happens under the same lock that
// closes the writer, so Close's Wait cannot race an Add from a new call.
func (w *Writer) beginAppend() ([]*streamSlot, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil, errWriterClosed
	}
	ctx := w.ownedContextLocked()
	slots := w.ensureSlotsLocked()
	for _, slot := range slots {
		if slot.ownedCtx == nil {
			slot.ownedCtx = ctx
		}
	}
	w.appendWG.Add(1)
	return slots, nil
}

// trackResult keeps terminal ownership after an attempt context expires.
// managedwriter's GetResult only reports that caller cancellation; it does not
// make the underlying AppendResult terminal. This waiter lives until the
// result resolves or Writer.Close cancels the component lifecycle context.
func (w *Writer) trackResult(
	slot *streamSlot, gen *streamGeneration, reqBytes int, res appendResult, started time.Time,
) {
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
			w.releaseResult(slot, gen, reqBytes)
			w.drainWG.Done()
		}()
		_, _ = res.GetResult(ctx)
	}()
}

// retireStream tears down gen only if it is still current on its slot. Another
// append on this slot may already have retired gen and installed a replacement;
// a late result from the old generation must not close that replacement, and
// other slots in the pool are completely unaffected.
func (w *Writer) retireStream(slot *streamSlot, gen *streamGeneration) {
	slot.mu.Lock()
	if slot.ms != gen {
		slot.mu.Unlock()
		return
	}
	slot.ms = nil
	markGenerationRetiredLocked(gen)
	slot.releaseGenerationSlotLocked(gen)
	slot.notifyGenerationChangeLocked()
	slot.mu.Unlock()
	_ = gen.Close()
	if w.opts.Observer != nil {
		w.opts.Observer.RecordStreamRetirement(context.Background())
	}
}

func markGenerationRetiredLocked(gen *streamGeneration) {
	if gen.retired {
		return
	}
	gen.retired = true
	close(gen.retiredDone)
}

// EffectiveMaxRequestBytes returns the current request split threshold,
// reading from the adaptive BackpressureController when configured.
func (w *Writer) EffectiveMaxRequestBytes() int {
	if w.opts.Backpressure != nil {
		if n := w.opts.Backpressure.EffectiveRequestBytes(); n > 0 {
			return n
		}
	}
	return w.opts.MaxRequestBytes
}

// EffectiveMaxInflightRequests returns the current concurrent dispatch limit,
// reading from the adaptive BackpressureController when configured.
func (w *Writer) EffectiveMaxInflightRequests() int {
	if w.opts.Backpressure != nil {
		if n := w.opts.Backpressure.EffectiveInflightRequests(); n >= 1 {
			return n
		}
	}
	if w.opts.MaxInflightRequests >= 1 {
		return w.opts.MaxInflightRequests
	}
	return 1
}

func slotSignalLocked(gen *streamGeneration) <-chan struct{} {
	if gen.slotSignal == nil {
		gen.slotSignal = make(chan struct{})
	}
	return gen.slotSignal
}

func (w *Writer) notifySlotChangeLocked(gen *streamGeneration) {
	if gen.slotSignal != nil {
		close(gen.slotSignal)
		gen.slotSignal = nil
	}
}

func (w *Writer) byteSignalLocked() <-chan struct{} {
	if w.byteSignal == nil {
		w.byteSignal = make(chan struct{})
	}
	return w.byteSignal
}

func (w *Writer) tryAcquireBytes(reqBytes int) bool {
	if reqBytes <= 0 {
		return true
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.inflightBytes+reqBytes > w.opts.MaxInflightBytes {
		return false
	}
	w.inflightBytes += reqBytes
	return true
}

func (w *Writer) waitForBytes(ctx context.Context, gen *streamGeneration, reqBytes int) error {
	if reqBytes <= 0 {
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case <-gen.retiredDone:
			return errStreamRetired
		default:
		}
		w.mu.Lock()
		if w.inflightBytes+reqBytes <= w.opts.MaxInflightBytes {
			w.inflightBytes += reqBytes
			w.mu.Unlock()
			return nil
		}
		sig := w.byteSignalLocked()
		w.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-gen.retiredDone:
			return errStreamRetired
		case <-sig:
		}
	}
}

func (w *Writer) releaseBytes(reqBytes int) {
	if reqBytes <= 0 {
		return
	}
	w.mu.Lock()
	w.inflightBytes -= reqBytes
	if w.byteSignal != nil {
		close(w.byteSignal)
		w.byteSignal = nil
	}
	w.mu.Unlock()
}

func (w *Writer) tryAcquireDispatchSlot(
	ctx context.Context, slot *streamSlot, gen *streamGeneration, reqBytes int,
) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	slot.mu.Lock()
	if gen.retired {
		slot.mu.Unlock()
		return false, errStreamRetired
	}
	if gen.dispatched >= w.EffectiveMaxInflightRequests() {
		slot.mu.Unlock()
		return false, nil
	}
	gen.dispatched++
	slot.mu.Unlock()

	if !w.tryAcquireBytes(reqBytes) {
		slot.mu.Lock()
		gen.dispatched--
		w.notifySlotChangeLocked(gen)
		slot.mu.Unlock()
		return false, nil
	}
	slot.pending.Add(1)
	return true, nil
}

func (w *Writer) waitForDispatchSlot(
	ctx context.Context, slot *streamSlot, gen *streamGeneration, reqBytes int,
) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		slot.mu.Lock()
		if gen.retired {
			slot.mu.Unlock()
			return errStreamRetired
		}
		if gen.dispatched < w.EffectiveMaxInflightRequests() {
			gen.dispatched++
			slot.mu.Unlock()
			if err := w.waitForBytes(ctx, gen, reqBytes); err != nil {
				slot.mu.Lock()
				gen.dispatched--
				w.notifySlotChangeLocked(gen)
				slot.mu.Unlock()
				return err
			}
			slot.pending.Add(1)
			return nil
		}
		sig := slotSignalLocked(gen)
		slot.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-gen.retiredDone:
			return errStreamRetired
		case <-sig:
		}
	}
}

func (w *Writer) releaseDispatchSlot(slot *streamSlot, gen *streamGeneration, reqBytes int) {
	slot.pending.Add(-1)
	w.releaseBytes(reqBytes)
	slot.mu.Lock()
	gen.dispatched--
	w.notifySlotChangeLocked(gen)
	slot.mu.Unlock()
}

func (w *Writer) registerResult(slot *streamSlot, gen *streamGeneration) {
	slot.mu.Lock()
	gen.pending++
	slot.mu.Unlock()
}

func (w *Writer) releaseResult(slot *streamSlot, gen *streamGeneration, reqBytes int) {
	slot.pending.Add(-1)
	w.releaseBytes(reqBytes)
	slot.mu.Lock()
	gen.pending--
	gen.dispatched--
	w.notifySlotChangeLocked(gen)
	slot.releaseGenerationSlotLocked(gen)
	slot.mu.Unlock()
}

func (w *Writer) releaseGenerationUser(slot *streamSlot, gen *streamGeneration) {
	slot.mu.Lock()
	gen.users--
	slot.releaseGenerationSlotLocked(gen)
	slot.mu.Unlock()
}

func (w *Writer) releaseRetiredSlotGen(slot *streamSlot, gen *streamGeneration) bool {
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if !gen.retired {
		return false
	}
	gen.users--
	slot.releaseGenerationSlotLocked(gen)
	return true
}

// releaseRetiredSlotGens drops cached user references to stream generations
// that have already been retired so a caller blocked on another slot or
// draining in-flight work does not pin activeGenerations until Append returns.
func (w *Writer) releaseRetiredSlotGens(slots []*streamSlot, slotGens []*streamGeneration) {
	if len(slots) <= 1 {
		return
	}
	for i, g := range slotGens {
		if g != nil && w.releaseRetiredSlotGen(slots[i], g) {
			slotGens[i] = nil
		}
	}
}

func (s *streamSlot) releaseGenerationSlotLocked(gen *streamGeneration) {
	if !gen.slotHeld || !gen.retired || gen.users != 0 || gen.pending != 0 {
		return
	}
	gen.slotHeld = false
	s.activeGenerations--
	s.notifyGenerationChangeLocked()
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

func (w *Writer) now() time.Time {
	if w.opts.Backpressure != nil && w.opts.Backpressure.now != nil {
		return w.opts.Backpressure.now()
	}
	return time.Now()
}

func (w *Writer) observeResultWait(started time.Time) {
	if w.opts.Observer != nil {
		w.opts.Observer.RecordAppendResultWait(context.Background(), w.now().Sub(started))
	}
}

func (w *Writer) observeTimeoutAfterDispatch() {
	if w.opts.Observer != nil {
		w.opts.Observer.RecordTimeoutAfterDispatch(context.Background())
	}
}

func (w *Writer) observeBackpressureTransition(trigger string) {
	if w.opts.Observer == nil || w.opts.Backpressure == nil {
		return
	}
	ctx := context.Background()
	w.opts.Observer.RecordBackpressureEvent(ctx, w.opts.Signal, w.opts.Table, trigger)
	w.opts.Observer.RecordBackpressureLevel(ctx, w.opts.Signal, w.opts.Table, w.opts.Backpressure.Level())
	w.opts.Observer.RecordEffectiveRequestBytes(ctx, w.opts.Signal, w.opts.Table, int64(w.opts.Backpressure.EffectiveRequestBytes()))
}

// isBackpressureThrottle reports whether a non-permanent server failure should
// step the adaptive controller down. Client-side attempt expiries
// (attemptEnded == true) are routed to OnSlowAck in recordFailureFeedback
// before reaching this switch, so LabelUncertainAck here applies only to
// server-returned codes.DeadlineExceeded when the caller's context is still live.
func isBackpressureThrottle(v Verdict) bool {
	if v.Class == Permanent {
		return false
	}
	switch v.Label {
	case LabelQuota, LabelOversizeRequest, LabelUnavailable, LabelUncertainAck:
		return true
	default:
		return false
	}
}

// recordFailureFeedback updates the adaptive backpressure controller after a
// sub-request failure. Client-side attempt expiry (attemptEnded) is measured
// only by elapsed latency so callers with nearly-spent deadlines do not pin
// the controller at its floor, and sibling stream teardown is ignored.
func (w *Writer) recordFailureFeedback(
	v Verdict, attemptEnded, siblingTeardown bool, elapsed time.Duration,
) {
	bp := w.opts.Backpressure
	if bp == nil || siblingTeardown {
		return
	}
	if attemptEnded {
		if bp.OnSlowAck(elapsed) {
			w.observeBackpressureTransition("latency")
		}
		return
	}
	if !isBackpressureThrottle(v) {
		return
	}
	if bp.OnThrottle() {
		w.observeBackpressureTransition("throttle")
	}
}

func (w *Writer) recordCleanOrSlowAck(elapsed time.Duration, gen *streamGeneration) {
	bp := w.opts.Backpressure
	if bp == nil {
		return
	}
	if bp.LatencyThreshold() > 0 && elapsed > bp.LatencyThreshold() {
		if bp.OnSlowAck(elapsed) {
			w.observeBackpressureTransition("latency")
		}
		return
	}
	if bp.OnCleanAck(elapsed) {
		// The larger window applies to every slot's live generation. gen may
		// already be retired and draining while callers wait for a slot on a
		// current generation, so wake those waiters too rather than leaving
		// them until the next slot release. Only current generations can be
		// live; waiters on retired ones have already left through retiredDone.
		if gen != nil && gen.slot != nil {
			gen.slot.mu.Lock()
			w.notifySlotChangeLocked(gen)
			gen.slot.mu.Unlock()
		}
		w.mu.Lock()
		slots := w.ensureSlotsLocked()
		w.mu.Unlock()
		for _, s := range slots {
			s.mu.Lock()
			if cur := s.ms; cur != nil && cur != gen {
				w.notifySlotChangeLocked(cur)
			}
			s.mu.Unlock()
		}
		w.observeBackpressureTransition("recovery")
	}
}

func (w *Writer) finishResult(
	slot *streamSlot, gen *streamGeneration, reqBytes int, started time.Time,
) {
	w.observeInflight(-1)
	w.observeResultWait(started)
	w.releaseResult(slot, gen, reqBytes)
}

func (w *Writer) classifyAndRetire(
	ctx context.Context, err error, slot *streamSlot, gen *streamGeneration, req requestSlice,
) (Verdict, bool, bool) {
	verdict, attemptEnded := w.classifyAppendFailure(ctx, err, req)
	siblingTeardown := false
	if !attemptEnded && verdict.Label != LabelShutdown && isTeardown(err) && w.isRetired(slot, gen) {
		// Another attempt retired this generation and closed the connection
		// under this request. It may already have reached the service, so the
		// outcome is as ambiguous as an expired attempt's and its replay can
		// duplicate.
		verdict = Verdict{Uncertain, OwnerExporterHelper, LabelUncertainAck, false}
		siblingTeardown = true
	}
	if verdict.StreamRecreate || attemptEnded || verdict.Class == Uncertain {
		w.retireStream(slot, gen)
	}
	if attemptEnded {
		w.observeTimeoutAfterDispatch()
	}
	return verdict, attemptEnded, siblingTeardown
}

// deliveryRank orders verdicts by what a batch still owes its rows:
// uncertain rows may already be applied and are replayed with duplicate
// accounting, retryable rows (including stream-recreation verdicts whose
// unacknowledged rows must be replayed on a fresh stream) certainly were not
// and are replayed, and only permanently rejected rows may be dropped.
func deliveryRank(v Verdict) int {
	switch {
	case v.Class == Uncertain:
		return 2
	case v.Class == Retryable || v.StreamRecreate:
		return 1
	default:
		return 0
	}
}

func (w *Writer) isRetired(slot *streamSlot, gen *streamGeneration) bool {
	slot.mu.Lock()
	defer slot.mu.Unlock()
	return gen.retired
}

func (w *Writer) hasCurrentStream() bool {
	w.mu.Lock()
	slots := w.ensureSlotsLocked()
	w.mu.Unlock()
	for _, slot := range slots {
		slot.mu.Lock()
		has := slot.ms != nil
		slot.mu.Unlock()
		if has {
			return true
		}
	}
	return false
}

func (w *Writer) isCurrentStream(s stream) bool {
	w.mu.Lock()
	slots := w.ensureSlotsLocked()
	w.mu.Unlock()
	for _, slot := range slots {
		slot.mu.Lock()
		match := slot.ms != nil && slot.ms.stream == s
		slot.mu.Unlock()
		if match {
			return true
		}
	}
	return false
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
	// AckedRanges records the half-open [start, end) batch row index intervals
	// of sub-requests that the service acknowledged.
	AckedRanges [][2]int
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

// IsAcked reports whether rowIndex (in the batch passed to Append) belongs to a
// sub-request that the service acknowledged.
func (o AppendOutcome) IsAcked(rowIndex int) bool {
	for _, r := range o.AckedRanges {
		if rowIndex >= r[0] && rowIndex < r[1] {
			return true
		}
	}
	return false
}

// requestSlice records one split sub-request alongside its wire size and the
// byte split limit under which it was formed.
type requestSlice struct {
	rows            [][]byte
	bytes           int
	splitLimitBytes int
}

// pendingAppend tracks one in-flight AppendRows sub-request and stamps its
// completion time as soon as Ready() closes so later local dispatch or drain
// delays do not inflate acknowledgement latency.
type pendingAppend struct {
	slot              *streamSlot
	gen               *streamGeneration
	reqBytes          int
	res               appendResult
	req               requestSlice
	start             int
	count             int
	dispatched        time.Time
	completedUnixNano atomic.Int64
	done              chan struct{}
	stopOnce          sync.Once
}

func stampNano(t time.Time) int64 {
	if ns := t.UnixNano(); ns != 0 {
		return ns
	}
	return 1
}

func (p *pendingAppend) pollReady(nowFn func() time.Time) bool {
	if p.completedUnixNano.Load() != 0 {
		return true
	}
	ready := p.res.Ready()
	if ready == nil {
		return false
	}
	select {
	case <-ready:
		p.completedUnixNano.CompareAndSwap(0, stampNano(nowFn()))
		return true
	default:
		return false
	}
}

func (p *pendingAppend) stopWatcher() {
	if p.done == nil {
		return
	}
	p.stopOnce.Do(func() { close(p.done) })
}

func (p *pendingAppend) ackDuration(nowFn func() time.Time) time.Duration {
	p.pollReady(nowFn)
	if completed := p.completedUnixNano.Load(); completed != 0 {
		if d := time.Duration(completed - p.dispatched.UnixNano()); d >= 0 {
			return d
		}
		return 0
	}
	if d := nowFn().Sub(p.dispatched); d >= 0 {
		return d
	}
	return 0
}

func pollInflightReady(inflight []*pendingAppend, nowFn func() time.Time) {
	for _, p := range inflight {
		p.pollReady(nowFn)
	}
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
	slots, err := w.beginAppend()
	if err != nil {
		return AppendOutcome{Verdict: classifyPreDispatchFailure(err), Err: err}
	}
	defer w.appendWG.Done()

	var slotGenBuf [16]*streamGeneration
	slotGens := slotGenBuf[:len(slots)]
	if len(slots) > len(slotGenBuf) {
		slotGens = make([]*streamGeneration, len(slots))
	}
	defer func() {
		for i, g := range slotGens {
			if g != nil {
				w.releaseGenerationUser(slots[i], g)
			}
		}
	}()

	if len(slots) == 1 {
		gen, serr := w.ensureSlotStream(ctx, slots[0])
		if serr != nil {
			return AppendOutcome{Verdict: classifyPreDispatchFailure(serr), Err: serr}
		}
		slotGens[0] = gen
	}

	requests := splitRequestSlices(rows, w.EffectiveMaxRequestBytes())

	inflight := make([]*pendingAppend, 0, len(requests))
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
		if deliveryRank(v) > deliveryRank(out.Verdict) {
			out.Verdict = v
		}
	}
	// Once the batch must be replayed anyway, dispatching more of it would
	// only send those rows twice. Until then, a permanent rejection of one
	// request does not keep the others from being sent.
	replayOwed := func() bool {
		return out.Err != nil && (out.Verdict.Class != Permanent || out.Verdict.StreamRecreate)
	}
	drainOne := func(p *pendingAppend) {
		defer p.stopWatcher()
		pollInflightReady(inflight, w.now)
		_, rerr := p.res.GetResult(ctx)
		pollInflightReady(inflight, w.now)
		elapsed := p.ackDuration(w.now)
		if rerr != nil {
			// Row errors arrive alongside the error; the whole request is not
			// appended when any row in it fails.
			rowErrs := extractRowErrors(ctx, p.res, p.start)
			if len(rowErrs) > 0 {
				w.finishResult(p.slot, p.gen, p.reqBytes, p.dispatched)
				out.RowErrors = append(out.RowErrors, rowErrs...)
				return
			}
			v, attemptEnded, siblingTeardown := w.classifyAndRetire(ctx, rerr, p.slot, p.gen, p.req)
			if attemptEnded {
				w.trackResult(p.slot, p.gen, p.reqBytes, p.res, p.dispatched)
			} else {
				w.finishResult(p.slot, p.gen, p.reqBytes, p.dispatched)
			}
			w.recordFailureFeedback(v, attemptEnded, siblingTeardown, elapsed)
			// Keep draining all other results already dispatched by this call.
			fail(v, rerr)
			return
		}
		w.finishResult(p.slot, p.gen, p.reqBytes, p.dispatched)
		w.recordCleanOrSlowAck(elapsed, p.gen)
		out.AcknowledgedRows += p.count
		out.AckedRanges = append(out.AckedRanges, [2]int{p.start, p.start + p.count})
	}

	offset := 0
dispatch:
	for _, req := range requests {
		start := offset
		offset += len(req.rows)
		reqBytes := w.boundedRequestBytes(req.rows)
		var (
			slot *streamSlot
			gen  *streamGeneration
		)
		for {
			if replayOwed() {
				break dispatch
			}
			w.releaseRetiredSlotGens(slots, slotGens)
			slot = w.selectSlot(slots)
			gen = slotGens[slot.index]
			if gen == nil {
				var (
					ensured bool
					serr    error
				)
				gen, ensured, serr = w.acquireSlotStream(ctx, slot, len(inflight) == 0)
				if serr != nil {
					fail(classifyPreDispatchFailure(serr), serr)
					break dispatch
				}
				if !ensured {
					w.releaseRetiredSlotGens(slots, slotGens)
					drainOne(inflight[0])
					inflight = inflight[1:]
					continue
				}
				slotGens[slot.index] = gen
			}
			acquired, acquireErr := w.tryAcquireDispatchSlot(ctx, slot, gen, reqBytes)
			if acquireErr != nil {
				if len(slots) > 1 && errors.Is(acquireErr, errStreamRetired) {
					w.releaseRetiredSlotGens(slots, slotGens)
					continue
				}
				fail(classifyPreDispatchFailure(acquireErr), acquireErr)
				break dispatch
			}
			if acquired {
				break
			}
			if len(inflight) > 0 {
				w.releaseRetiredSlotGens(slots, slotGens)
				drainOne(inflight[0])
				inflight = inflight[1:]
				continue
			}
			w.releaseRetiredSlotGens(slots, slotGens)
			if acquireErr := w.waitForDispatchSlot(ctx, slot, gen, reqBytes); acquireErr != nil {
				if len(slots) > 1 && errors.Is(acquireErr, errStreamRetired) {
					w.releaseRetiredSlotGens(slots, slotGens)
					continue
				}
				fail(classifyPreDispatchFailure(acquireErr), acquireErr)
				break dispatch
			}
			break
		}
		pollInflightReady(inflight, w.now)
		dispatched := w.now()
		res, aerr := gen.AppendRows(ctx, req.rows)
		pollInflightReady(inflight, w.now)
		if aerr == nil {
			// managedwriter's AppendRows can report success for a request it
			// never sent when the call races the end of the attempt or the
			// stream's closure, and nothing ever resolves that AppendResult.
			// Tracking it would pin this generation's window slot forever, so
			// such a success is as ambiguous as the attempt error itself.
			if err := ctx.Err(); err != nil {
				aerr = err
			} else if w.isRetired(slot, gen) {
				aerr = errStreamRetired
			}
		}
		if aerr != nil {
			w.releaseDispatchSlot(slot, gen, reqBytes)
			// AppendRows can return the attempt-context error after handing a
			// request to the bidirectional stream. Its acknowledgement is then
			// ambiguous, and this generation cannot safely be reused.
			v, attemptEnded, siblingTeardown := w.classifyAndRetire(ctx, aerr, slot, gen, req)
			w.recordFailureFeedback(v, attemptEnded, siblingTeardown, w.now().Sub(dispatched))
			fail(v, aerr)
			continue
		}
		w.registerResult(slot, gen)
		w.observeInflight(1)
		p := &pendingAppend{
			slot: slot, gen: gen, reqBytes: reqBytes,
			res: res, req: req, start: start, count: len(req.rows), dispatched: dispatched,
			done: make(chan struct{}),
		}
		if !p.pollReady(w.now) {
			if ready := res.Ready(); ready != nil {
				go func() {
					select {
					case <-ready:
						p.completedUnixNano.CompareAndSwap(0, stampNano(w.now()))
					case <-p.done:
					}
				}()
			}
		}
		inflight = append(inflight, p)
	}

	w.releaseRetiredSlotGens(slots, slotGens)
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
func (w *Writer) classifyAppendFailure(ctx context.Context, err error, req requestSlice) (Verdict, bool) {
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
	return ClassifySubRequest(err, len(req.rows), w.canSplitSmaller(req)), false
}

// canSplitSmaller reports whether replaying req at a lower split threshold can
// split its rows into smaller sub-requests. It checks both the current global
// threshold and the splitLimitBytes under which req was originally formed so an
// in-flight request is not permanently rejected when a sibling caller reaches
// the floor first.
func (w *Writer) canSplitSmaller(req requestSlice) bool {
	if len(req.rows) <= 1 || w.opts.Backpressure == nil {
		return false
	}
	floor := w.opts.Backpressure.MinRequestBytes()
	if w.opts.Backpressure.EffectiveRequestBytes() > floor {
		return true
	}
	return req.splitLimitBytes > floor && req.bytes > floor
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
	resp, _ := res.FullResponse(ctx)
	if resp == nil {
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

// requestEnvelopeOverheadBytes reserves space in each AppendRows request for
// outer protobuf framing outside ProtoRows.serialized_rows.
const requestEnvelopeOverheadBytes = 64

// rowWireBytes returns the protobuf wire size of one serialized row inside
// ProtoRows.serialized_rows (1-byte field tag + varint length prefix + payload).
func rowWireBytes(row []byte) int {
	return 1 + protowire.SizeVarint(uint64(len(row))) + len(row)
}

func requestWireBytes(req [][]byte) int {
	size := requestEnvelopeOverheadBytes
	for _, row := range req {
		size += rowWireBytes(row)
	}
	return size
}

func (w *Writer) boundedRequestBytes(req [][]byte) int {
	if w.opts.MaxInflightBytes <= 0 {
		return 0
	}
	n := requestWireBytes(req)
	if n > w.opts.MaxInflightBytes {
		return w.opts.MaxInflightBytes
	}
	return n
}

// SplitRequests groups serialized rows into requests that stay under the size
// threshold, accounting for per-row ProtoRows wire framing and a fixed
// per-request envelope reserve.
//
// A row larger than the threshold on its own is emitted as a single-row
// request: the caller is responsible for rejecting oversized rows before this
// point, and silently dropping one here would lose data without a count.
func SplitRequests(rows [][]byte, maxBytes int) [][][]byte {
	slices := splitRequestSlices(rows, maxBytes)
	out := make([][][]byte, len(slices))
	for i, s := range slices {
		out[i] = s.rows
	}
	return out
}

func splitRequestSlices(rows [][]byte, maxBytes int) []requestSlice {
	if len(rows) == 0 {
		return nil
	}
	if maxBytes <= 0 {
		size := requestEnvelopeOverheadBytes
		for _, row := range rows {
			size += rowWireBytes(row)
		}
		return []requestSlice{{rows: rows, bytes: size, splitLimitBytes: maxBytes}}
	}
	envelope := requestEnvelopeOverheadBytes
	if maxBytes <= envelope {
		envelope = 0
	}
	var (
		out     []requestSlice
		current [][]byte
		size    = envelope
	)
	for _, row := range rows {
		n := rowWireBytes(row)
		if len(current) > 0 && size+n > maxBytes {
			out = append(out, requestSlice{
				rows:            current,
				bytes:           size,
				splitLimitBytes: maxBytes,
			})
			current, size = nil, envelope
		}
		current = append(current, row)
		size += n
	}
	if len(current) > 0 {
		out = append(out, requestSlice{
			rows:            current,
			bytes:           size,
			splitLimitBytes: maxBytes,
		})
	}
	return out
}

// Close shuts all stream slots down. In-flight appends are drained by the
// caller's context deadline before this is called.
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
	slots := w.ensureSlotsLocked()
	cancel := w.ownedCancel
	if w.byteSignal != nil {
		close(w.byteSignal)
		w.byteSignal = nil
	}
	w.mu.Unlock()

	var gens []*streamGeneration
	for _, slot := range slots {
		slot.mu.Lock()
		slot.closed = true
		if gen := slot.ms; gen != nil {
			slot.ms = nil
			markGenerationRetiredLocked(gen)
			slot.releaseGenerationSlotLocked(gen)
			gens = append(gens, gen)
		}
		slot.notifyGenerationChangeLocked()
		slot.mu.Unlock()
	}

	if cancel != nil {
		cancel()
	}
	var errs []error
	for _, gen := range gens {
		if err := gen.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	w.constructorWG.Wait()
	// All Append calls must finish registering any asynchronous result owner
	// before cancellation and Wait; this ordering avoids Add/Wait races.
	w.appendWG.Wait()
	w.drainWG.Wait()
	err := errors.Join(errs...)
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
