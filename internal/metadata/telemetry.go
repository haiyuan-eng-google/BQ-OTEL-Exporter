// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package metadata // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/metadata"

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Telemetry holds the delivery-critical instruments.
//
// These exist so an operator can answer "is this pipeline losing data?" from
// the collector alone. Queue depth and queue drops are deliberately absent:
// exporterhelper already publishes them for the sending queue, and a second
// set of counters measuring the same queue would disagree under load and be
// worse than none.
//
// Every label domain here is closed. Nothing derived from telemetry content is
// ever used as a label, both because it would be unbounded cardinality and
// because it would leak payload into the metrics pipeline.
type Telemetry struct {
	// AcknowledgedRows counts rows the service confirmed. It is incremented
	// only after acknowledgement, never at enqueue time, so it can be
	// compared against what the producer sent.
	AcknowledgedRows metric.Int64Counter

	// RejectedRows counts permanently rejected rows by reason. In v1 there is
	// no dead-letter destination, so this counter is the only record that the
	// data existed.
	RejectedRows metric.Int64Counter

	// Retries counts retryable append failures by error class.
	Retries metric.Int64Counter

	// UncertainAckReplays counts in-loop replays triggered by an uncertain
	// acknowledgement. This is the observable side of the §8.4 duplicate
	// bound: N replays permit at most N+1 physical copies.
	//
	// Crash replays from the persistent queue are explicitly out of scope and
	// are not counted here.
	UncertainAckReplays metric.Int64Counter

	// InflightRequests is the number of dispatched AppendResults that have not
	// reached an exporter-owned terminal state.
	InflightRequests metric.Int64UpDownCounter

	// UnresolvedResults is the subset owned by lifecycle drainers after an
	// exporterhelper attempt context ended.
	UnresolvedResults metric.Int64UpDownCounter

	AppendResultWait      metric.Float64Histogram
	TimeoutsAfterDispatch metric.Int64Counter
	StreamRetirements     metric.Int64Counter
}

// Metric names, matching metadata.yaml.
const (
	nameAcknowledgedRows      = "otelcol_exporter_bigquery_acknowledged_rows"
	nameRejectedRows          = "otelcol_exporter_bigquery_rejected_rows"
	nameRetries               = "otelcol_exporter_bigquery_retries"
	nameUncertainAckReplays   = "otelcol_exporter_bigquery_uncertain_ack_replays"
	nameInflightRequests      = "otelcol_exporter_bigquery_inflight_requests"
	nameUnresolvedResults     = "otelcol_exporter_bigquery_unresolved_results"
	nameAppendResultWait      = "otelcol_exporter_bigquery_append_result_wait"
	nameTimeoutsAfterDispatch = "otelcol_exporter_bigquery_timeouts_after_dispatch"
	nameStreamRetirements     = "otelcol_exporter_bigquery_stream_retirements"
)

// Label keys.
const (
	AttrReason = "reason"
	AttrClass  = "class"
)

// NewTelemetry builds the instrument set.
func NewTelemetry(mp metric.MeterProvider) (*Telemetry, error) {
	m := mp.Meter(ScopeName)
	t := &Telemetry{}

	var err error
	if t.AcknowledgedRows, err = m.Int64Counter(nameAcknowledgedRows,
		metric.WithDescription("Rows acknowledged by the BigQuery Storage Write API."),
		metric.WithUnit("{row}")); err != nil {
		return nil, fmt.Errorf("creating %s: %w", nameAcknowledgedRows, err)
	}
	if t.RejectedRows, err = m.Int64Counter(nameRejectedRows,
		metric.WithDescription("Rows permanently rejected and dropped, by reason."),
		metric.WithUnit("{row}")); err != nil {
		return nil, fmt.Errorf("creating %s: %w", nameRejectedRows, err)
	}
	if t.Retries, err = m.Int64Counter(nameRetries,
		metric.WithDescription("Retryable append failures, by error class."),
		metric.WithUnit("{failure}")); err != nil {
		return nil, fmt.Errorf("creating %s: %w", nameRetries, err)
	}
	if t.UncertainAckReplays, err = m.Int64Counter(nameUncertainAckReplays,
		metric.WithDescription("Append replays triggered by an uncertain acknowledgement."),
		metric.WithUnit("{replay}")); err != nil {
		return nil, fmt.Errorf("creating %s: %w", nameUncertainAckReplays, err)
	}
	if t.InflightRequests, err = m.Int64UpDownCounter(nameInflightRequests,
		metric.WithDescription("Dispatched append results awaiting an exporter-owned terminal state."),
		metric.WithUnit("{request}")); err != nil {
		return nil, fmt.Errorf("creating %s: %w", nameInflightRequests, err)
	}
	if t.UnresolvedResults, err = m.Int64UpDownCounter(nameUnresolvedResults,
		metric.WithDescription("Append results retained by lifecycle drainers after their attempt ended."),
		metric.WithUnit("{result}")); err != nil {
		return nil, fmt.Errorf("creating %s: %w", nameUnresolvedResults, err)
	}
	if t.AppendResultWait, err = m.Float64Histogram(nameAppendResultWait,
		metric.WithDescription("Time from append dispatch until its result owner finishes."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.01, 0.1, 1, 5, 30, 70, 120)); err != nil {
		return nil, fmt.Errorf("creating %s: %w", nameAppendResultWait, err)
	}
	if t.TimeoutsAfterDispatch, err = m.Int64Counter(nameTimeoutsAfterDispatch,
		metric.WithDescription("Append attempts whose context ended after dispatch began."),
		metric.WithUnit("{timeout}")); err != nil {
		return nil, fmt.Errorf("creating %s: %w", nameTimeoutsAfterDispatch, err)
	}
	if t.StreamRetirements, err = m.Int64Counter(nameStreamRetirements,
		metric.WithDescription("Managed stream generations retired after an unsafe outcome."),
		metric.WithUnit("{retirement}")); err != nil {
		return nil, fmt.Errorf("creating %s: %w", nameStreamRetirements, err)
	}
	return t, nil
}

// RecordAcknowledged records confirmed rows.
func (t *Telemetry) RecordAcknowledged(ctx context.Context, n int) {
	if t == nil || n <= 0 {
		return
	}
	t.AcknowledgedRows.Add(ctx, int64(n))
}

// RecordRejected records permanently dropped rows under a bounded reason.
func (t *Telemetry) RecordRejected(ctx context.Context, reason string, n int) {
	if t == nil || n <= 0 {
		return
	}
	t.RejectedRows.Add(ctx, int64(n), metric.WithAttributes(attribute.String(AttrReason, reason)))
}

// RecordRetry records a retryable failure under a bounded class label.
func (t *Telemetry) RecordRetry(ctx context.Context, class string) {
	if t == nil {
		return
	}
	t.Retries.Add(ctx, 1, metric.WithAttributes(attribute.String(AttrClass, class)))
}

// RecordUncertainAckReplay records one in-loop replay from an uncertain ack.
func (t *Telemetry) RecordUncertainAckReplay(ctx context.Context) {
	if t == nil {
		return
	}
	t.UncertainAckReplays.Add(ctx, 1)
}

func (t *Telemetry) RecordInflightRequests(ctx context.Context, delta int64) {
	if t == nil || delta == 0 {
		return
	}
	t.InflightRequests.Add(ctx, delta)
}

func (t *Telemetry) RecordUnresolvedResults(ctx context.Context, delta int64) {
	if t == nil || delta == 0 {
		return
	}
	t.UnresolvedResults.Add(ctx, delta)
}

func (t *Telemetry) RecordAppendResultWait(ctx context.Context, elapsed time.Duration) {
	if t == nil {
		return
	}
	t.AppendResultWait.Record(ctx, elapsed.Seconds())
}

func (t *Telemetry) RecordTimeoutAfterDispatch(ctx context.Context) {
	if t == nil {
		return
	}
	t.TimeoutsAfterDispatch.Add(ctx, 1)
}

func (t *Telemetry) RecordStreamRetirement(ctx context.Context) {
	if t == nil {
		return
	}
	t.StreamRetirements.Add(ctx, 1)
}
