// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package metadata // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/metadata"

import (
	"context"
	"fmt"

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
}

// Metric names, matching metadata.yaml.
const (
	nameAcknowledgedRows    = "otelcol_exporter_bigquery_acknowledged_rows"
	nameRejectedRows        = "otelcol_exporter_bigquery_rejected_rows"
	nameRetries             = "otelcol_exporter_bigquery_retries"
	nameUncertainAckReplays = "otelcol_exporter_bigquery_uncertain_ack_replays"
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
