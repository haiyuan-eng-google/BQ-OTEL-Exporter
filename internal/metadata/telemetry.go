// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

// Package metadata holds the exporter's self-telemetry instruments and the
// component status generated from metadata.yaml.
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

	// RejectedRows counts permanently rejected rows by reason. It is counted
	// whether or not quarantine is enabled. With quarantine disabled, the
	// default, this counter is the only record that the data existed; with
	// quarantine enabled the rejected record is also written to the
	// otel_quarantine table.
	RejectedRows metric.Int64Counter

	// Retries counts retryable append failures by error class.
	Retries metric.Int64Counter

	// UncertainAckReplays counts in-loop replays triggered by an uncertain
	// acknowledgement. This is the observable side of the duplicate bound
	// described in docs/schema-rationale.md: N replays permit at most N+1
	// physical copies.
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
	QuarantinedRows       metric.Int64Counter
	QuarantineFailedRows  metric.Int64Counter
	AppendBytes           metric.Int64Counter
	OffloadedPayloads     metric.Int64Counter
	OffloadedBytes        metric.Int64Counter
	OffloadDedupHits      metric.Int64Counter
	OffloadErrors         metric.Int64Counter

	// RedactedValues counts attribute keys and string values masked by the
	// redaction step before offload, fingerprinting, or quarantine. The
	// labels name the signal and whether a key or a value matched; the key
	// name and the value itself are never recorded.
	RedactedValues metric.Int64Counter

	// BackpressureLevel reports the current AIMD backpressure level in
	// [0.0, 1.0] for each signal and destination table.
	BackpressureLevel metric.Float64Gauge

	// EffectiveRequestBytes reports the current AIMD per-request byte split
	// threshold for each signal and destination table.
	EffectiveRequestBytes metric.Int64Gauge

	// BackpressureEvents counts AIMD step-down and recovery transitions by
	// signal, destination table, and trigger.
	BackpressureEvents metric.Int64Counter
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
	nameQuarantinedRows       = "otelcol_exporter_bigquery_quarantined_rows"
	nameQuarantineFailedRows  = "otelcol_exporter_bigquery_quarantine_failed_rows"
	nameAppendBytes           = "otelcol_exporter_bigquery_append_bytes"
	nameOffloadedPayloads     = "otelcol_exporter_bigquery_offloaded_payloads"
	nameOffloadedBytes        = "otelcol_exporter_bigquery_offloaded_bytes"
	nameOffloadDedupHits      = "otelcol_exporter_bigquery_offload_dedup_hits"
	nameOffloadErrors         = "otelcol_exporter_bigquery_offload_errors"
	nameRedactedValues        = "otelcol_exporter_bigquery_redacted_values"
	nameBackpressureLevel     = "otelcol_exporter_bigquery_backpressure_level"
	nameEffectiveRequestBytes = "otelcol_exporter_bigquery_effective_request_bytes"
	nameBackpressureEvents    = "otelcol_exporter_bigquery_backpressure_events"
)

// Label keys.
const (
	AttrReason    = "reason"
	AttrClass     = "class"
	AttrSignal    = "signal"
	AttrMatchType = "match_type"
	AttrTable     = "table"
	AttrTrigger   = "trigger"
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
	if t.QuarantinedRows, err = m.Int64Counter(nameQuarantinedRows,
		metric.WithDescription("Rows acknowledged by the BigQuery quarantine table stream, by reason."),
		metric.WithUnit("{row}")); err != nil {
		return nil, fmt.Errorf("creating %s: %w", nameQuarantinedRows, err)
	}
	if t.QuarantineFailedRows, err = m.Int64Counter(nameQuarantineFailedRows,
		metric.WithDescription("Rows targeted for quarantine that could not be written, by reason."),
		metric.WithUnit("{row}")); err != nil {
		return nil, fmt.Errorf("creating %s: %w", nameQuarantineFailedRows, err)
	}
	if t.AppendBytes, err = m.Int64Counter(nameAppendBytes,
		metric.WithDescription("Serialized protobuf wire bytes dispatched to the BigQuery Storage Write API."),
		metric.WithUnit("By")); err != nil {
		return nil, fmt.Errorf("creating %s: %w", nameAppendBytes, err)
	}
	if t.OffloadedPayloads, err = m.Int64Counter(nameOffloadedPayloads,
		metric.WithDescription("Multimodal or oversized payloads offloaded to external object storage."),
		metric.WithUnit("{payload}")); err != nil {
		return nil, fmt.Errorf("creating %s: %w", nameOffloadedPayloads, err)
	}
	if t.OffloadedBytes, err = m.Int64Counter(nameOffloadedBytes,
		metric.WithDescription("Payload bytes offloaded to external object storage."),
		metric.WithUnit("By")); err != nil {
		return nil, fmt.Errorf("creating %s: %w", nameOffloadedBytes, err)
	}
	if t.OffloadDedupHits, err = m.Int64Counter(nameOffloadDedupHits,
		metric.WithDescription("Payload offload requests satisfied from the intra-batch coalescing cache or cross-batch generation LRU cache without a GCS upload, by signal."),
		metric.WithUnit("{hit}")); err != nil {
		return nil, fmt.Errorf("creating %s: %w", nameOffloadDedupHits, err)
	}
	if t.OffloadErrors, err = m.Int64Counter(nameOffloadErrors,
		metric.WithDescription("Payload offload attempts to external object storage that failed, by error class."),
		metric.WithUnit("{error}")); err != nil {
		return nil, fmt.Errorf("creating %s: %w", nameOffloadErrors, err)
	}
	if t.RedactedValues, err = m.Int64Counter(nameRedactedValues,
		metric.WithDescription("Attribute keys and string values masked by redaction before offload, fingerprinting, or quarantine, by signal and match type."),
		metric.WithUnit("{value}")); err != nil {
		return nil, fmt.Errorf("creating %s: %w", nameRedactedValues, err)
	}
	if t.BackpressureLevel, err = m.Float64Gauge(nameBackpressureLevel,
		metric.WithDescription("Current adaptive AIMD backpressure level in [0.0, 1.0]."),
		metric.WithUnit("1")); err != nil {
		return nil, fmt.Errorf("creating %s: %w", nameBackpressureLevel, err)
	}
	if t.EffectiveRequestBytes, err = m.Int64Gauge(nameEffectiveRequestBytes,
		metric.WithDescription("Current effective per-request byte split threshold."),
		metric.WithUnit("By")); err != nil {
		return nil, fmt.Errorf("creating %s: %w", nameEffectiveRequestBytes, err)
	}
	if t.BackpressureEvents, err = m.Int64Counter(nameBackpressureEvents,
		metric.WithDescription("Count of adaptive AIMD backpressure step-down and recovery transitions."),
		metric.WithUnit("{event}")); err != nil {
		return nil, fmt.Errorf("creating %s: %w", nameBackpressureEvents, err)
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

// RecordInflightRequests adjusts the count of dispatched append results that
// have not reached an exporter-owned terminal state. delta is positive when a
// result enters that state and negative when it leaves.
func (t *Telemetry) RecordInflightRequests(ctx context.Context, delta int64) {
	if t == nil || delta == 0 {
		return
	}
	t.InflightRequests.Add(ctx, delta)
}

// RecordUnresolvedResults adjusts the count of append results retained by
// lifecycle drainers after their exporterhelper attempt ended. delta is
// positive when a result is handed to a drainer and negative when it resolves.
func (t *Telemetry) RecordUnresolvedResults(ctx context.Context, delta int64) {
	if t == nil || delta == 0 {
		return
	}
	t.UnresolvedResults.Add(ctx, delta)
}

// RecordAppendResultWait records the time from append dispatch until the
// result owner finishes.
func (t *Telemetry) RecordAppendResultWait(ctx context.Context, elapsed time.Duration) {
	if t == nil {
		return
	}
	t.AppendResultWait.Record(ctx, elapsed.Seconds())
}

// RecordTimeoutAfterDispatch records an append attempt whose context ended
// after dispatch began.
func (t *Telemetry) RecordTimeoutAfterDispatch(ctx context.Context) {
	if t == nil {
		return
	}
	t.TimeoutsAfterDispatch.Add(ctx, 1)
}

// RecordStreamRetirement records a managed stream generation retired after an
// unsafe outcome.
func (t *Telemetry) RecordStreamRetirement(ctx context.Context) {
	if t == nil {
		return
	}
	t.StreamRetirements.Add(ctx, 1)
}

// RecordQuarantined records rows acknowledged by the quarantine stream under a bounded reason.
func (t *Telemetry) RecordQuarantined(ctx context.Context, reason string, n int) {
	if t == nil || n <= 0 {
		return
	}
	t.QuarantinedRows.Add(ctx, int64(n), metric.WithAttributes(attribute.String(AttrReason, reason)))
}

// RecordQuarantineFailed records quarantine rows that could not be written under a bounded reason.
func (t *Telemetry) RecordQuarantineFailed(ctx context.Context, reason string, n int) {
	if t == nil || n <= 0 {
		return
	}
	t.QuarantineFailedRows.Add(ctx, int64(n), metric.WithAttributes(attribute.String(AttrReason, reason)))
}

// RecordAppendBytes records serialized protobuf wire bytes dispatched in an AppendRows call.
func (t *Telemetry) RecordAppendBytes(ctx context.Context, n int64) {
	if t == nil || n <= 0 {
		return
	}
	t.AppendBytes.Add(ctx, n)
}

// RecordOffloadedPayloads records payloads offloaded to external object storage.
func (t *Telemetry) RecordOffloadedPayloads(ctx context.Context, n int) {
	if t == nil || n <= 0 {
		return
	}
	t.OffloadedPayloads.Add(ctx, int64(n))
}

// RecordOffloadedBytes records payload bytes offloaded to external object storage.
func (t *Telemetry) RecordOffloadedBytes(ctx context.Context, n int64) {
	if t == nil || n <= 0 {
		return
	}
	t.OffloadedBytes.Add(ctx, n)
}

// RecordOffloadDedupHit records payload offload requests satisfied from the
// intra-batch or cross-batch generation cache without a GCS upload.
func (t *Telemetry) RecordOffloadDedupHit(ctx context.Context, signal string, n int) {
	if t == nil || n <= 0 {
		return
	}
	t.OffloadDedupHits.Add(ctx, int64(n), metric.WithAttributes(attribute.String(AttrSignal, signal)))
}

// RecordOffloadError records a payload offload failure under a bounded error class.
func (t *Telemetry) RecordOffloadError(ctx context.Context, class string, n int) {
	if t == nil || n <= 0 {
		return
	}
	t.OffloadErrors.Add(ctx, int64(n), metric.WithAttributes(attribute.String(AttrClass, class)))
}

// RecordRedactedValues records keys or values masked by redaction. matchType
// is "key" or "value"; signal is the pipeline signal name. Neither the key
// name nor the value ever reaches the metric.
func (t *Telemetry) RecordRedactedValues(ctx context.Context, signal, matchType string, n int64) {
	if t == nil || n <= 0 {
		return
	}
	t.RedactedValues.Add(ctx, n, metric.WithAttributes(
		attribute.String(AttrSignal, signal),
		attribute.String(AttrMatchType, matchType),
	))
}

// RecordBackpressureLevel records the current AIMD backpressure level in [0.0, 1.0].
func (t *Telemetry) RecordBackpressureLevel(ctx context.Context, signal, table string, level float64) {
	if t == nil {
		return
	}
	t.BackpressureLevel.Record(ctx, level, metric.WithAttributes(
		attribute.String(AttrSignal, signal),
		attribute.String(AttrTable, table),
	))
}

// RecordEffectiveRequestBytes records the current AIMD per-request byte split threshold.
func (t *Telemetry) RecordEffectiveRequestBytes(ctx context.Context, signal, table string, bytes int64) {
	if t == nil || bytes <= 0 {
		return
	}
	t.EffectiveRequestBytes.Record(ctx, bytes, metric.WithAttributes(
		attribute.String(AttrSignal, signal),
		attribute.String(AttrTable, table),
	))
}

// RecordBackpressureEvent records one AIMD step-down or recovery transition.
func (t *Telemetry) RecordBackpressureEvent(ctx context.Context, signal, table, trigger string) {
	if t == nil {
		return
	}
	t.BackpressureEvents.Add(ctx, 1, metric.WithAttributes(
		attribute.String(AttrSignal, signal),
		attribute.String(AttrTable, table),
		attribute.String(AttrTrigger, trigger),
	))
}
