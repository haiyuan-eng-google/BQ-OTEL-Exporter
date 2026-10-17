// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigquery // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/bigquery"

import (
	"context"
	"errors"
	"io"
	"strings"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Class is how the exporter must react to a failure.
type Class int

const (
	// Permanent will never succeed on retry. The affected rows or batch are
	// dropped instead of retried. Rows the service rejects one by one are also
	// counted and, when quarantine is enabled, written to the quarantine table.
	Permanent Class = iota
	// Retryable is transient and should be retried with backoff.
	Retryable
	// Uncertain means the append may or may not have been applied. Replaying
	// it can duplicate rows, which the at-least-once contract permits and the
	// dedup SQL cleans up.
	Uncertain
)

func (c Class) String() string {
	switch c {
	case Permanent:
		return "permanent"
	case Retryable:
		return "retryable"
	case Uncertain:
		return "uncertain"
	default:
		return "unknown"
	}
}

// Owner names the component responsible for acting on a failure.
//
// Exactly one owner per class is the whole point: two components retrying the
// same failure multiplies attempts and makes the duplicate bound described in
// docs/schema-rationale.md meaningless.
type Owner int

const (
	// OwnerExporter handles the failure in-process: drop, fail fast, or
	// recreate the stream.
	OwnerExporter Owner = iota
	// OwnerExporterHelper means the exporter returns the error and lets
	// exporterhelper own backoff and replay.
	OwnerExporterHelper
)

func (o Owner) String() string {
	switch o {
	case OwnerExporter:
		return "exporter"
	case OwnerExporterHelper:
		return "exporterhelper"
	default:
		return "unknown"
	}
}

// Verdict is one row of the error matrix that Classify implements.
type Verdict struct {
	Class Class
	Owner Owner
	// Label is the bounded metric label for this failure kind. It must never
	// carry anything derived from telemetry content.
	Label string
	// StreamRecreate indicates the stream is unusable and should be rebuilt
	// once before the batch is failed.
	StreamRecreate bool
}

// Metric labels. Bounded by construction: this list is the whole domain.
const (
	LabelQuota           = "quota_exhausted"
	LabelOversizeRequest = "oversize_request"
	LabelUnavailable     = "unavailable"
	LabelInternal        = "internal"
	LabelPermission      = "permission_denied"
	LabelAuth            = "credential"
	LabelInvalidStream   = "invalid_stream"
	LabelSchemaMismatch  = "schema_mismatch"
	LabelInvalidArgument = "invalid_argument"
	LabelUncertainAck    = "uncertain_ack"
	LabelConnection      = "connection"
	LabelCanceled        = "request_canceled"
	LabelShutdown        = "shutdown_timeout"
	LabelUnknown         = "unknown"
)

// throughputQuotaPrefix is the message prefix BigQuery uses for AppendRows
// throughput exhaustion. Kept for tests that reference the canonical message.
const throughputQuotaPrefix = "Exceeds 'AppendRows throughput' quota"

// oversizeResourceExhaustedSubstrings identify ResourceExhausted errors caused
// by a single message or request exceeding a static size limit rather than
// transient quota, concurrent-connection, or rate-limit contention.
var oversizeResourceExhaustedSubstrings = []string{
	"message larger than max",
	"request payload size exceeds",
	"exceeds 'appendrows request size'",
	"request size limit",
}

func isOversizeResourceExhausted(msg string) bool {
	lower := strings.ToLower(msg)
	for _, sub := range oversizeResourceExhaustedSubstrings {
		if strings.Contains(lower, sub) {
			return true
		}
	}
	return false
}

// schemaMismatchMarkers appear in INVALID_ARGUMENT messages when the row
// descriptor no longer matches the destination table.
var schemaMismatchMarkers = []string{
	"schema mismatch",
	"Schema mismatch",
	"input schema has more fields than BigQuery schema",
}

// Classify maps an append error onto the error matrix: the failure class, the
// component that owns the reaction, and a bounded metric label.
func Classify(err error) Verdict {
	if err == nil {
		return Verdict{Permanent, OwnerExporter, LabelUnknown, false}
	}

	s, ok := status.FromError(err)
	if !ok {
		switch {
		case errors.Is(err, io.EOF):
			// Connection close is expected under normal operation.
			return Verdict{Retryable, OwnerExporterHelper, LabelConnection, false}
		case errors.Is(err, context.DeadlineExceeded):
			// The server may have applied the append before the deadline
			// fired, so a replay can duplicate.
			return Verdict{Uncertain, OwnerExporterHelper, LabelUncertainAck, false}
		case errors.Is(err, context.Canceled):
			return Verdict{Retryable, OwnerExporterHelper, LabelCanceled, false}
		default:
			return Verdict{Permanent, OwnerExporter, LabelUnknown, false}
		}
	}

	for _, detail := range s.Details() {
		se, ok := detail.(*storagepb.StorageError)
		if !ok || se == nil {
			continue
		}
		switch se.GetCode() {
		case storagepb.StorageError_SCHEMA_MISMATCH_EXTRA_FIELDS:
			return Verdict{Permanent, OwnerExporter, LabelSchemaMismatch, true}
		case storagepb.StorageError_STREAM_NOT_FOUND,
			storagepb.StorageError_INVALID_STREAM_STATE,
			storagepb.StorageError_INVALID_STREAM_TYPE,
			storagepb.StorageError_STREAM_ALREADY_COMMITTED,
			storagepb.StorageError_STREAM_FINALIZED,
			storagepb.StorageError_TABLE_NOT_FOUND:
			return Verdict{Permanent, OwnerExporter, LabelInvalidStream, true}
		case storagepb.StorageError_OFFSET_ALREADY_EXISTS,
			storagepb.StorageError_OFFSET_OUT_OF_RANGE,
			storagepb.StorageError_CMEK_NOT_PROVIDED,
			storagepb.StorageError_INVALID_CMEK_PROVIDED,
			storagepb.StorageError_CMEK_ENCRYPTION_ERROR:
			return Verdict{Permanent, OwnerExporter, LabelInvalidArgument, false}
		case storagepb.StorageError_KMS_PERMISSION_DENIED:
			return Verdict{Permanent, OwnerExporter, LabelPermission, false}
		case storagepb.StorageError_KMS_SERVICE_ERROR:
			return Verdict{Retryable, OwnerExporterHelper, LabelUnavailable, false}
		}
	}

	switch s.Code() {
	case codes.ResourceExhausted:
		// Static message/request size overruns cannot succeed on retry with
		// the same payload. All other ResourceExhausted statuses (throughput
		// quota, concurrent connection quota, rate limits) are transient.
		if isOversizeResourceExhausted(s.Message()) {
			return Verdict{Permanent, OwnerExporter, LabelQuota, false}
		}
		return Verdict{Retryable, OwnerExporterHelper, LabelQuota, false}

	case codes.Unavailable:
		return Verdict{Retryable, OwnerExporterHelper, LabelUnavailable, false}

	case codes.Internal, codes.Aborted:
		return Verdict{Retryable, OwnerExporterHelper, LabelInternal, false}

	case codes.DeadlineExceeded:
		return Verdict{Uncertain, OwnerExporterHelper, LabelUncertainAck, false}

	case codes.Canceled:
		return Verdict{Retryable, OwnerExporterHelper, LabelCanceled, false}

	case codes.PermissionDenied:
		// Fail fast and name the permission: an operator cannot fix this by
		// waiting, and burning retries hides the real problem.
		return Verdict{Permanent, OwnerExporter, LabelPermission, false}

	case codes.Unauthenticated:
		// Credential refresh is bounded-retryable at the auth layer.
		return Verdict{Retryable, OwnerExporterHelper, LabelAuth, false}

	case codes.NotFound:
		return Verdict{Permanent, OwnerExporter, LabelInvalidStream, true}

	case codes.FailedPrecondition:
		// Typically an invalid or finalized stream; one recreate attempt is
		// worth making before failing the batch.
		return Verdict{Permanent, OwnerExporter, LabelInvalidStream, true}

	case codes.InvalidArgument:
		for _, m := range schemaMismatchMarkers {
			if strings.Contains(s.Message(), m) {
				return Verdict{Permanent, OwnerExporter, LabelSchemaMismatch, true}
			}
		}
		return Verdict{Permanent, OwnerExporter, LabelInvalidArgument, false}

	default:
		return Verdict{Permanent, OwnerExporter, LabelUnknown, false}
	}
}

// ClassifySubRequest classifies an AppendRows sub-request error. When adaptive
// backpressure can still split the batch smaller (canSplitSmaller is true) and
// a multi-row sub-request hits a request-size ResourceExhausted error, it
// returns a Retryable verdict owned by exporterhelper so the batch is replayed
// at a smaller split threshold. A single-row oversize error, an oversize error
// already at the minimum split threshold, or a disabled backpressure
// configuration keeps the permanent Classify verdict.
func ClassifySubRequest(err error, rowCount int, canSplitSmaller bool) Verdict {
	v := Classify(err)
	if canSplitSmaller && rowCount > 1 && v.Class == Permanent && v.Label == LabelQuota {
		if s, ok := status.FromError(err); ok && s.Code() == codes.ResourceExhausted && isOversizeResourceExhausted(s.Message()) {
			return Verdict{Retryable, OwnerExporterHelper, LabelOversizeRequest, false}
		}
	}
	return v
}

// PermissionHint renders an actionable message for an authorization failure,
// naming the permission and the identity rather than echoing the raw error.
func PermissionHint(table string) string {
	return "append denied on " + table +
		": the writing identity needs bigquery.tables.updateData on this table" +
		" (the write-only custom role, or roles/bigquery.dataEditor)"
}
