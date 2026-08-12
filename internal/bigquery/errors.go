// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigquery // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/bigquery"

import (
	"context"
	"errors"
	"io"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Class is how the exporter must react to a failure.
type Class int

const (
	// Permanent will never succeed on retry. The affected rows or batch are
	// counted and dropped; v1 has no dead-letter destination, so "reject"
	// means drop.
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
// same failure multiplies attempts and makes the duplicate bound in §8.4
// meaningless.
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

// Verdict is one row of the §8.3 error matrix.
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
	LabelUnavailable     = "unavailable"
	LabelInternal        = "internal"
	LabelPermission      = "permission_denied"
	LabelAuth            = "credential"
	LabelInvalidStream   = "invalid_stream"
	LabelSchemaMismatch  = "schema_mismatch"
	LabelInvalidArgument = "invalid_argument"
	LabelUncertainAck    = "uncertain_ack"
	LabelConnection      = "connection"
	LabelShutdown        = "shutdown_timeout"
	LabelUnknown         = "unknown"
)

// throughputQuotaPrefix is the message prefix BigQuery uses for AppendRows
// throughput exhaustion.
//
// Matching on the message text is fragile, but the service does not return a
// structured quota failure for this case, so it is the only way to tell a
// retryable throughput cap apart from a permanent resource-exhaustion error.
// The Write API team's own Fluent Bit sink carries the same workaround.
const throughputQuotaPrefix = "Exceeds 'AppendRows throughput' quota"

// schemaMismatchMarkers appear in INVALID_ARGUMENT messages when the row
// descriptor no longer matches the destination table.
var schemaMismatchMarkers = []string{
	"schema mismatch",
	"Schema mismatch",
	"input schema has more fields than BigQuery schema",
}

// Classify maps an append error onto the §8.3 matrix.
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
			return Verdict{Retryable, OwnerExporterHelper, LabelShutdown, false}
		default:
			return Verdict{Permanent, OwnerExporter, LabelUnknown, false}
		}
	}

	switch s.Code() {
	case codes.ResourceExhausted:
		if strings.HasPrefix(s.Message(), throughputQuotaPrefix) {
			return Verdict{Retryable, OwnerExporterHelper, LabelQuota, false}
		}
		return Verdict{Permanent, OwnerExporter, LabelQuota, false}

	case codes.Unavailable:
		return Verdict{Retryable, OwnerExporterHelper, LabelUnavailable, false}

	case codes.Internal, codes.Aborted:
		return Verdict{Retryable, OwnerExporterHelper, LabelInternal, false}

	case codes.DeadlineExceeded:
		return Verdict{Uncertain, OwnerExporterHelper, LabelUncertainAck, false}

	case codes.Canceled:
		return Verdict{Retryable, OwnerExporterHelper, LabelShutdown, false}

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

// PermissionHint renders an actionable message for an authorization failure,
// naming the permission and the identity rather than echoing the raw error.
func PermissionHint(table string) string {
	return "append denied on " + table +
		": the writing identity needs bigquery.tables.updateData on this table" +
		" (the write-only custom role, or roles/bigquery.dataEditor)"
}
