// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

// Package bigquery wraps the Storage Write API managedwriter client with the
// stream management, request sizing and error classification this exporter
// needs.
package bigquery // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/bigquery"

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// StreamMode selects the Storage Write API stream type.
type StreamMode int

const (
	// DefaultStream is offset-free and gives at-least-once delivery. It is the
	// v1 choice: it needs no connection or offset bookkeeping, and measured
	// duplicate rates on it are small enough that read-time deduplication is
	// the proportionate answer for most consumers.
	DefaultStream StreamMode = iota
	// CommittedStream with explicit per-stream offsets is the exactly-once
	// path. Not wired up in v1; recorded so the option has a shape.
	CommittedStream
)

// WriterOptions configures a destination stream.
type WriterOptions struct {
	Project string
	Dataset string
	Table   string

	Mode StreamMode

	MaxRequestBytes     int
	MaxInflightRequests int
	MaxInflightBytes    int

	// TraceID identifies this client to the Storage Write API backend, which
	// makes producer-side attribution possible when the service team is
	// debugging on our behalf.
	TraceID string
}

// TableRef renders the fully qualified destination table path.
func (o WriterOptions) TableRef() string {
	return fmt.Sprintf("projects/%s/datasets/%s/tables/%s", o.Project, o.Dataset, o.Table)
}

// ErrorClass describes how the exporter should react to an append failure.
type ErrorClass int

const (
	// Permanent errors will never succeed on retry. The offending records are
	// counted and dropped rather than retried forever.
	Permanent ErrorClass = iota
	// Retryable errors are transient and should be retried with backoff.
	Retryable
	// Uncertain means the append may or may not have been applied. Replaying
	// it can duplicate rows, which the delivery contract permits.
	Uncertain
)

func (c ErrorClass) String() string {
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

// throughputQuotaPrefix is the message prefix BigQuery uses for AppendRows
// throughput exhaustion. Matching on the message is fragile, but the service
// does not yet return a structured quota failure for this case, so a string
// match is the only way to tell a retryable throughput cap apart from a
// permanent resource-exhaustion error.
const throughputQuotaPrefix = "Exceeds 'AppendRows throughput' quota"

// Classify maps an append error onto the exporter's error matrix.
func Classify(err error) ErrorClass {
	if err == nil {
		return Permanent
	}

	s, ok := status.FromError(err)
	if !ok {
		// A closed connection is expected under normal operation and the
		// append can safely be re-sent.
		if errors.Is(err, io.EOF) {
			return Retryable
		}
		return Permanent
	}

	switch s.Code() {
	case codes.Aborted, codes.Canceled, codes.Internal, codes.Unavailable:
		return Retryable
	case codes.DeadlineExceeded:
		// The server may have applied the append before the deadline fired.
		return Uncertain
	case codes.ResourceExhausted:
		if strings.HasPrefix(s.Message(), throughputQuotaPrefix) {
			return Retryable
		}
		return Permanent
	case codes.PermissionDenied, codes.Unauthenticated,
		codes.InvalidArgument, codes.NotFound, codes.FailedPrecondition:
		return Permanent
	default:
		return Permanent
	}
}
