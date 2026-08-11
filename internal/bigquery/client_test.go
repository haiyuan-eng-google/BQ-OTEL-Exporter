// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigquery

import (
	"errors"
	"io"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestClassify(t *testing.T) {
	tests := map[string]struct {
		err  error
		want ErrorClass
	}{
		"unavailable is retryable":       {status.Error(codes.Unavailable, "backend down"), Retryable},
		"internal is retryable":          {status.Error(codes.Internal, "oops"), Retryable},
		"deadline is uncertain":          {status.Error(codes.DeadlineExceeded, "too slow"), Uncertain},
		"permission denied is permanent": {status.Error(codes.PermissionDenied, "no"), Permanent},
		"invalid argument is permanent":  {status.Error(codes.InvalidArgument, "bad row"), Permanent},
		"eof is retryable":               {io.EOF, Retryable},
		"random error is permanent":      {errors.New("boom"), Permanent},
		"throughput quota is retryable": {
			status.Error(codes.ResourceExhausted, throughputQuotaPrefix+" for project X"), Retryable,
		},
		"other resource exhaustion is permanent": {
			status.Error(codes.ResourceExhausted, "table row limit reached"), Permanent,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := Classify(tc.err); got != tc.want {
				t.Fatalf("Classify(%v) = %s, want %s", tc.err, got, tc.want)
			}
		})
	}
}

func TestTableRef(t *testing.T) {
	o := WriterOptions{Project: "p", Dataset: "d", Table: "t"}
	if got, want := o.TableRef(), "projects/p/datasets/d/tables/t"; got != want {
		t.Fatalf("TableRef() = %q, want %q", got, want)
	}
}
