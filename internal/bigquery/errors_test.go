// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigquery

import (
	"context"
	"errors"
	"io"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// One row per line of the §8.3 error matrix. Class and owner together are the
// contract: two components retrying the same failure would multiply attempts
// and break the duplicate bound.
func TestClassifyMatrix(t *testing.T) {
	tests := map[string]struct {
		err      error
		class    Class
		owner    Owner
		label    string
		recreate bool
	}{
		"quota exhaustion is retryable": {
			status.Error(codes.ResourceExhausted, throughputQuotaPrefix+" for project x"),
			Retryable, OwnerExporterHelper, LabelQuota, false,
		},
		"other resource exhaustion is permanent": {
			status.Error(codes.ResourceExhausted, "table row limit reached"),
			Permanent, OwnerExporter, LabelQuota, false,
		},
		"unavailable is retryable": {
			status.Error(codes.Unavailable, "backend down"),
			Retryable, OwnerExporterHelper, LabelUnavailable, false,
		},
		"internal is retryable": {
			status.Error(codes.Internal, "oops"),
			Retryable, OwnerExporterHelper, LabelInternal, false,
		},
		"deadline mid-append is uncertain": {
			status.Error(codes.DeadlineExceeded, "too slow"),
			Uncertain, OwnerExporterHelper, LabelUncertainAck, false,
		},
		"permission denied fails fast": {
			status.Error(codes.PermissionDenied, "nope"),
			Permanent, OwnerExporter, LabelPermission, false,
		},
		"credential failure is retryable": {
			status.Error(codes.Unauthenticated, "token expired"),
			Retryable, OwnerExporterHelper, LabelAuth, false,
		},
		"invalid stream triggers one recreate": {
			status.Error(codes.FailedPrecondition, "stream is finalized"),
			Permanent, OwnerExporter, LabelInvalidStream, true,
		},
		"schema mismatch triggers recreate": {
			status.Error(codes.InvalidArgument, "Schema mismatch for field foo"),
			Permanent, OwnerExporter, LabelSchemaMismatch, true,
		},
		"plain invalid argument is permanent": {
			status.Error(codes.InvalidArgument, "malformed row"),
			Permanent, OwnerExporter, LabelInvalidArgument, false,
		},
		"connection close is retryable": {
			io.EOF, Retryable, OwnerExporterHelper, LabelConnection, false,
		},
		"context deadline is uncertain": {
			context.DeadlineExceeded, Uncertain, OwnerExporterHelper, LabelUncertainAck, false,
		},
		"unknown error is permanent": {
			errors.New("boom"), Permanent, OwnerExporter, LabelUnknown, false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := Classify(tc.err)
			if got.Class != tc.class {
				t.Errorf("class = %s, want %s", got.Class, tc.class)
			}
			if got.Owner != tc.owner {
				t.Errorf("owner = %s, want %s", got.Owner, tc.owner)
			}
			if got.Label != tc.label {
				t.Errorf("label = %s, want %s", got.Label, tc.label)
			}
			if got.StreamRecreate != tc.recreate {
				t.Errorf("stream recreate = %v, want %v", got.StreamRecreate, tc.recreate)
			}
		})
	}
}

// Every retryable failure must be owned by exporterhelper and every permanent
// one by the exporter. A retryable failure owned in-process would retry
// underneath exporterhelper's own retry and multiply attempts.
func TestRetryOwnershipIsConsistent(t *testing.T) {
	errs := []error{
		status.Error(codes.Unavailable, ""),
		status.Error(codes.Internal, ""),
		status.Error(codes.DeadlineExceeded, ""),
		status.Error(codes.PermissionDenied, ""),
		status.Error(codes.InvalidArgument, ""),
		status.Error(codes.ResourceExhausted, throughputQuotaPrefix),
		io.EOF,
		errors.New("x"),
	}
	for _, err := range errs {
		v := Classify(err)
		switch v.Class {
		case Retryable, Uncertain:
			if v.Owner != OwnerExporterHelper {
				t.Errorf("%v is %s but owned by %s; replay must have exactly one owner",
					err, v.Class, v.Owner)
			}
		case Permanent:
			if v.Owner != OwnerExporter {
				t.Errorf("%v is permanent but owned by %s", err, v.Owner)
			}
		}
	}
}

// The hint has to name the permission an operator must grant. "Permission
// denied" alone sends people to the wrong place.
func TestPermissionHintNamesThePermission(t *testing.T) {
	got := PermissionHint("p.d.t")
	for _, want := range []string{"p.d.t", "bigquery.tables.updateData"} {
		if !contains(got, want) {
			t.Errorf("permission hint %q should mention %q", got, want)
		}
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}
