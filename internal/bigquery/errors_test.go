// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigquery

import (
	"context"
	"errors"
	"io"
	"testing"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func mustStatusWithStorageError(t *testing.T, c codes.Code, msg string, seCode storagepb.StorageError_StorageErrorCode) error {
	t.Helper()
	st, err := status.New(c, msg).WithDetails(&storagepb.StorageError{Code: seCode})
	if err != nil {
		t.Fatalf("WithDetails: %v", err)
	}
	return st.Err()
}

// One row per line of the error matrix that Classify implements. Class and
// owner together are the contract: two components retrying the same failure
// would multiply attempts and break the duplicate bound.
func TestClassifyMatrix(t *testing.T) {
	tests := map[string]struct {
		err      error
		class    Class
		owner    Owner
		label    string
		recreate bool
	}{
		"throughput quota exhaustion is retryable": {
			status.Error(codes.ResourceExhausted, throughputQuotaPrefix+" for project x"),
			Retryable, OwnerExporterHelper, LabelQuota, false,
		},
		"concurrent connections quota exhaustion is retryable": {
			status.Error(codes.ResourceExhausted, "Exceeds 'Concurrent connections' quota for project x"),
			Retryable, OwnerExporterHelper, LabelQuota, false,
		},
		"rate limit resource exhaustion is retryable": {
			status.Error(codes.ResourceExhausted, "Rate limit exceeded"),
			Retryable, OwnerExporterHelper, LabelQuota, false,
		},
		"oversize grpc message resource exhaustion is permanent": {
			status.Error(codes.ResourceExhausted, "grpc: received message larger than max (10485760 vs. 10000000)"),
			Permanent, OwnerExporter, LabelQuota, false,
		},
		"oversize request payload resource exhaustion is permanent": {
			status.Error(codes.ResourceExhausted, "Request payload size exceeds the limit: 10485760 bytes."),
			Permanent, OwnerExporter, LabelQuota, false,
		},
		"appendrows request size quota is permanent": {
			status.Error(codes.ResourceExhausted, "Exceeds 'AppendRows request size' quota"),
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
		"structured StorageError SCHEMA_MISMATCH_EXTRA_FIELDS triggers recreate without schema wording": {
			mustStatusWithStorageError(t, codes.InvalidArgument, "extra fields present", storagepb.StorageError_SCHEMA_MISMATCH_EXTRA_FIELDS),
			Permanent, OwnerExporter, LabelSchemaMismatch, true,
		},
		"structured StorageError INVALID_STREAM_STATE triggers recreate": {
			mustStatusWithStorageError(t, codes.FailedPrecondition, "bad state", storagepb.StorageError_INVALID_STREAM_STATE),
			Permanent, OwnerExporter, LabelInvalidStream, true,
		},
		"structured StorageError STREAM_NOT_FOUND triggers recreate": {
			mustStatusWithStorageError(t, codes.NotFound, "entity not found", storagepb.StorageError_STREAM_NOT_FOUND),
			Permanent, OwnerExporter, LabelInvalidStream, true,
		},
		"structured StorageError STREAM_FINALIZED triggers recreate": {
			mustStatusWithStorageError(t, codes.FailedPrecondition, "closed", storagepb.StorageError_STREAM_FINALIZED),
			Permanent, OwnerExporter, LabelInvalidStream, true,
		},
		"structured StorageError TABLE_NOT_FOUND triggers recreate": {
			mustStatusWithStorageError(t, codes.NotFound, "table missing", storagepb.StorageError_TABLE_NOT_FOUND),
			Permanent, OwnerExporter, LabelInvalidStream, true,
		},
		"structured StorageError OFFSET_OUT_OF_RANGE overrides Schema mismatch message": {
			mustStatusWithStorageError(t, codes.InvalidArgument, "Schema mismatch for field foo", storagepb.StorageError_OFFSET_OUT_OF_RANGE),
			Permanent, OwnerExporter, LabelInvalidArgument, false,
		},
		"structured StorageError STREAM_NOT_FOUND overrides Internal status code": {
			mustStatusWithStorageError(t, codes.Internal, "internal stream failure", storagepb.StorageError_STREAM_NOT_FOUND),
			Permanent, OwnerExporter, LabelInvalidStream, true,
		},
		"structured StorageError STORAGE_ERROR_CODE_UNSPECIFIED falls through to message/code heuristic": {
			mustStatusWithStorageError(t, codes.InvalidArgument, "Schema mismatch for field foo", storagepb.StorageError_STORAGE_ERROR_CODE_UNSPECIFIED),
			Permanent, OwnerExporter, LabelSchemaMismatch, true,
		},
		"structured StorageError CMEK_NOT_PROVIDED is permanent invalid argument": {
			mustStatusWithStorageError(t, codes.InvalidArgument, "cmek missing", storagepb.StorageError_CMEK_NOT_PROVIDED),
			Permanent, OwnerExporter, LabelInvalidArgument, false,
		},
		"structured StorageError KMS_PERMISSION_DENIED is permanent permission": {
			mustStatusWithStorageError(t, codes.PermissionDenied, "kms denied", storagepb.StorageError_KMS_PERMISSION_DENIED),
			Permanent, OwnerExporter, LabelPermission, false,
		},
		"structured StorageError KMS_SERVICE_ERROR is retryable unavailable": {
			mustStatusWithStorageError(t, codes.Internal, "kms transient", storagepb.StorageError_KMS_SERVICE_ERROR),
			Retryable, OwnerExporterHelper, LabelUnavailable, false,
		},
		"connection close is retryable": {
			io.EOF, Retryable, OwnerExporterHelper, LabelConnection, false,
		},
		"context deadline is uncertain": {
			context.DeadlineExceeded, Uncertain, OwnerExporterHelper, LabelUncertainAck, false,
		},
		"runtime context cancellation is not mislabeled as shutdown": {
			context.Canceled, Retryable, OwnerExporterHelper, LabelCanceled, false,
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
		status.Error(codes.ResourceExhausted, "Exceeds 'Concurrent connections' quota"),
		status.Error(codes.ResourceExhausted, "grpc: received message larger than max"),
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

func TestClassifySubRequestOversizeWithBackpressure(t *testing.T) {
	oversizeErr := status.Error(codes.ResourceExhausted, "Request payload size exceeds the limit: 10485760 bytes.")

	t.Run("multi-row oversize with backpressure enabled is retryable", func(t *testing.T) {
		v := ClassifySubRequest(oversizeErr, 4, true)
		if v.Class != Retryable || v.Owner != OwnerExporterHelper || v.Label != LabelOversizeRequest {
			t.Fatalf("ClassifySubRequest(multi-row, enabled) = %+v, want {Retryable, OwnerExporterHelper, oversize_request}", v)
		}
	})

	t.Run("single-row oversize with backpressure enabled stays permanent", func(t *testing.T) {
		v := ClassifySubRequest(oversizeErr, 1, true)
		if v.Class != Permanent || v.Owner != OwnerExporter || v.Label != LabelQuota {
			t.Fatalf("ClassifySubRequest(single-row, enabled) = %+v, want {Permanent, OwnerExporter, quota_exhausted}", v)
		}
	})

	t.Run("multi-row oversize with backpressure disabled stays permanent", func(t *testing.T) {
		v := ClassifySubRequest(oversizeErr, 4, false)
		if v.Class != Permanent || v.Owner != OwnerExporter || v.Label != LabelQuota {
			t.Fatalf("ClassifySubRequest(multi-row, disabled) = %+v, want {Permanent, OwnerExporter, quota_exhausted}", v)
		}
	})

	t.Run("throughput quota exhaustion with backpressure enabled stays quota_exhausted", func(t *testing.T) {
		quotaErr := status.Error(codes.ResourceExhausted, throughputQuotaPrefix+" for project x")
		v := ClassifySubRequest(quotaErr, 4, true)
		if v.Class != Retryable || v.Owner != OwnerExporterHelper || v.Label != LabelQuota {
			t.Fatalf("ClassifySubRequest(throughput quota, enabled) = %+v, want {Retryable, OwnerExporterHelper, quota_exhausted}", v)
		}
	})
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
