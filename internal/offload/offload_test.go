// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package offload

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/api/option"
)

var fixedOffloadTime = time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)

func TestNewValidation(t *testing.T) {
	store := NewMemoryBlobStore()
	if _, err := New(store, Options{MinBytes: 10, MaxObjectBytes: 20}); err == nil {
		t.Fatal("expected error for empty bucket")
	}
	if _, err := New(store, Options{Bucket: "b", MinBytes: 0, MaxObjectBytes: 20}); err == nil {
		t.Fatal("expected error for non-positive min_bytes")
	}
	if _, err := New(store, Options{Bucket: "b", MinBytes: 30, MaxObjectBytes: 20}); err == nil {
		t.Fatal("expected error when max_object_bytes < min_bytes")
	}
}

func TestOffloadSpanAndLog(t *testing.T) {
	store := NewMemoryBlobStore()
	off, err := New(store, Options{
		Bucket:         "my-telemetry-bucket",
		Prefix:         "custom-prefix",
		Authorizer:     "projects/p/locations/us/connections/bq-gcs",
		MinBytes:       32,
		MaxObjectBytes: 1024,
		Timeout:        time.Second,
		Now:            func() time.Time { return fixedOffloadTime },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	largePrompt := strings.Repeat("A", 64)
	sum := sha256.Sum256([]byte(largePrompt))
	wantSHA := hex.EncodeToString(sum[:])
	wantObj := "custom-prefix/spans/2026-10-11/" + wantSHA
	wantURI := "gs://my-telemetry-bucket/" + wantObj

	span := ptrace.NewSpan()
	span.Attributes().PutStr("gen_ai.prompt", largePrompt)
	span.Attributes().PutStr("small", "keep-inline")
	span.Attributes().PutStr("too_huge", strings.Repeat("Z", 2048))
	ev := span.Events().AppendEmpty()
	ev.Attributes().PutEmptyBytes("raw_chunk").FromRaw([]byte(strings.Repeat("B", 48)))

	res := pcommon.NewResource()
	scope := pcommon.NewInstrumentationScope()

	refs, err := off.OffloadSpan(context.Background(), span, res, scope)
	if err != nil {
		t.Fatalf("OffloadSpan() error = %v", err)
	}
	if len(refs) != 2 {
		t.Fatalf("OffloadSpan returned %d refs, want 2: %+v", len(refs), refs)
	}
	if refs[0].AttributePath != "attributes.gen_ai.prompt" || refs[0].ObjectRef.URI != wantURI {
		t.Errorf("refs[0] = %+v, want path attributes.gen_ai.prompt and URI %s", refs[0], wantURI)
	}
	if refs[0].ObjectRef.Authorizer != "projects/p/locations/us/connections/bq-gcs" {
		t.Errorf("refs[0].ObjectRef.Authorizer = %q", refs[0].ObjectRef.Authorizer)
	}
	if refs[0].StorageMode != "gcs_object_ref" || refs[0].ByteSize != 64 || refs[0].SHA256 != wantSHA {
		t.Errorf("refs[0] metadata mismatch: %+v", refs[0])
	}
	if got, _ := span.Attributes().Get("gen_ai.prompt"); got.Str() != "<offloaded:"+wantURI+">" {
		t.Errorf("span attribute gen_ai.prompt = %q, want offloaded placeholder", got.Str())
	}
	if got, _ := span.Attributes().Get("small"); got.Str() != "keep-inline" {
		t.Errorf("small attribute mutated to %q", got.Str())
	}
	if got, _ := span.Attributes().Get("too_huge"); len(got.Str()) != 2048 {
		t.Errorf("value exceeding MaxObjectBytes should remain untouched, got len %d", len(got.Str()))
	}
	if obj, ok := store.Get("my-telemetry-bucket", wantObj); !ok || string(obj.Data) != largePrompt {
		t.Errorf("stored object = %+v (ok=%v), want %q", obj, ok, largePrompt)
	}

	// Test OffloadLog with nested map/slice and body
	lr := plog.NewLogRecord()
	largeBody := strings.Repeat("L", 80)
	lr.Body().SetStr(largeBody)
	nested := lr.Attributes().PutEmptyMap("ctx")
	sl := nested.PutEmptySlice("items")
	sl.AppendEmpty().SetStr(strings.Repeat("I", 40))

	logRefs, err := off.OffloadLog(context.Background(), lr, res, scope)
	if err != nil {
		t.Fatalf("OffloadLog() error = %v", err)
	}
	if len(logRefs) != 2 {
		t.Fatalf("OffloadLog returned %d refs, want 2: %+v", len(logRefs), logRefs)
	}
	if logRefs[0].AttributePath != "body" || logRefs[1].AttributePath != "attributes.ctx.items[0]" {
		t.Errorf("logRefs paths = [%q, %q], want [body, attributes.ctx.items[0]]",
			logRefs[0].AttributePath, logRefs[1].AttributePath)
	}
	if !strings.HasPrefix(lr.Body().Str(), "<offloaded:gs://my-telemetry-bucket/custom-prefix/logs/2026-10-11/") {
		t.Errorf("offloaded log body = %q", lr.Body().Str())
	}
}

func TestOffloadQuarantineAndUploadErrors(t *testing.T) {
	store := NewMemoryBlobStore()
	off, err := New(store, Options{
		Bucket:         "quarantine-bucket",
		MinBytes:       16,
		MaxObjectBytes: 512,
		Now:            func() time.Time { return fixedOffloadTime },
	})
	if err != nil {
		t.Fatal(err)
	}

	if got := off.OffloadQuarantine(context.Background(), "spans", []byte("short")); got.URI != "" {
		t.Errorf("below MinBytes should return empty ObjectRef, got %+v", got)
	}

	payload := []byte(strings.Repeat("Q", 64))
	ref := off.OffloadQuarantine(context.Background(), "spans", payload)
	if !strings.HasPrefix(ref.URI, "gs://quarantine-bucket/otel-offload/quarantine/spans/2026-10-11/") || !strings.HasSuffix(ref.URI, ".json") {
		t.Errorf("OffloadQuarantine URI = %q", ref.URI)
	}

	// Quarantine upload failure stays best-effort (returns empty ObjectRef),
	// whereas signal OffloadSpan/OffloadLog propagate the upload error so the
	// caller can return the record as retryable.
	outage := errors.New("simulated GCS outage")
	store.SetError(outage)
	if got := off.OffloadQuarantine(context.Background(), "spans", payload); got.URI != "" {
		t.Errorf("expected empty ObjectRef on quarantine upload failure, got %+v", got)
	}
	span := ptrace.NewSpan()
	span.Attributes().PutStr("prompt", strings.Repeat("P", 64))
	if refs, sErr := off.OffloadSpan(context.Background(), span, pcommon.NewResource(), pcommon.NewInstrumentationScope()); !errors.Is(sErr, outage) || len(refs) != 0 {
		t.Errorf("OffloadSpan() = (%+v, %v), want (nil, %v)", refs, sErr, outage)
	}
	if got, _ := span.Attributes().Get("prompt"); got.Str() != strings.Repeat("P", 64) {
		t.Errorf("failed upload should leave attribute untouched, got %q", got.Str())
	}

	lr := plog.NewLogRecord()
	lr.Body().SetStr(strings.Repeat("B", 64))
	if refs, lErr := off.OffloadLog(context.Background(), lr, pcommon.NewResource(), pcommon.NewInstrumentationScope()); !errors.Is(lErr, outage) || len(refs) != 0 {
		t.Errorf("OffloadLog() = (%+v, %v), want (nil, %v)", refs, lErr, outage)
	}
}

func TestRepeatedContentAddressedUploadsPreserveGeneration(t *testing.T) {
	store := NewMemoryBlobStore()
	off, err := New(store, Options{
		Bucket:         "dedup-bucket",
		MinBytes:       16,
		MaxObjectBytes: 512,
		Now:            func() time.Time { return fixedOffloadTime },
	})
	if err != nil {
		t.Fatal(err)
	}

	payload := strings.Repeat("D", 64)
	sp1 := ptrace.NewSpan()
	sp1.Attributes().PutStr("prompt", payload)
	refs1, err := off.OffloadSpan(context.Background(), sp1, pcommon.NewResource(), pcommon.NewInstrumentationScope())
	if err != nil || len(refs1) != 1 {
		t.Fatalf("first OffloadSpan() = (%+v, %v)", refs1, err)
	}

	sp2 := ptrace.NewSpan()
	sp2.Attributes().PutStr("prompt", payload)
	refs2, err := off.OffloadSpan(context.Background(), sp2, pcommon.NewResource(), pcommon.NewInstrumentationScope())
	if err != nil || len(refs2) != 1 {
		t.Fatalf("second OffloadSpan() = (%+v, %v)", refs2, err)
	}

	if refs1[0].ObjectRef.Version == "" || refs1[0].ObjectRef.Version != refs2[0].ObjectRef.Version {
		t.Errorf("generations = (%q, %q), want identical non-empty generation", refs1[0].ObjectRef.Version, refs2[0].ObjectRef.Version)
	}
	if store.Count() != 1 {
		t.Errorf("store.Count() = %d, want 1", store.Count())
	}
}

func TestOffloadRespectsCallerContextDeadline(t *testing.T) {
	store := NewMemoryBlobStore()
	off, err := New(store, Options{
		Bucket:         "ctx-bucket",
		MinBytes:       16,
		MaxObjectBytes: 512,
		Timeout:        10 * time.Second,
		Now:            func() time.Time { return fixedOffloadTime },
	})
	if err != nil {
		t.Fatal(err)
	}

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	if ref := off.OffloadQuarantine(cancelledCtx, "spans", []byte(strings.Repeat("Q", 64))); ref.URI != "" {
		t.Errorf("OffloadQuarantine with cancelled ctx returned %+v, want empty ObjectRef", ref)
	}
	sp := ptrace.NewSpan()
	sp.Attributes().PutStr("prompt", strings.Repeat("P", 64))
	if _, err := off.OffloadSpan(cancelledCtx, sp, pcommon.NewResource(), pcommon.NewInstrumentationScope()); !errors.Is(err, context.Canceled) {
		t.Errorf("OffloadSpan with cancelled ctx error = %v, want context.Canceled", err)
	}
}

func TestGCSBlobStoreUploadAndPreconditionPreservesGeneration(t *testing.T) {
	var uploadCalls, metaCalls int
	var gotPath, gotQuery, gotContentType string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/upload/storage/v1/b/test-bucket/o":
			uploadCalls++
			gotPath = r.URL.Path
			gotQuery = r.URL.RawQuery
			gotContentType = r.Header.Get("Content-Type")
			gotBody, _ = io.ReadAll(r.Body)
			if uploadCalls > 1 {
				w.WriteHeader(http.StatusPreconditionFailed)
				_, _ = w.Write([]byte(`{"error":{"code":412,"message":"Precondition Failed"}}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"generation":"987654321"}`))

		case r.Method == http.MethodGet && r.URL.Path == "/storage/v1/b/test-bucket/o/otel-offload/spans/2026-10-11/abc":
			metaCalls++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"generation":"987654321","size":"9"}`))

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	store, err := NewGCSBlobStore(
		context.Background(),
		option.WithEndpoint(srv.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("NewGCSBlobStore() error = %v", err)
	}
	defer func() { _ = store.Close() }()

	ver, err := store.WriteObject(context.Background(), "test-bucket", "otel-offload/spans/2026-10-11/abc", "text/plain", []byte("hello-gcs"))
	if err != nil {
		t.Fatalf("first WriteObject() error = %v", err)
	}
	if ver != "987654321" {
		t.Errorf("first version = %q, want 987654321", ver)
	}
	if gotPath != "/upload/storage/v1/b/test-bucket/o" {
		t.Errorf("request path = %q, want /upload/storage/v1/b/test-bucket/o", gotPath)
	}
	if !strings.Contains(gotQuery, "uploadType=media") || !strings.Contains(gotQuery, "ifGenerationMatch=0") || !strings.Contains(gotQuery, "name=otel-offload%2Fspans%2F2026-10-11%2Fabc") {
		t.Errorf("request query = %q, want uploadType=media, ifGenerationMatch=0, and escaped name", gotQuery)
	}
	if gotContentType != "text/plain" || string(gotBody) != "hello-gcs" {
		t.Errorf("content-type=%q body=%q", gotContentType, string(gotBody))
	}

	// Second upload of identical object receives 412 and fetches existing generation.
	ver2, err := store.WriteObject(context.Background(), "test-bucket", "otel-offload/spans/2026-10-11/abc", "text/plain", []byte("hello-gcs"))
	if err != nil {
		t.Fatalf("second WriteObject() error = %v", err)
	}
	if ver2 != "987654321" || metaCalls != 1 {
		t.Errorf("second version = %q (metaCalls=%d), want 987654321 and metaCalls=1", ver2, metaCalls)
	}

	// Size mismatch on existing object returns error.
	if _, err := store.WriteObject(context.Background(), "test-bucket", "otel-offload/spans/2026-10-11/abc", "text/plain", []byte("different-length")); err == nil {
		t.Error("expected error when existing object size does not match upload size")
	}
}
