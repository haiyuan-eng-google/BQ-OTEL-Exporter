// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package bigqueryexporter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
	bigqueryapi "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/googlehttp"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/offload"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/quarantinereplay"
)

type recordingBlobStore struct {
	inner   offload.BlobStore
	onWrite func(uri string)
}

func (r *recordingBlobStore) WriteObject(ctx context.Context, bucket, object, contentType string, data []byte) (string, error) {
	if r.onWrite != nil {
		r.onWrite("gs://" + bucket + "/" + object)
	}
	return r.inner.WriteObject(ctx, bucket, object, contentType, data)
}

func (r *recordingBlobStore) Close() error {
	if c, ok := r.inner.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

func TestLiveGCSOffloadAndQuarantineReplay(t *testing.T) {
	t.Run("HermeticRoundTrip", TestHermeticOffloadAndQuarantineReplay)
	t.Run("LiveGCSAndBigQueryRoundTrip", testLiveGCSAndBigQueryOffloadRoundTrip)
}

func testLiveGCSAndBigQueryOffloadRoundTrip(t *testing.T) {
	bucket := os.Getenv("BQ_OFFLOAD_TEST_BUCKET")
	if bucket == "" {
		bucket = os.Getenv("BQ_OFFLOAD_BUCKET")
	}
	project := os.Getenv("BQ_PROJECT")
	if bucket == "" || project == "" {
		if os.Getenv("REQUIRE_LIVE_GCS") == "1" {
			t.Fatalf("REQUIRE_LIVE_GCS=1 is set but BQ_PROJECT=%q BQ_OFFLOAD_BUCKET=%q", project, bucket)
		}
		t.Skip("BQ_OFFLOAD_BUCKET (or BQ_OFFLOAD_TEST_BUCKET) or BQ_PROJECT is unset; skipping live GCS + BigQuery offload round-trip")
	}
	location := os.Getenv("BQ_LOCATION")
	if location == "" {
		location = "US"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	runNano := time.Now().UnixNano()
	dataset := uniqueEphemeralDatasetID("otel_offload_it")
	prefix := fmt.Sprintf("it-offload-%d", runNano)

	httpClient, _, closeHTTP, err := googlehttp.NewClient(ctx, []string{
		bigqueryapi.BigqueryScope,
		"https://www.googleapis.com/auth/devstorage.read_write",
	})
	if err != nil {
		t.Fatalf("create cleanup/query HTTP client: %v", err)
	}
	admin, err := bq.NewClient(ctx, project, option.WithHTTPClient(httpClient))
	if err != nil {
		closeHTTP()
		t.Fatalf("create BigQuery admin client: %v", err)
	}
	var uploadedURIs []string
	var uriMu sync.Mutex
	seenURI := make(map[string]bool)
	recordURI := func(uri string) {
		if uri == "" {
			return
		}
		uriMu.Lock()
		if !seenURI[uri] {
			seenURI[uri] = true
			uploadedURIs = append(uploadedURIs, uri)
		}
		uriMu.Unlock()
	}

	t.Cleanup(func() {
		if err := admin.Close(); err != nil {
			t.Errorf("close BigQuery admin client: %v", err)
		}
		closeHTTP()
	})
	if err := createOwnedEphemeralDataset(ctx, admin, dataset, location, t.Cleanup); err != nil {
		t.Fatalf("create dataset %s.%s: %v", project, dataset, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		uriMu.Lock()
		uris := append([]string(nil), uploadedURIs...)
		uriMu.Unlock()
		for _, uri := range uris {
			deleteGCSObject(cleanupCtx, t, httpClient, uri)
		}
		listAndDeleteGCSPrefix(cleanupCtx, t, httpClient, bucket, prefix+"/")
	})

	cfg := validConfig()
	cfg.Project = project
	cfg.Dataset = dataset
	cfg.Location = location
	cfg.AutoCreate.Dataset = false
	cfg.AutoCreate.Tables = true
	cfg.Quarantine.Enabled = true
	cfg.Quarantine.MaxRawPayloadBytes = 256
	cfg.Redaction.Enabled = true
	cfg.Redaction.RedactLogBody = true
	cfg.Redaction.BlockedValuePatterns = []string{`sk-live-secret-token-[0-9a-f]+`}
	cfg.Limits.MaxAttributeCount = 4
	cfg.Offload.Enabled = true
	cfg.Offload.Bucket = bucket
	cfg.Offload.Prefix = prefix
	cfg.Offload.Authorizer = "us.live-authorizer"
	cfg.Offload.MinBytes = 256

	settings := component.TelemetrySettings{
		Logger:        zap.NewNop(),
		MeterProvider: noop.NewMeterProvider(),
	}

	traces, err := newTracesExporter(settings, cfg)
	if err != nil {
		t.Fatalf("newTracesExporter: %v", err)
	}
	if err := traces.start(ctx, nil); err != nil {
		t.Fatalf("traces.start: %v", err)
	}
	rawTraceStore := traces.offloader.Store()
	traces.offloader.SetStore(&recordingBlobStore{inner: rawTraceStore, onWrite: recordURI})
	defer func() { _ = traces.shutdown(context.Background()) }()

	logs, err := newLogsExporter(settings, cfg)
	if err != nil {
		t.Fatalf("newLogsExporter: %v", err)
	}
	if err := logs.start(ctx, nil); err != nil {
		t.Fatalf("logs.start: %v", err)
	}
	logs.offloader.SetStore(&recordingBlobStore{inner: logs.offloader.Store(), onWrite: recordURI})
	defer func() { _ = logs.shutdown(context.Background()) }()

	// Verify live GCS HTTP 412 Precondition Failed idempotent re-upload path
	// (ifGenerationMatch=0 -> fetchExistingGeneration) at the GCSBlobStore boundary.
	dupObject := prefix + "/spans/idempotent_precondition_check.txt"
	dupPayload := []byte("idempotent-gcs-precondition-check-payload")
	gen1, err := traces.offloader.Store().WriteObject(ctx, bucket, dupObject, "text/plain; charset=utf-8", dupPayload)
	if err != nil {
		t.Fatalf("first WriteObject(%s): %v", dupObject, err)
	}
	gen2, err := traces.offloader.Store().WriteObject(ctx, bucket, dupObject, "text/plain; charset=utf-8", dupPayload)
	if err != nil {
		t.Fatalf("second idempotent WriteObject(%s) (HTTP 412 fetchExistingGeneration path): %v", dupObject, err)
	}
	if gen1 == "" || gen1 != gen2 {
		t.Fatalf("idempotent WriteObject generations = (%q, %q), want matching non-empty generation", gen1, gen2)
	}

	const plantedSecret = "sk-live-secret-token-9876543210abcdef"
	td, ld := buildOffloadAndQuarantinePayloads(plantedSecret)

	if err := traces.pushTraceData(ctx, td); err != nil {
		t.Fatalf("pushTraceData: %v", err)
	}
	if err := logs.pushLogData(ctx, ld); err != nil {
		t.Fatalf("pushLogData: %v", err)
	}

	for _, tbl := range []string{cfg.Traces.Table + "_dedup", cfg.Logs.Table + "_dedup", cfg.Quarantine.Table} {
		if err := waitForRows(ctx, admin, project, dataset, tbl); err != nil {
			t.Fatalf("waitForRows(%s): %v", tbl, err)
		}
	}

	fetcher, closeFetcher, err := quarantinereplay.NewGCSObjectFetcher(ctx)
	if err != nil {
		t.Fatalf("NewGCSObjectFetcher: %v", err)
	}
	defer closeFetcher()

	// Verify live BigQuery payload_refs on otel_spans_dedup and otel_logs_dedup.
	for _, tc := range []struct {
		table        string
		wantPrefix   string
		wantAttrPath string
		wantMIMEType string
	}{
		{
			table:        cfg.Traces.Table + "_dedup",
			wantPrefix:   "gs://" + bucket + "/" + prefix + "/spans/",
			wantAttrPath: "attributes.gen_ai.prompt",
			wantMIMEType: "text/plain; charset=utf-8",
		},
		{
			table:        cfg.Logs.Table + "_dedup",
			wantPrefix:   "gs://" + bucket + "/" + prefix + "/logs/",
			wantAttrPath: "body",
			wantMIMEType: "text/plain; charset=utf-8",
		},
	} {
		q := admin.Query(fmt.Sprintf(`
			SELECT
				payload_refs[OFFSET(0)].storage_mode,
				payload_refs[OFFSET(0)].byte_size,
				payload_refs[OFFSET(0)].sha256,
				payload_refs[OFFSET(0)].object_ref.uri,
				payload_refs[OFFSET(0)].object_ref.version,
				payload_refs[OFFSET(0)].object_ref.authorizer,
				payload_refs[OFFSET(0)].attribute_path,
				payload_refs[OFFSET(0)].part_index,
				payload_refs[OFFSET(0)].mime_type,
				IF(payload_refs[OFFSET(0)].object_ref.details IS NULL, '', TO_JSON_STRING(payload_refs[OFFSET(0)].object_ref.details))
			FROM `+"`%s.%s.%s`"+`
			WHERE ARRAY_LENGTH(payload_refs) > 0
			LIMIT 1
		`, project, dataset, tc.table))
		it, err := q.Read(ctx)
		if err != nil {
			t.Fatalf("query %s payload_refs: %v", tc.table, err)
		}
		var vals []bq.Value
		if err := it.Next(&vals); err != nil {
			t.Fatalf("read %s payload_refs: %v", tc.table, err)
		}
		mode, _ := vals[0].(string)
		byteSize, _ := vals[1].(int64)
		wantSHA, _ := vals[2].(string)
		uri, _ := vals[3].(string)
		version, _ := vals[4].(string)
		authorizer, _ := vals[5].(string)
		attrPath, _ := vals[6].(string)
		partIndex, _ := vals[7].(int64)
		mimeType, _ := vals[8].(string)
		detailsStr, _ := vals[9].(string)
		recordURI(uri)

		if mode != "gcs_object_ref" {
			t.Errorf("%s storage_mode = %q, want gcs_object_ref", tc.table, mode)
		}
		if attrPath != tc.wantAttrPath {
			t.Errorf("%s attribute_path = %q, want %q", tc.table, attrPath, tc.wantAttrPath)
		}
		if partIndex != 0 {
			t.Errorf("%s part_index = %d, want 0", tc.table, partIndex)
		}
		if mimeType != tc.wantMIMEType {
			t.Errorf("%s mime_type = %q, want %q", tc.table, mimeType, tc.wantMIMEType)
		}
		if !strings.HasPrefix(uri, tc.wantPrefix) {
			t.Errorf("%s object_ref.uri = %q, want prefix %q", tc.table, uri, tc.wantPrefix)
		}
		if version == "" {
			t.Errorf("%s object_ref.version is empty", tc.table)
		}
		if authorizer != "us.live-authorizer" {
			t.Errorf("%s object_ref.authorizer = %q, want us.live-authorizer", tc.table, authorizer)
		}
		if !strings.Contains(detailsStr, wantSHA) {
			t.Errorf("%s object_ref.details = %q, want sha256 %s", tc.table, detailsStr, wantSHA)
		}

		blob, err := fetcher(ctx, uri+"?generation="+version)
		if err != nil {
			t.Fatalf("fetch generation-pinned live GCS object %s?generation=%s: %v", uri, version, err)
		}
		if _, wrongGenErr := fetcher(ctx, uri+"?generation=1"); wrongGenErr == nil && version != "1" {
			t.Fatalf("%s live GCS fetch with wrong generation=1 succeeded, want error", tc.table)
		}
		if int64(len(blob)) != byteSize {
			t.Errorf("%s fetched len = %d, want byte_size %d", tc.table, len(blob), byteSize)
		}
		sum := sha256.Sum256(blob)
		if gotSHA := hex.EncodeToString(sum[:]); gotSHA != wantSHA {
			t.Errorf("%s fetched sha256 = %s, want %s", tc.table, gotSHA, wantSHA)
		}
		if strings.Contains(string(blob), plantedSecret) {
			t.Fatalf("%s live GCS object %s contains unredacted secret", tc.table, uri)
		}
		if !strings.Contains(string(blob), "[REDACTED]") {
			t.Errorf("%s live GCS object %s missing [REDACTED] marker", tc.table, uri)
		}
	}

	// Query otel_quarantine and replay via quarantinereplay.Replay + live GCS fetcher.
	q := admin.Query(fmt.Sprintf(`
		SELECT
			signal,
			error_code,
			COALESCE(raw_payload, ''),
			COALESCE(payload_ref.uri, ''),
			COALESCE(payload_ref.version, ''),
			COALESCE(payload_ref.authorizer, ''),
			IF(payload_ref.details IS NULL, '', TO_JSON_STRING(payload_ref.details))
		FROM `+"`%s.%s.%s`"+`
	`, project, dataset, cfg.Quarantine.Table))
	it, err := q.Read(ctx)
	if err != nil {
		t.Fatalf("query otel_quarantine: %v", err)
	}
	var jsonl bytes.Buffer
	enc := json.NewEncoder(&jsonl)
	quarantineCount := 0
	wantQuarantinePrefix := "gs://" + bucket + "/" + prefix + "/quarantine/"
	for {
		var vals []bq.Value
		err := it.Next(&vals)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			t.Fatalf("read otel_quarantine: %v", err)
		}
		quarantineCount++
		rawPayload, _ := vals[2].(string)
		uri, _ := vals[3].(string)
		recordURI(uri)
		version, _ := vals[4].(string)
		authorizer, _ := vals[5].(string)
		detailsStr, _ := vals[6].(string)

		if rawPayload != "" {
			t.Errorf("quarantine row %d raw_payload = %q, want empty inline payload when offloaded", quarantineCount, rawPayload)
		}
		if !strings.HasPrefix(uri, wantQuarantinePrefix) {
			t.Errorf("quarantine row %d payload_ref.uri = %q, want prefix %q", quarantineCount, uri, wantQuarantinePrefix)
		}
		if version == "" {
			t.Errorf("quarantine row %d payload_ref.version is empty", quarantineCount)
		}
		if authorizer != "us.live-authorizer" {
			t.Errorf("quarantine row %d payload_ref.authorizer = %q, want us.live-authorizer", quarantineCount, authorizer)
		}
		if detailsStr == "" {
			t.Errorf("quarantine row %d payload_ref.details is empty", quarantineCount)
		}

		qBlob, err := fetcher(ctx, uri+"?generation="+version)
		if err != nil {
			t.Fatalf("fetch generation-pinned quarantine GCS object %s?generation=%s: %v", uri, version, err)
		}
		qSum := sha256.Sum256(qBlob)
		qWantSHA := hex.EncodeToString(qSum[:])
		if !strings.Contains(detailsStr, qWantSHA) {
			t.Errorf("quarantine row %d payload_ref.details = %q, want sha256 %s", quarantineCount, detailsStr, qWantSHA)
		}
		if strings.Contains(string(qBlob), plantedSecret) {
			t.Fatalf("quarantine GCS object %s contains unredacted secret", uri)
		}
		if !strings.Contains(string(qBlob), "[REDACTED]") {
			t.Errorf("quarantine GCS object %s missing [REDACTED] marker", uri)
		}

		row := quarantinereplay.Record{
			Signal:     vals[0].(string),
			ErrorCode:  vals[1].(string),
			RawPayload: rawPayload,
			PayloadRef: &quarantinereplay.ObjectRef{
				URI:        uri,
				Version:    version,
				Authorizer: authorizer,
				Details:    json.RawMessage(detailsStr),
			},
		}
		if err := enc.Encode(row); err != nil {
			t.Fatalf("encode quarantine row: %v", err)
		}
	}
	if quarantineCount != 2 {
		t.Fatalf("otel_quarantine rows = %d, want 2", quarantineCount)
	}

	liveSink := quarantinereplay.NewMemorySink()
	res, err := quarantinereplay.Replay(ctx, &jsonl, liveSink, quarantinereplay.Options{
		ObjectFetcher: fetcher,
	})
	if err != nil {
		t.Fatalf("live quarantinereplay.Replay: %v", err)
	}
	if res.TotalRead != 2 || res.ReplayedSpans != 1 || res.ReplayedLogs != 1 || res.FailedUnmarshal != 0 || res.FailedExport != 0 {
		t.Fatalf("live Replay Summary = %+v, want TotalRead=2 ReplayedSpans=1 ReplayedLogs=1 Failed=0", res)
	}
	if len(liveSink.Traces) != 1 || len(liveSink.Logs) != 1 {
		t.Fatalf("live replayed batches traces=%d logs=%d, want 1 and 1", len(liveSink.Traces), len(liveSink.Logs))
	}
	if err := assertReplayedBatchesRedacted(liveSink, plantedSecret); err != nil {
		t.Fatalf("live replayed sink redaction check: %v", err)
	}
}

func listAndDeleteGCSPrefix(ctx context.Context, t *testing.T, httpClient *stdhttp.Client, bucket, prefix string) {
	t.Helper()
	pageToken := ""
	for {
		listURL := fmt.Sprintf("https://storage.googleapis.com/storage/v1/b/%s/o?prefix=%s",
			url.PathEscape(bucket), url.QueryEscape(prefix))
		if pageToken != "" {
			listURL += "&pageToken=" + url.QueryEscape(pageToken)
		}
		req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodGet, listURL, nil)
		if err != nil {
			t.Errorf("build GCS list request for gs://%s/%s: %v", bucket, prefix, err)
			return
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Errorf("list GCS prefix gs://%s/%s: %v", bucket, prefix, err)
			return
		}
		if resp.StatusCode == stdhttp.StatusForbidden {
			// Under prefix-scoped conditional roles/storage.objectUser, bucket-wide
			// storage.objects.list may return 403; per-object deleteGCSObject via
			// recordingBlobStore already deleted all uploaded objects above.
			_ = resp.Body.Close()
			return
		}
		if resp.StatusCode != stdhttp.StatusOK {
			_ = resp.Body.Close()
			t.Errorf("list GCS prefix gs://%s/%s returned HTTP %d", bucket, prefix, resp.StatusCode)
			return
		}
		var payload struct {
			NextPageToken string `json:"nextPageToken"`
			Items         []struct {
				Name string `json:"name"`
			} `json:"items"`
		}
		decodeErr := json.NewDecoder(resp.Body).Decode(&payload)
		_ = resp.Body.Close()
		if decodeErr != nil {
			t.Errorf("decode GCS list response for gs://%s/%s: %v", bucket, prefix, decodeErr)
			return
		}
		for _, item := range payload.Items {
			if item.Name != "" {
				deleteGCSObject(ctx, t, httpClient, "gs://"+bucket+"/"+item.Name)
			}
		}
		if payload.NextPageToken == "" {
			return
		}
		pageToken = payload.NextPageToken
	}
}

func deleteGCSObject(ctx context.Context, t *testing.T, httpClient *stdhttp.Client, gsURI string) {
	t.Helper()
	rest := strings.TrimPrefix(gsURI, "gs://")
	bucket, object, ok := strings.Cut(rest, "/")
	if !ok || bucket == "" || object == "" {
		return
	}
	endpoint := fmt.Sprintf("https://storage.googleapis.com/storage/v1/b/%s/o/%s",
		url.PathEscape(bucket), url.PathEscape(object))
	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodDelete, endpoint, nil)
	if err != nil {
		t.Errorf("build GCS delete request for %s: %v", gsURI, err)
		return
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Errorf("delete GCS object %s: %v", gsURI, err)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != stdhttp.StatusNoContent && resp.StatusCode != stdhttp.StatusOK && resp.StatusCode != stdhttp.StatusNotFound {
		t.Errorf("delete GCS object %s returned HTTP %d", gsURI, resp.StatusCode)
	}
}
