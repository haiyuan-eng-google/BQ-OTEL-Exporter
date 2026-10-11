// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

// Package offload implements optional large-payload offloading to cloud object
// storage, emitting BigQuery ObjectRef metadata for the payload_refs and
// quarantine payload_ref columns.
package offload // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/offload"

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/api/option"
	htransport "google.golang.org/api/transport/http"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/transform"
)

const (
	defaultPrefix      = "otel-offload"
	defaultTimeout     = 10 * time.Second
	defaultGCSEndpoint = "https://storage.googleapis.com"
	gcsReadWriteScope  = "https://www.googleapis.com/auth/devstorage.read_write"
	storageModeGCS     = "gcs_object_ref"
)

// BlobStore writes an object to cloud object storage and returns its storage
// version/generation identifier (if any).
type BlobStore interface {
	WriteObject(ctx context.Context, bucket, object string, contentType string, data []byte) (version string, err error)
}

// GCSBlobStore implements BlobStore over the Google Cloud Storage JSON upload
// API using standard Google API HTTP transport options.
type GCSBlobStore struct {
	client  *http.Client
	baseURL string
}

// NewGCSBlobStore creates a GCSBlobStore configured with the provided client
// options.
func NewGCSBlobStore(ctx context.Context, opts ...option.ClientOption) (*GCSBlobStore, error) {
	allOpts := make([]option.ClientOption, 0, len(opts)+1)
	allOpts = append(allOpts, option.WithScopes(gcsReadWriteScope))
	allOpts = append(allOpts, opts...)

	client, endpoint, err := htransport.NewClient(ctx, allOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating GCS HTTP client: %w", err)
	}
	base := strings.TrimRight(endpoint, "/")
	if base == "" {
		base = defaultGCSEndpoint
	} else if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		base = "https://" + base
	}
	return &GCSBlobStore{
		client:  client,
		baseURL: base,
	}, nil
}

// WriteObject uploads data to gs://bucket/object via the GCS JSON media upload
// endpoint and returns the resulting object generation.
func (s *GCSBlobStore) WriteObject(
	ctx context.Context, bucket, object string, contentType string, data []byte,
) (string, error) {
	if s == nil || s.client == nil {
		return "", errors.New("GCS blob store is not initialized")
	}
	uploadURL := fmt.Sprintf(
		"%s/upload/storage/v1/b/%s/o?uploadType=media&name=%s",
		s.baseURL,
		url.PathEscape(bucket),
		url.QueryEscape(object),
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, uploadURL, bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("building GCS upload request: %w", err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.ContentLength = int64(len(data))

	resp, err := s.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("uploading object to GCS: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("GCS upload returned HTTP %d", resp.StatusCode)
	}

	var meta struct {
		Generation string `json:"generation"`
	}
	_ = json.Unmarshal(body, &meta)
	return meta.Generation, nil
}

// Close releases idle HTTP connections held by the underlying HTTP client.
func (s *GCSBlobStore) Close() error {
	if s != nil && s.client != nil {
		s.client.CloseIdleConnections()
	}
	return nil
}

// MemoryObject is one object stored in a MemoryBlobStore.
type MemoryObject struct {
	Bucket      string
	Object      string
	ContentType string
	Data        []byte
	Version     string
}

// MemoryBlobStore is a thread-safe in-memory BlobStore for hermetic tests.
type MemoryBlobStore struct {
	mu      sync.Mutex
	objects map[string]MemoryObject
	seq     int64
	failErr error
}

// NewMemoryBlobStore returns an empty MemoryBlobStore.
func NewMemoryBlobStore() *MemoryBlobStore {
	return &MemoryBlobStore{
		objects: make(map[string]MemoryObject),
	}
}

// SetError configures the store to fail every subsequent WriteObject with err.
func (m *MemoryBlobStore) SetError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failErr = err
}

// WriteObject stores data in memory under bucket/object.
func (m *MemoryBlobStore) WriteObject(
	ctx context.Context, bucket, object string, contentType string, data []byte,
) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failErr != nil {
		return "", m.failErr
	}
	m.seq++
	ver := strconv.FormatInt(m.seq, 10)
	key := bucket + "/" + object
	m.objects[key] = MemoryObject{
		Bucket:      bucket,
		Object:      object,
		ContentType: contentType,
		Data:        append([]byte(nil), data...),
		Version:     ver,
	}
	return ver, nil
}

// Get retrieves an object previously written to the store.
func (m *MemoryBlobStore) Get(bucket, object string) (MemoryObject, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok := m.objects[bucket+"/"+object]
	return obj, ok
}

// Count returns the number of objects currently stored.
func (m *MemoryBlobStore) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.objects)
}

// Options configures an Offloader.
type Options struct {
	Bucket         string
	Prefix         string
	Authorizer     string
	MinBytes       int
	MaxObjectBytes int
	Timeout        time.Duration
	Now            func() time.Time
}

// Offloader offloads large span/log attributes, log bodies, and quarantine
// payloads to a BlobStore and emits BigQuery ObjectRef metadata.
type Offloader struct {
	mu             sync.RWMutex
	store          BlobStore
	bucket         string
	prefix         string
	authorizer     string
	minBytes       int
	maxObjectBytes int
	timeout        time.Duration
	now            func() time.Time
}

// New constructs an Offloader for the given BlobStore and options.
func New(store BlobStore, opts Options) (*Offloader, error) {
	bucket := strings.TrimSpace(opts.Bucket)
	if bucket == "" {
		return nil, errors.New("offload bucket must not be empty")
	}
	if opts.MinBytes <= 0 {
		return nil, fmt.Errorf("offload min_bytes must be positive, got %d", opts.MinBytes)
	}
	if opts.MaxObjectBytes < opts.MinBytes {
		return nil, fmt.Errorf("offload max_object_bytes (%d) must be at least min_bytes (%d)",
			opts.MaxObjectBytes, opts.MinBytes)
	}
	prefix := strings.Trim(opts.Prefix, "/")
	if opts.Prefix == "" {
		prefix = defaultPrefix
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	return &Offloader{
		store:          store,
		bucket:         bucket,
		prefix:         prefix,
		authorizer:     opts.Authorizer,
		minBytes:       opts.MinBytes,
		maxObjectBytes: opts.MaxObjectBytes,
		timeout:        timeout,
		now:            nowFn,
	}, nil
}

// SetStore replaces the underlying BlobStore in a thread-safe manner.
func (o *Offloader) SetStore(store BlobStore) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.store = store
}

// Store returns the currently configured BlobStore.
func (o *Offloader) Store() BlobStore {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.store
}

// Close closes the underlying BlobStore if it implements io.Closer.
func (o *Offloader) Close() error {
	store := o.Store()
	if closer, ok := store.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// OffloadSpan inspects a working span, resource, and instrumentation scope,
// offloading eligible large values to object storage and replacing them with
// URI placeholders.
func (o *Offloader) OffloadSpan(
	span ptrace.Span, resource pcommon.Resource, scope pcommon.InstrumentationScope,
) []transform.PayloadRef {
	var refs []transform.PayloadRef
	o.offloadMap(schema.SignalSpans, resource.Attributes(), "resource.attributes", &refs)
	o.offloadMap(schema.SignalSpans, scope.Attributes(), "instrumentation_scope.attributes", &refs)
	o.offloadMap(schema.SignalSpans, span.Attributes(), "attributes", &refs)
	for i := 0; i < span.Events().Len(); i++ {
		o.offloadMap(schema.SignalSpans, span.Events().At(i).Attributes(), fmt.Sprintf("events[%d].attributes", i), &refs)
	}
	for i := 0; i < span.Links().Len(); i++ {
		o.offloadMap(schema.SignalSpans, span.Links().At(i).Attributes(), fmt.Sprintf("links[%d].attributes", i), &refs)
	}
	return refs
}

// OffloadLog inspects a working log record, resource, and instrumentation
// scope, offloading an eligible large body or attributes to object storage and
// replacing them with URI placeholders.
func (o *Offloader) OffloadLog(
	lr plog.LogRecord, resource pcommon.Resource, scope pcommon.InstrumentationScope,
) []transform.PayloadRef {
	var refs []transform.PayloadRef
	o.offloadValue(schema.SignalLogs, lr.Body(), "body", &refs)
	o.offloadMap(schema.SignalLogs, resource.Attributes(), "resource.attributes", &refs)
	o.offloadMap(schema.SignalLogs, scope.Attributes(), "instrumentation_scope.attributes", &refs)
	o.offloadMap(schema.SignalLogs, lr.Attributes(), "attributes", &refs)
	return refs
}

// OffloadQuarantine uploads an eligible quarantined OTLP JSON payload to
// object storage and returns its ObjectRef, or a zero ObjectRef if the payload
// is outside the offload threshold or the upload fails.
func (o *Offloader) OffloadQuarantine(signal string, payload []byte) transform.ObjectRef {
	if len(payload) < o.minBytes || len(payload) > o.maxObjectBytes {
		return transform.ObjectRef{}
	}
	ref, ok := o.upload("quarantine/"+signal, "application/json", payload, "raw_payload", ".json")
	if !ok {
		return transform.ObjectRef{}
	}
	return ref.ObjectRef
}

func (o *Offloader) offloadMap(signal string, m pcommon.Map, basePath string, refs *[]transform.PayloadRef) {
	if m.Len() == 0 {
		return
	}
	keys := make([]string, 0, m.Len())
	m.Range(func(k string, _ pcommon.Value) bool {
		keys = append(keys, k)
		return true
	})
	slices.Sort(keys)
	for _, k := range keys {
		v, ok := m.Get(k)
		if !ok {
			continue
		}
		o.offloadValue(signal, v, basePath+"."+k, refs)
	}
}

func (o *Offloader) offloadValue(signal string, v pcommon.Value, attrPath string, refs *[]transform.PayloadRef) {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		s := v.Str()
		if len(s) < o.minBytes || len(s) > o.maxObjectBytes {
			return
		}
		if ref, ok := o.upload(signal, "text/plain; charset=utf-8", []byte(s), attrPath, ""); ok {
			v.SetStr(fmt.Sprintf("<offloaded:%s>", ref.ObjectRef.URI))
			*refs = append(*refs, ref)
		}

	case pcommon.ValueTypeBytes:
		raw := v.Bytes().AsRaw()
		if len(raw) < o.minBytes || len(raw) > o.maxObjectBytes {
			return
		}
		if ref, ok := o.upload(signal, "application/octet-stream", raw, attrPath, ""); ok {
			v.SetStr(fmt.Sprintf("<offloaded:%s>", ref.ObjectRef.URI))
			*refs = append(*refs, ref)
		}

	case pcommon.ValueTypeMap:
		o.offloadMap(signal, v.Map(), attrPath, refs)

	case pcommon.ValueTypeSlice:
		sl := v.Slice()
		for i := 0; i < sl.Len(); i++ {
			o.offloadValue(signal, sl.At(i), fmt.Sprintf("%s[%d]", attrPath, i), refs)
		}
	}
}

func (o *Offloader) upload(
	signal, contentType string, data []byte, attrPath, ext string,
) (transform.PayloadRef, bool) {
	store := o.Store()
	if store == nil {
		return transform.PayloadRef{}, false
	}

	sum := sha256.Sum256(data)
	sumHex := hex.EncodeToString(sum[:])
	datePart := o.now().UTC().Format("2006-01-02")

	var objectKey string
	if o.prefix != "" {
		objectKey = fmt.Sprintf("%s/%s/%s/%s%s", o.prefix, signal, datePart, sumHex, ext)
	} else {
		objectKey = fmt.Sprintf("%s/%s/%s%s", signal, datePart, sumHex, ext)
	}

	ctx, cancel := context.WithTimeout(context.Background(), o.timeout)
	defer cancel()

	version, err := store.WriteObject(ctx, o.bucket, objectKey, contentType, data)
	if err != nil {
		return transform.PayloadRef{}, false
	}

	uri := fmt.Sprintf("gs://%s/%s", o.bucket, objectKey)
	details := fmt.Sprintf(`{"gcs_metadata":{"content_type":%q,"sha256":%q}}`, contentType, sumHex)
	return transform.PayloadRef{
		AttributePath: attrPath,
		PartIndex:     0,
		MIMEType:      contentType,
		StorageMode:   storageModeGCS,
		ByteSize:      int64(len(data)),
		SHA256:        sumHex,
		ObjectRef: transform.ObjectRef{
			URI:        uri,
			Version:    version,
			Authorizer: o.authorizer,
			Details:    details,
		},
	}, true
}
