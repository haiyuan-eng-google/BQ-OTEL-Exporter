// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

// Package offload implements optional large-payload offloading to cloud object
// storage, emitting BigQuery ObjectRef metadata for the payload_refs and
// quarantine payload_ref columns.
package offload // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/offload"

import (
	"bytes"
	"container/list"
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

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/googlehttp"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/transform"
)

const (
	defaultPrefix         = "otel-offload"
	defaultTimeout        = 10 * time.Second
	defaultMaxConcurrency = 8
	defaultGCSEndpoint    = "https://storage.googleapis.com"
	gcsReadWriteScope     = "https://www.googleapis.com/auth/devstorage.read_write"
	storageModeGCS        = "gcs_object_ref"
)

// GCSHTTPError is returned by GCSBlobStore when a GCS HTTP request returns a
// non-2xx status code.
type GCSHTTPError struct {
	StatusCode int
	Status     string
	Body       string
}

func (e *GCSHTTPError) Error() string {
	msg := strings.TrimSpace(e.Body)
	if msg != "" {
		if len(msg) > 256 {
			msg = msg[:256]
		}
		return fmt.Sprintf("GCS HTTP %d: %s", e.StatusCode, msg)
	}
	return fmt.Sprintf("GCS HTTP %d (%s)", e.StatusCode, e.Status)
}

// ClassifyOffloadError maps an offload upload error to a bounded telemetry
// error class label and reports whether the failure is permanent (meaning
// retrying the same upload will deterministically fail and the record should
// be routed to quarantine rather than retried).
func ClassifyOffloadError(err error) (errorCode string, permanent bool) {
	if err == nil {
		return "", false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return "request_canceled", false
	}
	var httpErr *GCSHTTPError
	if errors.As(err, &httpErr) {
		switch httpErr.StatusCode {
		case http.StatusBadRequest:
			return "http_400", true
		case http.StatusUnauthorized:
			return "http_401", true
		case http.StatusForbidden:
			return "http_403", true
		case http.StatusNotFound:
			return "http_404", true
		case http.StatusRequestTimeout:
			return "http_408", false
		case http.StatusTooManyRequests:
			return "http_429", false
		default:
			if httpErr.StatusCode >= 400 && httpErr.StatusCode < 500 {
				return "http_4xx", true
			}
			if httpErr.StatusCode >= 500 && httpErr.StatusCode < 600 {
				return "http_5xx", false
			}
		}
	}
	return "unknown", false
}

// IsPermanentOffloadError reports whether err is a permanent offload failure
// that should route the affected record to quarantine rather than retry.
func IsPermanentOffloadError(err error) bool {
	if err == nil {
		return false
	}
	_, permanent := ClassifyOffloadError(err)
	return permanent
}

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
	cleanup func()
}

// NewGCSBlobStore creates a GCSBlobStore configured with the provided client
// options.
func NewGCSBlobStore(ctx context.Context, opts ...option.ClientOption) (*GCSBlobStore, error) {
	client, endpoint, cleanup, err := googlehttp.NewClient(ctx, []string{gcsReadWriteScope}, opts...)
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
		cleanup: cleanup,
	}, nil
}

// WriteObject uploads data to gs://bucket/object via the GCS JSON media upload
// endpoint and returns the resulting object generation.
//
// Because offload keys are content-addressed by SHA-256, WriteObject sends
// ifGenerationMatch=0 so a repeated upload of identical content does not
// overwrite the existing object or churn its generation. On HTTP 412
// (Precondition Failed), WriteObject fetches and returns the existing object's
// generation.
func (s *GCSBlobStore) WriteObject(
	ctx context.Context, bucket, object string, contentType string, data []byte,
) (string, error) {
	if s == nil || s.client == nil {
		return "", errors.New("GCS blob store is not initialized")
	}
	uploadURL := fmt.Sprintf(
		"%s/upload/storage/v1/b/%s/o?uploadType=media&ifGenerationMatch=0&name=%s",
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
	if resp.StatusCode == http.StatusPreconditionFailed {
		return s.fetchExistingGeneration(ctx, bucket, object, int64(len(data)))
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", &GCSHTTPError{
			StatusCode: resp.StatusCode,
			Status:     resp.Status,
			Body:       string(body),
		}
	}

	var meta struct {
		Generation string `json:"generation"`
	}
	_ = json.Unmarshal(body, &meta)
	return meta.Generation, nil
}

func (s *GCSBlobStore) fetchExistingGeneration(
	ctx context.Context, bucket, object string, wantSize int64,
) (string, error) {
	metaURL := fmt.Sprintf(
		"%s/storage/v1/b/%s/o/%s",
		s.baseURL,
		url.PathEscape(bucket),
		url.PathEscape(object),
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metaURL, nil)
	if err != nil {
		return "", fmt.Errorf("building GCS metadata request: %w", err)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetching existing GCS object metadata: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("GCS metadata lookup after HTTP 412: %w", &GCSHTTPError{
			StatusCode: resp.StatusCode,
			Status:     resp.Status,
			Body:       string(body),
		})
	}
	var meta struct {
		Generation string `json:"generation"`
		Size       string `json:"size"`
	}
	if err := json.Unmarshal(body, &meta); err != nil {
		return "", fmt.Errorf("decoding existing GCS object metadata: %w", err)
	}
	if meta.Size != "" {
		gotSize, perr := strconv.ParseInt(meta.Size, 10, 64)
		if perr != nil {
			return "", fmt.Errorf("parsing existing GCS object size %q: %w", meta.Size, perr)
		}
		if gotSize != wantSize {
			return "", fmt.Errorf("existing GCS object %q size %d does not match upload size %d",
				object, gotSize, wantSize)
		}
	}
	return meta.Generation, nil
}

// Close releases idle HTTP connections held by the underlying HTTP client.
func (s *GCSBlobStore) Close() error {
	if s != nil && s.cleanup != nil {
		s.cleanup()
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

// WriteObject stores data in memory under bucket/object, preserving the
// existing generation when the same content-addressed key is written again.
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
	key := bucket + "/" + object
	if existing, ok := m.objects[key]; ok {
		if !bytes.Equal(existing.Data, data) {
			return "", fmt.Errorf("existing memory object %q content mismatch", key)
		}
		return existing.Version, nil
	}
	m.seq++
	ver := strconv.FormatInt(m.seq, 10)
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

type lruItem struct {
	key        string
	generation string
}

type generationCache struct {
	mu    sync.Mutex
	cap   int
	ll    *list.List
	items map[string]*list.Element
}

func newGenerationCache(capacity int) *generationCache {
	if capacity <= 0 {
		return nil
	}
	return &generationCache{
		cap:   capacity,
		ll:    list.New(),
		items: make(map[string]*list.Element, min(capacity, 256)),
	}
}

func (c *generationCache) get(key string) (string, bool) {
	if c == nil {
		return "", false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el)
		return el.Value.(lruItem).generation, true
	}
	return "", false
}

func (c *generationCache) put(key, generation string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el)
		el.Value = lruItem{key: key, generation: generation}
		return
	}
	if c.ll.Len() >= c.cap {
		if tail := c.ll.Back(); tail != nil {
			c.ll.Remove(tail)
			delete(c.items, tail.Value.(lruItem).key)
		}
	}
	el := c.ll.PushFront(lruItem{key: key, generation: generation})
	c.items[key] = el
}

func (c *generationCache) clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ll.Init()
	clear(c.items)
}

// Options configures an Offloader.
type Options struct {
	Bucket         string
	Prefix         string
	Authorizer     string
	MinBytes       int
	MaxObjectBytes int
	MaxConcurrency int
	CacheSize      int
	Timeout        time.Duration
	Now            func() time.Time
	OnOffload      func(payloads int, bytes int64, errClass string)
	OnDedupHit     func()
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
	maxConcurrency int
	cache          *generationCache
	sem            chan struct{}
	timeout        time.Duration
	now            func() time.Time
	onOffload      func(payloads int, bytes int64, errClass string)
	onDedupHit     func()
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
	if opts.CacheSize < 0 {
		return nil, fmt.Errorf("offload cache_size must be non-negative, got %d", opts.CacheSize)
	}
	prefix := strings.Trim(opts.Prefix, "/")
	if opts.Prefix == "" {
		prefix = defaultPrefix
	}
	maxConcurrency := opts.MaxConcurrency
	if maxConcurrency <= 0 {
		maxConcurrency = defaultMaxConcurrency
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
		maxConcurrency: maxConcurrency,
		cache:          newGenerationCache(opts.CacheSize),
		sem:            make(chan struct{}, maxConcurrency),
		timeout:        timeout,
		now:            nowFn,
		onOffload:      opts.OnOffload,
		onDedupHit:     opts.OnDedupHit,
	}, nil
}

// MaxConcurrency returns the maximum number of concurrent object-store uploads
// permitted by this Offloader.
func (o *Offloader) MaxConcurrency() int {
	return o.maxConcurrency
}

// SetStore replaces the underlying BlobStore in a thread-safe manner and clears
// any cached object generations from the previous store.
func (o *Offloader) SetStore(store BlobStore) {
	o.mu.Lock()
	o.store = store
	o.mu.Unlock()
	o.cache.clear()
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

type offloadCandidate struct {
	value       pcommon.Value
	attrPath    string
	contentType string
	data        []byte
}

// OffloadSpan inspects a working span, resource, and instrumentation scope,
// offloading eligible large values to object storage and replacing them with
// URI placeholders. Any upload failure is returned immediately so the caller
// can surface the span as retryable rather than rejecting it.
func (o *Offloader) OffloadSpan(
	ctx context.Context, span ptrace.Span, resource pcommon.Resource, scope pcommon.InstrumentationScope,
) ([]transform.PayloadRef, error) {
	ctx = transform.WithBatchUploadCache(ctx)
	var candidates []offloadCandidate
	o.collectMap(resource.Attributes(), "resource.attributes", &candidates)
	o.collectMap(scope.Attributes(), "instrumentation_scope.attributes", &candidates)
	o.collectMap(span.Attributes(), "attributes", &candidates)
	for i := 0; i < span.Events().Len(); i++ {
		o.collectMap(span.Events().At(i).Attributes(), fmt.Sprintf("events[%d].attributes", i), &candidates)
	}
	for i := 0; i < span.Links().Len(); i++ {
		o.collectMap(span.Links().At(i).Attributes(), fmt.Sprintf("links[%d].attributes", i), &candidates)
	}
	return o.uploadCandidates(ctx, schema.SignalSpans, candidates)
}

// OffloadLog inspects a working log record, resource, and instrumentation
// scope, offloading an eligible large body or attributes to object storage and
// replacing them with URI placeholders. Any upload failure is returned
// immediately so the caller can surface the log record as retryable rather than
// rejecting it.
func (o *Offloader) OffloadLog(
	ctx context.Context, lr plog.LogRecord, resource pcommon.Resource, scope pcommon.InstrumentationScope,
) ([]transform.PayloadRef, error) {
	ctx = transform.WithBatchUploadCache(ctx)
	var candidates []offloadCandidate
	o.collectValue(lr.Body(), "body", &candidates)
	o.collectMap(resource.Attributes(), "resource.attributes", &candidates)
	o.collectMap(scope.Attributes(), "instrumentation_scope.attributes", &candidates)
	o.collectMap(lr.Attributes(), "attributes", &candidates)
	return o.uploadCandidates(ctx, schema.SignalLogs, candidates)
}

// OffloadQuarantine uploads an eligible quarantined OTLP JSON payload to
// object storage under ctx and returns its ObjectRef, or a zero ObjectRef if
// the payload is outside the offload threshold or the upload fails.
func (o *Offloader) OffloadQuarantine(ctx context.Context, signal string, payload []byte) transform.ObjectRef {
	if len(payload) < o.minBytes || len(payload) > o.maxObjectBytes {
		return transform.ObjectRef{}
	}
	ctx = transform.WithBatchUploadCache(ctx)
	ref, err := o.upload(ctx, "quarantine/"+signal, "application/json", payload, "raw_payload", ".json")
	if err != nil {
		return transform.ObjectRef{}
	}
	return ref.ObjectRef
}

func (o *Offloader) collectMap(m pcommon.Map, basePath string, candidates *[]offloadCandidate) {
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
		o.collectValue(v, basePath+"."+k, candidates)
	}
}

func (o *Offloader) collectValue(v pcommon.Value, attrPath string, candidates *[]offloadCandidate) {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		s := v.Str()
		if len(s) < o.minBytes || len(s) > o.maxObjectBytes {
			return
		}
		*candidates = append(*candidates, offloadCandidate{
			value:       v,
			attrPath:    attrPath,
			contentType: "text/plain; charset=utf-8",
			data:        []byte(s),
		})

	case pcommon.ValueTypeBytes:
		raw := v.Bytes().AsRaw()
		if len(raw) < o.minBytes || len(raw) > o.maxObjectBytes {
			return
		}
		*candidates = append(*candidates, offloadCandidate{
			value:       v,
			attrPath:    attrPath,
			contentType: "application/octet-stream",
			data:        raw,
		})

	case pcommon.ValueTypeMap:
		o.collectMap(v.Map(), attrPath, candidates)

	case pcommon.ValueTypeSlice:
		sl := v.Slice()
		for i := 0; i < sl.Len(); i++ {
			o.collectValue(sl.At(i), fmt.Sprintf("%s[%d]", attrPath, i), candidates)
		}
	}
}

func (o *Offloader) uploadCandidates(
	ctx context.Context, signal string, candidates []offloadCandidate,
) ([]transform.PayloadRef, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	if len(candidates) == 1 || o.maxConcurrency <= 1 {
		refs := make([]transform.PayloadRef, 0, len(candidates))
		for _, c := range candidates {
			ref, err := o.upload(ctx, signal, c.contentType, c.data, c.attrPath, "")
			if err != nil {
				return nil, err
			}
			c.value.SetStr(fmt.Sprintf("<offloaded:%s>", ref.ObjectRef.URI))
			refs = append(refs, ref)
		}
		return refs, nil
	}

	refs := make([]transform.PayloadRef, len(candidates))
	errs := make([]error, len(candidates))
	workers := min(o.maxConcurrency, len(candidates))
	jobs := make(chan int, len(candidates))
	for i := range candidates {
		jobs <- i
	}
	close(jobs)

	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for i := range jobs {
				c := candidates[i]
				refs[i], errs[i] = o.upload(ctx, signal, c.contentType, c.data, c.attrPath, "")
			}
		}()
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	for i, c := range candidates {
		c.value.SetStr(fmt.Sprintf("<offloaded:%s>", refs[i].ObjectRef.URI))
	}
	return refs, nil
}

func (o *Offloader) upload(
	ctx context.Context, signal, contentType string, data []byte, attrPath, ext string,
) (transform.PayloadRef, error) {
	store := o.Store()
	if store == nil {
		return transform.PayloadRef{}, errors.New("offload blob store is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return transform.PayloadRef{}, err
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

	cacheKey := o.bucket + "/" + objectKey
	uploadDeadline := time.Now().Add(o.timeout)
	if dl, ok := ctx.Deadline(); ok {
		if time.Until(dl) <= 0 {
			return transform.PayloadRef{}, context.DeadlineExceeded
		}
		if dl.Before(uploadDeadline) {
			uploadDeadline = dl
		}
	}

	var lruHit bool
	version, shared, err := transform.BatchUploadCacheFromContext(ctx).Do(ctx, cacheKey, func(leaderCtx context.Context) (string, error) {
		if gen, ok := o.cache.get(cacheKey); ok {
			lruHit = true
			return gen, nil
		}
		if time.Until(uploadDeadline) <= 0 {
			err := context.DeadlineExceeded
			if o.onOffload != nil {
				errClass, _ := ClassifyOffloadError(err)
				o.onOffload(0, 0, errClass)
			}
			return "", err
		}
		uploadCtx, cancel := context.WithDeadline(leaderCtx, uploadDeadline)
		defer cancel()
		if o.sem != nil {
			select {
			case o.sem <- struct{}{}:
				defer func() { <-o.sem }()
			case <-uploadCtx.Done():
				err := uploadCtx.Err()
				if o.onOffload != nil {
					errClass, _ := ClassifyOffloadError(err)
					o.onOffload(0, 0, errClass)
				}
				return "", err
			}
		}
		ver, werr := store.WriteObject(uploadCtx, o.bucket, objectKey, contentType, data)
		if werr != nil {
			if o.onOffload != nil {
				errClass, _ := ClassifyOffloadError(werr)
				o.onOffload(0, 0, errClass)
			}
			return "", werr
		}
		o.cache.put(cacheKey, ver)
		if o.onOffload != nil {
			o.onOffload(1, int64(len(data)), "")
		}
		return ver, nil
	})
	if err != nil {
		return transform.PayloadRef{}, err
	}
	if shared || lruHit {
		if o.onDedupHit != nil {
			o.onDedupHit()
		}
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
	}, nil
}
