// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

// Package quarantinereplay reads rows exported from the BigQuery
// otel_quarantine table, filters them by signal, error_code, and time window,
// reconstructs ptrace.Traces and plog.Logs from raw_payload (or payload_ref),
// and replays them into a configurable Sink.
package quarantinereplay // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/quarantinereplay"

import (
	"bufio"
	"bytes"
	"context"
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

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/api/option"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/googlehttp"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
)

const (
	defaultGCSEndpoint = "https://storage.googleapis.com"
	gcsReadOnlyScope   = "https://www.googleapis.com/auth/devstorage.read_only"
	maxJSONLLineBytes  = 64 * 1024 * 1024
)

// CanonicalSignal normalizes a signal alias ("spans", "traces", "otel_spans",
// "logs", "otel_logs", "metrics", "otel_metrics") to its canonical
// otel_quarantine signal name ("spans", "logs", or "metrics"), or returns ""
// if unrecognized.
func CanonicalSignal(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case schema.SignalSpans, "traces", "otel_spans":
		return schema.SignalSpans
	case schema.SignalLogs, "otel_logs":
		return schema.SignalLogs
	case schema.SignalMetrics, "otel_metrics":
		return schema.SignalMetrics
	default:
		return ""
	}
}

// ParseSignals parses a comma-separated signal filter string into canonical
// signal names ("spans", "logs", "metrics"), returning an error if any token
// is unrecognized.
func ParseSignals(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var out []string
	for _, part := range strings.Split(raw, ",") {
		token := strings.TrimSpace(part)
		if token == "" {
			continue
		}
		canon := CanonicalSignal(token)
		if canon == "" {
			return nil, fmt.Errorf("unknown signal %q (valid: spans, traces, logs, metrics)", token)
		}
		if !slices.Contains(out, canon) {
			out = append(out, canon)
		}
	}
	return out, nil
}

// ObjectRef mirrors the BigQuery ObjectRef STRUCT stored in payload_ref.
type ObjectRef struct {
	URI        string `json:"uri"`
	Version    string `json:"version,omitempty"`
	Authorizer string `json:"authorizer,omitempty"`
	Details    any    `json:"details,omitempty"`
}

// Record represents a single row exported from otel_quarantine (either via
// `bq extract --destination_format=NEWLINE_DELIMITED_JSON` or
// `bq query --format=json`).
type Record struct {
	IngestTime   any        `json:"ingest_time"`
	Signal       string     `json:"signal"`
	ErrorCode    string     `json:"error_code"`
	ErrorMessage string     `json:"error_message"`
	ServiceName  string     `json:"service_name"`
	TraceID      string     `json:"trace_id"`
	SpanID       string     `json:"span_id"`
	RawPayload   string     `json:"raw_payload"`
	PayloadRef   *ObjectRef `json:"payload_ref"`
}

// ParsedIngestTime parses IngestTime from RFC3339 strings, BigQuery timestamp
// strings ("2006-01-02 15:04:05.999999 UTC"), or integer/float Unix
// microseconds / seconds.
func (r Record) ParsedIngestTime() (time.Time, bool) {
	switch v := r.IngestTime.(type) {
	case nil:
		return time.Time{}, false
	case string:
		s := strings.TrimSpace(v)
		if s == "" {
			return time.Time{}, false
		}
		for _, layout := range []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02 15:04:05.999999 UTC",
			"2006-01-02 15:04:05 UTC",
			"2006-01-02T15:04:05.999999",
		} {
			if t, err := time.Parse(layout, s); err == nil {
				return t.UTC(), true
			}
		}
		if iv, err := strconv.ParseInt(s, 10, 64); err == nil {
			return microsOrSecondsToTime(iv), true
		}
		if fv, err := strconv.ParseFloat(s, 64); err == nil {
			return floatToTime(fv), true
		}
	case json.Number:
		if iv, err := v.Int64(); err == nil {
			return microsOrSecondsToTime(iv), true
		}
		if fv, err := v.Float64(); err == nil {
			return floatToTime(fv), true
		}
	case float64:
		return floatToTime(v), true
	case int64:
		return microsOrSecondsToTime(v), true
	}
	return time.Time{}, false
}

func microsOrSecondsToTime(v int64) time.Time {
	// Values >= 1e12 are Unix microseconds (Storage Write API format).
	if v >= 1_000_000_000_000 {
		return time.UnixMicro(v).UTC()
	}
	return time.Unix(v, 0).UTC()
}

func floatToTime(v float64) time.Time {
	if v >= 1e12 {
		return time.UnixMicro(int64(v)).UTC()
	}
	sec := int64(v)
	nsec := int64((v - float64(sec)) * 1e9)
	return time.Unix(sec, nsec).UTC()
}

// Filter selects which quarantine records should be replayed.
type Filter struct {
	// Signal restricts replay to one or more comma-separated signals ("spans",
	// "logs", "metrics", or aliases such as "traces") when non-empty.
	Signal string
	// ErrorCodes restricts replay to records whose error_code is in the list
	// when non-empty.
	ErrorCodes []string
	// Since restricts replay to records with ingest_time >= Since when non-zero.
	Since time.Time
	// Until restricts replay to records with ingest_time <= Until when non-zero.
	Until time.Time
}

func (f Filter) matchesSignalAndErrorCode(rec Record) bool {
	if strings.TrimSpace(f.Signal) != "" {
		recCanon := CanonicalSignal(rec.Signal)
		matchedSignal := false
		for _, part := range strings.Split(f.Signal, ",") {
			token := strings.TrimSpace(part)
			if token == "" {
				continue
			}
			tokCanon := CanonicalSignal(token)
			if (tokCanon != "" && tokCanon == recCanon) || strings.EqualFold(token, rec.Signal) {
				matchedSignal = true
				break
			}
		}
		if !matchedSignal {
			return false
		}
	}
	if len(f.ErrorCodes) > 0 {
		matched := slices.ContainsFunc(f.ErrorCodes, func(code string) bool {
			return strings.EqualFold(strings.TrimSpace(code), rec.ErrorCode)
		})
		if !matched {
			return false
		}
	}
	return true
}

// Matches reports whether rec satisfies f.
func (f Filter) Matches(rec Record) bool {
	if !f.matchesSignalAndErrorCode(rec) {
		return false
	}
	if !f.Since.IsZero() || !f.Until.IsZero() {
		ts, ok := rec.ParsedIngestTime()
		if !ok {
			return false
		}
		if !f.Since.IsZero() && ts.Before(f.Since) {
			return false
		}
		if !f.Until.IsZero() && ts.After(f.Until) {
			return false
		}
	}
	return true
}

// Sink receives reconstructed OTLP traces, logs, and metrics batches.
type Sink interface {
	ExportTraces(ctx context.Context, td ptrace.Traces) error
	ExportLogs(ctx context.Context, ld plog.Logs) error
	ExportMetrics(ctx context.Context, md pmetric.Metrics) error
}

// ObjectFetcher resolves an offloaded payload URI (for example,
// "gs://bucket/path/to/object.json") to its raw bytes.
type ObjectFetcher func(ctx context.Context, uri string) ([]byte, error)

// Options configures a Replay run.
type Options struct {
	Filter        Filter
	ObjectFetcher ObjectFetcher
}

// Summary counts the outcome of a Replay run.
type Summary struct {
	TotalRead        int
	Matched          int
	ReplayedSpans    int
	ReplayedLogs     int
	ReplayedMetrics  int
	SkippedFiltered  int
	SkippedNoPayload int
	FailedUnmarshal  int
	FailedExport     int
}

// Replay reads quarantine records from r (either JSONL/NDJSON or a JSON array),
// filters them according to opts.Filter, reconstructs ptrace.Traces,
// plog.Logs, or pmetric.Metrics, and forwards each record to sink.
func Replay(ctx context.Context, r io.Reader, sink Sink, opts Options) (Summary, error) {
	if sink == nil {
		return Summary{}, errors.New("sink must not be nil")
	}

	var sum Summary
	var errs []error
	traceUnmarshaler := &ptrace.JSONUnmarshaler{}
	logUnmarshaler := &plog.JSONUnmarshaler{}
	metricUnmarshaler := &pmetric.JSONUnmarshaler{}

	processRecord := func(i int, rec Record) {
		sum.TotalRead++
		if !opts.Filter.matchesSignalAndErrorCode(rec) {
			sum.SkippedFiltered++
			return
		}
		if !opts.Filter.Since.IsZero() || !opts.Filter.Until.IsZero() {
			ts, ok := rec.ParsedIngestTime()
			if !ok {
				sum.FailedUnmarshal++
				errs = append(errs, fmt.Errorf("record %d: unparseable ingest_time %v", i, rec.IngestTime))
				return
			}
			if (!opts.Filter.Since.IsZero() && ts.Before(opts.Filter.Since)) ||
				(!opts.Filter.Until.IsZero() && ts.After(opts.Filter.Until)) {
				sum.SkippedFiltered++
				return
			}
		}
		sum.Matched++

		payload, ok, fetchErr := resolvePayload(ctx, rec, opts.ObjectFetcher)
		if fetchErr != nil {
			sum.FailedUnmarshal++
			errs = append(errs, fmt.Errorf("record %d: fetching payload_ref: %w", i, fetchErr))
			return
		}
		if !ok {
			sum.SkippedNoPayload++
			return
		}

		switch CanonicalSignal(rec.Signal) {
		case schema.SignalSpans:
			td, uerr := traceUnmarshaler.UnmarshalTraces(payload)
			if uerr != nil {
				sum.FailedUnmarshal++
				errs = append(errs, fmt.Errorf("record %d: unmarshaling traces: %w", i, uerr))
				return
			}
			if eerr := sink.ExportTraces(ctx, td); eerr != nil {
				sum.FailedExport++
				errs = append(errs, fmt.Errorf("record %d: exporting traces: %w", i, eerr))
				return
			}
			sum.ReplayedSpans++

		case schema.SignalLogs:
			ld, uerr := logUnmarshaler.UnmarshalLogs(payload)
			if uerr != nil {
				sum.FailedUnmarshal++
				errs = append(errs, fmt.Errorf("record %d: unmarshaling logs: %w", i, uerr))
				return
			}
			if eerr := sink.ExportLogs(ctx, ld); eerr != nil {
				sum.FailedExport++
				errs = append(errs, fmt.Errorf("record %d: exporting logs: %w", i, eerr))
				return
			}
			sum.ReplayedLogs++

		case schema.SignalMetrics:
			md, uerr := metricUnmarshaler.UnmarshalMetrics(payload)
			if uerr != nil {
				sum.FailedUnmarshal++
				errs = append(errs, fmt.Errorf("record %d: unmarshaling metrics: %w", i, uerr))
				return
			}
			if eerr := sink.ExportMetrics(ctx, md); eerr != nil {
				sum.FailedExport++
				errs = append(errs, fmt.Errorf("record %d: exporting metrics: %w", i, eerr))
				return
			}
			sum.ReplayedMetrics++

		default:
			sum.FailedUnmarshal++
			errs = append(errs, fmt.Errorf("record %d: unsupported signal %q", i, rec.Signal))
		}
	}

	br := bufio.NewReader(r)
	first, err := peekNonSpaceByte(br)
	if errors.Is(err, io.EOF) {
		return Summary{}, nil
	}
	if err != nil {
		return Summary{}, fmt.Errorf("reading quarantine input: %w", err)
	}

	if first == '[' {
		dec := json.NewDecoder(br)
		dec.UseNumber()
		var records []Record
		if err := dec.Decode(&records); err != nil {
			return Summary{}, fmt.Errorf("decoding JSON array of quarantine records: %w", err)
		}
		for i, rec := range records {
			if err := ctx.Err(); err != nil {
				return sum, err
			}
			processRecord(i, rec)
		}
		return sum, errors.Join(errs...)
	}

	scanner := bufio.NewScanner(br)
	scanner.Buffer(make([]byte, 0, 64*1024), maxJSONLLineBytes)
	recordIdx := 0
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return sum, err
		}
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.UseNumber()
		var rec Record
		if err := dec.Decode(&rec); err != nil {
			return sum, fmt.Errorf("decoding quarantine JSONL record %d: %w", recordIdx, err)
		}
		processRecord(recordIdx, rec)
		recordIdx++
	}
	if err := scanner.Err(); err != nil {
		return sum, fmt.Errorf("reading quarantine JSONL input: %w", err)
	}

	return sum, errors.Join(errs...)
}

func resolvePayload(ctx context.Context, rec Record, fetcher ObjectFetcher) ([]byte, bool, error) {
	if strings.TrimSpace(rec.RawPayload) != "" {
		return []byte(rec.RawPayload), true, nil
	}
	if rec.PayloadRef != nil && strings.TrimSpace(rec.PayloadRef.URI) != "" {
		if fetcher == nil {
			return nil, false, fmt.Errorf("payload_ref %q requires an ObjectFetcher", rec.PayloadRef.URI)
		}
		uri := strings.TrimSpace(rec.PayloadRef.URI)
		if ver := strings.TrimSpace(rec.PayloadRef.Version); ver != "" && !hasGenerationParam(uri) {
			sep := "?"
			if strings.Contains(uri, "?") {
				sep = "&"
			}
			uri = uri + sep + "generation=" + url.QueryEscape(ver)
		}
		b, err := fetcher(ctx, uri)
		if err != nil {
			return nil, false, err
		}
		return b, true, nil
	}
	return nil, false, nil
}

func hasGenerationParam(uri string) bool {
	_, rawQuery, ok := strings.Cut(uri, "?")
	if !ok {
		return false
	}
	vals, err := url.ParseQuery(rawQuery)
	return err == nil && strings.TrimSpace(vals.Get("generation")) != ""
}

// ReadRecords parses quarantine records from either a JSON array (such as
// `bq query --format=json`) or newline-delimited JSON (`bq extract`).
func ReadRecords(r io.Reader) ([]Record, error) {
	br := bufio.NewReader(r)
	first, err := peekNonSpaceByte(br)
	if errors.Is(err, io.EOF) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading quarantine input: %w", err)
	}

	dec := json.NewDecoder(br)
	dec.UseNumber()

	if first == '[' {
		var records []Record
		if err := dec.Decode(&records); err != nil {
			return nil, fmt.Errorf("decoding JSON array of quarantine records: %w", err)
		}
		return records, nil
	}

	var records []Record
	for {
		var rec Record
		if err := dec.Decode(&rec); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("decoding quarantine JSONL record %d: %w", len(records), err)
		}
		records = append(records, rec)
	}
	return records, nil
}

func peekNonSpaceByte(br *bufio.Reader) (byte, error) {
	for {
		b, err := br.ReadByte()
		if err != nil {
			return 0, err
		}
		if b != ' ' && b != '\t' && b != '\r' && b != '\n' {
			if uerr := br.UnreadByte(); uerr != nil {
				return 0, uerr
			}
			return b, nil
		}
	}
}

// JSONWriterSink writes replayed OTLP JSON requests as newline-delimited JSON
// envelopes (`{"signal":"spans|logs|metrics","otlp":{...}}`).
type JSONWriterSink struct {
	mu            sync.Mutex
	w             io.Writer
	traceMarshal  ptrace.JSONMarshaler
	logMarshal    plog.JSONMarshaler
	metricMarshal pmetric.JSONMarshaler
}

// NewJSONWriterSink creates a JSONWriterSink writing to w.
func NewJSONWriterSink(w io.Writer) *JSONWriterSink {
	return &JSONWriterSink{w: w}
}

// ExportTraces marshals td as OTLP JSON and writes one envelope line.
func (s *JSONWriterSink) ExportTraces(_ context.Context, td ptrace.Traces) error {
	raw, err := s.traceMarshal.MarshalTraces(td)
	if err != nil {
		return err
	}
	return s.writeEnvelope(schema.SignalSpans, raw)
}

// ExportLogs marshals ld as OTLP JSON and writes one envelope line.
func (s *JSONWriterSink) ExportLogs(_ context.Context, ld plog.Logs) error {
	raw, err := s.logMarshal.MarshalLogs(ld)
	if err != nil {
		return err
	}
	return s.writeEnvelope(schema.SignalLogs, raw)
}

// ExportMetrics marshals md as OTLP JSON and writes one envelope line.
func (s *JSONWriterSink) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	raw, err := s.metricMarshal.MarshalMetrics(md)
	if err != nil {
		return err
	}
	return s.writeEnvelope(schema.SignalMetrics, raw)
}

func (s *JSONWriterSink) writeEnvelope(signal string, rawOTLP []byte) error {
	env := struct {
		Signal string          `json:"signal"`
		OTLP   json.RawMessage `json:"otlp"`
	}{
		Signal: signal,
		OTLP:   json.RawMessage(bytes.TrimSpace(rawOTLP)),
	}
	line, err := json.Marshal(env)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err = s.w.Write(line)
	return err
}

type nopSink struct{}

// NewNopSink returns a Sink that discards all replayed batches (for dry runs).
func NewNopSink() Sink { return nopSink{} }

func (nopSink) ExportTraces(context.Context, ptrace.Traces) error    { return nil }
func (nopSink) ExportLogs(context.Context, plog.Logs) error          { return nil }
func (nopSink) ExportMetrics(context.Context, pmetric.Metrics) error { return nil }

// MemorySink records replayed traces, logs, and metrics batches in memory for tests.
type MemorySink struct {
	mu      sync.Mutex
	Traces  []ptrace.Traces
	Logs    []plog.Logs
	Metrics []pmetric.Metrics
}

// NewMemorySink returns an empty MemorySink.
func NewMemorySink() *MemorySink { return &MemorySink{} }

// ExportTraces appends a copy of td to s.Traces.
func (s *MemorySink) ExportTraces(_ context.Context, td ptrace.Traces) error {
	cp := ptrace.NewTraces()
	td.CopyTo(cp)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Traces = append(s.Traces, cp)
	return nil
}

// ExportLogs appends a copy of ld to s.Logs.
func (s *MemorySink) ExportLogs(_ context.Context, ld plog.Logs) error {
	cp := plog.NewLogs()
	ld.CopyTo(cp)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Logs = append(s.Logs, cp)
	return nil
}

// ExportMetrics appends a copy of md to s.Metrics.
func (s *MemorySink) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	cp := pmetric.NewMetrics()
	md.CopyTo(cp)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Metrics = append(s.Metrics, cp)
	return nil
}

// NewGCSObjectFetcher returns an ObjectFetcher that downloads `gs://bucket/object`
// URIs over the Google Cloud Storage JSON media API, plus a cleanup function.
func NewGCSObjectFetcher(ctx context.Context, opts ...option.ClientOption) (ObjectFetcher, func(), error) {
	client, endpoint, cleanup, err := googlehttp.NewClient(ctx, []string{gcsReadOnlyScope}, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("creating GCS HTTP client: %w", err)
	}
	base := strings.TrimRight(endpoint, "/")
	if base == "" {
		base = defaultGCSEndpoint
	} else if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		base = "https://" + base
	}

	fetcher := func(reqCtx context.Context, uri string) ([]byte, error) {
		bucket, object, generation, perr := parseGSURI(uri)
		if perr != nil {
			return nil, perr
		}
		downloadURL := fmt.Sprintf(
			"%s/storage/v1/b/%s/o/%s?alt=media",
			base,
			url.PathEscape(bucket),
			url.PathEscape(object),
		)
		if generation != "" {
			downloadURL += "&generation=" + url.QueryEscape(generation)
		}
		req, rerr := http.NewRequestWithContext(reqCtx, http.MethodGet, downloadURL, nil)
		if rerr != nil {
			return nil, rerr
		}
		resp, derr := client.Do(req)
		if derr != nil {
			return nil, derr
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("GCS download %q returned HTTP %d", uri, resp.StatusCode)
		}
		return io.ReadAll(resp.Body)
	}
	return fetcher, cleanup, nil
}

func parseGSURI(uri string) (bucket, object, generation string, err error) {
	if !strings.HasPrefix(uri, "gs://") {
		return "", "", "", fmt.Errorf("unsupported URI scheme in %q (expected gs://)", uri)
	}
	rest := strings.TrimPrefix(uri, "gs://")
	var rawQuery string
	if pathPart, q, hasQuery := strings.Cut(rest, "?"); hasQuery {
		rest = pathPart
		rawQuery = q
	}
	slash := strings.IndexByte(rest, '/')
	if slash <= 0 || slash == len(rest)-1 {
		return "", "", "", fmt.Errorf("invalid gs:// URI %q", uri)
	}
	if rawQuery != "" {
		vals, qerr := url.ParseQuery(rawQuery)
		if qerr != nil {
			return "", "", "", fmt.Errorf("invalid query in gs:// URI %q: %w", uri, qerr)
		}
		generation = strings.TrimSpace(vals.Get("generation"))
	}
	return rest[:slash], rest[slash+1:], generation, nil
}
