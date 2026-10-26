// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

// Package quarantinereplay reads rows exported from the BigQuery
// otel_quarantine table, filters them by signal, error_code, and time window,
// reconstructs ptrace.Traces and plog.Logs from raw_payload (or payload_ref),
// and replays them into a configurable Sink.
//
// Rows can also be read straight from the table (ReplayBigQuery) and
// replayed to an OTLP/HTTP receiver (OTLPHTTPSink). A RateLimiter paces a
// run, and a CheckpointConfig makes it resumable after a crash.
package quarantinereplay // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/quarantinereplay"

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
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
	// RateLimiter, when non-nil, paces the run: Replay waits for a token
	// before it hands each reconstructed batch to the sink, so the sink
	// receives at most the limiter's rate of records per second. Records
	// that are filtered out, carry no payload, or fail to decode do not
	// wait.
	RateLimiter *RateLimiter
	// Checkpoint, when non-nil, makes the run resumable after a crash. See
	// CheckpointConfig.
	Checkpoint *CheckpointConfig
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
	// SkippedCheckpointed counts records that an earlier run sharing
	// Options.Checkpoint already handled. They are not part of TotalRead.
	SkippedCheckpointed int
}

// Replay reads quarantine records from r (either JSONL/NDJSON or a JSON array),
// filters them according to opts.Filter, reconstructs ptrace.Traces,
// plog.Logs, or pmetric.Metrics, and forwards each record to sink.
//
// With opts.Checkpoint set, a record's position is its index among the
// input's records, so a resumed run must read the same input.
func Replay(ctx context.Context, r io.Reader, sink Sink, opts Options) (Summary, error) {
	if sink == nil {
		return Summary{}, errors.New("sink must not be nil")
	}
	rp, err := newReplayer(ctx, sink, opts, checkpointKindFile)
	if err != nil {
		return Summary{}, err
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
				return rp.sum, err
			}
			if err := rp.process(i, recordPosition{index: int64(i)}, rec); err != nil {
				return rp.sum, rp.stop(err)
			}
		}
		return rp.sum, errors.Join(rp.errs...)
	}

	scanner := bufio.NewScanner(br)
	scanner.Buffer(make([]byte, 0, 64*1024), maxJSONLLineBytes)
	recordIdx := 0
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return rp.sum, err
		}
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.UseNumber()
		var rec Record
		if err := dec.Decode(&rec); err != nil {
			return rp.sum, fmt.Errorf("decoding quarantine JSONL record %d: %w", recordIdx, err)
		}
		if err := rp.process(recordIdx, recordPosition{index: int64(recordIdx)}, rec); err != nil {
			return rp.sum, rp.stop(err)
		}
		recordIdx++
	}
	if err := scanner.Err(); err != nil {
		return rp.sum, fmt.Errorf("reading quarantine JSONL input: %w", err)
	}

	return rp.sum, errors.Join(rp.errs...)
}

// replayer replays one record at a time and keeps the run's counts, errors,
// and checkpoint.
type replayer struct {
	ctx               context.Context
	sink              Sink
	opts              Options
	cp                *checkpointer
	sum               Summary
	errs              []error
	traceUnmarshaler  ptrace.JSONUnmarshaler
	logUnmarshaler    plog.JSONUnmarshaler
	metricUnmarshaler pmetric.JSONUnmarshaler
}

func newReplayer(ctx context.Context, sink Sink, opts Options, kind string) (*replayer, error) {
	rp := &replayer{ctx: ctx, sink: sink, opts: opts}
	if opts.Checkpoint != nil {
		cp, err := openCheckpoint(*opts.Checkpoint, kind, opts.Filter, sink)
		if err != nil {
			return nil, err
		}
		rp.cp = cp
	}
	return rp, nil
}

// recordPosition locates a record in its source for the checkpoint: its
// index among a file's records, or its sort key in the BigQuery query.
type recordPosition struct {
	index        int64
	ingestMicros int64
	fingerprint  int64
	ordinal      int64
	// canonicalKey is the row's canonical JSON content key (row_key),
	// written into v2 BigQuery cursors and checked alongside ordinal when
	// resuming inside the cursor's (ingest_time, row_fp) group. If the
	// current group no longer has canonicalKey at ordinal (for example,
	// because earlier rows in the group were deleted or a cursor carries a
	// mismatched key), both the query and the client rewind that
	// (ingest_time, row_fp) group to replay it at-least-once.
	canonicalKey string
}

// after reports whether p sorts after the BigQuery cursor c. Within the
// cursor's (ingest_time, row_fp) group, a v2 cursor with a non-empty
// CanonicalRowKey only treats a position at or before DuplicateOrdinal as
// already handled when its canonicalKey is consistent with c.CanonicalRowKey
// (equal at DuplicateOrdinal, and <= c.CanonicalRowKey before it).
func (p recordPosition) after(c bigQueryCursor) bool {
	if p.ingestMicros != c.IngestTimeMicros {
		return p.ingestMicros > c.IngestTimeMicros
	}
	if p.fingerprint != c.RowFingerprint {
		return p.fingerprint > c.RowFingerprint
	}
	if c.DuplicateOrdinal < 0 || c.CanonicalRowKey == "" {
		return true
	}
	if p.ordinal == c.DuplicateOrdinal {
		return p.canonicalKey != c.CanonicalRowKey
	}
	if p.ordinal < c.DuplicateOrdinal {
		return p.canonicalKey > c.CanonicalRowKey
	}
	return true
}

// process replays rec, the record numbered i at pos. A non-nil error means
// the run must stop; failures of rec itself are counted and collected in
// rp.errs instead.
func (rp *replayer) process(i int, pos recordPosition, rec Record) error {
	if rp.cp != nil && rp.cp.handled(pos) {
		rp.sum.SkippedCheckpointed++
		return nil
	}
	retry, err := rp.replayRecord(i, rec)
	if err != nil {
		return err
	}
	if rp.cp == nil {
		return nil
	}
	if retry {
		// Moving past the record would lose it, so stop here and leave
		// it for the next run.
		if err := rp.ctx.Err(); err != nil {
			return err
		}
		return fmt.Errorf("record %d: replay stopped at a failure that may be transient; rerun with checkpoint %q to retry from this record", i, rp.cp.path)
	}
	return rp.cp.advance(pos)
}

// stop is what a run returns when process stops it: a context error alone,
// as a canceled run always returns, and anything else after the record
// errors collected so far.
func (rp *replayer) stop(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return errors.Join(append(rp.errs, err)...)
}

// replayRecord counts rec in rp.sum and, when it passes the filters and
// decodes, hands it to the sink. retry reports a failure that a later
// attempt could fix: a payload_ref that could not be fetched or a batch the
// sink did not take, unless the error is a PermanentError. A non-nil err
// means the run stopped before rec could be replayed; rec is then not
// counted at all.
func (rp *replayer) replayRecord(i int, rec Record) (retry bool, err error) {
	f := rp.opts.Filter
	if !f.matchesSignalAndErrorCode(rec) {
		rp.sum.TotalRead++
		rp.sum.SkippedFiltered++
		return false, nil
	}
	if !f.Since.IsZero() || !f.Until.IsZero() {
		ts, ok := rec.ParsedIngestTime()
		if !ok {
			rp.sum.TotalRead++
			rp.sum.FailedUnmarshal++
			rp.errs = append(rp.errs, fmt.Errorf("record %d: unparseable ingest_time %v", i, rec.IngestTime))
			return false, nil
		}
		if (!f.Since.IsZero() && ts.Before(f.Since)) ||
			(!f.Until.IsZero() && ts.After(f.Until)) {
			rp.sum.TotalRead++
			rp.sum.SkippedFiltered++
			return false, nil
		}
	}

	payload, ok, fetchErr := resolvePayload(rp.ctx, rec, rp.opts.ObjectFetcher)
	if fetchErr != nil {
		rp.failUnmarshal(fmt.Errorf("record %d: fetching payload_ref: %w", i, fetchErr))
		return !isPermanent(fetchErr), nil
	}
	if !ok {
		rp.countMatched()
		rp.sum.SkippedNoPayload++
		return false, nil
	}

	var (
		export   func() error
		replayed *int
		noun     string
	)
	switch CanonicalSignal(rec.Signal) {
	case schema.SignalSpans:
		td, uerr := rp.traceUnmarshaler.UnmarshalTraces(payload)
		if uerr != nil {
			rp.failUnmarshal(fmt.Errorf("record %d: unmarshaling traces: %w", i, uerr))
			return false, nil
		}
		export = func() error { return rp.sink.ExportTraces(rp.ctx, td) }
		replayed, noun = &rp.sum.ReplayedSpans, "traces"

	case schema.SignalLogs:
		ld, uerr := rp.logUnmarshaler.UnmarshalLogs(payload)
		if uerr != nil {
			rp.failUnmarshal(fmt.Errorf("record %d: unmarshaling logs: %w", i, uerr))
			return false, nil
		}
		export = func() error { return rp.sink.ExportLogs(rp.ctx, ld) }
		replayed, noun = &rp.sum.ReplayedLogs, "logs"

	case schema.SignalMetrics:
		md, uerr := rp.metricUnmarshaler.UnmarshalMetrics(payload)
		if uerr != nil {
			rp.failUnmarshal(fmt.Errorf("record %d: unmarshaling metrics: %w", i, uerr))
			return false, nil
		}
		export = func() error { return rp.sink.ExportMetrics(rp.ctx, md) }
		replayed, noun = &rp.sum.ReplayedMetrics, "metrics"

	default:
		rp.failUnmarshal(fmt.Errorf("record %d: unsupported signal %q", i, rec.Signal))
		return false, nil
	}

	if rp.opts.RateLimiter != nil {
		if err := rp.opts.RateLimiter.Wait(rp.ctx); err != nil {
			return false, err
		}
	}
	rp.countMatched()
	if eerr := export(); eerr != nil {
		rp.sum.FailedExport++
		rp.errs = append(rp.errs, fmt.Errorf("record %d: exporting %s: %w", i, noun, eerr))
		return !isPermanent(eerr), nil
	}
	*replayed++
	return false, nil
}

// countMatched counts a record that passed the filters.
func (rp *replayer) countMatched() {
	rp.sum.TotalRead++
	rp.sum.Matched++
}

// failUnmarshal counts a matched record whose payload could not be fetched
// or decoded.
func (rp *replayer) failUnmarshal(err error) {
	rp.countMatched()
	rp.sum.FailedUnmarshal++
	rp.errs = append(rp.errs, err)
}

// PermanentError marks a failure that retrying the same record cannot fix,
// such as an OTLP receiver rejecting a payload with HTTP 400, or an offloaded
// payload object that no longer exists. A checkpointed run counts such a
// record as failed and moves past it; any other fetch or export failure
// stops a checkpointed run so that the next run retries the record.
type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string { return e.Err.Error() }

func (e *PermanentError) Unwrap() error { return e.Err }

func isPermanent(err error) bool {
	var pe *PermanentError
	return errors.As(err, &pe)
}

// RateLimiter is a token bucket that admits at most a fixed number of
// records per second on average. The bucket holds up to one second's worth
// of tokens (at least one) and starts with a single token, so a run never
// opens with a burst, and the average rate over any window of a second or
// more stays at or below the limit.
type RateLimiter struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
	now    func() time.Time
	sleep  func(context.Context, time.Duration) error
}

// NewRateLimiter returns a RateLimiter that admits perSecond records per
// second.
func NewRateLimiter(perSecond float64) (*RateLimiter, error) {
	if !(perSecond > 0) || math.IsInf(perSecond, 0) {
		return nil, fmt.Errorf("rate limit must be a positive, finite number of records per second, got %v", perSecond)
	}
	return newRateLimiter(perSecond, time.Now, sleepContext), nil
}

func newRateLimiter(perSecond float64, now func() time.Time, sleep func(context.Context, time.Duration) error) *RateLimiter {
	return &RateLimiter{
		rate:   perSecond,
		burst:  math.Max(1, perSecond),
		tokens: 1,
		now:    now,
		sleep:  sleep,
	}
}

// Wait blocks until the bucket holds a token, then takes it. It returns the
// context's error if ctx ends first.
func (l *RateLimiter) Wait(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		l.mu.Lock()
		now := l.now()
		if l.last.IsZero() {
			l.last = now
		} else if elapsed := now.Sub(l.last); elapsed > 0 {
			l.tokens = math.Min(l.burst, l.tokens+elapsed.Seconds()*l.rate)
			l.last = now
		}
		if l.tokens >= 1 {
			l.tokens--
			l.mu.Unlock()
			return nil
		}
		wait := rateLimitWait(1-l.tokens, l.rate)
		l.mu.Unlock()
		if err := l.sleep(ctx, wait); err != nil {
			return err
		}
	}
}

func rateLimitWait(deficit, rate float64) time.Duration {
	if deficit <= 0 {
		return 0
	}
	ns := math.Ceil(deficit / rate * float64(time.Second))
	if ns >= float64(math.MaxInt64) || math.IsInf(ns, 1) || math.IsNaN(ns) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(ns)
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

const (
	checkpointVersion      = 1
	checkpointKindFile     = "file"
	checkpointKindBigQuery = "bigquery"
	// cursorOrderVersionCanonical is the row-ordering version a BigQuery
	// cursor is written with. Version 1 (or 0, the zero value for a legacy
	// checkpoint that predates the field) used an unordered ROW_NUMBER()
	// assignment whose duplicate_ordinal may not match the canonical
	// row_key ordering the current query uses. Version 2 cursors carry the
	// canonical row_key so the resume position is unambiguous.
	cursorOrderVersionCanonical = 2
)

// CheckpointConfig makes a replay resumable. After every record it handles,
// the run records its position in Path, replacing the file atomically: it
// writes a temporary file in the same directory, syncs it, renames it over
// Path, and syncs the directory. For file inputs and unchanged v2 BigQuery
// cursors, a run that finds Path skips the records an earlier run handled, so
// after a crash only the record in flight can be replayed twice (at-least-once
// delivery). For BigQuery checkpoints whose cursor is legacy
// (cursor_order_version 0 or 1), carries an empty canonical_row_key, or cannot
// reconcile its saved (canonical_row_key, duplicate_ordinal) with the current
// (ingest_time, row_fp) group (for example, when earlier rows in that group
// were removed from the table), the run rewinds and replays the whole current
// (ingest_time, row_fp) group at least once while keeping earlier groups
// excluded and later groups proceeding normally.
//
// A record counts as handled once it is replayed, filtered out, or fails in
// a way a retry cannot fix (no payload, an unparseable payload or time, an
// unsupported signal, or a PermanentError). Any other fetch or export
// failure stops the run at that record, so the next run retries it.
//
// Before each update the run flushes a sink that supports it (see
// JSONWriterSink.Sync), so the checkpoint never runs ahead of the output.
type CheckpointConfig struct {
	// Path is the checkpoint file.
	Path string
	// Source names the input, for example "file:/abs/path/quarantine.jsonl".
	// ReplayBigQuery fills it in from the table when it is empty. A
	// checkpoint written for another source or filter is refused rather
	// than applied to records it does not describe.
	Source string
}

// checkpointState is the checkpoint file's content.
type checkpointState struct {
	Version int    `json:"version"`
	Kind    string `json:"kind"`
	Source  string `json:"source"`
	Filter  string `json:"filter"`
	// Next is the index of the first record a file source has not handled.
	Next int64 `json:"next,omitempty"`
	// Cursor is the sort key of the last row a BigQuery source handled.
	Cursor *bigQueryCursor `json:"cursor,omitempty"`
	// Handled counts the records handled by every run sharing the file.
	Handled   int64     `json:"handled"`
	UpdatedAt time.Time `json:"updated_at"`
}

// bigQueryCursor is the (ingest_time, row_fp, row_ord) sort key of a
// quarantine row read by ReplayBigQuery. CursorOrderVersion records the
// row-ordering version the cursor was written under: version 2 cursors carry a
// validated CanonicalRowKey and use the canonical row_key ordering, skipping
// handled rows when CanonicalRowKey still sits at DuplicateOrdinal in the
// current (ingest_time, row_fp) group and rewinding that group at-least-once
// if the group's membership changed; legacy version 0 or 1 cursors (or a
// version 2 cursor with an empty CanonicalRowKey) are rewound to replay their
// whole (ingest_time, row_fp) group at-least-once before any delivery.
// Negative or unknown CursorOrderVersion values and malformed CanonicalRowKey
// values are rejected before querying.
type bigQueryCursor struct {
	IngestTimeMicros int64 `json:"ingest_time_micros"`
	RowFingerprint   int64 `json:"row_fingerprint"`
	DuplicateOrdinal int64 `json:"duplicate_ordinal,omitempty"`
	// CursorOrderVersion is the row-ordering version this cursor was
	// written under (supported: legacy 0 or 1, and current
	// cursorOrderVersionCanonical = 2).
	CursorOrderVersion int    `json:"cursor_order_version,omitempty"`
	CanonicalRowKey    string `json:"canonical_row_key,omitempty"`
}

// checkpointTempFile is the temporary file seam checkpointer.save uses.
type checkpointTempFile interface {
	Name() string
	Write([]byte) (int, error)
	Sync() error
	Close() error
}

// checkpointFS holds the filesystem operations checkpointer.save uses so
// tests can observe durability ordering and inject faults.
type checkpointFS struct {
	createTemp func(dir, pattern string) (checkpointTempFile, error)
	rename     func(oldpath, newpath string) error
	remove     func(name string) error
	syncDir    func(dir string) error
}

var defaultCheckpointFS = checkpointFS{
	createTemp: func(dir, pattern string) (checkpointTempFile, error) {
		return os.CreateTemp(dir, pattern)
	},
	rename:  os.Rename,
	remove:  os.Remove,
	syncDir: syncDir,
}

var checkpointHooks = defaultCheckpointFS

// checkpointer loads, advances, and saves a checkpoint.
type checkpointer struct {
	path  string
	sink  Sink
	state checkpointState
	fs    checkpointFS
}

func openCheckpoint(cfg CheckpointConfig, kind string, f Filter, sink Sink) (*checkpointer, error) {
	if strings.TrimSpace(cfg.Path) == "" {
		return nil, errors.New("checkpoint path must not be empty")
	}
	want := checkpointState{
		Version: checkpointVersion,
		Kind:    kind,
		Source:  cfg.Source,
		Filter:  f.fingerprint(),
	}
	c := &checkpointer{path: cfg.Path, sink: sink, state: want, fs: checkpointHooks}
	data, err := os.ReadFile(cfg.Path)
	if errors.Is(err, fs.ErrNotExist) {
		return c, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading checkpoint %q: %w", cfg.Path, err)
	}
	var got checkpointState
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&got); err != nil {
		return nil, fmt.Errorf("parsing checkpoint %q: %w", cfg.Path, err)
	}
	if got.Version != checkpointVersion {
		return nil, fmt.Errorf("checkpoint %q has version %d, want %d", cfg.Path, got.Version, checkpointVersion)
	}
	if got.Kind != want.Kind || got.Source != want.Source || got.Filter != want.Filter {
		return nil, fmt.Errorf(
			"checkpoint %q was written for %s source %q with filter %s, but this run reads %s source %q with filter %s; remove it or choose another checkpoint file",
			cfg.Path, got.Kind, got.Source, got.Filter, want.Kind, want.Source, want.Filter)
	}
	if got.Next < 0 || got.Handled < 0 || (got.Cursor != nil && got.Cursor.DuplicateOrdinal < 0) {
		return nil, fmt.Errorf("checkpoint %q holds a negative position", cfg.Path)
	}
	if got.Kind == checkpointKindBigQuery && got.Cursor != nil {
		switch got.Cursor.CursorOrderVersion {
		case 0, 1:
			// A BigQuery cursor written with an earlier row-ordering version
			// (before canonical row_key ordering was introduced) has
			// CursorOrderVersion 0 or 1. Its DuplicateOrdinal was assigned by
			// an unordered ROW_NUMBER(), so resuming after it can skip
			// undelivered rows in the same (ingest_time, row_fp) group.
			// Rewind it to replay the whole group under canonical ordering
			// at-least-once.
			got.Cursor.DuplicateOrdinal = -1
			got.Cursor.CursorOrderVersion = cursorOrderVersionCanonical
			got.Cursor.CanonicalRowKey = ""
		case cursorOrderVersionCanonical:
			if got.Cursor.CanonicalRowKey == "" {
				// An explicit v2 cursor with an empty CanonicalRowKey cannot
				// be reconciled with a specific row in its group; rewind the
				// group to replay it at-least-once.
				got.Cursor.DuplicateOrdinal = -1
			} else if err := validateCanonicalRowKey(got.Cursor.CanonicalRowKey); err != nil {
				return nil, fmt.Errorf("checkpoint %q has invalid canonical_row_key: %w", cfg.Path, err)
			}
		default:
			return nil, fmt.Errorf(
				"checkpoint %q has unsupported cursor_order_version %d (supported: 0, 1, %d)",
				cfg.Path, got.Cursor.CursorOrderVersion, cursorOrderVersionCanonical)
		}
	}
	c.state = got
	return c, nil
}

var allowedCanonicalRowKeyFields = map[string]bool{
	"signal":              true,
	"error_code":          true,
	"error_message":       true,
	"service_name":        true,
	"trace_id":            true,
	"span_id":             true,
	"raw_payload":         true,
	"payload_ref_uri":     true,
	"payload_ref_version": true,
}

func validateCanonicalRowKey(raw string) error {
	dec := json.NewDecoder(strings.NewReader(raw))
	var fields map[string]json.RawMessage
	if err := dec.Decode(&fields); err != nil {
		return fmt.Errorf("not a JSON object: %w", err)
	}
	if fields == nil {
		return errors.New("not a JSON object")
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("unexpected trailing data after JSON object")
	}
	if !json.Valid([]byte(raw)) {
		return errors.New("not valid JSON")
	}
	for k, v := range fields {
		if !allowedCanonicalRowKeyFields[k] {
			return fmt.Errorf("unexpected field %q", k)
		}
		if string(v) == "null" {
			continue
		}
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			return fmt.Errorf("field %q is not a string: %w", k, err)
		}
	}
	for _, req := range []string{"signal", "error_code", "raw_payload"} {
		if _, ok := fields[req]; !ok {
			return fmt.Errorf("missing required field %q", req)
		}
	}
	return nil
}

// rewindCursorGroup rewinds the BigQuery cursor's current (ingest_time, row_fp)
// group when its saved (CanonicalRowKey, DuplicateOrdinal) no longer matches
// the group's rows, so every row in that group is replayed at-least-once.
func (c *checkpointer) rewindCursorGroup() {
	if c != nil && c.state.Kind == checkpointKindBigQuery && c.state.Cursor != nil {
		c.state.Cursor.DuplicateOrdinal = -1
		c.state.Cursor.CanonicalRowKey = ""
	}
}

// handled reports whether an earlier run already handled the record at pos.
func (c *checkpointer) handled(pos recordPosition) bool {
	if c.state.Kind == checkpointKindBigQuery {
		return c.state.Cursor != nil && !pos.after(*c.state.Cursor)
	}
	return pos.index < c.state.Next
}

// advance records the record at pos as handled and saves the checkpoint.
func (c *checkpointer) advance(pos recordPosition) error {
	if c.state.Kind == checkpointKindBigQuery {
		if c.state.Cursor == nil || pos.after(*c.state.Cursor) {
			c.state.Cursor = &bigQueryCursor{
				IngestTimeMicros:   pos.ingestMicros,
				RowFingerprint:     pos.fingerprint,
				DuplicateOrdinal:   pos.ordinal,
				CursorOrderVersion: cursorOrderVersionCanonical,
				CanonicalRowKey:    pos.canonicalKey,
			}
		}
	} else if pos.index >= c.state.Next {
		c.state.Next = pos.index + 1
	}
	c.state.Handled++
	if err := syncSink(c.sink); err != nil {
		return fmt.Errorf("flushing replay output before updating checkpoint %q: %w", c.path, err)
	}
	return c.save()
}

// save replaces the checkpoint file atomically and durably.
func (c *checkpointer) save() error {
	c.state.UpdatedAt = time.Now().UTC()
	data, err := json.MarshalIndent(c.state, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding checkpoint %q: %w", c.path, err)
	}
	data = append(data, '\n')
	dir := filepath.Dir(c.path)
	tmp, err := c.fs.createTemp(dir, "."+filepath.Base(c.path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("creating temporary checkpoint file: %w", err)
	}
	tmpName := tmp.Name()
	renamed := false
	defer func() {
		if !renamed {
			_ = c.fs.remove(tmpName)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("writing checkpoint %q: %w", c.path, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("syncing checkpoint %q: %w", c.path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing checkpoint %q: %w", c.path, err)
	}
	if err := c.fs.rename(tmpName, c.path); err != nil {
		return fmt.Errorf("replacing checkpoint %q: %w", c.path, err)
	}
	renamed = true
	return c.fs.syncDir(dir)
}

// syncDir syncs dir so that a rename inside it survives a power loss.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("opening checkpoint directory: %w", err)
	}
	serr := d.Sync()
	cerr := d.Close()
	if serr != nil && !ignorableSyncError(serr) {
		return fmt.Errorf("syncing checkpoint directory: %w", serr)
	}
	if cerr != nil {
		return fmt.Errorf("closing checkpoint directory: %w", cerr)
	}
	return nil
}

// ignorableSyncError reports whether err only says that the file cannot be
// synced at all, as a pipe or a terminal cannot.
func ignorableSyncError(err error) bool {
	return errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTSUP) || errors.Is(err, errors.ErrUnsupported)
}

// syncer is implemented by sinks and writers whose output can be flushed to
// stable storage.
type syncer interface {
	Sync() error
}

func syncSink(s Sink) error {
	if sy, ok := s.(syncer); ok {
		return sy.Sync()
	}
	return nil
}

// fingerprint describes f canonically, so that equivalent spellings of a
// filter (such as "traces,logs" and "logs,spans") compare equal, and a
// checkpoint is never applied under a filter that selects other records.
func (f Filter) fingerprint() string {
	var signals []string
	for _, part := range strings.Split(f.Signal, ",") {
		token := strings.TrimSpace(part)
		if token == "" {
			continue
		}
		if canon := CanonicalSignal(token); canon != "" {
			token = canon
		}
		signals = append(signals, token)
	}
	slices.Sort(signals)
	signals = slices.Compact(signals)
	var codes []string
	for _, code := range f.ErrorCodes {
		codes = append(codes, strings.TrimSpace(code))
	}
	slices.Sort(codes)
	codes = slices.Compact(codes)
	desc := struct {
		Signals    []string `json:"signals,omitempty"`
		ErrorCodes []string `json:"error_codes,omitempty"`
		Since      string   `json:"since,omitempty"`
		Until      string   `json:"until,omitempty"`
	}{Signals: signals, ErrorCodes: codes}
	if !f.Since.IsZero() {
		desc.Since = f.Since.UTC().Format(time.RFC3339Nano)
	}
	if !f.Until.IsZero() {
		desc.Until = f.Until.UTC().Format(time.RFC3339Nano)
	}
	b, err := json.Marshal(desc)
	if err != nil {
		// Marshaling strings and string slices cannot fail.
		return fmt.Sprintf("%+v", desc)
	}
	return string(b)
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

// Sync flushes the underlying writer to stable storage when it supports
// Sync, as *os.File does. A checkpointed run calls it before recording
// progress, so a crash cannot leave the checkpoint ahead of the output. A
// writer that cannot be synced at all, such as a pipe, is not an error.
func (s *JSONWriterSink) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sy, ok := s.w.(syncer)
	if !ok {
		return nil
	}
	if st, ok := s.w.(interface{ Stat() (fs.FileInfo, error) }); ok {
		fi, err := st.Stat()
		if err != nil {
			return err
		}
		if !fi.Mode().IsRegular() {
			return nil
		}
	}
	if err := sy.Sync(); err != nil && !ignorableSyncError(err) {
		return err
	}
	return nil
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
			return nil, &PermanentError{Err: perr}
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
			err := fmt.Errorf("GCS download %q returned HTTP %d", uri, resp.StatusCode)
			if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusGone {
				// The object is gone (for example, expired by a lifecycle
				// rule); no retry can bring it back.
				return nil, &PermanentError{Err: err}
			}
			return nil, err
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
