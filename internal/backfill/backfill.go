// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

// Package backfill appends historical OTLP JSON archives held in Cloud
// Storage to the exporter's BigQuery tables.
//
// Every object under a gs:// prefix whose name ends in .jsonl or .jsonl.gz is
// read line by line. Each line is one OTLP/JSON export request, the format
// the Collector's file exporter writes: {"resourceSpans": [...]},
// {"resourceLogs": [...]}, or {"resourceMetrics": [...]}. Compression is
// detected from the gzip magic number rather than from the name, so an object
// that Cloud Storage serves decompressed (one stored with Content-Encoding:
// gzip) reads correctly too.
//
// Records go through the exporter's own pipeline: redaction as step 0, then
// the internal/transform row builders with the default structural limits,
// then the protobuf encoder for the compiled table schema, then the Storage
// Write API _default stream through internal/bigquery. Rows keep the times
// the telemetry carries. Spans partition on start_time and logs and metrics
// on timestamp, so a backfill writes into the historical partitions those
// times select; receive_time records when the backfill appended the row. A
// log record with neither an event time nor an observed time falls back to
// the receive time, as it does in the exporter.
//
// Three consequences follow. A table's partition_expiration_days applies:
// rows older than the expiration fall in partitions that have already
// expired, and BigQuery does not keep them, so check the expiration before
// backfilling older data. Delivery is at-least-once: an append whose
// acknowledgement was lost is sent again, and running a backfill again
// appends its rows again. The shipped _dedup views collapse those copies,
// because span identity and the log and metric fingerprints come from the
// telemetry, not from when it was appended. Finally, the destination tables
// must already exist; bq-otel-validate checks them.
package backfill // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/backfill"

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/auth/credentials"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	bigqueryapi "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"

	bqi "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/bigquery"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/googlehttp"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/protoenc"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/redact"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/transform"
)

// Values for Config.Signal.
const (
	SignalTraces  = "traces"
	SignalLogs    = "logs"
	SignalMetrics = "metrics"
	// SignalAuto appends every signal found, line by line.
	SignalAuto = "auto"
)

// Defaults for the zero values of Config fields.
const (
	DefaultTracesTable     = "otel_spans"
	DefaultLogsTable       = "otel_logs"
	DefaultMetricsTable    = "otel_metrics"
	DefaultBatchSize       = 500
	DefaultMaxWorkers      = 4
	DefaultMaxLineBytes    = 64 << 20
	DefaultMaxAttempts     = 5
	DefaultRetryBackoff    = time.Second
	DefaultRetryMaxBackoff = 30 * time.Second
)

// Write settings. They are the exporter's defaults; MaxRowBytes is the
// encoded size above which the exporter drops a row.
const (
	maxRowBytes         = 1 << 20
	maxRequestBytes     = 8 << 20
	maxInflightRequests = 8
	maxInflightBytes    = 64 << 20
	writeTraceID        = "otel-bigqueryexporter-backfill"

	gcsReadOnlyScope   = "https://www.googleapis.com/auth/devstorage.read_only"
	defaultGCSEndpoint = "https://storage.googleapis.com"

	// maxListedFailures caps Report.Failures; Report.FailureCount keeps
	// the total.
	maxListedFailures = 100
)

// allSignals fixes the order signals are flushed and reported in.
var allSignals = []string{SignalTraces, SignalLogs, SignalMetrics}

// Config configures one backfill run.
type Config struct {
	// Source is the gs://bucket/prefix whose .jsonl and .jsonl.gz objects
	// are appended. As in Cloud Storage listings, the prefix matches names
	// that start with it, so end it with "/" to select one folder.
	Source string
	// Project and Dataset name the destination dataset.
	Project string
	Dataset string
	// Signal selects what is appended: SignalTraces, SignalLogs,
	// SignalMetrics, or SignalAuto (the default) for all three. Lines of a
	// signal that is not selected are counted and skipped.
	Signal string
	// TracesTable, LogsTable, and MetricsTable name the destination tables.
	// Empty means the exporter's defaults.
	TracesTable  string
	LogsTable    string
	MetricsTable string
	// BatchSize caps the rows in one append. Zero means DefaultBatchSize.
	BatchSize int
	// MaxWorkers is how many objects are read and appended at once. Zero
	// means DefaultMaxWorkers.
	MaxWorkers int

	// Redact enables client-side redaction (the exporter's
	// redaction.enabled, which defaults to false). Like the live exporter,
	// Redact is authoritative: when Redact is false, redaction stays
	// disabled even if other redaction fields (such as MaskReplacement or
	// BlockedKeys copied from a deployment config) are populated.
	//
	// BlockedKeys are attribute keys masked in addition to
	// redact.DefaultBlockedKeys (unless DisableDefaultKeys is true),
	// matched case-insensitively; '*' is a glob.
	// DisableDefaultKeys disables redact.DefaultBlockedKeys so only
	// BlockedKeys apply (the exporter's redaction.disable_default_keys).
	// BlockedValuePatterns are RE2 expressions whose matches are masked
	// inside string values, MaskReplacement is the replacement text
	// (defaulting to redact.DefaultMaskReplacement), and RedactLogBody
	// applies value patterns to log bodies. Use the deployment's redaction
	// settings so backfilled rows look like exported ones.
	Redact               bool
	BlockedKeys          []string
	DisableDefaultKeys   bool
	BlockedValuePatterns []string
	MaskReplacement      string
	RedactLogBody        bool
	// LogNameAttribute and InsertIDAttribute are the exporter's
	// logs.log_name_attribute and logs.insert_id_attribute. Use the
	// deployment's values so backfilled logs get the same identity.
	LogNameAttribute  string
	InsertIDAttribute string

	// MaxLineBytes bounds one line; a longer line is reported and skipped.
	// Zero means DefaultMaxLineBytes.
	MaxLineBytes int
	// MaxAttempts bounds the attempts at one append and at one Cloud
	// Storage request. Zero means DefaultMaxAttempts.
	MaxAttempts int
	// RetryBackoff is the first wait between attempts. It doubles up to
	// RetryMaxBackoff. Zero means the defaults.
	RetryBackoff    time.Duration
	RetryMaxBackoff time.Duration

	// Store reads the archive. Nil means Cloud Storage through its JSON
	// API, configured by StorageClientOptions; with no options it uses
	// Application Default Credentials.
	Store                ObjectStore
	StorageClientOptions []option.ClientOption
	// WriteClientOptions configure the Storage Write API client. Nil uses
	// Application Default Credentials.
	WriteClientOptions []option.ClientOption

	// OnObject, when set, is called as each object finishes, whether read
	// to the end or stopped early. Calls are serialized.
	OnObject func(ObjectResult)
	// Now supplies the receive time stamped on every row. Nil means
	// time.Now.
	Now func() time.Time
}

// ParseSource splits a gs://bucket/prefix URL. An empty prefix selects the
// whole bucket.
func ParseSource(source string) (bucket, prefix string, err error) {
	rest, ok := strings.CutPrefix(source, "gs://")
	if !ok {
		return "", "", fmt.Errorf("source %q: want gs://bucket/prefix", source)
	}
	bucket, prefix, _ = strings.Cut(rest, "/")
	if bucket == "" {
		return "", "", fmt.Errorf("source %q names no bucket", source)
	}
	return bucket, prefix, nil
}

// Validate reports the first problem with c. It leaves c unchanged.
func (c Config) Validate() error {
	return c.normalize()
}

func (c *Config) normalize() error {
	if _, _, err := ParseSource(c.Source); err != nil {
		return err
	}
	if c.Project == "" {
		return errors.New("project is required")
	}
	if c.Dataset == "" {
		return errors.New("dataset is required")
	}
	switch c.Signal {
	case "":
		c.Signal = SignalAuto
	case SignalTraces, SignalLogs, SignalMetrics, SignalAuto:
	default:
		return fmt.Errorf("signal %q: want traces, logs, metrics, or auto", c.Signal)
	}
	for _, t := range []struct {
		field *string
		def   string
	}{
		{&c.TracesTable, DefaultTracesTable},
		{&c.LogsTable, DefaultLogsTable},
		{&c.MetricsTable, DefaultMetricsTable},
	} {
		if *t.field == "" {
			*t.field = t.def
		}
	}
	for _, n := range []struct {
		name  string
		field *int
		def   int
	}{
		{"batch size", &c.BatchSize, DefaultBatchSize},
		{"max workers", &c.MaxWorkers, DefaultMaxWorkers},
		{"max line bytes", &c.MaxLineBytes, DefaultMaxLineBytes},
		{"max attempts", &c.MaxAttempts, DefaultMaxAttempts},
	} {
		switch {
		case *n.field == 0:
			*n.field = n.def
		case *n.field < 0:
			return fmt.Errorf("%s must be positive, got %d", n.name, *n.field)
		}
	}
	for _, d := range []struct {
		name  string
		field *time.Duration
		def   time.Duration
	}{
		{"retry backoff", &c.RetryBackoff, DefaultRetryBackoff},
		{"retry max backoff", &c.RetryMaxBackoff, DefaultRetryMaxBackoff},
	} {
		switch {
		case *d.field == 0:
			*d.field = d.def
		case *d.field < 0:
			return fmt.Errorf("%s must be positive, got %v", d.name, *d.field)
		}
	}
	if c.RetryMaxBackoff < c.RetryBackoff {
		c.RetryMaxBackoff = c.RetryBackoff
	}
	if _, err := c.redactor(); err != nil {
		return fmt.Errorf("redaction: %w", err)
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	return nil
}

func (c *Config) redactor() (*redact.Redactor, error) {
	if _, err := redact.CompilePatterns(c.BlockedValuePatterns); err != nil {
		return nil, err
	}
	for i, k := range c.BlockedKeys {
		if strings.TrimSpace(k) == "" {
			return nil, fmt.Errorf("blocked_keys[%d] is empty", i)
		}
	}
	if !c.Redact {
		return nil, nil
	}
	var keys []string
	if !c.DisableDefaultKeys {
		keys = append(keys, redact.DefaultBlockedKeys()...)
	}
	keys = append(keys, c.BlockedKeys...)
	if len(keys) == 0 && len(c.BlockedValuePatterns) == 0 {
		return nil, errors.New("redact is enabled but disable_default_keys leaves no blocked_keys and no blocked_value_patterns")
	}
	mask := c.MaskReplacement
	if mask == "" {
		mask = redact.DefaultMaskReplacement
	}
	rd, err := redact.New(redact.Config{
		BlockedKeys:          keys,
		BlockedValuePatterns: c.BlockedValuePatterns,
		MaskReplacement:      mask,
		RedactLogBody:        c.RedactLogBody,
	})
	if err != nil {
		return nil, err
	}
	for _, item := range []struct{ field, key string }{
		{"insert_id_attribute", c.InsertIDAttribute},
		{"log_name_attribute", c.LogNameAttribute},
	} {
		if item.key != "" && rd.KeyBlocked(item.key) {
			return nil, fmt.Errorf("%s %q matches a blocked redaction key (built-in or blocked_keys) and would be masked on every record", item.field, item.key)
		}
	}
	return rd, nil
}

func (c *Config) signals() []string {
	if c.Signal == SignalAuto {
		return allSignals
	}
	return []string{c.Signal}
}

func (c *Config) table(signal string) string {
	switch signal {
	case SignalTraces:
		return c.TracesTable
	case SignalLogs:
		return c.LogsTable
	default:
		return c.MetricsTable
	}
}

func tableSchema(signal string) (*storagepb.TableSchema, string) {
	switch signal {
	case SignalTraces:
		return schema.SpansTableSchema(), schema.SignalSpans
	case SignalLogs:
		return schema.LogsTableSchema(), schema.SignalLogs
	default:
		return schema.MetricsTableSchema(), schema.SignalMetrics
	}
}

func recordUnit(signal string) string {
	switch signal {
	case SignalTraces:
		return "spans"
	case SignalLogs:
		return "log records"
	default:
		return "data points"
	}
}

// Object is one archive object.
type Object struct {
	Name string
	// Generation pins the version read, so an object overwritten during
	// the run is read as it was listed. Empty reads the live version.
	Generation string
	Size       int64
}

// ObjectStore lists and reads archive objects.
type ObjectStore interface {
	// List calls fn for every object whose name starts with prefix, in
	// name order, and stops at the first error fn returns.
	List(ctx context.Context, bucket, prefix string, fn func(Object) error) error
	// Open returns the object's bytes as served, compressed or not.
	Open(ctx context.Context, bucket string, obj Object) (io.ReadCloser, error)
}

// Report summarizes a run.
type Report struct {
	Source  string `json:"source"`
	Project string `json:"project"`
	Dataset string `json:"dataset"`
	// Objects counts the .jsonl and .jsonl.gz objects found, and Skipped
	// the other objects under the prefix. FailedObjects counts objects
	// with at least one failure.
	Objects       int `json:"objects"`
	Skipped       int `json:"skipped"`
	FailedObjects int `json:"failed_objects"`
	// Lines counts the non-blank lines read. MalformedLines counts those
	// that were not an OTLP JSON export request, and OtherSignalLines
	// those of a signal Config.Signal did not select.
	Lines            int64 `json:"lines"`
	MalformedLines   int64 `json:"malformed_lines"`
	OtherSignalLines int64 `json:"other_signal_lines"`
	// Signals has one entry per selected signal.
	Signals []SignalReport `json:"signals"`
	// Failures lists the first problems found, in the order found.
	// FailureCount counts all of them.
	Failures     []Failure `json:"failures"`
	FailureCount int       `json:"failure_count"`
	// Error is why the run stopped before reading every object, if it
	// did. Run returns the same error.
	Error string `json:"error,omitempty"`
}

// SignalReport counts one signal's records.
type SignalReport struct {
	Signal string `json:"signal"`
	Table  string `json:"table"`
	// Records counts the spans, log records, or metric data points read.
	Records int64 `json:"records"`
	// Appended counts the rows BigQuery acknowledged.
	Appended int64 `json:"appended"`
	// Rejected counts, by reason, records that were not appended and never
	// will be: the transform's reasons (structural limits, invalid IDs,
	// missing timestamps), "serialization" and "row_bytes" for rows that
	// could not be encoded or exceeded the 1 MiB row limit, and
	// "row_error" for rows the service refused. The exporter rejects the
	// same records.
	Rejected map[string]int64 `json:"rejected,omitempty"`
	// Unwritten counts rows that were built but neither appended nor
	// refused because their object failed or the run stopped.
	Unwritten int64 `json:"unwritten"`
}

// Failure is a problem confined to one object.
type Failure struct {
	Object string `json:"object"`
	// Line is the 1-based line number, or 0 when the problem is not tied to
	// one line.
	Line  int64  `json:"line,omitempty"`
	Error string `json:"error"`
}

// ObjectResult describes a finished object, for Config.OnObject.
type ObjectResult struct {
	// Object is the gs:// URL.
	Object   string
	Lines    int64
	Appended int64
	// Failures counts the object's failures, including Err.
	Failures int
	// Err is the problem that stopped the object early, if any.
	Err error
}

// Complete reports whether the run found nothing to fix: it read every
// object to the end, every line was an OTLP JSON export request, and every
// row that could be built was appended. Rejected records do not count
// against it, because the exporter rejects them too.
func (r *Report) Complete() bool {
	return r.Error == "" && r.FailureCount == 0
}

// WriteText writes a human-readable summary.
func (r *Report) WriteText(w io.Writer) error {
	var b strings.Builder
	fmt.Fprintf(&b, "backfill %s -> %s.%s\n", r.Source, r.Project, r.Dataset)
	fmt.Fprintf(&b, "objects  %d archives, %d other objects skipped, %d with failures\n",
		r.Objects, r.Skipped, r.FailedObjects)
	fmt.Fprintf(&b, "lines    %d read, %d malformed, %d of other signals\n",
		r.Lines, r.MalformedLines, r.OtherSignalLines)
	var appended int64
	for _, s := range r.Signals {
		appended += s.Appended
		var rejected int64
		reasons := make([]string, 0, len(s.Rejected))
		for reason, n := range s.Rejected {
			rejected += n
			reasons = append(reasons, fmt.Sprintf("%s %d", reason, n))
		}
		sort.Strings(reasons)
		detail := ""
		if len(reasons) > 0 {
			detail = " (" + strings.Join(reasons, ", ") + ")"
		}
		fmt.Fprintf(&b, "%-8s %s: %d %s, %d appended, %d rejected%s, %d unwritten\n",
			s.Signal, s.Table, s.Records, recordUnit(s.Signal), s.Appended, rejected, detail, s.Unwritten)
	}
	for _, f := range r.Failures {
		where := f.Object
		if f.Line > 0 {
			where += fmt.Sprintf(" line %d", f.Line)
		}
		fmt.Fprintf(&b, "FAIL     %s: %s\n", where, f.Error)
	}
	if more := r.FailureCount - len(r.Failures); more > 0 {
		fmt.Fprintf(&b, "FAIL     ... and %d more\n", more)
	}
	if r.Error != "" {
		fmt.Fprintf(&b, "STOPPED  %s\n", r.Error)
	}
	if r.Complete() {
		fmt.Fprintf(&b, "COMPLETE: %d rows appended\n", appended)
	} else {
		fmt.Fprintf(&b, "INCOMPLETE: %d rows appended, %d failures\n", appended, r.FailureCount)
	}
	_, err := io.WriteString(w, b.String())
	return err
}

// WriteJSON writes the report as indented JSON.
func (r *Report) WriteJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

// PermanentError is an append failure that retrying cannot fix, such as a
// missing table, a schema mismatch, or a missing permission. Every later
// append would fail the same way, so it stops the run.
type PermanentError struct {
	Table string
	// Label is the failure kind, from internal/bigquery's error matrix.
	Label string
	Err   error
}

func (e *PermanentError) Error() string {
	if e.Label == bqi.LabelPermission {
		return bqi.PermissionHint(e.Table) + ": " + e.Err.Error()
	}
	return fmt.Sprintf("append to %s failed permanently (%s): %v", e.Table, e.Label, e.Err)
}

func (e *PermanentError) Unwrap() error {
	return e.Err
}

// Run appends every archive object under cfg.Source and reports what
// happened.
//
// It returns an error, with the partial report, when the source cannot be
// listed, when an append fails permanently, or when ctx ends. Problems
// confined to one object (an unreadable object, a malformed line, an append
// that kept failing after MaxAttempts) are recorded in Report.Failures and
// the run goes on.
func Run(ctx context.Context, cfg Config) (*Report, error) {
	if err := cfg.normalize(); err != nil {
		return nil, err
	}
	bucket, prefix, _ := ParseSource(cfg.Source)
	rd, err := cfg.redactor()
	if err != nil {
		return nil, fmt.Errorf("redaction: %w", err)
	}

	store := cfg.Store
	if store == nil {
		gs, err := newGCSStore(ctx, cfg, cfg.StorageClientOptions...)
		if err != nil {
			return nil, err
		}
		defer gs.close()
		store = gs
	}
	sinks, closeSinks, err := openSinks(cfg)
	if err != nil {
		return nil, err
	}
	// Every append has been acknowledged or reported by the time the sinks
	// close, so a close error loses nothing.
	defer closeSinks()

	r := &run{
		cfg:    cfg,
		store:  store,
		bucket: bucket,
		sinks:  sinks,
		opts: transform.Options{
			Limits:            transform.DefaultLimits(),
			LogNameAttribute:  cfg.LogNameAttribute,
			InsertIDAttribute: cfg.InsertIDAttribute,
			MaxRowBytes:       maxRowBytes,
			Now:               cfg.Now,
			Redactor:          rd,
		},
		totals: map[string]*signalStats{},
		report: Report{
			Source:   cfg.Source,
			Project:  cfg.Project,
			Dataset:  cfg.Dataset,
			Failures: []Failure{},
		},
	}
	err = r.execute(ctx, prefix)
	rep := r.finish()
	if err != nil {
		rep.Error = err.Error()
	}
	return rep, err
}

// sink is one destination table.
type sink struct {
	table  string
	enc    *protoenc.Encoder
	writer *bqi.Writer
}

// openSinks opens one writer per selected signal on a shared client. The
// writers open their streams on first use.
func openSinks(cfg Config) (map[string]*sink, func(), error) {
	opts := cfg.WriteClientOptions
	closeCreds := func() {}
	if opts == nil {
		creds, cleanup, err := googlehttp.Credentials(credentials.DetectOptions{
			Scopes:           []string{bigqueryapi.BigqueryInsertdataScope},
			UseSelfSignedJWT: true,
		}, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("loading credentials for the Storage Write API: %w", err)
		}
		closeCreds = cleanup
		opts = []option.ClientOption{option.WithAuthCredentials(creds)}
	}
	// managedwriter keeps the constructor context for background work, so
	// it gets a lifetime of its own that outlasts a cancelled run until the
	// writers have closed.
	lifetimeCtx, cancel := context.WithCancel(context.Background())
	client, err := managedwriter.NewClient(lifetimeCtx, cfg.Project, opts...)
	if err != nil {
		cancel()
		closeCreds()
		return nil, nil, fmt.Errorf("creating the Storage Write API client: %w", err)
	}
	sinks := map[string]*sink{}
	closeAll := func() {
		for _, s := range sinks {
			_ = s.writer.Close()
		}
		_ = client.Close()
		cancel()
		closeCreds()
	}
	for _, signal := range cfg.signals() {
		ts, label := tableSchema(signal)
		enc, err := protoenc.New(ts)
		if err != nil {
			closeAll()
			return nil, nil, fmt.Errorf("building the %s row encoder: %w", signal, err)
		}
		sinks[signal] = &sink{
			table: cfg.table(signal),
			enc:   enc,
			writer: bqi.NewWriter(lifetimeCtx, client, bqi.WriterOptions{
				Project:             cfg.Project,
				Dataset:             cfg.Dataset,
				Table:               cfg.table(signal),
				Signal:              label,
				Descriptor:          enc.Descriptor(),
				MaxRequestBytes:     maxRequestBytes,
				MaxInflightRequests: maxInflightRequests,
				MaxInflightBytes:    maxInflightBytes,
				StreamPoolSize:      1,
				TraceID:             writeTraceID,
			}),
		}
	}
	return sinks, closeAll, nil
}

// run is the state of one Run call.
type run struct {
	cfg    Config
	store  ObjectStore
	bucket string
	sinks  map[string]*sink
	opts   transform.Options

	mu     sync.Mutex
	report Report
	totals map[string]*signalStats
	fatal  error
	cancel context.CancelFunc
}

func (r *run) execute(parent context.Context, prefix string) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	r.cancel = cancel

	jobs := make(chan Object)
	var wg sync.WaitGroup
	for range r.cfg.MaxWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for obj := range jobs {
				r.object(ctx, obj)
			}
		}()
	}
	listErr := r.store.List(ctx, r.bucket, prefix, func(obj Object) error {
		if !isArchive(obj.Name) {
			r.mu.Lock()
			r.report.Skipped++
			r.mu.Unlock()
			return nil
		}
		r.mu.Lock()
		r.report.Objects++
		r.mu.Unlock()
		select {
		case jobs <- obj:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	close(jobs)
	wg.Wait()

	r.mu.Lock()
	fatal := r.fatal
	r.mu.Unlock()
	switch {
	case fatal != nil:
		return fatal
	case parent.Err() != nil:
		return parent.Err()
	case listErr != nil:
		return fmt.Errorf("listing gs://%s/%s: %w", r.bucket, prefix, listErr)
	}
	return nil
}

// isArchive reports whether name is an object the run reads.
func isArchive(name string) bool {
	return strings.HasSuffix(name, ".jsonl") || strings.HasSuffix(name, ".jsonl.gz")
}

// abort records the error that stops the run and stops the other workers.
func (r *run) abort(err error) {
	r.mu.Lock()
	if r.fatal == nil {
		r.fatal = err
	}
	r.mu.Unlock()
	r.cancel()
}

// signalStats counts one signal, for one object or for the whole run.
type signalStats struct {
	records  int64
	encoded  int64
	appended int64
	rejected map[string]int64
}

func (s *signalStats) reject(reason string, n int64) {
	if s.rejected == nil {
		s.rejected = map[string]int64{}
	}
	s.rejected[reason] += n
}

// objectStats counts one object. Only its own worker touches it.
type objectStats struct {
	url          string
	lines        int64
	malformed    int64
	otherSignal  int64
	signals      map[string]*signalStats
	failures     []Failure
	failureCount int
}

func (st *objectStats) signal(name string) *signalStats {
	s, ok := st.signals[name]
	if !ok {
		s = &signalStats{}
		st.signals[name] = s
	}
	return s
}

func (st *objectStats) fail(line int64, err error) {
	st.failureCount++
	if len(st.failures) < maxListedFailures {
		st.failures = append(st.failures, Failure{Object: st.url, Line: line, Error: err.Error()})
	}
}

func (r *run) object(ctx context.Context, obj Object) {
	st := &objectStats{
		url:     "gs://" + r.bucket + "/" + obj.Name,
		signals: map[string]*signalStats{},
	}
	err := r.readObject(ctx, obj, st)
	var permanent *PermanentError
	switch {
	case err == nil:
	case errors.As(err, &permanent):
		// Run returns it; recording it once per object would only repeat it.
		r.abort(err)
	case ctx.Err() != nil:
		// The run is stopping, and Run reports why.
	default:
		st.fail(0, err)
	}
	r.merge(st, err)
}

func (r *run) merge(st *objectStats, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rep := &r.report
	rep.Lines += st.lines
	rep.MalformedLines += st.malformed
	rep.OtherSignalLines += st.otherSignal
	var appended int64
	for name, s := range st.signals {
		total, ok := r.totals[name]
		if !ok {
			total = &signalStats{}
			r.totals[name] = total
		}
		total.records += s.records
		total.encoded += s.encoded
		total.appended += s.appended
		for reason, n := range s.rejected {
			total.reject(reason, n)
		}
		appended += s.appended
	}
	if st.failureCount > 0 {
		rep.FailedObjects++
		rep.FailureCount += st.failureCount
		room := max(0, maxListedFailures-len(rep.Failures))
		rep.Failures = append(rep.Failures, st.failures[:min(room, len(st.failures))]...)
	}
	if r.cfg.OnObject != nil {
		r.cfg.OnObject(ObjectResult{
			Object:   st.url,
			Lines:    st.lines,
			Appended: appended,
			Failures: st.failureCount,
			Err:      err,
		})
	}
}

func (r *run) finish() *Report {
	r.mu.Lock()
	defer r.mu.Unlock()
	rep := r.report
	rep.Failures = slices.Clone(r.report.Failures)
	rep.Signals = make([]SignalReport, 0, len(r.sinks))
	for _, name := range allSignals {
		if _, ok := r.sinks[name]; !ok {
			continue
		}
		s := r.totals[name]
		if s == nil {
			s = &signalStats{}
		}
		sr := SignalReport{
			Signal:    name,
			Table:     r.cfg.table(name),
			Records:   s.records,
			Appended:  s.appended,
			Unwritten: s.encoded - s.appended - s.rejected[string(transform.ReasonRowError)],
		}
		if len(s.rejected) > 0 {
			sr.Rejected = map[string]int64{}
			for reason, n := range s.rejected {
				sr.Rejected[reason] = n
			}
		}
		rep.Signals = append(rep.Signals, sr)
	}
	return &rep
}

// readObject streams one object through the pipeline. It returns the error
// that stopped the object early, if any; line-level problems are recorded
// in st and do not stop it.
func (r *run) readObject(ctx context.Context, obj Object, st *objectStats) error {
	rc, err := r.store.Open(ctx, r.bucket, obj)
	if err != nil {
		return fmt.Errorf("opening the object: %w", err)
	}
	defer func() { _ = rc.Close() }()
	src, err := decompress(rc)
	if err != nil {
		return err
	}
	lines := &lineReader{r: bufio.NewReaderSize(src, 256<<10), max: r.cfg.MaxLineBytes}
	b := &batcher{run: r, st: st, rows: map[string][][]byte{}}
	for {
		line, err := lines.next()
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.Is(err, errLineTooLong) {
			st.lines++
			st.malformed++
			st.fail(lines.n, fmt.Errorf("line longer than %d bytes", r.cfg.MaxLineBytes))
			continue
		}
		if err != nil {
			return fmt.Errorf("reading line %d: %w", lines.n, err)
		}
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		st.lines++
		if err := r.line(ctx, st, b, lines.n, line); err != nil {
			return err
		}
	}
	return b.flushAll(ctx, lines.n)
}

// decompress sniffs the gzip magic number, so objects that Cloud Storage
// serves decompressed read correctly whatever their name says.
func decompress(r io.Reader) (io.Reader, error) {
	br := bufio.NewReaderSize(r, 64<<10)
	magic, err := br.Peek(2)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("reading the object: %w", err)
	}
	if len(magic) == 2 && magic[0] == 0x1f && magic[1] == 0x8b {
		zr, err := gzip.NewReader(br)
		if err != nil {
			return nil, fmt.Errorf("reading the gzip header: %w", err)
		}
		return zr, nil
	}
	return br, nil
}

// errLineTooLong reports a line over Config.MaxLineBytes.
var errLineTooLong = errors.New("line too long")

// lineReader splits a stream into lines without holding more than max bytes
// of one line.
type lineReader struct {
	r   *bufio.Reader
	max int
	buf []byte
	// n is the 1-based number of the line last returned.
	n int64
}

func (l *lineReader) next() ([]byte, error) {
	l.buf = l.buf[:0]
	tooLong := false
	for {
		frag, err := l.r.ReadSlice('\n')
		if !tooLong {
			if len(l.buf)+len(bytes.TrimSuffix(frag, []byte("\n"))) > l.max {
				tooLong = true
				l.buf = l.buf[:0]
			} else {
				l.buf = append(l.buf, frag...)
			}
		}
		switch {
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF):
			if !tooLong && len(l.buf) == 0 {
				return nil, io.EOF
			}
		case err != nil:
			l.n++
			return nil, err
		}
		l.n++
		if tooLong {
			return nil, errLineTooLong
		}
		return l.buf, nil
	}
}

// errNotExportRequest reports valid JSON that carries no telemetry.
var errNotExportRequest = errors.New("not an OTLP JSON export request: no resourceSpans, resourceLogs, or resourceMetrics")

// detectSignal names the signal of one export request from its first
// recognized top-level key. In the usual line that key comes first, so the
// rest of the line is not scanned.
func detectSignal(line []byte) (string, error) {
	dec := json.NewDecoder(bytes.NewReader(line))
	tok, err := dec.Token()
	if err != nil {
		return "", fmt.Errorf("not valid JSON: %w", err)
	}
	if tok != json.Delim('{') {
		return "", errors.New("not a JSON object")
	}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return "", fmt.Errorf("not valid JSON: %w", err)
		}
		switch key, _ := tok.(string); key {
		case "resourceSpans", "resource_spans":
			return SignalTraces, nil
		case "resourceLogs", "resource_logs":
			return SignalLogs, nil
		case "resourceMetrics", "resource_metrics":
			return SignalMetrics, nil
		}
		var skip json.RawMessage
		if err := dec.Decode(&skip); err != nil {
			return "", fmt.Errorf("not valid JSON: %w", err)
		}
	}
	return "", errNotExportRequest
}

// line converts one export request and queues its rows. It returns only
// errors that stop the object.
func (r *run) line(ctx context.Context, st *objectStats, b *batcher, n int64, line []byte) error {
	signal, err := detectSignal(line)
	if err != nil {
		st.malformed++
		st.fail(n, err)
		return nil
	}
	if _, ok := r.sinks[signal]; !ok {
		st.otherSignal++
		return nil
	}
	s := st.signal(signal)
	var (
		rows    []transform.Row
		rejects []transform.Rejection
	)
	switch signal {
	case SignalTraces:
		td, uerr := (&ptrace.JSONUnmarshaler{}).UnmarshalTraces(line)
		if uerr != nil {
			err = fmt.Errorf("decoding OTLP JSON traces: %w", uerr)
			break
		}
		s.records += int64(td.SpanCount())
		rows, rejects = transform.SpanRows(td, r.opts)
	case SignalLogs:
		ld, uerr := (&plog.JSONUnmarshaler{}).UnmarshalLogs(line)
		if uerr != nil {
			err = fmt.Errorf("decoding OTLP JSON logs: %w", uerr)
			break
		}
		s.records += int64(ld.LogRecordCount())
		rows, rejects = transform.LogRows(ld, r.opts)
	default:
		md, uerr := (&pmetric.JSONUnmarshaler{}).UnmarshalMetrics(line)
		if uerr != nil {
			err = fmt.Errorf("decoding OTLP JSON metrics: %w", uerr)
			break
		}
		s.records += int64(md.DataPointCount())
		rows, rejects = transform.MetricRows(md, r.opts)
	}
	if err != nil {
		st.malformed++
		st.fail(n, err)
		return nil
	}
	for _, rej := range rejects {
		s.reject(string(rej.Reason), 1)
	}
	return b.add(ctx, signal, rows, n)
}

// batcher holds one object's encoded rows until a batch fills.
type batcher struct {
	run  *run
	st   *objectStats
	rows map[string][][]byte
}

func (b *batcher) add(ctx context.Context, signal string, rows []transform.Row, line int64) error {
	sk := b.run.sinks[signal]
	s := b.st.signal(signal)
	for _, row := range rows {
		enc, err := sk.enc.Encode(row)
		if err != nil {
			s.reject(string(transform.ReasonSerialization), 1)
			continue
		}
		if len(enc) > maxRowBytes {
			s.reject(string(transform.ReasonRowBytes), 1)
			continue
		}
		s.encoded++
		b.rows[signal] = append(b.rows[signal], enc)
		if len(b.rows[signal]) >= b.run.cfg.BatchSize {
			if err := b.flush(ctx, signal, line); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *batcher) flush(ctx context.Context, signal string, line int64) error {
	rows := b.rows[signal]
	if len(rows) == 0 {
		return nil
	}
	b.rows[signal] = nil
	sk := b.run.sinks[signal]
	acked, refused, err := b.run.appendRows(ctx, sk, rows)
	s := b.st.signal(signal)
	s.appended += int64(acked)
	if refused > 0 {
		s.reject(string(transform.ReasonRowError), int64(refused))
	}
	if err != nil {
		var permanent *PermanentError
		if errors.As(err, &permanent) {
			return err
		}
		return fmt.Errorf("appending %s rows read by line %d to %s: %w", signal, line, sk.table, err)
	}
	return nil
}

func (b *batcher) flushAll(ctx context.Context, line int64) error {
	for _, signal := range allSignals {
		if err := b.flush(ctx, signal, line); err != nil {
			return err
		}
	}
	return nil
}

// appendRows appends rows and returns how many the service acknowledged and
// how many it refused row by row.
//
// A request refused for some of its rows appended none of them, so the
// valid rest is sent again at once. Transient failures are retried with
// backoff up to MaxAttempts. A failure that tells the writer to rebuild its
// stream gets one more attempt on a new stream; a permanent failure returns
// a *PermanentError.
func (r *run) appendRows(ctx context.Context, sk *sink, rows [][]byte) (acked, refused int, err error) {
	pending := rows
	backoff := r.cfg.RetryBackoff
	attempts := 0
	rebuilt := false
	for {
		out := sk.writer.Append(ctx, pending)
		invalid := make(map[int]bool, len(out.RowErrors))
		for _, re := range out.RowErrors {
			if i := int(re.Index); i >= 0 && i < len(pending) {
				invalid[i] = true
			}
		}
		refused += len(invalid)
		var rest [][]byte
		for i, row := range pending {
			switch {
			case invalid[i]:
			case out.IsAcked(i):
				acked++
			default:
				rest = append(rest, row)
			}
		}
		pending = rest
		if v := out.Verdict; out.Err != nil && v.Class == bqi.Permanent {
			if v.StreamRecreate && !rebuilt && len(pending) > 0 {
				rebuilt = true
				continue
			}
			return acked, refused, &PermanentError{Table: sk.table, Label: v.Label, Err: out.Err}
		}
		if len(pending) == 0 {
			return acked, refused, nil
		}
		if out.Err == nil && len(invalid) > 0 {
			continue
		}
		if cerr := ctx.Err(); cerr != nil {
			return acked, refused, cerr
		}
		cause := out.Err
		if cause == nil {
			cause = errors.New("the service acknowledged only part of the batch")
		}
		attempts++
		if attempts >= r.cfg.MaxAttempts {
			return acked, refused, fmt.Errorf("giving up after %d attempts: %w", attempts, cause)
		}
		if err := sleep(ctx, backoff); err != nil {
			return acked, refused, err
		}
		backoff = min(2*backoff, r.cfg.RetryMaxBackoff)
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// gcsStore is an ObjectStore over the Cloud Storage JSON API.
type gcsStore struct {
	client     *http.Client
	baseURL    string
	cleanup    func()
	attempts   int
	backoff    time.Duration
	maxBackoff time.Duration
}

func newGCSStore(ctx context.Context, cfg Config, opts ...option.ClientOption) (*gcsStore, error) {
	client, endpoint, cleanup, err := googlehttp.NewClient(ctx, []string{gcsReadOnlyScope}, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating the Cloud Storage client: %w", err)
	}
	base := strings.TrimRight(endpoint, "/")
	switch {
	case base == "":
		base = defaultGCSEndpoint
	case !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://"):
		base = "https://" + base
	}
	return &gcsStore{
		client:     client,
		baseURL:    base,
		cleanup:    cleanup,
		attempts:   cfg.MaxAttempts,
		backoff:    cfg.RetryBackoff,
		maxBackoff: cfg.RetryMaxBackoff,
	}, nil
}

func (s *gcsStore) close() {
	s.cleanup()
}

func (s *gcsStore) List(ctx context.Context, bucket, prefix string, fn func(Object) error) error {
	token := ""
	for {
		q := url.Values{}
		q.Set("fields", "items(name,generation,size),nextPageToken")
		if prefix != "" {
			q.Set("prefix", prefix)
		}
		if token != "" {
			q.Set("pageToken", token)
		}
		resp, err := s.get(ctx, fmt.Sprintf("%s/storage/v1/b/%s/o?%s", s.baseURL, url.PathEscape(bucket), q.Encode()))
		if err != nil {
			return err
		}
		var page struct {
			Items []struct {
				Name       string `json:"name"`
				Generation string `json:"generation"`
				Size       string `json:"size"`
			} `json:"items"`
			NextPageToken string `json:"nextPageToken"`
		}
		err = json.NewDecoder(resp.Body).Decode(&page)
		_ = resp.Body.Close()
		if err != nil {
			return fmt.Errorf("decoding the object list: %w", err)
		}
		for _, it := range page.Items {
			size, _ := strconv.ParseInt(it.Size, 10, 64)
			if err := fn(Object{Name: it.Name, Generation: it.Generation, Size: size}); err != nil {
				return err
			}
		}
		if page.NextPageToken == "" {
			return nil
		}
		token = page.NextPageToken
	}
}

func (s *gcsStore) Open(ctx context.Context, bucket string, obj Object) (io.ReadCloser, error) {
	u := fmt.Sprintf("%s/storage/v1/b/%s/o/%s?alt=media", s.baseURL, url.PathEscape(bucket), url.PathEscape(obj.Name))
	if obj.Generation != "" {
		u += "&generation=" + url.QueryEscape(obj.Generation)
	}
	resp, err := s.get(ctx, u)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// get sends a GET and retries throttling, server errors, and transport
// failures.
func (s *gcsStore) get(ctx context.Context, u string) (*http.Response, error) {
	backoff := s.backoff
	for attempt := 1; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, err
		}
		resp, err := s.client.Do(req)
		switch {
		case err != nil:
			if cerr := ctx.Err(); cerr != nil {
				return nil, cerr
			}
		case resp.StatusCode >= 200 && resp.StatusCode < 300:
			return resp, nil
		default:
			err = statusError(resp)
			if !retryableStatus(resp.StatusCode) {
				return nil, err
			}
		}
		if attempt >= s.attempts {
			return nil, err
		}
		if serr := sleep(ctx, backoff); serr != nil {
			return nil, serr
		}
		backoff = min(2*backoff, s.maxBackoff)
	}
}

func retryableStatus(code int) bool {
	switch code {
	case http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusInternalServerError,
		http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	return false
}

// statusError reads and closes an error response, keeping the service's
// message when it sends one.
func statusError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	_ = resp.Body.Close()
	var e struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &e) == nil && e.Error.Message != "" {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, e.Error.Message)
	}
	return fmt.Errorf("HTTP %d", resp.StatusCode)
}
