// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package quarantinereplay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	bigqueryapi "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/googlehttp"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
)

const (
	// DefaultQuarantineTable is the table the exporter quarantines rows in.
	DefaultQuarantineTable = "otel_quarantine"

	defaultQueryPageSize = 500
	// queryWaitMillis is how long one jobs.query or jobs.getQueryResults
	// call waits for the query to finish before it returns.
	queryWaitMillis = 10_000
	// bigQueryTimestampLayout formats TIMESTAMP query parameter values.
	bigQueryTimestampLayout = "2006-01-02 15:04:05.999999-07:00"
)

var (
	// projectIDPattern accepts project IDs, including domain-scoped ones
	// such as "example.com:my-project".
	projectIDPattern = regexp.MustCompile(`^([a-z0-9.-]+:)?[a-z0-9][a-z0-9-]*$`)
	datasetIDPattern = regexp.MustCompile(`^[A-Za-z0-9_]+$`)
	// tableIDPattern is BigQuery's table name character set: letters,
	// marks, numbers, connectors (including "_"), dashes, and spaces.
	tableIDPattern = regexp.MustCompile(`^[\p{L}\p{M}\p{N}\p{Pc}\p{Pd}\p{Zs}]+$`)
)

// quarantineQueryTemplate selects the quarantine rows a replay reads,
// ordered by (ingest_time, row_fp). row_fp fingerprints the row's content,
// so rows that share an ingest_time (every row of one quarantined batch
// does) still sort in a stable order and a checkpoint can name the last one
// it handled. The verbs are the backquoted table, the partition-pruning
// WHERE conditions, and the optional resume condition.
const quarantineQueryTemplate = "WITH quarantined AS (\n" +
	"  SELECT\n" +
	"    ingest_time,\n" +
	"    signal,\n" +
	"    error_code,\n" +
	"    error_message,\n" +
	"    service_name,\n" +
	"    trace_id,\n" +
	"    span_id,\n" +
	"    raw_payload,\n" +
	"    payload_ref.uri AS payload_ref_uri,\n" +
	"    payload_ref.version AS payload_ref_version,\n" +
	"    FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(\n" +
	"      signal, error_code, error_message, service_name, trace_id, span_id, raw_payload,\n" +
	"      payload_ref.uri AS payload_ref_uri, payload_ref.version AS payload_ref_version))) AS row_fp\n" +
	"  FROM `%s`\n" +
	"  WHERE %s\n" +
	")\n" +
	"SELECT\n" +
	"  UNIX_MICROS(ingest_time) AS ingest_time_us,\n" +
	"  signal, error_code, error_message, service_name, trace_id, span_id, raw_payload,\n" +
	"  payload_ref_uri, payload_ref_version, row_fp\n" +
	"FROM quarantined%s\n" +
	"ORDER BY ingest_time, row_fp"

// BigQuerySource names the quarantine table that ReplayBigQuery reads.
type BigQuerySource struct {
	// Project holds the dataset and runs the query job, so the caller needs
	// bigquery.jobs.create on it.
	Project string
	// Dataset holds the quarantine table.
	Dataset string
	// Table is the quarantine table. Empty means DefaultQuarantineTable.
	Table string
	// PageSize bounds the rows fetched per results page. Zero means 500.
	PageSize int64
}

func (s BigQuerySource) table() string {
	if s.Table == "" {
		return DefaultQuarantineTable
	}
	return s.Table
}

// ID returns the table's fully qualified project.dataset.table name.
func (s BigQuerySource) ID() string {
	return s.Project + "." + s.Dataset + "." + s.table()
}

// Validate returns an error unless s names a well-formed project, dataset,
// and table and has a non-negative page size. The table name is quoted into
// the query, so its character set is checked strictly.
func (s BigQuerySource) Validate() error {
	if !projectIDPattern.MatchString(s.Project) {
		return fmt.Errorf("invalid BigQuery project ID %q", s.Project)
	}
	if !datasetIDPattern.MatchString(s.Dataset) {
		return fmt.Errorf("invalid BigQuery dataset ID %q", s.Dataset)
	}
	if !tableIDPattern.MatchString(s.table()) {
		return fmt.Errorf("invalid BigQuery table ID %q", s.table())
	}
	if s.PageSize < 0 {
		return fmt.Errorf("page size must not be negative, got %d", s.PageSize)
	}
	return nil
}

// ReplayBigQuery reads quarantine rows straight from src's table and replays
// them as Replay does. It runs one parameterized GoogleSQL query whose WHERE
// clause compares ingest_time with the run's time window, so BigQuery prunes
// the table's daily ingest_time partitions (and a table that requires a
// partition filter accepts the query). Without opts.Filter.Since the lower
// bound is the Unix epoch: quarantine partitions expire, so this reads every
// partition that is left. The signal and error_code filters are pushed into
// the query too, and opts.Filter still runs on every row returned.
//
// Rows are read in (ingest_time, content fingerprint) order. With
// opts.Checkpoint set, a resumed run passes the last handled row's key to
// the query, which raises the partition bound and skips the handled rows on
// the server; they are therefore not counted in SkippedCheckpointed. Rows
// with identical content and ingest_time share a key, so a resumed run
// replays only one of a set of such duplicates if it stopped between them.
//
// The query runs in src.Project over the BigQuery REST API, using an HTTP
// client from internal/googlehttp built with clientOpts (Application
// Default Credentials unless clientOpts carry their own). Reading
// raw_payload needs bigquery.tables.getData on the table and, when the
// column carries a policy tag, Fine-Grained Reader.
func ReplayBigQuery(ctx context.Context, src BigQuerySource, sink Sink, opts Options, clientOpts ...option.ClientOption) (Summary, error) {
	if sink == nil {
		return Summary{}, errors.New("sink must not be nil")
	}
	if err := src.Validate(); err != nil {
		return Summary{}, err
	}
	if opts.Checkpoint != nil && opts.Checkpoint.Source == "" {
		cfg := *opts.Checkpoint
		cfg.Source = "bigquery:" + src.ID()
		opts.Checkpoint = &cfg
	}
	rp, err := newReplayer(ctx, sink, opts, checkpointKindBigQuery)
	if err != nil {
		return Summary{}, err
	}
	var cursor *bigQueryCursor
	if rp.cp != nil {
		cursor = rp.cp.state.Cursor
	}
	query, params := quarantineQuery(src.ID(), opts.Filter, cursor)

	hc, endpoint, cleanup, err := googlehttp.NewClient(ctx, []string{bigqueryapi.BigqueryScope}, clientOpts...)
	if err != nil {
		return Summary{}, fmt.Errorf("creating BigQuery HTTP client: %w", err)
	}
	defer cleanup()
	svcOpts := []option.ClientOption{option.WithHTTPClient(hc)}
	if endpoint != "" {
		svcOpts = append(svcOpts, option.WithEndpoint(endpoint))
	}
	svc, err := bigqueryapi.NewService(ctx, svcOpts...)
	if err != nil {
		return Summary{}, fmt.Errorf("creating BigQuery client: %w", err)
	}

	pageSize := src.PageSize
	if pageSize == 0 {
		pageSize = defaultQueryPageSize
	}
	useLegacySQL := false
	rows := &queryRows{
		svc:      svc,
		project:  src.Project,
		table:    src.ID(),
		pageSize: pageSize,
		req: &bigqueryapi.QueryRequest{
			Query:           query,
			UseLegacySql:    &useLegacySQL,
			ParameterMode:   "NAMED",
			QueryParameters: params,
			MaxResults:      pageSize,
			TimeoutMs:       queryWaitMillis,
		},
	}
	for i := 0; ; i++ {
		if err := ctx.Err(); err != nil {
			return rp.sum, err
		}
		row, err := rows.next(ctx)
		if errors.Is(err, io.EOF) {
			return rp.sum, errors.Join(rp.errs...)
		}
		if err != nil {
			return rp.sum, rp.stop(err)
		}
		if err := rp.process(i, row.pos, row.rec); err != nil {
			return rp.sum, rp.stop(err)
		}
	}
}

// quarantineQuery builds the query ReplayBigQuery runs and its parameters.
func quarantineQuery(table string, f Filter, cursor *bigQueryCursor) (string, []*bigqueryapi.QueryParameter) {
	since := f.Since
	if since.IsZero() {
		since = time.Unix(0, 0)
	}
	if cursor != nil {
		if c := time.UnixMicro(cursor.IngestTimeMicros); c.After(since) {
			since = c
		}
	}
	where := []string{"ingest_time >= @since"}
	params := []*bigqueryapi.QueryParameter{timestampParam("since", since)}
	if !f.Until.IsZero() {
		where = append(where, "ingest_time <= @until")
		params = append(params, timestampParam("until", f.Until))
	}
	if signals := signalPushdown(f.Signal); len(signals) > 0 {
		where = append(where, "signal IN UNNEST(@signals)")
		params = append(params, stringArrayParam("signals", signals))
	}
	if codes := errorCodePushdown(f.ErrorCodes); len(codes) > 0 {
		where = append(where, "error_code IN UNNEST(@error_codes)")
		params = append(params, stringArrayParam("error_codes", codes))
	}
	resume := ""
	if cursor != nil {
		resume = "\nWHERE ingest_time > @cursor_time OR (ingest_time = @cursor_time AND row_fp > @cursor_fp)"
		params = append(params,
			timestampParam("cursor_time", time.UnixMicro(cursor.IngestTimeMicros)),
			int64Param("cursor_fp", cursor.RowFingerprint))
	}
	return fmt.Sprintf(quarantineQueryTemplate, table, strings.Join(where, "\n    AND "), resume), params
}

// signalPushdown lists the stored signal values that can pass the signal
// filter: each requested signal's canonical name and aliases, and each token
// as given and lowercased. The filter itself still runs on every row
// returned, so the list only needs to cover the values rows can hold; the
// exporter always writes canonical names. Comparing the clustering column
// directly, rather than through LOWER or TRIM, keeps block pruning.
func signalPushdown(raw string) []string {
	var out []string
	add := func(values ...string) {
		for _, v := range values {
			if v != "" && !slices.Contains(out, v) {
				out = append(out, v)
			}
		}
	}
	for _, part := range strings.Split(raw, ",") {
		token := strings.TrimSpace(part)
		if token == "" {
			continue
		}
		add(token, strings.ToLower(token))
		switch CanonicalSignal(token) {
		case schema.SignalSpans:
			add(schema.SignalSpans, "traces", "otel_spans")
		case schema.SignalLogs:
			add(schema.SignalLogs, "otel_logs")
		case schema.SignalMetrics:
			add(schema.SignalMetrics, "otel_metrics")
		}
	}
	return out
}

// errorCodePushdown lists the stored error_code values that can pass the
// error code filter: each code trimmed, as given, uppercased, and
// lowercased. The filter matches case-insensitively and still runs on every
// row returned; the exporter always writes uppercase codes.
func errorCodePushdown(codes []string) []string {
	var out []string
	for _, code := range codes {
		code = strings.TrimSpace(code)
		for _, v := range []string{code, strings.ToUpper(code), strings.ToLower(code)} {
			if !slices.Contains(out, v) {
				out = append(out, v)
			}
		}
	}
	return out
}

func timestampParam(name string, t time.Time) *bigqueryapi.QueryParameter {
	return &bigqueryapi.QueryParameter{
		Name:           name,
		ParameterType:  &bigqueryapi.QueryParameterType{Type: "TIMESTAMP"},
		ParameterValue: &bigqueryapi.QueryParameterValue{Value: t.UTC().Format(bigQueryTimestampLayout)},
	}
}

func int64Param(name string, v int64) *bigqueryapi.QueryParameter {
	return &bigqueryapi.QueryParameter{
		Name:           name,
		ParameterType:  &bigqueryapi.QueryParameterType{Type: "INT64"},
		ParameterValue: &bigqueryapi.QueryParameterValue{Value: strconv.FormatInt(v, 10)},
	}
}

func stringArrayParam(name string, values []string) *bigqueryapi.QueryParameter {
	vals := make([]*bigqueryapi.QueryParameterValue, len(values))
	for i, v := range values {
		// ForceSendFields keeps an empty string, which omitempty would drop.
		vals[i] = &bigqueryapi.QueryParameterValue{Value: v, ForceSendFields: []string{"Value"}}
	}
	return &bigqueryapi.QueryParameter{
		Name: name,
		ParameterType: &bigqueryapi.QueryParameterType{
			Type:      "ARRAY",
			ArrayType: &bigqueryapi.QueryParameterType{Type: "STRING"},
		},
		ParameterValue: &bigqueryapi.QueryParameterValue{ArrayValues: vals},
	}
}

// quarantineRow is one decoded result row.
type quarantineRow struct {
	rec Record
	pos recordPosition
}

// queryRows runs the replay query and pages through its results.
type queryRows struct {
	svc      *bigqueryapi.Service
	project  string
	table    string
	pageSize int64
	req      *bigqueryapi.QueryRequest

	started bool
	job     *bigqueryapi.JobReference
	fields  []*bigqueryapi.TableFieldSchema
	page    []*bigqueryapi.TableRow
	token   string
}

// next returns the next result row, or io.EOF after the last one.
func (q *queryRows) next(ctx context.Context) (quarantineRow, error) {
	for len(q.page) == 0 {
		if q.started && q.token == "" {
			return quarantineRow{}, io.EOF
		}
		if err := q.fetch(ctx); err != nil {
			return quarantineRow{}, err
		}
	}
	row := q.page[0]
	q.page = q.page[1:]
	return decodeQuarantineRow(q.fields, row)
}

// fetch starts the query, or fetches the page after the current one.
func (q *queryRows) fetch(ctx context.Context) error {
	if !q.started {
		resp, err := q.svc.Jobs.Query(q.project, q.req).Context(ctx).Do()
		if err != nil {
			return queryError(q.project, q.table, err)
		}
		q.started = true
		q.job = resp.JobReference
		if resp.JobComplete {
			q.setPage(resp.Schema, resp.Rows, resp.PageToken)
			return nil
		}
	}
	return q.results(ctx, q.token)
}

// results waits for the query job to finish and fetches the results page
// that token names (the first page when token is empty).
func (q *queryRows) results(ctx context.Context, token string) error {
	if q.job == nil || q.job.JobId == "" {
		return fmt.Errorf("querying %s: BigQuery returned no job to fetch results from", q.table)
	}
	project := q.job.ProjectId
	if project == "" {
		project = q.project
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		call := q.svc.Jobs.GetQueryResults(project, q.job.JobId).
			Context(ctx).
			MaxResults(q.pageSize).
			TimeoutMs(queryWaitMillis)
		if q.job.Location != "" {
			call = call.Location(q.job.Location)
		}
		if token != "" {
			call = call.PageToken(token)
		}
		resp, err := call.Do()
		if err != nil {
			return queryError(q.project, q.table, err)
		}
		if resp.JobComplete {
			q.setPage(resp.Schema, resp.Rows, resp.PageToken)
			return nil
		}
	}
}

func (q *queryRows) setPage(ts *bigqueryapi.TableSchema, rows []*bigqueryapi.TableRow, token string) {
	if ts != nil {
		q.fields = ts.Fields
	}
	q.page = rows
	q.token = token
}

// queryError adds the permissions a replay query needs to a 403.
func queryError(project, table string, err error) error {
	var gerr *googleapi.Error
	if errors.As(err, &gerr) && gerr.Code == http.StatusForbidden {
		return fmt.Errorf("querying %s: %w (the caller needs bigquery.jobs.create on project %s, for example "+
			"through roles/bigquery.jobUser, and bigquery.tables.getData on the table, for example through "+
			"roles/bigquery.dataViewer; reading a policy-tagged raw_payload also needs Fine-Grained Reader)",
			table, err, project)
	}
	return fmt.Errorf("querying %s: %w", table, err)
}

// decodeQuarantineRow converts one REST result row into a Record and its
// sort key.
func decodeQuarantineRow(fields []*bigqueryapi.TableFieldSchema, row *bigqueryapi.TableRow) (quarantineRow, error) {
	if row == nil || len(row.F) != len(fields) {
		cells := 0
		if row != nil {
			cells = len(row.F)
		}
		return quarantineRow{}, fmt.Errorf("BigQuery returned a row with %d cells for %d columns", cells, len(fields))
	}
	var (
		out               quarantineRow
		haveTime, haveKey bool
		uri, version      string
	)
	for i, field := range fields {
		if field == nil {
			return quarantineRow{}, errors.New("BigQuery returned a result schema with an empty column")
		}
		var value string
		switch v := row.F[i].V.(type) {
		case nil:
		case string:
			value = v
		default:
			return quarantineRow{}, fmt.Errorf("column %s holds an unexpected %T value", field.Name, v)
		}
		switch field.Name {
		case "ingest_time_us":
			us, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return quarantineRow{}, fmt.Errorf("column ingest_time_us: %w", err)
			}
			out.rec.IngestTime = time.UnixMicro(us).UTC().Format(time.RFC3339Nano)
			out.pos.ingestMicros = us
			haveTime = true
		case "row_fp":
			fp, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return quarantineRow{}, fmt.Errorf("column row_fp: %w", err)
			}
			out.pos.fingerprint = fp
			haveKey = true
		case "signal":
			out.rec.Signal = value
		case "error_code":
			out.rec.ErrorCode = value
		case "error_message":
			out.rec.ErrorMessage = value
		case "service_name":
			out.rec.ServiceName = value
		case "trace_id":
			out.rec.TraceID = value
		case "span_id":
			out.rec.SpanID = value
		case "raw_payload":
			out.rec.RawPayload = value
		case "payload_ref_uri":
			uri = value
		case "payload_ref_version":
			version = value
		}
	}
	if !haveTime || !haveKey {
		return quarantineRow{}, errors.New("BigQuery result rows lack the ingest_time_us or row_fp column")
	}
	if uri != "" {
		out.rec.PayloadRef = &ObjectRef{URI: uri, Version: version}
	}
	return out, nil
}
