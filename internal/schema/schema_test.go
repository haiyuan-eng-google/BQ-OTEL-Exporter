// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"google.golang.org/protobuf/proto"
)

// The contract stays in alpha, now at its first documented correction, until
// at least two independent design partners complete representative
// workflows. Bumping this to v1 is a milestone gate, not a code change, so
// make it fail loudly.
func TestVersionIsAlphaBaseline(t *testing.T) {
	if Version != "v0alpha2" {
		t.Fatalf("schema Version = %q; v1 freezes only at M4", Version)
	}
}

// Every column the table contract marks REQUIRED must be NOT NULL in the DDL.
func TestRequiredColumns(t *testing.T) {
	spans := []string{
		"start_time", "start_time_unix_nano", "end_time", "end_time_unix_nano",
		"receive_time", "receive_time_unix_nano", "duration_nano",
		"trace_id", "span_id", "name", "kind", "schema_version",
	}
	logs := []string{
		"timestamp", "timestamp_source", "time_unix_nano", "observed_time_unix_nano",
		"receive_time", "receive_time_unix_nano",
		"log_name", "insert_id", "schema_version",
	}
	metrics := []string{
		"timestamp", "time_unix_nano", "receive_time", "receive_time_unix_nano",
		"metric_name", "metric_type", "point_fingerprint", "schema_version",
	}
	quarantine := []string{
		"ingest_time", "signal", "error_code", "error_message", "schema_version",
	}

	for _, tc := range []struct {
		name string
		ddl  string
		cols []string
	}{
		{"spans", SpansDDL, spans},
		{"logs", LogsDDL, logs},
		{"metrics", MetricsDDL, metrics},
		{"quarantine", QuarantineDDL, quarantine},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, col := range tc.cols {
				if !columnIsNotNull(tc.ddl, col) {
					t.Errorf("%s.%s must be declared NOT NULL", tc.name, col)
				}
			}
		})
	}
}

func columnIsNotNull(ddl, col string) bool {
	for _, line := range strings.Split(ddl, "\n") {
		f := strings.Fields(line)
		if len(f) > 0 && f[0] == col {
			return strings.Contains(line, "NOT NULL")
		}
	}
	return false
}

// Partitioning is what makes ordinary queries affordable on these tables.
// The quarantine table is a triage queue, so its partitions also expire.
func TestPartitioning(t *testing.T) {
	if !strings.Contains(SpansDDL, "PARTITION BY TIMESTAMP_TRUNC(start_time, HOUR)") {
		t.Error("spans must be hour-partitioned on start_time")
	}
	if !strings.Contains(LogsDDL, "PARTITION BY DATE(timestamp)") {
		t.Error("logs must be day-partitioned on timestamp")
	}
	if !strings.Contains(MetricsDDL, "PARTITION BY DATE(timestamp)") {
		t.Error("metrics must be day-partitioned on timestamp")
	}
	if !strings.Contains(QuarantineDDL, "PARTITION BY DATE(ingest_time)") {
		t.Error("quarantine must be day-partitioned on ingest_time")
	}
	if !strings.Contains(QuarantineDDL, "OPTIONS (\n  partition_expiration_days = 30\n);") {
		t.Error("quarantine partitions must expire after 30 days")
	}
	for name, ddl := range map[string]string{"spans": SpansDDL, "logs": LogsDDL, "metrics": MetricsDDL} {
		if strings.Contains(ddl, "partition_expiration_days") {
			t.Errorf("%s must not expire partitions; retention is the operator's call", name)
		}
	}
}

// Spans cluster on (service_name, trace_id, name), so per-service scans and
// whole-trace lookups both prune. Logs cluster on (service_name, trace_id,
// severity_number), so per-service scans, trace correlation and severity
// filters prune. Metrics cluster on (service_name, metric_name, metric_type).
// Quarantine clusters on (signal, error_code, service_name), so "what is
// failing, and where" queries prune.
func TestClustering(t *testing.T) {
	if !strings.Contains(SpansDDL, "CLUSTER BY service_name, trace_id, name;") {
		t.Error("spans must cluster on service_name, trace_id, name")
	}
	if !strings.Contains(LogsDDL, "CLUSTER BY service_name, trace_id, severity_number;") {
		t.Error("logs must cluster on service_name, trace_id, severity_number")
	}
	if !strings.Contains(MetricsDDL, "CLUSTER BY service_name, metric_name, metric_type;") {
		t.Error("metrics must cluster on service_name, metric_name, metric_type")
	}
	if !strings.Contains(QuarantineDDL, "\nCLUSTER BY signal, error_code, service_name\nOPTIONS (") {
		t.Error("quarantine must cluster on signal, error_code, service_name")
	}
}

// Span dedup must order on the raw nanos, not the microsecond TIMESTAMP:
// end_time alone can tie across retries.
func TestSpanDedupOrdersOnRawNanos(t *testing.T) {
	if !strings.Contains(SpansDedupViewDDL, "end_time_unix_nano DESC") {
		t.Error("span dedup must order on end_time_unix_nano, not end_time")
	}
	if !strings.Contains(SpansDedupViewDDL, "receive_time_unix_nano DESC") {
		t.Error("span dedup needs the ingestion-time tie-breaker")
	}
}

// The span dedup window must partition on the full span identity, led by the
// partitioning column and followed by every clustering and search-index
// column, so a filter on any of them still prunes when pushed below the window
// function.
func TestSpanDedupWindow(t *testing.T) {
	query := strings.Join(strings.Fields(SpansDedupViewQuery), " ")
	want := "QUALIFY ROW_NUMBER() OVER ( " +
		"PARTITION BY start_time, service_name, trace_id, span_id, name " +
		"ORDER BY end_time_unix_nano DESC, receive_time_unix_nano DESC ) = 1"
	if !strings.Contains(query, want) {
		t.Errorf("span dedup window = %q, want it to contain %q", query, want)
	}
}

// The log dedup window must partition on the full record identity as
// separate columns, led by the partitioning column and followed by every
// clustering and search-index column, so a filter on any of them still prunes
// when pushed below the window function. It must never flatten the identity
// into one string: CONCAT(log_name, '/', id) would collide ('a/b', 'c') with
// ('a', 'b/c').
func TestLogDedupWindow(t *testing.T) {
	if strings.Contains(LogsDedupViewDDL, "CONCAT(") {
		t.Fatal("log dedup must not concatenate identity columns into one partition key")
	}
	query := strings.Join(strings.Fields(LogsDedupViewQuery), " ")
	want := "QUALIFY ROW_NUMBER() OVER ( " +
		"PARTITION BY timestamp, time_unix_nano, service_name, trace_id, span_id, severity_number, log_name, insert_id " +
		"ORDER BY receive_time_unix_nano DESC ) = 1"
	if !strings.Contains(query, want) {
		t.Errorf("log dedup window = %q, want it to contain %q", query, want)
	}
}

// The DDL templates take (project, dataset, table) positionally; a wrong
// index count silently produces %!s(MISSING) rather than failing.
func TestDDLTemplatesFormatCleanly(t *testing.T) {
	for name, tmpl := range map[string]string{
		"spans":                SpansDDL,
		"logs":                 LogsDDL,
		"metrics":              MetricsDDL,
		"quarantine":           QuarantineDDL,
		"spans_view":           SpansDedupViewDDL,
		"logs_view":            LogsDedupViewDDL,
		"metrics_view":         MetricsDedupViewDDL,
		"spans_search_index":   SpansSearchIndexDDL,
		"logs_search_index":    LogsSearchIndexDDL,
		"metrics_search_index": MetricsSearchIndexDDL,
	} {
		got := fmt.Sprintf(tmpl, "proj", "ds", "tbl")
		// fmt marks a missing operand as %!s(MISSING). Match "(MISSING)", not
		// the bare word: QuarantineDDL names the MISSING_END_TIME error code.
		if strings.Contains(got, "%!") || strings.Contains(got, "(MISSING)") {
			t.Errorf("%s: template did not format cleanly:\n%s", name, got)
		}
		if !strings.Contains(got, "`proj.ds.tbl`") {
			t.Errorf("%s: expected fully qualified table reference", name)
		}
	}
}

func TestDedupQueriesAreBodiesAndDDLsAreStatements(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query string
		ddl   string
	}{
		{name: "spans", query: SpansDedupViewQuery, ddl: SpansDedupViewDDL},
		{name: "logs", query: LogsDedupViewQuery, ddl: LogsDedupViewDDL},
		{name: "metrics", query: MetricsDedupViewQuery, ddl: MetricsDedupViewDDL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := strings.TrimSpace(fmt.Sprintf(tc.query, "proj", "ds", "tbl"))
			if strings.HasSuffix(query, ";") {
				t.Fatalf("metadata API query must be a bare query body, got %q", query)
			}

			ddl := strings.TrimSpace(fmt.Sprintf(tc.ddl, "proj", "ds", "tbl"))
			if !strings.HasSuffix(ddl, ";") {
				t.Fatalf("operator-run DDL must end with a statement terminator, got %q", ddl)
			}
			if !strings.Contains(ddl, query) {
				t.Fatal("operator-run DDL does not contain the metadata API query body")
			}
		})
	}
}

// The window tests above pin the identity and ordering; this pins the whole
// view, so a filter, a projection or a source change cannot slip in either.
// The views are formatted for the default table names.
func TestDedupViewsMatchPublishedContract(t *testing.T) {
	for _, tc := range []struct {
		name      string
		query     string
		ddl       string
		table     string
		wantQuery string
	}{
		{
			name:  "spans",
			query: SpansDedupViewQuery,
			ddl:   SpansDedupViewDDL,
			table: "otel_spans",
			wantQuery: "SELECT * FROM `proj.ds.otel_spans` QUALIFY ROW_NUMBER() OVER ( " +
				"PARTITION BY start_time, service_name, trace_id, span_id, name " +
				"ORDER BY end_time_unix_nano DESC, receive_time_unix_nano DESC ) = 1",
		},
		{
			name:  "logs",
			query: LogsDedupViewQuery,
			ddl:   LogsDedupViewDDL,
			table: "otel_logs",
			wantQuery: "SELECT * FROM `proj.ds.otel_logs` QUALIFY ROW_NUMBER() OVER ( " +
				"PARTITION BY timestamp, time_unix_nano, service_name, trace_id, span_id, severity_number, log_name, insert_id " +
				"ORDER BY receive_time_unix_nano DESC ) = 1",
		},
		{
			name:  "metrics",
			query: MetricsDedupViewQuery,
			ddl:   MetricsDedupViewDDL,
			table: "otel_metrics",
			wantQuery: "SELECT * FROM `proj.ds.otel_metrics` QUALIFY ROW_NUMBER() OVER ( " +
				"PARTITION BY timestamp, time_unix_nano, service_name, metric_name, metric_type, point_fingerprint " +
				"ORDER BY receive_time_unix_nano DESC ) = 1",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := strings.Join(strings.Fields(fmt.Sprintf(tc.query, "proj", "ds", tc.table)), " ")
			if query != tc.wantQuery {
				t.Errorf("dedup view query =\n%s\nwant\n%s", query, tc.wantQuery)
			}
			ddl := strings.Join(strings.Fields(fmt.Sprintf(tc.ddl, "proj", "ds", tc.table)), " ")
			if want := "CREATE OR REPLACE VIEW `proj.ds." + tc.table + "_dedup` AS "; !strings.HasPrefix(ddl, want) {
				t.Errorf("dedup view DDL = %q, want it to start with %q", ddl, want)
			}
		})
	}
}

// accessPaths are the columns a base table is physically organized by: the
// column it is partitioned on, the columns it is clustered on, and the
// columns of its trace lookup search index.
type accessPaths struct {
	partition string
	cluster   []string
	index     []string
	indexAll  bool // the index covers ALL COLUMNS instead of a column list
}

var (
	partitionByRE = regexp.MustCompile(`(?m)^PARTITION BY \w+\((\w+)(?:, \w+)?\)$`)
	clusterByRE   = regexp.MustCompile(`(?m)^CLUSTER BY ([\w, ]+);$`)
	searchIndexRE = regexp.MustCompile(`\(([\w, ]+)\) OPTIONS \(analyzer`)
	windowKeysRE  = regexp.MustCompile(`(?s)PARTITION BY\s+(.+?)\s+ORDER BY`)
)

// parseAccessPaths reads the partitioning, clustering and search-index
// columns out of a table DDL and the DDL of its search index, both formatted
// for one table.
func parseAccessPaths(tableDDL, indexDDL string) (accessPaths, error) {
	var paths accessPaths
	m := partitionByRE.FindStringSubmatch(tableDDL)
	if m == nil {
		return paths, fmt.Errorf("no PARTITION BY clause in\n%s", tableDDL)
	}
	paths.partition = m[1]
	if m = clusterByRE.FindStringSubmatch(tableDDL); m == nil {
		return paths, fmt.Errorf("no CLUSTER BY clause in\n%s", tableDDL)
	}
	paths.cluster = splitColumns(m[1])
	if m = searchIndexRE.FindStringSubmatch(indexDDL); m == nil {
		return paths, fmt.Errorf("no indexed column list in\n%s", indexDDL)
	}
	if m[1] == "ALL COLUMNS" {
		paths.indexAll = true
	} else {
		paths.index = splitColumns(m[1])
	}
	return paths, nil
}

func splitColumns(list string) []string {
	var cols []string
	for _, col := range strings.Split(list, ",") {
		cols = append(cols, strings.TrimSpace(col))
	}
	return cols
}

// windowKeys returns the PARTITION BY columns of the window function in a
// dedup view query.
func windowKeys(viewQuery string) ([]string, error) {
	m := windowKeysRE.FindStringSubmatch(viewQuery)
	if m == nil {
		return nil, fmt.Errorf("no PARTITION BY ... ORDER BY window in\n%s", viewQuery)
	}
	return splitColumns(m[1]), nil
}

// uncoveredAccessPaths lists the partitioning, clustering and search-index
// columns that a dedup window does not partition on.
func uncoveredAccessPaths(window []string, paths accessPaths) []string {
	var missing []string
	for _, col := range slices.Concat([]string{paths.partition}, paths.cluster, paths.index) {
		if !slices.Contains(window, col) && !slices.Contains(missing, col) {
			missing = append(missing, col)
		}
	}
	return missing
}

// BigQuery pushes a filter below a window function only when the filter refers
// to PARTITION BY columns. Each dedup view must therefore partition on every
// column its base table is partitioned, clustered or search-indexed on, or a
// filter on a column it leaves out is applied only after the whole table has
// been scanned. The access paths are read out of the table DDLs instead of
// being repeated here, so adding a clustering column to a table without adding
// it to the dedup view fails this test.
//
// The metrics search index covers ALL COLUMNS, which adds no key: the exemplar
// trace_id and span_id it serves sit inside a repeated field, and a repeated
// field cannot be a PARTITION BY key.
func TestDedupWindowsCoverBaseTableAccessPaths(t *testing.T) {
	for _, tc := range []struct {
		name     string
		tableDDL string
		indexDDL string
		view     string
		contract *storagepb.TableSchema
	}{
		{"spans", SpansDDL, SpansSearchIndexDDL, SpansDedupViewQuery, SpansTableSchema()},
		{"logs", LogsDDL, LogsSearchIndexDDL, LogsDedupViewQuery, LogsTableSchema()},
		{"metrics", MetricsDDL, MetricsSearchIndexDDL, MetricsDedupViewQuery, MetricsTableSchema()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			format := func(tmpl string) string { return fmt.Sprintf(tmpl, "proj", "ds", "tbl") }
			paths, err := parseAccessPaths(format(tc.tableDDL), format(tc.indexDDL))
			if err != nil {
				t.Fatal(err)
			}
			window, err := windowKeys(format(tc.view))
			if err != nil {
				t.Fatal(err)
			}
			// A parse that found nothing would make the coverage check pass
			// vacuously.
			if paths.partition == "" || len(paths.cluster) == 0 || (len(paths.index) == 0 && !paths.indexAll) || len(window) == 0 {
				t.Fatalf("access paths %+v and window %v must all be non-empty", paths, window)
			}
			if missing := uncoveredAccessPaths(window, paths); len(missing) > 0 {
				t.Errorf("dedup window %v does not partition on %v, so a filter on them is not pushed below it (table access paths %+v)",
					window, missing, paths)
			}
			inTable := map[string]bool{}
			for _, name := range FieldNames(tc.contract) {
				inTable[name] = true
			}
			for _, key := range window {
				if !inTable[key] {
					t.Errorf("window key %q is not a column of the table", key)
				}
			}
			// The exporter creates the table from the metadata API, not from
			// the DDL, so the two must organize it the same way.
			md, _, err := DestinationMetadata(tc.contract, "proj", "ds", "tbl")
			if err != nil {
				t.Fatal(err)
			}
			if md.TimePartitioning.Field != paths.partition || !slices.Equal(md.Clustering.Fields, paths.cluster) {
				t.Errorf("table metadata partitions on %q and clusters on %v, the DDL on %q and %v",
					md.TimePartitioning.Field, md.Clustering.Fields, paths.partition, paths.cluster)
			}
		})
	}
}

// The coverage check must not be able to pass vacuously: a window that drops
// any partitioning, clustering or search-index column has to be reported.
func TestDedupWindowCoverageDetectsAMissingKey(t *testing.T) {
	format := func(tmpl string) string { return fmt.Sprintf(tmpl, "proj", "ds", "tbl") }
	paths, err := parseAccessPaths(format(SpansDDL), format(SpansSearchIndexDDL))
	if err != nil {
		t.Fatal(err)
	}
	full, err := windowKeys(format(SpansDedupViewQuery))
	if err != nil {
		t.Fatal(err)
	}
	if missing := uncoveredAccessPaths(full, paths); len(missing) != 0 {
		t.Fatalf("the published span window leaves %v uncovered", missing)
	}
	for _, drop := range []string{"start_time", "service_name", "trace_id", "name", "span_id"} {
		window := slices.DeleteFunc(slices.Clone(full), func(key string) bool { return key == drop })
		if len(window) != len(full)-1 {
			t.Fatalf("the span window %v does not contain %q", full, drop)
		}
		if missing := uncoveredAccessPaths(window, paths); !slices.Equal(missing, []string{drop}) {
			t.Errorf("a window without %q reports %v as uncovered, want [%s]", drop, missing, drop)
		}
	}
}

// The trace lookup indexes are operator-run statements, so their text is the
// contract: formatted for the default tables they must read exactly as
// published, and the index name must follow a custom table name.
func TestSearchIndexDDLs(t *testing.T) {
	for _, tc := range []struct {
		name  string
		tmpl  string
		table string
		want  string
	}{
		{
			name:  "spans",
			tmpl:  SpansSearchIndexDDL,
			table: "otel_spans",
			want: "CREATE SEARCH INDEX IF NOT EXISTS `otel_spans_trace_lookup_idx`\n" +
				"ON `proj.ds.otel_spans` (trace_id, span_id) OPTIONS (analyzer = 'NO_OP_ANALYZER');",
		},
		{
			name:  "logs",
			tmpl:  LogsSearchIndexDDL,
			table: "otel_logs",
			want: "CREATE SEARCH INDEX IF NOT EXISTS `otel_logs_trace_lookup_idx`\n" +
				"ON `proj.ds.otel_logs` (trace_id, span_id) OPTIONS (analyzer = 'NO_OP_ANALYZER');",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := strings.TrimSpace(fmt.Sprintf(tc.tmpl, "proj", "ds", tc.table)); got != tc.want {
				t.Errorf("search index DDL =\n%s\nwant\n%s", got, tc.want)
			}
			custom := fmt.Sprintf(tc.tmpl, "proj", "ds", "custom")
			if !strings.Contains(custom, "`custom_trace_lookup_idx`") {
				t.Errorf("index name must follow the table name, got\n%s", custom)
			}
		})
	}
}

// The companion views are optional, but once created they are queried
// directly. Their templates must format cleanly, read only the dedup views,
// and come both as a bare body for the metadata API and as a statement for
// operators.
func TestCompanionViewTemplates(t *testing.T) {
	for _, tc := range []struct {
		name    string
		view    string
		query   string
		ddl     string
		args    []any
		sources []string
	}{
		{
			name:    "allspans",
			view:    "otel_spans_as_allspans",
			query:   SpansAsAllSpansViewQuery,
			ddl:     SpansAsAllSpansViewDDL,
			args:    []any{"proj", "ds", "spans_tbl"},
			sources: []string{"FROM `proj.ds.spans_tbl_dedup`"},
		},
		{
			name:    "gen_ai_spans",
			view:    "v_gen_ai_spans",
			query:   GenAISpansViewQuery,
			ddl:     GenAISpansViewDDL,
			args:    []any{"proj", "ds", "spans_tbl"},
			sources: []string{"FROM `proj.ds.spans_tbl_dedup`"},
		},
		{
			name:  "gen_ai_logs",
			view:  "v_gen_ai_logs",
			query: GenAILogsViewQuery,
			ddl:   GenAILogsViewDDL,
			args:  []any{"proj", "ds", "logs_tbl", "spans_tbl"},
			sources: []string{
				"FROM `proj.ds.logs_tbl_dedup` l",
				"LEFT JOIN `proj.ds.spans_tbl_dedup` s " +
					"ON l.trace_id = s.trace_id AND l.span_id = s.span_id " +
					"AND l.service_name IS NOT DISTINCT FROM s.service_name " +
					"AND s.start_time BETWEEN TIMESTAMP_SUB(l.timestamp, INTERVAL 24 HOUR) " +
					"AND TIMESTAMP_ADD(l.timestamp, INTERVAL 1 HOUR)",
			},
		},
		{
			name:    "gen_ai_metrics",
			view:    "v_gen_ai_metrics",
			query:   GenAIMetricsViewQuery,
			ddl:     GenAIMetricsViewDDL,
			args:    []any{"proj", "ds", "metrics_tbl"},
			sources: []string{"FROM `proj.ds.metrics_tbl_dedup`"},
		},
		{
			name:    "gen_ai_model_costs",
			view:    "v_gen_ai_model_costs",
			query:   GenAIModelCostsViewQuery,
			ddl:     GenAIModelCostsViewDDL,
			args:    []any{"proj", "ds"},
			sources: []string{"FROM `proj.ds.v_gen_ai_spans`"},
		},
		{
			name:    "gen_ai_sessions",
			view:    "v_gen_ai_sessions",
			query:   GenAISessionsViewQuery,
			ddl:     GenAISessionsViewDDL,
			args:    []any{"proj", "ds"},
			sources: []string{"FROM `proj.ds.v_gen_ai_spans`"},
		},
		{
			name:    "gen_ai_tool_usage",
			view:    "v_gen_ai_tool_usage",
			query:   GenAIToolUsageViewQuery,
			ddl:     GenAIToolUsageViewDDL,
			args:    []any{"proj", "ds"},
			sources: []string{"FROM `proj.ds.v_gen_ai_spans`"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := strings.TrimSpace(fmt.Sprintf(tc.query, tc.args...))
			ddl := strings.TrimSpace(fmt.Sprintf(tc.ddl, tc.args...))
			for _, got := range []string{query, ddl} {
				if strings.Contains(got, "%!") || strings.Contains(got, "MISSING") {
					t.Fatalf("template did not format cleanly:\n%s", got)
				}
			}
			if strings.HasSuffix(query, ";") {
				t.Errorf("metadata API query must be a bare query body, got %q", query)
			}
			if !strings.HasSuffix(ddl, ";") || !strings.Contains(ddl, query) {
				t.Errorf("operator-run DDL must be the query body as a terminated statement, got %q", ddl)
			}
			if want := "CREATE OR REPLACE VIEW `proj.ds." + tc.view + "` AS\n"; !strings.HasPrefix(ddl, want) {
				t.Errorf("DDL must start with %q, got %q", want, ddl)
			}
			normalized := strings.Join(strings.Fields(query), " ")
			for _, source := range tc.sources {
				if !strings.Contains(normalized, source) {
					t.Errorf("query must contain %q, got %q", source, normalized)
				}
			}
			if strings.Contains(query, "_tbl`") {
				t.Errorf("query must read only the dedup views, never a raw table:\n%s", query)
			}
		})
	}
}

// The event name match is the only literal percent sign in the templates. It
// is doubled in the source because the templates go through fmt.Sprintf, so
// the formatted view must carry exactly one.
func TestGenAILogsViewMatchesGenAIEventNamePrefix(t *testing.T) {
	query := fmt.Sprintf(GenAILogsViewQuery, "proj", "ds", "logs_tbl", "spans_tbl")
	if !strings.Contains(query, "LIKE 'gen_ai.%'\n") {
		t.Errorf("v_gen_ai_logs must match event names by the gen_ai. prefix, got:\n%s", query)
	}
	if strings.Contains(query, "%%") {
		t.Errorf("formatted v_gen_ai_logs still has an escaped percent sign:\n%s", query)
	}
}

var attributeKeyRE = regexp.MustCompile(`(?:(\w+)\.)?attributes(?:, '\$\."([^"]+)"'|\['([^']+)'\])`)

// attributeKeys lists the attribute keys an expression reads, in order. A key
// read through a table alias is prefixed with it, so "s:gen_ai.system" is the
// key of the span a log record joins.
func attributeKeys(expr string) []string {
	var keys []string
	for _, m := range attributeKeyRE.FindAllStringSubmatch(expr, -1) {
		key := m[2]
		if key == "" {
			key = m[3]
		}
		if m[1] != "" {
			key = m[1] + ":" + key
		}
		keys = append(keys, key)
	}
	return keys
}

// projectionExprs maps each column a view projects to the expression that
// produces it.
func projectionExprs(t *testing.T, tmpl string, args ...any) map[string]string {
	t.Helper()
	items, err := selectList(fmt.Sprintf(tmpl, args...))
	if err != nil {
		t.Fatal(err)
	}
	exprs := make(map[string]string, len(items))
	for _, item := range items {
		exprs[item.alias] = item.expr
	}
	return exprs
}

func normalizeSQL(sql string) string {
	return strings.Join(strings.Fields(sql), " ")
}

// The reasoning token count arrives under gen_ai.usage.reasoning.output_tokens
// and, from earlier instrumentation, under gen_ai.usage.reasoning_tokens, and
// the current key must win. error.type is promoted for failure breakdowns.
func TestGenAISpansViewPromotesReasoningTokensAndErrorType(t *testing.T) {
	exprs := projectionExprs(t, GenAISpansViewQuery, "proj", "ds", "spans")
	for col, want := range map[string][]string{
		"gen_ai_usage_reasoning_tokens": {"gen_ai.usage.reasoning.output_tokens", "gen_ai.usage.reasoning_tokens"},
		"error_type":                    {"error.type"},
	} {
		if got := attributeKeys(exprs[col]); !slices.Equal(got, want) {
			t.Errorf("v_gen_ai_spans.%s reads %v, want %v", col, got, want)
		}
	}
}

// A log record and the span it belongs to describe one operation, so the
// system, the agent name, the session and the user come out of the same
// attribute keys, in the same order, as the columns of the span view with
// those names: the record's own attributes first, then the span's.
func TestGenAILogsViewAlignsWithGenAISpansView(t *testing.T) {
	spans := projectionExprs(t, GenAISpansViewQuery, "proj", "ds", "spans")
	logs := projectionExprs(t, GenAILogsViewQuery, "proj", "ds", "logs", "spans")
	for _, col := range []string{"gen_ai_system", "gen_ai_agent_name", "adk_session_id", "adk_user_id"} {
		keys := attributeKeys(spans[col])
		if len(keys) < 2 {
			t.Fatalf("v_gen_ai_spans.%s reads %v; expected a chain of attribute keys", col, keys)
		}
		var want []string
		for _, alias := range []string{"l", "s"} {
			for _, key := range keys {
				want = append(want, alias+":"+key)
			}
		}
		if got := attributeKeys(logs[col]); !slices.Equal(got, want) {
			t.Errorf("v_gen_ai_logs.%s reads %v, want the keys of v_gen_ai_spans.%s first from the record, then from the span: %v",
				col, got, col, want)
		}
	}
}

// v_gen_ai_model_costs and v_gen_ai_sessions must decide what an LLM call is,
// and which spans have token-reporting children, by one rule. Otherwise the
// call counts of the two views disagree.
func TestRollupViewsShareTheTokenParentsRule(t *testing.T) {
	cte := normalizeSQL(fmt.Sprintf(genAITokenParentsCTE, "proj", "ds"))
	for _, tc := range []struct {
		name  string
		query string
	}{
		{"gen_ai_model_costs", GenAIModelCostsViewQuery},
		{"gen_ai_sessions", GenAISessionsViewQuery},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := normalizeSQL(fmt.Sprintf(tc.query, "proj", "ds"))
			if !strings.HasPrefix(query, "WITH "+cte) {
				t.Errorf("query must open with the shared token_parents CTE:\n%s\nwant prefix\nWITH %s", query, cte)
			}
			if !strings.Contains(query, normalizeSQL(genAILLMCallPredicate)) {
				t.Errorf("query must apply the shared LLM call predicate:\n%s", query)
			}
			if !strings.Contains(query, "LEFT JOIN token_parents AS tp ON tp.trace_id = s.trace_id AND tp.span_id = s.span_id") {
				t.Errorf("query must left-join the token parents on trace_id and span_id:\n%s", query)
			}
		})
	}
	if !strings.HasPrefix(cte, "token_parents AS ( SELECT DISTINCT trace_id, parent_span_id AS span_id FROM `proj.ds.v_gen_ai_spans`") {
		t.Errorf("token_parents must list the parents of spans in v_gen_ai_spans, got %q", cte)
	}
}

// A session is one row for each service, UTC date of its spans' start times
// and session ID, so a filter on a grouping key reaches the span scan. Token
// totals count only spans without token-reporting children, which keeps a
// parent span from adding what its children already reported.
func TestGenAISessionsViewGrainAndTokenRule(t *testing.T) {
	query := normalizeSQL(fmt.Sprintf(GenAISessionsViewQuery, "proj", "ds"))
	for _, want := range []string{
		"DATE(s.start_time) AS session_start_date",
		"GROUP BY service_name, session_start_date, session_id",
	} {
		if !strings.Contains(query, want) {
			t.Errorf("sessions view must contain %q, got %q", want, query)
		}
	}
	if strings.Contains(query, "ANY_VALUE(service_name)") {
		t.Error("service_name is a grouping key of the sessions view, not an ANY_VALUE aggregate")
	}
	exprs := projectionExprs(t, GenAISessionsViewQuery, "proj", "ds")
	for _, col := range []string{
		"total_input_tokens", "total_output_tokens", "total_cache_read_input_tokens",
		"total_cache_creation_input_tokens", "total_reasoning_tokens",
	} {
		if !strings.HasPrefix(exprs[col], "SUM(IF(is_token_leaf, COALESCE(") {
			t.Errorf("sessions %s = %q, want a sum over token leaf spans only", col, exprs[col])
		}
	}
	for col, want := range map[string]string{
		"llm_call_count":         "COUNTIF(is_llm_call)",
		"agent_invocation_count": "COUNTIF(gen_ai_operation_name = 'invoke_agent')",
	} {
		if exprs[col] != want {
			t.Errorf("sessions %s = %q, want %q", col, exprs[col], want)
		}
	}
}

// v_gen_ai_metrics keeps only the gen_ai.* instruments, and the LIKE
// pattern's percent sign is doubled in the source because the template goes
// through fmt.Sprintf, so the formatted view must carry exactly one.
func TestGenAIMetricsView(t *testing.T) {
	query := strings.TrimSpace(fmt.Sprintf(GenAIMetricsViewQuery, "proj", "ds", "metrics_tbl"))
	if !strings.HasSuffix(query, "\nWHERE metric_name LIKE 'gen_ai.%'") {
		t.Errorf("v_gen_ai_metrics must end by matching metric names by the gen_ai. prefix, got:\n%s", query)
	}
	if strings.Contains(query, "%%") {
		t.Errorf("formatted v_gen_ai_metrics still has an escaped percent sign:\n%s", query)
	}
	exprs := projectionExprs(t, GenAIMetricsViewQuery, "proj", "ds", "metrics_tbl")
	for col, want := range map[string][]string{
		"gen_ai_system":         {"gen_ai.system", "gen_ai.provider.name"},
		"gen_ai_provider_name":  {"gen_ai.provider.name"},
		"gen_ai_operation_name": {"gen_ai.operation.name"},
		"gen_ai_request_model":  {"gen_ai.request.model"},
		"gen_ai_response_model": {"gen_ai.response.model"},
		"gen_ai_token_type":     {"gen_ai.token.type"},
		"error_type":            {"error.type"},
	} {
		if got := attributeKeys(exprs[col]); !slices.Equal(got, want) {
			t.Errorf("v_gen_ai_metrics.%s reads %v, want %v", col, got, want)
		}
	}
}

// The companion views are published layouts that queries pin against, so
// compare every projected column, in order. A column passed through unchanged
// must exist in the contract of the table it comes from, or BigQuery rejects
// the view when it is created. The rollup views read another view rather than
// a table, so they have no contract and only their projection is compared.
func TestCompanionViewsProjectPublishedColumns(t *testing.T) {
	for _, tc := range []struct {
		name      string
		query     string
		args      []any
		contract  *storagepb.TableSchema // nil for a view over another view
		qualifier string                 // table alias every passed-through column must use
		want      []string
	}{
		{
			name:     "allspans",
			query:    SpansAsAllSpansViewQuery,
			args:     []any{"proj", "ds", "spans"},
			contract: SpansTableSchema(),
			// The 25 columns of the Cloud Trace _AllSpans layout.
			want: []string{
				"trace_id", "span_id", "trace_state", "parent_span_id", "name", "kind",
				"start_time", "start_time_unix_nano", "end_time", "end_time_unix_nano",
				"receive_time", "receive_time_unix_nano", "duration_nano",
				"attributes", "attributes_schema", "dropped_attributes_count",
				"events", "dropped_events_count", "links", "dropped_links_count",
				"status", "resource", "resource_schema_url", "instrumentation_scope", "scope_schema_url",
			},
		},
		{
			name:     "gen_ai_spans",
			query:    GenAISpansViewQuery,
			args:     []any{"proj", "ds", "spans"},
			contract: SpansTableSchema(),
			// 29 identity, timing and promoted columns, the six raw columns
			// kept for anything not promoted, the 15 OTel GenAI
			// v1.37+/v1.40+ promoted columns, then error_type, which was
			// appended last so that no earlier column moved.
			want: []string{
				"start_time", "end_time", "duration_nano", "duration_ms",
				"trace_id", "span_id", "parent_span_id", "service_name",
				"span_name", "span_kind", "status_code", "status_message",
				"gen_ai_operation_name", "gen_ai_system", "gen_ai_request_model", "gen_ai_response_model",
				"gen_ai_agent_name", "gen_ai_agent_id", "gen_ai_conversation_id",
				"gen_ai_usage_input_tokens", "gen_ai_usage_output_tokens", "gen_ai_request_temperature",
				"gen_ai_response_finish_reason", "gen_ai_tool_name", "gen_ai_tool_call_id",
				"adk_event_id", "adk_session_id", "adk_invocation_id", "adk_user_id",
				"payload_refs", "attributes", "resource", "instrumentation_scope", "events", "links",
				"gen_ai_provider_name", "gen_ai_usage_cache_read_input_tokens",
				"gen_ai_usage_cache_creation_input_tokens", "gen_ai_usage_reasoning_tokens",
				"gen_ai_request_top_p", "gen_ai_request_top_k", "gen_ai_request_max_tokens",
				"gen_ai_output_type", "gen_ai_tool_type", "gen_ai_tool_description",
				"gen_ai_agent_description", "gen_ai_agent_parent_id", "gen_ai_data_source_id",
				"gen_ai_evaluation_name", "gen_ai_evaluation_score",
				"error_type",
			},
		},
		{
			name:      "gen_ai_logs",
			query:     GenAILogsViewQuery,
			args:      []any{"proj", "ds", "logs", "spans"},
			contract:  LogsTableSchema(),
			qualifier: "l",
			// 20 columns: the passed-through log columns and the promoted
			// GenAI and agent fields.
			want: []string{
				"timestamp", "receive_time", "trace_id", "span_id", "service_name",
				"event_name", "severity_number", "severity_text",
				"gen_ai_operation_name", "gen_ai_system", "gen_ai_agent_name",
				"adk_session_id", "adk_invocation_id", "adk_event_id", "adk_user_id",
				"body", "payload_refs", "attributes", "resource", "instrumentation_scope",
			},
		},
		{
			name:     "gen_ai_metrics",
			query:    GenAIMetricsViewQuery,
			args:     []any{"proj", "ds", "metrics"},
			contract: MetricsTableSchema(),
			// 36 columns: the metric identity, seven promoted GenAI
			// attributes, every point-value column of the table, and the
			// raw attribute, resource and scope bags.
			want: []string{
				"timestamp", "start_time", "receive_time", "service_name",
				"metric_name", "metric_description", "metric_unit", "metric_type",
				"aggregation_temporality", "is_monotonic",
				"gen_ai_operation_name", "gen_ai_system", "gen_ai_provider_name",
				"gen_ai_request_model", "gen_ai_response_model", "gen_ai_token_type", "error_type",
				"value_type", "value_double", "value_int64",
				"count", "sum", "min", "max", "explicit_bounds", "bucket_counts",
				"exp_scale", "exp_zero_count", "exp_zero_threshold", "exp_positive", "exp_negative",
				"summary_quantiles", "exemplars",
				"attributes", "resource", "instrumentation_scope",
			},
		},
		{
			name:      "gen_ai_model_costs",
			query:     GenAIModelCostsViewQuery,
			args:      []any{"proj", "ds"},
			qualifier: "s",
			// The 15 columns the view had before the double-counting fix,
			// in their original order.
			want: []string{
				"usage_hour", "service_name", "gen_ai_agent_name", "provider", "model",
				"call_count", "error_count",
				"total_input_tokens", "total_output_tokens", "total_cache_read_input_tokens",
				"total_cache_creation_input_tokens", "total_reasoning_tokens",
				"p50_duration_ms", "p95_duration_ms", "p99_duration_ms",
			},
		},
		{
			name:  "gen_ai_sessions",
			query: GenAISessionsViewQuery,
			args:  []any{"proj", "ds"},
			// The 15 columns the view had before the grain change, in
			// their original order, then the four it gained.
			want: []string{
				"session_id", "user_id", "service_name", "agent_name",
				"session_start_time", "session_end_time", "session_duration_ms",
				"turn_count", "invocation_count", "tool_call_count", "error_count",
				"total_input_tokens", "total_output_tokens", "total_cache_read_input_tokens",
				"total_reasoning_tokens",
				"session_start_date", "total_cache_creation_input_tokens",
				"llm_call_count", "agent_invocation_count",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			items, err := selectList(fmt.Sprintf(tc.query, tc.args...))
			if err != nil {
				t.Fatal(err)
			}
			inContract := map[string]bool{}
			if tc.contract != nil {
				for _, name := range FieldNames(tc.contract) {
					inContract[name] = true
				}
			}
			var got []string
			for _, item := range items {
				got = append(got, item.alias)
				if !item.passthrough {
					continue
				}
				if item.qualifier != tc.qualifier {
					t.Errorf("column %q is read through %q, want %q", item.alias, item.qualifier, tc.qualifier)
				}
				if tc.contract != nil && !inContract[item.alias] {
					t.Errorf("column %q passes through but is not in the contract", item.alias)
				}
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("projected %d columns:\n%v\nwant %d:\n%v", len(got), got, len(tc.want), tc.want)
			}
		})
	}
}

// selectItem is one entry of a view's projection.
type selectItem struct {
	alias       string // output column name
	expr        string // the expression that produces it, or the column itself
	qualifier   string // table alias of a passed-through column, if any
	passthrough bool   // the entry is a bare, possibly qualified, column
}

// selectList parses the projection of a query laid out as the companion views
// are: SELECT alone on a line, entries separated by top-level commas, "--"
// comment lines, and FROM at the start of the first line after the
// projection. A query that opens with common table expressions has one such
// block per SELECT, and the last one is the projection of the view itself. An
// entry is a column, optionally qualified, or an expression ending in "AS
// alias".
func selectList(query string) ([]selectItem, error) {
	var lines, last []string
	inSelect := false
	for _, line := range strings.Split(query, "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case trimmed == "SELECT":
			inSelect, lines = true, nil
		case !inSelect || strings.HasPrefix(trimmed, "--"):
		case strings.HasPrefix(trimmed, "FROM "):
			inSelect, last = false, lines
		default:
			lines = append(lines, trimmed)
		}
	}
	if len(last) == 0 {
		return nil, fmt.Errorf("no SELECT ... FROM projection in %q", query)
	}
	return splitSelectList(strings.Join(last, " "))
}

// splitSelectList splits a projection on the commas that are outside quotes,
// parentheses and brackets.
func splitSelectList(list string) ([]selectItem, error) {
	var items []selectItem
	depth, quoted, start := 0, false, 0
	for i := 0; i <= len(list); i++ {
		if i < len(list) {
			switch c := list[i]; {
			case c == '\'':
				quoted = !quoted
			case quoted:
			case c == '(' || c == '[':
				depth++
			case c == ')' || c == ']':
				depth--
			}
			if quoted || depth != 0 || list[i] != ',' {
				continue
			}
		}
		item, err := parseSelectItem(strings.Join(strings.Fields(list[start:i]), " "))
		if err != nil {
			return nil, err
		}
		items = append(items, item)
		start = i + 1
	}
	return items, nil
}

func parseSelectItem(entry string) (selectItem, error) {
	if i := strings.LastIndex(entry, " AS "); i >= 0 {
		if alias := entry[i+len(" AS "):]; isColumnName(alias) {
			return selectItem{alias: alias, expr: entry[:i]}, nil
		}
	}
	qualifier, column, qualified := strings.Cut(entry, ".")
	if !qualified {
		qualifier, column = "", entry
	}
	if !isColumnName(column) || (qualified && !isColumnName(qualifier)) {
		return selectItem{}, fmt.Errorf("projection entry %q is neither a column nor an aliased expression", entry)
	}
	return selectItem{alias: column, expr: entry, qualifier: qualifier, passthrough: true}, nil
}

// The DDL and the storagepb.TableSchema describe the same contract from two
// directions: the DDL is what an operator runs, the TableSchema is what the
// exporter writes against. If they drift, rows get written against a shape the
// table does not have, so diff them column for column.
func TestDDLAndTableSchemaAgree(t *testing.T) {
	for _, tc := range []struct {
		name string
		ddl  string
		cols []string
	}{
		{"spans", SpansDDL, FieldNames(SpansTableSchema())},
		{"logs", LogsDDL, FieldNames(LogsTableSchema())},
		{"metrics", MetricsDDL, FieldNames(MetricsTableSchema())},
		{"quarantine", QuarantineDDL, FieldNames(QuarantineTableSchema())},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inDDL := ddlColumns(tc.ddl)

			for _, c := range tc.cols {
				if !inDDL[c] {
					t.Errorf("column %q is in the TableSchema but not the DDL", c)
				}
				delete(inDDL, c)
			}
			for c := range inDDL {
				t.Errorf("column %q is in the DDL but not the TableSchema", c)
			}
		})
	}
}

// TestDDLAndTableSchemaAgree compares top-level names only. With STRUCT
// columns in the contract, a nested field, a type or a mode could drift
// unnoticed, so this parses each DDL into a TableSchema and requires the two
// to be identical.
func TestDDLMatchesTableSchema(t *testing.T) {
	for _, tc := range []struct {
		name string
		ddl  string
		want *storagepb.TableSchema
	}{
		{"spans", SpansDDL, SpansTableSchema()},
		{"logs", LogsDDL, LogsTableSchema()},
		{"metrics", MetricsDDL, MetricsTableSchema()},
		{"quarantine", QuarantineDDL, QuarantineTableSchema()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseDDL(tc.ddl)
			if err != nil {
				t.Fatal(err)
			}
			if diff := schemaDiff(got, tc.want); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

// The parser must not be able to pass vacuously: drift it has to catch,
// nested or at the top level, must make the comparison fail.
func TestDDLMatchesTableSchemaDetectsDrift(t *testing.T) {
	for name, mutate := range map[string]func(string) string{
		"nested type": func(ddl string) string {
			return strings.Replace(ddl, "code_name                        STRING", "code_name                        INT64", 1)
		},
		"top-level mode": func(ddl string) string {
			return strings.Replace(ddl, "duration_nano                      INT64     NOT NULL", "duration_nano                      INT64", 1)
		},
		"nested field": func(ddl string) string {
			return strings.Replace(ddl, "      authorizer                     STRING,\n", "", 1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			ddl := mutate(SpansDDL)
			if ddl == SpansDDL {
				t.Fatal("mutation did not apply; update it to match SpansDDL")
			}
			got, err := parseDDL(ddl)
			if err != nil {
				t.Fatal(err)
			}
			if schemaDiff(got, SpansTableSchema()) == "" {
				t.Fatal("drift went undetected")
			}
		})
	}
}

// QuarantineDDL carries trailing -- comments on some column lines, and one of
// them contains a ">". The parsers must read past those comments, never skip
// the commented lines, so drift on exactly those lines must still be caught.
func TestQuarantineDDLDriftOnCommentedLinesIsDetected(t *testing.T) {
	for name, mutate := range map[string]func(string) string{
		"mode before a comment": func(ddl string) string {
			return strings.Replace(ddl, "error_code                         STRING    NOT NULL, --", "error_code                         STRING, --", 1)
		},
		"type before a comment": func(ddl string) string {
			return strings.Replace(ddl, "raw_payload                        STRING,", "raw_payload                        JSON,", 1)
		},
		"field after a commented closer": func(ddl string) string {
			return strings.Replace(ddl, "  exporter_instance                  STRING,\n", "", 1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			ddl := mutate(QuarantineDDL)
			if ddl == QuarantineDDL {
				t.Fatal("mutation did not apply; update it to match QuarantineDDL")
			}
			got, err := parseDDL(ddl)
			if err != nil {
				t.Fatal(err)
			}
			if schemaDiff(got, QuarantineTableSchema()) == "" {
				t.Fatal("drift went undetected")
			}
		})
	}
	cols := ddlColumns(QuarantineDDL)
	for _, want := range []string{"payload_ref", "exporter_instance", "schema_version"} {
		if !cols[want] {
			t.Errorf("ddlColumns lost %q after the commented payload_ref closer", want)
		}
	}
	if cols["partition_expiration_days"] {
		t.Error("ddlColumns read a table option as a column")
	}
}

// The quarantine payload_ref column and payload_refs.object_ref on the signal
// tables both hold a BigQuery ObjectRef value, so they must have one shape.
func TestQuarantinePayloadRefMatchesObjectRef(t *testing.T) {
	var objectRef, payloadRef *storagepb.TableFieldSchema
	for _, fld := range SpansTableSchema().GetFields() {
		if fld.GetName() == "payload_refs" {
			for _, nested := range fld.GetFields() {
				if nested.GetName() == "object_ref" {
					objectRef = nested
				}
			}
		}
	}
	for _, fld := range QuarantineTableSchema().GetFields() {
		if fld.GetName() == "payload_ref" {
			payloadRef = fld
		}
	}
	if objectRef == nil || payloadRef == nil {
		t.Fatalf("object_ref = %v, payload_ref = %v; both must exist", objectRef, payloadRef)
	}
	renamed := proto.Clone(payloadRef).(*storagepb.TableFieldSchema)
	renamed.Name = objectRef.GetName()
	if !proto.Equal(renamed, objectRef) {
		t.Fatalf("payload_ref = %v, want the object_ref shape %v", payloadRef, objectRef)
	}
	if payloadRef.GetMode() != storagepb.TableFieldSchema_NULLABLE {
		t.Fatalf("payload_ref mode = %s, want NULLABLE", payloadRef.GetMode())
	}
}

// The quarantine contract is 12 columns in a fixed order.
func TestQuarantineTableSchemaColumns(t *testing.T) {
	want := []string{
		"ingest_time", "signal", "error_code", "error_message", "service_name",
		"trace_id", "span_id", "raw_payload", "raw_attributes", "payload_ref",
		"exporter_instance", "schema_version",
	}
	if got := FieldNames(QuarantineTableSchema()); !slices.Equal(got, want) {
		t.Fatalf("quarantine columns = %v, want %v", got, want)
	}
	if SignalSpans != "spans" || SignalLogs != "logs" || SignalMetrics != "metrics" {
		t.Fatalf("signal values = %q, %q, %q; the DDL documents 'spans' | 'logs' | 'metrics'",
			SignalSpans, SignalLogs, SignalMetrics)
	}
}

// parseDDL reads the column list of a CREATE TABLE statement laid out one
// field per line, as SpansDDL, LogsDDL, MetricsDDL and QuarantineDDL are. It
// accepts exactly the shapes those DDLs use: "name TYPE" with an optional NOT
// NULL, "name ARRAY<TYPE>", "name STRUCT<" and "name ARRAY<STRUCT<" openers,
// and lines that start with the matching ">" closers. A -- comment, whole-line
// or trailing, is ignored.
func parseDDL(ddl string) (*storagepb.TableSchema, error) {
	types := map[string]storagepb.TableFieldSchema_Type{
		"STRING":    storagepb.TableFieldSchema_STRING,
		"INT64":     storagepb.TableFieldSchema_INT64,
		"FLOAT64":   storagepb.TableFieldSchema_DOUBLE,
		"BOOL":      storagepb.TableFieldSchema_BOOL,
		"TIMESTAMP": storagepb.TableFieldSchema_TIMESTAMP,
		"JSON":      storagepb.TableFieldSchema_JSON,
	}
	type frame struct {
		field *storagepb.TableFieldSchema
		width int // closing brackets this frame consumes
	}
	stack := []frame{{field: &storagepb.TableFieldSchema{}}}
	inColumns := false

	for _, line := range strings.Split(ddl, "\n") {
		line, _, _ = strings.Cut(line, "--")
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "CREATE TABLE"):
			inColumns = true
			continue
		case !inColumns || line == "":
			continue
		case line == ")":
			if len(stack) != 1 {
				return nil, fmt.Errorf("column list ends inside %q", stack[len(stack)-1].field.GetName())
			}
			return &storagepb.TableSchema{Fields: stack[0].field.GetFields()}, nil
		case strings.HasPrefix(line, ">"):
			for n := len(line) - len(strings.TrimLeft(line, ">")); n > 0; {
				if len(stack) == 1 {
					return nil, fmt.Errorf("unbalanced %q", line)
				}
				n -= stack[len(stack)-1].width
				stack = stack[:len(stack)-1]
				if n < 0 {
					return nil, fmt.Errorf("closer %q splits an ARRAY<STRUCT<", line)
				}
			}
			continue
		}

		parts := strings.Fields(strings.TrimSuffix(line, ","))
		if len(parts) < 2 {
			return nil, fmt.Errorf("unsupported column definition %q", line)
		}
		parent := stack[len(stack)-1].field
		fld := &storagepb.TableFieldSchema{Name: parts[0], Mode: storagepb.TableFieldSchema_NULLABLE}
		parent.Fields = append(parent.Fields, fld)
		switch rest := strings.Join(parts[1:], " "); rest {
		case "STRUCT<":
			fld.Type = storagepb.TableFieldSchema_STRUCT
			stack = append(stack, frame{fld, 1})
		case "ARRAY<STRUCT<":
			fld.Type = storagepb.TableFieldSchema_STRUCT
			fld.Mode = storagepb.TableFieldSchema_REPEATED
			stack = append(stack, frame{fld, 2})
		default:
			if strings.HasPrefix(parts[1], "ARRAY<") && strings.HasSuffix(parts[1], ">") && len(parts) == 2 {
				elem := strings.TrimSuffix(strings.TrimPrefix(parts[1], "ARRAY<"), ">")
				typ, ok := types[elem]
				if !ok {
					return nil, fmt.Errorf("unsupported column definition %q", line)
				}
				fld.Type = typ
				fld.Mode = storagepb.TableFieldSchema_REPEATED
				continue
			}
			typ, ok := types[parts[1]]
			switch {
			case !ok:
				return nil, fmt.Errorf("unsupported column definition %q", line)
			case len(parts) == 2:
			case len(parts) == 4 && parts[2] == "NOT" && parts[3] == "NULL":
				fld.Mode = storagepb.TableFieldSchema_REQUIRED
			default:
				return nil, fmt.Errorf("unsupported column definition %q", line)
			}
			fld.Type = typ
		}
	}
	return nil, fmt.Errorf("no complete CREATE TABLE column list")
}

// schemaDiff returns "" when got and want are identical, and otherwise names
// the first field where they differ.
func schemaDiff(got, want *storagepb.TableSchema) string {
	if proto.Equal(got, want) {
		return ""
	}
	g, w := flattenSchema(got.GetFields(), ""), flattenSchema(want.GetFields(), "")
	for i := range max(len(g), len(w)) {
		var gi, wi string
		if i < len(g) {
			gi = g[i]
		}
		if i < len(w) {
			wi = w[i]
		}
		if gi != wi {
			return fmt.Sprintf("DDL and TableSchema first differ at field %d: DDL has %q, TableSchema has %q", i, gi, wi)
		}
	}
	return "DDL and TableSchema differ in a property flattenSchema does not show"
}

func flattenSchema(fields []*storagepb.TableFieldSchema, prefix string) []string {
	var out []string
	for _, fld := range fields {
		path := prefix + fld.GetName()
		out = append(out, fmt.Sprintf("%s %s %s", path, fld.GetType(), fld.GetMode()))
		out = append(out, flattenSchema(fld.GetFields(), path+".")...)
	}
	return out
}

// ddlColumns extracts the top-level column names of a CREATE TABLE column
// list, skipping the nested STRUCT fields of the events and links arrays.
// Comments are ignored, so a ">" inside one cannot unbalance the depth count,
// and reading stops at the closing ")", so table options are never taken for
// columns.
func ddlColumns(ddl string) map[string]bool {
	out := map[string]bool{}
	depth := 0
	for _, line := range strings.Split(ddl, "\n") {
		line, _, _ = strings.Cut(line, "--")
		trimmed := strings.TrimSpace(line)
		if trimmed == ")" {
			break
		}
		if depth == 0 {
			if f := strings.Fields(trimmed); len(f) >= 2 {
				if name := f[0]; isColumnName(name) && !isDDLKeyword(name) {
					out[name] = true
				}
			}
		}
		depth += strings.Count(line, "<") - strings.Count(line, ">")
	}
	return out
}

func isColumnName(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r != '_' && (r < 'a' || r > 'z') && (r < '0' || r > '9') {
			return false
		}
	}
	return true
}

func isDDLKeyword(s string) bool {
	switch s {
	case "create", "partition", "cluster", "select", "from", "qualify", "order", "where":
		return true
	}
	return false
}
