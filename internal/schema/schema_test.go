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
			name:  "gen_ai_metric_exemplars",
			view:  "v_gen_ai_metric_exemplars",
			query: GenAIMetricExemplarsViewQuery,
			ddl:   GenAIMetricExemplarsViewDDL,
			args:  []any{"proj", "ds", "spans_tbl"},
			sources: []string{
				"FROM `proj.ds.v_gen_ai_metrics` m, UNNEST(m.exemplars) AS ex",
				"LEFT JOIN `proj.ds.spans_tbl_dedup` s " +
					"ON s.trace_id = ex.trace_id AND s.span_id = ex.span_id " +
					"AND s.service_name IS NOT DISTINCT FROM m.service_name " +
					"AND s.start_time BETWEEN TIMESTAMP_SUB(m.timestamp, INTERVAL 24 HOUR) " +
					"AND TIMESTAMP_ADD(m.timestamp, INTERVAL 1 HOUR)",
			},
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
		{
			name:    "gen_ai_agent_trajectory",
			view:    "v_gen_ai_agent_trajectory",
			query:   GenAIAgentTrajectoryViewQuery,
			ddl:     GenAIAgentTrajectoryViewDDL,
			args:    []any{"proj", "ds"},
			sources: []string{"FROM `proj.ds.v_gen_ai_spans`"},
		},
		{
			name:    "mcp_spans",
			view:    "v_mcp_spans",
			query:   MCPSpansViewQuery,
			ddl:     MCPSpansViewDDL,
			args:    []any{"proj", "ds", "spans_tbl"},
			sources: []string{"FROM `proj.ds.spans_tbl_dedup`"},
		},
		{
			name:    "mcp_tool_usage",
			view:    "v_mcp_tool_usage",
			query:   MCPToolUsageViewQuery,
			ddl:     MCPToolUsageViewDDL,
			args:    []any{"proj", "ds"},
			sources: []string{"FROM `proj.ds.v_mcp_spans`"},
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
// (v1.41.0 and semantic-conventions-genai@f3c6d0c9) and under the legacy
// pre-semconv alias gen_ai.usage.reasoning_tokens, and the current key must win.
// error.type and the main-agent (semantic-conventions-genai@f3c6d0c9),
// agent-version and response-id (v1.40.0), and time-to-first-chunk (v1.41.0)
// attributes are promoted as additive columns.
func TestGenAISpansViewPromotesReasoningTokensAndErrorType(t *testing.T) {
	exprs := projectionExprs(t, GenAISpansViewQuery, "proj", "ds", "spans")
	for col, want := range map[string][]string{
		"gen_ai_usage_reasoning_tokens":       {"gen_ai.usage.reasoning.output_tokens", "gen_ai.usage.reasoning_tokens"},
		"error_type":                          {"error.type"},
		"gen_ai_main_agent_id":                {"gen_ai.main_agent.id", "resource:gen_ai.main_agent.id"},
		"gen_ai_main_agent_name":              {"gen_ai.main_agent.name", "resource:gen_ai.main_agent.name"},
		"gen_ai_main_agent_description":       {"gen_ai.main_agent.description", "resource:gen_ai.main_agent.description"},
		"gen_ai_agent_version":                {"gen_ai.agent.version", "resource:gen_ai.agent.version"},
		"gen_ai_response_id":                  {"gen_ai.response.id"},
		"gen_ai_response_time_to_first_chunk": {"gen_ai.response.time_to_first_chunk"},
	} {
		if got := attributeKeys(exprs[col]); !slices.Equal(got, want) {
			t.Errorf("v_gen_ai_spans.%s reads %v, want %v", col, got, want)
		}
	}
	if !strings.HasPrefix(exprs["gen_ai_response_time_to_first_chunk"], "LAX_FLOAT64(") {
		t.Errorf("v_gen_ai_spans.gen_ai_response_time_to_first_chunk = %q, want LAX_FLOAT64(...)", exprs["gen_ai_response_time_to_first_chunk"])
	}
	query := normalizeSQL(fmt.Sprintf(GenAISpansViewQuery, "proj", "ds", "spans"))
	for _, want := range []string{
		`OR JSON_VALUE(attributes, '$."gen_ai.main_agent.name"') IS NOT NULL`,
		`OR JSON_VALUE(attributes, '$."gen_ai.main_agent.id"') IS NOT NULL`,
	} {
		if !strings.Contains(query, want) {
			t.Errorf("v_gen_ai_spans WHERE clause must contain %q, got:\n%s", want, query)
		}
	}
	_, whereClause, hasWhere := strings.Cut(query, " WHERE ")
	if !hasWhere {
		t.Fatalf("v_gen_ai_spans query lacks WHERE clause:\n%s", query)
	}
	if strings.Contains(whereClause, "resource.attributes") {
		t.Errorf("v_gen_ai_spans WHERE clause must not match process-wide resource.attributes, got:\n%s", whereClause)
	}

	logExprs := projectionExprs(t, GenAILogsViewQuery, "proj", "ds", "logs", "spans")
	if got, want := attributeKeys(logExprs["error_type"]), []string{"l:error.type"}; !slices.Equal(got, want) {
		t.Errorf("v_gen_ai_logs.error_type reads %v, want %v (log record attributes only)", got, want)
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
	agg := normalizeSQL(genAITokenParentsAgg)
	leaf := normalizeSQL(genAITokenLeaf)
	predicate := normalizeSQL(genAILLMCallPredicate)
	for _, tc := range []struct {
		name  string
		query string
		want  []string
	}{
		{"gen_ai_model_costs", GenAIModelCostsViewQuery, []string{
			agg + " OVER (",
			"WHERE " + predicate,
		}},
		{"gen_ai_sessions", GenAISessionsViewQuery, []string{
			agg + " OVER (",
			leaf + " AS is_token_leaf",
			"COALESCE(" + predicate + ", FALSE) AS is_llm_call",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := normalizeSQL(fmt.Sprintf(tc.query, "proj", "ds"))
			for _, want := range tc.want {
				if !strings.Contains(query, want) {
					t.Errorf("query must contain the shared rule %q, got:\n%s", want, query)
				}
			}
		})
	}
	if !strings.HasPrefix(predicate, leaf+" AND ") {
		t.Errorf("the LLM call predicate must open with the token leaf test, got %q", predicate)
	}
	if want := "s.parent_span_id IS NOT NULL AND (s.gen_ai_usage_input_tokens IS NOT NULL OR s.gen_ai_usage_output_tokens IS NOT NULL), s.parent_span_id, NULL"; !strings.Contains(agg, want) {
		t.Errorf("the token parent aggregate must collect the parent of every span reporting tokens, got %q", agg)
	}
	if want := "NOT EXISTS ( SELECT 1 FROM UNNEST(s.token_parent_span_ids) AS token_parent_span_id WHERE token_parent_span_id = s.span_id )"; leaf != want {
		t.Errorf("the token leaf test = %q, want %q", leaf, want)
	}
}

// windowPartitionRE captures the PARTITION BY list of a window in normalized
// SQL, and plainWindowKeyRE matches a key that is a column of the CTE the
// window reads.
var (
	windowPartitionRE = regexp.MustCompile(`OVER \( PARTITION BY ([^()]+) \)`)
	plainWindowKeyRE  = regexp.MustCompile(`^s\.[a-z_]+$`)
)

// The rollup views read v_gen_ai_spans once. A second read, whether by a join
// or by a subquery, cannot be narrowed by a filter on the rollup, so each
// query scans every span partition and fails under require_partition_filter.
// The window that finds the token parents runs over that one read, and its
// PARTITION BY list is the matching rule: it must hold the column a time filter
// binds to, so that filter can reach the span scan, and service_name, so a
// parent and a child match within one service.
func TestRollupViewsReadSpansOnceAndWindowOnTheirGrain(t *testing.T) {
	for _, tc := range []struct {
		name      string
		query     string
		timeKey   string // the grouping column a time filter on the view binds to
		timeExpr  string // how the lower CTE defines that column
		partition []string
		before    string // text that must come before the window, or empty
	}{
		{
			name:      "gen_ai_model_costs",
			query:     GenAIModelCostsViewQuery,
			timeKey:   "usage_hour",
			timeExpr:  "TIMESTAMP_TRUNC(s.start_time, HOUR) AS usage_hour",
			partition: []string{"s.usage_hour", "s.service_name", "s.trace_id"},
		},
		{
			name:      "gen_ai_sessions",
			query:     GenAISessionsViewQuery,
			timeKey:   "session_start_date",
			timeExpr:  "DATE(s.start_time) AS session_start_date",
			partition: []string{"s.service_name", "s.session_start_date", "s.session_id", "s.trace_id"},
			// Spans without a session id leave before the window runs, so
			// they never take part in the matching.
			before: "WHERE COALESCE(s.adk_session_id, s.gen_ai_conversation_id) IS NOT NULL",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := normalizeSQL(fmt.Sprintf(tc.query, "proj", "ds"))
			if n := strings.Count(query, "`proj.ds.v_gen_ai_spans`"); n != 1 {
				t.Errorf("query reads v_gen_ai_spans %d times, want once:\n%s", n, query)
			}
			if strings.Contains(query, " JOIN ") {
				t.Errorf("query joins, which reads a table twice:\n%s", query)
			}
			if n := strings.Count(query, " OVER ("); n != 1 {
				t.Fatalf("query has %d windows, want one:\n%s", n, query)
			}
			m := windowPartitionRE.FindStringSubmatch(query)
			if m == nil {
				t.Fatalf("the window has no PARTITION BY list:\n%s", query)
			}
			got := strings.Split(m[1], ", ")
			if !slices.Contains(got, "s."+tc.timeKey) {
				t.Errorf("window partitions by %v, which lacks %s: a filter on it could not reach the span scan", got, tc.timeKey)
			}
			if !slices.Contains(got, "s.service_name") {
				t.Errorf("window partitions by %v, which lacks service_name", got)
			}
			if !slices.Equal(got, tc.partition) {
				t.Errorf("window partitions by %v, want %v", got, tc.partition)
			}
			// A key must be a plain column of the CTE the window reads. An
			// expression written inline in PARTITION BY showed no narrowing
			// of the span scan in BigQuery dry runs.
			for _, key := range got {
				if !plainWindowKeyRE.MatchString(key) {
					t.Errorf("window key %q is not a plain column of the CTE it reads", key)
				}
			}
			window := strings.Index(query, " OVER (")
			if def := strings.Index(query, tc.timeExpr); def < 0 || def > window {
				t.Errorf("%q must be defined before the window that partitions by it, got:\n%s", tc.timeExpr, query)
			}
			if tc.before != "" {
				if at := strings.Index(query, tc.before); at < 0 || at > window {
					t.Errorf("%q must come before the window, got:\n%s", tc.before, query)
				}
			}
		})
	}
}

// BigQuery rejects IGNORE NULLS, RESPECT NULLS and DISTINCT on the analytic
// form of ARRAY_AGG, and an ORDER BY inside its arguments. It accepts a
// CREATE VIEW statement that uses one and reports the error only when the view
// is queried, so no check that stops short of running the query would catch it.
func TestTokenParentsAggUsesOnlyWhatTheAnalyticFormAccepts(t *testing.T) {
	agg := strings.ToUpper(genAITokenParentsAgg)
	for _, option := range []string{"IGNORE NULLS", "RESPECT NULLS", "DISTINCT", "ORDER BY"} {
		if strings.Contains(agg, option) {
			t.Errorf("the token parent aggregate uses %s, which BigQuery rejects in a window:\n%s", option, genAITokenParentsAgg)
		}
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
		"gen_ai_token_modality": {"gen_ai.token.modality"},
		"gen_ai_agent_name":     {"gen_ai.agent.name", "gen_ai.main_agent.name", "resource:gen_ai.main_agent.name"},
	} {
		if got := attributeKeys(exprs[col]); !slices.Equal(got, want) {
			t.Errorf("v_gen_ai_metrics.%s reads %v, want %v", col, got, want)
		}
	}
	dimExpr := normalizeSQL(exprs["gen_ai_inference_usage_dimension"])
	for _, want := range []string{
		"WHEN metric_name = 'gen_ai.client.inference.usage.input_tokens' THEN 'input'",
		"WHEN metric_name = 'gen_ai.client.inference.usage.output_tokens' THEN 'output'",
		"WHEN metric_name = 'gen_ai.client.inference.usage.cache_read.input_tokens' THEN 'cache_read_input'",
		"WHEN metric_name = 'gen_ai.client.inference.usage.cache_write.input_tokens' THEN 'cache_write_input'",
		"WHEN metric_name = 'gen_ai.client.inference.usage.reasoning.output_tokens' THEN 'reasoning_output'",
		`ELSE JSON_VALUE(attributes, '$."gen_ai.token.type"') END`,
	} {
		if !strings.Contains(dimExpr, want) {
			t.Errorf("gen_ai_inference_usage_dimension = %q, missing %q", dimExpr, want)
		}
	}
}

// v_gen_ai_metric_exemplars unnests v_gen_ai_metrics.exemplars (whose timestamp
// field is time, not timestamp), keeps only exemplars carrying a non-empty
// trace_id and span_id, and left joins the span dedup view on trace_id, span_id
// and service_name within a [-24h, +1h] window around m.timestamp.
func TestGenAIMetricExemplarsView(t *testing.T) {
	query := strings.TrimSpace(fmt.Sprintf(GenAIMetricExemplarsViewQuery, "proj", "ds", "spans_tbl"))
	normalized := normalizeSQL(query)
	for _, want := range []string{
		"FROM `proj.ds.v_gen_ai_metrics` m, UNNEST(m.exemplars) AS ex WITH OFFSET AS exemplar_ordinal",
		"LEFT JOIN `proj.ds.spans_tbl_dedup` s ON s.trace_id = ex.trace_id AND s.span_id = ex.span_id AND s.service_name IS NOT DISTINCT FROM m.service_name",
		"AND s.start_time BETWEEN TIMESTAMP_SUB(m.timestamp, INTERVAL 24 HOUR) AND TIMESTAMP_ADD(m.timestamp, INTERVAL 1 HOUR)",
		"WHERE ex.trace_id IS NOT NULL AND ex.span_id IS NOT NULL AND ex.trace_id != '' AND ex.span_id != ''",
		"QUALIFY ROW_NUMBER() OVER ( PARTITION BY m.timestamp, m.time_unix_nano, m.service_name, m.metric_name, m.metric_type, m.point_fingerprint, exemplar_ordinal ORDER BY s.end_time DESC, s.start_time DESC, s.receive_time DESC, s.name ASC ) = 1",
	} {
		if !strings.Contains(normalized, want) {
			t.Errorf("v_gen_ai_metric_exemplars must contain %q, got:\n%s", want, normalized)
		}
	}
	if strings.Contains(normalized, "ex.timestamp") {
		t.Errorf("v_gen_ai_metric_exemplars must read ex.time, not ex.timestamp:\n%s", normalized)
	}
	exprs := projectionExprs(t, GenAIMetricExemplarsViewQuery, "proj", "ds", "spans_tbl")
	for col, want := range map[string]string{
		"metric_timestamp":             "m.timestamp",
		"metric_error_type":            "m.error_type",
		"exemplar_timestamp":           "ex.time",
		"exemplar_value_double":        "ex.value_double",
		"exemplar_value_int64":         "ex.value_int64",
		"trace_id":                     "ex.trace_id",
		"span_id":                      "ex.span_id",
		"exemplar_filtered_attributes": "ex.filtered_attributes",
		"span_start_time":              "s.start_time",
		"span_end_time":                "s.end_time",
		"span_duration_nano":           "s.duration_nano",
		"span_duration_ms":             "ROUND(s.duration_nano / 1e6, 3)",
		"span_name":                    "s.name",
		"span_status_code":             "s.status.code",
		"span_status_message":          "s.status.message",
	} {
		if exprs[col] != want {
			t.Errorf("v_gen_ai_metric_exemplars.%s = %q, want %q", col, exprs[col], want)
		}
	}
	for col, want := range map[string][]string{
		"span_gen_ai_agent_name": {"s:gen_ai.agent.name", "s:gcp.vertex.agent.name", "resource:gen_ai.agent.name", "s:gen_ai.main_agent.name", "resource:gen_ai.main_agent.name"},
		"span_gen_ai_tool_name":  {"s:gen_ai.tool.name"},
		"adk_session_id":         {"s:gcp.vertex.agent.session_id", "s:gen_ai.conversation.id"},
		"span_error_type":        {"s:error.type"},
	} {
		if got := attributeKeys(exprs[col]); !slices.Equal(got, want) {
			t.Errorf("v_gen_ai_metric_exemplars.%s reads %v, want %v", col, got, want)
		}
	}
}

// The exemplar window ranks the span rows of one exemplar slot, so it
// partitions by the identity of a point in the metric dedup view, which
// v_gen_ai_metrics passes through unchanged, and then by the exemplar's array
// offset. Leaving an identity column out would merge distinct points that
// share a microsecond timestamp and keep only one of their exemplars.
func TestGenAIMetricExemplarsViewPartitionsByMetricDedupIdentity(t *testing.T) {
	partitionBy := func(view, query string) []string {
		t.Helper()
		_, rest, ok := strings.Cut(normalizeSQL(query), "PARTITION BY ")
		if !ok {
			t.Fatalf("%s has no PARTITION BY:\n%s", view, query)
		}
		list, _, ok := strings.Cut(rest, " ORDER BY ")
		if !ok {
			t.Fatalf("%s has no ORDER BY after its PARTITION BY:\n%s", view, query)
		}
		cols := strings.Split(list, ",")
		for i, col := range cols {
			cols[i] = strings.TrimSpace(col)
		}
		return cols
	}
	identity := partitionBy("otel_metrics_dedup", fmt.Sprintf(MetricsDedupViewQuery, "proj", "ds", "metrics"))
	for _, col := range []string{"timestamp", "time_unix_nano", "point_fingerprint"} {
		if !slices.Contains(identity, col) {
			t.Fatalf("otel_metrics_dedup partitions by %v, which has no %s", identity, col)
		}
	}
	want := make([]string, 0, len(identity)+1)
	for _, col := range identity {
		want = append(want, "m."+col)
	}
	want = append(want, "exemplar_ordinal")
	got := partitionBy("v_gen_ai_metric_exemplars", fmt.Sprintf(GenAIMetricExemplarsViewQuery, "proj", "ds", "spans"))
	if !slices.Equal(got, want) {
		t.Errorf("v_gen_ai_metric_exemplars partitions by %v, want %v", got, want)
	}
	exprs := projectionExprs(t, GenAIMetricsViewQuery, "proj", "ds", "metrics")
	for _, col := range identity {
		if exprs[col] != col {
			t.Errorf("v_gen_ai_metrics.%s = %q, want the dedup view's column passed through", col, exprs[col])
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
			// v1.37.0+/v1.40.0 promoted columns, error_type, and six v1.40.0,
			// v1.41.0, and semantic-conventions-genai@f3c6d0c9 main-agent,
			// agent-version, and response columns appended last so that no
			// earlier column moved.
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
				"gen_ai_main_agent_id", "gen_ai_main_agent_name", "gen_ai_main_agent_description",
				"gen_ai_agent_version", "gen_ai_response_id", "gen_ai_response_time_to_first_chunk",
			},
		},
		{
			name:      "gen_ai_logs",
			query:     GenAILogsViewQuery,
			args:      []any{"proj", "ds", "logs", "spans"},
			contract:  LogsTableSchema(),
			qualifier: "l",
			// 21 columns: the passed-through log columns, the promoted
			// GenAI and agent fields, and error_type appended last.
			want: []string{
				"timestamp", "receive_time", "trace_id", "span_id", "service_name",
				"event_name", "severity_number", "severity_text",
				"gen_ai_operation_name", "gen_ai_system", "gen_ai_agent_name",
				"adk_session_id", "adk_invocation_id", "adk_event_id", "adk_user_id",
				"body", "payload_refs", "attributes", "resource", "instrumentation_scope",
				"error_type",
			},
		},
		{
			name:     "gen_ai_metrics",
			query:    GenAIMetricsViewQuery,
			args:     []any{"proj", "ds", "metrics"},
			contract: MetricsTableSchema(),
			// 41 columns: the metric identity, seven promoted GenAI
			// attributes, every point-value column of the table, the raw
			// attribute, resource and scope bags, three
			// semantic-conventions-genai@f3c6d0c9 metric dimensions, and
			// the two columns that complete the metric dedup identity,
			// appended last.
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
				"gen_ai_token_modality", "gen_ai_agent_name", "gen_ai_inference_usage_dimension",
				"time_unix_nano", "point_fingerprint",
			},
		},
		{
			name:      "gen_ai_model_costs",
			query:     GenAIModelCostsViewQuery,
			args:      []any{"proj", "ds"},
			qualifier: "s",
			// The 15 columns the view had before the double-counting fix,
			// in their original order, plus typed_error_count and
			// top_error_types appended last.
			want: []string{
				"usage_hour", "service_name", "gen_ai_agent_name", "provider", "model",
				"call_count", "error_count",
				"total_input_tokens", "total_output_tokens", "total_cache_read_input_tokens",
				"total_cache_creation_input_tokens", "total_reasoning_tokens",
				"p50_duration_ms", "p95_duration_ms", "p99_duration_ms",
				"typed_error_count", "top_error_types",
			},
		},
		{
			name:  "gen_ai_sessions",
			query: GenAISessionsViewQuery,
			args:  []any{"proj", "ds"},
			// The 15 columns the view had before the grain change, in
			// their original order, then the four it gained, then
			// typed_error_count and top_error_types appended last.
			want: []string{
				"session_id", "user_id", "service_name", "agent_name",
				"session_start_time", "session_end_time", "session_duration_ms",
				"turn_count", "invocation_count", "tool_call_count", "error_count",
				"total_input_tokens", "total_output_tokens", "total_cache_read_input_tokens",
				"total_reasoning_tokens",
				"session_start_date", "total_cache_creation_input_tokens",
				"llm_call_count", "agent_invocation_count",
				"typed_error_count", "top_error_types",
			},
		},
		{
			name:  "gen_ai_tool_usage",
			query: GenAIToolUsageViewQuery,
			args:  []any{"proj", "ds"},
			// The 10 columns of v_gen_ai_tool_usage, plus
			// typed_error_count and top_error_types appended last.
			want: []string{
				"usage_hour", "service_name", "gen_ai_agent_name",
				"gen_ai_tool_name", "gen_ai_tool_type",
				"call_count", "error_count", "error_rate",
				"p50_duration_ms", "p95_duration_ms",
				"typed_error_count", "top_error_types",
			},
		},
		{
			name:      "gen_ai_metric_exemplars",
			query:     GenAIMetricExemplarsViewQuery,
			args:      []any{"proj", "ds", "spans"},
			qualifier: "m",
			want: []string{
				"metric_timestamp", "service_name", "metric_name", "metric_unit", "metric_type",
				"gen_ai_operation_name", "gen_ai_system", "gen_ai_request_model", "gen_ai_response_model",
				"gen_ai_agent_name", "gen_ai_token_type", "gen_ai_token_modality", "metric_error_type",
				"exemplar_timestamp", "exemplar_value_double", "exemplar_value_int64",
				"trace_id", "span_id", "exemplar_filtered_attributes",
				"span_start_time", "span_end_time", "span_duration_nano", "span_duration_ms",
				"span_name", "span_status_code", "span_status_message",
				"span_gen_ai_agent_name", "span_gen_ai_tool_name", "adk_session_id", "span_error_type",
			},
		},
		{
			name:      "gen_ai_agent_trajectory",
			query:     GenAIAgentTrajectoryViewQuery,
			args:      []any{"proj", "ds"},
			qualifier: "t",
			// 31 base columns from v_gen_ai_spans followed by the 9
			// recursive trajectory columns.
			want: []string{
				"start_time", "end_time", "duration_nano", "duration_ms",
				"trace_id", "span_id", "parent_span_id", "service_name",
				"span_name", "span_kind", "status_code", "status_message",
				"gen_ai_operation_name", "gen_ai_system", "gen_ai_provider_name",
				"gen_ai_request_model", "gen_ai_response_model",
				"gen_ai_agent_id", "gen_ai_agent_name", "gen_ai_agent_version",
				"gen_ai_main_agent_id", "gen_ai_main_agent_name",
				"gen_ai_tool_name", "gen_ai_tool_call_id",
				"gen_ai_usage_input_tokens", "gen_ai_usage_output_tokens", "gen_ai_usage_reasoning_tokens",
				"adk_session_id", "adk_invocation_id", "adk_user_id",
				"error_type",
				"depth", "root_span_id", "nearest_agent_span_id", "nearest_agent_name",
				"agent_path", "is_orphan", "step_index", "is_leaf", "is_token_leaf",
			},
		},
		{
			name:     "mcp_spans",
			query:    MCPSpansViewQuery,
			args:     []any{"proj", "ds", "spans"},
			contract: SpansTableSchema(),
			// 34 columns: 12 span identity/status fields, 6 MCP & JSON-RPC
			// fields, 7 GenAI correlation fields, 3 server endpoint fields,
			// is_tool_call, also_in_gen_ai_view, and 4 raw bags.
			want: []string{
				"start_time", "end_time", "duration_nano", "duration_ms",
				"trace_id", "span_id", "parent_span_id", "service_name",
				"span_name", "span_kind", "status_code", "status_message",
				"mcp_method_name", "mcp_protocol_version", "mcp_session_id",
				"mcp_resource_uri", "jsonrpc_request_id", "rpc_response_status_code",
				"gen_ai_operation_name", "gen_ai_system", "gen_ai_tool_name",
				"gen_ai_tool_call_id", "gen_ai_prompt_name", "gen_ai_agent_name",
				"error_type",
				"server_address", "server_port", "network_transport",
				"is_tool_call", "also_in_gen_ai_view",
				"payload_refs", "attributes", "resource", "instrumentation_scope",
			},
		},
		{
			name:  "mcp_tool_usage",
			query: MCPToolUsageViewQuery,
			args:  []any{"proj", "ds"},
			// 18 columns: 8 grouping keys and 10 hourly volume, overlap,
			// error, and latency rollup columns.
			want: []string{
				"usage_hour", "service_name", "mcp_method_name", "mcp_protocol_version",
				"gen_ai_tool_name", "mcp_target_name", "span_kind", "server_address",
				"call_count", "gen_ai_tagged_call_count", "error_count", "typed_error_count",
				"top_error_types", "error_rate", "avg_duration_ms",
				"p50_duration_ms", "p95_duration_ms", "p99_duration_ms",
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

// Pre-existing view columns must never shift their 0-based index when new
// columns are appended. This pins the exact index of the first and last
// pre-existing column on every companion view modified in PR 1.1.
func TestCompanionViewPreExistingColumnIndicesUnchanged(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query string
		args  []any
		pins  map[int]string
	}{
		{
			name:  "gen_ai_spans",
			query: GenAISpansViewQuery,
			args:  []any{"proj", "ds", "spans"},
			pins: map[int]string{
				0:  "start_time",
				35: "gen_ai_provider_name",
				50: "error_type",
				51: "gen_ai_main_agent_id",
				56: "gen_ai_response_time_to_first_chunk",
			},
		},
		{
			name:  "gen_ai_logs",
			query: GenAILogsViewQuery,
			args:  []any{"proj", "ds", "logs", "spans"},
			pins: map[int]string{
				0:  "timestamp",
				19: "instrumentation_scope",
				20: "error_type",
			},
		},
		{
			name:  "gen_ai_metrics",
			query: GenAIMetricsViewQuery,
			args:  []any{"proj", "ds", "metrics"},
			pins: map[int]string{
				0:  "timestamp",
				16: "error_type",
				35: "instrumentation_scope",
				36: "gen_ai_token_modality",
				38: "gen_ai_inference_usage_dimension",
				39: "time_unix_nano",
				40: "point_fingerprint",
			},
		},
		{
			name:  "gen_ai_model_costs",
			query: GenAIModelCostsViewQuery,
			args:  []any{"proj", "ds"},
			pins: map[int]string{
				0:  "usage_hour",
				14: "p99_duration_ms",
				15: "typed_error_count",
				16: "top_error_types",
			},
		},
		{
			name:  "gen_ai_sessions",
			query: GenAISessionsViewQuery,
			args:  []any{"proj", "ds"},
			pins: map[int]string{
				0:  "session_id",
				15: "session_start_date",
				18: "agent_invocation_count",
				19: "typed_error_count",
				20: "top_error_types",
			},
		},
		{
			name:  "gen_ai_tool_usage",
			query: GenAIToolUsageViewQuery,
			args:  []any{"proj", "ds"},
			pins: map[int]string{
				0:  "usage_hour",
				9:  "p95_duration_ms",
				10: "typed_error_count",
				11: "top_error_types",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			items, err := selectList(fmt.Sprintf(tc.query, tc.args...))
			if err != nil {
				t.Fatal(err)
			}
			for idx, want := range tc.pins {
				if idx >= len(items) {
					t.Errorf("index %d (%s) out of range; view has %d columns", idx, want, len(items))
					continue
				}
				if got := items[idx].alias; got != want {
					t.Errorf("column[%d] = %q, want %q", idx, got, want)
				}
			}
		})
	}
}

func TestGenAIAgentTrajectoryView(t *testing.T) {
	query := normalizeSQL(fmt.Sprintf(GenAIAgentTrajectoryViewQuery, "proj", "ds"))
	baseCols := []string{
		"start_time", "end_time", "duration_nano", "duration_ms",
		"trace_id", "span_id", "parent_span_id", "service_name",
		"span_name", "span_kind", "status_code", "status_message",
		"gen_ai_operation_name", "gen_ai_system", "gen_ai_provider_name",
		"gen_ai_request_model", "gen_ai_response_model",
		"gen_ai_agent_id", "gen_ai_agent_name", "gen_ai_agent_version",
		"gen_ai_main_agent_id", "gen_ai_main_agent_name",
		"gen_ai_tool_name", "gen_ai_tool_call_id",
		"gen_ai_usage_input_tokens", "gen_ai_usage_output_tokens", "gen_ai_usage_reasoning_tokens",
		"adk_session_id", "adk_invocation_id", "adk_user_id",
		"error_type",
	}
	prefixed := func(prefix string) string {
		cols := make([]string, len(baseCols))
		for i, c := range baseCols {
			cols[i] = prefix + c
		}
		return strings.Join(cols, ", ")
	}
	if strings.Contains(query, "[OFFSET(ARRAY_LENGTH(t.agent_path)") {
		t.Fatalf("v_gen_ai_agent_trajectory must use SAFE_OFFSET on t.agent_path, got:\n%s", query)
	}
	for _, want := range []string{
		"WITH RECURSIVE spans AS ( SELECT " + strings.Join(baseCols, ", ") + ", " +
			`COALESCE( JSON_VALUE(attributes, '$."gen_ai.agent.name"'), JSON_VALUE(attributes, '$."gcp.vertex.agent.name"') ) AS declared_agent_name, ` +
			`COALESCE( JSON_VALUE(attributes, '$."gen_ai.agent.name"'), JSON_VALUE(attributes, '$."gcp.vertex.agent.name"'), IF(gen_ai_operation_name IN ('invoke_agent', 'create_agent'), gen_ai_agent_name, NULL) ) AS agent_path_name ` +
			"FROM `proj.ds.v_gen_ai_spans` " +
			"QUALIFY ROW_NUMBER() OVER ( PARTITION BY trace_id, service_name, span_id ORDER BY start_time ASC, end_time DESC, span_name ASC, duration_nano DESC ) = 1 )",
		"parent_ids AS ( SELECT DISTINCT trace_id, service_name, parent_span_id AS span_id FROM spans WHERE parent_span_id IS NOT NULL AND parent_span_id != '' )",
		"trajectory AS ( SELECT " + prefixed("s.") + ", 0 AS depth, s.span_id AS root_span_id, " +
			"CASE WHEN s.gen_ai_operation_name IN ('invoke_agent', 'create_agent') OR s.declared_agent_name IS NOT NULL THEN s.span_id ELSE CAST(NULL AS STRING) END AS nearest_agent_span_id, " +
			"CASE WHEN s.gen_ai_operation_name IN ('invoke_agent', 'create_agent') OR s.declared_agent_name IS NOT NULL THEN COALESCE(s.declared_agent_name, s.gen_ai_agent_name) ELSE CAST(NULL AS STRING) END AS nearest_agent_name, " +
			"CASE WHEN s.agent_path_name IS NOT NULL THEN [s.agent_path_name] ELSE ARRAY<STRING>[] END AS agent_path, " +
			"(s.parent_span_id IS NOT NULL AND s.parent_span_id != '') AS is_orphan, [s.span_id] AS visited_span_ids " +
			"FROM spans AS s LEFT JOIN spans AS p ON s.trace_id = p.trace_id AND s.parent_span_id = p.span_id AND s.service_name IS NOT DISTINCT FROM p.service_name AND p.start_time BETWEEN TIMESTAMP_SUB(s.start_time, INTERVAL 24 HOUR) AND TIMESTAMP_ADD(s.start_time, INTERVAL 1 HOUR) WHERE s.parent_span_id IS NULL OR s.parent_span_id = '' OR p.span_id IS NULL",
		"UNION ALL SELECT " + prefixed("c.") + ", t.depth + 1 AS depth, t.root_span_id AS root_span_id, " +
			"CASE WHEN c.gen_ai_operation_name IN ('invoke_agent', 'create_agent') OR (t.nearest_agent_span_id IS NULL AND c.declared_agent_name IS NOT NULL) THEN c.span_id ELSE t.nearest_agent_span_id END AS nearest_agent_span_id, " +
			"CASE WHEN c.gen_ai_operation_name IN ('invoke_agent', 'create_agent') OR (t.nearest_agent_span_id IS NULL AND c.declared_agent_name IS NOT NULL) THEN COALESCE(c.declared_agent_name, c.gen_ai_agent_name) ELSE t.nearest_agent_name END AS nearest_agent_name, " +
			"CASE WHEN c.agent_path_name IS NOT NULL AND ( ARRAY_LENGTH(t.agent_path) = 0 OR t.agent_path[SAFE_OFFSET(ARRAY_LENGTH(t.agent_path) - 1)] != c.agent_path_name ) THEN ARRAY_CONCAT(t.agent_path, [c.agent_path_name]) ELSE t.agent_path END AS agent_path, " +
			"t.is_orphan AS is_orphan, ARRAY_CONCAT(t.visited_span_ids, [c.span_id]) AS visited_span_ids " +
			"FROM trajectory AS t JOIN spans AS c ON c.trace_id = t.trace_id AND c.parent_span_id = t.span_id AND c.service_name IS NOT DISTINCT FROM t.service_name AND c.start_time BETWEEN TIMESTAMP_SUB(t.start_time, INTERVAL 1 HOUR) AND TIMESTAMP_ADD(t.start_time, INTERVAL 24 HOUR) WHERE t.depth < 16 AND c.span_id NOT IN UNNEST(t.visited_span_ids) )",
		"emitted_ancestors AS ( SELECT c.trace_id, c.service_name, c.span_id AS descendant_span_id, c.start_time AS descendant_start_time, c.gen_ai_usage_input_tokens IS NOT NULL AS has_input, c.gen_ai_usage_output_tokens IS NOT NULL AS has_output, c.gen_ai_usage_reasoning_tokens IS NOT NULL AS has_reasoning, p.span_id AS ancestor_span_id, p.parent_span_id AS ancestor_parent_span_id, p.start_time AS ancestor_start_time, 1 AS hop, [c.span_id, p.span_id] AS path_span_ids FROM trajectory AS c JOIN trajectory AS p ON p.trace_id = c.trace_id AND p.span_id = c.parent_span_id AND p.service_name IS NOT DISTINCT FROM c.service_name WHERE p.span_id != c.span_id UNION ALL SELECT ea.trace_id, ea.service_name, ea.descendant_span_id, ea.descendant_start_time, ea.has_input, ea.has_output, ea.has_reasoning, p.span_id AS ancestor_span_id, p.parent_span_id AS ancestor_parent_span_id, p.start_time AS ancestor_start_time, ea.hop + 1 AS hop, ARRAY_CONCAT(ea.path_span_ids, [p.span_id]) AS path_span_ids FROM emitted_ancestors AS ea JOIN trajectory AS p ON p.trace_id = ea.trace_id AND p.span_id = ea.ancestor_parent_span_id AND p.service_name IS NOT DISTINCT FROM ea.service_name WHERE ea.hop < 32 AND p.span_id NOT IN UNNEST(ea.path_span_ids) )",
		"token_parent_coverage AS ( SELECT ea.trace_id, ea.service_name, ea.ancestor_span_id AS span_id, LOGICAL_OR(ea.has_input) AS has_input, LOGICAL_OR(ea.has_output) AS has_output, LOGICAL_OR(ea.has_reasoning) AS has_reasoning FROM emitted_ancestors AS ea LEFT JOIN emitted_ancestors AS reverse_path ON reverse_path.trace_id = ea.trace_id AND reverse_path.service_name IS NOT DISTINCT FROM ea.service_name AND reverse_path.descendant_span_id = ea.ancestor_span_id AND reverse_path.ancestor_span_id = ea.descendant_span_id WHERE (ea.has_input OR ea.has_output OR ea.has_reasoning) AND ( reverse_path.ancestor_span_id IS NULL OR ea.descendant_start_time > ea.ancestor_start_time OR ( ea.descendant_start_time = ea.ancestor_start_time AND ea.descendant_span_id > ea.ancestor_span_id ) ) GROUP BY ea.trace_id, ea.service_name, ea.ancestor_span_id )",
		"IF( COALESCE(token_leaf_check.has_input, FALSE) AND ( (t.gen_ai_usage_output_tokens IS NOT NULL AND NOT COALESCE(token_leaf_check.has_output, FALSE)) OR (t.gen_ai_usage_reasoning_tokens IS NOT NULL AND NOT COALESCE(token_leaf_check.has_reasoning, FALSE)) ), CAST(NULL AS INT64), t.gen_ai_usage_input_tokens ) AS gen_ai_usage_input_tokens",
		"IF( COALESCE(token_leaf_check.has_output, FALSE) AND ( (t.gen_ai_usage_input_tokens IS NOT NULL AND NOT COALESCE(token_leaf_check.has_input, FALSE)) OR (t.gen_ai_usage_reasoning_tokens IS NOT NULL AND NOT COALESCE(token_leaf_check.has_reasoning, FALSE)) ), CAST(NULL AS INT64), t.gen_ai_usage_output_tokens ) AS gen_ai_usage_output_tokens",
		"IF( COALESCE(token_leaf_check.has_reasoning, FALSE) AND ( (t.gen_ai_usage_input_tokens IS NOT NULL AND NOT COALESCE(token_leaf_check.has_input, FALSE)) OR (t.gen_ai_usage_output_tokens IS NOT NULL AND NOT COALESCE(token_leaf_check.has_output, FALSE)) ), CAST(NULL AS INT64), t.gen_ai_usage_reasoning_tokens ) AS gen_ai_usage_reasoning_tokens",
		"ROW_NUMBER() OVER ( PARTITION BY t.trace_id, t.service_name ORDER BY t.start_time ASC, t.span_id ASC ) AS step_index",
		"( (t.gen_ai_usage_input_tokens IS NOT NULL AND NOT COALESCE(token_leaf_check.has_input, FALSE)) OR (t.gen_ai_usage_output_tokens IS NOT NULL AND NOT COALESCE(token_leaf_check.has_output, FALSE)) OR (t.gen_ai_usage_reasoning_tokens IS NOT NULL AND NOT COALESCE(token_leaf_check.has_reasoning, FALSE)) ) AS is_token_leaf",
		"LEFT JOIN parent_ids AS leaf_check ON t.trace_id = leaf_check.trace_id AND t.span_id = leaf_check.span_id AND t.service_name IS NOT DISTINCT FROM leaf_check.service_name",
		"LEFT JOIN token_parent_coverage AS token_leaf_check ON t.trace_id = token_leaf_check.trace_id AND t.span_id = token_leaf_check.span_id AND t.service_name IS NOT DISTINCT FROM token_leaf_check.service_name",
	} {
		if !strings.Contains(query, want) {
			t.Errorf("v_gen_ai_agent_trajectory query missing %q:\n%s", want, query)
		}
	}
	if n := strings.Count(query, "IS NOT DISTINCT FROM"); n != 7 {
		t.Errorf("v_gen_ai_agent_trajectory query has %d IS NOT DISTINCT FROM clauses, want 7", n)
	}
}

func TestMCPSpansAndToolUsageViews(t *testing.T) {
	genAIQuery := normalizeSQL(fmt.Sprintf(GenAISpansViewQuery, "proj", "ds", "spans"))
	_, genAIWhere, ok := strings.Cut(genAIQuery, " WHERE ")
	if !ok {
		t.Fatalf("v_gen_ai_spans query lacks WHERE clause:\n%s", genAIQuery)
	}

	mcpExprs := projectionExprs(t, MCPSpansViewQuery, "proj", "ds", "spans")
	alsoInGenAI := strings.TrimSuffix(strings.TrimPrefix(mcpExprs["also_in_gen_ai_view"], "( "), " )")
	if alsoInGenAI != genAIWhere {
		t.Errorf("v_mcp_spans.also_in_gen_ai_view =\n%s\nwant exact v_gen_ai_spans WHERE predicate:\n%s", alsoInGenAI, genAIWhere)
	}
	if got, want := mcpExprs["is_tool_call"], `COALESCE(JSON_VALUE(attributes, '$."mcp.method.name"') = 'tools/call', FALSE)`; got != want {
		t.Errorf("v_mcp_spans.is_tool_call = %q, want %q", got, want)
	}
	wantAgentExpr := `COALESCE( JSON_VALUE(attributes, '$."gen_ai.agent.name"'), JSON_VALUE(attributes, '$."gcp.vertex.agent.name"'), JSON_VALUE(attributes, '$."gen_ai.main_agent.name"') )`
	if got := mcpExprs["gen_ai_agent_name"]; got != wantAgentExpr {
		t.Errorf("v_mcp_spans.gen_ai_agent_name = %q, want %q", got, wantAgentExpr)
	}
	if got, want := attributeKeys(mcpExprs["gen_ai_agent_name"]), []string{"gen_ai.agent.name", "gcp.vertex.agent.name", "gen_ai.main_agent.name"}; !slices.Equal(got, want) {
		t.Errorf("v_mcp_spans.gen_ai_agent_name keys = %v, want %v", got, want)
	}

	mcpQuery := normalizeSQL(fmt.Sprintf(MCPSpansViewQuery, "proj", "ds", "spans"))
	if n := strings.Count(mcpQuery, " WHERE "); n != 1 {
		t.Errorf("v_mcp_spans WHERE count = %d, want 1:\n%s", n, mcpQuery)
	}
	if _, mcpWhere, ok := strings.Cut(mcpQuery, " WHERE "); !ok {
		t.Fatalf("v_mcp_spans query lacks WHERE clause:\n%s", mcpQuery)
	} else if want := `JSON_VALUE(attributes, '$."mcp.method.name"') IS NOT NULL OR JSON_VALUE(attributes, '$."mcp.session.id"') IS NOT NULL OR JSON_VALUE(attributes, '$."mcp.resource.uri"') IS NOT NULL`; mcpWhere != want {
		t.Errorf("v_mcp_spans WHERE = %q, want %q", mcpWhere, want)
	}

	usageQuery := normalizeSQL(fmt.Sprintf(MCPToolUsageViewQuery, "proj", "ds"))
	for _, want := range []string{
		"FROM `proj.ds.v_mcp_spans` GROUP BY usage_hour, service_name, mcp_method_name, mcp_protocol_version, gen_ai_tool_name, mcp_target_name, span_kind, server_address",
		"COALESCE(gen_ai_tool_name, mcp_resource_uri, gen_ai_prompt_name) AS mcp_target_name",
		"COUNTIF(also_in_gen_ai_view) AS gen_ai_tagged_call_count",
		"COUNTIF(status_code = 2 OR error_type IS NOT NULL) AS error_count",
		"APPROX_TOP_COUNT(error_type, 6) AS raw_top_error_types",
		"WHERE x.value IS NOT NULL ORDER BY x.count DESC, x.value ASC LIMIT 5",
	} {
		if !strings.Contains(usageQuery, want) {
			t.Errorf("v_mcp_tool_usage query missing %q:\n%s", want, usageQuery)
		}
	}
	if !strings.HasSuffix(usageQuery, "FROM mcp_tool_agg") {
		t.Errorf("v_mcp_tool_usage outer query must end with FROM mcp_tool_agg without an outer WHERE clause:\n%s", usageQuery)
	}
	callCountIdx := strings.Index(usageQuery, "COUNT(*) AS call_count")
	if callCountIdx < 0 {
		t.Fatalf("v_mcp_tool_usage query lacks COUNT(*) AS call_count:\n%s", usageQuery)
	}
	cteHead := usageQuery[:callCountIdx]
	prev := -1
	for _, col := range []string{
		"AS usage_hour",
		"service_name,",
		"mcp_method_name,",
		"mcp_protocol_version,",
		"gen_ai_tool_name,",
		"AS mcp_target_name",
		"span_kind,",
		"server_address,",
	} {
		idx := strings.Index(cteHead, col)
		if idx <= prev {
			t.Errorf("v_mcp_tool_usage grouping column %q does not appear in order before COUNT(*) AS call_count:\n%s", col, cteHead)
		}
		prev = idx
	}
}

// selectItem is one entry of a view's projection.
type selectItem struct {
	alias       string // output column name
	expr        string // the expression that produces it, or the column itself
	qualifier   string // table alias of a passed-through column, if any
	passthrough bool   // the entry is a bare, possibly qualified, column
}

var exceptRE = regexp.MustCompile(`^\* EXCEPT \(([a-z_]+)\)$`)

// selectList parses the projection of a query laid out as the companion views
// are: SELECT alone on a line, entries separated by top-level commas, "--"
// comment lines, and FROM at the start of the first top-level line after the
// projection. A query that opens with common table expressions has one such
// block per SELECT, and the last one is the projection of the view itself.
// When the outer SELECT starts with "* EXCEPT (col)", it expands the remaining
// columns from the immediately preceding CTE SELECT block.
func selectList(query string) ([]selectItem, error) {
	var blocks [][]string
	var lines []string
	inSelect := false
	depth := 0
	for _, line := range strings.Split(query, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}
		if depth == 0 && trimmed == "SELECT" {
			inSelect, lines = true, nil
			continue
		}
		if !inSelect {
			continue
		}
		if depth == 0 && strings.HasPrefix(trimmed, "FROM ") {
			inSelect = false
			blocks = append(blocks, lines)
			continue
		}
		lines = append(lines, trimmed)
		for i := 0; i < len(trimmed); i++ {
			switch trimmed[i] {
			case '(', '[':
				depth++
			case ')', ']':
				depth--
			}
		}
	}
	if len(blocks) == 0 {
		return nil, fmt.Errorf("no SELECT ... FROM projection in %q", query)
	}
	items, err := splitSelectList(strings.Join(blocks[len(blocks)-1], " "))
	if err != nil {
		return nil, err
	}
	if len(items) > 0 && items[0].alias == "*" && len(blocks) >= 2 {
		prev, err := splitSelectList(strings.Join(blocks[len(blocks)-2], " "))
		if err != nil {
			return nil, err
		}
		prev = slices.DeleteFunc(prev, func(it selectItem) bool { return it.alias == items[0].qualifier })
		items = slices.Concat(prev, items[1:])
	}
	return items, nil
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
	if m := exceptRE.FindStringSubmatch(entry); m != nil {
		return selectItem{alias: "*", expr: entry, qualifier: m[1]}, nil
	}
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
