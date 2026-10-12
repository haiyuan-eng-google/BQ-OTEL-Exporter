// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"fmt"
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
// partitioning and clustering columns so a filter on them still prunes when
// pushed below the window function.
func TestSpanDedupWindow(t *testing.T) {
	query := strings.Join(strings.Fields(SpansDedupViewQuery), " ")
	want := "QUALIFY ROW_NUMBER() OVER ( " +
		"PARTITION BY start_time, service_name, trace_id, span_id " +
		"ORDER BY end_time_unix_nano DESC, receive_time_unix_nano DESC ) = 1"
	if !strings.Contains(query, want) {
		t.Errorf("span dedup window = %q, want it to contain %q", query, want)
	}
}

// The log dedup window must partition on the full record identity as
// separate columns, led by the partitioning and clustering columns so a
// filter on them still prunes when pushed below the window function. It must
// never flatten the identity into one string: CONCAT(log_name, '/', id) would
// collide ('a/b', 'c') with ('a', 'b/c').
func TestLogDedupWindow(t *testing.T) {
	if strings.Contains(LogsDedupViewDDL, "CONCAT(") {
		t.Fatal("log dedup must not concatenate identity columns into one partition key")
	}
	query := strings.Join(strings.Fields(LogsDedupViewQuery), " ")
	want := "QUALIFY ROW_NUMBER() OVER ( " +
		"PARTITION BY timestamp, time_unix_nano, service_name, log_name, insert_id " +
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
				"PARTITION BY start_time, service_name, trace_id, span_id " +
				"ORDER BY end_time_unix_nano DESC, receive_time_unix_nano DESC ) = 1",
		},
		{
			name:  "logs",
			query: LogsDedupViewQuery,
			ddl:   LogsDedupViewDDL,
			table: "otel_logs",
			wantQuery: "SELECT * FROM `proj.ds.otel_logs` QUALIFY ROW_NUMBER() OVER ( " +
				"PARTITION BY timestamp, time_unix_nano, service_name, log_name, insert_id " +
				"ORDER BY receive_time_unix_nano DESC ) = 1",
		},
		{
			name:  "metrics",
			query: MetricsDedupViewQuery,
			ddl:   MetricsDedupViewDDL,
			table: "otel_metrics",
			wantQuery: "SELECT * FROM `proj.ds.otel_metrics` QUALIFY ROW_NUMBER() OVER ( " +
				"PARTITION BY timestamp, time_unix_nano, service_name, metric_name, point_fingerprint " +
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
				"LEFT JOIN `proj.ds.spans_tbl_dedup` s USING (service_name, trace_id, span_id)",
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

// The companion views are published layouts that queries pin against, so
// compare every projected column, in order. A column passed through unchanged
// must exist in the contract of the table it comes from, or BigQuery rejects
// the view when it is created.
func TestCompanionViewsProjectPublishedColumns(t *testing.T) {
	for _, tc := range []struct {
		name      string
		query     string
		args      []any
		contract  *storagepb.TableSchema
		qualifier string // table alias every passed-through column must use
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
			// kept for anything not promoted, then the 15 OTel GenAI
			// v1.37+/v1.40+ promoted columns.
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			items, err := selectList(fmt.Sprintf(tc.query, tc.args...))
			if err != nil {
				t.Fatal(err)
			}
			inContract := map[string]bool{}
			for _, name := range FieldNames(tc.contract) {
				inContract[name] = true
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
				if !inContract[item.alias] {
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
	qualifier   string // table alias of a passed-through column, if any
	passthrough bool   // the entry is a bare, possibly qualified, column
}

// selectList parses the projection of a query laid out as the companion views
// are: SELECT alone on a line, entries separated by top-level commas, "--"
// comment lines, and FROM at the start of the first line after the
// projection. An entry is a column, optionally qualified, or an expression
// ending in "AS alias".
func selectList(query string) ([]selectItem, error) {
	var lines []string
	inSelect := false
	for _, line := range strings.Split(query, "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case trimmed == "SELECT":
			inSelect = true
		case !inSelect || strings.HasPrefix(trimmed, "--"):
		case strings.HasPrefix(trimmed, "FROM "):
			return splitSelectList(strings.Join(lines, " "))
		default:
			lines = append(lines, trimmed)
		}
	}
	return nil, fmt.Errorf("no SELECT ... FROM projection in %q", query)
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
			return selectItem{alias: alias}, nil
		}
	}
	qualifier, column, qualified := strings.Cut(entry, ".")
	if !qualified {
		qualifier, column = "", entry
	}
	if !isColumnName(column) || (qualified && !isColumnName(qualifier)) {
		return selectItem{}, fmt.Errorf("projection entry %q is neither a column nor an aliased expression", entry)
	}
	return selectItem{alias: column, qualifier: qualifier, passthrough: true}, nil
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
