// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"fmt"
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

// Every column the design doc marks REQUIRED must be NOT NULL in the DDL.
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

	for _, tc := range []struct {
		name string
		ddl  string
		cols []string
	}{
		{"spans", SpansDDL, spans},
		{"logs", LogsDDL, logs},
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
func TestPartitioning(t *testing.T) {
	if !strings.Contains(SpansDDL, "PARTITION BY TIMESTAMP_TRUNC(start_time, HOUR)") {
		t.Error("spans must be hour-partitioned on start_time")
	}
	if !strings.Contains(LogsDDL, "PARTITION BY DATE(timestamp)") {
		t.Error("logs must be day-partitioned on timestamp")
	}
}

// Spans cluster on (service_name, trace_id, name), so per-service scans and
// whole-trace lookups both prune. Logs cluster on (service_name, trace_id,
// severity_number), so per-service scans, trace correlation and severity
// filters prune.
func TestClustering(t *testing.T) {
	if !strings.Contains(SpansDDL, "CLUSTER BY service_name, trace_id, name;") {
		t.Error("spans must cluster on service_name, trace_id, name")
	}
	if !strings.Contains(LogsDDL, "CLUSTER BY service_name, trace_id, severity_number;") {
		t.Error("logs must cluster on service_name, trace_id, severity_number")
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
		"spans":      SpansDDL,
		"logs":       LogsDDL,
		"spans_view": SpansDedupViewDDL,
		"logs_view":  LogsDedupViewDDL,
	} {
		got := fmt.Sprintf(tmpl, "proj", "ds", "tbl")
		if strings.Contains(got, "%!") || strings.Contains(got, "MISSING") {
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

// parseDDL reads the column list of a CREATE TABLE statement laid out one
// field per line, as SpansDDL and LogsDDL are. It accepts exactly the shapes
// those DDLs use: "name TYPE" with an optional NOT NULL, "name STRUCT<" and
// "name ARRAY<STRUCT<" openers, and lines that start with the matching ">"
// closers.
func parseDDL(ddl string) (*storagepb.TableSchema, error) {
	types := map[string]storagepb.TableFieldSchema_Type{
		"STRING":    storagepb.TableFieldSchema_STRING,
		"INT64":     storagepb.TableFieldSchema_INT64,
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
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "CREATE TABLE"):
			inColumns = true
			continue
		case !inColumns || line == "" || strings.HasPrefix(line, "--"):
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

// ddlColumns extracts top-level column names, skipping the nested STRUCT
// fields of the events and links arrays.
func ddlColumns(ddl string) map[string]bool {
	out := map[string]bool{}
	depth := 0
	for _, line := range strings.Split(ddl, "\n") {
		trimmed := strings.TrimSpace(line)
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
		if !(r == '_' || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')) {
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
