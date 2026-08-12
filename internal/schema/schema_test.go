// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"fmt"
	"strings"
	"testing"
)

// The contract is v0alpha1 until at least two independent design partners
// complete representative workflows. Bumping this to v1 is a milestone gate,
// not a code change, so make it fail loudly.
func TestVersionIsAlphaBaseline(t *testing.T) {
	if Version != "v0alpha1" {
		t.Fatalf("schema Version = %q; v1 freezes only at M4", Version)
	}
}

// Every column the design doc marks REQUIRED must be NOT NULL in the DDL.
func TestRequiredColumns(t *testing.T) {
	spans := []string{
		"start_timestamp", "start_time_unix_nano", "export_received_timestamp",
		"trace_id", "span_id", "name", "schema_version",
	}
	logs := []string{
		"timestamp", "time_unix_nano", "observed_time_unix_nano",
		"export_received_timestamp", "schema_version", "record_fingerprint",
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
	if !strings.Contains(SpansDDL, "PARTITION BY DATE(start_timestamp)") {
		t.Error("spans must be day-partitioned on start_timestamp")
	}
	if !strings.Contains(LogsDDL, "PARTITION BY DATE(timestamp)") {
		t.Error("logs must be day-partitioned on timestamp")
	}
}

// Logs get no default clustering, and that decision is final rather than
// pending. Spans cluster on (service_name, name) as the M3 candidate.
func TestClustering(t *testing.T) {
	if !strings.Contains(SpansDDL, "CLUSTER BY service_name, name") {
		t.Error("spans must cluster on service_name, name")
	}
	if strings.Contains(LogsDDL, "CLUSTER BY") {
		t.Error("logs must have no default clustering")
	}
}

// Span dedup must order on the raw nanos, not the microsecond TIMESTAMP:
// end_timestamp alone can tie across retries.
func TestSpanDedupOrdersOnRawNanos(t *testing.T) {
	if !strings.Contains(SpansDedupViewDDL, "CAST(end_time_unix_nano AS BIGNUMERIC) DESC") {
		t.Error("span dedup must order on end_time_unix_nano, not end_timestamp")
	}
	if !strings.Contains(SpansDedupViewDDL, "export_received_timestamp DESC") {
		t.Error("span dedup needs the ingestion-time tie-breaker")
	}
}

// The log dedup partition must keep the two identity kinds in disjoint
// partitions and must never flatten the tuple into one string. A
// CONCAT(namespace, '/', id) form would collide ('a/b','c') with ('a','b/c')
// and mix source IDs with fingerprints in a single domain.
func TestLogDedupPartitionIsNotFlattened(t *testing.T) {
	if strings.Contains(LogsDedupViewDDL, "CONCAT(") {
		t.Fatal("log dedup must not concatenate namespace and ID into one partition key")
	}
	for _, want := range []string{
		"(source_record_namespace IS NOT NULL AND source_record_id IS NOT NULL)",
		`IFNULL(source_record_namespace, '')`,
		"record_fingerprint",
	} {
		if !strings.Contains(LogsDedupViewDDL, want) {
			t.Errorf("log dedup partition is missing %q", want)
		}
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
