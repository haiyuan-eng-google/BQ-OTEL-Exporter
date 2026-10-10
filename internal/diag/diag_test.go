// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package diag

import (
	"strings"
	"testing"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
)

// The core FR13 property: nothing derived from telemetry content reaches a
// diagnostic. These are the strings an operator is most tempted to log and
// exactly the ones that must never appear.
func TestSanitizeDropsTelemetryContent(t *testing.T) {
	secrets := []string{
		"4bf92f3577b34da6a3ce929d0e0e4736",     // a trace ID
		"00f067aa0ba902b7",                     // a span ID
		"fp1:9f86d081884c7d659a2feaa0c55ad015", // a record fingerprint
		"user@example.com",                     // an attribute value
		"Bearer sk-abc123",                     // a credential in a body
		"customer-order-99182",                 // an upstream insert ID
	}

	for _, secret := range secrets {
		msg := "append failed for field body with value " + secret
		got := SanitizeFieldPath(msg)
		if strings.Contains(got, secret) {
			t.Errorf("sanitized output leaked %q: %q", secret, got)
		}
	}
}

// Sanitizing must still leave something useful: the column names involved.
func TestSanitizeKeepsColumnNames(t *testing.T) {
	got := SanitizeFieldPath("Invalid value for field attributes_schema in row")
	if got != "attributes_schema" {
		t.Fatalf("got %q, want attributes_schema", got)
	}
}

// The allowlist must track the table contracts exactly: a missing name would
// drop a location an operator needs, and an extra one is a name no column has.
func TestKnownFieldTokensMatchTheContract(t *testing.T) {
	want := map[string]bool{}
	var walk func([]*storagepb.TableFieldSchema)
	walk = func(fields []*storagepb.TableFieldSchema) {
		for _, f := range fields {
			want[f.GetName()] = true
			walk(f.GetFields())
		}
	}
	walk(schema.SpansTableSchema().GetFields())
	walk(schema.LogsTableSchema().GetFields())

	for name := range want {
		if !knownFieldTokens[name] {
			t.Errorf("contract field %q is missing from knownFieldTokens", name)
		}
	}
	for name := range knownFieldTokens {
		if !want[name] {
			t.Errorf("knownFieldTokens has %q, which names no contract field", name)
		}
	}
}

func TestSanitizeMultipleColumns(t *testing.T) {
	got := SanitizeFieldPath("mismatch between trace_id and span_id")
	if !strings.Contains(got, "trace_id") || !strings.Contains(got, "span_id") {
		t.Fatalf("got %q, want both column names", got)
	}
}

// v0alpha2 nests fields in STRUCT columns, and a service message names one by
// its dotted path. Each part must be recognized, or the diagnostic would lose
// the location an operator needs.
func TestSanitizeKeepsNestedFieldPaths(t *testing.T) {
	for msg, want := range map[string]string{
		"Invalid value for field status.code_name":                         "status,code_name",
		"field resource.attributes_schema is not valid JSON":               "resource,attributes_schema",
		"instrumentation_scope.version exceeds the maximum length":         "instrumentation_scope,version",
		"events.time out of range in row":                                  "events,time",
		"payload_refs.object_ref.uri is required":                          "payload_refs,object_ref,uri",
		"bad start_time, end_time or receive_time_unix_nano duration_nano": "start_time,end_time,receive_time_unix_nano,duration_nano",
	} {
		if got := SanitizeFieldPath(msg); got != want {
			t.Errorf("SanitizeFieldPath(%q) = %q, want %q", msg, got, want)
		}
	}
}

// An unrecognized message must degrade to "unknown" rather than passing
// through. Allowlisting is what makes the guarantee hold by construction.
func TestSanitizeUnknownMessage(t *testing.T) {
	if got := SanitizeFieldPath("something entirely unexpected happened"); got != "unknown" {
		t.Fatalf("got %q, want unknown", got)
	}
	if got := SanitizeFieldPath(""); got != "" {
		t.Fatalf("empty message should stay empty, got %q", got)
	}
}

// The operation ID correlates log lines within one process. It must be random
// rather than derived: a content hash would be a stable cross-system
// correlator, which is the thing FR13 prohibits.
func TestOperationIDIsRandom(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := OperationID()
		if len(id) != 16 {
			t.Fatalf("operation ID %q is not 16 hex chars", id)
		}
		if seen[id] {
			t.Fatalf("operation ID %q repeated within 1000 draws", id)
		}
		seen[id] = true
	}
}

func TestRowErrorFieldsCarryNoContent(t *testing.T) {
	fields := RowError("p.d.t", "abcd1234", 7, "FIELDS_ERROR", "bad value 'secret-token' in field body")
	for _, f := range fields {
		if strings.Contains(f.String, "secret-token") {
			t.Fatalf("row error diagnostic leaked a value: %+v", f)
		}
	}
}
