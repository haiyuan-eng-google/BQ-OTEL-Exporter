// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package transform

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/redact"
)

// The secret below is long enough to cross the fake offloader's threshold on
// its own and distinctive enough that a substring search finds any leak.
const (
	leakedSecret  = "sk-live-0123456789abcdef0123456789abcdef0123456789abcdef"
	offloadCutoff = 64
	redactedMark  = "[REDACTED]"
)

// contentOffloader mimics the real offloader's contract closely enough to
// prove ordering: any string value longer than offloadCutoff is "uploaded"
// (recorded by the SHA-256 of its bytes, exactly as the GCS object key is
// derived) and replaced inline by a reference.
type contentOffloader struct {
	uploads map[string][]byte // sha256 hex -> uploaded bytes
}

func newContentOffloader() *contentOffloader {
	return &contentOffloader{uploads: map[string][]byte{}}
}

func (c *contentOffloader) offloadMap(m pcommon.Map, path string) []PayloadRef {
	var refs []PayloadRef
	for k, v := range m.All() {
		if v.Type() != pcommon.ValueTypeStr || len(v.Str()) <= offloadCutoff {
			continue
		}
		data := []byte(v.Str())
		sum := sha256.Sum256(data)
		key := hex.EncodeToString(sum[:])
		c.uploads[key] = append([]byte(nil), data...)
		v.SetStr("[offloaded]")
		refs = append(refs, PayloadRef{
			AttributePath: path + "." + k,
			ObjectRef:     ObjectRef{URI: "gs://bucket/" + key},
		})
	}
	return refs
}

func (c *contentOffloader) OffloadSpan(_ context.Context, span ptrace.Span, res pcommon.Resource, scope pcommon.InstrumentationScope) ([]PayloadRef, error) {
	var refs []PayloadRef
	refs = append(refs, c.offloadMap(res.Attributes(), "resource.attributes")...)
	refs = append(refs, c.offloadMap(scope.Attributes(), "instrumentation_scope.attributes")...)
	refs = append(refs, c.offloadMap(span.Attributes(), "attributes")...)
	return refs, nil
}

func (c *contentOffloader) OffloadLog(_ context.Context, lr plog.LogRecord, res pcommon.Resource, scope pcommon.InstrumentationScope) ([]PayloadRef, error) {
	var refs []PayloadRef
	refs = append(refs, c.offloadMap(res.Attributes(), "resource.attributes")...)
	refs = append(refs, c.offloadMap(scope.Attributes(), "instrumentation_scope.attributes")...)
	refs = append(refs, c.offloadMap(lr.Attributes(), "attributes")...)
	if lr.Body().Type() == pcommon.ValueTypeStr && len(lr.Body().Str()) > offloadCutoff {
		data := []byte(lr.Body().Str())
		sum := sha256.Sum256(data)
		key := hex.EncodeToString(sum[:])
		c.uploads[key] = append([]byte(nil), data...)
		lr.Body().SetStr("[offloaded]")
		refs = append(refs, PayloadRef{AttributePath: "body", ObjectRef: ObjectRef{URI: "gs://bucket/" + key}})
	}
	return refs, nil
}

func testRedactor(t *testing.T, logBody bool) *redact.Redactor {
	t.Helper()
	r, err := redact.New(redact.Config{
		BlockedKeys:          redact.DefaultBlockedKeys(),
		BlockedValuePatterns: []string{`sk-live-[0-9a-f]+`},
		MaskReplacement:      redactedMark,
		RedactLogBody:        logBody,
	})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// flattenRow renders every string and JSON column of a row (recursively
// through nested rows) so a substring search finds a leak anywhere.
func flattenRow(row Row) string {
	var b strings.Builder
	var walk func(v any)
	walk = func(v any) {
		switch x := v.(type) {
		case Row:
			for _, k := range slices.Sorted(maps.Keys(x)) {
				b.WriteString(k)
				b.WriteByte('=')
				walk(x[k])
				b.WriteByte(';')
			}
		case []Row:
			for _, r := range x {
				walk(r)
			}
		case []byte:
			b.Write(x)
		default:
			fmt.Fprintf(&b, "%v", x)
		}
	}
	walk(row)
	return b.String()
}

func marshalTracesJSON(t *testing.T, td ptrace.Traces) string {
	t.Helper()
	b, err := (&ptrace.JSONMarshaler{}).MarshalTraces(td)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func marshalLogsJSON(t *testing.T, ld plog.Logs) string {
	t.Helper()
	b, err := (&plog.JSONMarshaler{}).MarshalLogs(ld)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func marshalMetricsJSON(t *testing.T, md pmetric.Metrics) string {
	t.Helper()
	b, err := (&pmetric.JSONMarshaler{}).MarshalMetrics(md)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TestRedactionRunsBeforeOffloadAndFingerprint is the ordering gate for the
// redaction step. Each sub-test fails if redaction were moved after the
// stage it names.
func TestRedactionRunsBeforeOffloadAndFingerprint(t *testing.T) {
	t.Run("spans: offload sees redacted bytes and the object key hashes them", func(t *testing.T) {
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", "checkout")
		span := validSpan(rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty())
		span.SetName("call_llm")
		// Oversized value whose middle carries a secret: value-pattern
		// redaction shrinks it but it stays above the offload cutoff, so the
		// offloader must receive the redacted form.
		prompt := strings.Repeat("prompt ", 20) + leakedSecret + strings.Repeat(" tail", 20)
		span.Attributes().PutStr("gen_ai.prompt", prompt)
		// Oversized value under a blocked key: key redaction collapses it to
		// the mask, which is below the cutoff, so it is never uploaded at all.
		span.Attributes().PutStr("authorization", "Bearer "+strings.Repeat("x", 200))
		before := marshalTracesJSON(t, td)

		off := newContentOffloader()
		opts := testOpts()
		opts.PayloadOffloader = off
		opts.Redactor = testRedactor(t, false)

		rows, rejects := SpanRows(td, opts)
		if len(rows) != 1 || len(rejects) != 0 {
			t.Fatalf("rows=%d rejects=%+v, want 1 row", len(rows), rejects)
		}
		if after := marshalTracesJSON(t, td); after != before {
			t.Fatal("caller's ptrace.Traces was mutated by SpanRows")
		}

		wantUploaded := strings.ReplaceAll(prompt, leakedSecret, redactedMark)
		wantSum := sha256.Sum256([]byte(wantUploaded))
		wantKey := hex.EncodeToString(wantSum[:])
		if len(off.uploads) != 1 {
			t.Fatalf("uploads = %d, want exactly the redacted prompt (authorization must shrink below the cutoff)", len(off.uploads))
		}
		got, ok := off.uploads[wantKey]
		if !ok {
			t.Fatalf("object key was not the SHA-256 of the redacted bytes; have keys %v", keysOf(off.uploads))
		}
		if strings.Contains(string(got), leakedSecret) || !strings.Contains(string(got), redactedMark) {
			t.Fatalf("uploaded bytes leak the secret: %q", got)
		}
		refs, _ := rows[0]["payload_refs"].([]Row)
		var gotURI any
		if len(refs) == 1 {
			if ref, ok := refs[0]["object_ref"].(Row); ok {
				gotURI = ref["uri"]
			}
		}
		if gotURI != "gs://bucket/"+wantKey {
			t.Fatalf("payload_refs = %#v, want one ref keyed by the redacted digest", rows[0]["payload_refs"])
		}
		flat := flattenRow(rows[0])
		if strings.Contains(flat, leakedSecret) || strings.Contains(flat, "Bearer") {
			t.Fatalf("row leaks cleartext: %s", flat)
		}
	})

	t.Run("spans: key redaction shrinks an oversized value below max_value_bytes", func(t *testing.T) {
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", "checkout")
		span := validSpan(rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty())
		span.Attributes().PutStr("api_key", strings.Repeat("k", 300))

		opts := testOpts()
		opts.Limits.MaxValueBytes = 128

		if _, rejects := SpanRows(td, opts); len(rejects) != 1 || rejects[0].Reason != ReasonValueSize {
			t.Fatalf("without redaction the span must be rejected for value_size, got %+v", rejects)
		}
		opts.Redactor = testRedactor(t, false)
		rows, rejects := SpanRows(td, opts)
		if len(rows) != 1 || len(rejects) != 0 {
			t.Fatalf("with redaction before limits the span must pass: rows=%d rejects=%+v", len(rows), rejects)
		}
		if got := asString(rows[0]["attributes"]); !strings.Contains(got, redactedMark) || strings.Contains(got, "kkk") {
			t.Fatalf("attributes = %s", got)
		}
	})

	t.Run("logs: record_fingerprint and insert_id derive from the redacted record", func(t *testing.T) {
		build := func() plog.Logs {
			ld := plog.NewLogs()
			rl := ld.ResourceLogs().AppendEmpty()
			rl.Resource().Attributes().PutStr("service.name", "checkout")
			lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
			lr.SetTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000))
			lr.Attributes().PutStr("session_token", "tok-123")
			lr.Attributes().PutStr("note", "value "+leakedSecret)
			lr.Body().SetStr("body with " + leakedSecret + " inside")
			return ld
		}
		ld := build()
		before := marshalLogsJSON(t, ld)

		opts := testOpts()
		plain, _ := LogRows(build(), opts)

		opts.Redactor = testRedactor(t, true)
		rows, rejects := LogRows(ld, opts)
		if len(rows) != 1 || len(rejects) != 0 {
			t.Fatalf("rows=%d rejects=%+v", len(rows), rejects)
		}
		if marshalLogsJSON(t, ld) != before {
			t.Fatal("caller's plog.Logs was mutated by LogRows")
		}

		// Expected identity: what LogRows computes for a batch that was
		// already redacted before it arrived. If redaction ran after the
		// fingerprint, the row would instead carry plain's fingerprint.
		expectedInput := build()
		opts.Redactor.RedactLogs(expectedInput)
		expected, _ := LogRows(expectedInput, testOpts())

		if rows[0]["record_fingerprint"] != expected[0]["record_fingerprint"] {
			t.Fatalf("record_fingerprint = %v, want %v (fingerprint of the redacted record)",
				rows[0]["record_fingerprint"], expected[0]["record_fingerprint"])
		}
		if rows[0]["record_fingerprint"] == plain[0]["record_fingerprint"] {
			t.Fatal("record_fingerprint equals the cleartext fingerprint: redaction ran after hashing")
		}
		if rows[0]["insert_id"] != expected[0]["insert_id"] {
			t.Fatalf("insert_id = %v, want %v", rows[0]["insert_id"], expected[0]["insert_id"])
		}
		flat := flattenRow(rows[0])
		if strings.Contains(flat, leakedSecret) || strings.Contains(flat, "tok-123") {
			t.Fatalf("row leaks cleartext: %s", flat)
		}
		if body := asString(rows[0]["body"]); !strings.Contains(body, redactedMark) {
			t.Fatalf("body = %s, want the mask (redact_log_body)", body)
		}
	})

	t.Run("logs: offload receives the redacted body", func(t *testing.T) {
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", "checkout")
		lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		lr.SetTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000))
		body := strings.Repeat("log ", 30) + leakedSecret
		lr.Body().SetStr(body)

		off := newContentOffloader()
		opts := testOpts()
		opts.PayloadOffloader = off
		opts.Redactor = testRedactor(t, true)
		rows, rejects := LogRows(ld, opts)
		if len(rows) != 1 || len(rejects) != 0 {
			t.Fatalf("rows=%d rejects=%+v", len(rows), rejects)
		}
		wantSum := sha256.Sum256([]byte(strings.ReplaceAll(body, leakedSecret, redactedMark)))
		if _, ok := off.uploads[hex.EncodeToString(wantSum[:])]; !ok || len(off.uploads) != 1 {
			t.Fatalf("uploaded body was not the redacted one; keys %v", keysOf(off.uploads))
		}
	})

	t.Run("metrics: point_fingerprint derives from the redacted point", func(t *testing.T) {
		build := func() pmetric.Metrics {
			md := pmetric.NewMetrics()
			rm := md.ResourceMetrics().AppendEmpty()
			rm.Resource().Attributes().PutStr("service.name", "checkout")
			m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			m.SetName("gen_ai.client.token.usage")
			m.Metadata().PutStr("client_secret", "cs")
			dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
			dp.SetTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000))
			dp.SetIntValue(7)
			dp.Attributes().PutStr("access_token", "at")
			dp.Attributes().PutStr("note", leakedSecret)
			dp.Exemplars().AppendEmpty().FilteredAttributes().PutStr("password", "pw")
			return md
		}
		md := build()
		before := marshalMetricsJSON(t, md)
		plain, _ := MetricRows(build(), testOpts())

		opts := testOpts()
		opts.Redactor = testRedactor(t, false)
		rows, rejects := MetricRows(md, opts)
		if len(rows) != 1 || len(rejects) != 0 {
			t.Fatalf("rows=%d rejects=%+v", len(rows), rejects)
		}
		if marshalMetricsJSON(t, md) != before {
			t.Fatal("caller's pmetric.Metrics was mutated by MetricRows")
		}
		expectedInput := build()
		opts.Redactor.RedactMetrics(expectedInput)
		expected, _ := MetricRows(expectedInput, testOpts())
		if rows[0]["point_fingerprint"] != expected[0]["point_fingerprint"] {
			t.Fatalf("point_fingerprint = %v, want %v", rows[0]["point_fingerprint"], expected[0]["point_fingerprint"])
		}
		if rows[0]["point_fingerprint"] == plain[0]["point_fingerprint"] {
			t.Fatal("point_fingerprint equals the cleartext fingerprint: redaction ran after hashing")
		}
		flat := flattenRow(rows[0])
		for _, leak := range []string{leakedSecret, `"at"`, `"cs"`, `"pw"`} {
			if strings.Contains(flat, leak) {
				t.Fatalf("row leaks %q: %s", leak, flat)
			}
		}
	})

	t.Run("quarantine: raw_payload and raw_attributes are redacted for all three signals", func(t *testing.T) {
		opts := quarantineOpts()
		opts.Redactor = testRedactor(t, true)

		// Spans: an invalid trace ID forces rejection.
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", "checkout")
		rs.Resource().Attributes().PutStr("cloud.account.password", "acct-pw")
		sp := validSpan(rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty())
		sp.SetTraceID(pcommon.NewTraceIDEmpty())
		sp.Attributes().PutStr("authorization", "Bearer abc")
		sp.Attributes().PutStr("note", leakedSecret)
		sp.Events().AppendEmpty().Attributes().PutStr("x-api-key", "ev")
		tdBefore := marshalTracesJSON(t, td)
		_, spanRejects := SpanRows(td, opts)
		spanQ := SpanQuarantineRows(td, entriesFor(spanRejects), opts)
		if len(spanQ) != 1 {
			t.Fatalf("span quarantine rows = %d", len(spanQ))
		}
		if marshalTracesJSON(t, td) != tdBefore {
			t.Fatal("SpanQuarantineRows mutated the caller's batch")
		}
		assertQuarantineRedacted(t, "spans", spanQ[0], []string{leakedSecret, "Bearer abc", "acct-pw", `"ev"`})

		// Logs: a record with no timestamps still produces a row, so force a
		// rejection through a structural limit instead.
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", "checkout")
		lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		lr.SetTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000))
		lr.Attributes().PutStr("refresh_token", "rt")
		lr.Body().SetStr("body " + leakedSecret)
		for i := 0; i < opts.Limits.MaxAttributeCount+1; i++ {
			lr.Attributes().PutStr("k"+strconv.Itoa(i), "v")
		}
		_, logRejects := LogRows(ld, opts)
		if len(logRejects) != 1 {
			t.Fatalf("expected one log rejection, got %+v", logRejects)
		}
		logQ := LogQuarantineRows(ld, entriesFor(logRejects), opts)
		if len(logQ) != 1 {
			t.Fatalf("log quarantine rows = %d", len(logQ))
		}
		assertQuarantineRedacted(t, "logs", logQ[0], []string{leakedSecret, `"rt"`})

		// Metrics: a NaN-free but attribute-overflowing point.
		md := pmetric.NewMetrics()
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("service.name", "checkout")
		m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		m.SetName("g")
		m.Metadata().PutStr("id_token", "idt")
		dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000))
		dp.SetIntValue(1)
		dp.Attributes().PutStr("private_key", "pk")
		dp.Attributes().PutStr("note", leakedSecret)
		for i := 0; i < opts.Limits.MaxAttributeCount+1; i++ {
			dp.Attributes().PutStr("k"+strconv.Itoa(i), "v")
		}
		_, metricRejects := MetricRows(md, opts)
		if len(metricRejects) != 1 {
			t.Fatalf("expected one metric rejection, got %+v", metricRejects)
		}
		metricQ := MetricQuarantineRows(md, entriesFor(metricRejects), opts)
		if len(metricQ) != 1 {
			t.Fatalf("metric quarantine rows = %d", len(metricQ))
		}
		assertQuarantineRedacted(t, "metrics", metricQ[0], []string{leakedSecret, `"pk"`, `"idt"`})
	})

	t.Run("quarantine: the offloaded payload is the redacted one", func(t *testing.T) {
		opts := quarantineOpts()
		opts.Redactor = testRedactor(t, false)
		var uploaded []byte
		opts.QuarantinePayloadOffloader = func(_ context.Context, _ string, payload []byte) ObjectRef {
			uploaded = append([]byte(nil), payload...)
			return ObjectRef{URI: "gs://bucket/q.json"}
		}
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", "checkout")
		sp := validSpan(rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty())
		sp.SetTraceID(pcommon.NewTraceIDEmpty())
		sp.Attributes().PutStr("password", "hunter2")
		_, rejects := SpanRows(td, opts)
		rows := SpanQuarantineRows(td, entriesFor(rejects), opts)
		if len(rows) != 1 || uploaded == nil {
			t.Fatalf("rows=%d uploaded=%v", len(rows), uploaded != nil)
		}
		if strings.Contains(string(uploaded), "hunter2") || !strings.Contains(string(uploaded), redactedMark) {
			t.Fatalf("quarantine offload received cleartext: %s", uploaded)
		}
	})
}

func assertQuarantineRedacted(t *testing.T, signal string, row Row, leaks []string) {
	t.Helper()
	if row["signal"] != signal {
		t.Fatalf("signal = %v, want %s", row["signal"], signal)
	}
	payload, _ := row["raw_payload"].(string)
	if payload == "" {
		t.Fatalf("%s: raw_payload is empty", signal)
	}
	attrs, _ := row["raw_attributes"].(string)
	for _, leak := range leaks {
		if strings.Contains(payload, leak) {
			t.Errorf("%s raw_payload leaks %q: %s", signal, leak, payload)
		}
		if strings.Contains(attrs, leak) {
			t.Errorf("%s raw_attributes leaks %q: %s", signal, leak, attrs)
		}
	}
	if !strings.Contains(payload, redactedMark) {
		t.Errorf("%s raw_payload carries no mask: %s", signal, payload)
	}
}

// asString renders a row column that may be a string or JSON bytes.
func asString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprintf("%v", x)
	}
}

func keysOf(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestRedactionNilRedactorIsZeroCost pins that an unset Redactor leaves the
// pipeline byte-for-byte identical, so enabling the option is the only way
// row content can change.
func TestRedactionNilRedactorIsZeroCost(t *testing.T) {
	td, _ := newTraces()
	with, _ := SpanRows(td, testOpts())
	opts := testOpts()
	opts.Redactor = nil
	without, _ := SpanRows(td, opts)
	if flattenRow(with[0]) != flattenRow(without[0]) {
		t.Fatal("nil Redactor changed row content")
	}
}
