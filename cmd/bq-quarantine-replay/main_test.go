// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestCLIRun(t *testing.T) {
	td := ptrace.NewTraces()
	sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetName("cli-span")
	sp.SetTraceID(pcommon.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	sp.SetSpanID(pcommon.SpanID{1, 2, 3, 4, 5, 6, 7, 8})
	raw, err := (&ptrace.JSONMarshaler{}).MarshalTraces(td)
	if err != nil {
		t.Fatal(err)
	}

	tmpDir := t.TempDir()
	inPath := filepath.Join(tmpDir, "quarantine.jsonl")
	outPath := filepath.Join(tmpDir, "replayed.jsonl")
	line := `{"ingest_time":"2026-10-11T10:00:00Z","signal":"spans","error_code":"OVERSIZE_ROW","raw_payload":` +
		strconvQuote(string(raw)) + "}\n"
	if err := os.WriteFile(inPath, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	code := run([]string{
		"-input", inPath,
		"-output", outPath,
		"-signal", "spans",
		"-error-code", "OVERSIZE_ROW,ROW_ERROR",
		"-since", "2026-10-11T09:00:00Z",
		"-until", "2026-10-11T11:00:00Z",
	}, strings.NewReader(""), &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run() = %d, stderr=%s", code, stderr.String())
	}
	outBytes, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(outBytes), `"signal":"spans"`) || !strings.Contains(string(outBytes), "cli-span") {
		t.Errorf("replayed output = %s", string(outBytes))
	}

	// Dry-run from stdin
	stdout.Reset()
	stderr.Reset()
	code = run([]string{"-dry-run"}, strings.NewReader(line), &stdout, &stderr)
	if code != 0 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "replayed_spans=1") {
		t.Errorf("dry-run code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}

	// Invalid timestamp flag returns 2
	if got := run([]string{"-since", "bad-time"}, strings.NewReader(""), &stdout, &stderr); got != 2 {
		t.Errorf("bad -since flag returned %d, want 2", got)
	}
}

func strconvQuote(s string) string {
	var b bytes.Buffer
	b.WriteByte('"')
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '"', '\\':
			b.WriteByte('\\')
			b.WriteByte(s[i])
		case '\n':
			b.WriteString(`\n`)
		default:
			b.WriteByte(s[i])
		}
	}
	b.WriteByte('"')
	return b.String()
}
