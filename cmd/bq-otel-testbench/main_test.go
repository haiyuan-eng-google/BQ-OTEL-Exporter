// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/testbench"
)

func TestCLIRunBQFakeAndJSONOut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	jsonPath := filepath.Join(t.TempDir(), "report.json")
	var stdout, stderr bytes.Buffer

	err := run(ctx, []string{
		"-mode=bqfake",
		"-signals=traces,logs",
		"-duration=200ms",
		"-batch-size=40",
		"-workers=2",
		"-stream-pool-size=2",
		"-redaction=true",
		"-format=markdown",
		"-json-out=" + jsonPath,
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run() unexpected error: %v (stderr: %s)", err, stderr.String())
	}

	outStr := stdout.String()
	if !strings.Contains(outStr, "| `traces` |") || !strings.Contains(outStr, "| `logs` |") {
		t.Fatalf("stdout missing expected markdown table rows:\n%s", outStr)
	}

	raw, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatalf("reading json-out file: %v", err)
	}
	var report testbench.BenchmarkReport
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatalf("unmarshaling json-out: %v", err)
	}
	if len(report.Results) != 2 {
		t.Fatalf("len(report.Results) = %d, want 2", len(report.Results))
	}

	// Regression check for small -payload-bytes values (e.g. 64 bytes in [11, 105])
	// with -unique-payloads and -redaction enabled.
	stdout.Reset()
	stderr.Reset()
	if err := run(ctx, []string{
		"-mode=bqfake",
		"-signals=traces,logs",
		"-duration=100ms",
		"-batch-size=20",
		"-workers=1",
		"-stream-pool-size=1",
		"-payload-bytes=64",
		"-unique-payloads=true",
		"-redaction=true",
		"-format=json",
	}, &stdout, &stderr); err != nil {
		t.Fatalf("run(-payload-bytes=64) unexpected error: %v (stderr: %s)", err, stderr.String())
	}
}

func TestCLISweepAndFlagValidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var stdout, stderr bytes.Buffer
	err := run(ctx, []string{
		"-mode=bqfake",
		"-signals=traces",
		"-duration=150ms",
		"-batch-size=32",
		"-workers=2",
		"-stream-pool-sweep=1,2",
		"-format=json",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run(-stream-pool-sweep=1,2) error: %v", err)
	}

	var report testbench.BenchmarkReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("unmarshaling json stdout: %v", err)
	}
	if len(report.Results) != 2 {
		t.Fatalf("len(report.Results) = %d, want 2", len(report.Results))
	}

	badFlagCases := [][]string{
		{"-format=xml"},
		{"-stream-pool-sweep=0,abc"},
		{"unexpected-positional-arg"},
	}
	for _, args := range badFlagCases {
		stdout.Reset()
		stderr.Reset()
		if err := run(ctx, args, &stdout, &stderr); err == nil {
			t.Errorf("run(%v) succeeded, want error", args)
		}
	}
}
