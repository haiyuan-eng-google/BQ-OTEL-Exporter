// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package example_test

import (
	"os"
	"strings"
	"testing"
)

func TestCustomCollectorExampleIsSelfContainedAndPinned(t *testing.T) {
	tests := []struct {
		path       string
		contains   []string
		notContain []string
	}{
		{
			path: "otelcol-builder.yaml",
			contains: []string{
				"name: bigqueryexporter",
				"import: github.com/haiyuan-eng-google/BQ-OTEL-Exporter",
				"path: .",
				"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/fileexporter v0.158.0",
				"go.opentelemetry.io/collector/receiver/otlpreceiver v0.158.0",
			},
		},
		{
			path: "Dockerfile",
			contains: []string{
				"golang:1.26.5-bookworm@sha256:",
				"gcr.io/distroless/base-debian12:nonroot@sha256:",
			},
			notContain: []string{"latest"},
		},
		{
			path: "docker-compose.yml",
			contains: []string{
				"dockerfile: example/Dockerfile",
				"BQ_ADC_PATH",
			},
			notContain: []string{"otel/opentelemetry-collector-contrib", "latest"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			raw, err := os.ReadFile(tc.path)
			if err != nil {
				t.Fatal(err)
			}
			text := string(raw)
			for _, want := range tc.contains {
				if !strings.Contains(text, want) {
					t.Errorf("%s does not contain %q", tc.path, want)
				}
			}
			for _, forbidden := range tc.notContain {
				if strings.Contains(text, forbidden) {
					t.Errorf("%s contains forbidden unpinned/stock reference %q", tc.path, forbidden)
				}
			}
		})
	}
}
