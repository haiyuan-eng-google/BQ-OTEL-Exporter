// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package example_test

import (
	"os"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
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

func TestComposePublishesOTLPOnlyOnLoopback(t *testing.T) {
	raw, err := os.ReadFile("docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	var compose struct {
		Services map[string]struct {
			Ports []string `yaml:"ports"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(raw, &compose); err != nil {
		t.Fatal(err)
	}
	got := compose.Services["otelcollector"].Ports
	want := []string{"127.0.0.1:4317:4317", "127.0.0.1:4318:4318"}
	if len(got) != len(want) {
		t.Fatalf("published ports = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("published ports = %v, want loopback-only %v", got, want)
		}
	}
}

func TestDockerBuildContextExcludesCommonCredentialFiles(t *testing.T) {
	raw, err := os.ReadFile("../.dockerignore")
	if err != nil {
		t.Fatal(err)
	}
	patterns := strings.Fields(string(raw))
	for _, want := range []string{".env", "**/*.json", "**/*.key", "**/*.pem"} {
		found := false
		for _, pattern := range patterns {
			if pattern == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf(".dockerignore does not exclude common credential pattern %q", want)
		}
	}
}
