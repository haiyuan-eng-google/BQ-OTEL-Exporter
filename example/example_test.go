// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package example_test

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

// collectorVersionsFromGoMod reads the Collector release this module builds
// against from go.mod: the v0.x version shared by the Collector component and
// contrib modules (read from componentstatus) and the v1.x version shared by the
// stable modules (read from confmap). The example's pins are compared with these
// values instead of repeating a literal that must be edited with each release.
func collectorVersionsFromGoMod(t *testing.T, path string) (collectorVersion, confmapVersion string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	required := func(module string) string {
		t.Helper()
		m := regexp.MustCompile(`(?m)^\s*(?:require\s+)?` + regexp.QuoteMeta(module) + ` (v\d+\.\d+\.\d+)`).FindSubmatch(raw)
		if m == nil {
			t.Fatalf("%s has no requirement for %s", path, module)
		}
		return string(m[1])
	}
	return required("go.opentelemetry.io/collector/component/componentstatus"),
		required("go.opentelemetry.io/collector/confmap")
}

func TestCustomCollectorExampleIsSelfContainedAndPinned(t *testing.T) {
	collectorVersion, confmapVersion := collectorVersionsFromGoMod(t, "../go.mod")

	tests := []struct {
		path       string
		contains   []string
		matches    []string
		notContain []string
	}{
		{
			path: "otelcol-builder.yaml",
			contains: []string{
				"name: bigqueryexporter",
				"import: github.com/haiyuan-eng-google/BQ-OTEL-Exporter",
				"path: .",
				"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/fileexporter " + collectorVersion,
				"go.opentelemetry.io/collector/receiver/otlpreceiver " + collectorVersion,
				"go.opentelemetry.io/collector/processor/memorylimiterprocessor " + collectorVersion,
				"github.com/open-telemetry/opentelemetry-collector-contrib/extension/healthcheckextension " + collectorVersion,
				"go.opentelemetry.io/collector/confmap/provider/envprovider " + confmapVersion,
			},
		},
		{
			path: "Dockerfile",
			contains: []string{
				"go.opentelemetry.io/collector/cmd/builder@" + collectorVersion,
				"gcr.io/distroless/base-debian12:nonroot@sha256:",
			},
			// The builder image is pinned by release and digest. Which release
			// is compared with the CI and release workflows by the root
			// package's release asset tests.
			matches:    []string{`golang:\d+\.\d+\.\d+-bookworm@sha256:[0-9a-f]{64}`},
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
			for _, pattern := range tc.matches {
				if !regexp.MustCompile(pattern).MatchString(text) {
					t.Errorf("%s does not match %q", tc.path, pattern)
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
	want := []string{"127.0.0.1:4317:4317", "127.0.0.1:4318:4318", "127.0.0.1:13133:13133"}
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
