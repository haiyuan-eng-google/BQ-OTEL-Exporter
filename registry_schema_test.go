// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/dlclark/regexp2"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"gopkg.in/yaml.v3"
)

// The destination schema uses negative lookahead, which Go's regexp omits.
type registryRegexp struct{ *regexp2.Regexp }

func (r registryRegexp) MatchString(s string) bool {
	matched, err := r.Regexp.MatchString(s)
	return err == nil && matched
}

func registrySchema(t *testing.T) *jsonschema.Schema {
	t.Helper()
	t.Cleanup(regexp2.StopTimeoutClock)
	raw, err := os.ReadFile("testdata/registry/schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	c.UseRegexpEngine(func(pattern string) (jsonschema.Regexp, error) {
		r, err := regexp2.Compile(pattern, regexp2.ECMAScript)
		if err != nil {
			return nil, err
		}
		r.MatchTimeout = time.Second
		return registryRegexp{r}, nil
	})
	const location = "https://opentelemetry.io/ecosystem/registry/schema.json"
	if err := c.AddResource(location, doc); err != nil {
		t.Fatal(err)
	}
	s, err := c.Compile(location)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func registryEntry(t *testing.T) map[string]any {
	t.Helper()
	var entry map[string]any
	if err := yaml.Unmarshal([]byte(readRepoText(t, "docs/otel-registry-entry.yaml")), &entry); err != nil {
		t.Fatal(err)
	}
	return entry
}

func TestRegistrySchemaRejectsInvalidEntries(t *testing.T) {
	s := registrySchema(t)
	for _, tc := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"obsolete version", func(e map[string]any) { e["otVersion"] = "v0.1.0" }},
		{"unknown property", func(e map[string]any) { e["unknown"] = true }},
		{"nested unknown property", func(e map[string]any) { e["urls"].(map[string]any)["unknown"] = true }},
		{"missing author contact", func(e map[string]any) { e["authors"] = []any{map[string]any{"name": "Example"}} }},
		{"reserved author name", func(e map[string]any) {
			e["authors"] = []any{map[string]any{"name": "OpenTelemetry Impersonator", "url": "https://example.com"}}
		}},
		{"invalid author URL", func(e map[string]any) {
			e["authors"] = []any{map[string]any{"name": "Example", "url": "not a URL"}}
		}},
		{"commercial license", func(e map[string]any) { e["license"] = "Commercial" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := registryEntry(t)
			tc.mutate(e)
			if err := s.Validate(e); err == nil {
				t.Fatal("destination schema accepted invalid registry entry")
			}
		})
	}
	// The schema also permits entries with fewer tags and a supported package
	// object; validation must not enshrine the obsolete otVersion field.
	e := registryEntry(t)
	e["tags"] = []any{"collector"}
	e["package"] = map[string]any{"registry": "go-collector", "name": "example.org/exporter"}
	if err := s.Validate(e); err != nil {
		t.Fatalf("schema-valid alternative entry rejected: %v", err)
	}
}
