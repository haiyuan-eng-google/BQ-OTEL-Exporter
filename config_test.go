// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"strings"
	"testing"
)

func validConfig() *Config {
	cfg := createDefaultConfig().(*Config)
	cfg.Project = "my-project"
	cfg.Dataset = "otel"
	return cfg
}

func TestValidateAcceptsDefaults(t *testing.T) {
	if err := validConfig().Validate(); err != nil {
		t.Fatalf("default config should validate, got: %v", err)
	}
}

func TestValidateRejects(t *testing.T) {
	tests := map[string]struct {
		mutate func(*Config)
		want   string
	}{
		"missing project":       {func(c *Config) { c.Project = "" }, "project must be specified"},
		"bad project":           {func(c *Config) { c.Project = "Not A Project" }, "not a valid Google Cloud project ID"},
		"missing dataset":       {func(c *Config) { c.Dataset = "" }, "dataset must be specified"},
		"bad dataset":           {func(c *Config) { c.Dataset = "otel-dash" }, "letters, numbers and underscores"},
		"empty spans table":     {func(c *Config) { c.SpansTable = "" }, "spans_table must not be empty"},
		"request over api cap":  {func(c *Config) { c.Write.MaxRequestBytes = apiRequestLimitBytes }, "must stay below the Storage Write API"},
		"row over request":      {func(c *Config) { c.Write.MaxRowBytes = c.Write.MaxRequestBytes + 1 }, "must not exceed"},
		"no inflight requests":  {func(c *Config) { c.Write.MaxInflightRequests = 0 }, "max_inflight_requests must be positive"},
		"autocreate no location": {func(c *Config) { c.AutoCreate.Dataset = true }, "auto_create.location is required"},
		"bad promoted type":     {func(c *Config) { c.PromoteAttributes = []PromotedAttribute{{Attribute: "a.b", Type: "DECIMAL"}} }, "must be one of STRING"},
		"duplicate promoted column": {func(c *Config) {
			c.PromoteAttributes = []PromotedAttribute{
				{Attribute: "gen_ai.system", Type: "STRING"},
				{Attribute: "gen_ai/system", Type: "STRING"},
			}
		}, "duplicate destination column"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := validConfig()
			tc.mutate(cfg)
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("expected an error containing %q, got nil", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error containing %q, got: %v", tc.want, err)
			}
		})
	}
}

func TestSanitizeColumn(t *testing.T) {
	for in, want := range map[string]string{
		"gen_ai.system":   "gen_ai_system",
		"http.route":      "http_route",
		"already_ok":      "already_ok",
		"weird-chars!here": "weird_chars_here",
	} {
		if got := sanitizeColumn(in); got != want {
			t.Errorf("sanitizeColumn(%q) = %q, want %q", in, got, want)
		}
	}
}

// The headroom threshold must stay meaningfully below the API limit; if the
// two ever converge, FR7's distinction between our threshold and the API's
// has quietly stopped being true.
func TestDefaultHeadroomBelowAPILimit(t *testing.T) {
	if defaultMaxRequestBytes >= apiRequestLimitBytes {
		t.Fatalf("default headroom %d is not below the API limit %d", defaultMaxRequestBytes, apiRequestLimitBytes)
	}
}
