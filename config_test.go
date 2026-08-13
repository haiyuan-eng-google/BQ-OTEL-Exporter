// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"strings"
	"testing"
	"time"
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
		"missing project":      {func(c *Config) { c.Project = "" }, "project must be specified"},
		"bad project":          {func(c *Config) { c.Project = "Not A Project" }, "not a valid Google Cloud project ID"},
		"missing dataset":      {func(c *Config) { c.Dataset = "" }, "dataset must be specified"},
		"bad dataset":          {func(c *Config) { c.Dataset = "otel-dash" }, "letters, numbers and underscores"},
		"empty traces table":   {func(c *Config) { c.Traces.Table = "" }, "traces.table must not be empty"},
		"empty logs table":     {func(c *Config) { c.Logs.Table = "" }, "logs.table must not be empty"},
		"request over api cap": {func(c *Config) { c.Write.MaxRequestBytes = apiRequestLimitBytes }, "must stay below the Storage Write API"},
		"row over request":     {func(c *Config) { c.Write.MaxRowBytes = c.Write.MaxRequestBytes + 1 }, "must not exceed"},
		"no inflight requests": {func(c *Config) { c.Write.MaxInflightRequests = 0 }, "max_inflight_requests must be positive"},
		"inflight below request": {
			func(c *Config) { c.Write.MaxInflightBytes = c.Write.MaxRequestBytes - 1 },
			"a single full request could never be sent",
		},
		"autocreate needs location": {func(c *Config) { c.AutoCreate.Dataset = true }, "location is required"},
		"bad limits":                {func(c *Config) { c.Limits.MaxNestingDepth = 0 }, "max_nesting_depth must be positive"},
		"two credential methods": {func(c *Config) {
			c.Credentials.File = "/k.json"
			c.Credentials.ImpersonateServiceAccount = "sa@p.iam.gserviceaccount.com"
		}, "mutually exclusive"},
		"insecure without endpoint": {func(c *Config) { c.Endpoint.Insecure = true }, "endpoint.insecure requires endpoint.url"},
		"no-auth without endpoint":  {func(c *Config) { c.Endpoint.WithoutAuthentication = true }, "requires endpoint.url"},
		"no-auth with dataset creation": {func(c *Config) {
			c.Endpoint.URL = "localhost:9050"
			c.Endpoint.WithoutAuthentication = true
			c.AutoCreate.Dataset = true
			c.Location = "US"
		}, "auto_create.dataset cannot be enabled"},
		"no-auth with table creation": {func(c *Config) {
			c.Endpoint.URL = "localhost:9050"
			c.Endpoint.WithoutAuthentication = true
			c.AutoCreate.Tables = true
		}, "auto_create.tables cannot be enabled"},
		"zero exporter timeout": {func(c *Config) {
			c.TimeoutSettings.Timeout = 0
		}, "timeout must be positive"},
		"unbounded retry horizon":     {func(c *Config) { c.BackOffConfig.MaxElapsedTime = 0 }, "must be finite"},
		"source id without namespace": {func(c *Config) { c.Logs.SourceRecordIDAttribute = "uid" }, "not a record identity"},
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

// Turning off TLS or authentication against a remote endpoint would put real
// credentials, or real telemetry, on the wire in the clear. The override is
// for emulators, so it is refused anywhere else.
func TestEndpointOverrideIsLoopbackOnly(t *testing.T) {
	t.Run("loopback is allowed", func(t *testing.T) {
		for _, url := range []string{"localhost:9050", "127.0.0.1:9050", "http://localhost:9050"} {
			cfg := validConfig()
			cfg.Endpoint.URL = url
			cfg.Endpoint.Insecure = true
			cfg.Endpoint.WithoutAuthentication = true
			if err := cfg.Validate(); err != nil {
				t.Errorf("%s should be allowed: %v", url, err)
			}
		}
	})

	t.Run("remote is refused", func(t *testing.T) {
		for _, url := range []string{"bigquerystorage.example.com:443", "10.0.0.5:9050", "evil.test"} {
			cfg := validConfig()
			cfg.Endpoint.URL = url
			cfg.Endpoint.Insecure = true
			if err := cfg.Validate(); err == nil {
				t.Errorf("%s must not be allowed with insecure transport", url)
			}
		}
	})

	t.Run("remote endpoint with TLS is fine", func(t *testing.T) {
		cfg := validConfig()
		cfg.Endpoint.URL = "bigquerystorage.example.com:443"
		if err := cfg.Validate(); err != nil {
			t.Errorf("a remote endpoint with TLS and auth intact should be allowed: %v", err)
		}
	})
}

// The headroom threshold must stay meaningfully below the API limit; if the
// two converge, the distinction between our threshold and the API's has
// quietly stopped being true.
func TestDefaultHeadroomBelowAPILimit(t *testing.T) {
	if defaultMaxRequestBytes >= apiRequestLimitBytes {
		t.Fatalf("default headroom %d is not below the API limit %d", defaultMaxRequestBytes, apiRequestLimitBytes)
	}
}

// A finite retry horizon is what makes the duplicate bound meaningful, and it
// has to outlast the ten-minute outage the reliability journey exercises.
func TestDefaultRetryHorizon(t *testing.T) {
	cfg := validConfig()
	if cfg.BackOffConfig.MaxElapsedTime == 0 {
		t.Fatal("default retry horizon must be finite")
	}
	if cfg.BackOffConfig.MaxElapsedTime <= 10*time.Minute {
		t.Fatalf("default horizon %v does not exceed the ten-minute outage case",
			cfg.BackOffConfig.MaxElapsedTime)
	}
}
