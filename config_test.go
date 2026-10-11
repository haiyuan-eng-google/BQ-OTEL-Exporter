// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/config/configopaque"
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
		"missing project":     {func(c *Config) { c.Project = "" }, "project must be specified"},
		"bad project":         {func(c *Config) { c.Project = "Not A Project" }, "not a valid Google Cloud project ID"},
		"missing dataset":     {func(c *Config) { c.Dataset = "" }, "dataset must be specified"},
		"bad dataset":         {func(c *Config) { c.Dataset = "otel-dash" }, "letters, numbers and underscores"},
		"empty traces table":  {func(c *Config) { c.Traces.Table = "" }, "traces.table must not be empty"},
		"empty logs table":    {func(c *Config) { c.Logs.Table = "" }, "logs.table must not be empty"},
		"empty metrics table": {func(c *Config) { c.Metrics.Table = "" }, "metrics.table must not be empty"},
		"negative traces partition expiry": {
			func(c *Config) { c.Traces.PartitionExpirationDays = -1 },
			"traces.partition_expiration_days (-1) must not be negative",
		},
		"negative logs partition expiry": {
			func(c *Config) { c.Logs.PartitionExpirationDays = -1 },
			"logs.partition_expiration_days (-1) must not be negative",
		},
		"negative metrics partition expiry": {
			func(c *Config) { c.Metrics.PartitionExpirationDays = -1 },
			"metrics.partition_expiration_days (-1) must not be negative",
		},
		"negative quarantine partition expiry": {
			func(c *Config) { c.Quarantine.PartitionExpirationDays = -1 },
			"quarantine.partition_expiration_days (-1) must not be negative",
		},
		"request over api cap": {func(c *Config) { c.Write.MaxRequestBytes = apiRequestLimitBytes }, "must stay below the Storage Write API"},
		"row over request":     {func(c *Config) { c.Write.MaxRowBytes = c.Write.MaxRequestBytes + 1 }, "must not exceed"},
		"no inflight requests": {func(c *Config) { c.Write.MaxInflightRequests = 0 }, "max_inflight_requests must be positive"},
		"inflight below request": {
			func(c *Config) { c.Write.MaxInflightBytes = c.Write.MaxRequestBytes - 1 },
			"a single full request could never be sent",
		},
		"negative connection pool size": {
			func(c *Config) { c.Write.ConnectionPoolSize = -1 },
			"write.connection_pool_size must not be negative",
		},
		"zero connection pool size with multiplexing": {
			func(c *Config) {
				c.Write.EnableMultiplexing = true
				c.Write.ConnectionPoolSize = 0
			},
			"write.connection_pool_size must be positive when write.enable_multiplexing is enabled",
		},
		"unsupported compression": {
			func(c *Config) { c.Write.Compression = "snappy" },
			`write.compression "snappy" must be empty or "gzip"`,
		},
		"negative keepalive time": {
			func(c *Config) { c.Write.Keepalive.Time = -time.Second },
			"write.keepalive.time must not be negative",
		},
		"negative keepalive timeout": {
			func(c *Config) { c.Write.Keepalive.Timeout = -time.Second },
			"write.keepalive.timeout must not be negative",
		},
		"autocreate needs location": {func(c *Config) { c.AutoCreate.Dataset = true }, "location is required"},
		"companion views need table creation": {
			func(c *Config) { c.AutoCreate.CompanionViews = true },
			"auto_create.companion_views requires auto_create.tables",
		},
		"search indexes need table creation": {
			func(c *Config) { c.AutoCreate.SearchIndexes = true },
			"auto_create.search_indexes requires auto_create.tables",
		},
		"bad limits": {func(c *Config) { c.Limits.MaxNestingDepth = 0 }, "max_nesting_depth must be positive"},
		"two credential methods": {func(c *Config) {
			c.Credentials.File = "/k.json"
			c.Credentials.ImpersonateServiceAccount = "sa@p.iam.gserviceaccount.com"
		}, "mutually exclusive"},
		"file and json credentials": {func(c *Config) {
			c.Credentials.File = "/k.json"
			c.Credentials.JSON = configopaque.String(`{"type":"service_account"}`)
		}, "credentials.file and credentials.json are mutually exclusive"},
		"json and impersonate credentials": {func(c *Config) {
			c.Credentials.JSON = configopaque.String(`{"type":"service_account"}`)
			c.Credentials.ImpersonateServiceAccount = "sa@p.iam.gserviceaccount.com"
		}, "credentials.json and credentials.impersonate_service_account are mutually exclusive"},
		"impersonate delegates without target": {func(c *Config) {
			c.Credentials.ImpersonateDelegates = []string{"delegate@p.iam.gserviceaccount.com"}
		}, "credentials.impersonate_delegates requires credentials.impersonate_service_account"},
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
		"no-auth with search index creation": {func(c *Config) {
			c.Endpoint.URL = "localhost:9050"
			c.Endpoint.WithoutAuthentication = true
			c.AutoCreate.Tables = true
			c.AutoCreate.SearchIndexes = true
		}, "auto_create.search_indexes cannot be enabled"},
		"zero exporter timeout": {func(c *Config) {
			c.TimeoutSettings.Timeout = 0
		}, "timeout must be positive"},
		"unbounded retry horizon": {func(c *Config) { c.BackOffConfig.MaxElapsedTime = 0 }, "must be finite"},
		"bad quarantine table": {
			func(c *Config) { c.Quarantine.Table = "dead-letter" },
			`quarantine.table "dead-letter" must contain only letters, numbers and underscores`,
		},
		"quarantine without a table": {func(c *Config) {
			c.Quarantine.Enabled = true
			c.Quarantine.Table = ""
		}, "quarantine.table must not be empty when quarantine.enabled is set"},
		"quarantine in the traces table": {func(c *Config) {
			c.Quarantine.Enabled = true
			c.Quarantine.Table = c.Traces.Table
		}, `quarantine.table "otel_spans" must differ from traces.table and logs.table`},
		"quarantine in the logs table": {func(c *Config) {
			c.Quarantine.Enabled = true
			c.Quarantine.Table = c.Logs.Table
		}, `quarantine.table "otel_logs" must differ from traces.table and logs.table`},
		"quarantine in the metrics table": {func(c *Config) {
			c.Quarantine.Enabled = true
			c.Quarantine.Table = c.Metrics.Table
		}, `quarantine.table "otel_metrics" must differ from traces.table and logs.table`},
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

// Companion views are opt-in, and ride on table creation, which provides the
// dedup views they read.
func TestValidateAcceptsCompanionViewsWithTableCreation(t *testing.T) {
	cfg := validConfig()
	if cfg.AutoCreate.CompanionViews {
		t.Fatal("auto_create.companion_views must be off by default")
	}
	cfg.AutoCreate.Tables = true
	cfg.AutoCreate.CompanionViews = true
	cfg.AutoCreate.SearchIndexes = true
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}
}

func TestValidateAcceptsEnterpriseAndLifecycleSettings(t *testing.T) {
	cfg := validConfig()
	cfg.Traces.PartitionExpirationDays = 365
	cfg.Traces.RequirePartitionFilter = true
	cfg.Logs.PartitionExpirationDays = 90
	cfg.Logs.RequirePartitionFilter = true
	cfg.Metrics.PartitionExpirationDays = 180
	cfg.Metrics.RequirePartitionFilter = true
	cfg.Quarantine.PartitionExpirationDays = 14
	cfg.Quarantine.RequirePartitionFilter = true
	cfg.Credentials.ImpersonateServiceAccount = "writer@my-project.iam.gserviceaccount.com"
	cfg.Credentials.ImpersonateDelegates = []string{"hop1@my-project.iam.gserviceaccount.com"}
	cfg.Credentials.QuotaProject = "billing-project"
	cfg.Credentials.UniverseDomain = "apis-tpclp.goog"
	cfg.Endpoint.AdminURL = "https://bigquery.apis-tpclp.goog"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}

	jsonCfg := validConfig()
	jsonCfg.Credentials.JSON = configopaque.String(`{"type":"service_account","private_key":"secret"}`)
	if err := jsonCfg.Validate(); err != nil {
		t.Fatalf("Validate() with credentials.json = %v, want nil", err)
	}
	if got := fmt.Sprint(jsonCfg.Credentials.JSON); got != "[REDACTED]" {
		t.Fatalf("credentials.json formatted as %q, want [REDACTED]", got)
	}
}

// Quarantine is opt-in, names its own table by default, and needs no table
// creation, so it also works against an emulator without authentication.
func TestValidateAcceptsQuarantine(t *testing.T) {
	cfg := validConfig()
	if cfg.Quarantine.Enabled {
		t.Fatal("quarantine must be off by default")
	}
	if cfg.Quarantine.Table != "otel_quarantine" {
		t.Fatalf("default quarantine.table = %q, want otel_quarantine", cfg.Quarantine.Table)
	}

	tests := map[string]func(*Config){
		"enabled with the default table": func(c *Config) { c.Quarantine.Enabled = true },
		"enabled with a custom table": func(c *Config) {
			c.Quarantine.Enabled = true
			c.Quarantine.Table = "dead_letters"
		},
		"disabled without a table": func(c *Config) { c.Quarantine.Table = "" },
		"enabled against an emulator": func(c *Config) {
			c.Endpoint.URL = "localhost:9050"
			c.Endpoint.WithoutAuthentication = true
			c.Quarantine.Enabled = true
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := validConfig()
			mutate(cfg)
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
		})
	}
}

// The log identity attributes are independent: either may be set alone.
func TestValidateAcceptsLogIdentityAttributes(t *testing.T) {
	tests := map[string]func(*Config){
		"log name only":  func(c *Config) { c.Logs.LogNameAttribute = "log.file.name" },
		"insert id only": func(c *Config) { c.Logs.InsertIDAttribute = "log.record.uid" },
		"both": func(c *Config) {
			c.Logs.LogNameAttribute = "log.file.name"
			c.Logs.InsertIDAttribute = "log.record.uid"
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := validConfig()
			mutate(cfg)
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
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

// The metadata client authenticates every request, so endpoint.admin_url may
// use plain HTTP only for a loopback emulator. A remote http:// URL would send
// the bearer token in the clear, and a URL without a scheme or host cannot be
// resolved against at all.
func TestEndpointAdminURLRequiresTLSUnlessLoopback(t *testing.T) {
	for _, adminURL := range []string{
		"https://bigquery.googleapis.com",
		"https://private.example.test/bigquery/v2/",
		"http://localhost:9050",
		"http://127.0.0.1:9050/bigquery/v2/",
		"http://[::1]:9050",
	} {
		cfg := validConfig()
		cfg.Endpoint.AdminURL = adminURL
		if err := cfg.Validate(); err != nil {
			t.Errorf("admin_url %q should be allowed: %v", adminURL, err)
		}
	}

	for adminURL, want := range map[string]string{
		"http://private.example.test/bigquery/v2/": "must use https:// unless it is a loopback address",
		"http://10.0.0.5:9050":                     "must use https:// unless it is a loopback address",
		"private.example.test/bigquery/v2/":        "must be an absolute http(s):// URL",
		"ftp://private.example.test":               "must be an absolute http(s):// URL",
		"https://":                                 "must be an absolute http(s):// URL",
	} {
		cfg := validConfig()
		cfg.Endpoint.AdminURL = adminURL
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("admin_url %q: Validate() = %v, want an error containing %q", adminURL, err, want)
		}
	}
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

func TestValidateOffloadAndQuarantineHardening(t *testing.T) {
	cfg := validConfig()
	if cfg.Offload.Enabled {
		t.Fatal("offload must be off by default")
	}
	if cfg.Limits.MaxAttributeKeyBytes != 1024 {
		t.Fatalf("default limits.max_attribute_key_bytes = %d, want 1024", cfg.Limits.MaxAttributeKeyBytes)
	}
	if cfg.Quarantine.MaxRawPayloadBytes != 8*1024*1024 || cfg.Quarantine.Timeout != 5*time.Second {
		t.Fatalf("default quarantine max_raw_payload_bytes=%d timeout=%v, want 8MiB and 5s",
			cfg.Quarantine.MaxRawPayloadBytes, cfg.Quarantine.Timeout)
	}

	validOffload := validConfig()
	validOffload.Offload.Enabled = true
	validOffload.Offload.Bucket = "my-otel-offload-bucket"
	validOffload.Quarantine.Enabled = true
	validOffload.Quarantine.MetadataOnly = true
	if err := validOffload.Validate(); err != nil {
		t.Fatalf("valid offload + quarantine metadata_only config failed Validate(): %v", err)
	}

	rejects := map[string]struct {
		mutate func(*Config)
		want   string
	}{
		"zero attribute key limit": {
			func(c *Config) { c.Limits.MaxAttributeKeyBytes = 0 },
			"max_attribute_key_bytes must be positive",
		},
		"zero quarantine max_raw_payload_bytes when enabled": {
			func(c *Config) {
				c.Quarantine.Enabled = true
				c.Quarantine.MaxRawPayloadBytes = 0
			},
			"quarantine.max_raw_payload_bytes must be positive",
		},
		"zero quarantine timeout when enabled": {
			func(c *Config) {
				c.Quarantine.Enabled = true
				c.Quarantine.Timeout = 0
			},
			"quarantine.timeout must be positive",
		},
		"offload enabled without bucket": {
			func(c *Config) { c.Offload.Enabled = true },
			"offload.bucket must not be empty",
		},
		"offload bucket with gs prefix": {
			func(c *Config) {
				c.Offload.Enabled = true
				c.Offload.Bucket = "gs://my-bucket"
			},
			"bare bucket name without gs://",
		},
		"offload max_object_bytes below min_bytes": {
			func(c *Config) {
				c.Offload.Enabled = true
				c.Offload.Bucket = "my-bucket"
				c.Offload.MinBytes = 4096
				c.Offload.MaxObjectBytes = 2048
			},
			"offload.max_object_bytes (2048) must be at least offload.min_bytes (4096)",
		},
		"offload zero timeout": {
			func(c *Config) {
				c.Offload.Enabled = true
				c.Offload.Bucket = "my-bucket"
				c.Offload.Timeout = 0
			},
			"offload.timeout must be positive",
		},
		"offload zero max_concurrency": {
			func(c *Config) {
				c.Offload.Enabled = true
				c.Offload.Bucket = "my-bucket"
				c.Offload.MaxConcurrency = 0
			},
			"offload.max_concurrency must be positive",
		},
	}

	for name, tc := range rejects {
		t.Run(name, func(t *testing.T) {
			c := validConfig()
			tc.mutate(c)
			err := c.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate() = %v, want error containing %q", err, tc.want)
			}
		})
	}
}
