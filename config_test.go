// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"fmt"
	"math"
	"reflect"
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

// Whether the cross-signal companion views (v_gen_ai_logs,
// v_gen_ai_metric_exemplars) can be queried depends on whether the spans table
// in effect requires a partition filter, which only startup can read, because
// traces.require_partition_filter applies only when that table is created.
// No combination of companion_views, require_partition_filter, and
// allow_unprunable_views is therefore a configuration error: startup skips the
// cross-signal views with a warning when they could not be queried.
func TestValidateAcceptsCompanionViewsWithPartitionFilters(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Config)
	}{
		{
			name: "traces require_partition_filter with companion_views",
			mutate: func(c *Config) {
				c.AutoCreate.Tables = true
				c.AutoCreate.CompanionViews = true
				c.Traces.RequirePartitionFilter = true
			},
		},
		{
			name: "traces require_partition_filter with companion_views and allow_unprunable_views",
			mutate: func(c *Config) {
				c.AutoCreate.Tables = true
				c.AutoCreate.CompanionViews = true
				c.Traces.RequirePartitionFilter = true
				c.AutoCreate.AllowUnprunableViews = true
			},
		},
		{
			name: "traces require_partition_filter without companion_views",
			mutate: func(c *Config) {
				c.AutoCreate.Tables = true
				c.Traces.RequirePartitionFilter = true
			},
		},
		{
			name: "allow_unprunable_views without companion_views",
			mutate: func(c *Config) {
				c.AutoCreate.AllowUnprunableViews = true
			},
		},
		{
			name: "logs and metrics require_partition_filter with companion_views",
			mutate: func(c *Config) {
				c.AutoCreate.Tables = true
				c.AutoCreate.CompanionViews = true
				c.Logs.RequirePartitionFilter = true
				c.Metrics.RequirePartitionFilter = true
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig()
			tc.mutate(cfg)
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
		})
	}
}

// auto_create.allow_unprunable_views is the key operators write. The full
// section of the testdata fixture enables companion views with a spans
// partition filter, and validates without that key, as it did before the key
// existed, so a configuration written before it keeps starting.
func TestAllowUnprunableViewsConfigKey(t *testing.T) {
	for _, want := range []bool{false, true} {
		t.Run(fmt.Sprintf("decodes %v", want), func(t *testing.T) {
			cfg := decodeExporterConfig(t, "allow_unprunable_views", map[string]any{
				"project": "my-project",
				"dataset": "otel",
				"traces":  map[string]any{"require_partition_filter": true},
				"auto_create": map[string]any{
					"tables":                 true,
					"companion_views":        true,
					"allow_unprunable_views": want,
				},
			})
			if cfg.AutoCreate.AllowUnprunableViews != want {
				t.Fatalf("auto_create.allow_unprunable_views decoded as %v, want %v",
					cfg.AutoCreate.AllowUnprunableViews, want)
			}
		})
	}

	t.Run("testdata full section without the key", func(t *testing.T) {
		doc := parseYAMLMap(t, "testdata/config.yaml", readRepoText(t, "testdata/config.yaml"))
		autoCreate, ok := yamlPath(doc, "exporters", "bigquery/full", "auto_create").(map[string]any)
		if !ok {
			t.Fatal("testdata/config.yaml has no exporters::bigquery/full::auto_create section")
		}
		if _, ok := autoCreate["allow_unprunable_views"]; !ok {
			t.Fatal("exporters::bigquery/full::auto_create does not set allow_unprunable_views; this check removes it")
		}
		delete(autoCreate, "allow_unprunable_views")
		section := yamlPath(doc, "exporters", "bigquery/full").(map[string]any)
		cfg := decodeExporterConfig(t, "bigquery/full without allow_unprunable_views", section)
		if !cfg.AutoCreate.Tables || !cfg.AutoCreate.CompanionViews || !cfg.Traces.RequirePartitionFilter ||
			cfg.AutoCreate.AllowUnprunableViews {
			t.Fatalf("auto_create = %+v, traces.require_partition_filter = %v; want tables and companion_views "+
				"on, allow_unprunable_views off, and a traces partition filter",
				cfg.AutoCreate, cfg.Traces.RequirePartitionFilter)
		}
	})
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
		"http://localhost:80@attacker.com":         "must use https:// unless it is a loopback address",
		"http://attacker.com#localhost":            "must use https:// unless it is a loopback address",
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
		"blank location": {
			func(c *Config) { c.Location = "   " },
			"location \"   \" must not be blank or contain whitespace",
		},
		"location with whitespace": {
			func(c *Config) { c.Location = "US EAST1" },
			"location \"US EAST1\" must not be blank or contain whitespace",
		},
		"invalid top-level kms_key_name": {
			func(c *Config) { c.KMSKeyName = "projects/my-project/cryptoKeys/bad" },
			"kms_key_name \"projects/my-project/cryptoKeys/bad\" must match projects/{project}/locations/{location}/keyRings/{keyRing}/cryptoKeys/{cryptoKey}",
		},
		"invalid signal kms_key_name": {
			func(c *Config) { c.Traces.KMSKeyName = "bad-kms-key" },
			"traces.kms_key_name \"bad-kms-key\" must match projects/{project}/locations/{location}/keyRings/{keyRing}/cryptoKeys/{cryptoKey}",
		},
		"kms_key_name with cryptoKeyVersions rejected": {
			func(c *Config) {
				c.Traces.KMSKeyName = "projects/my-project/locations/us-central1/keyRings/otel-ring/cryptoKeys/traces-key/cryptoKeyVersions/1"
			},
			"traces.kms_key_name \"projects/my-project/locations/us-central1/keyRings/otel-ring/cryptoKeys/traces-key/cryptoKeyVersions/1\" must match projects/{project}/locations/{location}/keyRings/{keyRing}/cryptoKeys/{cryptoKey}",
		},
		"invalid label key": {
			func(c *Config) { c.Labels = map[string]string{"InvalidUpper": "ok"} },
			"labels key \"InvalidUpper\" must start with a lowercase letter and contain at most 63",
		},
		"invalid label value": {
			func(c *Config) { c.Logs.Labels = map[string]string{"env": "Prod!"} },
			"logs.labels[\"env\"] value \"Prod!\" must contain at most 63",
		},
		"too many labels": {
			func(c *Config) {
				c.Metrics.Labels = make(map[string]string, 65)
				for i := range 65 {
					c.Metrics.Labels[fmt.Sprintf("k%d", i)] = "v"
				}
			},
			"metrics.labels has 65 entries; BigQuery allows at most 64 labels",
		},
		"max_staleness below 1s": {
			func(c *Config) { c.Traces.MaxStaleness = "500ms" },
			"traces.max_staleness (500ms) must be between 1s and 168h0m0s",
		},
		"max_staleness above 168h": {
			func(c *Config) { c.Quarantine.MaxStaleness = "169h" },
			"quarantine.max_staleness (169h0m0s) must be between 1s and 168h0m0s",
		},
		"max_staleness invalid duration": {
			func(c *Config) { c.Logs.MaxStaleness = "not-a-duration" },
			"logs.max_staleness \"not-a-duration\" is not a valid duration",
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

func TestConfigValidateAcceptsDomainPrefixedProjectAndGovernanceOptions(t *testing.T) {
	c := validConfig()
	c.Project = "google.com:my-project-123"
	c.Location = "us-central1"
	c.KMSKeyName = "projects/my-project/locations/us-central1/keyRings/otel-ring/cryptoKeys/otel-key"
	c.Labels = map[string]string{
		"env":  "prod",
		"team": "observability_1",
	}
	c.Traces.KMSKeyName = "projects/my-project/locations/us-central1/keyRings/otel-ring/cryptoKeys/traces-key"
	c.Traces.Labels = map[string]string{"signal": "traces"}
	c.Traces.MaxStaleness = "15m"
	c.Logs.MaxStaleness = "1h"
	c.Metrics.MaxStaleness = "168h"
	c.Quarantine.MaxStaleness = "5m"
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate() unexpected error: %v", err)
	}
}

func TestValidateRedaction(t *testing.T) {
	t.Run("default config carries the mask and is disabled", func(t *testing.T) {
		cfg := validConfig()
		if cfg.Redaction.Enabled {
			t.Fatal("redaction must be opt-in")
		}
		if cfg.Redaction.MaskReplacement != "[REDACTED]" {
			t.Fatalf("default mask_replacement = %q", cfg.Redaction.MaskReplacement)
		}
	})

	t.Run("enabled with defaults validates", func(t *testing.T) {
		cfg := validConfig()
		cfg.Redaction.Enabled = true
		if err := cfg.Validate(); err != nil {
			t.Fatalf("enabled redaction with default keys should validate, got: %v", err)
		}
		keys := cfg.Redaction.effectiveBlockedKeys()
		if len(keys) == 0 {
			t.Fatal("effective keys must include the defaults")
		}
	})

	t.Run("enabled with custom keys and patterns validates", func(t *testing.T) {
		cfg := validConfig()
		cfg.Redaction.Enabled = true
		cfg.Redaction.DisableDefaultKeys = true
		cfg.Redaction.BlockedKeys = []string{"x-internal-*", "Customer.SSN"}
		cfg.Redaction.BlockedValuePatterns = []string{`\d{3}-\d{2}-\d{4}`}
		cfg.Redaction.MaskReplacement = "***"
		cfg.Redaction.RedactLogBody = true
		if err := cfg.Validate(); err != nil {
			t.Fatalf("custom redaction should validate, got: %v", err)
		}
		if got := cfg.Redaction.effectiveBlockedKeys(); len(got) != 2 {
			t.Fatalf("effective keys = %v, want only the two custom keys", got)
		}
	})

	rejects := map[string]struct {
		mutate func(*Config)
		want   string
	}{
		"invalid regex even when disabled": {
			func(c *Config) { c.Redaction.BlockedValuePatterns = []string{"ok", "(unclosed"} },
			"redaction.blocked_value_patterns[1]",
		},
		"empty pattern": {
			func(c *Config) { c.Redaction.BlockedValuePatterns = []string{" "} },
			"redaction.blocked_value_patterns[0] is empty",
		},
		"empty key": {
			func(c *Config) { c.Redaction.BlockedKeys = []string{"authorization", ""} },
			"redaction.blocked_keys[1] is empty",
		},
		"enabled with nothing to match": {
			func(c *Config) {
				c.Redaction.Enabled = true
				c.Redaction.DisableDefaultKeys = true
			},
			"no blocked_keys and no blocked_value_patterns",
		},
		"insert_id attribute would be masked": {
			func(c *Config) {
				c.Redaction.Enabled = true
				c.Logs.InsertIDAttribute = "session_token"
			},
			`logs.insert_id_attribute "session_token" matches a blocked redaction key`,
		},
		"log_name attribute would be masked by a custom glob": {
			func(c *Config) {
				c.Redaction.Enabled = true
				c.Redaction.BlockedKeys = []string{"gcp.*"}
				c.Logs.LogNameAttribute = "gcp.log_name"
			},
			`logs.log_name_attribute "gcp.log_name" matches a blocked redaction key`,
		},
	}
	for name, tc := range rejects {
		t.Run(name, func(t *testing.T) {
			cfg := validConfig()
			tc.mutate(cfg)
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("expected error containing %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.want)
			}
		})
	}

	t.Run("identity attribute that is not blocked validates", func(t *testing.T) {
		cfg := validConfig()
		cfg.Redaction.Enabled = true
		cfg.Logs.InsertIDAttribute = "log.record.uid"
		cfg.Logs.LogNameAttribute = "gcp.log_name"
		if err := cfg.Validate(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("toRedact falls back to the default mask", func(t *testing.T) {
		cfg := validConfig()
		cfg.Redaction.Enabled = true
		cfg.Redaction.MaskReplacement = ""
		rc := cfg.Redaction.toRedact(nil)
		if rc.MaskReplacement != "[REDACTED]" {
			t.Fatalf("mask = %q", rc.MaskReplacement)
		}
	})
}

// TestRedactionConfigKeys pins the YAML keys operators write. A mistyped
// mapstructure tag would otherwise surface only when a collector refuses to
// start with the documented configuration.
func TestRedactionConfigKeys(t *testing.T) {
	top, ok := reflect.TypeOf(Config{}).FieldByName("Redaction")
	if !ok || top.Tag.Get("mapstructure") != "redaction" {
		t.Fatalf("Config.Redaction mapstructure tag = %q, want %q", top.Tag.Get("mapstructure"), "redaction")
	}
	want := map[string]string{
		"Enabled":              "enabled",
		"BlockedKeys":          "blocked_keys",
		"DisableDefaultKeys":   "disable_default_keys",
		"BlockedValuePatterns": "blocked_value_patterns",
		"MaskReplacement":      "mask_replacement",
		"RedactLogBody":        "redact_log_body",
	}
	rt := reflect.TypeOf(RedactionConfig{})
	if rt.NumField() != len(want) {
		t.Fatalf("RedactionConfig has %d fields, want %d; document and pin any new option here", rt.NumField(), len(want))
	}
	for field, key := range want {
		f, ok := rt.FieldByName(field)
		if !ok || f.Tag.Get("mapstructure") != key {
			t.Errorf("RedactionConfig.%s mapstructure tag = %q, want %q", field, f.Tag.Get("mapstructure"), key)
		}
	}
}

func TestValidateOffloadCacheSize(t *testing.T) {
	cfg := validConfig()
	if cfg.Offload.CacheSize != 4096 {
		t.Fatalf("default Offload.CacheSize = %d, want 4096", cfg.Offload.CacheSize)
	}
	cfg.Offload.CacheSize = 0
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Offload.CacheSize = 0 should validate (disables cross-batch cache), got %v", err)
	}
	cfg.Offload.CacheSize = -1
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "offload.cache_size must not be negative") {
		t.Fatalf("Offload.CacheSize = -1 error = %v, want offload.cache_size must not be negative", err)
	}
}

func TestValidateBackpressure(t *testing.T) {
	t.Run("defaults are disabled with expected tuning", func(t *testing.T) {
		cfg := validConfig()
		b := cfg.Write.Backpressure
		if b.Enabled {
			t.Fatal("write.backpressure.enabled must be false by default")
		}
		if b.MinRequestBytes != 1024*1024 {
			t.Fatalf("default min_request_bytes = %d, want 1048576", b.MinRequestBytes)
		}
		if b.MinInflightRequests != 1 {
			t.Fatalf("default min_inflight_requests = %d, want 1", b.MinInflightRequests)
		}
		if b.DecreaseFactor != 0.5 {
			t.Fatalf("default decrease_factor = %v, want 0.5", b.DecreaseFactor)
		}
		if b.RecoveryInterval != 10*time.Second {
			t.Fatalf("default recovery_interval = %v, want 10s", b.RecoveryInterval)
		}
		if b.LatencyThreshold != 2*time.Second {
			t.Fatalf("default latency_threshold = %v, want 2s", b.LatencyThreshold)
		}
	})

	t.Run("enabled with defaults validates", func(t *testing.T) {
		cfg := validConfig()
		cfg.Write.Backpressure.Enabled = true
		if err := cfg.Validate(); err != nil {
			t.Fatalf("enabled backpressure with defaults failed Validate(): %v", err)
		}
	})

	t.Run("enabled with zero latency threshold validates", func(t *testing.T) {
		cfg := validConfig()
		cfg.Write.Backpressure.Enabled = true
		cfg.Write.Backpressure.LatencyThreshold = 0
		if err := cfg.Validate(); err != nil {
			t.Fatalf("enabled backpressure with latency_threshold=0 failed Validate(): %v", err)
		}
	})

	rejects := map[string]struct {
		mutate func(*Config)
		want   string
	}{
		"negative min_request_bytes when disabled": {
			func(c *Config) { c.Write.Backpressure.MinRequestBytes = -1 },
			"write.backpressure.min_request_bytes must not be negative",
		},
		"negative min_inflight_requests when disabled": {
			func(c *Config) { c.Write.Backpressure.MinInflightRequests = -1 },
			"write.backpressure.min_inflight_requests must not be negative",
		},
		"negative recovery_interval when disabled": {
			func(c *Config) { c.Write.Backpressure.RecoveryInterval = -time.Second },
			"write.backpressure.recovery_interval must not be negative",
		},
		"negative latency_threshold when disabled": {
			func(c *Config) { c.Write.Backpressure.LatencyThreshold = -time.Second },
			"write.backpressure.latency_threshold must not be negative",
		},
		"zero min_request_bytes when enabled": {
			func(c *Config) {
				c.Write.Backpressure.Enabled = true
				c.Write.Backpressure.MinRequestBytes = 0
			},
			"write.backpressure.min_request_bytes must be positive",
		},
		"min_request_bytes exceeds max_request_bytes": {
			func(c *Config) {
				c.Write.Backpressure.Enabled = true
				c.Write.Backpressure.MinRequestBytes = c.Write.MaxRequestBytes + 1
			},
			"write.backpressure.min_request_bytes",
		},
		"zero min_inflight_requests when enabled": {
			func(c *Config) {
				c.Write.Backpressure.Enabled = true
				c.Write.Backpressure.MinInflightRequests = 0
			},
			"write.backpressure.min_inflight_requests must be at least 1",
		},
		"min_inflight_requests exceeds max_inflight_requests": {
			func(c *Config) {
				c.Write.Backpressure.Enabled = true
				c.Write.Backpressure.MinInflightRequests = c.Write.MaxInflightRequests + 1
			},
			"write.backpressure.min_inflight_requests",
		},
		"decrease_factor zero": {
			func(c *Config) {
				c.Write.Backpressure.Enabled = true
				c.Write.Backpressure.DecreaseFactor = 0
			},
			"write.backpressure.decrease_factor (0) must be strictly between 0 and 1",
		},
		"decrease_factor one": {
			func(c *Config) {
				c.Write.Backpressure.Enabled = true
				c.Write.Backpressure.DecreaseFactor = 1.0
			},
			"write.backpressure.decrease_factor (1) must be strictly between 0 and 1",
		},
		"decrease_factor NaN": {
			func(c *Config) {
				c.Write.Backpressure.Enabled = true
				c.Write.Backpressure.DecreaseFactor = math.NaN()
			},
			"write.backpressure.decrease_factor (NaN) must be strictly between 0 and 1",
		},
		"zero recovery_interval when enabled": {
			func(c *Config) {
				c.Write.Backpressure.Enabled = true
				c.Write.Backpressure.RecoveryInterval = 0
			},
			"write.backpressure.recovery_interval must be positive",
		},
	}
	for name, tc := range rejects {
		t.Run(name, func(t *testing.T) {
			cfg := validConfig()
			tc.mutate(cfg)
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate() = %v, want error containing %q", err, tc.want)
			}
		})
	}
}

func TestBackpressureConfigKeys(t *testing.T) {
	writeType := reflect.TypeOf(WriteConfig{})
	top, ok := writeType.FieldByName("Backpressure")
	if !ok || top.Tag.Get("mapstructure") != "backpressure" {
		t.Fatalf("WriteConfig.Backpressure mapstructure tag = %q, want %q", top.Tag.Get("mapstructure"), "backpressure")
	}
	want := map[string]string{
		"Enabled":             "enabled",
		"MinRequestBytes":     "min_request_bytes",
		"MinInflightRequests": "min_inflight_requests",
		"DecreaseFactor":      "decrease_factor",
		"RecoveryInterval":    "recovery_interval",
		"LatencyThreshold":    "latency_threshold",
	}
	bt := reflect.TypeOf(BackpressureConfig{})
	if bt.NumField() != len(want) {
		t.Fatalf("BackpressureConfig has %d fields, want %d; document and pin any new option here", bt.NumField(), len(want))
	}
	for field, key := range want {
		f, ok := bt.FieldByName(field)
		if !ok || f.Tag.Get("mapstructure") != key {
			t.Errorf("BackpressureConfig.%s mapstructure tag = %q, want %q", field, f.Tag.Get("mapstructure"), key)
		}
	}
}

func TestStreamPoolConfigValidation(t *testing.T) {
	if got := validConfig().Write.StreamPoolSize; got != 1 {
		t.Fatalf("default Write.StreamPoolSize = %d, want 1", got)
	}
	f, ok := reflect.TypeOf(WriteConfig{}).FieldByName("StreamPoolSize")
	if !ok || f.Tag.Get("mapstructure") != "stream_pool_size" {
		t.Fatalf("WriteConfig.StreamPoolSize mapstructure tag = %q, want %q", f.Tag.Get("mapstructure"), "stream_pool_size")
	}

	accepts := map[string]func(*Config){
		"min pool size 1":  func(c *Config) { c.Write.StreamPoolSize = 1 },
		"max pool size 16": func(c *Config) { c.Write.StreamPoolSize = 16 },
		"non-multiplexed with matching connection pool": func(c *Config) {
			c.Write.EnableMultiplexing = false
			c.Write.StreamPoolSize = 4
			c.Write.ConnectionPoolSize = 4
		},
		"non-multiplexed with zero connection pool": func(c *Config) {
			c.Write.EnableMultiplexing = false
			c.Write.StreamPoolSize = 4
			c.Write.ConnectionPoolSize = 0
		},
	}
	for name, mutate := range accepts {
		t.Run("accept/"+name, func(t *testing.T) {
			cfg := validConfig()
			mutate(cfg)
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
		})
	}

	rejects := map[string]struct {
		mutate func(*Config)
		want   string
	}{
		"zero stream_pool_size": {
			func(c *Config) { c.Write.StreamPoolSize = 0 },
			"write.stream_pool_size (0) must be between 1 and 16",
		},
		"negative stream_pool_size": {
			func(c *Config) { c.Write.StreamPoolSize = -1 },
			"write.stream_pool_size (-1) must be between 1 and 16",
		},
		"above max stream_pool_size": {
			func(c *Config) { c.Write.StreamPoolSize = 17 },
			"write.stream_pool_size (17) must be between 1 and 16",
		},
		"non-multiplexed connection pool smaller than stream pool": {
			func(c *Config) {
				c.Write.EnableMultiplexing = false
				c.Write.StreamPoolSize = 4
				c.Write.ConnectionPoolSize = 2
			},
			"write.connection_pool_size (2) must be at least write.stream_pool_size (4) when write.enable_multiplexing is false",
		},
	}
	for name, tc := range rejects {
		t.Run("reject/"+name, func(t *testing.T) {
			cfg := validConfig()
			tc.mutate(cfg)
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate() = %v, want error containing %q", err, tc.want)
			}
		})
	}
}
