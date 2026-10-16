// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter"

import (
	"errors"
	"fmt"
	"maps"
	"net"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/redact"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/transform"
)

// apiRequestLimitBytes is the hard per-AppendRows request limit imposed by the
// BigQuery Storage Write API. It is not configurable; Write.MaxRequestBytes is
// our own headroom threshold beneath it.
const apiRequestLimitBytes = 10 * 1000 * 1000

var (
	// Dataset and table IDs accept letters, numbers and underscores.
	idPattern = regexp.MustCompile(`^[A-Za-z0-9_]+$`)
	// Project IDs are lowercase letters, digits and hyphens, with an optional
	// organization domain prefix (e.g. "example.com:my-project").
	projectPattern = regexp.MustCompile(`^(?:[a-z0-9.-]+\.[a-z]{2,}:)?[a-z][a-z0-9-]{4,28}[a-z0-9]$`)
	// KMSKeyName must be a Cloud KMS CryptoKey resource path.
	kmsKeyPattern = regexp.MustCompile(`^projects/[^/\s]+/locations/[^/\s]+/keyRings/[^/\s]+/cryptoKeys/[^/\s]+$`)
	// BigQuery label keys and values follow Google Cloud resource label rules.
	labelKeyPattern   = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,62}$`)
	labelValuePattern = regexp.MustCompile(`^[a-z0-9_-]{0,63}$`)
)

const (
	maxBigQueryLabels = 64
	minTableStaleness = time.Second
	maxTableStaleness = 7 * 24 * time.Hour
)

// Config defines configuration for the BigQuery exporter.
//
// There is no mode enum: one schema, one exporter. A mode switch would turn
// the schema contract into a family of contracts, and every downstream query
// would have to know which one it was reading.
type Config struct {
	TimeoutSettings exporterhelper.TimeoutConfig                             `mapstructure:",squash"`
	QueueSettings   configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`
	BackOffConfig   configretry.BackOffConfig                                `mapstructure:"retry_on_failure"`

	// Project is the Google Cloud project owning the destination dataset.
	Project string `mapstructure:"project"`
	// Dataset is the destination BigQuery dataset ID.
	Dataset string `mapstructure:"dataset"`
	// Location is the dataset location. Required only when creating a
	// dataset; otherwise it is informational.
	Location string `mapstructure:"location"`
	// KMSKeyName is the default Cloud KMS key resource name
	// (projects/{p}/locations/{l}/keyRings/{r}/cryptoKeys/{k}) applied to
	// auto-created datasets and tables unless overridden per table.
	KMSKeyName string `mapstructure:"kms_key_name"`
	// Labels are default Google Cloud resource labels applied to auto-created
	// datasets, tables, and views, merged with any per-table labels.
	Labels map[string]string `mapstructure:"labels"`

	// Traces, Logs, and Metrics configure the three signal pipelines.
	Traces  SignalConfig `mapstructure:"traces"`
	Logs    LogsConfig   `mapstructure:"logs"`
	Metrics SignalConfig `mapstructure:"metrics"`

	// Credentials selects how the exporter authenticates.
	Credentials CredentialsConfig `mapstructure:"credentials"`

	// Endpoint overrides the Storage Write API and REST v2 control-plane endpoints.
	Endpoint EndpointConfig `mapstructure:"endpoint"`

	// AutoCreate governs dataset and table creation. Creation is a separate
	// control-plane concern; leaving it off keeps the exporter on a
	// write-only IAM path.
	AutoCreate AutoCreateConfig `mapstructure:"auto_create"`

	// Write holds Storage Write API tuning.
	Write WriteConfig `mapstructure:"write"`

	// Limits are the structural bounds applied before serialization.
	Limits LimitsConfig `mapstructure:"limits"`

	// Quarantine routes records the signal tables cannot take to an opt-in
	// dead-letter table instead of only counting them.
	Quarantine QuarantineConfig `mapstructure:"quarantine"`

	// Offload routes oversized span/log values and quarantine payloads to
	// Google Cloud Storage and records ObjectRef references in BigQuery.
	Offload OffloadConfig `mapstructure:"offload"`

	// Redaction scrubs sensitive attribute keys and string values from
	// telemetry before any offload, size check, fingerprint, or quarantine
	// serialization sees it.
	Redaction RedactionConfig `mapstructure:"redaction"`
}

// SignalConfig is the per-signal destination, partition lifecycle, and
// governance policy.
type SignalConfig struct {
	Table                   string            `mapstructure:"table"`
	PartitionExpirationDays int               `mapstructure:"partition_expiration_days"`
	RequirePartitionFilter  bool              `mapstructure:"require_partition_filter"`
	KMSKeyName              string            `mapstructure:"kms_key_name"`
	Labels                  map[string]string `mapstructure:"labels"`
	MaxStaleness            string            `mapstructure:"max_staleness"`
}

// LogsConfig adds the log-only record identity settings.
type LogsConfig struct {
	Table                   string            `mapstructure:"table"`
	PartitionExpirationDays int               `mapstructure:"partition_expiration_days"`
	RequirePartitionFilter  bool              `mapstructure:"require_partition_filter"`
	KMSKeyName              string            `mapstructure:"kms_key_name"`
	Labels                  map[string]string `mapstructure:"labels"`
	MaxStaleness            string            `mapstructure:"max_staleness"`

	// LogNameAttribute names a string log record attribute that carries the
	// log_name column. When unset, or absent on a record, log_name falls back
	// to well-known log name attributes, then to the service name, then to
	// "default".
	LogNameAttribute string `mapstructure:"log_name_attribute"`

	// InsertIDAttribute names a string log record attribute that carries an
	// upstream record ID for the insert_id column. When unset, or absent on a
	// record, insert_id falls back to well-known insert ID attributes, then to
	// the record fingerprint.
	//
	// The dedup view treats insert_id as record identity within a log stream
	// and timestamp, so only name an attribute whose values are unique per
	// record.
	InsertIDAttribute string `mapstructure:"insert_id_attribute"`
}

// CredentialsConfig selects an authentication method and optional enterprise
// project/domain overrides. At most one explicit credential source (file, json,
// or impersonate_service_account) may be set; the default is application
// default credentials.
type CredentialsConfig struct {
	// File is a path to a Google credentials JSON file. The file's type must be
	// one of service_account, authorized_user, external_account,
	// external_account_authorized_user, or impersonated_service_account.
	File string `mapstructure:"file"`
	// JSON is an inline credential JSON payload, masked when formatted.
	JSON configopaque.String `mapstructure:"json"`
	// ImpersonateServiceAccount is the target principal for impersonation,
	// using the ambient credentials as the source.
	ImpersonateServiceAccount string `mapstructure:"impersonate_service_account"`
	// ImpersonateDelegates is an optional chain of intermediate service
	// account principals for delegated impersonation. Requires
	// ImpersonateServiceAccount.
	ImpersonateDelegates []string `mapstructure:"impersonate_delegates"`
	// QuotaProject overrides the project billed and charged for API quota.
	QuotaProject string `mapstructure:"quota_project"`
	// UniverseDomain overrides the Google Cloud universe domain (default
	// "googleapis.com") for Sovereign Cloud / Trusted Partner Cloud
	// deployments.
	UniverseDomain string `mapstructure:"universe_domain"`
}

// EndpointConfig overrides the service endpoints for emulator, Private Google
// Access, Private Service Connect, and Sovereign Cloud use.
//
// Insecure transport and disabled authentication are separate explicit flags
// rather than an inferred consequence of setting an endpoint. Someone pointing
// at an internal proxy with real credentials should not silently lose TLS.
type EndpointConfig struct {
	// URL is the alternate Storage Write API gRPC endpoint.
	URL string `mapstructure:"url"`
	// AdminURL is the alternate BigQuery REST v2 control-plane endpoint
	// used during startup metadata validation and DDL execution.
	AdminURL string `mapstructure:"admin_url"`
	// Insecure disables TLS. Refused unless the endpoint is a loopback
	// address.
	Insecure bool `mapstructure:"insecure"`
	// WithoutAuthentication sends no credentials. Refused unless the
	// endpoint is a loopback address.
	WithoutAuthentication bool `mapstructure:"without_authentication"`
}

// AutoCreateConfig controls creation of the destination dataset, tables,
// views, and search indexes.
type AutoCreateConfig struct {
	Dataset bool `mapstructure:"dataset"`
	Tables  bool `mapstructure:"tables"`
	// CompanionViews creates the companion views over the dedup views if absent
	// and reconciles an existing view to the current definition (the query
	// only, best effort): otel_spans_as_allspans, v_gen_ai_spans,
	// v_gen_ai_model_costs, v_gen_ai_sessions, and v_gen_ai_tool_usage with the
	// traces table, v_gen_ai_logs with the logs table once the traces dedup
	// view exists, and v_gen_ai_metrics and v_gen_ai_metric_exemplars with the
	// metrics table (the latter once the traces dedup view exists).
	// Requires tables.
	CompanionViews bool `mapstructure:"companion_views"`
	// AllowUnprunableViews creates the cross-signal companion views
	// (v_gen_ai_logs, v_gen_ai_metric_exemplars) even when the spans table
	// requires a partition filter. Those views join the span deduplication
	// view on a row-dependent timestamp window rather than a constant
	// start_time filter, so BigQuery rejects queries over them when the spans
	// table requires a partition filter. By default they are therefore skipped
	// with a warning at startup in that case; the single-signal companion
	// views are created either way. Whether the spans table requires a
	// partition filter is read from its metadata at startup, and
	// traces.require_partition_filter is consulted only while the table does
	// not exist yet.
	AllowUnprunableViews bool `mapstructure:"allow_unprunable_views"`
	// SearchIndexes runs CREATE SEARCH INDEX IF NOT EXISTS on signal tables
	// at startup. Requires tables and bigquery.jobs.create.
	SearchIndexes bool `mapstructure:"search_indexes"`
}

// WriteConfig holds Storage Write API tuning knobs.
type WriteConfig struct {
	// MaxRequestBytes is our own request-size headroom threshold, enforced
	// after protobuf serialization and before AppendRows. It is deliberately
	// distinct from, and below, the API's own per-request limit.
	MaxRequestBytes int `mapstructure:"max_request_bytes"`
	// MaxRowBytes rejects individual serialized rows above this size.
	MaxRowBytes int `mapstructure:"max_row_bytes"`
	// MaxInflightRequests bounds asynchronous in-flight appends.
	MaxInflightRequests int `mapstructure:"max_inflight_requests"`
	// MaxInflightBytes bounds the memory held by in-flight appends.
	MaxInflightBytes int `mapstructure:"max_inflight_bytes"`
	// EnableMultiplexing enables BigQuery Storage Write API connection
	// sharing across default write streams.
	EnableMultiplexing bool `mapstructure:"enable_multiplexing"`
	// ConnectionPoolSize sets the gRPC connection pool size for the
	// Storage Write client when multiplexing is enabled.
	ConnectionPoolSize int `mapstructure:"connection_pool_size"`
	// Compression selects the gRPC compressor for AppendRows calls ("" or "gzip").
	Compression string `mapstructure:"compression"`
	// Keepalive configures gRPC client keepalive parameters for Storage
	// Write streams.
	Keepalive KeepaliveConfig `mapstructure:"keepalive"`
}

// KeepaliveConfig configures gRPC client keepalive parameters.
type KeepaliveConfig struct {
	// Time is the idle duration after which the client pings the server.
	Time time.Duration `mapstructure:"time"`
	// Timeout is the duration the client waits for a keepalive ping ack.
	Timeout time.Duration `mapstructure:"timeout"`
	// PermitWithoutStream sends keepalive pings even with no active RPCs.
	PermitWithoutStream bool `mapstructure:"permit_without_stream"`
}

// LimitsConfig mirrors transform.Limits in configuration.
type LimitsConfig struct {
	MaxAttributeCount    int `mapstructure:"max_attribute_count"`
	MaxEventCount        int `mapstructure:"max_event_count"`
	MaxLinkCount         int `mapstructure:"max_link_count"`
	MaxNestingDepth      int `mapstructure:"max_nesting_depth"`
	MaxCollectionSize    int `mapstructure:"max_collection_size"`
	MaxAttributeKeyBytes int `mapstructure:"max_attribute_key_bytes"`
	MaxValueBytes        int `mapstructure:"max_value_bytes"`
}

func (l LimitsConfig) toTransform() transform.Limits {
	return transform.Limits{
		MaxAttributeCount:    l.MaxAttributeCount,
		MaxEventCount:        l.MaxEventCount,
		MaxLinkCount:         l.MaxLinkCount,
		MaxNestingDepth:      l.MaxNestingDepth,
		MaxCollectionSize:    l.MaxCollectionSize,
		MaxAttributeKeyBytes: l.MaxAttributeKeyBytes,
		MaxValueBytes:        l.MaxValueBytes,
	}
}

// QuarantineConfig controls the opt-in dead-letter table.
//
// When enabled, a record that is permanently rejected, whether by the record
// contract, a structural limit, serialization, the row size limit or the
// service's own row validation, is also written to Table with its error code
// and its original content, so it can be inspected and replayed. Quarantine
// writes are best effort: they are never retried, and they never change the
// outcome of the signal write that rejected the record.
type QuarantineConfig struct {
	Enabled bool `mapstructure:"enabled"`
	// Table is the quarantine table ID in the destination dataset.
	Table                   string            `mapstructure:"table"`
	PartitionExpirationDays int               `mapstructure:"partition_expiration_days"`
	RequirePartitionFilter  bool              `mapstructure:"require_partition_filter"`
	KMSKeyName              string            `mapstructure:"kms_key_name"`
	Labels                  map[string]string `mapstructure:"labels"`
	MaxStaleness            string            `mapstructure:"max_staleness"`
	// MetadataOnly omits raw_payload, raw_attributes, and payload_ref from
	// quarantine rows when enabled, writing only metadata columns.
	MetadataOnly bool `mapstructure:"metadata_only"`
	// MaxRawPayloadBytes bounds the UTF-8 byte length of raw_payload in
	// quarantine rows before any row-size cap is applied.
	MaxRawPayloadBytes int `mapstructure:"max_raw_payload_bytes"`
	// Timeout bounds each best-effort quarantine Append call on a context
	// detached from the caller's export cancellation.
	Timeout time.Duration `mapstructure:"timeout"`
}

// OffloadConfig configures optional large-payload offloading to Google Cloud
// Storage with BigQuery ObjectRef references in payload_refs and quarantine
// payload_ref.
type OffloadConfig struct {
	Enabled bool `mapstructure:"enabled"`
	// Bucket is the destination GCS bucket name (without gs://).
	Bucket string `mapstructure:"bucket"`
	// Prefix is the object key prefix within Bucket.
	Prefix string `mapstructure:"prefix"`
	// Authorizer is the optional BigQuery connection resource name
	// recorded in ObjectRef.authorizer.
	Authorizer string `mapstructure:"authorizer"`
	// MinBytes is the minimum byte length for a value to be offloaded.
	MinBytes int `mapstructure:"min_bytes"`
	// MaxObjectBytes is the maximum byte length eligible for offloading.
	MaxObjectBytes int `mapstructure:"max_object_bytes"`
	// MaxConcurrency bounds concurrent GCS uploads within an offload batch.
	MaxConcurrency int `mapstructure:"max_concurrency"`
	// CacheSize bounds the in-memory LRU cache of content-addressed object
	// generations across batches (0 disables cross-batch caching).
	CacheSize int `mapstructure:"cache_size"`
	// Timeout bounds each object upload attempt.
	Timeout time.Duration `mapstructure:"timeout"`
}

// RedactionConfig configures client-side scrubbing of sensitive attribute
// keys and string values. Redaction runs as the first step of every
// transform, on a copy of the batch, so the Storage Write API, the GCS
// offloader, the record fingerprints, and the quarantine table only ever see
// the masked form.
type RedactionConfig struct {
	Enabled bool `mapstructure:"enabled"`
	// BlockedKeys are attribute keys whose whole value is replaced by
	// MaskReplacement. Matching is case-insensitive; a key containing '*' is
	// a glob. Merged with the built-in list unless DisableDefaultKeys is set.
	BlockedKeys []string `mapstructure:"blocked_keys"`
	// DisableDefaultKeys drops the built-in key list (authorization, cookie,
	// api_key, *token, *secret*, *password*, ...) so only BlockedKeys apply.
	DisableDefaultKeys bool `mapstructure:"disable_default_keys"`
	// BlockedValuePatterns are RE2 regular expressions; every match inside a
	// string value is replaced by MaskReplacement.
	BlockedValuePatterns []string `mapstructure:"blocked_value_patterns"`
	// MaskReplacement is the replacement text. Defaults to "[REDACTED]".
	MaskReplacement string `mapstructure:"mask_replacement"`
	// RedactLogBody also scans string, map, and slice log bodies.
	RedactLogBody bool `mapstructure:"redact_log_body"`
}

// effectiveBlockedKeys is the key list a Redactor is built from: the
// built-in defaults unless disabled, followed by the configured keys.
func (r RedactionConfig) effectiveBlockedKeys() []string {
	var keys []string
	if !r.DisableDefaultKeys {
		keys = append(keys, redact.DefaultBlockedKeys()...)
	}
	return append(keys, r.BlockedKeys...)
}

// toRedact builds the redact.Config for this configuration. onRedact may be
// nil.
func (r RedactionConfig) toRedact(onRedact func(matchType string, n int)) redact.Config {
	mask := r.MaskReplacement
	if mask == "" {
		mask = redact.DefaultMaskReplacement
	}
	return redact.Config{
		BlockedKeys:          r.effectiveBlockedKeys(),
		BlockedValuePatterns: r.BlockedValuePatterns,
		MaskReplacement:      mask,
		RedactLogBody:        r.RedactLogBody,
		OnRedact:             onRedact,
	}
}

// Validate checks the configuration and fails fast at startup.
func (cfg *Config) Validate() error {
	var errs []error
	if cfg.TimeoutSettings.Timeout <= 0 {
		errs = append(errs, errors.New("timeout must be positive so startup and export attempts are bounded"))
	}

	if cfg.Project == "" {
		errs = append(errs, errors.New("project must be specified"))
	} else if !projectPattern.MatchString(cfg.Project) {
		errs = append(errs, fmt.Errorf("project %q is not a valid Google Cloud project ID", cfg.Project))
	}

	if cfg.Dataset == "" {
		errs = append(errs, errors.New("dataset must be specified"))
	} else if !idPattern.MatchString(cfg.Dataset) {
		errs = append(errs, fmt.Errorf("dataset %q must contain only letters, numbers and underscores", cfg.Dataset))
	}

	if cfg.Location != "" && (strings.TrimSpace(cfg.Location) == "" || strings.ContainsAny(cfg.Location, " \t\r\n")) {
		errs = append(errs, fmt.Errorf("location %q must not be blank or contain whitespace", cfg.Location))
	}

	for name, table := range map[string]string{
		"traces.table":  cfg.Traces.Table,
		"logs.table":    cfg.Logs.Table,
		"metrics.table": cfg.Metrics.Table,
	} {
		if table == "" {
			errs = append(errs, fmt.Errorf("%s must not be empty", name))
		} else if !idPattern.MatchString(table) {
			errs = append(errs, fmt.Errorf("%s %q must contain only letters, numbers and underscores", name, table))
		}
	}

	for name, days := range map[string]int{
		"traces.partition_expiration_days":     cfg.Traces.PartitionExpirationDays,
		"logs.partition_expiration_days":       cfg.Logs.PartitionExpirationDays,
		"metrics.partition_expiration_days":    cfg.Metrics.PartitionExpirationDays,
		"quarantine.partition_expiration_days": cfg.Quarantine.PartitionExpirationDays,
	} {
		if days < 0 {
			errs = append(errs, fmt.Errorf("%s (%d) must not be negative", name, days))
		}
	}

	errs = append(errs, cfg.validateGovernance()...)
	errs = append(errs, cfg.validateWrite()...)
	errs = append(errs, cfg.validateCredentials()...)
	errs = append(errs, cfg.validateEndpoint()...)
	errs = append(errs, cfg.validateQuarantine()...)
	errs = append(errs, cfg.validateOffload()...)
	errs = append(errs, cfg.validateRedaction()...)

	if err := cfg.Limits.toTransform().Validate(); err != nil {
		errs = append(errs, err)
	}

	if cfg.AutoCreate.Dataset && cfg.Location == "" {
		errs = append(errs, errors.New("location is required when auto_create.dataset is enabled"))
	}
	if cfg.AutoCreate.CompanionViews && !cfg.AutoCreate.Tables {
		errs = append(errs, errors.New("auto_create.companion_views requires auto_create.tables"))
	}
	if cfg.AutoCreate.SearchIndexes && !cfg.AutoCreate.Tables {
		errs = append(errs, errors.New("auto_create.search_indexes requires auto_create.tables"))
	}
	if cfg.Endpoint.WithoutAuthentication {
		if cfg.AutoCreate.Dataset {
			errs = append(errs, errors.New(
				"auto_create.dataset cannot be enabled with endpoint.without_authentication"))
		}
		if cfg.AutoCreate.Tables {
			errs = append(errs, errors.New(
				"auto_create.tables cannot be enabled with endpoint.without_authentication"))
		}
		if cfg.AutoCreate.SearchIndexes {
			errs = append(errs, errors.New(
				"auto_create.search_indexes cannot be enabled with endpoint.without_authentication"))
		}
	}

	if !cfg.BackOffConfig.Enabled {
		// Not an error, but worth stating: with retries off, a transient
		// BigQuery blip drops data that the no-loss contract otherwise covers.
		_ = cfg.BackOffConfig
	} else if cfg.BackOffConfig.MaxElapsedTime == 0 {
		// An unbounded retry horizon makes the at-most-N+1 duplicate bound
		// meaningless, because N is then unbounded.
		errs = append(errs, errors.New(
			"retry_on_failure.max_elapsed_time must be finite: an unbounded horizon "+
				"removes the bound on duplicate copies per logical record"))
	}

	return errors.Join(errs...)
}

func (cfg *Config) validateGovernance() []error {
	var errs []error
	for _, item := range []struct {
		field string
		value string
	}{
		{"kms_key_name", cfg.KMSKeyName},
		{"traces.kms_key_name", cfg.Traces.KMSKeyName},
		{"logs.kms_key_name", cfg.Logs.KMSKeyName},
		{"metrics.kms_key_name", cfg.Metrics.KMSKeyName},
		{"quarantine.kms_key_name", cfg.Quarantine.KMSKeyName},
	} {
		if item.value != "" && !kmsKeyPattern.MatchString(item.value) {
			errs = append(errs, fmt.Errorf(
				"%s %q must match projects/{project}/locations/{location}/keyRings/{keyRing}/cryptoKeys/{cryptoKey}",
				item.field, item.value))
		}
	}

	for _, item := range []struct {
		field  string
		labels map[string]string
	}{
		{"labels", cfg.Labels},
		{"traces.labels", cfg.Traces.Labels},
		{"logs.labels", cfg.Logs.Labels},
		{"metrics.labels", cfg.Metrics.Labels},
		{"quarantine.labels", cfg.Quarantine.Labels},
	} {
		if err := validateLabelsMap(item.field, item.labels); err != nil {
			errs = append(errs, err)
		}
		if item.field != "labels" && len(cfg.Labels) > 0 && len(item.labels) > 0 {
			if merged := mergeLabels(cfg.Labels, item.labels); len(merged) > maxBigQueryLabels {
				errs = append(errs, fmt.Errorf(
					"merged labels and %s have %d entries; BigQuery allows at most %d labels",
					item.field, len(merged), maxBigQueryLabels))
			}
		}
	}

	for _, item := range []struct {
		field string
		value string
	}{
		{"traces.max_staleness", cfg.Traces.MaxStaleness},
		{"logs.max_staleness", cfg.Logs.MaxStaleness},
		{"metrics.max_staleness", cfg.Metrics.MaxStaleness},
		{"quarantine.max_staleness", cfg.Quarantine.MaxStaleness},
	} {
		if _, err := parseMaxStaleness(item.field, item.value); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

func validateLabelsMap(field string, m map[string]string) error {
	if len(m) > maxBigQueryLabels {
		return fmt.Errorf("%s has %d entries; BigQuery allows at most %d labels", field, len(m), maxBigQueryLabels)
	}
	for _, k := range slices.Sorted(maps.Keys(m)) {
		v := m[k]
		if !labelKeyPattern.MatchString(k) {
			return fmt.Errorf(
				"%s key %q must start with a lowercase letter and contain at most 63 lowercase letters, digits, underscores, or hyphens",
				field, k)
		}
		if !labelValuePattern.MatchString(v) {
			return fmt.Errorf(
				"%s[%q] value %q must contain at most 63 lowercase letters, digits, underscores, or hyphens",
				field, k, v)
		}
	}
	return nil
}

func parseMaxStaleness(field, raw string) (time.Duration, error) {
	if raw == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s %q is not a valid duration: %w", field, raw, err)
	}
	if d < minTableStaleness || d > maxTableStaleness {
		return 0, fmt.Errorf("%s (%v) must be between %v and %v", field, d, minTableStaleness, maxTableStaleness)
	}
	return d, nil
}

func (cfg *Config) validateWrite() []error {
	var errs []error
	w := cfg.Write

	if w.MaxRequestBytes <= 0 {
		errs = append(errs, errors.New("write.max_request_bytes must be positive"))
	} else if w.MaxRequestBytes >= apiRequestLimitBytes {
		errs = append(errs, fmt.Errorf(
			"write.max_request_bytes (%d) must stay below the Storage Write API per-request limit of %d bytes",
			w.MaxRequestBytes, apiRequestLimitBytes))
	}

	if w.MaxRowBytes <= 0 {
		errs = append(errs, errors.New("write.max_row_bytes must be positive"))
	} else if w.MaxRowBytes > w.MaxRequestBytes {
		errs = append(errs, fmt.Errorf(
			"write.max_row_bytes (%d) must not exceed write.max_request_bytes (%d), otherwise a legal row can never be sent",
			w.MaxRowBytes, w.MaxRequestBytes))
	}

	if w.MaxInflightRequests <= 0 {
		errs = append(errs, errors.New("write.max_inflight_requests must be positive"))
	}
	if w.MaxInflightBytes <= 0 {
		errs = append(errs, errors.New("write.max_inflight_bytes must be positive"))
	}
	if w.MaxInflightBytes > 0 && w.MaxRequestBytes > 0 && w.MaxInflightBytes < w.MaxRequestBytes {
		errs = append(errs, fmt.Errorf(
			"write.max_inflight_bytes (%d) is below write.max_request_bytes (%d): a single full request could never be sent",
			w.MaxInflightBytes, w.MaxRequestBytes))
	}
	if w.ConnectionPoolSize < 0 {
		errs = append(errs, errors.New("write.connection_pool_size must not be negative"))
	} else if w.EnableMultiplexing && w.ConnectionPoolSize <= 0 {
		errs = append(errs, errors.New("write.connection_pool_size must be positive when write.enable_multiplexing is enabled"))
	}
	if w.Compression != "" && w.Compression != "gzip" {
		errs = append(errs, fmt.Errorf("write.compression %q must be empty or \"gzip\"", w.Compression))
	}
	if w.Keepalive.Time < 0 {
		errs = append(errs, errors.New("write.keepalive.time must not be negative"))
	}
	if w.Keepalive.Timeout < 0 {
		errs = append(errs, errors.New("write.keepalive.timeout must not be negative"))
	}
	return errs
}

func (cfg *Config) validateCredentials() []error {
	var errs []error
	c := cfg.Credentials
	if c.File != "" && c.JSON != "" {
		errs = append(errs, errors.New(
			"credentials.file and credentials.json are mutually exclusive"))
	}
	if c.File != "" && c.ImpersonateServiceAccount != "" {
		errs = append(errs, errors.New(
			"credentials.file and credentials.impersonate_service_account are mutually exclusive"))
	}
	if c.JSON != "" && c.ImpersonateServiceAccount != "" {
		errs = append(errs, errors.New(
			"credentials.json and credentials.impersonate_service_account are mutually exclusive"))
	}
	if len(c.ImpersonateDelegates) > 0 && c.ImpersonateServiceAccount == "" {
		errs = append(errs, errors.New(
			"credentials.impersonate_delegates requires credentials.impersonate_service_account"))
	}
	return errs
}

func (cfg *Config) validateEndpoint() []error {
	var errs []error
	e := cfg.Endpoint

	// The metadata client always sends credentials, whatever
	// without_authentication says about the Storage Write endpoint. Plain HTTP
	// is therefore acceptable only to a loopback emulator; anywhere else it
	// would put the bearer token on the wire in the clear.
	if e.AdminURL != "" {
		u, err := url.Parse(e.AdminURL)
		switch {
		case err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http"):
			errs = append(errs, fmt.Errorf(
				"endpoint.admin_url must be an absolute http(s):// URL, got %q", e.AdminURL))
		case u.Scheme == "http" && !isLoopbackHost(u.Hostname()):
			errs = append(errs, fmt.Errorf(
				"endpoint.admin_url must use https:// unless it is a loopback address, got %q", e.AdminURL))
		}
	}

	if e.URL == "" {
		if e.Insecure {
			errs = append(errs, errors.New("endpoint.insecure requires endpoint.url"))
		}
		if e.WithoutAuthentication {
			errs = append(errs, errors.New("endpoint.without_authentication requires endpoint.url"))
		}
		return errs
	}

	// Turning off TLS or authentication against a non-loopback address would
	// put real credentials, or real telemetry, on the wire in the clear. The
	// override exists for emulators, so require the endpoint to look like one.
	if e.Insecure && !isLoopback(e.URL) {
		errs = append(errs, fmt.Errorf(
			"endpoint.insecure is only allowed for a loopback endpoint, got %q", e.URL))
	}
	if e.WithoutAuthentication && !isLoopback(e.URL) {
		errs = append(errs, fmt.Errorf(
			"endpoint.without_authentication is only allowed for a loopback endpoint, got %q", e.URL))
	}
	return errs
}

func (cfg *Config) validateQuarantine() []error {
	var errs []error
	q := cfg.Quarantine

	if q.Table != "" && !idPattern.MatchString(q.Table) {
		errs = append(errs, fmt.Errorf(
			"quarantine.table %q must contain only letters, numbers and underscores", q.Table))
	}
	if q.MaxRawPayloadBytes < 0 {
		errs = append(errs, errors.New("quarantine.max_raw_payload_bytes must not be negative"))
	}
	if q.Timeout < 0 {
		errs = append(errs, errors.New("quarantine.timeout must not be negative"))
	}
	if !q.Enabled {
		return errs
	}
	switch q.Table {
	case "":
		errs = append(errs, errors.New("quarantine.table must not be empty when quarantine.enabled is set"))
	case cfg.Traces.Table, cfg.Logs.Table, cfg.Metrics.Table:
		// The quarantine table has its own schema, so sharing a signal
		// table could only ever fail schema validation at startup.
		errs = append(errs, fmt.Errorf(
			"quarantine.table %q must differ from traces.table and logs.table (and metrics.table)", q.Table))
	}
	if q.MaxRawPayloadBytes <= 0 {
		errs = append(errs, errors.New("quarantine.max_raw_payload_bytes must be positive when quarantine.enabled is set"))
	}
	if q.Timeout <= 0 {
		errs = append(errs, errors.New("quarantine.timeout must be positive when quarantine.enabled is set"))
	}
	return errs
}

func (cfg *Config) validateOffload() []error {
	var errs []error
	o := cfg.Offload

	if o.MinBytes < 0 {
		errs = append(errs, errors.New("offload.min_bytes must not be negative"))
	}
	if o.MaxObjectBytes < 0 {
		errs = append(errs, errors.New("offload.max_object_bytes must not be negative"))
	}
	if o.CacheSize < 0 {
		errs = append(errs, errors.New("offload.cache_size must not be negative"))
	}
	if o.Timeout < 0 {
		errs = append(errs, errors.New("offload.timeout must not be negative"))
	}
	if !o.Enabled {
		return errs
	}
	bucket := strings.TrimSpace(o.Bucket)
	if bucket == "" {
		errs = append(errs, errors.New("offload.bucket must not be empty when offload.enabled is set"))
	} else if strings.HasPrefix(bucket, "gs://") || strings.Contains(bucket, "/") {
		errs = append(errs, fmt.Errorf("offload.bucket %q must be a bare bucket name without gs:// or slashes", o.Bucket))
	}
	if o.MinBytes <= 0 {
		errs = append(errs, errors.New("offload.min_bytes must be positive when offload.enabled is set"))
	}
	if o.MaxObjectBytes < o.MinBytes {
		errs = append(errs, fmt.Errorf(
			"offload.max_object_bytes (%d) must be at least offload.min_bytes (%d)",
			o.MaxObjectBytes, o.MinBytes))
	}
	if o.MaxConcurrency <= 0 {
		errs = append(errs, errors.New("offload.max_concurrency must be positive when offload.enabled is set"))
	}
	if o.Timeout <= 0 {
		errs = append(errs, errors.New("offload.timeout must be positive when offload.enabled is set"))
	}
	return errs
}

// validateRedaction compiles the value patterns whether or not redaction is
// enabled, so a typo surfaces before the operator flips the switch, and
// checks the enabled configuration can actually be built.
func (cfg *Config) validateRedaction() []error {
	var errs []error
	r := cfg.Redaction
	if _, err := redact.CompilePatterns(r.BlockedValuePatterns); err != nil {
		errs = append(errs, fmt.Errorf("redaction.%w", err))
	}
	for i, k := range r.BlockedKeys {
		if strings.TrimSpace(k) == "" {
			errs = append(errs, fmt.Errorf("redaction.blocked_keys[%d] is empty", i))
		}
	}
	if !r.Enabled || len(errs) > 0 {
		return errs
	}
	if len(r.effectiveBlockedKeys()) == 0 && len(r.BlockedValuePatterns) == 0 {
		return append(errs, errors.New(
			"redaction.enabled is set but disable_default_keys leaves no blocked_keys and no blocked_value_patterns"))
	}
	rd, err := redact.New(r.toRedact(nil))
	if err != nil {
		return append(errs, fmt.Errorf("redaction: %w", err))
	}
	// A log identity attribute that the key list masks would give every
	// record the same insert_id or log_name and collapse them in the dedup
	// view, so refuse the combination up front.
	for _, item := range []struct{ field, key string }{
		{"logs.insert_id_attribute", cfg.Logs.InsertIDAttribute},
		{"logs.log_name_attribute", cfg.Logs.LogNameAttribute},
	} {
		if item.key != "" && rd.KeyBlocked(item.key) {
			errs = append(errs, fmt.Errorf(
				"%s %q matches a blocked redaction key (built-in or redaction.blocked_keys) and would be masked on every record", item.field, item.key))
		}
	}
	return errs
}

// isLoopback reports whether the endpoint addresses the local machine.
func isLoopback(endpoint string) bool {
	host := endpoint
	for _, prefix := range []string{"http://", "https://", "dns:///", "passthrough:///"} {
		host = trimPrefix(host, prefix)
	}
	if i := indexByte(host, '/'); i >= 0 {
		host = host[:i]
	}
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	} else if i := lastIndexByte(host, ':'); i >= 0 && !strings.Contains(host, "]") {
		host = host[:i]
	}
	return isLoopbackHost(host)
}

func isLoopbackHost(host string) bool {
	h := strings.Trim(host, "[]")
	switch h {
	case "localhost", "127.0.0.1", "::1":
		return true
	}
	ip := net.ParseIP(h)
	return ip != nil && ip.IsLoopback()
}

func trimPrefix(s, prefix string) string {
	if len(s) >= len(prefix) && s[:len(prefix)] == prefix {
		return s[len(prefix):]
	}
	return s
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

func lastIndexByte(s string, b byte) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == b {
			return i
		}
	}
	return -1
}
