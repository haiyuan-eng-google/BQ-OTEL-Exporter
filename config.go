// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter"

import (
	"errors"
	"fmt"
	"regexp"

	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/transform"
)

// apiRequestLimitBytes is the hard per-AppendRows request limit imposed by the
// BigQuery Storage Write API. It is not configurable; Write.MaxRequestBytes is
// our own headroom threshold beneath it.
const apiRequestLimitBytes = 10 * 1000 * 1000

var (
	// Dataset and table IDs accept letters, numbers and underscores.
	idPattern = regexp.MustCompile(`^[A-Za-z0-9_]+$`)
	// Project IDs are lowercase letters, digits and hyphens.
	projectPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{4,28}[a-z0-9]$`)
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

	// Traces and Logs configure the two signal pipelines.
	Traces SignalConfig `mapstructure:"traces"`
	Logs   LogsConfig   `mapstructure:"logs"`

	// Credentials selects how the exporter authenticates.
	Credentials CredentialsConfig `mapstructure:"credentials"`

	// Endpoint overrides the Storage Write API endpoint. Emulator and
	// development use only.
	Endpoint EndpointConfig `mapstructure:"endpoint"`

	// AutoCreate governs dataset and table creation. Creation is a separate
	// control-plane concern; leaving it off keeps the exporter on a
	// write-only IAM path.
	AutoCreate AutoCreateConfig `mapstructure:"auto_create"`

	// Write holds Storage Write API tuning.
	Write WriteConfig `mapstructure:"write"`

	// Limits are the structural bounds applied before serialization.
	Limits LimitsConfig `mapstructure:"limits"`
}

// SignalConfig is the per-signal destination.
type SignalConfig struct {
	Table string `mapstructure:"table"`
}

// LogsConfig adds the log-only record identity settings.
type LogsConfig struct {
	Table string `mapstructure:"table"`

	// SourceRecordNamespace is the trusted namespace for source_record_id.
	//
	// It must come from deployment-controlled configuration, never from an
	// application-supplied attribute: it participates in record identity, so
	// letting a payload choose it would let a payload claim another
	// producer's identity.
	SourceRecordNamespace string `mapstructure:"source_record_namespace"`

	// SourceRecordIDAttribute names the log attribute carrying a stable
	// source record ID. Only honored when SourceRecordNamespace is also set.
	SourceRecordIDAttribute string `mapstructure:"source_record_id_attribute"`
}

// CredentialsConfig selects an authentication method. At most one may be set;
// the default is application default credentials.
type CredentialsConfig struct {
	// File is a path to a service account key file.
	File string `mapstructure:"file"`
	// ImpersonateServiceAccount is the target principal for impersonation,
	// using the ambient credentials as the source.
	ImpersonateServiceAccount string `mapstructure:"impersonate_service_account"`
}

// EndpointConfig overrides the service endpoint for emulator and development
// use.
//
// Insecure transport and disabled authentication are separate explicit flags
// rather than an inferred consequence of setting an endpoint. Someone pointing
// at an internal proxy with real credentials should not silently lose TLS.
type EndpointConfig struct {
	// URL is the alternate Storage Write API endpoint.
	URL string `mapstructure:"url"`
	// Insecure disables TLS. Refused unless the endpoint is a loopback
	// address.
	Insecure bool `mapstructure:"insecure"`
	// WithoutAuthentication sends no credentials. Refused unless the
	// endpoint is a loopback address.
	WithoutAuthentication bool `mapstructure:"without_authentication"`
}

// AutoCreateConfig controls creation of the destination dataset and tables.
type AutoCreateConfig struct {
	Dataset bool `mapstructure:"dataset"`
	Tables  bool `mapstructure:"tables"`
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
}

// LimitsConfig mirrors transform.Limits in configuration.
type LimitsConfig struct {
	MaxAttributeCount int `mapstructure:"max_attribute_count"`
	MaxEventCount     int `mapstructure:"max_event_count"`
	MaxLinkCount      int `mapstructure:"max_link_count"`
	MaxNestingDepth   int `mapstructure:"max_nesting_depth"`
	MaxCollectionSize int `mapstructure:"max_collection_size"`
	MaxValueBytes     int `mapstructure:"max_value_bytes"`
}

func (l LimitsConfig) toTransform() transform.Limits {
	return transform.Limits{
		MaxAttributeCount: l.MaxAttributeCount,
		MaxEventCount:     l.MaxEventCount,
		MaxLinkCount:      l.MaxLinkCount,
		MaxNestingDepth:   l.MaxNestingDepth,
		MaxCollectionSize: l.MaxCollectionSize,
		MaxValueBytes:     l.MaxValueBytes,
	}
}

// Validate checks the configuration and fails fast at startup.
func (cfg *Config) Validate() error {
	var errs []error

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

	for name, table := range map[string]string{
		"traces.table": cfg.Traces.Table,
		"logs.table":   cfg.Logs.Table,
	} {
		if table == "" {
			errs = append(errs, fmt.Errorf("%s must not be empty", name))
		} else if !idPattern.MatchString(table) {
			errs = append(errs, fmt.Errorf("%s %q must contain only letters, numbers and underscores", name, table))
		}
	}

	errs = append(errs, cfg.validateWrite()...)
	errs = append(errs, cfg.validateCredentials()...)
	errs = append(errs, cfg.validateEndpoint()...)

	if err := cfg.Limits.toTransform().Validate(); err != nil {
		errs = append(errs, err)
	}

	if cfg.AutoCreate.Dataset && cfg.Location == "" {
		errs = append(errs, errors.New("location is required when auto_create.dataset is enabled"))
	}

	// An ID without a namespace is not an identity: two producers could mint
	// the same ID, and the dedup view would merge unrelated records.
	if cfg.Logs.SourceRecordIDAttribute != "" && cfg.Logs.SourceRecordNamespace == "" {
		errs = append(errs, errors.New(
			"logs.source_record_id_attribute requires logs.source_record_namespace: "+
				"an ID without a trusted namespace is not a record identity"))
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
	return errs
}

func (cfg *Config) validateCredentials() []error {
	c := cfg.Credentials
	if c.File != "" && c.ImpersonateServiceAccount != "" {
		return []error{errors.New(
			"credentials.file and credentials.impersonate_service_account are mutually exclusive")}
	}
	return nil
}

func (cfg *Config) validateEndpoint() []error {
	var errs []error
	e := cfg.Endpoint

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

// isLoopback reports whether the endpoint addresses the local machine.
func isLoopback(endpoint string) bool {
	host := endpoint
	for _, prefix := range []string{"http://", "https://", "dns:///", "passthrough:///"} {
		host = trimPrefix(host, prefix)
	}
	if i := indexByte(host, '/'); i >= 0 {
		host = host[:i]
	}
	if i := lastIndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	switch host {
	case "localhost", "127.0.0.1", "::1", "[::1]":
		return true
	}
	return false
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
