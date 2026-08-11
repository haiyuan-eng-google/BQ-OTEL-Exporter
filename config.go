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
)

// apiRequestLimitBytes is the hard per-AppendRows request limit imposed by the
// BigQuery Storage Write API. It is not configurable; MaxRequestBytes is our
// own headroom threshold beneath it (FR7).
const apiRequestLimitBytes = 10 * 1000 * 1000

var (
	// Dataset and table IDs accept letters, numbers and underscores.
	idPattern = regexp.MustCompile(`^[A-Za-z0-9_]+$`)
	// Project IDs are lowercase letters, digits and hyphens.
	projectPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{4,28}[a-z0-9]$`)
)

// Config defines configuration for the BigQuery exporter.
type Config struct {
	TimeoutSettings exporterhelper.TimeoutConfig                             `mapstructure:",squash"`
	QueueSettings   configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`
	BackOffConfig   configretry.BackOffConfig                                `mapstructure:"retry_on_failure"`

	// Project is the Google Cloud project owning the destination dataset.
	Project string `mapstructure:"project"`
	// Dataset is the destination BigQuery dataset ID.
	Dataset string `mapstructure:"dataset"`
	// SpansTable is the destination table for OTLP spans.
	SpansTable string `mapstructure:"spans_table"`
	// LogsTable is the destination table for OTLP log records.
	LogsTable string `mapstructure:"logs_table"`

	// AutoCreate governs dataset and table creation. Creation is a separate
	// control-plane concern; leaving it off keeps the exporter on a write-only
	// IAM path (NFR4).
	AutoCreate AutoCreateConfig `mapstructure:"auto_create"`

	// Write holds Storage Write API tuning.
	Write WriteConfig `mapstructure:"write"`

	// PromoteAttributes lifts named attributes out of the attribute bag into
	// their own nullable top-level columns. Promoted columns are derived
	// projections: they are excluded from the canonical fingerprint input, so a
	// fleet running heterogeneous promotion configs still produces identical
	// record fingerprints for the same logical record.
	PromoteAttributes []PromotedAttribute `mapstructure:"promote_attributes"`
}

// AutoCreateConfig controls creation of the destination dataset and tables.
type AutoCreateConfig struct {
	Dataset bool `mapstructure:"dataset"`
	Tables  bool `mapstructure:"tables"`
	// Location is the dataset location used when creating a dataset.
	Location string `mapstructure:"location"`
}

// WriteConfig holds Storage Write API tuning knobs.
type WriteConfig struct {
	// MaxRequestBytes is our own request-size headroom threshold, enforced
	// after protobuf serialization and before AppendRows. It is deliberately
	// distinct from, and below, the API's own per-request limit.
	MaxRequestBytes int `mapstructure:"max_request_bytes"`
	// MaxRowBytes rejects individual rows above this serialized size.
	MaxRowBytes int `mapstructure:"max_row_bytes"`
	// MaxInflightRequests bounds asynchronous in-flight appends.
	MaxInflightRequests int `mapstructure:"max_inflight_requests"`
	// MaxInflightBytes bounds the memory held by in-flight appends.
	MaxInflightBytes int `mapstructure:"max_inflight_bytes"`
}

// PromotedAttribute names an attribute to lift into a top-level column.
type PromotedAttribute struct {
	// Attribute is the OTLP attribute key, for example "gen_ai.system".
	Attribute string `mapstructure:"attribute"`
	// Column is the destination column name. Defaults to a sanitized
	// Attribute when empty.
	Column string `mapstructure:"column"`
	// Type is the BigQuery column type: STRING, INT64, FLOAT64 or BOOL.
	Type string `mapstructure:"type"`
}

var validPromotedTypes = map[string]bool{
	"STRING": true, "INT64": true, "FLOAT64": true, "BOOL": true,
}

// Validate checks the configuration and fails fast on startup.
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

	for name, table := range map[string]string{"spans_table": cfg.SpansTable, "logs_table": cfg.LogsTable} {
		if table == "" {
			errs = append(errs, fmt.Errorf("%s must not be empty", name))
		} else if !idPattern.MatchString(table) {
			errs = append(errs, fmt.Errorf("%s %q must contain only letters, numbers and underscores", name, table))
		}
	}

	if cfg.Write.MaxRequestBytes <= 0 {
		errs = append(errs, errors.New("write.max_request_bytes must be positive"))
	} else if cfg.Write.MaxRequestBytes >= apiRequestLimitBytes {
		errs = append(errs, fmt.Errorf(
			"write.max_request_bytes (%d) must stay below the Storage Write API per-request limit of %d bytes",
			cfg.Write.MaxRequestBytes, apiRequestLimitBytes))
	}

	if cfg.Write.MaxRowBytes <= 0 {
		errs = append(errs, errors.New("write.max_row_bytes must be positive"))
	} else if cfg.Write.MaxRowBytes > cfg.Write.MaxRequestBytes {
		errs = append(errs, fmt.Errorf(
			"write.max_row_bytes (%d) must not exceed write.max_request_bytes (%d), otherwise a legal row can never be sent",
			cfg.Write.MaxRowBytes, cfg.Write.MaxRequestBytes))
	}

	if cfg.Write.MaxInflightRequests <= 0 {
		errs = append(errs, errors.New("write.max_inflight_requests must be positive"))
	}
	if cfg.Write.MaxInflightBytes <= 0 {
		errs = append(errs, errors.New("write.max_inflight_bytes must be positive"))
	}

	if cfg.AutoCreate.Dataset && cfg.AutoCreate.Location == "" {
		errs = append(errs, errors.New("auto_create.location is required when auto_create.dataset is enabled"))
	}

	seen := make(map[string]struct{}, len(cfg.PromoteAttributes))
	for i, pa := range cfg.PromoteAttributes {
		if pa.Attribute == "" {
			errs = append(errs, fmt.Errorf("promote_attributes[%d]: attribute must not be empty", i))
			continue
		}
		column := pa.Column
		if column == "" {
			column = sanitizeColumn(pa.Attribute)
		}
		if !idPattern.MatchString(column) {
			errs = append(errs, fmt.Errorf("promote_attributes[%d]: column %q is not a valid BigQuery column name", i, column))
		}
		if _, dup := seen[column]; dup {
			errs = append(errs, fmt.Errorf("promote_attributes[%d]: duplicate destination column %q", i, column))
		}
		seen[column] = struct{}{}
		if !validPromotedTypes[pa.Type] {
			errs = append(errs, fmt.Errorf("promote_attributes[%d]: type %q must be one of STRING, INT64, FLOAT64, BOOL", i, pa.Type))
		}
	}

	return errors.Join(errs...)
}

// sanitizeColumn maps an attribute key onto a legal BigQuery column name.
func sanitizeColumn(attr string) string {
	out := make([]rune, 0, len(attr))
	for _, r := range attr {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			out = append(out, r)
		default:
			out = append(out, '_')
		}
	}
	return string(out)
}
