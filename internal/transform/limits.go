// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

// Package transform maps pdata onto the relational table contract: structural
// limit enforcement (FR8), row construction and the log record fingerprint.
// docs/schema-rationale.md describes the contract and defines the FR labels used
// in code comments.
package transform // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/transform"

import (
	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// Limits are the FR8 structural bounds, enforced before serialization.
//
// These are about shape, not encoded size: a payload can be structurally
// pathological (a deeply self-nested attribute, a million events) while still
// serializing small enough to pass the FR7 byte checks. Encoded-size
// enforcement belongs to FR7 and runs after serialization.
type Limits struct {
	// MaxAttributeCount bounds attributes on any single bag.
	MaxAttributeCount int
	// MaxEventCount bounds events on one span.
	MaxEventCount int
	// MaxLinkCount bounds links on one span.
	MaxLinkCount int
	// MaxNestingDepth bounds AnyValue nesting. Depth 1 is a scalar.
	MaxNestingDepth int
	// MaxCollectionSize bounds elements in a single array or kvlist value.
	MaxCollectionSize int
	// MaxAttributeKeyBytes bounds one attribute or nested map key, measured in
	// UTF-8 bytes before encoding.
	MaxAttributeKeyBytes int
	// MaxValueBytes bounds one string or bytes value, measured before
	// encoding. When a PayloadOffloader is configured, this bound is enforced
	// after eligible large values have been offloaded.
	MaxValueBytes int
}

// DefaultLimits are deliberately generous: they exist to stop pathological
// payloads from becoming unqueryable rows or from consuming unbounded memory
// during serialization, not to shape ordinary telemetry.
func DefaultLimits() Limits {
	return Limits{
		MaxAttributeCount:    1024,
		MaxEventCount:        1024,
		MaxLinkCount:         1024,
		MaxNestingDepth:      16,
		MaxCollectionSize:    4096,
		MaxAttributeKeyBytes: 1024,
		MaxValueBytes:        256 * 1024,
	}
}

// Validate rejects a limits configuration that could never accept a record.
func (l Limits) Validate() error {
	fields := map[string]int{
		"max_attribute_count":     l.MaxAttributeCount,
		"max_event_count":         l.MaxEventCount,
		"max_link_count":          l.MaxLinkCount,
		"max_nesting_depth":       l.MaxNestingDepth,
		"max_collection_size":     l.MaxCollectionSize,
		"max_attribute_key_bytes": l.MaxAttributeKeyBytes,
		"max_value_bytes":         l.MaxValueBytes,
	}
	for name, v := range fields {
		if v <= 0 {
			return fmt.Errorf("limits.%s must be positive, got %d", name, v)
		}
	}
	return nil
}

// Reason identifies why a record was permanently rejected. These values are
// the bounded label set on the rejected_rows counter, so the set must stay
// small and must never include anything derived from telemetry content.
type Reason string

// The rejection reasons. Each value is one label on the rejected_rows counter.
const (
	ReasonAttributeCount   Reason = "attribute_count"
	ReasonAttributeKeySize Reason = "attribute_key_size"
	ReasonEventCount       Reason = "event_count"
	ReasonLinkCount        Reason = "link_count"
	ReasonNestingDepth     Reason = "nesting_depth"
	ReasonCollectionSize   Reason = "collection_size"
	ReasonValueSize        Reason = "value_size"
	ReasonSerialization    Reason = "serialization"
	ReasonRowBytes         Reason = "row_bytes"
	ReasonRowError         Reason = "row_error"

	// Contract violations. A record that cannot fill a NOT NULL column, or
	// whose value does not fit the column type, has no valid row at all.
	ReasonInvalidID         Reason = "invalid_id"
	ReasonMissingTimestamp  Reason = "missing_timestamp"
	ReasonTimestampOverflow Reason = "timestamp_overflow"
)

// Violation describes a structural limit breach or a contract violation.
//
// FieldPath is a sanitized location such as "attributes" or
// "events[3].attributes" — the shape of the path only. It never carries an
// attribute key or value, because FR13 prohibits telemetry content in logs and
// diagnostics flow into logs.
type Violation struct {
	Reason    Reason
	FieldPath string
	// Limit and Actual are counts or byte sizes, which are metadata rather
	// than content and are safe to log. Both are zero for a contract
	// violation, which breaches no configured limit.
	Limit  int
	Actual int
}

func (v Violation) Error() string {
	if v.Limit == 0 && v.Actual == 0 {
		return fmt.Sprintf("%s: %s", v.Reason, v.FieldPath)
	}
	return fmt.Sprintf("%s: %s exceeds limit (%d > %d)", v.Reason, v.FieldPath, v.Actual, v.Limit)
}

// checkMap validates one attribute bag, including string/bytes value sizes.
func (l Limits) checkMap(m pcommon.Map, path string) *Violation {
	return l.checkMapMode(m, path, true)
}

func (l Limits) checkMapMode(m pcommon.Map, path string, checkValueBytes bool) *Violation {
	if m.Len() > l.MaxAttributeCount {
		return &Violation{ReasonAttributeCount, path, l.MaxAttributeCount, m.Len()}
	}
	var found *Violation
	m.Range(func(k string, v pcommon.Value) bool {
		// The key is deliberately dropped from the path: it is telemetry
		// content. The bag name alone tells an operator where to look.
		if l.MaxAttributeKeyBytes > 0 && len(k) > l.MaxAttributeKeyBytes {
			found = &Violation{ReasonAttributeKeySize, path, l.MaxAttributeKeyBytes, len(k)}
			return false
		}
		if found = l.checkValueMode(v, path, 1, checkValueBytes); found != nil {
			return false
		}
		return true
	})
	return found
}

func (l Limits) checkValueMode(v pcommon.Value, path string, depth int, checkValueBytes bool) *Violation {
	if depth > l.MaxNestingDepth {
		return &Violation{ReasonNestingDepth, path, l.MaxNestingDepth, depth}
	}

	switch v.Type() {
	case pcommon.ValueTypeStr:
		if checkValueBytes {
			if n := len(v.Str()); n > l.MaxValueBytes {
				return &Violation{ReasonValueSize, path, l.MaxValueBytes, n}
			}
		}
	case pcommon.ValueTypeBytes:
		if checkValueBytes {
			if n := v.Bytes().Len(); n > l.MaxValueBytes {
				return &Violation{ReasonValueSize, path, l.MaxValueBytes, n}
			}
		}
	case pcommon.ValueTypeSlice:
		s := v.Slice()
		if s.Len() > l.MaxCollectionSize {
			return &Violation{ReasonCollectionSize, path, l.MaxCollectionSize, s.Len()}
		}
		for i := 0; i < s.Len(); i++ {
			if bad := l.checkValueMode(s.At(i), path, depth+1, checkValueBytes); bad != nil {
				return bad
			}
		}
	case pcommon.ValueTypeMap:
		inner := v.Map()
		if inner.Len() > l.MaxCollectionSize {
			return &Violation{ReasonCollectionSize, path, l.MaxCollectionSize, inner.Len()}
		}
		var found *Violation
		inner.Range(func(k string, iv pcommon.Value) bool {
			if l.MaxAttributeKeyBytes > 0 && len(k) > l.MaxAttributeKeyBytes {
				found = &Violation{ReasonAttributeKeySize, path, l.MaxAttributeKeyBytes, len(k)}
				return false
			}
			if found = l.checkValueMode(iv, path, depth+1, checkValueBytes); found != nil {
				return false
			}
			return true
		})
		if found != nil {
			return found
		}
	}
	return nil
}
