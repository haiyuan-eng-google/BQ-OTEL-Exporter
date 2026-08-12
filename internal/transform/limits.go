// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

// Package transform maps pdata onto the §7 relational contract: structural
// limit enforcement (FR8), row construction (§7.1, §7.2) and the log record
// fingerprint (§7.4).
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
	// MaxValueBytes bounds one string or bytes value, measured before
	// encoding.
	MaxValueBytes int
}

// DefaultLimits are deliberately generous: they exist to stop pathological
// payloads from becoming unqueryable rows or from consuming unbounded memory
// during serialization, not to shape ordinary telemetry.
func DefaultLimits() Limits {
	return Limits{
		MaxAttributeCount: 1024,
		MaxEventCount:     1024,
		MaxLinkCount:      1024,
		MaxNestingDepth:   16,
		MaxCollectionSize: 4096,
		MaxValueBytes:     256 * 1024,
	}
}

// Validate rejects a limits configuration that could never accept a record.
func (l Limits) Validate() error {
	fields := map[string]int{
		"max_attribute_count": l.MaxAttributeCount,
		"max_event_count":     l.MaxEventCount,
		"max_link_count":      l.MaxLinkCount,
		"max_nesting_depth":   l.MaxNestingDepth,
		"max_collection_size": l.MaxCollectionSize,
		"max_value_bytes":     l.MaxValueBytes,
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

const (
	ReasonAttributeCount Reason = "attribute_count"
	ReasonEventCount     Reason = "event_count"
	ReasonLinkCount      Reason = "link_count"
	ReasonNestingDepth   Reason = "nesting_depth"
	ReasonCollectionSize Reason = "collection_size"
	ReasonValueSize      Reason = "value_size"
	ReasonSerialization  Reason = "serialization"
	ReasonRowBytes       Reason = "row_bytes"
	ReasonRowError       Reason = "row_error"
)

// Violation describes a structural limit breach.
//
// FieldPath is a sanitized location such as "span_attributes" or
// "events[3].attributes" — the shape of the path only. It never carries an
// attribute key or value, because FR13 prohibits telemetry content in logs and
// diagnostics flow into logs.
type Violation struct {
	Reason    Reason
	FieldPath string
	// Limit and Actual are counts or byte sizes, which are metadata rather
	// than content and are safe to log.
	Limit  int
	Actual int
}

func (v Violation) Error() string {
	return fmt.Sprintf("%s: %s exceeds limit (%d > %d)", v.Reason, v.FieldPath, v.Actual, v.Limit)
}

// checkMap validates one attribute bag.
func (l Limits) checkMap(m pcommon.Map, path string) *Violation {
	if m.Len() > l.MaxAttributeCount {
		return &Violation{ReasonAttributeCount, path, l.MaxAttributeCount, m.Len()}
	}
	var found *Violation
	m.Range(func(_ string, v pcommon.Value) bool {
		// The key is deliberately dropped from the path: it is telemetry
		// content. The bag name alone tells an operator where to look.
		if found = l.checkValue(v, path, 1); found != nil {
			return false
		}
		return true
	})
	return found
}

// checkValue walks an AnyValue, enforcing depth, collection size and value
// size as it goes.
func (l Limits) checkValue(v pcommon.Value, path string, depth int) *Violation {
	if depth > l.MaxNestingDepth {
		return &Violation{ReasonNestingDepth, path, l.MaxNestingDepth, depth}
	}

	switch v.Type() {
	case pcommon.ValueTypeStr:
		if n := len(v.Str()); n > l.MaxValueBytes {
			return &Violation{ReasonValueSize, path, l.MaxValueBytes, n}
		}
	case pcommon.ValueTypeBytes:
		if n := v.Bytes().Len(); n > l.MaxValueBytes {
			return &Violation{ReasonValueSize, path, l.MaxValueBytes, n}
		}
	case pcommon.ValueTypeSlice:
		s := v.Slice()
		if s.Len() > l.MaxCollectionSize {
			return &Violation{ReasonCollectionSize, path, l.MaxCollectionSize, s.Len()}
		}
		for i := 0; i < s.Len(); i++ {
			if bad := l.checkValue(s.At(i), path, depth+1); bad != nil {
				return bad
			}
		}
	case pcommon.ValueTypeMap:
		inner := v.Map()
		if inner.Len() > l.MaxCollectionSize {
			return &Violation{ReasonCollectionSize, path, l.MaxCollectionSize, inner.Len()}
		}
		var found *Violation
		inner.Range(func(_ string, iv pcommon.Value) bool {
			if found = l.checkValue(iv, path, depth+1); found != nil {
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
