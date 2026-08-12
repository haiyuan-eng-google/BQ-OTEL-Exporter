// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

// Package protoenc serializes rows into the protobuf wire format the Storage
// Write API expects.
//
// The descriptor is derived from the BigQuery table schema rather than
// generated ahead of time, so the exporter writes against the shape the table
// actually has. That is also what makes schema-mismatch detection possible at
// startup instead of at the first append.
package protoenc // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/protoenc"

import (
	"fmt"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/transform"
)

// Encoder serializes rows against one table schema.
type Encoder struct {
	md         protoreflect.MessageDescriptor
	normalized *descriptorpb.DescriptorProto
}

// New builds an encoder for the given table schema.
func New(ts *storagepb.TableSchema) (*Encoder, error) {
	md, err := adapt.StorageSchemaToProto2Descriptor(ts, "root")
	if err != nil {
		return nil, fmt.Errorf("deriving proto descriptor from table schema: %w", err)
	}
	msgDesc, ok := md.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("derived descriptor is %T, not a message descriptor", md)
	}

	// The stream needs a self-contained descriptor: nested types must be
	// inlined rather than referenced, since the service has no access to our
	// file registry.
	normalized, err := adapt.NormalizeDescriptor(msgDesc)
	if err != nil {
		return nil, fmt.Errorf("normalizing proto descriptor: %w", err)
	}

	return &Encoder{md: msgDesc, normalized: normalized}, nil
}

// Descriptor returns the normalized descriptor to hand to the write stream.
func (e *Encoder) Descriptor() *descriptorpb.DescriptorProto {
	return e.normalized
}

// Encode serializes one row.
//
// A row that cannot be encoded is a permanent, row-scoped failure: retrying it
// would produce the same result. The caller counts and drops it rather than
// failing the whole batch.
func (e *Encoder) Encode(row transform.Row) ([]byte, error) {
	msg := dynamicpb.NewMessage(e.md)
	if err := setFields(msg, e.md, row); err != nil {
		return nil, err
	}
	b, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshaling row: %w", err)
	}
	return b, nil
}

func setFields(msg *dynamicpb.Message, md protoreflect.MessageDescriptor, row transform.Row) error {
	fields := md.Fields()
	for name, val := range row {
		fd := fields.ByName(protoreflect.Name(name))
		if fd == nil {
			// A column the table does not have. Silently dropping it would
			// hide a schema drift bug, so surface it.
			return fmt.Errorf("column %q is not present in the destination table schema", name)
		}
		if fd.IsList() {
			nested, ok := val.([]transform.Row)
			if !ok {
				return fmt.Errorf("column %q: expected a list of rows, got %T", name, val)
			}
			list := msg.Mutable(fd).List()
			for i, item := range nested {
				elem := dynamicpb.NewMessage(fd.Message())
				if err := setFields(elem, fd.Message(), item); err != nil {
					return fmt.Errorf("column %q[%d]: %w", name, i, err)
				}
				list.Append(protoreflect.ValueOfMessage(elem))
			}
			continue
		}
		pv, err := scalar(fd, val)
		if err != nil {
			return fmt.Errorf("column %q: %w", name, err)
		}
		msg.Set(fd, pv)
	}
	return nil
}

// scalar converts a Go value to the protobuf kind the descriptor declares.
//
// The conversion is driven by the descriptor rather than by the Go type,
// because the descriptor is derived from the real table: if BigQuery says a
// column is INT64, an int64 must go on the wire regardless of how the row was
// built.
func scalar(fd protoreflect.FieldDescriptor, val any) (protoreflect.Value, error) {
	switch fd.Kind() {
	case protoreflect.StringKind:
		s, ok := val.(string)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected string, got %T", val)
		}
		return protoreflect.ValueOfString(s), nil

	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		switch v := val.(type) {
		case int64:
			return protoreflect.ValueOfInt64(v), nil
		case int:
			return protoreflect.ValueOfInt64(int64(v)), nil
		default:
			return protoreflect.Value{}, fmt.Errorf("expected int64, got %T", val)
		}

	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		switch v := val.(type) {
		case int32:
			return protoreflect.ValueOfInt32(v), nil
		case int64:
			return protoreflect.ValueOfInt32(int32(v)), nil
		case int:
			return protoreflect.ValueOfInt32(int32(v)), nil
		default:
			return protoreflect.Value{}, fmt.Errorf("expected int32, got %T", val)
		}

	case protoreflect.DoubleKind:
		f, ok := val.(float64)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected float64, got %T", val)
		}
		return protoreflect.ValueOfFloat64(f), nil

	case protoreflect.BoolKind:
		b, ok := val.(bool)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected bool, got %T", val)
		}
		return protoreflect.ValueOfBool(b), nil

	case protoreflect.BytesKind:
		b, ok := val.([]byte)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected []byte, got %T", val)
		}
		return protoreflect.ValueOfBytes(b), nil

	default:
		return protoreflect.Value{}, fmt.Errorf("unsupported field kind %s", fd.Kind())
	}
}

// SchemaMismatch reports columns the exporter writes that the destination
// table does not have.
//
// Extra columns in the table are fine: they are additive evolution, and a
// writer pinned to an older contract simply leaves them NULL. Missing columns
// are not, because rows would be written against a shape the table cannot
// accept.
func SchemaMismatch(want, have *storagepb.TableSchema) []string {
	present := make(map[string]bool, len(have.GetFields()))
	for _, f := range have.GetFields() {
		present[f.GetName()] = true
	}
	var missing []string
	for _, f := range want.GetFields() {
		if !present[f.GetName()] {
			missing = append(missing, f.GetName())
		}
	}
	return missing
}

// ProtoDescriptorFor is a convenience for callers that only need the
// descriptor, e.g. to configure a stream.
func ProtoDescriptorFor(ts *storagepb.TableSchema) (*descriptorpb.DescriptorProto, error) {
	e, err := New(ts)
	if err != nil {
		return nil, err
	}
	return e.Descriptor(), nil
}

var _ = protodesc.ToFileDescriptorProto
