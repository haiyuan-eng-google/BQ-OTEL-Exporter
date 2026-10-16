// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigquery

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
)

func FuzzSplitRequests(f *testing.F) {
	f.Add(10, 64, 256)
	f.Add(1, 512, 128)
	f.Add(0, 0, 1024)
	f.Add(25, 100, 100)

	f.Fuzz(func(t *testing.T, numRows, rowSize, maxBytes int) {
		if numRows < 0 || numRows > 200 {
			return
		}
		if rowSize < 0 || rowSize > 4096 {
			return
		}
		if maxBytes <= 0 || maxBytes > 65536 {
			return
		}

		rows := make([][]byte, numRows)
		for i := 0; i < numRows; i++ {
			b := bytes.Repeat([]byte{byte(i + 1)}, rowSize)
			rows[i] = b
		}

		chunks := SplitRequests(rows, maxBytes)

		// Conservation property: flattening chunks yields the exact input slice in order.
		var flattened [][]byte
		for _, ch := range chunks {
			if len(ch) == 0 {
				t.Fatalf("SplitRequests produced an empty chunk")
			}
			var chunkBytes int
			for _, r := range ch {
				chunkBytes += len(r)
			}
			if len(ch) > 1 && chunkBytes > maxBytes {
				t.Fatalf("multi-row chunk size %d exceeded maxBytes %d", chunkBytes, maxBytes)
			}
			flattened = append(flattened, ch...)
		}

		if len(flattened) != len(rows) {
			t.Fatalf("row count mismatch after SplitRequests: got %d, want %d", len(flattened), len(rows))
		}
		for i := range rows {
			if !bytes.Equal(flattened[i], rows[i]) {
				t.Fatalf("row %d content mismatch after SplitRequests", i)
			}
		}
	})
}

func BenchmarkSplitRequests(b *testing.B) {
	rows := make([][]byte, 100)
	for i := range rows {
		rows[i] = bytes.Repeat([]byte("x"), 32*1024) // 32 KB per row (~3.2 MB total)
	}
	const maxBytes = 900 * 1024 // 900 KB per chunk

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chunks := SplitRequests(rows, maxBytes)
		if len(chunks) == 0 {
			b.Fatal("expected non-empty chunks")
		}
	}
}

func BenchmarkAppendSplitWithBackpressure(b *testing.B) {
	rows := make([][]byte, 100)
	for i := range rows {
		rows[i] = bytes.Repeat([]byte("x"), 32*1024)
	}
	ctrl := NewBackpressureController(BackpressureOptions{
		MinRequestBytes:     256 * 1024,
		MaxRequestBytes:     900 * 1024,
		MinInflightRequests: 1,
		MaxInflightRequests: 8,
	})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chunks := SplitRequests(rows, ctrl.EffectiveRequestBytes())
		if len(chunks) == 0 {
			b.Fatal("expected non-empty chunks")
		}
	}
}

var closedBenchReady = func() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

type benchAppendResult struct{}

func (benchAppendResult) Ready() <-chan struct{} { return closedBenchReady }

func (benchAppendResult) GetResult(context.Context) (int64, error) { return 0, nil }

func (benchAppendResult) FullResponse(context.Context) (*storagepb.AppendRowsResponse, error) {
	return nil, nil
}

type benchStream struct{}

func (benchStream) AppendRows(context.Context, [][]byte, ...managedwriter.AppendOption) (appendResult, error) {
	return benchAppendResult{}, nil
}

func (benchStream) Close() error { return nil }

func BenchmarkWriterPool(b *testing.B) {
	rows := make([][]byte, 16)
	for i := range rows {
		rows[i] = bytes.Repeat([]byte("x"), 512)
	}
	for _, poolSize := range []int{1, 2, 4} {
		b.Run(fmt.Sprintf("pool=%d", poolSize), func(b *testing.B) {
			w := &Writer{
				opts: WriterOptions{
					Project:             "p",
					Dataset:             "d",
					Table:               "t",
					MaxRequestBytes:     2048,
					MaxInflightRequests: 8,
					MaxInflightBytes:    64 * 1024 * 1024,
					StreamPoolSize:      poolSize,
				},
				lifetimeCtx: context.Background(),
				factory: func(context.Context, ...managedwriter.WriterOption) (stream, error) {
					return benchStream{}, nil
				},
			}
			b.Cleanup(func() { _ = w.Close() })
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					out := w.Append(context.Background(), rows)
					if out.Err != nil || out.AcknowledgedRows != len(rows) {
						b.Fatalf("Append outcome = %+v", out)
					}
				}
			})
		})
	}
}
