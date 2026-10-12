// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigquery

import (
	"bytes"
	"testing"
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
