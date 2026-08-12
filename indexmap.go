// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter"

import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/transform"

// rowPositionToRecordIndex maps a position in the produced row slice back to
// the position of the record that produced it in the original batch.
//
// The two drift apart as soon as anything is rejected: rows are the survivors,
// records are everything the pipeline handed us. Rebuilding a retry payload
// needs record positions, so the mapping has to be explicit rather than
// assumed.
func rowPositionToRecordIndex(rowCount int, rejects []transform.Rejection) []int {
	rejected := make(map[int]bool, len(rejects))
	for _, r := range rejects {
		rejected[r.Index] = true
	}

	out := make([]int, 0, rowCount)
	for recordIdx := 0; len(out) < rowCount; recordIdx++ {
		if rejected[recordIdx] {
			continue
		}
		out = append(out, recordIdx)
	}
	return out
}
