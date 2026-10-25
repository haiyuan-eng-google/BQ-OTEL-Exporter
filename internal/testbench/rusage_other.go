// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

//go:build !linux && !darwin

package testbench

func readProcessCPUAndRSS() (cpuSeconds float64, peakRSSMB float64) {
	return 0, 0
}
