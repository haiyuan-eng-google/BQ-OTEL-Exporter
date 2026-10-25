// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package testbench

import "syscall"

func readProcessCPUAndRSS() (cpuSeconds float64, peakRSSMB float64) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, 0
	}
	userSec := float64(ru.Utime.Sec) + float64(ru.Utime.Usec)/1e6
	sysSec := float64(ru.Stime.Sec) + float64(ru.Stime.Usec)/1e6
	// On Linux, Maxrss is reported in kilobytes (KiB).
	rssMB := float64(ru.Maxrss) / 1024.0
	return userSec + sysSec, rssMB
}
