// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"testing"

	"go.uber.org/goleak"
)

// The schema package is pure and synchronous; no test may leave a goroutine
// behind.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
