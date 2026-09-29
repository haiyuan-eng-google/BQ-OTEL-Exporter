// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigquery

import (
	"testing"

	"go.uber.org/goleak"
)

// Result owners, stream constructors and managedwriter connections must all
// end with the writer that started them.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// opencensus starts its stats worker at package init and keeps it for
		// the life of the process.
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		// managedwriter cancels a connection's previous context after a fixed
		// 20 s grace period, including on the connection's first open. The
		// goroutine ends on its own.
		goleak.IgnoreAnyFunction("cloud.google.com/go/bigquery/storage/managedwriter.(*connection).getStream.func1"),
	)
}
