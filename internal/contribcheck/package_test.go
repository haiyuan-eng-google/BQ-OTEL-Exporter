// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package contribcheck

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// The example configuration test links this repository's exporter,
		// whose BigQuery client depends on opencensus. opencensus starts its
		// stats worker at package init and keeps it for the life of the
		// process.
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	)
}
