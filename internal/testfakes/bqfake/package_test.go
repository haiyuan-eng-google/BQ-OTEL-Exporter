// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bqfake

import (
	"os"
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	_ = os.Setenv("GOOGLE_API_USE_CLIENT_CERTIFICATE", "false")
	_ = os.Unsetenv("GOOGLE_API_CERTIFICATE_CONFIG")
	goleak.VerifyTestMain(
		m,
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		goleak.IgnoreAnyFunction("cloud.google.com/go/bigquery/storage/managedwriter.(*connection).getStream.func1"),
	)
}
