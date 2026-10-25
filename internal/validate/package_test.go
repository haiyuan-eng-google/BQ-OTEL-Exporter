// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"os"
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	// The Storage Write API probe builds a real managedwriter client. When
	// the machine running the tests exports mTLS or context-aware access
	// settings, that client starts a certificate helper whose reader
	// goroutine outlives the test, so the tests clear those settings. They
	// never talk to Google.
	_ = os.Setenv("GOOGLE_API_USE_CLIENT_CERTIFICATE", "false")
	_ = os.Unsetenv("GOOGLE_API_CERTIFICATE_CONFIG")
	_ = os.Unsetenv("GOOGLE_APPLICATION_CREDENTIALS")
	goleak.VerifyTestMain(m,
		// opencensus starts its stats worker at package init and keeps it
		// for the life of the process.
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	)
}
