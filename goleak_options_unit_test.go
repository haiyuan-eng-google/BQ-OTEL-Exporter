// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

//go:build !integration

package bigqueryexporter

import (
	"os"

	"go.uber.org/goleak"
)

// init makes the unit-test binary hermetic with respect to ambient Google
// auth configuration.
//
// Several tests start the exporter against a no-auth loopback endpoint, which
// builds a real managedwriter client. When the machine running the tests
// exports mTLS or context-aware access settings, that client starts an
// enterprise-certificate-proxy helper whose RPC reader goroutine outlives the
// test and fails the goroutine-leak check. Unit tests never talk to Google, so
// the settings are cleared for the whole test binary.
//
// This runs from init rather than from TestMain because TestMain lives in the
// generated generated_package_test.go. Integration builds do not do this; see
// goleak_options_integration_test.go.
func init() {
	if err := os.Setenv("GOOGLE_API_USE_CLIENT_CERTIFICATE", "false"); err != nil {
		panic(err)
	}
	for _, key := range []string{
		"GOOGLE_API_CERTIFICATE_CONFIG",
		"GOOGLE_APPLICATION_CREDENTIALS",
	} {
		if err := os.Unsetenv(key); err != nil {
			panic(err)
		}
	}
}

func integrationGoleakOptions() []goleak.Option {
	return []goleak.Option{
		// managedwriter cancels a connection's previous context after a fixed
		// 20 s grace period, including on the connection's first open. The
		// goroutine ends on its own.
		goleak.IgnoreAnyFunction("cloud.google.com/go/bigquery/storage/managedwriter.(*connection).getStream.func1"),
	}
}
