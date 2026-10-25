// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	if err := os.Setenv("GOOGLE_API_USE_CLIENT_CERTIFICATE", "false"); err != nil {
		panic(fmt.Sprintf("setting GOOGLE_API_USE_CLIENT_CERTIFICATE: %v", err))
	}
	for _, k := range []string{"GOOGLE_API_CERTIFICATE_CONFIG", "GOOGLE_APPLICATION_CREDENTIALS"} {
		if err := os.Unsetenv(k); err != nil {
			panic(fmt.Sprintf("unsetting %s: %v", k, err))
		}
	}
	goleak.VerifyTestMain(
		m,
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		goleak.IgnoreAnyFunction("cloud.google.com/go/bigquery/storage/managedwriter.(*connection).getStream.func1"),
	)
}
