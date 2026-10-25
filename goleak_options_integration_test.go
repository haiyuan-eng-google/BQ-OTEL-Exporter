// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package bigqueryexporter

import "go.uber.org/goleak"

func integrationGoleakOptions() []goleak.Option {
	return []goleak.Option{
		// With mTLS or context-aware access configured, the enterprise
		// certificate proxy client reads from its helper process for as long
		// as the process lives. Integration runs keep the ambient
		// environment because they talk to real Google APIs.
		goleak.IgnoreAnyFunction("net/rpc.(*Client).input"),
		// managedwriter cancels a connection's previous context after a fixed
		// 20 s grace period, including on its first open. This timer goroutine
		// exits on its own and holds no HTTP connection pool.
		goleak.IgnoreAnyFunction("cloud.google.com/go/bigquery/storage/managedwriter.(*connection).getStream.func1"),
	}
}
