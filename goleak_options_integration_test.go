// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package bigqueryexporter

import "go.uber.org/goleak"

func integrationGoleakOptions() []goleak.Option {
	return []goleak.Option{
		// Google auth keeps token-fetch HTTP/2 connections pooled beyond the
		// client call that requested the token.
		goleak.IgnoreAnyFunction("net/http.(*http2ClientConn).readLoop"),
		goleak.IgnoreAnyFunction("golang.org/x/net/http2.(*ClientConn).readLoop"),
		// managedwriter intentionally delays cancellation of a replaced
		// connection for a bounded grace period.
		goleak.IgnoreAnyFunction("cloud.google.com/go/bigquery/storage/managedwriter.(*connection).getStream.func1"),
	}
}
