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
		goleak.IgnoreAnyFunction("net/http/internal/http2.(*ClientConn).readLoop"),
		goleak.IgnoreAnyFunction("net/http/internal/http2.(*clientConnReadLoop).run"),
		goleak.IgnoreAnyFunction("golang.org/x/net/http2.(*ClientConn).readLoop"),
		// Google API and oauth2 HTTP/1.1 keepalive connection pools outlive
		// individual client calls.
		goleak.IgnoreAnyFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreAnyFunction("net/http.(*persistConn).writeLoop"),
		// With mTLS or context-aware access configured, the enterprise
		// certificate proxy client reads from its helper process for as long
		// as the process lives. Integration runs keep the ambient
		// environment because they talk to real Google APIs.
		goleak.IgnoreAnyFunction("net/rpc.(*Client).input"),
		// managedwriter intentionally delays cancellation of a replaced
		// connection for a bounded grace period.
		goleak.IgnoreAnyFunction("cloud.google.com/go/bigquery/storage/managedwriter.(*connection).getStream.func1"),
	}
}
