// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

//go:build !integration

package bigqueryexporter

import "go.uber.org/goleak"

func integrationGoleakOptions() []goleak.Option { return nil }
