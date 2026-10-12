// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestProbe(t *testing.T) {
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(healthy.Close)

	if err := probe(context.Background(), healthy.URL, time.Second); err != nil {
		t.Fatalf("probe(healthy) = %v, want nil", err)
	}

	unhealthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(unhealthy.Close)

	if err := probe(context.Background(), unhealthy.URL, time.Second); err == nil {
		t.Fatal("probe(unhealthy) = nil, want non-nil error")
	}
}
