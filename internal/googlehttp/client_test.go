// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package googlehttp

import (
	"context"
	"net/http"
	"testing"

	"google.golang.org/api/option"
)

func TestHasAuthOption(t *testing.T) {
	cases := []struct {
		name     string
		opts     []option.ClientOption
		expected bool
	}{
		{
			name:     "empty options",
			opts:     nil,
			expected: false,
		},
		{
			name:     "non-auth option endpoint",
			opts:     []option.ClientOption{option.WithEndpoint("https://example.com")},
			expected: false,
		},
		{
			name:     "without authentication",
			opts:     []option.ClientOption{option.WithoutAuthentication()},
			expected: true,
		},
		{
			name:     "with api key",
			opts:     []option.ClientOption{option.WithAPIKey("fake-key")},
			expected: true,
		},
		{
			name: "mixed options containing auth",
			opts: []option.ClientOption{
				option.WithEndpoint("https://example.com"),
				option.WithoutAuthentication(),
			},
			expected: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := hasAuthOption(tc.opts)
			if got != tc.expected {
				t.Fatalf("hasAuthOption(%v) = %v; want %v", tc.opts, got, tc.expected)
			}
		})
	}
}

func TestNewClientWithoutAuthentication(t *testing.T) {
	ctx := context.Background()
	client, endpoint, cleanup, err := NewClient(ctx, nil, option.WithoutAuthentication(), option.WithEndpoint("https://bigquery.googleapis.com/"))
	if err != nil {
		t.Fatalf("NewClient with WithoutAuthentication failed: %v", err)
	}
	defer cleanup()

	if client == nil {
		t.Fatal("expected non-nil client")
	}
	if endpoint == "" {
		t.Fatal("expected non-empty endpoint")
	}
}

func TestNewClientWithHTTPClient(t *testing.T) {
	ctx := context.Background()
	customClient := &http.Client{}
	client, _, cleanup, err := NewClient(ctx, nil, option.WithHTTPClient(customClient))
	if err != nil {
		t.Fatalf("NewClient with WithHTTPClient failed: %v", err)
	}
	defer cleanup()

	if client != customClient {
		t.Fatalf("expected client to match customClient; got %v, want %v", client, customClient)
	}
}
