// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package googlehttp

import (
	"context"
	"crypto/tls"
	"testing"

	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
)

func TestNewClientDefaultEndpointOptions(t *testing.T) {
	cases := []struct {
		name       string
		opts       []option.ClientOption
		cert, mode string
		want       string
	}{
		{name: "default", opts: []option.ClientOption{legacyEndpointOption("https://example.test/custom/")}, want: "https://example.test/custom/"},
		{name: "template", opts: []option.ClientOption{internaloption.WithDefaultEndpointTemplate("https://storage.UNIVERSE_DOMAIN/custom/")}, want: "https://storage.googleapis.com/custom/"},
		{name: "default universe option", opts: []option.ClientOption{internaloption.WithDefaultEndpointTemplate("https://storage.UNIVERSE_DOMAIN/custom/"), internaloption.WithDefaultUniverseDomain("example.test")}, want: "https://storage.googleapis.com/custom/"},
		{name: "explicit universe", opts: []option.ClientOption{internaloption.WithDefaultEndpointTemplate("https://storage.UNIVERSE_DOMAIN/custom/"), internaloption.WithDefaultUniverseDomain("default.test"), option.WithUniverseDomain("example.test")}, want: "https://storage.example.test/custom/"},
		{name: "host override", opts: []option.ClientOption{legacyEndpointOption("https://example.test/custom/"), option.WithEndpoint("localhost:1234")}, want: "https://localhost:1234/custom/"},
		{name: "full override", opts: []option.ClientOption{legacyEndpointOption("https://example.test/custom/"), option.WithEndpoint("https://override.test/path/")}, want: "https://override.test/path/"},
		{name: "forced mtls", mode: "always", opts: []option.ClientOption{legacyEndpointOption("https://example.test/custom/"), internaloption.WithDefaultMTLSEndpoint("https://mtls.example.test/custom/")}, want: "https://mtls.example.test/custom/"},
		{name: "certificate mtls", cert: "true", opts: []option.ClientOption{legacyEndpointOption("https://example.test/custom/"), internaloption.WithDefaultMTLSEndpoint("https://mtls.example.test/custom/"), option.WithClientCertSource(func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return &tls.Certificate{}, nil })}, want: "https://mtls.example.test/custom/"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cert, mode := tc.cert, tc.mode
			if cert == "" {
				cert = "false"
			}
			if mode == "" {
				mode = "auto"
			}
			t.Setenv("GOOGLE_API_USE_CLIENT_CERTIFICATE", cert)
			t.Setenv("GOOGLE_API_USE_MTLS_ENDPOINT", mode)
			t.Setenv("GOOGLE_CLOUD_UNIVERSE_DOMAIN", "")
			opts := append([]option.ClientOption{option.WithoutAuthentication()}, tc.opts...)
			client, endpoint, cleanup, err := NewClient(context.Background(), nil, opts...)
			if err != nil {
				t.Fatal(err)
			}
			defer cleanup()
			if client == nil || endpoint != tc.want {
				t.Fatalf("endpoint=%q, want %q (client=%v)", endpoint, tc.want, client)
			}
		})
	}
}

// legacyEndpointOption covers compatibility with older generated clients.
//
// Deprecated: New clients should use WithDefaultEndpointTemplate.
func legacyEndpointOption(endpoint string) option.ClientOption {
	return internaloption.WithDefaultEndpoint(endpoint)
}
