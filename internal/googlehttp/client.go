// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

// Package googlehttp retains the connection pools underneath API and auth
// middleware, which does not forward CloseIdleConnections.
package googlehttp

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"reflect"
	"sync"
	"time"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/credentials"
	"cloud.google.com/go/auth/httptransport"
	"google.golang.org/api/option"
	htransport "google.golang.org/api/transport/http"
)

func newBase() (*http.Transport, error) {
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, errors.New("default HTTP transport is not configurable")
	}
	return base.Clone(), nil
}

// Credentials creates credentials whose token requests use an owned pool.
// The returned cleanup must run after all clients using the credentials close.
func Credentials(detect credentials.DetectOptions) (*auth.Credentials, func(), error) {
	base, err := newBase()
	if err != nil {
		return nil, nil, err
	}
	// Let the auth library resolve ambient certificate configuration, then
	// retain the pool ourselves rather than letting credential detection
	// allocate an inaccessible token transport.
	opts := &httptransport.Options{
		DisableAuthentication: true,
		BaseRoundTripper:      base,
		UniverseDomain:        detect.UniverseDomain,
	}
	if _, err := httptransport.NewClient(opts); err != nil {
		base.CloseIdleConnections()
		return nil, nil, err
	}
	if opts.ClientCertProvider != nil {
		if base.TLSClientConfig == nil {
			base.TLSClientConfig = &tls.Config{}
		}
		base.TLSClientConfig.GetClientCertificate = opts.ClientCertProvider
		detect.TokenURL = credentials.GoogleMTLSTokenURL
	}
	detect.Client = &http.Client{Transport: base, Timeout: 30 * time.Second}
	// Keep refreshes on the RPC context so shutdown can cancel all token
	// requests before closing the pool, including refreshes of stale tokens.
	detect.DisableAsyncRefresh = true
	// Resolve ADC when the API constructor first asks for a credential
	// property. Merely rendering client options must not consult ADC.
	lazy := &lazyCredentials{detect: detect}
	creds := auth.NewCredentials(&auth.CredentialsOptions{
		TokenProvider:          auth.NewCachedTokenProvider(lazy, &auth.CachedTokenProviderOptions{DisableAsyncRefresh: true}),
		JSON:                   detect.CredentialsJSON,
		ProjectIDProvider:      lazy.property((*auth.Credentials).ProjectID),
		QuotaProjectIDProvider: lazy.property((*auth.Credentials).QuotaProjectID),
		UniverseDomainProvider: lazy.property((*auth.Credentials).UniverseDomain),
	})
	return creds, base.CloseIdleConnections, nil
}

type lazyCredentials struct {
	once   sync.Once
	detect credentials.DetectOptions
	creds  *auth.Credentials
	err    error
}

func (l *lazyCredentials) load() (*auth.Credentials, error) {
	l.once.Do(func() { l.creds, l.err = credentials.DetectDefault(&l.detect) })
	return l.creds, l.err
}

func (l *lazyCredentials) Token(ctx context.Context) (*auth.Token, error) {
	creds, err := l.load()
	if err != nil {
		return nil, err
	}
	return creds.Token(ctx)
}

func (l *lazyCredentials) property(get func(*auth.Credentials, context.Context) (string, error)) auth.CredentialsPropertyProvider {
	return auth.CredentialsPropertyFunc(func(ctx context.Context) (string, error) {
		creds, err := l.load()
		if err != nil {
			return "", err
		}
		return get(creds, ctx)
	})
}

// NewClient creates an API client and returns its endpoint and pool cleanup.
// With no options it also owns ADC token transport. Callers supplying options
// must supply credentials with their own lifetime, or disable authentication.
func NewClient(ctx context.Context, scopes []string, opts ...option.ClientOption) (*http.Client, string, func(), error) {
	for _, opt := range opts {
		if typ := reflect.TypeOf(opt); typ != nil && typ.Name() == "withHTTPClient" {
			// A supplied client and its pools belong to the caller.
			client, endpoint, err := htransport.NewClient(ctx, opts...)
			return client, endpoint, func() {}, err
		}
	}
	closeAuth := func() {}
	if len(opts) == 0 {
		creds, cleanup, err := Credentials(credentials.DetectOptions{Scopes: scopes, UseSelfSignedJWT: true})
		if err != nil {
			return nil, "", nil, err
		}
		closeAuth = cleanup
		opts = []option.ClientOption{option.WithAuthCredentials(creds)}
	}
	base, err := newBase()
	if err != nil {
		closeAuth()
		return nil, "", nil, err
	}
	cleanup := func() {
		base.CloseIdleConnections()
		closeAuth()
	}
	allOpts := append([]option.ClientOption{option.WithScopes(scopes...)}, opts...)
	rt, err := htransport.NewTransport(ctx, base, allOpts...)
	if err != nil {
		cleanup()
		return nil, "", nil, err
	}
	client := &http.Client{Transport: rt}
	// Resolve the endpoint without allocating another API transport. Other
	// options already apply to rt and can conflict with WithHTTPClient.
	endpointOpts := []option.ClientOption{option.WithHTTPClient(client)}
	for _, opt := range opts {
		if typ := reflect.TypeOf(opt); typ != nil && (typ.Name() == "withEndpoint" || typ.Name() == "withUniverseDomain") {
			endpointOpts = append(endpointOpts, opt)
		}
	}
	_, endpoint, err := htransport.NewClient(ctx, endpointOpts...)
	if err != nil {
		cleanup()
		return nil, "", nil, err
	}
	return client, endpoint, cleanup, nil
}
