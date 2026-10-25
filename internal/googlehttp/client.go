// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

// Package googlehttp retains the connection pools underneath API and auth
// middleware, which does not forward CloseIdleConnections.
package googlehttp

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptrace"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"runtime"
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

// Credentials owns ordinary token pools and drains certificate-specific
// connections after each synchronous token exchange.
// The provided JSON is restricted to supported credential types and loaded
// through the typed API. Nil JSON selects ADC. The returned cleanup must run
// after all clients using the credentials close.
func Credentials(detect credentials.DetectOptions, data []byte) (*auth.Credentials, func(), error) {
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
	lazy := &lazyCredentials{detect: detect, data: data}
	creds := auth.NewCredentials(&auth.CredentialsOptions{
		TokenProvider:          auth.NewCachedTokenProvider(lazy, &auth.CachedTokenProviderOptions{DisableAsyncRefresh: true}),
		JSON:                   data,
		ProjectIDProvider:      lazy.property((*auth.Credentials).ProjectID),
		QuotaProjectIDProvider: lazy.property((*auth.Credentials).QuotaProjectID),
		UniverseDomainProvider: lazy.property((*auth.Credentials).UniverseDomain),
	})
	return creds, base.CloseIdleConnections, nil
}

type lazyCredentials struct {
	closeTokenConnections bool
	once                  sync.Once
	detect                credentials.DetectOptions
	data                  []byte
	creds                 *auth.Credentials
	err                   error
}

func (l *lazyCredentials) load() (*auth.Credentials, error) {
	l.once.Do(func() {
		data := l.data
		if len(data) == 0 {
			if filename := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"); filename != "" {
				data, l.err = os.ReadFile(filename)
				if l.err != nil {
					return
				}
			} else {
				homeDir := os.Getenv("HOME")
				if homeDir == "" {
					if current, err := user.Current(); err == nil {
						homeDir = current.HomeDir
					}
				}
				filename := filepath.Join(homeDir, ".config", "gcloud", "application_default_credentials.json")
				if runtime.GOOS == "windows" {
					filename = filepath.Join(os.Getenv("APPDATA"), "gcloud", "application_default_credentials.json")
				}
				data, _ = os.ReadFile(filename)
				if len(data) == 0 {
					l.creds, l.err = credentials.DetectDefault(&l.detect)
					return
				}
			}
		}
		var header struct {
			Type credentials.CredType `json:"type"`
		}
		if err := json.Unmarshal(data, &header); err != nil {
			l.err = fmt.Errorf("parsing credentials JSON: %w", err)
			return
		}
		if header.Type == credentials.ExternalAccount {
			var file struct {
				Source struct {
					Certificate json.RawMessage `json:"certificate"`
				} `json:"credential_source"`
			}
			if err := json.Unmarshal(data, &file); err != nil {
				l.err = err
				return
			}
			if len(file.Source.Certificate) > 0 && string(file.Source.Certificate) != "null" {
				// A custom client suppresses default workload certificate selection. Let
				// the typed library resolve both default and explicit locations, regardless
				// of the separate ambient API certificate setting.
				l.detect.Client = nil
				l.closeTokenConnections = true
			}
		}
		switch header.Type {
		case credentials.ServiceAccount, credentials.AuthorizedUser,
			credentials.ExternalAccount, credentials.ExternalAccountAuthorizedUser,
			credentials.ImpersonatedServiceAccount, credentials.GDCHServiceAccount:
			l.creds, l.err = credentials.NewCredentialsFromJSON(header.Type, data, &l.detect)
		case "":
			// Preserve the startup diagnostic for an ADC file that can be read
			// but is not a credential, including an empty JSON placeholder.
			l.err = errors.New("google: missing 'type' field in credentials")
		default:
			l.err = fmt.Errorf("unsupported credentials type %q", header.Type)
		}
	})
	return l.creds, l.err
}

func (l *lazyCredentials) Token(ctx context.Context) (*auth.Token, error) {
	creds, err := l.load()
	if err != nil {
		return nil, err
	}
	if l.closeTokenConnections {
		// The pinned library does not expose its certificate-specific transport.
		// These refreshes are synchronous: close their actual connections once all
		// token response bodies have been consumed, instead of leaving a hidden pool
		// alive until process exit. This also covers HTTP/2 and impersonation hops.
		trace, closeConnections := tokenConnectionTrace()
		defer closeConnections()
		ctx = httptrace.WithClientTrace(ctx, trace)
	}
	return creds.Token(ctx)
}

func tokenConnectionTrace() (*httptrace.ClientTrace, func()) {
	var mu sync.Mutex
	connections := make(map[net.Conn]struct{})
	trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) {
		mu.Lock()
		connections[info.Conn] = struct{}{}
		mu.Unlock()
	}}
	closeConnections := func() {
		mu.Lock()
		defer mu.Unlock()
		for conn := range connections {
			_ = conn.Close()
		}
	}
	return trace, closeConnections
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

func hasAuthOption(opts []option.ClientOption) bool {
	for _, opt := range opts {
		if typ := reflect.TypeOf(opt); typ != nil {
			switch typ.Name() {
			case "withoutAuthentication", "withAuthCredentials", "withTokenSource",
				"withCredFile", "withCredentialsJSON", "withAuthCredentialsJSON",
				"withCredentials", "withAPIKey":
				return true
			}
		}
	}
	return false
}

// NewClient creates an API client and returns its endpoint and pool cleanup.
// It also owns ADC token transport when options only configure the API.
// Explicit credentials and HTTP clients retain their caller-owned lifetime.
func NewClient(ctx context.Context, scopes []string, opts ...option.ClientOption) (*http.Client, string, func(), error) {
	for _, opt := range opts {
		if typ := reflect.TypeOf(opt); typ != nil && typ.Name() == "withHTTPClient" {
			// A supplied client and its pools belong to the caller.
			client, endpoint, err := htransport.NewClient(ctx, opts...)
			return client, endpoint, func() {}, err
		}
	}
	closeAuth := func() {}
	if !hasAuthOption(opts) {
		creds, cleanup, err := Credentials(credentials.DetectOptions{Scopes: scopes, UseSelfSignedJWT: true}, nil)
		if err != nil {
			return nil, "", nil, err
		}
		closeAuth = cleanup
		opts = append([]option.ClientOption{option.WithAuthCredentials(creds)}, opts...)
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
	if err := configureAPICertificate(base, opts); err != nil {
		cleanup()
		return nil, "", nil, err
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

// configureAPICertificate resolves the provider before auth middleware wraps the
// retained pool. NewTransport leaves this step to callers supplying a base.
func configureAPICertificate(base *http.Transport, opts []option.ClientOption) error {
	config := &httptransport.Options{DisableAuthentication: true, BaseRoundTripper: base}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		switch reflect.TypeOf(opt).Name() {
		case "withUniverseDomain":
			config.UniverseDomain = reflect.ValueOf(opt).String()
		case "withEndpoint":
			config.Endpoint = reflect.ValueOf(opt).String()
		case "withClientCertSource":
			// ClientOption.Apply exposes its settings argument only through an internal
			// type. Apply the option to that type to read its exported certificate
			// callback, without inspecting private option fields or using unsafe.
			apply := reflect.ValueOf(opt).MethodByName("Apply")
			settings := reflect.New(apply.Type().In(0).Elem())
			apply.Call([]reflect.Value{settings})
			source := settings.Elem().FieldByName("ClientCertSource").Interface().(option.ClientCertSource)
			config.ClientCertProvider = source
		}
	}
	if _, err := httptransport.NewClient(config); err != nil {
		return err
	}
	if config.ClientCertProvider != nil {
		if base.TLSClientConfig == nil {
			base.TLSClientConfig = &tls.Config{}
		}
		base.TLSClientConfig.GetClientCertificate = config.ClientCertProvider
	}
	// newBase clones DialTLSContext as well as TLSClientConfig. In particular,
	// installing the certificate must not discard a caller's TLS dialer.
	return nil
}
