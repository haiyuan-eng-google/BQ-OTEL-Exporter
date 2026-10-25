// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package googlehttp

import (
	"context"
	"net/http"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/credentials/impersonate"
)

// ImpersonatedCredentials binds every refresh to both its RPC and its owner's
// lifetime, including refreshes of stale tokens.
func ImpersonatedCredentials(lifetime context.Context, source *auth.Credentials, client *http.Client, target string, delegates, scopes []string) *auth.Credentials {
	refresh := tokenProviderFunc(func(ctx context.Context) (*auth.Token, error) {
		if err := lifetime.Err(); err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ctx, cancel := context.WithCancel(ctx)
		stop := context.AfterFunc(lifetime, cancel)
		defer stop()
		defer cancel()
		if lifetime.Err() != nil {
			cancel()
		}
		// A fresh provider leaves caching to our synchronous outer cache; the
		// library's default cache otherwise starts refreshes on a background context.
		creds, err := impersonate.NewCredentials(&impersonate.CredentialsOptions{
			TargetPrincipal: target, Delegates: delegates, Scopes: scopes,
			Client: client, Credentials: source,
		})
		if err != nil {
			return nil, err
		}
		return creds.Token(ctx)
	})
	return auth.NewCredentials(&auth.CredentialsOptions{
		// Expose the library's cache directly, so HTTP auth middleware recognizes it
		// and does not add a second cache with asynchronous refresh enabled.
		TokenProvider:          auth.NewCachedTokenProvider(refresh, &auth.CachedTokenProviderOptions{DisableAsyncRefresh: true}),
		UniverseDomainProvider: auth.CredentialsPropertyFunc(source.UniverseDomain),
	})
}

type tokenProviderFunc func(context.Context) (*auth.Token, error)

func (f tokenProviderFunc) Token(ctx context.Context) (*auth.Token, error) { return f(ctx) }
