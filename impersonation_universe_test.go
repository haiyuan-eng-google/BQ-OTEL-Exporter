// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
	bigqueryapi "google.golang.org/api/bigquery/v2"
)

func TestExplicitImpersonationUniverseDomain(t *testing.T) {
	for _, domain := range []string{"", "example.com"} {
		name := domain
		if name == "" {
			name = "default"
		}
		t.Run(name, func(t *testing.T) {
			baseline := append([]goleak.Option{goleak.IgnoreCurrent()}, integrationGoleakOptions()...)
			t.Cleanup(func() { goleak.VerifyNone(t, baseline...) })
			var iamRequests atomic.Int64
			e, creds, sourceRequests := impersonationTestCredentials(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				iamRequests.Add(1)
				var request struct {
					Scopes   []string `json:"scope"`
					Lifetime string   `json:"lifetime"`
				}
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
				}
				if len(request.Scopes) != 1 || request.Scopes[0] != bigqueryapi.BigqueryInsertdataScope || request.Lifetime != "3600s" {
					t.Errorf("impersonation request=%+v", request)
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprintf(w, `{"accessToken":"domain-token","expireTime":%q}`, time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
			}), domain)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			token, err := creds.Token(ctx)
			if err != nil {
				t.Fatalf("impersonation token exchange for universe %q: %v", domain, err)
			}
			if token.Value != "domain-token" || sourceRequests.Load() != 1 || iamRequests.Load() != 1 {
				t.Fatalf("token=%v source exchanges=%d IAM exchanges=%d", token, sourceRequests.Load(), iamRequests.Load())
			}
			if err := e.shutdownStatus(context.Background(), false); err != nil {
				t.Fatal(err)
			}
		})
	}
}
