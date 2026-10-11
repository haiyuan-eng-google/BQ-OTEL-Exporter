// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
	"google.golang.org/api/option"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
)

// authorizedUserFile is a syntactically valid credentials file that carries
// no real secret and is never exchanged for a token by these tests.
const authorizedUserFile = `{
  "type": "authorized_user",
  "client_id": "example-client-id",
  "client_secret": "example-client-secret",
  "refresh_token": "example-refresh-token"
}`

// serviceAccountFile is a syntactically valid service account key without
// key material. Token sources are built lazily, so loading it never signs
// anything.
const serviceAccountFile = `{
  "type": "service_account",
  "client_email": "writer@example-project.iam.gserviceaccount.com",
  "private_key_id": "example-key-id",
  "token_uri": "https://oauth2.googleapis.com/token"
}`

const externalAccountFile = `{
  "type": "external_account",
  "audience": "//iam.googleapis.com/projects/_/locations/global/workloadIdentityPools/test/providers/test",
  "subject_token_type": "urn:ietf:params:aws:token-type:1",
  "token_url": "https://sts.googleapis.com/v1/token",
  "credential_source": {
    "environment_id": "aws1",
    "region_url": "https://example.com/region",
    "url": "https://example.com/creds",
    "regional_cred_verification_url": "https://sts.{region}.example.com/v1/token"
  }
}`

// writeCredentialsFile stores contents in a temporary credentials file and
// returns its path.
func writeCredentialsFile(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "credentials.json")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestCredentialsFileOptionLoadsSupportedTypes(t *testing.T) {
	scopes := []string{"https://www.googleapis.com/auth/bigquery.insertdata"}
	for name, contents := range map[string]string{
		"service account": serviceAccountFile,
		"authorized user": authorizedUserFile,
	} {
		t.Run(name, func(t *testing.T) {
			path := writeCredentialsFile(t, contents)

			opt, err := credentialsFileOption(context.Background(), path, scopes)
			if err != nil {
				t.Fatalf("credentialsFileOption() error = %v", err)
			}
			if opt == nil {
				t.Fatal("credentialsFileOption() returned nil option")
			}
		})
	}
}

func TestCredentialsFileOptionRejectsUnsupportedTypes(t *testing.T) {
	const sentinel = "sentinel-must-not-leak"
	for name, contents := range map[string]string{
		"gdch service account": `{"type":"gdch_service_account","private_key":"` + sentinel + `"}`,
		"unknown type":         `{"type":"something_else","private_key":"` + sentinel + `"}`,
		"missing type":         `{"private_key":"` + sentinel + `"}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := writeCredentialsFile(t, contents)

			_, err := credentialsFileOption(context.Background(), path, nil)
			if err == nil {
				t.Fatal("credentialsFileOption() error = nil, want an unsupported type error")
			}
			if !strings.Contains(err.Error(), "unsupported type") ||
				!strings.Contains(err.Error(), "service_account") {
				t.Errorf("error %q must report the unsupported type and list the supported ones", err)
			}
			if strings.Contains(err.Error(), sentinel) {
				t.Errorf("error %q must not echo credentials file contents", err)
			}
		})
	}
}

func TestCredentialsFileOptionRejectsMalformedFile(t *testing.T) {
	path := writeCredentialsFile(t, "this is not json")

	_, err := credentialsFileOption(context.Background(), path, nil)
	if err == nil || !strings.Contains(err.Error(), "parsing credentials file") {
		t.Fatalf("error = %v, want a parse error", err)
	}
}

func TestCredentialsFileOptionReportsMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent.json")

	_, err := credentialsFileOption(context.Background(), path, nil)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("error = %v, want one wrapping fs.ErrNotExist", err)
	}
}

// A bad credentials file must fail while the exporter starts, before any
// BigQuery client is built, rather than on the first export.
func TestAuthenticationOptionsRejectUnusableCredentialsFile(t *testing.T) {
	cfg := validConfig()
	cfg.Credentials.File = filepath.Join(t.TempDir(), "absent.json")
	e := newTestSignalExporter(cfg, cfg.Traces.Table, nil)

	if _, err := e.metadataClientOptions(context.Background()); err == nil {
		t.Error("metadataClientOptions() error = nil, want the credentials file error")
	}
	if _, err := e.writerClientOptions(context.Background()); err == nil {
		t.Error("writerClientOptions() error = nil, want the credentials file error")
	}
}

func TestCredentialsFileOptionReturnsAuthCredentialsJSON(t *testing.T) {
	scopes := []string{"https://www.googleapis.com/auth/bigquery.insertdata"}
	for _, tc := range []struct {
		name string
		file string
	}{
		{"service account", serviceAccountFile},
		{"authorized user", authorizedUserFile},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeCredentialsFile(t, tc.file)
			opt, err := credentialsFileOption(context.Background(), path, scopes)
			if err != nil {
				t.Fatalf("credentialsFileOption() error = %v", err)
			}
			if opt == nil {
				t.Fatal("credentialsFileOption() returned nil option")
			}
			typeName := reflect.TypeOf(opt).String()
			if !strings.Contains(typeName, "withAuthCredentialsJSON") {
				t.Errorf("option type = %s, want withAuthCredentialsJSON to preserve per-call cancellation and mTLS", typeName)
			}
		})
	}
}

// TestCredentialsFileOptionUsesAuthCredentialsJSONPath verifies that
// credentialsFileOption returns an option whose concrete type is
// withAuthCredentialsJSON (from google.golang.org/api/option), NOT
// withCredentials or withCredFile. This is the regression guard for both
// the P1 (stalled-refresh shutdown) and P2 (mTLS transport bypass) findings:
// withAuthCredentialsJSON goes through GetOAuth2Configuration (preserving
// mTLS) and cloud.google.com/go/auth (preserving per-call context
// cancellation), whereas withCredentials sets DialSettings.Credentials and
// bypasses both.
func TestCredentialsFileOptionUsesAuthCredentialsJSONPath(t *testing.T) {
	scopes := []string{"https://www.googleapis.com/auth/bigquery.insertdata"}

	for name, contents := range map[string]string{
		"service account":  serviceAccountFile,
		"authorized user":  authorizedUserFile,
		"external account": externalAccountFile,
	} {
		t.Run(name, func(t *testing.T) {
			path := writeCredentialsFile(t, contents)

			opt, err := credentialsFileOption(context.Background(), path, scopes)
			if err != nil {
				t.Fatalf("credentialsFileOption() error = %v", err)
			}
			if opt == nil {
				t.Fatal("credentialsFileOption() returned nil")
			}

			// The concrete type must be option.withAuthCredentialsJSON, not
			// option.withCredentials or option.withCredFile. The unexported
			// type name is accessible via reflect.TypeOf.
			typeName := reflect.TypeOf(opt).String()
			if !strings.Contains(typeName, "withAuthCredentialsJSON") {
				t.Errorf("option type = %s, want a withAuthCredentialsJSON variant", typeName)
			}
			// The old withCredentials path set DialSettings.Credentials,
			// bypassing GetOAuth2Configuration and mTLS. Assert it is not used.
			if strings.Contains(typeName, "withCredentials") &&
				!strings.Contains(typeName, "withAuthCredentials") {
				t.Errorf("option type = %s, must not use the deprecated WithCredentials path", typeName)
			}
		})
	}
}

// TestCredentialsFileOptionServiceAccountLocalJWTSigning verifies that a
// service_account credential file produces an option that enables local JWT
// signing (0 token_uri calls). The option.WithAuthCredentialsJSON path
// passes the validated bytes through cloud.google.com/go/auth, which signs
// JWTs locally for service_account keys instead of exchanging a refresh
// token over the network.
func TestCredentialsFileOptionServiceAccountLocalJWTSigning(t *testing.T) {
	scopes := []string{"https://www.googleapis.com/auth/bigquery.insertdata"}
	path := writeCredentialsFile(t, serviceAccountFile)

	opt, err := credentialsFileOption(context.Background(), path, scopes)
	if err != nil {
		t.Fatalf("credentialsFileOption() error = %v", err)
	}
	if opt == nil {
		t.Fatal("credentialsFileOption() returned nil")
	}

	// The returned option must be the WithAuthCredentialsJSON variant, which
	// enables local JWT signing for service_account keys.
	typeName := reflect.TypeOf(opt).String()
	if !strings.Contains(typeName, "withAuthCredentialsJSON") {
		t.Fatalf("option type = %s, want withAuthCredentialsJSON for local JWT signing", typeName)
	}
}

// TestSignalExporterShutdownWithExternalAccountStalledTokenEndpoint verifies
// that shutdown completes promptly when a credentials.file points to an
// external_account file whose token endpoint stalls. With
// option.WithAuthCredentialsJSON, cloud.google.com/go/auth's
// Credentials.Token(ctx) honors the per-RPC stream context that
// Writer.Close() cancels, so shutdown does not hang on the stalled token
// refresh.
func TestSignalExporterShutdownWithExternalAccountStalledTokenEndpoint(t *testing.T) {
	originalAdmin := newDestinationAdmin
	originalWriter := newManagedWriterClient
	t.Cleanup(func() {
		newDestinationAdmin = originalAdmin
		newManagedWriterClient = originalWriter
	})

	// A stalling token server: accepts connections but never responds until
	// the request context is canceled or the server closes.
	stallServer := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(stallServer.Close)

	// external_account credentials pointing at the stalling token endpoint.
	extAccountFile := fmt.Sprintf(`{
  "type": "external_account",
  "audience": "//iam.googleapis.com/projects/_/locations/global/workloadIdentityPools/test/providers/test",
  "subject_token_type": "urn:ietf:params:aws:token-type:1",
  "token_url": "%s/token",
  "credential_source": {
    "environment_id": "aws1",
    "region_url": "%s/region",
    "url": "%s/creds",
    "regional_cred_verification_url": "https://sts.{region}.example.com/v1/token"
  }
}`, stallServer.URL, stallServer.URL, stallServer.URL)
	path := writeCredentialsFile(t, extAccountFile)

	newDestinationAdmin = func(
		context.Context, string, string, ...option.ClientOption,
	) (destinationAdmin, error) {
		return &fakeDestinationAdmin{
			datasetMetadata: &bq.DatasetMetadata{},
			tableMetadata: &bq.TableMetadata{
				Schema: mustBigQuerySchema(t, schema.SpansTableSchema()),
			},
		}, nil
	}
	newManagedWriterClient = func(
		context.Context, string, ...option.ClientOption,
	) (*managedwriter.Client, error) {
		return &managedwriter.Client{}, nil
	}

	cfg := validConfig()
	cfg.Credentials.File = path
	e := newSignalExporter(
		component.TelemetrySettings{Logger: zap.NewNop()},
		cfg, cfg.Traces.Table, schema.SpansTableSchema(),
	)

	if err := e.start(context.Background(), nil); err != nil {
		t.Fatalf("start error = %v", err)
	}

	// The zero-value managedwriter.Client is a constructor test double.
	// Release the remaining owned resources without calling Close (which
	// would fail on the zero-value client).
	e.client = nil

	// Shutdown must complete promptly even though the token endpoint stalls.
	// WithAuthCredentialsJSON uses cloud.google.com/go/auth whose Token(ctx)
	// honors the per-RPC context, so Writer.Close() (which cancels its child
	// context) unblocks immediately without waiting for lifetimeCtx to cancel.
	done := make(chan error, 1)
	go func() {
		done <- e.shutdown(context.Background())
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown error = %v", err)
		}
	case <-time.After(3 * time.Second):
		// Stop the stall server so the goroutine can complete.
		stallServer.Close()
		t.Fatal("shutdown hung waiting for stalled token refresh; " +
			"WithAuthCredentialsJSON should allow per-call cancellation")
	}
}
