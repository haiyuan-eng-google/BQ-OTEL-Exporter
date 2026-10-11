// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
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

func TestCredentialsFromFileLoadsSupportedTypes(t *testing.T) {
	scopes := []string{"https://www.googleapis.com/auth/bigquery.insertdata"}
	for name, contents := range map[string]string{
		"service account": serviceAccountFile,
		"authorized user": authorizedUserFile,
	} {
		t.Run(name, func(t *testing.T) {
			path := writeCredentialsFile(t, contents)

			creds, err := credentialsFromFile(context.Background(), path, scopes)
			if err != nil {
				t.Fatalf("credentialsFromFile() error = %v", err)
			}
			if creds == nil || creds.TokenSource == nil {
				t.Fatal("credentialsFromFile() returned credentials without a token source")
			}
		})
	}
}

func TestCredentialsFromFileRejectsUnsupportedTypes(t *testing.T) {
	const sentinel = "sentinel-must-not-leak"
	for name, contents := range map[string]string{
		"gdch service account": `{"type":"gdch_service_account","private_key":"` + sentinel + `"}`,
		"unknown type":         `{"type":"something_else","private_key":"` + sentinel + `"}`,
		"missing type":         `{"private_key":"` + sentinel + `"}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := writeCredentialsFile(t, contents)

			_, err := credentialsFromFile(context.Background(), path, nil)
			if err == nil {
				t.Fatal("credentialsFromFile() error = nil, want an unsupported type error")
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

func TestCredentialsFromFileRejectsMalformedFile(t *testing.T) {
	path := writeCredentialsFile(t, "this is not json")

	_, err := credentialsFromFile(context.Background(), path, nil)
	if err == nil || !strings.Contains(err.Error(), "parsing credentials file") {
		t.Fatalf("error = %v, want a parse error", err)
	}
}

func TestCredentialsFromFileReportsMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent.json")

	_, err := credentialsFromFile(context.Background(), path, nil)
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
