// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

// Command healthcheck is a minimal, shell-free HTTP readiness/liveness probe
// bundled into distroless container images for Docker healthchecks.
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

const (
	defaultTargetURL = "http://127.0.0.1:13133/"
	probeTimeout     = 3 * time.Second
)

func main() {
	target := defaultTargetURL
	if len(os.Args) > 1 && os.Args[1] != "" {
		target = os.Args[1]
	}
	if err := probe(context.Background(), target, probeTimeout); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func probe(parent context.Context, target string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return fmt.Errorf("building healthcheck request: %w", err)
	}
	client := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("probing %s: %w", target, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("unhealthy status %d from %s", resp.StatusCode, target)
	}
	return nil
}
