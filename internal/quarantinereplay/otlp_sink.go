// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package quarantinereplay

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	defaultOTLPTimeout     = 30 * time.Second
	defaultOTLPMaxAttempts = 5
	otlpInitialBackoff     = 500 * time.Millisecond
	// otlpMaxRetryDelay caps both the backoff and a Retry-After delay.
	otlpMaxRetryDelay = time.Minute
	// otlpMaxResponseBytes bounds how much of a response body is read.
	otlpMaxResponseBytes = 1 << 20
	// otlpMaxErrorDetail bounds the response text quoted in an error.
	otlpMaxErrorDetail      = 512
	otlpProtobufContentType = "application/x-protobuf"
)

// OTLPHTTPSinkOptions configures an OTLPHTTPSink.
type OTLPHTTPSinkOptions struct {
	// Timeout bounds each HTTP attempt. Zero means 30 seconds.
	Timeout time.Duration
	// MaxAttempts bounds the attempts made for one batch, the first one
	// included. Zero means 5.
	MaxAttempts int
}

// OTLPHTTPSink sends replayed batches to an OTLP/HTTP receiver, such as an
// OpenTelemetry Collector's otlp receiver, as binary protobuf POST requests
// to <endpoint>/v1/traces, <endpoint>/v1/logs, and <endpoint>/v1/metrics.
//
// Responses are handled as the OTLP/HTTP specification describes. A batch
// that gets 429, 502, 503, or 504, or no response at all, is sent again
// after an exponential backoff that starts at 500ms, or after the delay a
// Retry-After header asks for (at most a minute), until MaxAttempts
// attempts were made. 400 and 413 mean the receiver will never take the
// batch, and a 2xx response whose partial_success rejects part of the batch
// means the rest landed but the rejected part never will; both are
// PermanentErrors, so a checkpointed replay moves past the record. Any other
// response, redirects included, is an ordinary error, which stops a
// checkpointed replay at the record: such a response says nothing about the
// batch (a wrong URL or missing credentials, for example), so the next run
// retries it.
//
// The sink owns its HTTP transport. Close releases the transport's
// connections.
type OTLPHTTPSink struct {
	endpoint    string
	display     string
	client      *http.Client
	transport   *http.Transport
	maxAttempts int
	sleep       func(context.Context, time.Duration) error
}

// NewOTLPHTTPSink returns a sink that sends to endpoint, the receiver's
// base URL, such as "http://localhost:4318". The signal paths are appended
// to it.
func NewOTLPHTTPSink(endpoint string, opts OTLPHTTPSinkOptions) (*OTLPHTTPSink, error) {
	u, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil {
		return nil, fmt.Errorf("invalid OTLP endpoint: %w", err)
	}
	switch {
	case u.Scheme != "http" && u.Scheme != "https":
		return nil, fmt.Errorf("invalid OTLP endpoint %q: the scheme must be http or https", u.Redacted())
	case u.Host == "":
		return nil, fmt.Errorf("invalid OTLP endpoint %q: it names no host", u.Redacted())
	case u.RawQuery != "" || u.Fragment != "":
		return nil, fmt.Errorf("invalid OTLP endpoint %q: it must not carry a query or fragment", u.Redacted())
	case opts.Timeout < 0:
		return nil, fmt.Errorf("OTLP timeout must not be negative, got %v", opts.Timeout)
	case opts.MaxAttempts < 0:
		return nil, fmt.Errorf("OTLP max attempts must not be negative, got %d", opts.MaxAttempts)
	}
	var transport *http.Transport
	if dt, ok := http.DefaultTransport.(*http.Transport); ok {
		transport = dt.Clone()
	} else {
		transport = &http.Transport{Proxy: http.ProxyFromEnvironment}
	}
	return &OTLPHTTPSink{
		endpoint: strings.TrimRight(u.String(), "/"),
		display:  strings.TrimRight(u.Redacted(), "/"),
		client: &http.Client{
			Transport: transport,
			Timeout:   cmp.Or(opts.Timeout, defaultOTLPTimeout),
			// A redirect is reported rather than followed, so a batch is
			// never sent to a host the operator did not name.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		transport:   transport,
		maxAttempts: cmp.Or(opts.MaxAttempts, defaultOTLPMaxAttempts),
		sleep:       sleepContext,
	}, nil
}

// ExportTraces implements Sink.
func (s *OTLPHTTPSink) ExportTraces(ctx context.Context, td ptrace.Traces) error {
	body, err := ptraceotlp.NewExportRequestFromTraces(td).MarshalProto()
	if err != nil {
		return &PermanentError{Err: fmt.Errorf("encoding traces: %w", err)}
	}
	return s.send(ctx, "/v1/traces", body, "spans", func(resp []byte, isJSON bool) (int64, string, error) {
		r := ptraceotlp.NewExportResponse()
		if err := unmarshalOTLPResponse(r, resp, isJSON); err != nil {
			return 0, "", err
		}
		return r.PartialSuccess().RejectedSpans(), r.PartialSuccess().ErrorMessage(), nil
	})
}

// ExportLogs implements Sink.
func (s *OTLPHTTPSink) ExportLogs(ctx context.Context, ld plog.Logs) error {
	body, err := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
	if err != nil {
		return &PermanentError{Err: fmt.Errorf("encoding logs: %w", err)}
	}
	return s.send(ctx, "/v1/logs", body, "log records", func(resp []byte, isJSON bool) (int64, string, error) {
		r := plogotlp.NewExportResponse()
		if err := unmarshalOTLPResponse(r, resp, isJSON); err != nil {
			return 0, "", err
		}
		return r.PartialSuccess().RejectedLogRecords(), r.PartialSuccess().ErrorMessage(), nil
	})
}

// ExportMetrics implements Sink.
func (s *OTLPHTTPSink) ExportMetrics(ctx context.Context, md pmetric.Metrics) error {
	body, err := pmetricotlp.NewExportRequestFromMetrics(md).MarshalProto()
	if err != nil {
		return &PermanentError{Err: fmt.Errorf("encoding metrics: %w", err)}
	}
	return s.send(ctx, "/v1/metrics", body, "data points", func(resp []byte, isJSON bool) (int64, string, error) {
		r := pmetricotlp.NewExportResponse()
		if err := unmarshalOTLPResponse(r, resp, isJSON); err != nil {
			return 0, "", err
		}
		return r.PartialSuccess().RejectedDataPoints(), r.PartialSuccess().ErrorMessage(), nil
	})
}

// Close releases the sink's connections. The sink must not be used
// afterwards.
func (s *OTLPHTTPSink) Close() {
	s.transport.CloseIdleConnections()
}

// partialSuccessFunc decodes an export response body and returns how many
// items the receiver rejected and its explanation.
type partialSuccessFunc func(body []byte, isJSON bool) (rejected int64, message string, err error)

type otlpResponse interface {
	UnmarshalProto(data []byte) error
	UnmarshalJSON(data []byte) error
}

func unmarshalOTLPResponse(r otlpResponse, body []byte, isJSON bool) error {
	if isJSON {
		return r.UnmarshalJSON(body)
	}
	return r.UnmarshalProto(body)
}

// send posts body to path, retrying as the type comment describes. unit
// names the items partial success counts.
func (s *OTLPHTTPSink) send(ctx context.Context, path string, body []byte, unit string, partial partialSuccessFunc) error {
	for attempt := 1; ; attempt++ {
		retry, wait, err := s.post(ctx, path, body, unit, partial)
		if err == nil {
			return nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if !retry || attempt >= s.maxAttempts {
			if retry {
				return fmt.Errorf("%w (gave up after %d attempts)", err, attempt)
			}
			return err
		}
		if wait <= 0 {
			wait = otlpBackoff(attempt)
		}
		if err := s.sleep(ctx, wait); err != nil {
			return err
		}
	}
}

// post makes one attempt. retry reports a failure that another attempt may
// not hit; wait is the delay the receiver asked for, if any.
func (s *OTLPHTTPSink) post(ctx context.Context, path string, body []byte, unit string, partial partialSuccessFunc) (retry bool, wait time.Duration, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint+path, bytes.NewReader(body))
	if err != nil {
		return false, 0, &PermanentError{Err: fmt.Errorf("building the OTLP request: %w", err)}
	}
	req.Header.Set("Content-Type", otlpProtobufContentType)
	resp, err := s.client.Do(req)
	if err != nil {
		return true, 0, fmt.Errorf("POST %s%s: %w", s.display, path, err)
	}
	defer func() {
		// Drain what is left so the connection can be reused.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, otlpMaxResponseBytes))
		_ = resp.Body.Close()
	}()
	respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, otlpMaxResponseBytes))
	mediaType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	isJSON := mediaType == "application/json"

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		// The receiver took the batch. A body that cannot be read or
		// decoded leaves nothing to report: sending the batch again would
		// only duplicate it.
		if readErr != nil || len(respBody) == 0 {
			return false, 0, nil
		}
		rejected, message, perr := partial(respBody, isJSON)
		if perr == nil && rejected > 0 {
			return false, 0, &PermanentError{Err: fmt.Errorf("POST %s%s: the receiver rejected %d %s: %s",
				s.display, path, rejected, unit, quoteDetail(message))}
		}
		return false, 0, nil
	}

	err = fmt.Errorf("POST %s%s: %s%s", s.display, path, resp.Status, errorDetail(respBody, mediaType))
	switch resp.StatusCode {
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true, parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()), err
	case http.StatusBadRequest, http.StatusRequestEntityTooLarge:
		return false, 0, &PermanentError{Err: err}
	default:
		return false, 0, err
	}
}

// otlpBackoff returns the delay before attempt+1: 500ms doubled per
// attempt, at most otlpMaxRetryDelay.
func otlpBackoff(attempt int) time.Duration {
	d := otlpInitialBackoff
	for i := 1; i < attempt && d < otlpMaxRetryDelay; i++ {
		d *= 2
	}
	return min(d, otlpMaxRetryDelay)
}

// parseRetryAfter reads a Retry-After header, either delay seconds or an
// HTTP date, capped at otlpMaxRetryDelay. It returns 0 for a missing,
// malformed, or past value.
func parseRetryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.ParseInt(v, 10, 64); err == nil {
		if secs <= 0 {
			return 0
		}
		if secs >= int64(otlpMaxRetryDelay/time.Second) {
			return otlpMaxRetryDelay
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := t.Sub(now); d > 0 {
			return min(d, otlpMaxRetryDelay)
		}
	}
	return 0
}

// errorDetail extracts the message of the google.rpc.Status an OTLP/HTTP
// receiver returns with an error, or quotes the start of any other body.
func errorDetail(body []byte, mediaType string) string {
	if len(body) == 0 {
		return ""
	}
	message := string(body)
	if mediaType == otlpProtobufContentType {
		var st statuspb.Status
		if err := proto.Unmarshal(body, &st); err == nil && st.GetMessage() != "" {
			message = st.GetMessage()
		}
	}
	return ": " + quoteDetail(message)
}

func quoteDetail(s string) string {
	if len(s) > otlpMaxErrorDetail {
		s = s[:otlpMaxErrorDetail] + "..."
	}
	return strconv.Quote(s)
}
