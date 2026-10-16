// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package quarantinereplay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
)

// FuzzQuarantineReplayRow flag bits.
const (
	// replayFuzzNumericTime carries ingest_time as a JSON number when it
	// parses as one.
	replayFuzzNumericTime = 1 << iota
	// replayFuzzPayloadRef moves the payload from raw_payload to an object
	// behind payload_ref.
	replayFuzzPayloadRef
	// replayFuzzValidPayload replaces the payload with valid OTLP/JSON for
	// the row's signal (spans when the signal is not recognized) that
	// carries the fuzzed payload text.
	replayFuzzValidPayload
	// replayFuzzSignalFilter sets Filter.Signal to the fuzzed filter.
	replayFuzzSignalFilter
	// replayFuzzErrorCodes sets Filter.ErrorCodes to replayFuzzErrorCodeFilter.
	replayFuzzErrorCodes
	// replayFuzzTimeWindow bounds Filter.Since and Filter.Until to one day.
	replayFuzzTimeWindow
	// replayFuzzFailingSink makes every export fail.
	replayFuzzFailingSink
	// replayFuzzCanceled cancels the context before Replay starts.
	replayFuzzCanceled
)

// replayFuzzErrorCodeFilter mixes case and padding, both of which the error
// code filter ignores.
var replayFuzzErrorCodeFilter = []string{"OVERSIZE_ROW", " row_error "}

var (
	replayFuzzSince = time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)
	replayFuzzUntil = time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
)

// replayFuzzTimestamp is 2026-10-11T12:00:00Z, inside the time window.
const replayFuzzTimestamp = pcommon.Timestamp(1_791_720_000_000_000_000)

var errReplayFuzzExport = errors.New("fuzz sink rejects every batch")

// FuzzQuarantineReplayRow replays a quarantine row built from fuzzed fields,
// followed by a metadata-only twin, both as JSONL and as a JSON array, and
// then replays the raw stream bytes too. Whatever the input, Replay must not
// panic and every row it reads must land in exactly one outcome. For the
// built rows the two encodings must agree, and each row must be counted as
// its filters, payload, and signal dictate: a valid payload replays exactly
// once and losslessly, a row without a payload is skipped, and every other
// row fails with an error.
//
// flags selects how the row is built and replayed; see the replayFuzz*
// constants.
func FuzzQuarantineReplayRow(f *testing.F) {
	none := []byte(nil)
	f.Add("spans", "", "OVERSIZE_ROW", "checkout", "2026-10-11T12:00:00Z", "", uint8(replayFuzzValidPayload), none)
	f.Add("otel_logs", "", "ROW_ERROR", "user said hello", "2026-10-11 12:00:00.5 UTC", "", uint8(replayFuzzValidPayload), none)
	f.Add(" Metrics ", "", "FIELDS_ERROR", "gen_ai.client.token.usage", "1791720000", "", uint8(replayFuzzValidPayload|replayFuzzNumericTime), none)
	// A metadata-only row and a row whose signal no sink handles.
	f.Add("spans", "", "OVERSIZE_ROW", "", "", "", uint8(0), none)
	f.Add("profiles", "", "ROW_ERROR", "unknown signal", "", "", uint8(replayFuzzValidPayload), none)
	// A payload that is not OTLP/JSON.
	f.Add("logs", "", "ROW_ERROR", "{not-valid-otlp", "", "", uint8(0), none)
	// Offloaded payloads: one the fetcher resolves and one it cannot.
	f.Add("traces", "", "OVERSIZE_ROW", "offloaded", "", "gs://quarantine/spans/1.json", uint8(replayFuzzPayloadRef|replayFuzzValidPayload), none)
	f.Add("traces", "", "OVERSIZE_ROW", "offloaded", "", "https://example.com/1.json", uint8(replayFuzzPayloadRef|replayFuzzValidPayload), none)
	// Filters: a match on every filter, a row outside the window, and a
	// window applied to a row whose ingest_time does not parse.
	f.Add("logs", "metrics, traces,logs", "row_error", "kept", "1791720000000000", "",
		uint8(replayFuzzNumericTime|replayFuzzValidPayload|replayFuzzSignalFilter|replayFuzzErrorCodes|replayFuzzTimeWindow), none)
	f.Add("spans", "", "ROW_ERROR", "late", "2026-10-13T00:00:00Z", "", uint8(replayFuzzValidPayload|replayFuzzTimeWindow), none)
	f.Add("spans", "", "ROW_ERROR", "undated", "yesterday", "", uint8(replayFuzzValidPayload|replayFuzzTimeWindow), none)
	// A sink that rejects every batch, and a canceled replay.
	f.Add("spans", "", "ROW_ERROR", "rejected", "", "", uint8(replayFuzzValidPayload|replayFuzzFailingSink), none)
	f.Add("metrics", "", "ROW_ERROR", "canceled", "", "", uint8(replayFuzzValidPayload|replayFuzzCanceled), none)
	// Raw streams: a JSON array, and JSONL that stops at a malformed line.
	f.Add("spans", "", "", "", "", "", uint8(0),
		[]byte(`[{"signal":"spans","raw_payload":"{}"},{"signal":"logs","payload_ref":{"uri":"gs://b/o","version":"7"}}]`))
	f.Add("spans", "", "", "", "", "", uint8(0),
		[]byte("{\"signal\":\"metrics\",\"raw_payload\":\"{\\\"resourceMetrics\\\":[]}\"}\n\n{\"ingest_time\":1e400}\nnot json\n"))

	f.Fuzz(func(t *testing.T, signal, signalFilter, errorCode, rawPayload, ingestTime, refURI string, flags uint8, stream []byte) {
		h := newReplayFuzzHarness(t, signal, signalFilter, rawPayload, flags)
		row := Record{
			IngestTime:   replayFuzzIngestTime(ingestTime, flags&replayFuzzNumericTime != 0),
			Signal:       signal,
			ErrorCode:    errorCode,
			ErrorMessage: "fuzzed quarantine row",
			ServiceName:  "fuzz",
			RawPayload:   h.payload,
		}
		if flags&replayFuzzPayloadRef != 0 {
			row.RawPayload = ""
			row.PayloadRef = &ObjectRef{URI: refURI}
		}
		twin := Record{IngestTime: row.IngestTime, Signal: row.Signal, ErrorCode: row.ErrorCode, ErrorMessage: "metadata only"}
		h.checkRows(t, row, twin)
		h.replay(t, "fuzzed stream", stream)
	})
}

// replayFuzzHarness is one FuzzQuarantineReplayRow configuration.
type replayFuzzHarness struct {
	ctx      context.Context
	opts     Options
	canceled bool
	failing  bool
	// payload is what the row carries, inline or behind payload_ref.
	payload string
	// built is the protobuf encoding of the batch payload holds, when the
	// harness generated it.
	built []byte
}

func newReplayFuzzHarness(t *testing.T, signal, signalFilter, rawPayload string, flags uint8) *replayFuzzHarness {
	t.Helper()
	h := &replayFuzzHarness{
		ctx:      context.Background(),
		canceled: flags&replayFuzzCanceled != 0,
		failing:  flags&replayFuzzFailingSink != 0,
		payload:  rawPayload,
	}
	if flags&replayFuzzValidPayload != 0 {
		h.payload, h.built = replayFuzzBatch(t, CanonicalSignal(signal), rawPayload)
	}
	if flags&replayFuzzSignalFilter != 0 {
		h.opts.Filter.Signal = signalFilter
	}
	if flags&replayFuzzErrorCodes != 0 {
		h.opts.Filter.ErrorCodes = replayFuzzErrorCodeFilter
	}
	if flags&replayFuzzTimeWindow != 0 {
		h.opts.Filter.Since, h.opts.Filter.Until = replayFuzzSince, replayFuzzUntil
	}
	// The object store holds the payload under every gs:// URI and has
	// nothing anywhere else.
	h.opts.ObjectFetcher = func(_ context.Context, uri string) ([]byte, error) {
		if !strings.HasPrefix(uri, "gs://") {
			return nil, fmt.Errorf("no object at %q", uri)
		}
		return []byte(h.payload), nil
	}
	if h.canceled {
		ctx, cancel := context.WithCancel(h.ctx)
		cancel()
		h.ctx = ctx
	}
	return h
}

// checkRows replays rows as JSONL and as a JSON array and compares both runs
// with what each row predicts.
func (h *replayFuzzHarness) checkRows(t *testing.T, rows ...Record) {
	t.Helper()
	var jsonl, array bytes.Buffer
	var want Summary
	var wantBatch []byte
	array.WriteByte('[')
	for i, row := range rows {
		line, err := json.Marshal(row)
		if err != nil {
			t.Fatalf("encoding row %d: %v", i, err)
		}
		jsonl.Write(line)
		jsonl.WriteByte('\n')
		if i > 0 {
			array.WriteByte(',')
		}
		array.Write(line)

		// Predict from the row as Replay decodes it: encoding/json has
		// already replaced any invalid UTF-8 in it.
		var decoded Record
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.UseNumber()
		if err := dec.Decode(&decoded); err != nil {
			t.Fatalf("decoding row %d: %v", i, err)
		}
		sum, batch := h.expect(t, decoded)
		want = addReplayFuzzSummaries(want, sum)
		if batch != nil {
			wantBatch = batch
		}
	}
	array.WriteByte(']')
	if h.canceled {
		want, wantBatch = Summary{}, nil
	}

	lines := h.replay(t, "JSONL", jsonl.Bytes())
	arr := h.replay(t, "JSON array", array.Bytes())
	if lines.sum != want {
		t.Fatalf("JSONL replay counted %+v, want %+v (error: %v)", lines.sum, want, lines.err)
	}
	if arr.sum != lines.sum {
		t.Fatalf("JSON array replay counted %+v, JSONL replay %+v", arr.sum, lines.sum)
	}
	if fmt.Sprint(arr.err) != fmt.Sprint(lines.err) {
		t.Fatalf("JSON array replay returned %v, JSONL replay %v", arr.err, lines.err)
	}
	failed := want.FailedUnmarshal + want.FailedExport
	switch {
	case h.canceled:
		if !errors.Is(lines.err, context.Canceled) {
			t.Fatalf("canceled replay returned %v, want context.Canceled", lines.err)
		}
	case failed > 0 && lines.err == nil:
		t.Fatalf("%d rows failed but Replay returned no error", failed)
	case failed == 0 && lines.err != nil:
		t.Fatalf("no row failed but Replay returned %v", lines.err)
	}
	if !slices.EqualFunc(arr.batches, lines.batches, bytes.Equal) {
		t.Fatalf("JSON array and JSONL replays exported different batches")
	}
	if wantBatch == nil {
		return
	}
	if !bytes.Equal(lines.batches[0], wantBatch) {
		t.Fatalf("replayed batch differs from what the payload decodes to")
	}
	if h.built != nil && !bytes.Equal(wantBatch, h.built) {
		t.Fatalf("payload did not survive the OTLP/JSON round trip")
	}
}

// expect predicts how Replay counts row, which must already have been
// through the JSON decoding Replay applies. For a row that replays, it also
// returns the protobuf encoding of the batch the sink must receive.
func (h *replayFuzzHarness) expect(t *testing.T, row Record) (Summary, []byte) {
	t.Helper()
	sum := Summary{TotalRead: 1}
	f := h.opts.Filter
	switch {
	case !(Filter{Signal: f.Signal, ErrorCodes: f.ErrorCodes}).Matches(row):
		sum.SkippedFiltered++
		return sum, nil
	case !f.Since.IsZero() || !f.Until.IsZero():
		// A time window fails a row whose ingest_time does not parse
		// rather than filtering it out.
		if _, ok := row.ParsedIngestTime(); !ok {
			sum.FailedUnmarshal++
			return sum, nil
		}
		if !f.Matches(row) {
			sum.SkippedFiltered++
			return sum, nil
		}
	}
	sum.Matched++

	var payload []byte
	switch {
	case strings.TrimSpace(row.RawPayload) != "":
		payload = []byte(row.RawPayload)
	case row.PayloadRef != nil && strings.TrimSpace(row.PayloadRef.URI) != "":
		if !strings.HasPrefix(strings.TrimSpace(row.PayloadRef.URI), "gs://") {
			sum.FailedUnmarshal++
			return sum, nil
		}
		payload = []byte(h.payload)
	default:
		sum.SkippedNoPayload++
		return sum, nil
	}

	signal := CanonicalSignal(row.Signal)
	batch, ok := replayFuzzDecode(t, signal, payload)
	switch {
	case !ok:
		sum.FailedUnmarshal++
		return sum, nil
	case h.failing:
		sum.FailedExport++
		return sum, nil
	case signal == schema.SignalSpans:
		sum.ReplayedSpans++
	case signal == schema.SignalLogs:
		sum.ReplayedLogs++
	default:
		sum.ReplayedMetrics++
	}
	return sum, batch
}

// replayFuzzRun is the outcome of one Replay call.
type replayFuzzRun struct {
	sum     Summary
	err     error
	batches [][]byte
}

// replay runs Replay over input into a fresh sink and checks what holds for
// any input: every row read lands in exactly one outcome, nothing is read
// once the context is canceled, failures surface as an error, and the sink
// saw exactly the exports the summary reports.
func (h *replayFuzzHarness) replay(t *testing.T, name string, input []byte) replayFuzzRun {
	t.Helper()
	sink := &replayFuzzSink{MemorySink: NewMemorySink(), fail: h.failing}
	sum, err := Replay(h.ctx, bytes.NewReader(input), sink, h.opts)

	replayed := sum.ReplayedSpans + sum.ReplayedLogs + sum.ReplayedMetrics
	if sum.TotalRead != replayed+sum.SkippedFiltered+sum.SkippedNoPayload+sum.FailedUnmarshal+sum.FailedExport {
		t.Fatalf("%s: outcomes do not add up to the rows read: %+v", name, sum)
	}
	if h.canceled && sum.TotalRead != 0 {
		t.Fatalf("%s: read %d rows after the context was canceled", name, sum.TotalRead)
	}
	window := !h.opts.Filter.Since.IsZero() || !h.opts.Filter.Until.IsZero()
	if screened := sum.Matched + sum.SkippedFiltered; screened > sum.TotalRead || (!window && screened != sum.TotalRead) {
		t.Fatalf("%s: matched and filtered rows do not account for the rows read: %+v", name, sum)
	}
	if sum.Matched < replayed+sum.SkippedNoPayload+sum.FailedExport {
		t.Fatalf("%s: more rows replayed, skipped for lack of a payload, or rejected than matched: %+v", name, sum)
	}
	if failed := sum.FailedUnmarshal + sum.FailedExport; failed > 0 && err == nil {
		t.Fatalf("%s: %d rows failed but Replay returned no error", name, failed)
	}
	if sink.calls != replayed+sum.FailedExport {
		t.Fatalf("%s: sink saw %d exports, the summary reports %d replayed and %d rejected", name, sink.calls, replayed, sum.FailedExport)
	}
	if h.failing && replayed != 0 {
		t.Fatalf("%s: %d rows counted as replayed although the sink rejected every batch", name, replayed)
	}
	ms := sink.MemorySink
	if len(ms.Traces) != sum.ReplayedSpans || len(ms.Logs) != sum.ReplayedLogs || len(ms.Metrics) != sum.ReplayedMetrics {
		t.Fatalf("%s: sink holds %d traces, %d logs, and %d metrics batches, the summary reports %+v",
			name, len(ms.Traces), len(ms.Logs), len(ms.Metrics), sum)
	}
	return replayFuzzRun{sum: sum, err: err, batches: replayFuzzBatches(t, ms)}
}

func addReplayFuzzSummaries(a, b Summary) Summary {
	return Summary{
		TotalRead:        a.TotalRead + b.TotalRead,
		Matched:          a.Matched + b.Matched,
		ReplayedSpans:    a.ReplayedSpans + b.ReplayedSpans,
		ReplayedLogs:     a.ReplayedLogs + b.ReplayedLogs,
		ReplayedMetrics:  a.ReplayedMetrics + b.ReplayedMetrics,
		SkippedFiltered:  a.SkippedFiltered + b.SkippedFiltered,
		SkippedNoPayload: a.SkippedNoPayload + b.SkippedNoPayload,
		FailedUnmarshal:  a.FailedUnmarshal + b.FailedUnmarshal,
		FailedExport:     a.FailedExport + b.FailedExport,
	}
}

// replayFuzzIngestTime is how a row carries s: null when s is empty, a JSON
// number when numeric is set and s is one, and a string otherwise.
func replayFuzzIngestTime(s string, numeric bool) any {
	if s == "" {
		return nil
	}
	if numeric && json.Valid([]byte(s)) {
		dec := json.NewDecoder(strings.NewReader(s))
		dec.UseNumber()
		var v any
		if dec.Decode(&v) == nil {
			if n, ok := v.(json.Number); ok {
				return n
			}
		}
	}
	return s
}

// replayFuzzBatch encodes a batch of one item of signal (spans when signal
// is not recognized) that carries text, made valid UTF-8, as OTLP/JSON. It
// also returns the batch's protobuf encoding.
func replayFuzzBatch(t *testing.T, signal, text string) (string, []byte) {
	t.Helper()
	text = strings.ToValidUTF8(text, "\uFFFD")
	var js, pb []byte
	var jsErr, pbErr error
	switch signal {
	case schema.SignalLogs:
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", "fuzz")
		lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		lr.SetTimestamp(replayFuzzTimestamp)
		lr.Body().SetStr(text)
		lr.Attributes().PutStr("fuzz.text", text)
		js, jsErr = (&plog.JSONMarshaler{}).MarshalLogs(ld)
		pb, pbErr = (&plog.ProtoMarshaler{}).MarshalLogs(ld)
	case schema.SignalMetrics:
		md := pmetric.NewMetrics()
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("service.name", "fuzz")
		m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		m.SetName(text)
		dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetTimestamp(replayFuzzTimestamp)
		dp.SetIntValue(42)
		dp.Attributes().PutStr("fuzz.text", text)
		js, jsErr = (&pmetric.JSONMarshaler{}).MarshalMetrics(md)
		pb, pbErr = (&pmetric.ProtoMarshaler{}).MarshalMetrics(md)
	default:
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", "fuzz")
		span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		span.SetTraceID(pcommon.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
		span.SetSpanID(pcommon.SpanID{1, 2, 3, 4, 5, 6, 7, 8})
		span.SetName(text)
		span.SetStartTimestamp(replayFuzzTimestamp)
		span.SetEndTimestamp(replayFuzzTimestamp + 1_000_000)
		span.Attributes().PutStr("fuzz.text", text)
		js, jsErr = (&ptrace.JSONMarshaler{}).MarshalTraces(td)
		pb, pbErr = (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
	}
	if err := errors.Join(jsErr, pbErr); err != nil {
		t.Fatalf("encoding the generated %q batch: %v", signal, err)
	}
	return string(js), pb
}

// replayFuzzDecode is what Replay must make of a payload of signal: the
// protobuf encoding of the batch the OTLP/JSON unmarshaler decodes, or false
// when the signal is not recognized or the payload does not decode.
func replayFuzzDecode(t *testing.T, signal string, payload []byte) ([]byte, bool) {
	t.Helper()
	var pb []byte
	var err error
	switch signal {
	case schema.SignalSpans:
		td, uerr := (&ptrace.JSONUnmarshaler{}).UnmarshalTraces(payload)
		if uerr != nil {
			return nil, false
		}
		pb, err = (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
	case schema.SignalLogs:
		ld, uerr := (&plog.JSONUnmarshaler{}).UnmarshalLogs(payload)
		if uerr != nil {
			return nil, false
		}
		pb, err = (&plog.ProtoMarshaler{}).MarshalLogs(ld)
	case schema.SignalMetrics:
		md, uerr := (&pmetric.JSONUnmarshaler{}).UnmarshalMetrics(payload)
		if uerr != nil {
			return nil, false
		}
		pb, err = (&pmetric.ProtoMarshaler{}).MarshalMetrics(md)
	default:
		return nil, false
	}
	if err != nil {
		t.Fatalf("encoding a decoded %s batch: %v", signal, err)
	}
	return pb, true
}

// replayFuzzBatches returns the protobuf encoding of every batch sink holds,
// traces first, then logs, then metrics.
func replayFuzzBatches(t *testing.T, sink *MemorySink) [][]byte {
	t.Helper()
	var out [][]byte
	add := func(b []byte, err error) {
		if err != nil {
			t.Fatalf("encoding a replayed batch: %v", err)
		}
		out = append(out, b)
	}
	for _, td := range sink.Traces {
		add((&ptrace.ProtoMarshaler{}).MarshalTraces(td))
	}
	for _, ld := range sink.Logs {
		add((&plog.ProtoMarshaler{}).MarshalLogs(ld))
	}
	for _, md := range sink.Metrics {
		add((&pmetric.ProtoMarshaler{}).MarshalMetrics(md))
	}
	return out
}

// replayFuzzSink keeps batches like MemorySink and counts every export call.
// With fail set it rejects every batch instead.
type replayFuzzSink struct {
	*MemorySink
	fail  bool
	calls int
}

func (s *replayFuzzSink) accept() error {
	s.calls++
	if s.fail {
		return errReplayFuzzExport
	}
	return nil
}

func (s *replayFuzzSink) ExportTraces(ctx context.Context, td ptrace.Traces) error {
	if err := s.accept(); err != nil {
		return err
	}
	return s.MemorySink.ExportTraces(ctx, td)
}

func (s *replayFuzzSink) ExportLogs(ctx context.Context, ld plog.Logs) error {
	if err := s.accept(); err != nil {
		return err
	}
	return s.MemorySink.ExportLogs(ctx, ld)
}

func (s *replayFuzzSink) ExportMetrics(ctx context.Context, md pmetric.Metrics) error {
	if err := s.accept(); err != nil {
		return err
	}
	return s.MemorySink.ExportMetrics(ctx, md)
}
