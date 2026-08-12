// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigquery

import (
	"context"
	"errors"
	"testing"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSplitRequests(t *testing.T) {
	row := func(n int) []byte { return make([]byte, n) }

	t.Run("groups rows under the threshold", func(t *testing.T) {
		got := SplitRequests([][]byte{row(400), row(400), row(400)}, 1000)
		if len(got) != 2 {
			t.Fatalf("got %d requests, want 2", len(got))
		}
		if len(got[0]) != 2 || len(got[1]) != 1 {
			t.Fatalf("unexpected grouping: %d then %d", len(got[0]), len(got[1]))
		}
	})

	t.Run("no request exceeds the threshold", func(t *testing.T) {
		rows := make([][]byte, 50)
		for i := range rows {
			rows[i] = row(300)
		}
		for _, req := range SplitRequests(rows, 1000) {
			total := 0
			for _, r := range req {
				total += len(r)
			}
			// A single oversized row is the documented exception; none here.
			if total > 1000 && len(req) > 1 {
				t.Fatalf("request of %d bytes exceeds the 1000-byte threshold", total)
			}
		}
	})

	// Oversized rows must not vanish. The caller rejects them earlier with a
	// count; dropping one here would lose data with no record of it.
	t.Run("an oversized row still goes out alone", func(t *testing.T) {
		got := SplitRequests([][]byte{row(5000)}, 1000)
		if len(got) != 1 || len(got[0]) != 1 {
			t.Fatalf("oversized row was not preserved: %v", got)
		}
	})

	t.Run("every row survives the split", func(t *testing.T) {
		rows := make([][]byte, 37)
		for i := range rows {
			rows[i] = row(100)
		}
		count := 0
		for _, req := range SplitRequests(rows, 250) {
			count += len(req)
		}
		if count != len(rows) {
			t.Fatalf("split produced %d rows from %d", count, len(rows))
		}
	})
}

// --- fakes ---

type fakeResult struct {
	err  error
	resp *storagepb.AppendRowsResponse
}

func (f fakeResult) GetResult(context.Context) (int64, error) { return 0, f.err }
func (f fakeResult) FullResponse(context.Context) (*storagepb.AppendRowsResponse, error) {
	return f.resp, nil
}

type fakeStream struct {
	results  []appendResult
	dispatch error
	calls    int
	closed   int
}

func (f *fakeStream) AppendRows(_ context.Context, _ [][]byte, _ ...managedwriter.AppendOption) (appendResult, error) {
	if f.dispatch != nil {
		return nil, f.dispatch
	}
	r := f.results[min(f.calls, len(f.results)-1)]
	f.calls++
	return r, nil
}

func (f *fakeStream) Close() error { f.closed++; return nil }

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func newTestWriter(s *fakeStream) *Writer {
	return &Writer{
		opts: WriterOptions{Project: "p", Dataset: "d", Table: "t", MaxRequestBytes: 1000},
		factory: func(context.Context, ...managedwriter.WriterOption) (stream, error) {
			return s, nil
		},
	}
}

func TestAppendSuccess(t *testing.T) {
	s := &fakeStream{results: []appendResult{fakeResult{}}}
	out := newTestWriter(s).Append(context.Background(), [][]byte{{1}, {2}, {3}})

	if out.Err != nil {
		t.Fatalf("unexpected error: %v", out.Err)
	}
	if out.AcknowledgedRows != 3 {
		t.Fatalf("acknowledged %d rows, want 3", out.AcknowledgedRows)
	}
}

// Acknowledged rows must only be counted after the service confirms them.
func TestAppendCountsNothingOnFailure(t *testing.T) {
	s := &fakeStream{results: []appendResult{fakeResult{err: status.Error(codes.Unavailable, "down")}}}
	out := newTestWriter(s).Append(context.Background(), [][]byte{{1}, {2}})

	if out.AcknowledgedRows != 0 {
		t.Fatalf("acknowledged %d rows on a failed append, want 0", out.AcknowledgedRows)
	}
	if out.Verdict.Class != Retryable {
		t.Fatalf("class = %s, want retryable", out.Verdict.Class)
	}
}

// Row errors are reported per row, with indices translated into the caller's
// batch coordinates rather than the request's.
func TestAppendReportsRowErrors(t *testing.T) {
	resp := &storagepb.AppendRowsResponse{
		RowErrors: []*storagepb.RowError{
			{Index: 1, Code: storagepb.RowError_FIELDS_ERROR, Message: "bad field body"},
		},
	}
	s := &fakeStream{results: []appendResult{fakeResult{err: errors.New("row errors"), resp: resp}}}

	out := newTestWriter(s).Append(context.Background(), [][]byte{{1}, {2}, {3}})
	if len(out.RowErrors) != 1 {
		t.Fatalf("got %d row errors, want 1", len(out.RowErrors))
	}
	if out.RowErrors[0].Index != 1 {
		t.Fatalf("row index = %d, want 1", out.RowErrors[0].Index)
	}
}

// An invalid stream must be torn down so the next append rebuilds it, rather
// than retrying forever against a stream the service has finalized.
func TestAppendRecreatesStreamOnInvalidStream(t *testing.T) {
	s := &fakeStream{results: []appendResult{fakeResult{err: status.Error(codes.FailedPrecondition, "finalized")}}}
	w := newTestWriter(s)

	out := w.Append(context.Background(), [][]byte{{1}})
	if !out.Verdict.StreamRecreate {
		t.Fatal("expected the verdict to call for a stream recreate")
	}
	if s.closed == 0 {
		t.Fatal("the stream should have been closed so the next append rebuilds it")
	}
	if w.ms != nil {
		t.Fatal("writer should have dropped its stream reference")
	}
}

func TestAppendEmptyIsNoop(t *testing.T) {
	s := &fakeStream{}
	out := newTestWriter(s).Append(context.Background(), nil)
	if out.Err != nil || out.AcknowledgedRows != 0 || s.calls != 0 {
		t.Fatalf("empty append should do nothing, got %+v after %d calls", out, s.calls)
	}
}

func TestWriterOptionsRefs(t *testing.T) {
	o := WriterOptions{Project: "p", Dataset: "d", Table: "t"}
	if got, want := o.TableRef(), "projects/p/datasets/d/tables/t"; got != want {
		t.Errorf("TableRef() = %q, want %q", got, want)
	}
	if got, want := o.TableID(), "p.d.t"; got != want {
		t.Errorf("TableID() = %q, want %q", got, want)
	}
}

// Retry ownership is a contract, not a default. If write retries were ever
// enabled here, managedwriter and exporterhelper would both replay and the
// at-most-N+1 duplicate bound would stop holding.
func TestWriteRetriesAreExplicitlyDisabled(t *testing.T) {
	w := newTestWriter(&fakeStream{})
	w.opts.Descriptor = nil
	opts := w.writerOptions()
	if len(opts) == 0 {
		t.Fatal("expected writer options")
	}
	// The option set is opaque, so assert on the count as a change detector
	// alongside the explicit EnableWriteRetries(false) call in writerOptions.
	if len(opts) != 7 {
		t.Fatalf("writer option count changed to %d; confirm EnableWriteRetries(false) is still set", len(opts))
	}
}
