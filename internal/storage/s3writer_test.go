package storage

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/zimbatm/octoflow/internal/config"
	"github.com/zimbatm/octoflow/pkg/events"
)

func TestEncodeParquetRoundTrip(t *testing.T) {
	w := &S3Writer{config: &config.S3Config{CompressionType: "snappy", RowGroupSize: 10000}}
	received := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	in := []*events.GitHubEvent{{
		ID:           "d1",
		Event:        "pull_request",
		ReceivedAt:   received,
		Payload:      json.RawMessage(`{"action":"opened"}`),
		RepositoryID: "4542716",
		Metadata:     map[string]string{"source": "webhook"},
	}}

	data, err := w.encodeParquet(in)
	if err != nil {
		t.Fatal(err)
	}

	pr, err := reader.NewParquetReader(buffer.NewBufferFileFromBytes(data), new(parquetRow), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer pr.ReadStop()
	rows := make([]parquetRow, pr.GetNumRows())
	if err := pr.Read(&rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	got := rows[0]
	if got.ID != "d1" || got.Payload != `{"action":"opened"}` || got.RepositoryID != "4542716" {
		t.Errorf("unexpected row %+v", got)
	}
	if got.ReceivedAt != received.UnixMilli() {
		t.Errorf("received_at = %d, want %d", got.ReceivedAt, received.UnixMilli())
	}
}

type fakeUploader struct {
	keys [][]byte
	err  error
	last string
}

func (u *fakeUploader) Upload(_ context.Context, key string, data []byte) error {
	if u.err != nil {
		return u.err
	}
	u.last = key
	u.keys = append(u.keys, data)
	return nil
}

func rowCount(t *testing.T, data []byte) int {
	t.Helper()
	pr, err := reader.NewParquetReader(buffer.NewBufferFileFromBytes(data), new(parquetRow), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer pr.ReadStop()
	return int(pr.GetNumRows())
}

func testWriter(up Uploader, batchSize int) (*S3Writer, *time.Time) {
	cfg := &config.S3Config{
		KeyPrefix:       "events",
		BatchSize:       batchSize,
		BatchTimeout:    time.Minute,
		BatchMaxBytes:   1 << 20,
		CompressionType: "snappy",
		RowGroupSize:    100,
	}
	w := NewWriter(cfg, up)
	now := time.Date(2026, 10, 14, 9, 30, 0, 0, time.UTC)
	w.now = func() time.Time { return now }
	return w, &now
}

func event(id string) *events.GitHubEvent {
	return &events.GitHubEvent{ID: id, Event: "push", Payload: json.RawMessage(`{"ref":"refs/heads/main"}`)}
}

func TestFullBatchIsUploadedAndAcked(t *testing.T) {
	up := &fakeUploader{}
	w, _ := testWriter(up, 2)
	var acked []string
	done := func(id string) Done {
		return func(err error) {
			if err != nil {
				t.Errorf("event %s: unexpected error %v", id, err)
			}
			acked = append(acked, id)
		}
	}

	w.AddEvent(event("a"), done("a"))
	if len(up.keys) != 0 || len(acked) != 0 {
		t.Fatal("flushed before the batch was full")
	}
	w.AddEvent(event("b"), done("b"))

	if len(up.keys) != 1 {
		t.Fatalf("uploads = %d, want 1", len(up.keys))
	}
	if n := rowCount(t, up.keys[0]); n != 2 {
		t.Errorf("rows = %d, want 2", n)
	}
	if len(acked) != 2 {
		t.Errorf("acked = %v, want a and b", acked)
	}
	if up.last != "events/2026/10/14/1791970200000000000.parquet" {
		t.Errorf("key = %s", up.last)
	}
	if len(w.batch) != 0 || len(w.done) != 0 || w.batchSize != 0 {
		t.Error("batch not reset after flush")
	}
}

func TestFailedUploadNaksEveryEvent(t *testing.T) {
	up := &fakeUploader{err: errors.New("bucket gone")}
	w, _ := testWriter(up, 2)
	failed := 0
	done := func(err error) {
		if err == nil {
			t.Error("expected the upload error")
		}
		failed++
	}
	w.AddEvent(event("a"), done)
	w.AddEvent(event("b"), done)
	if failed != 2 {
		t.Errorf("done called with an error %d times, want 2", failed)
	}
	if len(w.batch) != 0 {
		t.Error("a failed batch must be dropped; JetStream redelivers")
	}

	up.err = nil
	w.AddEvent(event("c"), func(error) {})
	w.Flush()
	if len(up.keys) != 1 || rowCount(t, up.keys[0]) != 1 {
		t.Error("the next batch must not carry the failed events")
	}
}

func TestFlushIfDueWaitsForTheTimeout(t *testing.T) {
	up := &fakeUploader{}
	w, now := testWriter(up, 100)
	w.AddEvent(event("a"), func(error) {})

	w.FlushIfDue()
	if len(up.keys) != 0 {
		t.Fatal("flushed before the batch timeout")
	}
	*now = now.Add(2 * time.Minute)
	w.FlushIfDue()
	if len(up.keys) != 1 {
		t.Fatal("not flushed after the batch timeout")
	}
	w.FlushIfDue()
	if len(up.keys) != 1 {
		t.Error("an empty batch must not upload")
	}
}

func TestBatchMaxBytesTriggersFlush(t *testing.T) {
	up := &fakeUploader{}
	w, _ := testWriter(up, 100)
	w.config.BatchMaxBytes = 30
	w.AddEvent(event("a"), func(error) {})
	if len(up.keys) != 0 {
		t.Fatal("flushed under the byte limit")
	}
	w.AddEvent(event("b"), func(error) {})
	if len(up.keys) != 1 {
		t.Fatal("not flushed once the byte limit was reached")
	}
}

func TestDecodeParquetRestoresTheEvent(t *testing.T) {
	w := &S3Writer{config: &config.S3Config{CompressionType: "snappy", RowGroupSize: 10000}}
	received := time.Date(2026, 10, 14, 12, 0, 0, 123_000_000, time.UTC)
	in := &events.GitHubEvent{
		ID: "d1", Event: "pull_request", Action: "opened", HookID: "h", HookInstallation: "i",
		ReceivedAt: received, Payload: json.RawMessage(`{"action":"opened","number":7}`),
		Repository: "o/r", RepositoryID: "111", Organization: "o", OrganizationID: "222",
		Sender: "s", SenderID: "2", Metadata: map[string]string{"source": "webhook"},
	}
	data, err := w.encodeParquet([]*events.GitHubEvent{in, event("d2")})
	if err != nil {
		t.Fatal(err)
	}

	out, err := DecodeParquet(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 || out[1].ID != "d2" {
		t.Fatalf("decoded %d events, want d1 then d2", len(out))
	}
	got := out[0]
	want := *in
	// metadata survives the round trip as a JSON column
	if !got.ReceivedAt.Equal(received) {
		t.Errorf("received_at = %v, want %v (millisecond precision)", got.ReceivedAt, received)
	}
	got.ReceivedAt, want.ReceivedAt = time.Time{}, time.Time{}
	gotJSON, _ := json.Marshal(got)
	wantJSON, _ := json.Marshal(&want)
	if string(gotJSON) != string(wantJSON) {
		t.Errorf("decoded\n%s\nwant\n%s", gotJSON, wantJSON)
	}
}

func TestDayPrefix(t *testing.T) {
	got := DayPrefix("github-events", time.Date(2026, 1, 5, 23, 59, 0, 0, time.FixedZone("x", 3600)))
	if got != "github-events/2026/01/05/" {
		t.Errorf("DayPrefix = %q", got)
	}
}

func TestBatchTimeoutCountsFromTheFirstEvent(t *testing.T) {
	up := &fakeUploader{}
	w, now := testWriter(up, 100)
	*now = now.Add(3 * time.Hour)
	w.AddEvent(event("a"), func(error) {})
	w.FlushIfDue()
	if len(up.keys) != 0 {
		t.Fatal("a quiet period before the first event must not close the batch at once")
	}
	*now = now.Add(59 * time.Second)
	w.AddEvent(event("b"), func(error) {})
	w.FlushIfDue()
	if len(up.keys) != 0 {
		t.Fatal("flushed before the timeout since the first event")
	}
	*now = now.Add(time.Second)
	w.FlushIfDue()
	evs, _ := DecodeParquet(up.keys[0])
	if len(up.keys) != 1 || len(evs) != 2 {
		t.Fatalf("uploads = %d, events in the first = %d; want one file with both events", len(up.keys), len(evs))
	}
}
