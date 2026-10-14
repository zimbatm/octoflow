package storage

import (
	"encoding/json"
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
