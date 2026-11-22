package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/zimbatm/octoflow/internal/config"
	"github.com/zimbatm/octoflow/internal/storage/storagetest"
	"github.com/zimbatm/octoflow/pkg/events"
)

var _ StreamStore = (*storagetest.Store)(nil)

func TestDailyKey(t *testing.T) {
	day := time.Date(2026, 11, 21, 23, 59, 0, 0, time.UTC)
	key := DailyKey("github-events-daily", day)
	if key != "github-events-daily/2026-11-21.parquet" {
		t.Errorf("DailyKey = %q", key)
	}
	if got, ok := DailyDay("github-events-daily", key); !ok || !got.Equal(day.Truncate(24*time.Hour)) {
		t.Errorf("DailyDay(%q) = %v, %v", key, got, ok)
	}
	for _, other := range []string{"github-events/2026/11/21/1.parquet", "github-events-daily/README.md", "github-events-daily/2026-11.parquet"} {
		if _, ok := DailyDay("github-events-daily", other); ok {
			t.Errorf("%q must not count as a daily file", other)
		}
	}
	// restore and wipe list the batch files by these two prefixes
	for _, prefix := range []string{DayPrefix("github-events", day), "github-events/"} {
		if strings.HasPrefix(key, prefix) {
			t.Errorf("a listing of %q would return the daily file %q", prefix, key)
		}
	}
}

func bytesOpener(data []byte, opened *int) func() (io.ReadSeekCloser, error) {
	return func() (io.ReadSeekCloser, error) {
		*opened++
		return struct {
			*bytes.Reader
			io.Closer
		}{bytes.NewReader(data), io.NopCloser(nil)}, nil
	}
}

// 700 events of 100 kB pass the 64 MiB at which the writer closes a row
// group, so the file has more than one and the reader must cross them.
func TestStreamedFileRoundTripsAcrossRowGroups(t *testing.T) {
	cfg := &config.S3Config{CompressionType: "snappy", RowGroupSize: 10000}
	var buf bytes.Buffer
	w, err := NewParquetWriter(cfg, &buf)
	if err != nil {
		t.Fatal(err)
	}
	pad := strings.Repeat("x", 100<<10)
	start := time.Date(2026, 11, 21, 0, 0, 0, 0, time.UTC)
	const n = 700
	for i := range n {
		e := event(fmt.Sprintf("d%03d", i))
		e.ReceivedAt = start.Add(time.Duration(i) * time.Second)
		e.Payload = json.RawMessage(fmt.Sprintf(`{"n":%d,"pad":%q}`, i, pad))
		e.Metadata = map[string]string{"source": "webhook"}
		if err := w.Write(e); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if buf.Len() > n*len(pad)/4 {
		t.Errorf("file is %d bytes for %d bytes of payload", buf.Len(), n*len(pad))
	}

	var got []*events.GitHubEvent
	opened := 0
	err = ReadParquet(bytesOpener(buf.Bytes(), &opened), func(ev *events.GitHubEvent) error {
		got = append(got, ev)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != n {
		t.Fatalf("read %d events, want %d", len(got), n)
	}
	for i, ev := range got {
		if ev.ID != fmt.Sprintf("d%03d", i) || !ev.ReceivedAt.Equal(start.Add(time.Duration(i)*time.Second)) ||
			!strings.HasPrefix(string(ev.Payload), fmt.Sprintf(`{"n":%d,`, i)) || ev.Metadata["source"] != "webhook" {
			t.Fatalf("event %d came back as %s at %v", i, ev.ID, ev.ReceivedAt)
		}
	}
	if opened < 2 {
		t.Errorf("the reader opened %d handles; it must take one per column", opened)
	}

	pr, err := reader.NewParquetReader(buffer.NewBufferFileFromBytes(buf.Bytes()), new(parquetRow), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer pr.ReadStop()
	if groups := len(pr.Footer.RowGroups); groups < 2 {
		t.Errorf("the file has %d row groups, the test needs two", groups)
	}

	// the batch reader takes the same file
	all, err := DecodeParquet(buf.Bytes())
	if err != nil || len(all) != n {
		t.Fatalf("DecodeParquet: %d events, %v", len(all), err)
	}
}

func TestReadParquetStopsOnTheCallbackError(t *testing.T) {
	data, err := EncodeParquet(&config.S3Config{CompressionType: "zstd", RowGroupSize: 10000}, []*events.GitHubEvent{event("a"), event("b")})
	if err != nil {
		t.Fatal(err)
	}
	stop := errors.New("stop")
	seen, opened := 0, 0
	err = ReadParquet(bytesOpener(data, &opened), func(*events.GitHubEvent) error {
		seen++
		return stop
	})
	if !errors.Is(err, stop) || seen != 1 {
		t.Errorf("err = %v after %d events, want the callback error after the first", err, seen)
	}
}

func TestPutParquetStoresNothingWhenTheWriterFails(t *testing.T) {
	cfg := &config.S3Config{CompressionType: "zstd", RowGroupSize: 10000}
	store := &storagetest.Store{}
	store.Set("k", []byte("old"))
	ctx := context.Background()

	boom := errors.New("boom")
	err := PutParquet(ctx, store, cfg, "k", func(w *ParquetWriter) error {
		if err := w.Write(event("a")); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the writer's error", err)
	}
	if data, _ := store.File("k"); string(data) != "old" {
		t.Error("a failed write replaced the object")
	}

	store.OnPutStream = func(string, []byte) ([]byte, error) { return nil, boom }
	err = PutParquet(ctx, store, cfg, "k", func(w *ParquetWriter) error { return w.Write(event("a")) })
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the store's error", err)
	}

	store.OnPutStream = nil
	if err := PutParquet(ctx, store, cfg, "k", func(w *ParquetWriter) error { return w.Write(event("a")) }); err != nil {
		t.Fatal(err)
	}
	data, _ := store.File("k")
	if evs, err := DecodeParquet(data); err != nil || len(evs) != 1 || evs[0].ID != "a" {
		t.Errorf("stored file: %v, %v", evs, err)
	}
}
