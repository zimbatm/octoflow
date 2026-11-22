package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/xitongsys/parquet-go-source/writerfile"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
	"github.com/zimbatm/octoflow/internal/config"
	"github.com/zimbatm/octoflow/pkg/events"
)

// DailyKey is the key of the file that holds one UTC day of events,
// prefix/YYYY-MM-DD.parquet.
func DailyKey(dailyPrefix string, day time.Time) string {
	return fmt.Sprintf("%s/%s.parquet", strings.Trim(dailyPrefix, "/"), day.UTC().Format(time.DateOnly))
}

// DailyDay returns the day of a daily file, or false for any other key.
func DailyDay(dailyPrefix, key string) (time.Time, bool) {
	name, ok := strings.CutPrefix(key, strings.Trim(dailyPrefix, "/")+"/")
	if !ok {
		return time.Time{}, false
	}
	name, ok = strings.CutSuffix(name, ".parquet")
	if !ok {
		return time.Time{}, false
	}
	day, err := time.Parse(time.DateOnly, name)
	return day, err == nil
}

// StreamStore adds what a daily file needs: it is too large to hold in
// memory, so it is read by range and written as it is produced.
type StreamStore interface {
	ObjectStore
	// Open returns a handle that fetches the ranges it is asked for.
	Open(ctx context.Context, key string) (io.ReadSeekCloser, error)
	// PutStream stores what r yields under key. When r fails nothing is
	// stored, and an object already under key stays as it was.
	PutStream(ctx context.Context, key string, r io.Reader) error
}

// NewStreamStore reads and writes the bucket in cfg; see newClient for credentials.
func NewStreamStore(cfg *config.S3Config) (StreamStore, error) {
	client, err := newClient(cfg)
	if err != nil {
		return nil, err
	}
	return &minioStore{client: client, bucket: cfg.Bucket}, nil
}

func (s *minioStore) Open(ctx context.Context, key string) (io.ReadSeekCloser, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to open %s: %w", key, err)
	}
	return obj, nil
}

func (s *minioStore) PutStream(ctx context.Context, key string, r io.Reader) error {
	// without a length the client sizes its part buffer for a 5 TiB
	// object, 512 MiB; 16 MiB parts allow 160 GiB
	_, err := s.client.PutObject(ctx, s.bucket, key, r, -1,
		minio.PutObjectOptions{ContentType: "application/vnd.apache.parquet", PartSize: 16 << 20})
	if err != nil {
		return fmt.Errorf("failed to put %s: %w", key, err)
	}
	return nil
}

// rowGroupBytes is how much payload a row group of a streamed file
// holds, and with it what the writer holds before it writes.
const rowGroupBytes = 64 << 20

// ParquetWriter writes events as one archive file to an io.Writer, a row
// group at a time, so the file is never whole in memory.
type ParquetWriter struct {
	pw      *writer.ParquetWriter
	pending int
}

// NewParquetWriter starts an archive file on w with the compression of cfg.
func NewParquetWriter(cfg *config.S3Config, w io.Writer) (*ParquetWriter, error) {
	pw, err := newParquetWriter(cfg, writerfile.NewWriterFile(w))
	if err != nil {
		return nil, err
	}
	// the library closes a row group on its estimate of the row size,
	// taken from a few rows; after a run of small events it buffers far
	// more than it thinks. Write counts the bytes and closes the groups.
	pw.RowGroupSize = math.MaxInt64 / 2
	return &ParquetWriter{pw: pw}, nil
}

func (w *ParquetWriter) Write(event *events.GitHubEvent) error {
	if err := w.pw.Write(toRow(event)); err != nil {
		return fmt.Errorf("failed to write event %s: %w", event.ID, err)
	}
	if w.pending += len(event.Payload) + 256; w.pending >= rowGroupBytes {
		w.pending = 0
		if err := w.pw.Flush(true); err != nil {
			return fmt.Errorf("failed to write a row group: %w", err)
		}
	}
	return nil
}

// Close writes the last row group and the footer.
func (w *ParquetWriter) Close() error {
	if err := w.pw.WriteStop(); err != nil {
		return fmt.Errorf("failed to finish parquet file: %w", err)
	}
	return nil
}

// rangeFile lets the parquet reader work on a file it fetches by range.
// The reader opens one handle per column.
type rangeFile struct {
	io.ReadSeekCloser
	open func() (io.ReadSeekCloser, error)
}

func (f *rangeFile) Open(string) (source.ParquetFile, error) {
	h, err := f.open()
	if err != nil {
		return nil, err
	}
	return &rangeFile{ReadSeekCloser: h, open: f.open}, nil
}

func (f *rangeFile) Create(string) (source.ParquetFile, error) {
	return nil, errors.New("read only")
}

func (f *rangeFile) Write([]byte) (int, error) {
	return 0, errors.New("read only")
}

// readChunk is how many rows ReadParquet decodes at once.
const readChunk = 256

// ReadParquet calls fn for every event of an archive file, in file order.
// It holds readChunk rows and a page per column, not the file. open
// returns a new handle on the file each time.
func ReadParquet(open func() (io.ReadSeekCloser, error), fn func(*events.GitHubEvent) error) error {
	h, err := open()
	if err != nil {
		return err
	}
	defer func() { _ = h.Close() }()
	pr, err := reader.NewParquetReader(&rangeFile{ReadSeekCloser: h, open: open}, new(parquetRow), 1)
	if pr != nil {
		defer pr.ReadStop()
	}
	if err != nil {
		return fmt.Errorf("failed to open parquet: %w", err)
	}

	for left := pr.GetNumRows(); left > 0; {
		rows := make([]parquetRow, min(left, readChunk))
		if err := pr.Read(&rows); err != nil {
			return fmt.Errorf("failed to read parquet rows: %w", err)
		}
		if len(rows) == 0 {
			return errors.New("parquet file ends before its row count")
		}
		left -= int64(len(rows))
		for i := range rows {
			if err := fn(fromRow(&rows[i])); err != nil {
				return err
			}
		}
	}
	return nil
}

// PutParquet stores under key the archive file that fill writes, as it
// is written. When fill fails nothing is stored.
func PutParquet(ctx context.Context, store StreamStore, cfg *config.S3Config, key string, fill func(*ParquetWriter) error) error {
	pr, pw := io.Pipe()
	put := make(chan error, 1)
	go func() {
		err := store.PutStream(ctx, key, pr)
		// a writer blocked on the pipe must not wait for a reader that left
		_ = pr.CloseWithError(err)
		put <- err
	}()

	err := func() error {
		w, err := NewParquetWriter(cfg, pw)
		if err != nil {
			return err
		}
		if err := fill(w); err != nil {
			return err
		}
		return w.Close()
	}()
	_ = pw.CloseWithError(err)
	if putErr := <-put; err == nil {
		err = putErr
	}
	return err
}
