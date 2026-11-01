package storage

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"github.com/zimbatm/octoflow/internal/config"
	"github.com/zimbatm/octoflow/internal/metrics"
	"github.com/zimbatm/octoflow/pkg/events"
)

// parquetRow is the on-disk schema. parquet-go cannot encode time.Time,
// json.RawMessage or maps, so GitHubEvent is not used directly.
type parquetRow struct {
	ID               string `parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8"`
	Event            string `parquet:"name=event, type=BYTE_ARRAY, convertedtype=UTF8"`
	HookID           string `parquet:"name=hook_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	HookInstallation string `parquet:"name=hook_installation, type=BYTE_ARRAY, convertedtype=UTF8"`
	ReceivedAt       int64  `parquet:"name=received_at, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	Payload          string `parquet:"name=payload, type=BYTE_ARRAY, convertedtype=UTF8"`
	Repository       string `parquet:"name=repository, type=BYTE_ARRAY, convertedtype=UTF8"`
	Organization     string `parquet:"name=organization, type=BYTE_ARRAY, convertedtype=UTF8"`
	Sender           string `parquet:"name=sender, type=BYTE_ARRAY, convertedtype=UTF8"`
	Action           string `parquet:"name=action, type=BYTE_ARRAY, convertedtype=UTF8"`
	OrganizationID   string `parquet:"name=organization_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	RepositoryID     string `parquet:"name=repository_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	SenderID         string `parquet:"name=sender_id, type=BYTE_ARRAY, convertedtype=UTF8"`
}

func toRow(e *events.GitHubEvent) parquetRow {
	return parquetRow{
		ID:               e.ID,
		Event:            e.Event,
		HookID:           e.HookID,
		HookInstallation: e.HookInstallation,
		ReceivedAt:       e.ReceivedAt.UnixMilli(),
		Payload:          string(e.Payload),
		Repository:       e.Repository,
		Organization:     e.Organization,
		Sender:           e.Sender,
		Action:           e.Action,
		OrganizationID:   e.OrganizationID,
		RepositoryID:     e.RepositoryID,
		SenderID:         e.SenderID,
	}
}

// Done is called once per event, after its batch is uploaded (nil) or
// dropped (the upload error). The caller acks or naks the source message there.
type Done func(error)

// Uploader stores one object under key.
type Uploader interface {
	Upload(ctx context.Context, key string, data []byte) error
}

// S3Writer batches events into Parquet files and hands each file to an
// Uploader. It is safe for concurrent use.
type S3Writer struct {
	config   *config.S3Config
	uploader Uploader
	now      func() time.Time

	mu        sync.Mutex
	batch     []*events.GitHubEvent
	done      []Done
	batchSize int64
	lastFlush time.Time
}

// NewS3Writer uploads to the bucket in cfg. Static credentials are used when
// AccessKeyID is set; otherwise the environment, the shared credentials
// file and the instance role are tried in that order.
func NewS3Writer(cfg *config.S3Config) (*S3Writer, error) {
	endpoint := cfg.Endpoint
	if endpoint == "" {
		endpoint = fmt.Sprintf("s3.%s.amazonaws.com", cfg.Region)
	}
	useSSL := !strings.HasPrefix(endpoint, "http://")
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")

	var creds *credentials.Credentials
	if cfg.AccessKeyID != "" {
		creds = credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken)
	} else {
		creds = credentials.NewChainCredentials([]credentials.Provider{
			&credentials.EnvAWS{},
			&credentials.FileAWSCredentials{},
			&credentials.IAM{},
		})
	}

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  creds,
		Secure: useSSL,
		Region: cfg.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}
	return NewWriter(cfg, &minioUploader{client: client, bucket: cfg.Bucket}), nil
}

// NewWriter batches events for up.
func NewWriter(cfg *config.S3Config, up Uploader) *S3Writer {
	return &S3Writer{
		config:    cfg,
		uploader:  up,
		now:       time.Now,
		batch:     make([]*events.GitHubEvent, 0, cfg.BatchSize),
		lastFlush: time.Now(),
	}
}

type minioUploader struct {
	client *minio.Client
	bucket string
}

func (u *minioUploader) Upload(ctx context.Context, key string, data []byte) error {
	_, err := u.client.PutObject(ctx, u.bucket, key, bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: "application/vnd.apache.parquet"})
	return err
}

// AddEvent adds an event to the batch and flushes when the batch is full.
func (w *S3Writer) AddEvent(event *events.GitHubEvent, done Done) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.batch = append(w.batch, event)
	w.done = append(w.done, done)
	w.batchSize += int64(len(event.Payload))

	if w.shouldFlush() {
		w.flushLocked()
	}
}

func (w *S3Writer) shouldFlush() bool {
	return len(w.batch) >= w.config.BatchSize ||
		w.batchSize >= w.config.BatchMaxBytes ||
		w.now().Sub(w.lastFlush) >= w.config.BatchTimeout
}

// FlushIfDue flushes when the batch timeout has passed.
func (w *S3Writer) FlushIfDue() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.batch) > 0 && w.now().Sub(w.lastFlush) >= w.config.BatchTimeout {
		w.flushLocked()
	}
}

// Flush writes the current batch. A failed upload is logged here and
// reported to each event's Done callback.
func (w *S3Writer) Flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.flushLocked()
}

// flushLocked always empties the batch: on failure the callers nak, and
// JetStream redelivers into a later batch.
func (w *S3Writer) flushLocked() {
	if len(w.batch) == 0 {
		return
	}

	start := w.now()
	eventCount := len(w.batch)
	batchSize := w.batchSize
	status := "success"

	err := w.upload()
	if err != nil {
		status = "upload_error"
		slog.Error("Failed to write batch to S3", "event_count", eventCount, "error", err)
	}
	metrics.RecordS3Write(status, eventCount, batchSize, w.now().Sub(start).Seconds())

	for _, done := range w.done {
		done(err)
	}
	w.batch = w.batch[:0]
	w.done = w.done[:0]
	w.batchSize = 0
	w.lastFlush = w.now()
}

func (w *S3Writer) upload() error {
	data, err := w.encodeParquet(w.batch)
	if err != nil {
		return fmt.Errorf("failed to encode parquet: %w", err)
	}

	key := w.objectKey()
	if err := w.uploader.Upload(context.Background(), key, data); err != nil {
		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	slog.Info("Uploaded batch to S3",
		"event_count", len(w.batch),
		"bucket", w.config.Bucket,
		"key", key,
		"bytes", len(data))
	return nil
}

func (w *S3Writer) encodeParquet(eventList []*events.GitHubEvent) ([]byte, error) {
	fw := buffer.NewBufferFile()

	pw, err := writer.NewParquetWriter(fw, new(parquetRow), 4)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet writer: %w", err)
	}

	switch w.config.CompressionType {
	case "gzip":
		pw.CompressionType = parquet.CompressionCodec_GZIP
	case "lz4":
		pw.CompressionType = parquet.CompressionCodec_LZ4
	default:
		pw.CompressionType = parquet.CompressionCodec_SNAPPY
	}
	pw.RowGroupSize = int64(w.config.RowGroupSize)

	for _, event := range eventList {
		if err := pw.Write(toRow(event)); err != nil {
			return nil, fmt.Errorf("failed to write event %s: %w", event.ID, err)
		}
	}
	if err := pw.WriteStop(); err != nil {
		return nil, fmt.Errorf("failed to finish parquet file: %w", err)
	}
	return fw.Bytes(), nil
}

// objectKey returns prefix/YYYY/MM/DD/<unix nanos>.parquet
func (w *S3Writer) objectKey() string {
	now := w.now().UTC()
	return fmt.Sprintf("%s/%04d/%02d/%02d/%d.parquet",
		w.config.KeyPrefix, now.Year(), now.Month(), now.Day(), now.UnixNano())
}
