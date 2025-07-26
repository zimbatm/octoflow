package storage

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"github.com/zimbatm/octoflow/internal/config"
	"github.com/zimbatm/octoflow/internal/metrics"
	"github.com/zimbatm/octoflow/pkg/events"
)

// S3Writer writes events to S3 in Parquet format
type S3Writer struct {
	config      *config.S3Config
	minioClient *minio.Client
	batch       []*events.GitHubEvent
	batchSize   int64
	lastFlush   time.Time
}

// NewS3Writer creates a new S3 writer
func NewS3Writer(cfg *config.S3Config) (*S3Writer, error) {
	// Determine endpoint
	endpoint := cfg.Endpoint
	if endpoint == "" {
		// Default to AWS S3 endpoint for the region
		endpoint = fmt.Sprintf("s3.%s.amazonaws.com", cfg.Region)
	}

	// Determine if using SSL
	useSSL := true
	if len(endpoint) > 7 && endpoint[:7] == "http://" {
		useSSL = false
		endpoint = endpoint[7:]
	} else if len(endpoint) > 8 && endpoint[:8] == "https://" {
		endpoint = endpoint[8:]
	}

	// Create MinIO client options
	opts := &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken),
		Secure: useSSL,
		Region: cfg.Region,
	}

	// Create MinIO client
	minioClient, err := minio.New(endpoint, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create MinIO client: %w", err)
	}

	return &S3Writer{
		config:      cfg,
		minioClient: minioClient,
		batch:       make([]*events.GitHubEvent, 0, cfg.BatchSize),
		lastFlush:   time.Now(),
	}, nil
}

// AddEvent adds an event to the batch
func (w *S3Writer) AddEvent(event *events.GitHubEvent) error {
	w.batch = append(w.batch, event)
	w.batchSize += int64(len(event.Payload))

	// Check if we should flush
	if w.shouldFlush() {
		return w.Flush()
	}

	return nil
}

// shouldFlush determines if the batch should be flushed
func (w *S3Writer) shouldFlush() bool {
	return len(w.batch) >= w.config.BatchSize ||
		w.batchSize >= w.config.BatchMaxBytes ||
		time.Since(w.lastFlush) >= w.config.BatchTimeout
}

// Flush writes the current batch to S3
func (w *S3Writer) Flush() error {
	if len(w.batch) == 0 {
		return nil
	}

	start := time.Now()
	eventCount := len(w.batch)
	batchSize := w.batchSize
	status := "success"

	defer func() {
		// Record metrics
		metrics.RecordS3Write(status, eventCount, batchSize, time.Since(start).Seconds())
	}()

	// Generate S3 key based on current time
	key := w.generateS3Key()

	// Write to temporary file first
	tempFile := fmt.Sprintf("/tmp/octoflow_%d.parquet", time.Now().UnixNano())
	if err := w.writeParquetFile(tempFile, w.batch); err != nil {
		status = "write_error"
		return fmt.Errorf("failed to write parquet file: %w", err)
	}

	// Get file size for metrics
	if info, err := os.Stat(tempFile); err == nil {
		batchSize = info.Size()
	}

	// Upload to S3
	if err := w.uploadToS3(tempFile, key); err != nil {
		status = "upload_error"
		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	slog.Info("Uploaded batch to S3",
		"event_count", len(w.batch),
		"bucket", w.config.Bucket,
		"key", key)

	// Reset batch
	w.batch = w.batch[:0]
	w.batchSize = 0
	w.lastFlush = time.Now()

	return nil
}

// writeParquetFile writes events to a Parquet file
func (w *S3Writer) writeParquetFile(filename string, eventList []*events.GitHubEvent) error {
	fw, err := local.NewLocalFileWriter(filename)
	if err != nil {
		return fmt.Errorf("failed to create file writer: %w", err)
	}
	defer fw.Close()

	// Create Parquet writer - use the first event for schema if available
	var schemaObj interface{}
	if len(eventList) > 0 {
		schemaObj = eventList[0]
	} else {
		// Create empty event for schema
		schemaObj = &events.GitHubEvent{}
	}

	pw, err := writer.NewParquetWriter(fw, schemaObj, 4)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}
	defer pw.WriteStop()

	// Set compression
	switch w.config.CompressionType {
	case "snappy":
		pw.CompressionType = parquet.CompressionCodec_SNAPPY
	case "gzip":
		pw.CompressionType = parquet.CompressionCodec_GZIP
	case "lz4":
		pw.CompressionType = parquet.CompressionCodec_LZ4
	default:
		pw.CompressionType = parquet.CompressionCodec_SNAPPY
	}

	// Set row group size
	pw.RowGroupSize = int64(w.config.RowGroupSize)

	// Write events
	for _, event := range eventList {
		if err := pw.Write(event); err != nil {
			return fmt.Errorf("failed to write event: %w", err)
		}
	}

	return nil
}

// uploadToS3 uploads a file to S3
func (w *S3Writer) uploadToS3(filename, key string) error {
	// Open the file
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Get file info
	fileStat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	// Upload options
	opts := minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	}

	// Upload to S3
	_, err = w.minioClient.PutObject(
		context.Background(),
		w.config.Bucket,
		key,
		file,
		fileStat.Size(),
		opts,
	)

	if err != nil {
		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	// Clean up temp file
	os.Remove(filename)

	return nil
}

// generateS3Key generates an S3 key based on current time
func (w *S3Writer) generateS3Key() string {
	now := time.Now().UTC()

	// Format: prefix/year/month/day/timestamp_batchid.parquet
	return fmt.Sprintf("%s/%04d/%02d/%02d/%d_%s.parquet",
		w.config.KeyPrefix,
		now.Year(),
		now.Month(),
		now.Day(),
		now.UnixNano(),
		generateBatchID(),
	)
}

// generateBatchID generates a unique batch ID
func generateBatchID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// Close flushes any remaining events
func (w *S3Writer) Close() error {
	return w.Flush()
}
