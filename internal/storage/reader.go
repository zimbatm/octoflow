package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/zimbatm/octoflow/internal/config"
	"github.com/zimbatm/octoflow/pkg/events"
)

// ObjectStore is the part of S3 that a restore reads and a wipe rewrites.
type ObjectStore interface {
	List(ctx context.Context, prefix string) ([]string, error)
	Get(ctx context.Context, key string) ([]byte, error)
	Put(ctx context.Context, key string, data []byte) error
	Delete(ctx context.Context, key string) error
}

// NewObjectStore reads the bucket in cfg; see newClient for credentials.
func NewObjectStore(cfg *config.S3Config) (ObjectStore, error) {
	client, err := newClient(cfg)
	if err != nil {
		return nil, err
	}
	return &minioStore{client: client, bucket: cfg.Bucket}, nil
}

type minioStore struct {
	client *minio.Client
	bucket string
}

func (s *minioStore) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for obj := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return nil, fmt.Errorf("failed to list %s: %w", prefix, obj.Err)
		}
		keys = append(keys, obj.Key)
	}
	return keys, nil
}

func (s *minioStore) Get(ctx context.Context, key string) ([]byte, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get %s: %w", key, err)
	}
	defer func() { _ = obj.Close() }()
	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", key, err)
	}
	return data, nil
}

func (s *minioStore) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, s.bucket, key, bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		return fmt.Errorf("failed to put %s: %w", key, err)
	}
	return nil
}

func (s *minioStore) Delete(ctx context.Context, key string) error {
	if err := s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("failed to delete %s: %w", key, err)
	}
	return nil
}

// DecodeParquet returns the events of one archive file, in file order.
func DecodeParquet(data []byte) ([]*events.GitHubEvent, error) {
	pr, err := reader.NewParquetReader(buffer.NewBufferFileFromBytes(data), new(parquetRow), 1)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet: %w", err)
	}
	defer pr.ReadStop()

	rows := make([]parquetRow, pr.GetNumRows())
	if err := pr.Read(&rows); err != nil {
		return nil, fmt.Errorf("failed to read parquet rows: %w", err)
	}
	out := make([]*events.GitHubEvent, 0, len(rows))
	for i := range rows {
		out = append(out, fromRow(&rows[i]))
	}
	return out, nil
}

func fromRow(r *parquetRow) *events.GitHubEvent {
	var metadata map[string]string
	if r.Metadata != "" {
		_ = json.Unmarshal([]byte(r.Metadata), &metadata)
	}
	return &events.GitHubEvent{
		ID:               r.ID,
		Event:            r.Event,
		HookID:           r.HookID,
		HookInstallation: r.HookInstallation,
		ReceivedAt:       time.UnixMilli(r.ReceivedAt).UTC(),
		Payload:          json.RawMessage(r.Payload),
		Repository:       r.Repository,
		Organization:     r.Organization,
		Sender:           r.Sender,
		Action:           r.Action,
		OrganizationID:   r.OrganizationID,
		RepositoryID:     r.RepositoryID,
		SenderID:         r.SenderID,
		Metadata:         metadata,
	}
}
