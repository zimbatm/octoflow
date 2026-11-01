package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/zimbatm/octoflow/internal/config"
	"github.com/zimbatm/octoflow/pkg/events"
)

// ObjectStore is the part of S3 that a restore reads.
type ObjectStore interface {
	// List returns the keys under prefix, sorted.
	List(ctx context.Context, prefix string) ([]string, error)
	Get(ctx context.Context, key string) ([]byte, error)
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
	sort.Strings(keys)
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

// DecodeParquet returns the events of one archive file, in file order.
// Metadata is not archived, so restored events have none.
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
	}
}
