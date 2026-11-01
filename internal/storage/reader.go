package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
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
// parquet-go panics when the target struct has a column the file lacks,
// so the file's own schema decides between the current row and the one
// written before the metadata column existed.
func DecodeParquet(data []byte) ([]*events.GitHubEvent, error) {
	probe, err := reader.NewParquetReader(buffer.NewBufferFileFromBytes(data), nil, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet: %w", err)
	}
	// the reader rewrites footer names into Go identifiers (Metadata), so
	// the comparison ignores case.
	hasMetadata := false
	for _, s := range probe.Footer.Schema {
		if strings.EqualFold(s.Name, "metadata") {
			hasMetadata = true
		}
	}
	probe.ReadStop()

	if hasMetadata {
		rows, err := readRows[parquetRow](data)
		if err != nil {
			return nil, err
		}
		out := make([]*events.GitHubEvent, 0, len(rows))
		for i := range rows {
			out = append(out, fromRow(&rows[i]))
		}
		return out, nil
	}
	rows, err := readRows[legacyRow](data)
	if err != nil {
		return nil, err
	}
	out := make([]*events.GitHubEvent, 0, len(rows))
	for i := range rows {
		r := rows[i].row()
		out = append(out, fromRow(&r))
	}
	return out, nil
}

func readRows[T any](data []byte) ([]T, error) {
	pr, err := reader.NewParquetReader(buffer.NewBufferFileFromBytes(data), new(T), 1)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet: %w", err)
	}
	defer pr.ReadStop()
	rows := make([]T, pr.GetNumRows())
	if err := pr.Read(&rows); err != nil {
		return nil, fmt.Errorf("failed to read parquet rows: %w", err)
	}
	return rows, nil
}

// legacyRow is the schema of files written before 2026-11-01, without the
// metadata column.
type legacyRow struct {
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

func (r *legacyRow) row() parquetRow {
	return parquetRow{
		ID: r.ID, Event: r.Event, HookID: r.HookID, HookInstallation: r.HookInstallation,
		ReceivedAt: r.ReceivedAt, Payload: r.Payload, Repository: r.Repository,
		Organization: r.Organization, Sender: r.Sender, Action: r.Action,
		OrganizationID: r.OrganizationID, RepositoryID: r.RepositoryID, SenderID: r.SenderID,
	}
}

func fromRow(r *parquetRow) *events.GitHubEvent {
	var metadata map[string]string
	if r.Metadata != "" {
		_ = json.Unmarshal([]byte(r.Metadata), &metadata)
	}
	return &events.GitHubEvent{
		Metadata:         metadata,
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
