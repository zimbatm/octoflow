package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/layout"
	"github.com/xitongsys/parquet-go/source"
	"github.com/zimbatm/octoflow/internal/config"
)

// ClickHouse reads pages by the offset index and prunes them by the
// column index; DuckDB and the Go reader use neither, so only this test
// sees them.
func TestPageIndexIsWhatAReaderMayTrust(t *testing.T) {
	fw := buffer.NewBufferFile()
	pw, err := newParquetWriter(&config.S3Config{CompressionType: "zstd", RowGroupSize: 10000}, fw)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 40 {
		e := event(fmt.Sprintf("d%02d", i))
		e.Payload = json.RawMessage(fmt.Sprintf(`{"n":%d,"pad":"%x"}`, i, bytes.Repeat([]byte{byte(i)}, 100<<10)))
		if err := pw.Write(toRow(e)); err != nil {
			t.Fatal(err)
		}
	}
	if err := finishParquet(pw); err != nil {
		t.Fatal(err)
	}
	data := fw.Bytes()

	i, multi := 0, false
	for _, group := range pw.Footer.RowGroups {
		for _, chunk := range group.Columns {
			name := strings.Join(chunk.MetaData.PathInSchema, ".")
			at := chunk.MetaData.DataPageOffset
			pages := pw.OffsetIndexes[i].PageLocations
			multi = multi || len(pages) > 1
			for p, page := range pages {
				if page.Offset != at {
					t.Fatalf("%s page %d: offset %d, want %d, where the page before ends", name, p, page.Offset, at)
				}
				header, err := layout.ReadPageHeader(source.ConvertToThriftReader(buffer.NewBufferFileFromBytes(data), page.Offset, 4096))
				if err != nil {
					t.Fatalf("%s page %d: no page header at its offset: %v", name, p, err)
				}
				if page.CompressedPageSize <= header.CompressedPageSize {
					t.Errorf("%s page %d: size %d in the offset index does not cover the header and its %d bytes of data",
						name, p, page.CompressedPageSize, header.CompressedPageSize)
				}
				at += int64(page.CompressedPageSize)
			}
			if end := chunk.MetaData.DataPageOffset + chunk.MetaData.TotalCompressedSize; at != end {
				t.Errorf("%s: the pages end at %d, the column chunk at %d", name, at, end)
			}

			bounds := pw.ColumnIndexes[i]
			for p := range bounds.MinValues {
				if bytes.Compare(bounds.MinValues[p], bounds.MaxValues[p]) > 0 {
					t.Errorf("%s page %d: bounds %q to %q", name, p, bounds.MinValues[p], bounds.MaxValues[p])
				}
				if name == "payload" && (len(bounds.MinValues[p]) != 0 || bytes.Compare(bounds.MaxValues[p], []byte(`{"n"`)) <= 0) {
					t.Errorf("payload page %d: bounds %q to %q leave out its values", p, bounds.MinValues[p], bounds.MaxValues[p])
				}
			}
			i++
		}
	}
	if !multi {
		t.Error("no column has two pages, the test needs one")
	}
	if evs, err := DecodeParquet(data); err != nil || len(evs) != 40 {
		t.Errorf("read back %d events, %v", len(evs), err)
	}
}
