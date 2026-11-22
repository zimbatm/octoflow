package compact

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/zimbatm/octoflow/internal/config"
	"github.com/zimbatm/octoflow/internal/restore"
	"github.com/zimbatm/octoflow/internal/storage"
	"github.com/zimbatm/octoflow/internal/storage/storagetest"
	"github.com/zimbatm/octoflow/internal/wipe"
	"github.com/zimbatm/octoflow/pkg/events"
)

var cfg = &config.S3Config{KeyPrefix: "events", DailyPrefix: "events-daily", CompressionType: "zstd", RowGroupSize: 10000}

func at(day, hour, minute int) time.Time {
	return time.Date(2026, 11, day, hour, minute, 0, 0, time.UTC)
}

func ev(id string, received time.Time) *events.GitHubEvent {
	return &events.GitHubEvent{ID: id, Event: "push", RepositoryID: "7", ReceivedAt: received,
		Payload: json.RawMessage(`{"ref":"refs/heads/main","id":"` + id + `"}`)}
}

// batch stores events as the file the s3 service uploads at uploaded.
func batch(t *testing.T, store *storagetest.Store, uploaded time.Time, evs ...*events.GitHubEvent) string {
	t.Helper()
	data, err := storage.EncodeParquet(cfg, evs)
	if err != nil {
		t.Fatal(err)
	}
	key := fmt.Sprintf("%s%d.parquet", storage.DayPrefix(cfg.KeyPrefix, uploaded), uploaded.UnixNano())
	store.Set(key, data)
	return key
}

func daily(t *testing.T, store *storagetest.Store, day int) []string {
	t.Helper()
	data, ok := store.File(storage.DailyKey(cfg.DailyPrefix, at(day, 0, 0)))
	if !ok {
		t.Fatalf("no daily file for day %d; the store holds %v", day, store.Keys())
	}
	evs, err := storage.DecodeParquet(data)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for i, e := range evs {
		if i > 0 && e.ReceivedAt.Before(evs[i-1].ReceivedAt) {
			t.Errorf("row %d (%s) was received before row %d", i, e.ID, i-1)
		}
		if string(e.Payload) != `{"ref":"refs/heads/main","id":"`+e.ID+`"}` {
			t.Errorf("row %s lost its payload: %s", e.ID, e.Payload)
		}
		ids = append(ids, e.ID)
	}
	return ids
}

func TestDayIsSortedAndHasOneRowPerID(t *testing.T) {
	store := &storagetest.Store{}
	// received on day 20 and 21, uploaded on 21
	batch(t, store, at(21, 0, 30), ev("before", at(20, 23, 50)), ev("c", at(21, 0, 20)), ev("a", at(21, 0, 10)))
	// out of order across files, and "a" redelivered after the duplicate window
	batch(t, store, at(21, 12, 0), ev("d", at(21, 11, 0)), ev("b", at(21, 0, 15)), ev("a", at(21, 11, 30)))
	// received on 21, uploaded after midnight with events of 22
	batch(t, store, at(22, 0, 40), ev("e", at(21, 23, 59)), ev("after", at(22, 0, 1)))
	// uploaded two days later: not read
	batch(t, store, at(23, 1, 0), ev("late", at(21, 5, 0)))

	st, err := Day(context.Background(), store, cfg, at(21, 0, 0), false)
	if err != nil {
		t.Fatal(err)
	}
	if got := daily(t, store, 21); !slices.Equal(got, []string{"a", "b", "c", "d", "e"}) {
		t.Errorf("daily file holds %v", got)
	}
	if st.Files != 3 || st.Rows != 6 || st.Events != 5 || st.Duplicates != 1 || !st.Written {
		t.Errorf("stats: %+v", st)
	}
}

func TestDayIsIdempotentAndFollowsTheBatchFiles(t *testing.T) {
	store := &storagetest.Store{}
	ctx := context.Background()
	key := storage.DailyKey(cfg.DailyPrefix, at(21, 0, 0))
	first := batch(t, store, at(21, 1, 0), ev("a", at(21, 0, 10)), ev("b", at(21, 0, 20)))

	if st, err := Day(ctx, store, cfg, at(20, 0, 0), false); err != nil || st.Written || st.Removed {
		t.Fatalf("a day without events: %+v, %v", st, err)
	}
	if _, ok := store.File(storage.DailyKey(cfg.DailyPrefix, at(20, 0, 0))); ok {
		t.Fatal("a day without events got a daily file")
	}

	if st, err := Day(ctx, store, cfg, at(21, 0, 0), true); err != nil || st.Events != 2 || st.Written {
		t.Fatalf("dry run: %+v, %v", st, err)
	}
	if _, ok := store.File(key); ok {
		t.Fatal("a dry run wrote the daily file")
	}

	if _, err := Day(ctx, store, cfg, at(21, 0, 0), false); err != nil {
		t.Fatal(err)
	}
	one, _ := store.File(key)
	if _, err := Day(ctx, store, cfg, at(21, 0, 0), false); err != nil {
		t.Fatal(err)
	}
	two, _ := store.File(key)
	if !bytes.Equal(one, two) {
		t.Error("a second run over the same batch files wrote other bytes")
	}

	batch(t, store, at(22, 0, 30), ev("late", at(21, 23, 0)))
	if _, err := Day(ctx, store, cfg, at(21, 0, 0), false); err != nil {
		t.Fatal(err)
	}
	if got := daily(t, store, 21); !slices.Equal(got, []string{"a", "b", "late"}) {
		t.Errorf("after a late batch the daily file holds %v", got)
	}

	// what a wipe does to the batch files
	rest, err := storage.EncodeParquet(cfg, []*events.GitHubEvent{ev("b", at(21, 0, 20))})
	if err != nil {
		t.Fatal(err)
	}
	store.Set(first, rest)
	if _, err := Day(ctx, store, cfg, at(21, 0, 0), false); err != nil {
		t.Fatal(err)
	}
	if got := daily(t, store, 21); !slices.Equal(got, []string{"b", "late"}) {
		t.Errorf("after a rewritten batch the daily file holds %v", got)
	}

	for _, k := range store.Keys() {
		if strings.HasPrefix(k, cfg.KeyPrefix+"/") {
			_ = store.Delete(ctx, k)
		}
	}
	st, err := Day(ctx, store, cfg, at(21, 0, 0), false)
	if err != nil || !st.Removed || st.Written {
		t.Fatalf("day left without events: %+v, %v", st, err)
	}
	if _, ok := store.File(key); ok {
		t.Error("the daily file of a day left without events must go")
	}
}

func TestRestoreAndTheArchiveWipeDoNotReadTheDailyFile(t *testing.T) {
	store := &storagetest.Store{}
	ctx := context.Background()
	batch(t, store, at(21, 1, 0), ev("a", at(21, 0, 10)))
	batch(t, store, at(21, 2, 0), ev("b", at(21, 1, 10)))
	if _, err := Day(ctx, store, cfg, at(21, 0, 0), false); err != nil {
		t.Fatal(err)
	}

	rs, err := restore.Run(ctx, store, nil, restore.Options{KeyPrefix: cfg.KeyPrefix, From: at(21, 0, 0), To: at(21, 0, 0)})
	if err != nil || rs.Files != 2 || rs.Rows != 2 {
		t.Errorf("restore read %+v, %v; want the two batch files", rs, err)
	}
	// without --from the wipe lists the whole batch prefix
	ws, err := wipe.Archive(ctx, store, cfg, wipe.Options{RepoID: "7"}, true)
	if err != nil || ws.Files != 2 || ws.Rows != 2 {
		t.Errorf("wipe read %+v, %v; want the two batch files", ws, err)
	}
}

// Two files whose events alternate, and a budget below one file: every
// row needs the file that was just dropped.
func TestCacheBudgetIsKeptAndTheOutputIsTheSame(t *testing.T) {
	store := &storagetest.Store{}
	ctx := context.Background()
	var one, two []*events.GitHubEvent
	var want []string
	for i := range 6 {
		id := fmt.Sprintf("e%d", i)
		want = append(want, id)
		if i%2 == 0 {
			one = append(one, ev(id, at(21, 1, i)))
		} else {
			two = append(two, ev(id, at(21, 1, i)))
		}
	}
	batch(t, store, at(21, 2, 0), one...)
	batch(t, store, at(21, 3, 0), two...)

	if _, err := Day(ctx, store, cfg, at(21, 0, 0), false); err != nil {
		t.Fatal(err)
	}
	roomy, _ := store.File(storage.DailyKey(cfg.DailyPrefix, at(21, 0, 0)))
	if reads := len(store.Gets()); reads != 4 {
		t.Errorf("%d reads with room for both files, want each file twice", reads)
	}

	defer func(old int64) { cacheBudget = old }(cacheBudget)
	cacheBudget = 1
	if _, err := Day(ctx, store, cfg, at(21, 0, 0), false); err != nil {
		t.Fatal(err)
	}
	if got := daily(t, store, 21); !slices.Equal(got, want) {
		t.Errorf("daily file holds %v", got)
	}
	tight, _ := store.File(storage.DailyKey(cfg.DailyPrefix, at(21, 0, 0)))
	if !bytes.Equal(roomy, tight) {
		t.Error("the budget changed the output")
	}
	if reads := len(store.Gets()) - 4; reads != 2+6 {
		t.Errorf("%d reads with no room, want 2 to locate the rows and one per row", reads)
	}
}

func TestDayFailsWhenABatchFileChangesUnderIt(t *testing.T) {
	store := &storagetest.Store{}
	ctx := context.Background()
	key := batch(t, store, at(21, 1, 0), ev("a", at(21, 0, 10)), ev("b", at(21, 0, 20)))
	if _, err := Day(ctx, store, cfg, at(21, 0, 0), false); err != nil {
		t.Fatal(err)
	}
	before, _ := store.File(storage.DailyKey(cfg.DailyPrefix, at(21, 0, 0)))

	// a wipe of "a" lands between the two reads
	wiped, err := storage.EncodeParquet(cfg, []*events.GitHubEvent{ev("b", at(21, 0, 20))})
	if err != nil {
		t.Fatal(err)
	}
	reads := 0
	store.OnGet = func(_ string, data []byte) []byte {
		if reads++; reads > 1 {
			return wiped
		}
		return data
	}
	st, err := Day(ctx, store, cfg, at(21, 0, 0), false)
	if err == nil || !strings.Contains(err.Error(), key) || st.Written {
		t.Fatalf("stats %+v, err = %v; want an error that names the batch file", st, err)
	}
	after, _ := store.File(storage.DailyKey(cfg.DailyPrefix, at(21, 0, 0)))
	if !bytes.Equal(before, after) {
		t.Error("a failed compaction replaced the daily file")
	}
}

func TestDayKeepsTheOldFileWhenTheUploadFails(t *testing.T) {
	store := &storagetest.Store{}
	ctx := context.Background()
	batch(t, store, at(21, 1, 0), ev("a", at(21, 0, 10)))
	if _, err := Day(ctx, store, cfg, at(21, 0, 0), false); err != nil {
		t.Fatal(err)
	}
	batch(t, store, at(21, 2, 0), ev("b", at(21, 1, 10)))
	boom := errors.New("boom")
	store.OnPutStream = func(string, []byte) ([]byte, error) { return nil, boom }
	if st, err := Day(ctx, store, cfg, at(21, 0, 0), false); !errors.Is(err, boom) || st.Written {
		t.Fatalf("stats %+v, err = %v", st, err)
	}
	if got := daily(t, store, 21); !slices.Equal(got, []string{"a"}) {
		t.Errorf("daily file holds %v after a failed upload", got)
	}
}
