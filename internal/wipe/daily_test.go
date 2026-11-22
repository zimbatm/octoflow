package wipe

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/zimbatm/octoflow/internal/compact"
	"github.com/zimbatm/octoflow/internal/config"
	"github.com/zimbatm/octoflow/internal/storage"
	"github.com/zimbatm/octoflow/internal/storage/storagetest"
	"github.com/zimbatm/octoflow/pkg/events"
)

var dailyCfg = &config.S3Config{KeyPrefix: "events", DailyPrefix: "events-daily", CompressionType: "zstd", RowGroupSize: 10000}

func day(d, hour int) time.Time { return time.Date(2026, 11, d, hour, 0, 0, 0, time.UTC) }

// archived returns a store with batch files and the daily files compact
// makes of them: day 3 holds a (user 1) and b (user 2), day 4 only c
// (user 1), day 5 only d (user 3).
func archived(t *testing.T) *storagetest.Store {
	t.Helper()
	store := &storagetest.Store{}
	for d, evs := range map[int][]*events.GitHubEvent{
		3: {pr("a", "7", 1, "1", day(3, 9)), pr("b", "7", 2, "2", day(3, 10))},
		4: {pr("c", "7", 3, "1", day(4, 9))},
		5: {pr("d", "8", 1, "3", day(5, 9))},
	} {
		data, err := storage.EncodeParquet(dailyCfg, evs)
		if err != nil {
			t.Fatal(err)
		}
		store.Set(storage.DayPrefix(dailyCfg.KeyPrefix, day(d, 0))+"1.parquet", data)
		if _, err := compact.Day(context.Background(), store, dailyCfg, day(d, 0), false); err != nil {
			t.Fatal(err)
		}
	}
	return store
}

func dailyIDs(t *testing.T, store *storagetest.Store, d int) []string {
	t.Helper()
	data, ok := store.File(storage.DailyKey(dailyCfg.DailyPrefix, day(d, 0)))
	if !ok {
		return nil
	}
	evs, err := storage.DecodeParquet(data)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, ev := range evs {
		if len(ev.Payload) == 0 {
			t.Errorf("%s lost its payload", ev.ID)
		}
		ids = append(ids, ev.ID)
	}
	return ids
}

func TestDailyRewritesAndRemovesFiles(t *testing.T) {
	store := archived(t)
	ctx := context.Background()
	untouched, _ := store.File(storage.DailyKey(dailyCfg.DailyPrefix, day(5, 0)))
	before := store.Keys()

	st, err := Daily(ctx, store, dailyCfg, Options{UserID: "1"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if st.Files != 3 || st.Rows != 4 || st.Matched != 2 || st.Rewritten != 0 || st.Removed != 0 {
		t.Errorf("dry run: %+v", st)
	}
	if !slices.Equal(before, store.Keys()) || !slices.Equal(dailyIDs(t, store, 3), []string{"a", "b"}) {
		t.Error("a dry run changed the daily files")
	}

	st, err = Daily(ctx, store, dailyCfg, Options{UserID: "1"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if st.Matched != 2 || st.Rewritten != 1 || st.Removed != 1 {
		t.Errorf("wipe: %+v", st)
	}
	if got := dailyIDs(t, store, 3); !slices.Equal(got, []string{"b"}) {
		t.Errorf("day 3 holds %v, want only b", got)
	}
	if got := dailyIDs(t, store, 4); got != nil {
		t.Errorf("day 4 holds %v; a file left without rows must be removed", got)
	}
	if after, _ := store.File(storage.DailyKey(dailyCfg.DailyPrefix, day(5, 0))); !bytes.Equal(untouched, after) {
		t.Error("a daily file without a selected event was written")
	}

	st, err = Daily(ctx, store, dailyCfg, Options{UserID: "1"}, true)
	if err != nil || st.Matched != 0 || st.Files != 2 {
		t.Errorf("after the wipe: %+v, %v", st, err)
	}
}

func TestDailyReadsOnlyTheDaysAsked(t *testing.T) {
	store := archived(t)
	st, err := Daily(context.Background(), store, dailyCfg, Options{UserID: "1", From: day(4, 0), To: day(5, 0)}, false)
	if err != nil {
		t.Fatal(err)
	}
	if st.Files != 2 || st.Matched != 1 || st.Removed != 1 {
		t.Errorf("wipe of days 4 to 5: %+v", st)
	}
	if got := dailyIDs(t, store, 3); !slices.Equal(got, []string{"a", "b"}) {
		t.Errorf("day 3 is outside the range and holds %v", got)
	}
}

// A wipe that stopped after the batch files leaves the event in the
// daily file only; the next run must still find it there.
func TestDailyDoesNotDependOnTheBatchFiles(t *testing.T) {
	store := archived(t)
	ctx := context.Background()
	as, err := Archive(ctx, store, dailyCfg, Options{IDs: []string{"a"}}, false)
	if err != nil || as.Rewritten != 1 {
		t.Fatalf("archive wipe: %+v, %v", as, err)
	}
	as, err = Archive(ctx, store, dailyCfg, Options{IDs: []string{"a"}}, false)
	if err != nil || as.Matched != 0 {
		t.Fatalf("second archive wipe: %+v, %v", as, err)
	}
	// the batch files are gone altogether, as under a lifecycle rule
	for _, k := range store.Keys() {
		if strings.HasPrefix(k, dailyCfg.KeyPrefix+"/") {
			_ = store.Delete(ctx, k)
		}
	}
	st, err := Daily(ctx, store, dailyCfg, Options{IDs: []string{"a"}}, false)
	if err != nil || st.Matched != 1 || st.Rewritten != 1 {
		t.Fatalf("daily wipe: %+v, %v", st, err)
	}
	if got := dailyIDs(t, store, 3); !slices.Equal(got, []string{"b"}) {
		t.Errorf("day 3 holds %v, want b kept without its batch file", got)
	}
}

func TestDailyFailsWhenTheEventIsStillThere(t *testing.T) {
	ctx := context.Background()
	key := storage.DailyKey(dailyCfg.DailyPrefix, day(3, 0))

	store := archived(t)
	boom := errors.New("boom")
	store.OnPutStream = func(string, []byte) ([]byte, error) { return nil, boom }
	st, err := Daily(ctx, store, dailyCfg, Options{IDs: []string{"a"}}, false)
	if !errors.Is(err, boom) || st.Rewritten != 0 {
		t.Errorf("failed upload: %+v, %v", st, err)
	}

	// a compaction that read the batch files before they were wiped
	// uploads its file right after the rewrite
	store = archived(t)
	old, _ := store.File(key)
	store.OnPutStream = func(string, []byte) ([]byte, error) { return old, nil }
	st, err = Daily(ctx, store, dailyCfg, Options{IDs: []string{"a"}}, false)
	if err == nil || !strings.Contains(err.Error(), key) || st.Rewritten != 0 {
		t.Errorf("file put back with the event: %+v, %v; want an error that names it", st, err)
	}
}

func TestDailyIgnoresOtherObjectsUnderThePrefix(t *testing.T) {
	store := archived(t)
	store.Set(dailyCfg.DailyPrefix+"/README.md", []byte("about this dataset"))
	st, err := Daily(context.Background(), store, dailyCfg, Options{UserID: "3"}, false)
	if err != nil || st.Files != 3 || st.Removed != 1 {
		t.Errorf("%+v, %v", st, err)
	}
	if _, ok := store.File(dailyCfg.DailyPrefix + "/README.md"); !ok {
		t.Error(fmt.Sprint("the wipe removed an object that is not a daily file: ", store.Keys()))
	}
}
